import logging
import uuid
from copy import copy
from datetime import datetime, timezone
from typing import Iterable, List, Dict, Any, Optional, override

import clickhouse_connect

from ListingRecord import ListingRecord, read_listings
from usecases import Usecases, DEFAULT_MIN_LISTINGS, DEFAULT_MAX_PRICE, DEFAULT_BROKER_ID, DEFAULT_PERCENT_DELTA, \
    DEFAULT_LIMIT, DEFAULT_POSTAL_CODE, DEFAULT_BELOW_AVG_PCT, DEFAULT_CITY, DEFAULT_MIN_BEDROOMS, DEFAULT_MAX_SIZE_SQM, \
    DEFAULT_DATA_FILE_PATH_FOR_IMPORT, DEFAULT_BATCH_SIZE

logger = logging.getLogger(__name__)
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)



class ClickHouseAdapter(Usecases):
    def __init__(self, host: str = '152.53.248.27', port: int = 8123, database: str = 'default', user: str = 'mds',
                 password: str = 'mds'):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.table_name = 'real_estate_listings'  # Changed from f'{self.database}.{self.table_name}' to just table name for queries. Database is specified in client.
        self.fq_table_name = f'{self.database}.{self.table_name}'  # Fully qualified for DDL
        self._client: Optional[clickhouse_connect.driver.Client] = None

        self._connect()
        self._create_table_if_not_exists()

    def _connect(self) -> None:
        try:
            if self._client:  # Close existing client before creating a new one
                try:
                    self._client.close()
                except Exception:  # nosemgrep
                    pass  # Ignore errors on close if we are reconnecting
            self._client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                database=self.database,  # Specify database at client level
                user=self.user,
                password=self.password,
                connect_timeout=10,  # seconds
                send_receive_timeout=300  # seconds, for long queries/mutations
            )
            self._client.ping()
            logger.info(f"Successfully connected to ClickHouse server {self.host}:{self.port}, database '{self.database}'")
        except Exception as e:
            logger.info(f"Failed to connect to ClickHouse ({self.host}:{self.port}, db: {self.database}): {e}")
            self._client = None  # Ensure client is None if connection failed
            raise ConnectionError(f"ClickHouse connection failed: {e}")

    def _get_client(self) -> clickhouse_connect.driver.Client:
        # Check if client is None or not alive (ping failed)
        client_uninitialized = self._client is None
        client_not_responding = False
        if not client_uninitialized:
            try:
                if not self._client.ping():  # type: ignore
                    client_not_responding = True
            except Exception:  # Could be various connection errors
                client_not_responding = True

        if client_uninitialized or client_not_responding:
            logger.info("ClickHouse client is not available or not responding, attempting to reconnect...")
            self._connect()  # This will raise ConnectionError if it fails

        if self._client is None:  # Should not happen if _connect succeeds or raises
            raise ConnectionError("ClickHouse client is None after connection attempt.")
        return self._client  # type: ignore

    def _create_table_if_not_exists(self) -> None:
        client = self._get_client()
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.fq_table_name} (
            id UUID,
            brokered_by Int32,
            status Nullable(String),
            price DECIMAL(14, 2),
            lot_size_sqm Float64,
            street DECIMAL(15,2),
            city String,
            state String ,
            zip_code Int32,
            bed Nullable(Int16),
            bath Nullable(Int16),
            house_size_sqm Nullable(DECIMAL(10,2)),
            prev_sold_date Nullable(DateTime),
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (city, state, zip_code, id)
        """
        try:
            client.command(create_table_query)
            logger.info(f"Table {self.fq_table_name} ensured to exist.")
        except Exception as e:
            logger.info(f"Failed to create table {self.fq_table_name}: {e}")
            raise

    def _rows_to_dicts(self, query_result: clickhouse_connect.driver.summary.QueryResult) -> List[Dict[str, Any]]:
        column_names = query_result.column_names
        return [dict(zip(column_names, row)) for row in query_result.result_rows]

    def reset_database(self) -> None:
        client = self._get_client()
        try:
            client.command(f'TRUNCATE TABLE IF EXISTS {self.fq_table_name}')
            client.command(f'ALTER TABLE {self.fq_table_name} DROP COLUMN IF EXISTS solar_panels')
            logger.info(f"Table {self.fq_table_name} truncated.")
        except Exception as e:
            logger.info(f"Failed to reset database (truncate table {self.fq_table_name}): {e}")
            raise

    @override
    def usecase1_filter_properties(self, min_listings: int = DEFAULT_MIN_LISTINGS, max_price: float= DEFAULT_MAX_PRICE) -> List[Dict[str, Any]]:
        client = self._get_client()
        query = f"""
SELECT *
FROM {self.table_name}
WHERE tuple(city, state) IN ( -- Check against a tuple of (city, state)
    SELECT city, state        -- Select both city and state
    FROM {self.table_name}
    GROUP BY city, state      -- Group by both city and state
    HAVING count(*) > %(min_listings)s
)
AND price < %(max_price)s
        """
        try:

            result = client.query_df(query, parameters={'min_listings': min_listings, 'max_price': max_price})
            return result
        except Exception as e:
            logger.info(f"Usecase 1 error: {e}")
            raise

    def usecase2_update_prices(
        self,
        broker_id: str = DEFAULT_BROKER_ID,
        percent_delta: float = DEFAULT_PERCENT_DELTA,
        limit: int = DEFAULT_LIMIT
    ) -> int:
        client = self._get_client()
        if limit <= 0:
            return 0

        try:
            broker_id_float = float(broker_id)
        except ValueError:
            logger.info(f"Invalid broker_id format: '{broker_id}'. Must be convertible to float.")
            return 0

        select_ids_query = f"""
        SELECT id
        FROM {self.table_name}
        WHERE brokered_by = %(broker_id_float)s
        ORDER BY id -- Stable ordering for "first limit"
        LIMIT %(limit)s
        """
        try:
            query_result = client.query(select_ids_query,
                                                     parameters={'broker_id_float': broker_id_float, 'limit': limit})
            ids_to_update = [row[0] for row in query_result.result_rows if row]

            if not ids_to_update:
                return 0

            update_query = f"""
            ALTER TABLE {self.fq_table_name}
            UPDATE price = price * (1 + %(percent_delta)s)
            WHERE id IN %(ids_to_update)s
            """
            # mutations_sync=1 waits for local server, =2 waits for all replicas.
            # For performance test, synchronous is important.
            client.command(update_query, parameters={'percent_delta': percent_delta, 'ids_to_update': ids_to_update},
                           settings={'mutations_sync': 1})
            return len(ids_to_update)
        except Exception as e:
            logger.info(f"Usecase 2 error: {e}")
            raise

    def usecase3_add_solar_panels(self) -> int:
        client = self._get_client()

        # 1) Add the new column if it doesn’t already exist
        add_column_query = f"""
        ALTER TABLE {self.fq_table_name}
        ADD COLUMN IF NOT EXISTS solar_panels Nullable(UInt8) DEFAULT NULL
        """
        try:
            client.command(add_column_query, settings={'mutations_sync': 1})
        except Exception as e:
            logger.info(f"Usecase 3 (add column) error: {e}")
            raise

        # 2) Populate the column (0 or 1) for every existing row
        update_column_query = f"""
        ALTER TABLE {self.fq_table_name}
        UPDATE solar_panels = modulo(rand(), 2)
        WHERE 1 = 1
        """
        try:
            client.command(update_column_query, settings={'mutations_sync': 1})
        except Exception as e:
            logger.info(f"Usecase 3 (update column) error: {e}")
            raise

        # 3) Return how many rows we now have in the table
        count_query = f"SELECT count() FROM {self.table_name}"
        try:
            query_result = client.query(count_query)
            total_rows = 0
            if query_result.result_rows and query_result.result_rows[0] and query_result.result_rows[0][0] is not None:
                total_rows = query_result.result_rows[0][0]
            return total_rows
        except Exception as e:
            logger.info(f"Usecase 3 (count) error: {e}")
            raise


    def usecase4_price_analysis(
        self,
        postal_code: str = DEFAULT_POSTAL_CODE,
        below_avg_pct: float = DEFAULT_BELOW_AVG_PCT,
        city: str = DEFAULT_CITY
    ) -> Dict[str, List[Dict[str, Any]]]:
        client = self._get_client()
        results: Dict[str, List[Dict[str, Any]]] = {"below_threshold": [], "sorted_by_city": []}

        try: postal_code_int = int(postal_code)
        except ValueError:
            logger.info(f"Invalid postal_code format: '{postal_code}'. Must be an integer.")
            return results

        # Part 1: Find properties below threshold
        avg_price_query = f"""
        SELECT avgIf(price / lot_size_sqm, lot_size_sqm > 0 AND price > 0)
        FROM {self.table_name} WHERE zip_code = %(postal_code_int)s
        """
        try:
            query_result_avg = client.query(avg_price_query, parameters={'postal_code_int': postal_code_int})
            local_avg_price_per_sqm = None
            if query_result_avg.result_rows and query_result_avg.result_rows[0] and query_result_avg.result_rows[0][0] is not None:
                local_avg_price_per_sqm = query_result_avg.result_rows[0][0]

            if local_avg_price_per_sqm is not None and local_avg_price_per_sqm > 0:
                threshold_price_per_sqm = local_avg_price_per_sqm * (1.0 - below_avg_pct)
                below_threshold_query = f"""
                SELECT * FROM {self.table_name}
                WHERE zip_code = %(postal_code_int)s
                  AND lot_size_sqm IS NOT NULL AND lot_size_sqm > 0
                  AND price IS NOT NULL AND price > 0 
                  AND (price / lot_size_sqm) < %(threshold_price_per_sqm)s
                """
                query_result_bt = client.query(
                    below_threshold_query,
                    parameters={'postal_code_int': postal_code_int, 'threshold_price_per_sqm': threshold_price_per_sqm}
                )
                results["below_threshold"] = self._rows_to_dicts(query_result_bt)
        except Exception as e:
            logger.info(f"Usecase 4 (Part 1 - below_threshold) error: {e}")

        # Part 2: Sort all properties in city by cheapest price
        # Ensure to filter for valid prices for meaningful sorting
        sorted_by_city_query = f"""
        SELECT * FROM {self.table_name} 
        WHERE city = %(city)s AND price IS NOT NULL AND price > 0
        ORDER BY price ASC
        """
        try:
            query_result_sc = client.query(sorted_by_city_query, parameters={'city': city})
            results["sorted_by_city"] = self._rows_to_dicts(query_result_sc)
        except Exception as e:
            logger.info(f"Usecase 4 (Part 2 - sorted_by_city) error: {e}")
        return results

    def usecase5_average_price_per_city(self) -> Dict[str, float]:
        client = self._get_client()
        # Aligned to use lot_size_sqm
        query = f"""
        SELECT city, state, avgIf(price / lot_size_sqm, lot_size_sqm > 0 AND price > 0) AS avg_price_per_sqm
        FROM {self.table_name} GROUP BY city, state
        """
        try:
            query_result = client.query(query)
            city_avg_prices = {}
            city_idx = query_result.column_names.index('city')
            state_idx = query_result.column_names.index('state')  # Get state index
            avg_price_idx = query_result.column_names.index('avg_price_per_sqm')

            for row in query_result.result_rows:
                city_name = row[city_idx]
                state_name = row[state_idx]
                avg_price = row[avg_price_idx]
                if avg_price is not None:
                    city_avg_prices[f"{city_name}, {state_name}"] = float(avg_price)
            return city_avg_prices
        except Exception as e:
            logger.info(f"Usecase 5 error: {e}")
            raise

    def usecase6_filter_by_bedrooms_and_size(
        self,
        min_bedrooms: int = DEFAULT_MIN_BEDROOMS,
        max_size: float = DEFAULT_MAX_SIZE_SQM
    ) -> List[Dict[str, Any]]:
        client = self._get_client()
        query = f"""
        SELECT * FROM {self.table_name}
        WHERE bed > %(min_bedrooms)s AND house_size_sqm < %(max_size)s
        """
        try:
            query_result = client.query(query, parameters={'min_bedrooms': min_bedrooms, 'max_size': max_size})
            return self._rows_to_dicts(query_result)
        except Exception as e:
            logger.info(f"Usecase 6 error: {e}")
            raise

    def usecase7_bulk_import(
            self,
            data: Iterable[ListingRecord] = read_listings(DEFAULT_DATA_FILE_PATH_FOR_IMPORT),  # Replace ListingRecord with actual type
            batch_size: int = DEFAULT_BATCH_SIZE
    ) -> None:
        client = self._get_client()  # Make sure this method is defined in your class
        column_names = [
            'id', 'brokered_by', 'status', 'price', 'lot_size_sqm', 'street',
            'city', 'state', 'zip_code', 'bed', 'bath', 'house_size_sqm',
            'prev_sold_date'
        ]

        batch: List[tuple] = []
        processed_count = 0
        # MIN_YEAR and MAX_YEAR define the valid range for prev_sold_date
        # Your data conversion script now filters out year 1970 and earlier.
        # So, MIN_YEAR here should reflect the earliest valid year (e.g., 1971).
        MIN_ACCEPTABLE_YEAR = 1970
        MAX_ACCEPTABLE_YEAR = 2105  # Max supported by ClickHouse DateTime is around 2106

        try:
            for record in data:
                record_id = uuid.uuid4()

                prev_sold_attr_val = getattr(record, 'prev_sold_date', None)
                current_prev_sold_to_insert: Optional[datetime] = None

                if isinstance(prev_sold_attr_val, datetime):
                    temp_dt = prev_sold_attr_val
                    # Ensure it's naive UTC if it's timezone-aware
                    if temp_dt.tzinfo is not None and temp_dt.tzinfo.utcoffset(temp_dt) is not None:
                        temp_dt = temp_dt.astimezone(timezone.utc).replace(tzinfo=None)

                    # Apply year clamping based on your data rules (post-1970 as per last fix)
                    if MIN_ACCEPTABLE_YEAR <= temp_dt.year <= MAX_ACCEPTABLE_YEAR:
                        current_prev_sold_to_insert = temp_dt
                    # else: it remains None, effectively filtering out dates outside the valid range
                # If prev_sold_attr_val is None or not a datetime, current_prev_sold_to_insert remains None

                record_tuple = (
                    record_id,
                    getattr(record, 'brokered_by', None), getattr(record, 'status', None),
                    getattr(record, 'price', None), getattr(record, 'lot_size_sqm', None),
                    getattr(record, 'street', None), getattr(record, 'city', None),
                    getattr(record, 'state', None), getattr(record, 'zip_code', None),
                    getattr(record, 'bed', None), getattr(record, 'bath', None),
                    getattr(record, 'house_size_sqm', None), current_prev_sold_to_insert
                )
                batch.append(record_tuple)

                if len(batch) >= batch_size:
                    try:
                        client.insert(self.table_name, batch, column_names=column_names)
                        processed_count += len(batch)
                        logger.info(f"Inserted batch of {len(batch)} records. Total processed: {processed_count}")
                    except Exception as insert_exc:
                        logger.info(
                            f"ERROR during client.insert for a batch. Processed before this batch: {processed_count}. Batch size: {len(batch)}. Error: {insert_exc}")
                        raise
                    finally:
                        batch = []

            # Insert any remaining records
            if batch:
                try:
                    client.insert(self.table_name, batch, column_names=column_names)
                    processed_count += len(batch)
                    logger.info(f"Inserted final batch of {len(batch)} records. Total processed: {processed_count}")
                except Exception as insert_exc:
                    logger.info(
                        f"ERROR during client.insert for the final batch. Processed before this batch: {processed_count}. Final batch size: {len(batch)}. Error: {insert_exc}")
                    raise
                finally:
                    batch = []

        except Exception as e:
            logger.info(
                f"Usecase 7 (bulk import) failed. Records processed successfully into DB: {processed_count}. Records in current (failed) batch: {len(batch)}. Error: {e}")
            raise



    def close(self) -> None:
        if self._client:
            try:
                self._client.close()
                logger.info("ClickHouse client closed.")
            except Exception as e:
                logger.info(f"Error closing ClickHouse client: {e}")
            finally:
                self._client = None

    def __del__(self) -> None:
        self.close()

if __name__ == "__main__":
        clickhouse = ClickHouseAdapter()
        clickhouse.reset_database()
        clickhouse.usecase7_bulk_import()  # Import data from the default file
        logger.info(f'Usecase 4: {len(clickhouse.usecase4_price_analysis()["below_threshold"])}')
        logger.info(f'Usecase 4: {len(clickhouse.usecase4_price_analysis()["sorted_by_city"])}')
        logger.info(f"Usecase 5: {len(clickhouse.usecase5_average_price_per_city())}")

        logger.info(f"Usecase 1: {len(clickhouse.usecase1_filter_properties())}")
        clickhouse.close()