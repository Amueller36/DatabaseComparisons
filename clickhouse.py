import uuid
from copy import copy
from datetime import datetime, timezone
from typing import Iterable, List, Dict, Any, Optional

import clickhouse_connect

from ListingRecord import ListingRecord, read_listings
from usecases import Usecases


class ClickHouseAdapter(Usecases):
    def __init__(self, host: str = 'localhost', port: int = 8123, database: str = 'default', user: str = 'mds',
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
            print(f"Successfully connected to ClickHouse server {self.host}:{self.port}, database '{self.database}'")
        except Exception as e:
            print(f"Failed to connect to ClickHouse ({self.host}:{self.port}, db: {self.database}): {e}")
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
            print("ClickHouse client is not available or not responding, attempting to reconnect...")
            self._connect()  # This will raise ConnectionError if it fails

        if self._client is None:  # Should not happen if _connect succeeds or raises
            raise ConnectionError("ClickHouse client is None after connection attempt.")
        return self._client  # type: ignore

    def _create_table_if_not_exists(self) -> None:
        client = self._get_client()
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.fq_table_name} (
            id UUID,
            brokered_by Float64,
            status String,
            price Float64,
            lot_size_sqm Float64,
            street Float64,
            city String,
            state String,
            zip_code Int32,
            bed Nullable(Int16),
            bath Nullable(Int16),
            house_size_sqm Nullable(Float64),
            prev_sold_date Nullable(DateTime),
            solar_panels Nullable(UInt8) DEFAULT NULL -- Add solar_panels here for Usecase 3
        ) ENGINE = MergeTree()
        ORDER BY (city, zip_code, id)
        """
        try:
            client.command(create_table_query)
            print(f"Table {self.fq_table_name} ensured to exist.")
        except Exception as e:
            print(f"Failed to create table {self.fq_table_name}: {e}")
            raise

    def _rows_to_dicts(self, query_result: clickhouse_connect.driver.summary.QueryResult) -> List[Dict[str, Any]]:
        column_names = query_result.column_names
        return [dict(zip(column_names, row)) for row in query_result.result_rows]

    def reset_database(self) -> None:
        client = self._get_client()
        try:
            client.command(f'TRUNCATE TABLE IF EXISTS {self.fq_table_name}')
            print(f"Table {self.fq_table_name} truncated.")
        except Exception as e:
            print(f"Failed to reset database (truncate table {self.fq_table_name}): {e}")
            raise

    def usecase1_filter_properties(self, min_listings: int, max_price: float) -> List[Dict[str, Any]]:
        client = self._get_client()
        query = f"""
        SELECT *
        FROM {self.table_name}
        WHERE city IN (
            SELECT city
            FROM {self.table_name}
            GROUP BY city
            HAVING count(*) > %(min_listings)s
        )
        AND price < %(max_price)s
        """
        try:

            result = client.query_df(query, parameters={'min_listings': min_listings, 'max_price': max_price})
            return result
        except Exception as e:
            print(f"Usecase 1 error: {e}")
            raise

    def usecase2_update_prices(self, broker_id: str, percent_delta: float, limit: int) -> int:
        client = self._get_client()
        if limit <= 0:
            return 0

        try:
            broker_id_float = float(broker_id)
        except ValueError:
            print(f"Invalid broker_id format: '{broker_id}'. Must be convertible to float.")
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
            print(f"Usecase 2 error: {e}")
            raise

    def usecase3_add_solar_panels(self) -> int:
        client = self._get_client()
        # Using modulo function for robust parsing, as fixed previously
        update_column_query = f"""
        ALTER TABLE {self.fq_table_name}
        UPDATE solar_panels = modulo(rand(), 2)
        WHERE 1 = 1 
        """
        try:
            client.command(update_column_query, settings={'mutations_sync': 1})
        except Exception as e:
            print(f"Usecase 3 (update column) error: {e}")
            raise

        count_query = f"SELECT count() FROM {self.table_name}"
        try:
            query_result = client.query(count_query)
            total_rows = 0
            # Extract scalar from result_rows
            if query_result.result_rows and query_result.result_rows[0] and query_result.result_rows[0][0] is not None:
                total_rows = query_result.result_rows[0][0]
            return total_rows
        except Exception as e:
            print(f"Usecase 3 (count) error: {e}")
            raise


    def usecase4_price_analysis(self, postal_code: str, below_avg_pct: float, city: str) -> Dict[str, List[Dict[str, Any]]]:
        client = self._get_client()
        results: Dict[str, List[Dict[str, Any]]] = {"below_threshold": [], "sorted_by_city": []}

        try: postal_code_int = int(postal_code)
        except ValueError:
            print(f"Invalid postal_code format: '{postal_code}'. Must be an integer.")
            return results

        avg_price_query = f"""
        SELECT avgIf(price / house_size_sqm, house_size_sqm > 0)
        FROM {self.table_name} WHERE zip_code = %(postal_code_int)s
        """
        try:
            query_result_avg = client.query(avg_price_query, parameters={'postal_code_int': postal_code_int})
            local_avg_price_per_sqm = None
            # Extract scalar from result_rows
            if query_result_avg.result_rows and query_result_avg.result_rows[0] and query_result_avg.result_rows[0][0] is not None:
                local_avg_price_per_sqm = query_result_avg.result_rows[0][0]

            if local_avg_price_per_sqm is not None and local_avg_price_per_sqm > 0:
                threshold_price_per_sqm = local_avg_price_per_sqm * (1.0 - below_avg_pct)
                below_threshold_query = f"""
                SELECT * FROM {self.table_name}
                WHERE zip_code = %(postal_code_int)s
                  AND house_size_sqm IS NOT NULL AND house_size_sqm > 0
                  AND (price / house_size_sqm) < %(threshold_price_per_sqm)s
                """
                query_result_bt = client.query(
                    below_threshold_query,
                    parameters={'postal_code_int': postal_code_int, 'threshold_price_per_sqm': threshold_price_per_sqm}
                )
                results["below_threshold"] = self._rows_to_dicts(query_result_bt)
        except Exception as e:
            print(f"Usecase 4 (Part 1 - below_threshold) error: {e}")

        sorted_by_city_query = f"""
        SELECT * FROM {self.table_name} WHERE city = %(city)s ORDER BY price ASC
        """
        try:
            query_result_sc = client.query(sorted_by_city_query, parameters={'city': city})
            results["sorted_by_city"] = self._rows_to_dicts(query_result_sc)
        except Exception as e:
            print(f"Usecase 4 (Part 2 - sorted_by_city) error: {e}")
        return results

    def usecase5_average_price_per_city(self) -> Dict[str, float]:
        client = self._get_client()
        query = f"""
        SELECT city, avgIf(price / house_size_sqm, house_size_sqm > 0) AS avg_price_per_sqm
        FROM {self.table_name} GROUP BY city
        """
        try:
            query_result = client.query(query)
            city_avg_prices = {}
            # Assuming column order is 'city', 'avg_price_per_sqm' based on query
            # For more robustness, could use column_names.index('city') etc.
            # but direct indexing is fine if query is stable.
            city_idx = query_result.column_names.index('city')
            avg_price_idx = query_result.column_names.index('avg_price_per_sqm')

            for row in query_result.result_rows:
                city_name = row[city_idx]
                avg_price = row[avg_price_idx]
                if avg_price is not None: # avgIf can result in None
                    city_avg_prices[city_name] = float(avg_price)
            return city_avg_prices
        except Exception as e:
            print(f"Usecase 5 error: {e}")
            raise

    def usecase6_filter_by_bedrooms_and_size(self, min_bedrooms: int, max_size: float) -> List[Dict[str, Any]]:
        client = self._get_client()
        query = f"""
        SELECT * FROM {self.table_name}
        WHERE bed > %(min_bedrooms)s AND house_size_sqm < %(max_size)s
        """
        try:
            query_result = client.query(query, parameters={'min_bedrooms': min_bedrooms, 'max_size': max_size})
            return self._rows_to_dicts(query_result)
        except Exception as e:
            print(f"Usecase 6 error: {e}")
            raise

    def usecase7_bulk_import(self, data: Iterable[ListingRecord], batch_size: int = 1000) -> None:
        client = self._get_client()
        column_names = [
            'id', 'brokered_by', 'status', 'price', 'lot_size_sqm', 'street',
            'city', 'state', 'zip_code', 'bed', 'bath', 'house_size_sqm',
            'prev_sold_date', 'solar_panels'  # solar_panels is part of the table now
        ]

        batch = []
        processed_count = 0
        try:
            for record in data:
                record_id = uuid.uuid4()
                # solar_panels will be NULL by default on new insert unless provided
                # The use case doesn't specify setting solar_panels during import.
                # Use astuple if ListingRecord matches order. Here, manual for clarity with added id.
                MIN_TIMESTAMP = 0
                # Max epoch for DateTime (approx year 2106), Python's timestamp might have its own limits earlier.
                # Max for ClickHouse DateTime is often 2106-02-07 06:28:15 UTC (4294967295)
                # Let's use a slightly more conservative upper bound for safety if system time_t is smaller.
                # Python datetime.MAXYEAR is 9999. timestamp() fails for dates far in future.
                # A common practical limit might be around year 2038 (signed 32-bit time_t) or 2100.
                # We'll primarily rely on catching OSError from timestamp() for out-of-range.
                # ClickHouse's DateTime can go up to around 2106.
                MAX_YEAR = 2105
                MIN_YEAR = 1970
                prev_sold = record.prev_sold_date
                if isinstance(prev_sold, datetime):
                    # Ensure datetime is naive or convert to UTC if timezone-aware
                    if prev_sold.tzinfo is not None and prev_sold.tzinfo.utcoffset(prev_sold) is not None:
                        prev_sold = prev_sold.astimezone(timezone.utc)

                    # Truncate to supported range for ClickHouse DateTime (1970-2106)
                    # and to avoid .timestamp() OSError on some systems (like Windows for pre-1970)
                    if prev_sold.year < MIN_YEAR or prev_sold.year > MAX_YEAR:  # MIN_YEAR was set to 1970
                        print(
                            f"Warning: prev_sold_date {prev_sold} at record {processed_count} is out of supported year range [{MIN_YEAR}-{MAX_YEAR}]. Setting to None.")
                        prev_sold = None
                    else:
                        try:
                            # Test timestamp conversion. This is where OSError was occurring.
                            prev_sold.timestamp()
                        except (OSError, ValueError) as e_ts:  # ValueError for e.g. year > 9999
                            print(f"Warning: Timestamp conversion error for prev_sold_date {prev_sold} "
                                  f"at record {processed_count}: {e_ts}. Setting to None.")
                            prev_sold = None
                record_tuple = (
                    record_id, record.brokered_by, record.status, record.price,
                    record.lot_size_sqm, record.street, record.city, record.state,
                    record.zip_code, record.bed, record.bath, record.house_size_sqm,
                    prev_sold, None  # Default solar_panels to None on import
                )
                batch.append(record_tuple)
                processed_count += 1

                if len(batch) >= batch_size:
                    client.insert(self.table_name, batch, column_names=column_names)
                    print(f"Inserted batch of {len(batch)} records. Total processed: {processed_count}")
                    batch = []

            if batch:  # Insert any remaining records
                client.insert(self.table_name, batch, column_names=column_names)
                print(f"Inserted final batch of {len(batch)} records. Total processed: {processed_count}")
        except Exception as e:
            print(f"Usecase 7 (bulk import) error at record count approx {processed_count}: {e}")
            raise

    def close(self) -> None:
        if self._client:
            try:
                self._client.close()
                print("ClickHouse client closed.")
            except Exception as e:
                print(f"Error closing ClickHouse client: {e}")
            finally:
                self._client = None

    def __del__(self) -> None:
        self.close()


if __name__ == '__main__':

    print("Initializing ClickHouseAdapter...")

    try:
        adapter = ClickHouseAdapter(host='152.53.248.27', port=8123, database='default', user='mds', password='mds')
        print("Adapter initialized.")

        print("\nResetting database...")
        adapter.reset_database()


        # data = list(generate_dummy_listings(100))
        data = read_listings("transformed_real_estate_data.csv")
        data2 = read_listings("transformed_real_estate_data.csv")
        print(len(list(data2)), "records found.")
        adapter.usecase7_bulk_import(data, batch_size=20000)

        print("Bulk import complete.")

        print("\nRunning Usecase 1: Filter Properties (min_listings=10, max_price=250000)...")
        uc1_results = adapter.usecase1_filter_properties(min_listings=10, max_price=250000.0)
        print(f"Usecase 1 found {len(uc1_results)} properties. First few: {uc1_results[:2]}")

        print("\nRunning Usecase 3: Add Solar Panels...")
        uc3_count = adapter.usecase3_add_solar_panels()
        print(f"Usecase 3 processed {uc3_count} documents.")
        # Verify by fetching a record
        if uc3_count > 0:
            sample_solar = adapter._get_client().query(
                f"SELECT id, solar_panels FROM {adapter.table_name} LIMIT 1")
            print(f"Sample solar panel data: {sample_solar.result_rows}")

        print("\nRunning Usecase 2: Update Prices (broker_id='10.0', delta=0.1, limit=5)...")
        # brokered_by is float, so broker_id should be string representation of a float
        uc2_updated_count = adapter.usecase2_update_prices(broker_id="10.0", percent_delta=0.10, limit=5)
        print(f"Usecase 2 updated {uc2_updated_count} documents for broker '10.0'.")

        print("\nRunning Usecase 4: Price Analysis (postal_code='90210', below_avg_pct=0.1, city='City_0')...")
        uc4_results = adapter.usecase4_price_analysis(postal_code="90210", below_avg_pct=0.1, city="City_0")
        print(f"Usecase 4 'below_threshold' count: {len(uc4_results['below_threshold'])}")
        print(f"Usecase 4 'sorted_by_city' count for City_0: {len(uc4_results['sorted_by_city'])}")
        if uc4_results['sorted_by_city']:
            print(f"Cheapest in City_0 price: {uc4_results['sorted_by_city'][0]['price']}")

        print("\nRunning Usecase 5: Average Price Per City...")
        uc5_results = adapter.usecase5_average_price_per_city()
        print(f"Usecase 5 average prices: {uc5_results}")

        print("\nRunning Usecase 6: Filter by Bedrooms and Size (min_bedrooms=2, max_size=150.0)...")
        uc6_results = adapter.usecase6_filter_by_bedrooms_and_size(min_bedrooms=2, max_size=150.0)
        print(f"Usecase 6 found {len(uc6_results)} properties. First few: {uc6_results[:2]}")

        adapter.close()
        print("\nAdapter closed. Demo finished.")

    except ConnectionError as ce:
        print(f"Could not connect to ClickHouse. Please ensure it is running and accessible. Details: {ce}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback

        traceback.print_exc()
