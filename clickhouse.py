import uuid
from copy import copy
from datetime import datetime, timezone
from typing import Iterable, List, Dict, Any, Optional, override

import clickhouse_connect

from ListingRecord import ListingRecord, read_listings
from usecases import Usecases, DEFAULT_MIN_LISTINGS, DEFAULT_MAX_PRICE, DEFAULT_BROKER_ID, DEFAULT_PERCENT_DELTA, \
    DEFAULT_LIMIT, DEFAULT_POSTAL_CODE, DEFAULT_BELOW_AVG_PCT, DEFAULT_CITY, DEFAULT_MIN_BEDROOMS, DEFAULT_MAX_SIZE_SQM, \
    DEFAULT_DATA_FILE_PATH_FOR_IMPORT, DEFAULT_BATCH_SIZE


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

    @override
    def usecase1_filter_properties(self, min_listings: int = DEFAULT_MIN_LISTINGS, max_price: float= DEFAULT_MAX_PRICE) -> List[Dict[str, Any]]:
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

        # 1) Add the new column if it doesn’t already exist
        add_column_query = f"""
        ALTER TABLE {self.fq_table_name}
        ADD COLUMN IF NOT EXISTS solar_panels Nullable(UInt8) DEFAULT NULL
        """
        try:
            client.command(add_column_query, settings={'mutations_sync': 1})
        except Exception as e:
            print(f"Usecase 3 (add column) error: {e}")
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
            print(f"Usecase 3 (update column) error: {e}")
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
            print(f"Usecase 3 (count) error: {e}")
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
            print(f"Usecase 6 error: {e}")
            raise

    def usecase7_bulk_import(
            self,
            data: Iterable[ListingRecord],  # Replace ListingRecord with actual type if different
            batch_size: int = DEFAULT_BATCH_SIZE
    ) -> None:
        client = self._get_client()
        column_names = [
            'id', 'brokered_by', 'status', 'price', 'lot_size_sqm', 'street',
            'city', 'state', 'zip_code', 'bed', 'bath', 'house_size_sqm',
            'prev_sold_date'
        ]
        try:
            prev_sold_date_idx = column_names.index('prev_sold_date')
        except ValueError:
            print("Critical Error: 'prev_sold_date' not in column_names. Aborting.")  # Console
            raise

        batch: List[tuple] = []
        processed_count = 0

        # --- Debugging Configuration ---
        debug_start_record_overall = 560000
        debug_end_record_overall = 620000
        # debug_sleep_duration = 0.0 # Set to 0 if writing to file, or keep for console progress
        debug_log_file_path = "prev_sold_date_debug_log.txt"  # Log file name
        # --- End Debugging Configuration ---

        # Open the debug log file. 'w' for overwrite each run, 'a' to append.
        # Using a try/finally to ensure it's closed even if errors occur mid-function.
        debug_file = None
        if debug_log_file_path:  # Only open if a path is provided
            try:
                # Ensure the directory exists if the path includes one (e.g., "logs/debug.txt")
                # For simplicity here, assuming it's in the current directory or path is simple.
                # os.makedirs(os.path.dirname(debug_log_file_path), exist_ok=True) # If needed
                debug_file = open(debug_log_file_path, 'w', encoding='utf-8')
                print(
                    f"INFO: Detailed prev_sold_date debug logging will be written to: {debug_log_file_path}")  # Console
            except IOError as e:
                print(
                    f"WARNING: Could not open debug log file {debug_log_file_path}: {e}. Debug logs will go to console.")  # Console
                debug_file = None  # Fallback to console if file open fails

        try:
            for record_idx, record in enumerate(data):
                current_record_global_approx = record_idx + 1
                record_id = uuid.uuid4()
                MIN_YEAR = 1970
                MAX_YEAR = 2105

                prev_sold_attr_val = getattr(record, 'prev_sold_date', None)
                current_prev_sold_to_insert: Optional[datetime] = None

                if isinstance(prev_sold_attr_val, datetime):
                    current_prev_sold_to_insert = prev_sold_attr_val
                    if current_prev_sold_to_insert.tzinfo is not None and \
                            current_prev_sold_to_insert.tzinfo.utcoffset(current_prev_sold_to_insert) is not None:
                        current_prev_sold_to_insert = current_prev_sold_to_insert.astimezone(timezone.utc).replace(
                            tzinfo=None)
                    if current_prev_sold_to_insert.year < MIN_YEAR or current_prev_sold_to_insert.year > MAX_YEAR:
                        current_prev_sold_to_insert = None
                elif prev_sold_attr_val is not None:
                    current_prev_sold_to_insert = None
                else:
                    current_prev_sold_to_insert = None

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
                    actual_processed_this_loop = processed_count + len(batch)
                    batch_start_record = processed_count + 1
                    batch_end_record = actual_processed_this_loop

                    perform_debug_logging = False
                    if max(batch_start_record, debug_start_record_overall) <= min(batch_end_record,
                                                                                  debug_end_record_overall):
                        perform_debug_logging = True

                    if perform_debug_logging:
                        log_target = debug_file if debug_file else print  # Use file if open, else print
                        header_msg = f"\nDEBUG: Batch Records: {batch_start_record}-{batch_end_record}. Overall (approx): {actual_processed_this_loop}\n"
                        if debug_file:
                            debug_file.write(header_msg)
                        else:
                            print(header_msg)  # Console for this general batch header

                        for r_idx, r_tuple in enumerate(batch):
                            psd_val_in_tuple = r_tuple[prev_sold_date_idx]
                            global_record_num = batch_start_record + r_idx
                            log_line_parts = [
                                f"  Global Record ~{global_record_num} (Batch Idx {r_idx}): prev_sold_date='{psd_val_in_tuple}', type={type(psd_val_in_tuple)}"
                            ]
                            if isinstance(psd_val_in_tuple, datetime) and psd_val_in_tuple is not None:
                                try:
                                    ts = psd_val_in_tuple.timestamp()
                                    log_line_parts.append(
                                        f"    timestamp: {ts}, int(timestamp): {int(ts)}{' <<< NEGATIVE!' if ts < 0 else ''}")
                                except Exception as ts_e:
                                    log_line_parts.append(f"    Error getting timestamp for {psd_val_in_tuple}: {ts_e}")
                            elif psd_val_in_tuple is not None:
                                log_line_parts.append(
                                    f"    WARNING: prev_sold_date is not None and not datetime: {psd_val_in_tuple} (type: {type(psd_val_in_tuple)})")

                            if debug_file:
                                debug_file.write("".join(log_line_parts) + "\n")
                            else:
                                print("".join(log_line_parts))  # Console

                        footer_msg = "--- End of Debug Log for this Batch ---\n"
                        if debug_file:
                            debug_file.write(footer_msg); debug_file.flush()  # Ensure it's written immediately
                        else:
                            print(footer_msg)  # Console

                    try:
                        client.insert(self.table_name, batch, column_names=column_names)
                    except Exception as insert_exc:
                        print(
                            f"ERROR during client.insert for batch covering records {batch_start_record}-{batch_end_record}.")  # Console
                        if debug_file:  # Also log error context to file
                            debug_file.write(
                                f"\n!!! ERROR during client.insert for batch records {batch_start_record}-{batch_end_record}. See console for full traceback. !!!\n")
                            debug_file.write("Detailed prev_sold_date values in the failing batch (also on console):\n")

                        print("Detailed prev_sold_date values in the failing batch:")  # Console
                        for r_idx, r_tuple in enumerate(batch):
                            psd_val_in_tuple = r_tuple[prev_sold_date_idx];
                            global_record_num = batch_start_record + r_idx
                            log_line = f"  FAILING BATCH - Global Record ~{global_record_num}: prev_sold_date='{psd_val_in_tuple}', type={type(psd_val_in_tuple)}"
                            if isinstance(psd_val_in_tuple, datetime) and psd_val_in_tuple is not None:
                                try:
                                    ts = psd_val_in_tuple.timestamp(); log_line += f" (ts: {ts}, int(ts): {int(ts)})"
                                except Exception:
                                    log_line += " (timestamp error)"
                            print(log_line)  # Console
                            if debug_file: debug_file.write(log_line + "\n")
                        if debug_file: debug_file.flush()
                        raise insert_exc

                    processed_count += len(batch)
                    print(f"Inserted batch of {len(batch)} records. Total processed: {processed_count}")  # Console
                    batch = []

            # Insert any remaining records (final batch)
            if batch:
                actual_processed_this_loop = processed_count + len(batch)
                batch_start_record = processed_count + 1
                batch_end_record = actual_processed_this_loop

                perform_debug_logging = False
                if max(batch_start_record, debug_start_record_overall) <= min(batch_end_record,
                                                                              debug_end_record_overall):
                    perform_debug_logging = True

                if perform_debug_logging:
                    header_msg = f"\nDEBUG: FINAL Batch Records: {batch_start_record}-{batch_end_record}. Overall (approx): {actual_processed_this_loop}\n"
                    if debug_file:
                        debug_file.write(header_msg)
                    else:
                        print(header_msg)

                    for r_idx, r_tuple in enumerate(batch):
                        psd_val_in_tuple = r_tuple[prev_sold_date_idx];
                        global_record_num = batch_start_record + r_idx
                        log_line_parts = [
                            f"  FINAL Global Record ~{global_record_num} (Batch Idx {r_idx}): prev_sold_date='{psd_val_in_tuple}', type={type(psd_val_in_tuple)}"
                        ]
                        if isinstance(psd_val_in_tuple, datetime) and psd_val_in_tuple is not None:
                            try:
                                ts = psd_val_in_tuple.timestamp(); log_line_parts.append(
                                    f"    timestamp: {ts}, int(timestamp): {int(ts)}{' <<< NEGATIVE!' if ts < 0 else ''}")
                            except Exception as ts_e:
                                log_line_parts.append(f"    Error getting timestamp for {psd_val_in_tuple}: {ts_e}")
                        elif psd_val_in_tuple is not None:
                            log_line_parts.append(
                                f"    WARNING: prev_sold_date is not None and not datetime: {psd_val_in_tuple} (type: {type(psd_val_in_tuple)})")

                        if debug_file:
                            debug_file.write("".join(log_line_parts) + "\n")
                        else:
                            print("".join(log_line_parts))

                    footer_msg = "--- End of Debug Log for FINAL Batch ---\n"
                    if debug_file:
                        debug_file.write(footer_msg); debug_file.flush()
                    else:
                        print(footer_msg)

                try:
                    client.insert(self.table_name, batch, column_names=column_names)
                except Exception as insert_exc:
                    print(
                        f"ERROR during client.insert for FINAL batch covering records {batch_start_record}-{batch_end_record}.")  # Console
                    if debug_file:
                        debug_file.write(
                            f"\n!!! ERROR during client.insert for FINAL batch records {batch_start_record}-{batch_end_record}. See console for full traceback. !!!\n")
                        debug_file.write(
                            "Detailed prev_sold_date values in the failing FINAL batch (also on console):\n")

                    print("Detailed prev_sold_date values in the failing FINAL batch:")  # Console
                    for r_idx, r_tuple in enumerate(batch):
                        psd_val_in_tuple = r_tuple[prev_sold_date_idx];
                        global_record_num = batch_start_record + r_idx
                        log_line = f"  FAILING FINAL BATCH - Global Record ~{global_record_num}: prev_sold_date='{psd_val_in_tuple}', type={type(psd_val_in_tuple)}"
                        if isinstance(psd_val_in_tuple, datetime) and psd_val_in_tuple is not None:
                            try:
                                ts = psd_val_in_tuple.timestamp(); log_line += f" (ts: {ts}, int(ts): {int(ts)})"
                            except Exception:
                                log_line += " (timestamp error)"
                        print(log_line)  # Console
                        if debug_file: debug_file.write(log_line + "\n")
                    if debug_file: debug_file.flush()
                    raise insert_exc

                processed_count += len(batch)
                print(f"Inserted final batch of {len(batch)} records. Total processed: {processed_count}")  # Console

        except Exception as e:
            current_total_records_processed_before_error = processed_count + len(batch)
            print(
                f"Usecase 7 (bulk import) error. Records in DB: {processed_count}. Records in current batch: {len(batch)}. Total before error: {current_total_records_processed_before_error}. Error: {e}")  # Console
            if debug_file:
                debug_file.write(f"\n!!! Usecase 7 (bulk import) error. See console for full traceback. !!!\n")
                debug_file.write(
                    f"Records in DB: {processed_count}. Records in current batch: {len(batch)}. Total before error: {current_total_records_processed_before_error}.\n")
            if batch:
                msg_batch_error = "Data in current batch when error occurred (if any):"
                print(msg_batch_error)  # Console
                if debug_file: debug_file.write(msg_batch_error + "\n")
                batch_start_record_on_error = processed_count + 1
                for r_idx, r_tuple in enumerate(batch):
                    psd_val_in_tuple = r_tuple[prev_sold_date_idx]
                    global_record_num = batch_start_record_on_error + r_idx
                    log_line = f"  Uninserted Batch (Global Record ~{global_record_num}): prev_sold_date='{psd_val_in_tuple}', type={type(psd_val_in_tuple)}"
                    print(log_line)  # Console
                    if debug_file: debug_file.write(log_line + "\n")
                if debug_file: debug_file.flush()
            raise
        finally:
            if debug_file:
                print(f"INFO: Closing debug log file: {debug_log_file_path}")  # Console
                debug_file.close()



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

if __name__ == "__main__":
    adapter = ClickHouseAdapter()
    try:
        adapter.usecase7_bulk_import()

    finally:
        adapter.close()