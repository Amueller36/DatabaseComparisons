import io
import os
from typing import Optional, Dict, Any, List, Iterable

import pandas as pd
from logging import getLogger
import logging
import psycopg2  # Changed from sqlalchemy
import psycopg2.extras  # For potential future use, e.g., extras.execute_values

from ListingRecord import ListingRecord
from usecases import Usecases

# Configuration
# DATABASE_URL = "postgresql+psycopg2://mds:mds@152.53.248.27:5432/postgres" # Old SQLAlchemy URL
DB_DSN = "postgresql://mds:mds@152.53.248.27:5432/postgres"  # Standard DSN for psycopg2
logger = logging.getLogger(__name__)
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Columns order for staging table
STAGING_COLS = [
    'idx',
    'brokered_by',
    'status',
    'price',
    'bed',
    'bath',
    'lot_size_sqm',
    'street',
    'city',
    'state',
    'zip_code',
    'house_size_sqm',
    'prev_sold_date'
]

# Type casting map to strip ".0" from integer-like floats
CAST_MAP = {
    'idx': 'int64',
    'brokered_by': 'Int64',  # Pandas nullable integer type
    'bed': 'Int64',
    'bath': 'Int64',
    'zip_code': 'Int64'
}


def fast_bulk_insert(df: pd.DataFrame):
    """
    Fast bulk insert using a temporary staging table and COPY FROM STDIN in CSV mode,
    with progress logging, using psycopg2 directly.
    """
    # Rename incoming columns if needed
    df = df.rename(columns={
        'acre_lot': 'lot_size_sqm',
        'house_size': 'house_size_sqm'
    })

    total_rows = len(df)
    logging.info(f"Starting bulk insert for {total_rows} rows using psycopg2.")

    conn = None  # Initialize conn to None for error handling
    try:
        with psycopg2.connect(DB_DSN) as conn:  # Connection context manager
            with conn.cursor() as cur:  # Cursor context manager
                # 1) Create temp staging table
                logging.info("Step 1/5: Creating temporary staging table...")
                cur.execute("""
                            CREATE
                            TEMP TABLE staging (
                        idx              BIGINT,
                        brokered_by      INT,
                        status           TEXT,
                        price            NUMERIC,
                        bed              INT,
                        bath             INT,
                        lot_size_sqm     DOUBLE PRECISION,
                        street           TEXT,
                        city             TEXT,
                        state            TEXT,
                        zip_code         INT,
                        house_size_sqm   DOUBLE PRECISION,
                        prev_sold_date   DATE
                    )
                            """)
                logging.info("Temporary staging table created.")

                # 2) Prepare DataFrame for COPY
                logging.info("Step 2/5: Preparing DataFrame and copying to staging table...")

                # Assuming df.index is suitable for 'idx' (e.g., a unique integer index)
                # If df.index is not 'idx', you might need df.reset_index() and use the new 'index' column
                temp_df = (
                    df
                    .assign(idx=df.index)
                    [STAGING_COLS]
                    .astype(CAST_MAP)
                )

                buf = io.StringIO()
                # na_rep='' ensures that pandas NA/None values become empty strings in CSV
                temp_df.to_csv(buf, header=False, index=False, na_rep='')
                buf.seek(0)

                # Use copy_expert for complex COPY statements (or copy_from for simpler ones)
                cur.copy_expert(
                    sql=f"""
                    COPY staging ({', '.join(STAGING_COLS)})
                    FROM STDIN WITH (
                        FORMAT CSV,
                        HEADER FALSE,
                        DELIMITER ',',
                        QUOTE '"',
                        NULL ''  -- Interprets empty strings (from na_rep='') as SQL NULL
                    )
                    """,
                    file=buf
                )
                logging.info(f"Copied {len(temp_df)} rows into staging table.")

                # 3) Deduplicate & insert zip_codes and brokers
                logging.info("Step 3/5: Inserting distinct zip_codes and brokers...")
                cur.execute("""
                            INSERT INTO zip_codes (zip_code, city, state)
                            SELECT DISTINCT zip_code, city, state
                            FROM staging
                            WHERE zip_code IS NOT NULL -- Avoid inserting NULL zip codes if any
                                ON CONFLICT DO NOTHING;
                            """)
                cur.execute("""
                            INSERT INTO brokers (brokered_by)
                            SELECT DISTINCT brokered_by
                            FROM staging
                            WHERE brokered_by IS NOT NULL -- Avoid inserting NULL brokers
                                ON CONFLICT DO NOTHING;
                            """)
                logging.info("Inserted zip_codes and brokers.")

                # 4) Bulk insert listings and capture listing_id + idx (Revised for robustness)
                logging.info("Step 4/5: Inserting listings and capturing new IDs...")
                cur.execute("DROP TABLE IF EXISTS new_listings;")  # Clean for re-runs in same session
                cur.execute("""
                            CREATE
                            TEMP TABLE new_listings AS
                    WITH staging_ordered AS (
                        -- Assign a row number to staging rows based on idx for stable ordering
                        SELECT idx, brokered_by, status, price, prev_sold_date, 
                               ROW_NUMBER() OVER (ORDER BY idx) as rn
                        FROM staging
                    ),
                    inserted_data AS (
                        -- Insert into listings, ensuring order by 'rn' from staging_ordered
                        -- RETURNING clause will then output listing_ids in this same order
                        INSERT INTO listings (brokered_by, status, price, prev_sold_date)
                        SELECT brokered_by, 
                               status::listing_status_enum, -- Assuming listing_status_enum is a custom ENUM type
                               price, 
                               prev_sold_date
                        FROM staging_ordered
                        ORDER BY rn
                        RETURNING listing_id 
                    ),
                    inserted_data_with_rn AS (
                        -- Assign a row number to the returned listing_ids
                        -- This rn will correspond to the rn from staging_ordered due to ordered insert and RETURNING behavior
                        SELECT listing_id, ROW_NUMBER() OVER () as rn 
                        FROM inserted_data
                    )
                    -- Join the returned listing_ids (with their new rn) back to the original staging data (with its rn)
                    -- to correctly associate listing_id with the original idx.
                            SELECT id_rn.listing_id, so.idx
                            FROM inserted_data_with_rn id_rn
                                     JOIN staging_ordered so ON id_rn.rn = so.rn;
                            """)
                logging.info("Inserted listings and created new_listings temp table.")

                # 5) Insert dependent tables in pure SQL
                logging.info("Step 5/5: Inserting dependent tables (estate_details, land_data, addresses)...")
                cur.execute("""
                            INSERT INTO estate_details (listing_id, bed, bath, house_size)
                            SELECT nl.listing_id,
                                   s.bed,
                                   s.bath,
                                   s.house_size_sqm
                            FROM staging s
                                     JOIN new_listings nl ON s.idx = nl.idx
                            WHERE s.bed IS NOT NULL
                              AND s.bath IS NOT NULL
                              AND s.house_size_sqm IS NOT NULL;
                            """)

                cur.execute("""
                            INSERT INTO land_data (listing_id, area_size_in_square_m)
                            SELECT nl.listing_id,
                                   s.lot_size_sqm
                            FROM staging s
                                     JOIN new_listings nl ON s.idx = nl.idx
                            WHERE s.lot_size_sqm IS NOT NULL;
                            """)

                cur.execute("""
                            INSERT INTO addresses (listing_id, street, zip_code)
                            SELECT nl.listing_id,
                                   s.street,
                                   s.zip_code
                            FROM staging s
                                     JOIN new_listings nl ON s.idx = nl.idx
                            WHERE s.street IS NOT NULL
                              AND s.zip_code IS NOT NULL; -- Added basic NOT NULL checks
                            """)
                logging.info("Dependent table inserts complete.")

            conn.commit()  # Commit the transaction
            logging.info("Bulk insert transaction committed successfully.")

    except psycopg2.Error as e:
        logging.error(f"Database transaction failed: {e.SQLSTATE if e.pgcode else 'N/A'} - {e.pgerror}")
        if conn:
            conn.rollback()  # Rollback on error
        raise  # Re-raise the exception to inform the caller
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        if conn:  # conn might not be established if error is before psycopg2.connect
            conn.rollback()
        raise
    # `finally: if conn: conn.close()` is handled by `with psycopg2.connect(...) as conn:`


class PostgresUsecases(Usecases):
    def __init__(self, db_params: Optional[Dict[str, Any]] = None):
        if db_params is None:
            self.db_params = {
                "host": os.getenv("PG_HOST", "152.53.248.27"),
                "port": os.getenv("PG_PORT", "5432"),
                "database": os.getenv("PG_DATABASE", "postgres"),
                "user": os.getenv("PG_USER", "mds"),
                "password": os.getenv("PG_PASSWORD", "mds")
            }
        else:
            self.db_params = db_params
        self.conn = None
        self._connect()

    def _connect(self):
        try:
            self.conn = psycopg2.connect(**self.db_params)
            logger.info("Successfully connected to PostgreSQL database.")
        except psycopg2.Error as e:
            logger.error(f"Error connecting to PostgreSQL database: {e}")
            raise

    def _execute_query(self, query: str, params: Optional[tuple] = None,
                       commit: bool = False, fetch_one: bool = False,
                       fetch_all: bool = False, row_count: bool = False) -> Any:
        if not self.conn or self.conn.closed:
            logger.warning("Database connection closed. Reconnecting.")
            self._connect()

        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params)

                if commit:
                    self.conn.commit()
                    if row_count:
                        return cur.rowcount
                    return None  # DML statements like UPDATE/INSERT/DELETE often don't need to return rows

                if fetch_one:
                    return cur.fetchone()
                if fetch_all:
                    colnames = [desc[0] for desc in cur.description]
                    return [dict(zip(colnames, row)) for row in cur.fetchall()]
                if row_count:  # For SELECT queries where we only need the count of results from a previous DML
                    return cur.rowcount
                return None  # Default if no fetch/commit action specified explicitly for a query that might not return
        except psycopg2.Error as e:
            logger.error(f"Database query error: {e}\nQuery: {query}\nParams: {params}")
            if self.conn and not self.conn.closed:  # Check if conn is not None
                self.conn.rollback()  # Rollback on error
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during query execution: {e}")
            if self.conn and not self.conn.closed:
                self.conn.rollback()
            raise

    def _get_full_listing_details_sql_select_clause(self) -> str:
        # Check if solar_panels column exists in listings
        # This is a bit of a workaround. Ideally, schema migrations handle this.
        # For this exercise, we'll query information_schema.
        # This check adds overhead, so in production, you'd know if the column exists.
        solar_panels_select = "NULL AS solar_panels"  # Default if column doesn't exist
        try:
            # This check should ideally be done once at init or managed by migration status
            # For simplicity, we are not caching this check here.
            res = self._execute_query(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'listings'
                  AND column_name = 'solar_panels';
                """, fetch_one=True
            )
            if res:
                solar_panels_select = "l.solar_panels"
        except Exception:  # Broad exception if query fails (e.g. during setup before table exists)
            pass

        return f"""
            l.listing_id, l.brokered_by, l.status::text, l.price, l.prev_sold_date, {solar_panels_select},
            ed.bed, ed.bath, ed.house_size AS house_size_sqm,
            ld.area_size_in_square_m AS lot_size_sqm,
            a.street,
            zc.zip_code, zc.city, zc.state
        """

    def _validate_broker_exists(self, broker_id: int):
        query = "SELECT 1 FROM brokers WHERE brokered_by = %s;"
        if not self._execute_query(query, (broker_id,), fetch_one=True):
            raise ValueError(f"Broker with ID {broker_id} not found.")

    def _validate_zip_code_exists(self, zip_code: int):
        query = "SELECT 1 FROM zip_codes WHERE zip_code = %s;"
        if not self._execute_query(query, (zip_code,), fetch_one=True):
            raise ValueError(f"Zip code {zip_code} not found.")

    def _validate_city_exists(self, city: str):
        query = "SELECT 1 FROM zip_codes WHERE city = %s LIMIT 1;"
        if not self._execute_query(query, (city,), fetch_one=True):
            raise ValueError(f"City '{city}' not found.")

    def usecase1_filter_properties(self, min_listings: int, max_price: float) -> List[Dict[str, Any]]:
        if not isinstance(min_listings, int) or min_listings < 0:
            raise ValueError("min_listings must be a non-negative integer.")
        if not isinstance(max_price, (int, float)) or max_price < 0:
            raise ValueError("max_price must be a non-negative number.")

        select_clause = self._get_full_listing_details_sql_select_clause()
        query = f"""
            SELECT {select_clause}
            FROM listings l
            JOIN addresses a ON l.listing_id = a.listing_id
            JOIN zip_codes zc ON a.zip_code = zc.zip_code
            LEFT JOIN estate_details ed ON l.listing_id = ed.listing_id
            LEFT JOIN land_data ld ON l.listing_id = ld.listing_id
            WHERE l.price < %s
            AND zc.city IN (
                SELECT city_sub.city
                FROM (
                    SELECT z_sub.city, COUNT(l_sub.listing_id) as listing_count
                    FROM listings l_sub
                    JOIN addresses a_sub ON l_sub.listing_id = a_sub.listing_id
                    JOIN zip_codes z_sub ON a_sub.zip_code = z_sub.zip_code
                    GROUP BY z_sub.city
                    HAVING COUNT(l_sub.listing_id) > %s
                ) AS city_sub
            )
            ORDER BY zc.city, l.price;
        """
        return self._execute_query(query, (max_price, min_listings), fetch_all=True)

    def usecase2_update_prices(self, broker_id: int, percent_delta: float, limit: int) -> int:
        if not isinstance(broker_id, int):
            raise ValueError("broker_id must be an integer.")
        if not isinstance(percent_delta, (int, float)):  # e.g. 0.05 for +5%, -0.1 for -10%
            raise ValueError("percent_delta must be a number.")
        if percent_delta <= -1.0:
            raise ValueError("percent_delta cannot be -1.0 or less (price cannot be zero or negative).")
        if not isinstance(limit, int) or limit <= 0:
            raise ValueError("limit must be a positive integer.")

        self._validate_broker_exists(broker_id)

        query = """
                UPDATE listings
                SET price = price * (1 + %s)
                WHERE listing_id IN (SELECT listing_id \
                                     FROM listings \
                                     WHERE brokered_by = %s \
                                     ORDER BY listing_id -- Define "first" (can be price, date, etc.)
                    LIMIT %s
                    ); \
                """
        # For PostgreSQL, UPDATE doesn't return rowcount directly in the same way for execute.
        # We modify the query to use RETURNING and then count. Or rely on cursor.rowcount.
        # cursor.rowcount after an UPDATE is standard.
        return self._execute_query(query, (percent_delta, broker_id, limit), commit=True, row_count=True)

    def usecase3_add_solar_panels(self) -> int:
        try:
            # 1. Add column if not exists to 'listings' table
            self._execute_query(
                "ALTER TABLE listings ADD COLUMN IF NOT EXISTS solar_panels BOOLEAN;",
                commit=True
            )

            # 2. Update with random boolean values
            # We update all rows. The number of affected rows is returned.
            # Set solar_panels to NULL first for existing rows to ensure idempotency of count if re-run for testing.
            # Or better, only update where it's NULL if it's a one-time fill.
            # For "befüllen mit Zufallswerten", we assume all rows are targeted.
            # If the column was just added, all values are NULL.
            updated_count = self._execute_query(
                "UPDATE listings SET solar_panels = (RANDOM() < 0.5);",
                commit=True,
                row_count=True
            )
            logger.info(f"Added/updated 'solar_panels' field for {updated_count} listings.")
            return updated_count
        except psycopg2.Error as e:
            logger.error(f"Error in usecase3_add_solar_panels: {e}")
            # Potentially re-raise or handle more gracefully
            raise

    def usecase4_price_analysis(self, postal_code: int, below_avg_pct: float, city: str) -> Dict[
        str, List[Dict[str, Any]]]:
        if not isinstance(postal_code, int):
            raise ValueError("postal_code must be an integer.")
        if not isinstance(below_avg_pct, (int, float)) or not (0 < below_avg_pct < 1):
            raise ValueError("below_avg_pct must be a float between 0 and 1 (exclusive).")
        if not isinstance(city, str) or not city.strip():
            raise ValueError("city must be a non-empty string.")

        self._validate_zip_code_exists(postal_code)
        self._validate_city_exists(city)

        select_clause = self._get_full_listing_details_sql_select_clause()

        # Part 1: Properties in postal_code below average price/sqm
        query_below_threshold = f"""
            WITH PostalCodeAvg AS (
                SELECT AVG(l.price / NULLIF(ed.house_size, 0)) as avg_price_per_sqm
                FROM listings l
                JOIN estate_details ed ON l.listing_id = ed.listing_id
                JOIN addresses a ON l.listing_id = a.listing_id
                WHERE a.zip_code = %s AND ed.house_size IS NOT NULL AND ed.house_size > 0
            )
            SELECT {select_clause}
            FROM listings l
            JOIN addresses a ON l.listing_id = a.listing_id
            JOIN zip_codes zc ON a.zip_code = zc.zip_code
            LEFT JOIN estate_details ed ON l.listing_id = ed.listing_id
            LEFT JOIN land_data ld ON l.listing_id = ld.listing_id
            CROSS JOIN PostalCodeAvg pca
            WHERE a.zip_code = %s
              AND ed.house_size IS NOT NULL AND ed.house_size > 0 AND pca.avg_price_per_sqm IS NOT NULL
              AND (l.price / ed.house_size) < pca.avg_price_per_sqm * (1 - %s)
            ORDER BY (l.price / ed.house_size);
        """
        below_threshold_properties = self._execute_query(query_below_threshold,
                                                         (postal_code, postal_code, below_avg_pct), fetch_all=True)

        # Part 2: Properties in city sorted by price
        query_sorted_by_city = f"""
            SELECT {select_clause}
            FROM listings l
            JOIN addresses a ON l.listing_id = a.listing_id
            JOIN zip_codes zc ON a.zip_code = zc.zip_code
            LEFT JOIN estate_details ed ON l.listing_id = ed.listing_id
            LEFT JOIN land_data ld ON l.listing_id = ld.listing_id
            WHERE zc.city = %s
            ORDER BY l.price ASC;
        """
        sorted_by_city_properties = self._execute_query(query_sorted_by_city, (city,), fetch_all=True)

        return {
            "below_threshold": below_threshold_properties,
            "sorted_by_city": sorted_by_city_properties
        }

    def usecase5_average_price_per_city(self) -> Dict[str, float]:
        query = """
                SELECT zc.city, \
                       AVG(l.price / NULLIF(ed.house_size, 0)) as avg_price_per_unit_area
                FROM listings l
                         JOIN estate_details ed ON l.listing_id = ed.listing_id
                         JOIN addresses a ON l.listing_id = a.listing_id
                         JOIN zip_codes zc ON a.zip_code = zc.zip_code
                WHERE ed.house_size IS NOT NULL \
                  AND ed.house_size > 0
                  AND l.price IS NOT NULL -- ensure price is also not null
                GROUP BY zc.city
                HAVING AVG(l.price / NULLIF(ed.house_size, 0)) IS NOT NULL; \
                """
        results = self._execute_query(query, fetch_all=True)
        return {row['city']: float(row['avg_price_per_unit_area']) for row in results}

    def usecase6_filter_by_bedrooms_and_size(self, min_bedrooms: int, max_size: float) -> List[Dict[str, Any]]:
        if not isinstance(min_bedrooms, int) or min_bedrooms < 0:
            raise ValueError("min_bedrooms must be a non-negative integer.")
        if not isinstance(max_size, (int, float)) or max_size <= 0:
            raise ValueError("max_size must be a positive number.")

        select_clause = self._get_full_listing_details_sql_select_clause()
        query = f"""
            SELECT {select_clause}
            FROM listings l
            JOIN estate_details ed ON l.listing_id = ed.listing_id -- INNER JOIN as bed and house_size are key
            JOIN addresses a ON l.listing_id = a.listing_id
            JOIN zip_codes zc ON a.zip_code = zc.zip_code
            LEFT JOIN land_data ld ON l.listing_id = ld.listing_id
            WHERE ed.bed > %s
              AND ed.house_size < %s
              AND ed.bed IS NOT NULL       -- Redundant if > min_bedrooms implies not null, but good for clarity
              AND ed.house_size IS NOT NULL; -- Redundant if < max_size implies not null, but good for clarity
        """
        return self._execute_query(query, (min_bedrooms, max_size), fetch_all=True)

    def usecase7_batch_import(self, data: Iterable[ListingRecord], batch_size: int = 1000) -> int:
        if not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer.")

        total_imported_listings = 0
        records_batch = []

        for record in data:
            if not isinstance(record, ListingRecord):
                logger.warning(f"Skipping invalid data item: {record}")
                continue
            records_batch.append(record)
            if len(records_batch) >= batch_size:
                total_imported_listings += self._process_batch(records_batch)
                records_batch = []

        if records_batch:  # Process any remaining records
            total_imported_listings += self._process_batch(records_batch)

        logger.info(f"Batch import completed. Total listings imported: {total_imported_listings}")
        return total_imported_listings

    def _process_batch(self, batch: List[ListingRecord]) -> int:
        imported_in_batch = 0
        try:
            with self.conn.cursor() as cur:
                # Prepare data for batch inserts
                brokers_data = list(set([(int(r.brokered_by),) for r in batch]))
                zip_codes_data = list(set([(r.zip_code, r.city, r.state) for r in batch]))

                # Insert into brokers
                psycopg2.extras.execute_values(cur,
                                               "INSERT INTO brokers (brokered_by) VALUES %s ON CONFLICT (brokered_by) DO NOTHING",
                                               brokers_data, page_size=len(brokers_data)
                                               )

                # Insert into zip_codes
                psycopg2.extras.execute_values(cur,
                                               "INSERT INTO zip_codes (zip_code, city, state) VALUES %s ON CONFLICT (zip_code) DO NOTHING",
                                               zip_codes_data, page_size=len(zip_codes_data)
                                               )

                # For listings and related tables, we need to insert one by one to get listing_id
                # or use more complex CTEs with INSERT ... RETURNING for a true batch effect.
                # For simplicity and clarity with FKs, we'll iterate here within the transaction for the batch.
                for record in batch:
                    try:
                        # Insert into listings and get listing_id
                        cur.execute(
                            """
                            INSERT INTO listings (brokered_by, status, price, prev_sold_date)
                            VALUES (%s, %s::listing_status_enum, %s, %s) RETURNING listing_id;
                            """,
                            (int(record.brokered_by), record.status, record.price, record.prev_sold_date)
                        )
                        listing_id = cur.fetchone()[0]

                        # Insert into estate_details (if data present)
                        if record.bed is not None or record.bath is not None or record.house_size_sqm is not None:
                            cur.execute(
                                """
                                INSERT INTO estate_details (listing_id, bed, bath, house_size)
                                VALUES (%s, %s, %s, %s) ON CONFLICT (listing_id) DO NOTHING;
                                """,  # Added ON CONFLICT for idempotency if re-importing same listing_id details
                                (listing_id, record.bed, record.bath, record.house_size_sqm)
                            )

                        # Insert into land_data
                        cur.execute(
                            """
                            INSERT INTO land_data (listing_id, area_size_in_square_m)
                            VALUES (%s, %s) ON CONFLICT (listing_id) DO NOTHING;
                            """,  # Added ON CONFLICT
                            (listing_id, record.lot_size_sqm)
                        )

                        # Insert into addresses
                        # Ensuring street is handled as string version of its integer part
                        street_val_str = str(int(record.street))
                        cur.execute(
                            """
                            INSERT INTO addresses (listing_id, street, zip_code)
                            VALUES (%s, %s, %s) ON CONFLICT (listing_id) DO NOTHING;
                            """,  # Added ON CONFLICT
                            (listing_id, street_val_str, record.zip_code)
                        )
                        imported_in_batch += 1
                    except psycopg2.IntegrityError as e:  # e.g. status enum mismatch, FK violation not caught by ON CONFLICT
                        logger.error(f"Integrity error inserting record {record}: {e}")
                        self.conn.rollback()  # Rollback this specific record or the batch? Here: rollback batch
                        raise  # Re-raise to signal failure of batch
                    except ValueError as e:  # From ListingRecord validation, though should be caught earlier
                        logger.error(f"Validation error for record {record}: {e}")
                        self.conn.rollback()
                        raise
            self.conn.commit()
            logger.info(f"Successfully processed batch of {len(batch)} records. {imported_in_batch} listings inserted.")
            return imported_in_batch
        except psycopg2.Error as e:
            logger.error(f"Database error during batch processing: {e}")
            if self.conn and not self.conn.closed:
                self.conn.rollback()
            # Decide if to re-raise or return 0, etc.
            # For now, re-raise to indicate batch failure clearly
            raise
        except Exception as e:
            logger.error(f"Unexpected error during batch processing: {e}")
            if self.conn and not self.conn.closed:
                self.conn.rollback()
            raise

    def reset_database(self) -> None:
        # Order is important if not using CASCADE extensively or if specific FKs are RESTRICT
        # TRUNCATE ... RESTART IDENTITY CASCADE is generally robust.
        query = """
                TRUNCATE TABLE
                    brokers,
                zip_codes,
                listings,
                estate_details,
                land_data,
                addresses
            RESTART IDENTITY CASCADE; \
                """
        try:
            self._execute_query(query, commit=True)
            logger.info("Database has been reset (all entries deleted, identities restarted).")
        except psycopg2.Error as e:
            logger.error(f"Failed to reset database: {e}")
            raise

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("PostgreSQL connection closed.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()



if __name__ == "__main__":
    postgres = PostgresUsecases()
    # Example usage
    test = postgres.usecase1_filter_properties(10, 250000)
    print(f"Usecase 1  Found: {len(test)}")