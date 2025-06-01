import io
import math
import os
from typing import Optional, Dict, Any, List, Iterable

import pandas as pd
import logging
import psycopg2
import psycopg2.extras

from ListingRecord import ListingRecord, read_listings
from usecases import Usecases

# Configuration
DB_DSN = "postgresql://mds:mds@localhost:5432/postgres"  # Standard DSN for psycopg2
logger = logging.getLogger(__name__)
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

CSV_FILE_PATH = "transformed_real_estate_data.csv"


class PostgresAdapter(Usecases):
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
        try:
            self._connect()

            self._create_enum_types_if_not_existent()
            self._create_tables_if_not_existent()
            print("Datenbankschema erfolgreich initialisiert/überprüft.")
        except psycopg2.Error as e:
            print(f"Fehler während der Initialisierung des PostgresDBAdapters: {e}")
            # Hier könnten Sie entscheiden, ob der Fehler weitergereicht oder anders behandelt wird
            if self.conn and not self.conn.closed:
                self.conn.close()  # Verbindung schließen, falls sie noch offen ist
            raise  # Fehler weiterleiten, um auf das Problem aufmerksam zu machen

    def _connect(self):
        try:
            self.conn = psycopg2.connect(**self.db_params)
            logger.info("Successfully connected to PostgreSQL database.")
        except psycopg2.Error as e:
            logger.error(f"Error connecting to PostgreSQL database: {e}")
            raise

    def _execute_command(self, command_string, commit=True):
        """Führt einen einzelnen SQL-Befehl aus."""
        if not self.conn or self.conn.closed:
            print("Verbindung ist nicht aktiv. Versuche erneut zu verbinden.")
            self._connect()  # Stellt sicher, dass eine Verbindung besteht

        try:
            with self.conn.cursor() as cur:
                cur.execute(command_string)
            if commit:
                self.conn.commit()
            # print(f"Befehl erfolgreich ausgeführt: {command_string[:100].strip()}...")
        except psycopg2.Error as e:
            print(f"Fehler beim Ausführen des Befehls: {command_string[:100].strip()}... \nFehlerdetails: {e}")
            if self.conn and not self.conn.closed:  # Nur rollbacken, wenn die Verbindung noch besteht
                try:
                    self.conn.rollback()
                except psycopg2.Error as rb_e:
                    print(f"Fehler beim Rollback: {rb_e}")
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

    def _create_enum_types_if_not_existent(self):
        """Erstellt ENUM-Typen, falls sie noch nicht existieren."""
        enum_creation_sql = """
           DO $$
           BEGIN
               IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'listing_status_enum') THEN
                   CREATE TYPE listing_status_enum AS ENUM (
                       'for_sale',
                       'ready_to_build',
                       'sold'
                   );
                   RAISE NOTICE 'ENUM Typ listing_status_enum erstellt.';
               ELSE
                   RAISE NOTICE 'ENUM Typ listing_status_enum existiert bereits.';
               END IF;
           END$$;
           """
        print("Prüfe/Erstelle ENUM Typ 'listing_status_enum'...")
        self._execute_command(enum_creation_sql)

    def _create_tables_if_not_existent(self):
        """Erstellt die Tabellen, falls sie noch nicht existieren."""
        table_commands = [
            """
            CREATE TABLE IF NOT EXISTS "brokers"
            (
                "brokered_by"
                INT
                PRIMARY
                KEY
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS "zip_codes"
            (
                "zip_code"
                INT
                PRIMARY
                KEY,
                "city"
                VARCHAR
            (
                100
            ),
                "state" VARCHAR
            (
                100
            )
                );
            """,
            """
            CREATE TABLE IF NOT EXISTS "listings"
            (
                "listing_id"
                INT
                GENERATED
                BY
                DEFAULT AS
                IDENTITY
                PRIMARY
                KEY,
                "brokered_by"
                INT
                NOT
                NULL,
                "status"
                listing_status_enum,
                "price"
                NUMERIC
            (
                14,
                2
            ),
                "prev_sold_date" DATE,
                CONSTRAINT "fk_listings_broker"
                FOREIGN KEY
            (
                "brokered_by"
            )
                REFERENCES "brokers"
            (
                "brokered_by"
            )
                ON DELETE RESTRICT
                );
            """,
            """
            CREATE TABLE IF NOT EXISTS "estate_details"
            (
                "listing_id"
                INT
                PRIMARY
                KEY,
                "bed"
                INT,
                "bath"
                INT,
                "house_size"
                NUMERIC
            (
                10,
                2
            ),
                CONSTRAINT "fk_estate_details_listing"
                FOREIGN KEY
            (
                "listing_id"
            )
                REFERENCES "listings"
            (
                "listing_id"
            )
                ON DELETE CASCADE
                );
            """,
            """
            CREATE TABLE IF NOT EXISTS "land_data"
            (
                "listing_id"
                INT
                PRIMARY
                KEY,
                "area_size_in_square_m"
                NUMERIC
            (
                12,
                2
            ),
                CONSTRAINT "fk_land_data_listing"
                FOREIGN KEY
            (
                "listing_id"
            )
                REFERENCES "listings"
            (
                "listing_id"
            )
                ON DELETE CASCADE
                );
            """,
            """
            CREATE TABLE IF NOT EXISTS "addresses"
            (
                "listing_id"
                INT
                PRIMARY
                KEY,
                "street"
                VARCHAR
            (
                255
            ) NOT NULL,
                "zip_code" INT NOT NULL,
                CONSTRAINT "fk_addresses_listing"
                FOREIGN KEY
            (
                "listing_id"
            )
                REFERENCES "listings"
            (
                "listing_id"
            )
                ON DELETE CASCADE,
                CONSTRAINT "fk_addresses_zip_code"
                FOREIGN KEY
            (
                "zip_code"
            )
                REFERENCES "zip_codes"
            (
                "zip_code"
            )
                ON DELETE RESTRICT
                );
            """
        ]

        print("Prüfe/Erstelle Tabellen...")
        for i, command in enumerate(table_commands):
            table_name_guess = command.split("CREATE TABLE IF NOT EXISTS \"")[1].split("\"")[
                0] if "CREATE TABLE IF NOT EXISTS \"" in command else f"Tabelle {i + 1}"
            try:
                self._execute_command(command)
                print(f"  Tabelle '{table_name_guess}' geprüft/erstellt.")
            except psycopg2.Error as e:
                # Fehler wurde bereits in _execute_command behandelt und geloggt
                # Hier könnten Sie spezifische Logik hinzufügen, wenn das Erstellen einer Tabelle fehlschlägt
                print(f"  Fehler beim Erstellen/Prüfen der Tabelle '{table_name_guess}'.")
                raise  # Wichtig, um den Initialisierungsprozess abzubrechen, falls kritisch

    def create_indexes(self):
        """Erstellt Indizes, falls sie noch nicht existieren."""
        index_commands = [
            """CREATE INDEX IF NOT EXISTS idx_zip_codes_city ON "zip_codes"("city");""",
            """CREATE INDEX IF NOT EXISTS idx_zip_codes_state_city ON "zip_codes"("state", "city");""",
            """CREATE INDEX IF NOT EXISTS idx_listings_price ON "listings"("price");""",
            """CREATE INDEX IF NOT EXISTS idx_listings_status ON "listings"("status");""",
            """CREATE INDEX IF NOT EXISTS idx_listings_brokered_by_price ON "listings"("brokered_by", "price");""",
            """CREATE INDEX IF NOT EXISTS idx_estate_details_bed ON "estate_details"("bed");""",
            """CREATE INDEX IF NOT EXISTS idx_estate_details_house_size ON "estate_details"("house_size");""",
            """CREATE INDEX IF NOT EXISTS idx_estate_details_bed_house_size ON "estate_details"("bed", "house_size");""",
            """CREATE INDEX IF NOT EXISTS idx_land_data_area_size ON "land_data"("area_size_in_square_m");""",
            # Der folgende Index verwendet varchar_pattern_ops.
            # Dies kann fehlschlagen, wenn die entsprechende Operator-Klasse nicht verfügbar ist
            # oder nicht für B-Tree-Indizes auf VARCHAR ohne Weiteres passt.
            # Ein Standard-B-Tree-Index wäre ("zip_code", "street").
            """CREATE INDEX IF NOT EXISTS idx_addresses_zip_code_street ON "addresses"("zip_code", "street" varchar_pattern_ops);"""
        ]

        print("Prüfe/Erstelle Indizes...")
        for i, command in enumerate(index_commands):
            index_name_guess = command.split("CREATE INDEX IF NOT EXISTS ")[1].split(" ON")[
                0] if "CREATE INDEX IF NOT EXISTS " in command else f"Index {i + 1}"
            try:
                self._execute_command(command)
                print(f"  Index '{index_name_guess}' geprüft/erstellt.")
            except psycopg2.Error as e:
                # Spezifische Behandlung für den varchar_pattern_ops Index, falls er fehlschlägt
                if "idx_addresses_zip_code_street" in command and "varchar_pattern_ops" in command:
                    print(
                        f"  WARNUNG: Index '{index_name_guess}' mit varchar_pattern_ops konnte nicht erstellt werden. "
                        "Möglicherweise fehlt eine Erweiterung oder die Operator-Klasse ist nicht passend. "
                        f"Fehler: {e}")
                    print("  Versuche stattdessen einen Standard-Index für ('zip_code', 'street')...")
                    try:
                        standard_index_cmd = """CREATE INDEX IF NOT EXISTS idx_addresses_zip_code_street_std ON "addresses"("zip_code", "street");"""
                        self._execute_command(standard_index_cmd)
                        print(f"  Alternativer Index 'idx_addresses_zip_code_street_std' geprüft/erstellt.")
                    except psycopg2.Error as e_std:
                        print(f"  Fehler auch beim Erstellen des alternativen Standard-Index: {e_std}")
                else:
                    # Fehler wurde bereits in _execute_command behandelt und geloggt
                    print(f"  Fehler beim Erstellen/Prüfen des Index '{index_name_guess}'.")
                    # raise # Optional: Abbruch, wenn Indizes kritisch sind

    def drop_indexes(self):
        """Löscht alle benutzerdefinierten Indizes, sofern sie existieren."""
        index_names = [
            "idx_zip_codes_city",
            "idx_zip_codes_state_city",
            "idx_listings_price",
            "idx_listings_status",
            "idx_listings_brokered_by_price",
            "idx_estate_details_bed",
            "idx_estate_details_house_size",
            "idx_estate_details_bed_house_size",
            "idx_land_data_area_size",
            "idx_addresses_zip_code_street",
            "idx_addresses_zip_code_street_std",  # Falls dieser verwendet wurde
        ]
        print("Lösche benutzerdefinierte Indizes...")
        for idx in index_names:
            try:
                drop_cmd = f'DROP INDEX IF EXISTS "{idx}";'
                self._execute_command(drop_cmd)
                print(f"  Index '{idx}' gelöscht (oder existierte nicht).")
            except psycopg2.Error as e:
                print(f"  Fehler beim Löschen des Index '{idx}': {e}")

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
                    WHERE z_sub.city IS NOT NULL
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

    def usecase7_bulk_import(self, data: Iterable[ListingRecord], batch_size: int = 1000) -> int:
        if not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer.")


        records_list = list(data)

        total_records = len(records_list)

        if total_records == 0:
            logger.info("No listings to import.")
            return 0

        # Calculate total number of batches
        total_batches = math.ceil(total_records / batch_size)

        logger.info(
            f"Starting batch import. Total listings to process: {total_records}, Batch size: {batch_size}, Total batches anticipated: {total_batches}")

        total_imported_listings = 0
        records_batch = []
        current_batch_number = 0

        for record_index, record in enumerate(records_list):
            records_batch.append(record)

            # Process the batch if it's full, or if it's the last record and the batch has items
            if len(records_batch) >= batch_size or (record_index == total_records - 1 and records_batch):
                current_batch_number += 1
                logger.info(
                    f"Processing batch {current_batch_number} of {total_batches} ({len(records_batch)} records)...")

                try:
                    # Assuming _process_batch is a method of the same class or a function you call
                    imported_in_this_batch = self._process_batch(records_batch)
                    total_imported_listings += imported_in_this_batch
                    logger.info(
                        f"Batch {current_batch_number} of {total_batches} processed. Successfully imported in this batch: {imported_in_this_batch}")
                except Exception as e:
                    # _process_batch should ideally handle its own transaction rollback on error
                    logger.error(f"Failed to process batch {current_batch_number} of {total_batches}: {e}")
                    # Depending on requirements, you might want to stop all further processing:
                    logger.error("Aborting batch import due to error in a batch.")
                    raise  # Re-raise the exception to stop the import process
                    # Or, if you want to attempt subsequent batches (less common for DB imports unless designed for it):
                    # logger.warning(f"Skipping batch {current_batch_number} due to error. Continuing with next batch.")
                    # continue
                finally:
                    records_batch = []  # Reset batch for the next set of records

        logger.info(
            f"Batch import completed. Total listings imported: {total_imported_listings} out of {total_records} listings processed across {current_batch_number} batches (anticipated: {total_batches}).")
        return total_imported_listings

    def _process_batch(self, batch: List[ListingRecord]) -> int:
        imported_in_batch = 0
        try:
            with self.conn.cursor() as cur:
                # 1. Prepare and Insert Brokers (as before)
                brokers_data = list(set([(int(r.brokered_by),) for r in batch]))
                if brokers_data:  # Ensure not empty
                    psycopg2.extras.execute_values(
                        cur,
                        "INSERT INTO brokers (brokered_by) VALUES %s ON CONFLICT (brokered_by) DO NOTHING",
                        brokers_data,
                        page_size=len(brokers_data)
                    )

                # 2. Prepare and Insert Zip Codes (as before)
                zip_codes_data = list(set([(r.zip_code, r.city, r.state) for r in batch]))
                if zip_codes_data:  # Ensure not empty
                    psycopg2.extras.execute_values(
                        cur,
                        "INSERT INTO zip_codes (zip_code, city, state) VALUES %s ON CONFLICT (zip_code) DO NOTHING",
                        zip_codes_data,
                        page_size=len(zip_codes_data)
                    )

                # 3. Prepare and Insert Listings, RETURNING listing_id
                # We need a way to map returned listing_ids back to the original records.
                # We can include a temporary unique key from the batch (like its index)
                # or unique combination of fields from the record itself IF THEY ARE GUARANTEED TO BE UNIQUE IN DB
                # For simplicity, let's assume brokered_by + price + status + street + zip_code could act as a temporary key
                # A safer method is to pass a batch-local index if your ON CONFLICT for listings is DO NOTHING.
                # If ON CONFLICT ... DO UPDATE, then things are more complex for mapping.
                # Assuming ON CONFLICT DO NOTHING or new inserts mostly.

                listings_values_to_insert = []
                # Keep track of original records for easier linking post-insert
                # We'll use the index in the batch as a temporary key
                for i, record in enumerate(batch):
                    listings_values_to_insert.append((
                        int(record.brokered_by), record.status, record.price, record.prev_sold_date,
                        i  # Original index in batch
                    ))

                if not listings_values_to_insert:
                    self.conn.commit()  # Nothing else to do for this batch
                    return 0

                # Note: The SQL must match the order of values in listings_values_to_insert
                # The `RETURNING original_batch_index, listing_id` is key here.
                # The column name `original_batch_index` is just for clarity in the SQL.
                # The `VALUES %s` will expand to (val1, val2, ..., original_idx_val).
                # The `RETURNING` clause gets the auto-generated listing_id AND our passed-through original_idx_val.
                # It's often better to use a CTE with UNNEST for this if using more complex types or many columns.
                # But for this structure, fetching specific columns in RETURNING will work.

                # We'll create a temporary structure for the INSERT that includes the original index
                # This is a common pattern. PostgreSQL doesn't directly support named parameters in VALUES for execute_values.
                # So the `original_batch_index` is just another value in the tuple.
                sql_insert_listings = """
                                      WITH val_list (brokered_by_val, status_val, price_val, prev_sold_date_val, \
                                                     original_batch_index) AS (VALUES %s)
                                      INSERT \
                                      INTO listings (brokered_by, status, price, prev_sold_date)
                                      SELECT brokered_by_val, status_val::listing_status_enum, price_val, prev_sold_date_val
                                      FROM val_list
                                               -- Add ON CONFLICT if necessary. E.g. ON CONFLICT (unique_constraint_columns) DO NOTHING
                                               -- If using ON CONFLICT DO NOTHING, rows that conflict won't be returned by RETURNING.
                                               -- Consider what makes a listing truly unique if re-importing.
                                               RETURNING (SELECT original_batch_index FROM val_list v WHERE v.brokered_by_val = listings.brokered_by AND v.price_val = listings.price AND v.status_val = listings.status), \
                                           listing_id; \
                                      """
                # The above RETURNING clause is a bit tricky to make it work with execute_values structure.
                # A simpler, more robust way for RETURNING with mapping:
                # Insert and return listing_id and the fields that allow you to uniquely identify the record
                # from the original batch, then map in Python.
                # Alternative for `listings_values_to_insert` and `sql_insert_listings`:

                listings_to_insert_tuples = [
                    (int(r.brokered_by), r.status, r.price, r.prev_sold_date)
                    for r in batch
                ]

                # Ensure there are records to insert
                if not listings_to_insert_tuples:
                    self.conn.commit()
                    return 0

                # This SQL assumes that the order of rows in `RETURNING` corresponds to the order of rows in `VALUES %s`.
                # This is generally true for simple INSERTs without ON CONFLICT DO NOTHING that skips rows.
                # If ON CONFLICT DO NOTHING might skip rows, this mapping becomes unreliable.
                # A safer bet with ON CONFLICT DO NOTHING is to `RETURNING listing_id, brokered_by, status, price`
                # and then reconstruct the mapping in Python, which is more complex.

                # For now, assuming either new inserts or ON CONFLICT DO UPDATE, where order is maintained or all rows are processed:
                # This simpler `RETURNING listing_id` relies on the order of returned IDs matching the order of inserted tuples.
                # This is generally true IF NO ROWS ARE SKIPPED by ON CONFLICT DO NOTHING.
                # If you have a unique constraint for listings and use ON CONFLICT DO NOTHING,
                # some rows in your batch might not be inserted, and the list of returned listing_ids
                # will be shorter than your batch, breaking the 1:1 index mapping.
                # A robust solution for ON CONFLICT DO NOTHING involves returning identifying columns
                # from the inserted row and re-matching in Python, or using more complex SQL (e.g., temp tables).

                # Let's proceed with a common use case: you want to get IDs for all rows attempted,
                # possibly fetching existing ones if they conflict and were not inserted.
                # This would typically involve INSERT ... ON CONFLICT ... DO UPDATE ... RETURNING listing_id
                # OR INSERT ... ON CONFLICT ... DO NOTHING and then a separate SELECT.

                # For this example, let's assume a simpler scenario where we want IDs for *newly inserted* rows.
                # We use a CTE approach which is generally more robust with `execute_values`.
                # The `template` argument in `execute_values` allows passing the original record's data alongside.

                # This is a more advanced and robust way to map:
                # Create a list of dictionaries or objects that will be passed as the `argslist`
                # The template will reference keys from these dicts/attributes from objects.

                # To keep it slightly simpler here, if you can't use complex CTEs with UNNEST easily through psycopg2,
                # and ON CONFLICT DO NOTHING is a possibility for `listings`:
                # The "row-by-row" for listings with RETURNING listing_id was not entirely bad if it's wrapped in a single transaction.
                # The *true* batching gain is for `execute_values`.
                # A hybrid: batch insert listings, then fetch their IDs if needed, then batch dependent.

                # Revised strategy for `listings` to get IDs reliably:
                # Insert listings and retrieve enough info to map back to original `ListingRecord` objects.
                # For example, if (brokered_by, price, street, zip_code) is unique enough *within the batch context*
                # or if you can add a unique batch identifier to your source data.

                # Simpler Python-side mapping if ON CONFLICT DO NOTHING is NOT heavily used for `listings`
                # or if `listings` always get inserted (no unique constraint / or you handle conflicts before Python)

                # Let's refine the `listings` insertion to be more robust with mapping.
                # We will insert and return the `listing_id` AND the values that help us map back to the original `ListingRecord`.
                # We need to ensure these identifying values are part of the `RETURNING` clause.
                # For this example, let's assume `brokered_by`, `price`, `status` are good enough to map back to the input `record`.
                # This is often the trickiest part.

                # Construct data for listings insert
                listing_insert_data = []
                # This list will hold tuples of (original_record_ref, insert_tuple_for_sql)
                # This helps map returned IDs back if needed, though execute_values doesn't directly support complex return mapping.

                # Fallback to a slightly less "pure batch" but often practical approach for the main table with RETURNING:
                # Using a loop for the main table to get IDs, but then batching dependent tables.
                # This is still better than full row-by-row for everything.
                # Or, use `psycopg2.extras.execute_batch` for the main listing inserts if you can construct statements.

                # Let's try to stick to `execute_values` for `listings` as well
                # This requires careful handling of `RETURNING` and mapping.
                # Assume `data_for_listing_insert` is `[(brokered_by, status, price, prev_sold_date, <unique_key_for_mapping_in_batch>), ...]`
                # For example, `<unique_key_for_mapping_in_batch>` could be the index of the record in `batch`.

                records_with_listing_id = []  # Will store (listing_id, original_record)

                # If you can define a unique constraint on listings that allows ON CONFLICT DO UPDATE SET ... RETURNING listing_id,
                # then you'll always get IDs back.
                # If using ON CONFLICT DO NOTHING, you'll only get IDs for newly inserted rows.

                # For simplicity here, and to ensure each original record gets a listing_id (either new or existing):
                # We might need to insert, then SELECT. Or use a more complex CTE.
                # The original code's loop for listings wasn't the worst part IF the goal is one ID at a time.
                # The issue was doing it for *everything*.

                # Let's try a common pattern: insert into `listings` and get back `listing_id`s.
                # We'll use `execute_values` and assume that the order of returned IDs matches the input order.
                # This is only safe if there are NO `ON CONFLICT DO NOTHING` clauses that would skip records.
                # If you must use `ON CONFLICT DO NOTHING`, this mapping strategy needs to be more robust.

                listing_tuples_for_insert = [
                    (int(r.brokered_by), r.status, r.price, r.prev_sold_date) for r in batch
                ]

                if listing_tuples_for_insert:
                    # This form of RETURNING with execute_values relies on order matching.
                    # It's generally safer to use a CTE with VALUES and an explicit join key for RETURNING.
                    inserted_listing_ids_tuples = psycopg2.extras.execute_values(
                        cur,
                        """INSERT INTO listings (brokered_by, status, price, prev_sold_date)
                           VALUES %s
                               -- IMPORTANT: Add ON CONFLICT (your_unique_columns_for_listing) DO UPDATE SET price = EXCLUDED.price -- or whatever
                               -- RETURNING listing_id;
                               -- If using ON CONFLICT DO NOTHING, this will not return IDs for skipped rows.
                               RETURNING listing_id;
                        """,
                        listing_tuples_for_insert,
                        template=None,  # No template needed for simple tuples
                        page_size=len(listing_tuples_for_insert),
                        fetch=True  # Crucial to get the RETURNING values
                    )

                    # **** CRITICAL Assumption for mapping `inserted_listing_ids_tuples` back to `batch` ****
                    # This assumes that `inserted_listing_ids_tuples` has the same number of elements
                    # and is in the same order as `listing_tuples_for_insert`.
                    # This holds if:
                    #   1. All rows are new and successfully inserted.
                    #   2. OR you use `ON CONFLICT ... DO UPDATE ... RETURNING listing_id`, so every row in the
                    #      batch results in a returned `listing_id` (either new or existing updated).
                    # This will BREAK if `ON CONFLICT ... DO NOTHING` causes some rows to be skipped,
                    # as `inserted_listing_ids_tuples` will be shorter than `batch`.

                    if len(inserted_listing_ids_tuples) != len(batch):
                        # This is a sign that the mapping assumption above is violated.
                        # You need a more robust mapping strategy, e.g., returning identifying columns
                        # along with listing_id and then re-matching in Python.
                        # Or use a temporary table / UNNEST approach in SQL.
                        # For now, we'll log an error and potentially raise or skip dependent inserts.
                        logger.error(
                            f"Mismatch in returned listing IDs ({len(inserted_listing_ids_tuples)}) vs batch size ({len(batch)}). "
                            "This may be due to ON CONFLICT DO NOTHING skipping rows. Dependent data might be incorrect.")
                        # Decide how to handle this: raise error, or try to proceed with matched ones?
                        # For robustness, it's better to have a mapping strategy that handles this.
                        # One way: fetch listing_id, plus the values used in ON CONFLICT clause, then map in Python.

                    # Assuming the mapping holds for now:
                    for i, record in enumerate(batch):
                        if i < len(inserted_listing_ids_tuples):
                            listing_id = inserted_listing_ids_tuples[i][0]
                            records_with_listing_id.append({'id': listing_id, 'record': record})
                            imported_in_batch += 1  # Count based on successful listing inserts.
                        else:
                            # This record was likely skipped by ON CONFLICT DO NOTHING
                            logger.warning(
                                f"Listing record for {record} likely skipped by ON CONFLICT DO NOTHING, no ID returned.")

                # 4. Prepare and Batch Insert Estate Details
                estate_details_data = []
                for item in records_with_listing_id:
                    listing_id, record = item['id'], item['record']
                    if record.bed is not None or record.bath is not None or record.house_size_sqm is not None:
                        estate_details_data.append((
                            listing_id, record.bed, record.bath, record.house_size_sqm
                        ))
                if estate_details_data:
                    psycopg2.extras.execute_values(
                        cur,
                        "INSERT INTO estate_details (listing_id, bed, bath, house_size) VALUES %s ON CONFLICT (listing_id) DO NOTHING",
                        estate_details_data,
                        page_size=len(estate_details_data)
                    )

                # 5. Prepare and Batch Insert Land Data
                land_data_values = []
                for item in records_with_listing_id:
                    listing_id, record = item['id'], item['record']
                    land_data_values.append((listing_id, record.lot_size_sqm))  # lot_size_sqm is mandatory

                if land_data_values:
                    psycopg2.extras.execute_values(
                        cur,
                        "INSERT INTO land_data (listing_id, area_size_in_square_m) VALUES %s ON CONFLICT (listing_id) DO NOTHING",
                        land_data_values,
                        page_size=len(land_data_values)
                    )

                # 6. Prepare and Batch Insert Addresses
                addresses_data = []
                for item in records_with_listing_id:
                    listing_id, record = item['id'], item['record']
                    street_val_str = str(int(record.street))  # Assuming street is float but stored as string of int
                    addresses_data.append((listing_id, street_val_str, record.zip_code))

                if addresses_data:
                    psycopg2.extras.execute_values(
                        cur,
                        "INSERT INTO addresses (listing_id, street, zip_code) VALUES %s ON CONFLICT (listing_id) DO NOTHING",
                        addresses_data,
                        page_size=len(addresses_data)
                    )

            self.conn.commit()
            logger.info(f"Successfully processed batch. {imported_in_batch} listings potentially inserted/updated.")
            return imported_in_batch  # This count might be off if ON CONFLICT DO NOTHING was used without robust mapping.
            # A more accurate count would be len(inserted_listing_ids_tuples)

        except psycopg2.Error as e:
            logger.error(f"Database error during batch processing: {e}")
            if self.conn and not self.conn.closed:
                self.conn.rollback()
            raise
        except Exception as e:
            logger.error(f"Unexpected error during batch processing: {e}")
            if self.conn and not self.conn.closed:
                self.conn.rollback()
            raise

    def get_total_count(self) -> int:
        query = "SELECT COUNT(*) FROM listings;"
        try:
            result = self._execute_query(query, fetch_one=True)
            return result[0] if result else 0
        except psycopg2.Error as e:
            logger.error(f"Failed to get total count of listings: {e}")
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
            self.drop_indexes()
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
    postgres = PostgresAdapter()
    print(f"Total count {postgres.get_total_count()}")

    data = read_listings(CSV_FILE_PATH)

    postgres.reset_database()
    postgres.usecase7_bulk_import(data, 20000)
    postgres.create_indexes()

    # Example usage
    test = postgres.usecase1_filter_properties(10, 250000)
    print(f"Usecase 1  Found: {len(test)}")
