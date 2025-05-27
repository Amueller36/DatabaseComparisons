import pandas as pd
from pymongo import MongoClient, errors

from datetime import datetime

# --- Configuration ---
MONGO_URI = "mongodb://localhost:27020"
DATABASE_NAME = "real_estate_db"
COLLECTION_NAME = "listings"  # This matches your main SQL table name
CSV_FILE_PATH = "transformed_real_estate_data.csv"  # <--- ENSURE THIS IS THE CORRECT CSV (likely the one already transformed to SQM)
BATCH_SIZE = 1000


# --- Helper Conversion Functions ---
def safe_to_int(value_str):
    """Safely converts a string (potentially representing a float like "123.00") to an int."""
    stripped_value = str(value_str).strip()
    if stripped_value:
        try:
            return int(float(stripped_value))
        except ValueError:
            print(f"  Warning: Could not convert '{value_str}' to int.")
            return None
    return None


def safe_to_float(value_str):
    """Safely converts a string to a float."""
    stripped_value = str(value_str).strip()
    if stripped_value:
        try:
            return float(stripped_value)
        except ValueError:
            print(f"  Warning: Could not convert '{value_str}' to float.")
            return None
    return None


def parse_date_flexible(date_str):
    """Parses a date string into a datetime object."""
    if not date_str or (isinstance(date_str, float) and pd.isna(date_str)):
        return None
    date_str = str(date_str).strip()
    if not date_str:
        return None
    formats_to_try = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y", "%d-%m-%Y %H:%M:%S", "%d-%m-%Y"]
    for fmt in formats_to_try:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    print(f"Warning: Could not parse date string: '{date_str}' with known formats.")
    return None


def main():
    print(f"Starting data ingestion from '{CSV_FILE_PATH}' to MongoDB collection '{COLLECTION_NAME}'...")

    try:
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        print(f"Connected to MongoDB: {MONGO_URI}, Database: {DATABASE_NAME}, Collection: {COLLECTION_NAME}")

        # Define new index structures based on the new schema
        print("Ensuring indexes exist based on new schema...")
        try:
            # Indexes for 'listings' collection
            collection.create_index([("brokered_by", 1)], name="idx_brokered_by")
            collection.create_index([("price", 1)], name="idx_price")
            collection.create_index([("status", 1)], name="idx_status")

            # Indexes for fields within embedded documents
            collection.create_index([("estate_details.bed", 1)], name="idx_estate_bed")
            collection.create_index([("estate_details.house_size", 1)], name="idx_estate_house_size")
            collection.create_index([("land_data.area_size_in_square_m", 1)], name="idx_land_area_size")
            collection.create_index([("address.zip_code", 1)], name="idx_address_zip")
            collection.create_index([("address.city", 1)], name="idx_address_city")
            collection.create_index([("address.state", 1)], name="idx_address_state")

            # Example compound indexes (adjust as needed for your queries)
            collection.create_index([("address.city", 1), ("price", 1)], name="idx_address_city_price")
            collection.create_index([("estate_details.bed", 1), ("estate_details.house_size", 1)],
                                    name="idx_estate_bed_size")

            print("Indexes ensured successfully.")
        except errors.OperationFailure as e:
            print(f"Warning: Index creation failed or partially failed: {e}")

        total_documents_inserted = 0
        chunk_count = 0

        for df_chunk in pd.read_csv(CSV_FILE_PATH, chunksize=BATCH_SIZE, low_memory=False, na_filter=False):
            chunk_count += 1
            print(f"Processing chunk {chunk_count} (approx. {len(df_chunk)} rows)...")

            documents_batch = []
            rows_in_chunk_processed = 0
            for index, row in df_chunk.iterrows():
                rows_in_chunk_processed += 1
                try:
                    doc = {}

                    # --- listings ---
                    doc["brokered_by"] = safe_to_int(row.get("brokered_by", ""))  # From CSV brokered_by
                    doc["status"] = str(row.get("status", "")).strip() or None
                    doc["price"] = safe_to_float(row.get("price", ""))  # From CSV price
                    doc["prev_sold_date"] = parse_date_flexible(row.get("prev_sold_date", ""))

                    # --- estate_details (embedded) ---
                    estate_details = {
                        "bed": safe_to_int(row.get("bed", "")),  # From CSV bed
                        "bath": safe_to_int(row.get("bath", "")),  # From CSV bath
                        # IMPORTANT: Assumes CSV has 'house_size_sqm' column (value in sqm)
                        "house_size": safe_to_float(row.get("house_size_sqm", ""))  # SQL name: house_size
                    }
                    doc["estate_details"] = estate_details

                    # --- land_data (embedded) ---
                    land_data = {
                        # IMPORTANT: Assumes CSV has 'lot_size_sqm' column (value in sqm)
                        "area_size_in_square_m": safe_to_float(row.get("lot_size_sqm", ""))
                        # SQL name: area_size_in_square_m
                    }
                    doc["land_data"] = land_data

                    # --- addresses & zip_codes (combined and embedded as 'address') ---
                    # SQL wants zip_code as int, which will lose leading zeros (e.g., "00601" becomes 601)
                    # If preserving leading zeros is important, store zip_code as string in MongoDB.
                    # For this script, implementing as int per your SQL schema.
                    address = {
                        "street": safe_to_float(row.get("street", "")),  # From CSV street, SQL type float
                        "zip_code": safe_to_int(row.get("zip_code", "")),  # From CSV zip_code
                        "city": str(row.get("city", "")).strip() or None,  # From CSV city
                        "state": str(row.get("state", "")).strip() or None  # From CSV state
                    }
                    doc["address"] = address

                    documents_batch.append(doc)
                except Exception as e:  # Catch any other unexpected error during doc creation
                    print(
                        f"  Skipping row due to UNEXPECTED error during document creation (chunk {chunk_count}, approx row in CSV: {total_documents_inserted + rows_in_chunk_processed}): {e}. Data: {row.to_dict()}")

            if documents_batch:
                try:
                    collection.insert_many(documents_batch, ordered=False)
                    inserted_count_this_batch = len(documents_batch)
                    total_documents_inserted += inserted_count_this_batch
                    print(
                        f"  Inserted batch of {inserted_count_this_batch} documents. Total inserted so far: {total_documents_inserted}")
                except errors.BulkWriteError as bwe:
                    # Basic logging for bulk write errors; bwe.details contains more info
                    # Successfully inserted count can be derived from bwe.details['nInserted'] if needed.
                    print(
                        f"  Bulk write error during insert_many. Some documents may not have been inserted. Details (partial): {bwe.details.get('writeErrors', [])[:2]}")  # Log first 2 errors
                    # You might want to count successful inserts from bwe.details if precision is critical here
                    # For now, total_documents_inserted reflects attempted appends to batch.
            else:
                print("  No documents to insert in this batch (all rows might have had errors or chunk was empty).")

    except FileNotFoundError:
        print(f"Fatal Error: CSV file not found at '{CSV_FILE_PATH}'. Please check the path.")
        return
    except errors.ServerSelectionTimeoutError as sste:  # More specific connection error
        print(
            f"Fatal Error: Could not connect to MongoDB at '{MONGO_URI}' (Server selection timeout). Check if MongoDB is running, accessible, and URI is correct. Error: {sste}")
        return
    except errors.ConnectionFailure as cf:
        print(
            f"Fatal Error: Could not connect to MongoDB at '{MONGO_URI}'. Check if MongoDB is running and accessible. Error: {cf}")
        return
    except Exception as e:
        print(f"An unexpected fatal error occurred: {e}")
        # For detailed debugging:
        # import traceback
        # traceback.print_exc()
        return
    finally:
        if 'client' in locals() and client:
            client.close()
            print("MongoDB connection closed.")

    print(f"\n--- Ingestion Summary ---")
    print(f"Processed {chunk_count} chunks.")
    print(
        f"Total documents successfully added to insert batches: {total_documents_inserted}")  # This count might be higher than actual DB inserts if BulkWriteErrors occurred.
    print(f"Data ingestion finished.")


if __name__ == "__main__":
    # Before running:
    # 1. Ensure your MongoDB server is running and accessible via MONGO_URI.
    # 2. CRITICAL: Verify CSV_FILE_PATH. This script assumes it's the CSV that has
    #    ALREADY been transformed (contains 'lot_size_sqm' and 'house_size_sqm').
    #    If you are using the ORIGINAL CSV, the logic for 'house_size' and
    #    'area_size_in_square_m' needs to include the unit conversions (sqft/acres to sqm).
    # 3. Install necessary libraries: pip install pymongo pandas
    main()