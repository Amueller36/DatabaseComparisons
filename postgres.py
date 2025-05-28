# Imports
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy import text

#Create a Connection
DATABASE_URL = "postgresql+psycopg2://mds:mds@152.53.248.27:5432/postgres"
engine = create_engine(DATABASE_URL)

# Global Variables
CHUNK_SIZE = 10000
invalid_rows = []

#Functions
def from_feet_to_sqm(sq_feet):
    return sq_feet * 0.092903

def prepare_bulk_data(df):
    listings_input = []
    zip_codes = set()
    brokers = set()
    row_map = {}

    for idx, row in df.iterrows():
        try:
            if any(pd.isna(row[col]) or str(row[col]).strip() == '' for col in ['zip_code', 'brokered_by', 'status', 'price']):
                invalid_rows.append((idx, row.to_dict()))
                continue

            zip_codes.add((int(row['zip_code']), row['city'], row['state']))
            brokers.add((int(row['brokered_by']),))

            listings_input.append((
                int(row['brokered_by']),
                row['status'],
                row['price'],
                row['prev_sold_date'] if not pd.isna(row['prev_sold_date']) else None
            ))

            row_map[len(listings_input) - 1] = row  # Map insert order index to row

        except Exception as e:
            invalid_rows.append((idx, row.to_dict(), str(e)))

    return listings_input, zip_codes, brokers, row_map

def bulk_named_insert(conn, sql, tuple_data):
    if not tuple_data:
        return
    named_data = [{str(i + 1): val for i, val in enumerate(row)} for row in tuple_data]
    conn.execute(text(sql), named_data)

def bulk_insert_all(df):
    with engine.begin() as conn:
        for i in range(0, len(df), CHUNK_SIZE):
            print(f"working on chunk number {i}")
            chunk = df.iloc[i:i + CHUNK_SIZE]

            listings_input, zip_codes, brokers, row_map = prepare_bulk_data(chunk)

            # Insert zip codes
            bulk_named_insert(conn,
                "INSERT INTO zip_codes (zip_code, city, state) VALUES (:1, :2, :3) ON CONFLICT DO NOTHING",
                list(zip_codes)
            )

            # Insert brokers
            bulk_named_insert(conn,
                "INSERT INTO brokers (brokered_by) VALUES (:1) ON CONFLICT DO NOTHING",
                list(brokers)
            )

            # Insert listings and capture listing_ids
            inserted_listing_ids = []
            if listings_input:
                result = conn.execute(
                    text("""
                        INSERT INTO listings (brokered_by, status, price, prev_sold_date)
                        VALUES (:brokered_by, :status, :price, :prev_sold_date)
                        RETURNING listing_id
                    """),
                    [
                        {
                            'brokered_by': b,
                            'status': s,
                            'price': p,
                            'prev_sold_date': d
                        } for b, s, p, d in listings_input
                    ]
                )
                inserted_listing_ids = result.scalars().all()

            # Prepare dependent inserts
            estate_details = []
            land_data = []
            addresses = []

            for i, listing_id in enumerate(inserted_listing_ids):
                row = row_map[i]
                try:
                    if any(not pd.isna(row[col]) and str(row[col]).strip() != '' for col in ['bed', 'bath', 'house_size']):
                        estate_details.append((
                            listing_id,
                            row['bed'],
                            row['bath'],
                            from_feet_to_sqm(row['house_size'])
                        ))

                    if not pd.isna(row['acre_lot']):
                        area_m2 = row['acre_lot'] * 4046.8564224
                        land_data.append((listing_id, area_m2))

                    addresses.append((
                        listing_id,
                        row['street'] if not pd.isna(row['street']) else None,
                        int(row['zip_code'])
                    ))
                except Exception as e:
                    invalid_rows.append((i, row.to_dict(), str(e)))

            # Insert dependent tables
            bulk_named_insert(conn,
                "INSERT INTO estate_details (listing_id, bed, bath, house_size) VALUES (:1, :2, :3, :4)",
                estate_details
            )

            bulk_named_insert(conn,
                "INSERT INTO land_data (listing_id, area_size_in_square_m) VALUES (:1, :2)",
                land_data
            )

            bulk_named_insert(conn,
                "INSERT INTO addresses (listing_id, street, zip_code) VALUES (:1, :2, :3)",
                addresses
            )

#Main
def main():
    # Load data
    df = pd.read_csv(r"realtor-data.csv")
    bulk_insert_all(df)
    print(f"Rows that could not be inserted '({len(invalid_rows)})':")
    for bad_row in invalid_rows:
        print(bad_row)

if __name__ == "__main__":
    main()