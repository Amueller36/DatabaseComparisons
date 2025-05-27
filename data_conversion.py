import pandas as pd

# --- Configuration ---
# IMPORTANT: Replace with your actual input CSV file name
INPUT_CSV_FILE = "realtor-data.zip.csv"
# Name for the new CSV file that will be created
OUTPUT_CSV_FILE = "transformed_real_estate_data.csv"

# --- Conversion Factors ---
ACRE_TO_SQM = 4046.8564224  # 1 acre = 4046.8564224 square meters
SQFT_TO_SQM = 0.09290304  # 1 square foot = 0.09290304 square meters


def transform_real_estate_data(df_input):
    """
    Transforms 'acre_lot' from acres to square meters and renames it to 'lot_size_sqm'.
    Transforms 'house_size' from square feet to square meters and renames it to 'house_size_sqm'.
    Original 'acre_lot' and 'house_size' columns are effectively replaced.
    """
    df = df_input.copy()  # Work on a copy to avoid modifying the original DataFrame if it's used elsewhere

    # --- Transform 'acre_lot' ---
    # Convert the 'acre_lot' column to numeric, coercing errors to NaN (Not a Number)
    # This means if a value isn't a valid number, it becomes NaN.
    acre_lot_numeric = pd.to_numeric(df.get('acre_lot'), errors='coerce')
    # Perform the conversion to square meters. NaN * number results in NaN.
    df['lot_size_sqm'] = acre_lot_numeric * ACRE_TO_SQM

    # --- Transform 'house_size' ---
    # Convert the 'house_size' column to numeric, coercing errors to NaN
    house_size_numeric = pd.to_numeric(df.get('house_size'), errors='coerce')
    # Perform the conversion to square meters
    df['house_size_sqm'] = house_size_numeric * SQFT_TO_SQM

    # --- Drop the original columns ---
    # We check if they exist before dropping to avoid KeyErrors if they were missing
    columns_to_drop = []
    if 'acre_lot' in df.columns:
        columns_to_drop.append('acre_lot')
    if 'house_size' in df.columns:
        columns_to_drop.append('house_size')

    if columns_to_drop:
        df.drop(columns=columns_to_drop, inplace=True)

    return df


def main():
    print(f"Starting CSV transformation process...")
    print(f"Reading data from: {INPUT_CSV_FILE}")
    print(f"Transformed data will be saved to: {OUTPUT_CSV_FILE}")

    try:
        # Read the input CSV file into a pandas DataFrame
        # Pandas automatically infers data types for columns.
        df_original = pd.read_csv(INPUT_CSV_FILE)
        print(
            f"Successfully read {len(df_original)} rows and {len(df_original.columns)} columns from {INPUT_CSV_FILE}.")

        # Keep track of the original column order to try and maintain it
        original_column_order = df_original.columns.tolist()

        # Apply the transformations
        df_transformed = transform_real_estate_data(df_original)
        print("Data transformation complete.")

        # Reconstruct the column order to place new columns where the old ones were
        final_column_order = []
        for col_name in original_column_order:
            if col_name == 'acre_lot' and 'lot_size_sqm' in df_transformed.columns:
                final_column_order.append('lot_size_sqm')
            elif col_name == 'house_size' and 'house_size_sqm' in df_transformed.columns:
                final_column_order.append('house_size_sqm')
            elif col_name not in ['acre_lot', 'house_size']:  # Add other original columns
                if col_name in df_transformed.columns:  # Check if it wasn't dropped for other reasons
                    final_column_order.append(col_name)

        # Ensure all columns from df_transformed are present in the final order
        # This adds any new columns that weren't explicitly placed (e.g. if transform_real_estate_data changed more)
        for col in df_transformed.columns:
            if col not in final_column_order:
                final_column_order.append(col)

        df_output = df_transformed[final_column_order]

        # Write the transformed DataFrame to a new CSV file
        # index=False prevents pandas from writing the DataFrame index as a column
        # float_format='%.2f' formats floating point numbers to 4 decimal places for cleaner output
        df_output.to_csv(OUTPUT_CSV_FILE, index=False, float_format='%.2f')
        print(f"Successfully saved transformed data with {len(df_output.columns)} columns to {OUTPUT_CSV_FILE}.")
        print(f"Output columns: {', '.join(df_output.columns)}")

    except FileNotFoundError:
        print(f"Error: Input file '{INPUT_CSV_FILE}' not found. Please check the file path and name.")
    except pd.errors.EmptyDataError:
        print(f"Error: Input file '{INPUT_CSV_FILE}' is empty or not a valid CSV.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        # For detailed debugging, you might want to print the full traceback:
        # import traceback
        # traceback.print_exc()


if __name__ == "__main__":
   main()