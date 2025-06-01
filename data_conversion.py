import pandas as pd

# --- Configuration ---
INPUT_CSV_FILE = "realtor-data.zip.csv"
OUTPUT_CSV_FILE = "transformed_real_estate_data.csv"

# --- Conversion Factors ---
ACRE_TO_SQM = 4046.8564224
SQFT_TO_SQM = 0.09290304


def transform_real_estate_data(df: pd.DataFrame) -> pd.DataFrame:
    # 0. Create a copy to avoid SettingWithCopyWarning
    df = df.copy()

    # 1) Spalten umbenennen + neue Spalten anlegen
    df.rename(columns={
        "acre_lot": "lot_size_sqm",
        "house_size": "house_size_sqm"
    }, inplace=True)

    # 2) Numerische Konvertierung für Flächenangaben, fehlende Werte bleiben NaN
    area_conversions = {
        "lot_size_sqm": ACRE_TO_SQM,
        "house_size_sqm": SQFT_TO_SQM,
    }
    for col, factor in area_conversions.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce") * factor
        else:
            # If original column (e.g. acre_lot) was missing, add the new col as NaN
            # so it's handled consistently if it's a required field later.
            df[col] = pd.NA

    # 3) Datum konvertieren (prev_sold_date)
    if "prev_sold_date" in df.columns:
        df["prev_sold_date"] = pd.to_datetime(df["prev_sold_date"], errors="coerce")

        # Ungültige Daten (pre-1970) zu NaT konvertieren
        if not df["prev_sold_date"].empty:
            # Ensure the column is actually of datetime type before using .dt accessor
            if pd.api.types.is_datetime64_any_dtype(df["prev_sold_date"]):
                pre_1970_mask = df["prev_sold_date"].dt.year < 1970
                # Set pre-1970 dates to NaT
                df.loc[pre_1970_mask, "prev_sold_date"] = pd.NaT
            else:
                # If after to_datetime it's still not a datetime (e.g. all NaT became object)
                # treat all as NaT for safety, though to_datetime should handle this.
                df["prev_sold_date"] = pd.NaT

                # NaT-Werte (ursprüngliche Fehler oder pre-1970 Daten) in leere Strings umwandeln für CSV
        default_datetime_str = "1970-01-01"
        # Fill invalid/missing dates with the default ClickHouse-safe value

        df["prev_sold_date"] = df["prev_sold_date"].where(
            df["prev_sold_date"].notna(),
            default_datetime_str
        )
        df["prev_sold_date"] = df["prev_sold_date"].dt.strftime("%Y-%m-%d")



    else:
        # If prev_sold_date is missing, create it as empty strings to maintain column structure
        # if it's expected in the output CSV schema. ListingRecord handles it as Optional.
        df["prev_sold_date"] = ""

    # 4) Erforderliche Spalten validieren und ungültige Zeilen entfernen
    #    Diese Spezifikationen basieren auf den Pflichtfeldern von ListingRecord.
    required_field_specs = {
        "brokered_by": "numeric",
        "status": "string",
        "price": "numeric",
        "lot_size_sqm": "numeric",  # Wird in Schritt 2 erstellt/konvertiert
        "street": "numeric",  # ListingRecord erwartet float
        "city": "string",
        "state": "string",
        "zip_code": "numeric"  # ListingRecord erwartet int
    }

    cols_to_drop_na = []

    for col, spec_type in required_field_specs.items():
        if col not in df.columns:
            print(f"WARNUNG: Erforderliche Spalte '{col}' fehlt in der Eingabedatei. "
                  "Zeilen ohne diese Spalte können nicht valide sein.")
            # Add a column of NAs to ensure rows might be dropped if logic depends on it,
            # or handle as a more critical error if necessary.
            # For now, this means ListingRecord would fail later if this column is truly essential
            # and not caught by dropna based on other criteria.
            # However, dropna will only act on columns present in its 'subset' argument.
            continue  # Skip processing for a missing column

        cols_to_drop_na.append(col)  # Add to list for dropna

        if spec_type == "numeric":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif spec_type == "string":
            df[col] = df[col].astype(str).str.strip()
            # Ersetze leere Strings und verschiedene String-Darstellungen von Null/NaN durch pd.NA
            df[col] = df[col].replace(['', 'nan', 'NaN', 'None', 'none', 'NA', 'NaT', '<NA>', 'null'], pd.NA)

    # Entferne Zeilen, in denen einer der *vorhandenen* erforderlichen Spalten NaN/pd.NA ist
    existing_cols_for_dropna = [c for c in cols_to_drop_na if c in df.columns]
    if existing_cols_for_dropna:  # only call dropna if there are columns to check
        df.dropna(subset=existing_cols_for_dropna, inplace=True)

    # 5) Optionale Spalten-Typen einschränken (für Speicheroptimierung und korrekte Typen für ListingRecord)
    #    Dies geschieht nach dropna, da dropna auf NaN (float) oder pd.NA arbeitet.
    #    ListingRecord erwartet int für bed, bath, zip_code.
    dtype_casts = {
        "brokered_by": "float32",  # ListingRecord: float
        "price": "float32",  # ListingRecord: float
        "lot_size_sqm": "float32",  # ListingRecord: float
        "street": "float32",  # ListingRecord: float
        # Optionale Felder:
        "bed": "Int16",  # ListingRecord: Optional[int], Nullable Integer
        "bath": "Int16",  # ListingRecord: Optional[int], Nullable Integer
        "house_size_sqm": "float32",  # ListingRecord: Optional[float]
        # Pflichtfeld, aber Typanpassung hier:
        "zip_code": "Int64",  # ListingRecord: int, Nullable Integer für pd.NA Handling
    }
    for col, dtype in dtype_casts.items():
        if col in df.columns:
            # Für Int-Typen müssen pd.to_numeric Werte zuerst in Float (mit NaN) oder String sein,
            # dann können sie in Nullable Int (z.B. Int64) konvertiert werden.
            if pd.api.types.is_integer_dtype(dtype):  # Checks for 'Int16', 'Int64' etc.
                # errors='coerce' ensures unconvertible values become pd.NA
                df[col] = pd.to_numeric(df[col], errors='coerce').astype(dtype)
            else:  # For float types
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)

    # 6) Reihenfolge der Spalten (optional, falls eine bestimmte Reihenfolge gewünscht ist)
    #    Wenn keine spezifische Reihenfolge benötigt wird, kann dieser Schritt entfallen.
    #    Beispiel: Definieren einer gewünschten Spaltenreihenfolge
    #    final_column_order = [
    #        "brokered_by", "status", "price", "lot_size_sqm", "street", "city", "state", "zip_code",
    #        "bed", "bath", "house_size_sqm", "prev_sold_date",
    #        # ... andere Spalten, die behalten werden sollen ...
    #    ]
    #    existing_final_cols = [c for c in final_column_order if c in df.columns]
    #    df = df[existing_final_cols]

    return df


def main():
    try:
        df_orig = pd.read_csv(INPUT_CSV_FILE)
    except FileNotFoundError:
        print(f"Fehler: {INPUT_CSV_FILE} nicht gefunden.")
        return
    except pd.errors.EmptyDataError:
        print(f"Fehler: {INPUT_CSV_FILE} ist leer oder kein gültiges CSV.")
        return
    except Exception as e:
        print(f"Ein unerwarteter Fehler beim Lesen der CSV-Datei ist aufgetreten: {e}")
        return

    print(f"Gelesen: {len(df_orig)} Zeilen × {len(df_orig.columns)} Spalten.")
    df_trans = transform_real_estate_data(df_orig.copy())  # df.copy() to ensure original is not modified by transform
    print(f"Nach Bearbeitung und Entfernen ungültiger Zeilen: {len(df_trans)} Datensätze übrig.")

    try:
        df_trans.to_csv(
            OUTPUT_CSV_FILE,
            index=False,
            float_format="%.2f",
            date_format="%Y-%m-%d",  # Gilt für echte datetime-Objekte
            na_rep=""  # Schreibt NaN/pd.NA als leere Strings, was ListingRecord.from_csv_row erwartet
        )
        print(f"Geschrieben: {len(df_trans.columns)} Spalten nach {OUTPUT_CSV_FILE}.")
    except Exception as e:
        print(f"Fehler beim Schreiben der CSV-Datei {OUTPUT_CSV_FILE}: {e}")


if __name__ == "__main__":
    main()