import pandas as pd

# --- Configuration ---
INPUT_CSV_FILE = "realtor-data.zip.csv"
OUTPUT_CSV_FILE = "transformed_real_estate_data.csv"

# --- Conversion Factors ---
ACRE_TO_SQM = 4046.8564224
SQFT_TO_SQM = 0.09290304

def format_city(city):
    """Formatiere Städtenamen einheitlich: 'Title Case' und keine Leerzeichen."""
    if pd.isna(city):
        return pd.NA
    # Nur Alphabetisches behalten und alles korrekt großschreiben
    return str(city).strip().title()

def transform_real_estate_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 1) Spalten umbenennen + neue Spalten anlegen
    df.rename(columns={
        "acre_lot": "lot_size_sqm",
        "house_size": "house_size_sqm"
    }, inplace=True)

    # 2) Flächen konvertieren
    area_conversions = {
        "lot_size_sqm": ACRE_TO_SQM,
        "house_size_sqm": SQFT_TO_SQM,
    }
    for col, factor in area_conversions.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce") * factor
        else:
            df[col] = pd.NA

    # 3) Datum (prev_sold_date) konvertieren
    if "prev_sold_date" in df.columns:
        df["prev_sold_date"] = pd.to_datetime(df["prev_sold_date"], errors="coerce", utc=True)
        # Ungültige/zu alte Daten entfernen
        year_1970_or_earlier_mask = df["prev_sold_date"].dt.year <= 1970
        df.loc[year_1970_or_earlier_mask, "prev_sold_date"] = pd.NaT
        df["prev_sold_date"] = df["prev_sold_date"].dt.to_pydatetime()
    else:
        df["prev_sold_date"] = pd.NA

    # 4) Pflichtfelder prüfen
    required_field_specs = {
        "brokered_by": "numeric",
        "status": "string",
        "price": "numeric",
        "lot_size_sqm": "numeric",
        "street": "numeric",
        "city": "string",
        "state": "string",
        "zip_code": "numeric"
    }
    cols_to_drop_na = []
    for col, spec_type in required_field_specs.items():
        if col not in df.columns:
            print(f"WARNUNG: Erforderliche Spalte '{col}' fehlt in der Eingabedatei.")
            continue
        cols_to_drop_na.append(col)
        if spec_type == "numeric":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif spec_type == "string":
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace(['', 'nan', 'NaN', 'None', 'none', 'NA', 'NaT', '<NA>', 'null'], pd.NA)

    # 4b) Städte vereinheitlichen!
    if "city" in df.columns:
        df["city"] = df["city"].apply(format_city)

    # 5) Zeilen mit fehlenden Pflichtfeldern entfernen
    existing_cols_for_dropna = [c for c in cols_to_drop_na if c in df.columns]
    if existing_cols_for_dropna:
        df.dropna(subset=existing_cols_for_dropna, inplace=True)

    # 6) Optional: Datentypen für bestimmte Felder setzen
    dtype_casts = {
        "brokered_by": "float32",
        "price": "float32",
        "lot_size_sqm": "float32",
        "street": "float32",
        "bed": "Int16",
        "bath": "Int16",
        "house_size_sqm": "float32",
        "zip_code": "Int64",
    }
    for col, dtype in dtype_casts.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)

    # 7) Dubletten entfernen – nach allen wichtigen Feldern!
    # Tipp: city, street, zip_code, price sind oft die besten "natürlichen" Keys für Immobilien
    dedup_columns = ["brokered_by", "prev_sold_date", "price", "street", "city", "zip_code"]
    subset = [col for col in dedup_columns if col in df.columns]
    if subset:
        df.drop_duplicates(subset=subset, keep="first", inplace=True)

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
    df_trans = transform_real_estate_data(df_orig.copy())
    print(f"Nach Bearbeitung und Entfernen ungültiger Zeilen: {len(df_trans)} Datensätze übrig.")

    try:
        df_trans.to_csv(
            OUTPUT_CSV_FILE,
            index=False,
            float_format="%.2f",
            date_format="%Y-%m-%d %H:%M:%S",  # Behält volle Datetime-Info
            na_rep=""
        )
        print(f"Geschrieben: {len(df_trans.columns)} Spalten nach {OUTPUT_CSV_FILE}.")
    except Exception as e:
        print(f"Fehler beim Schreiben der CSV-Datei {OUTPUT_CSV_FILE}: {e}")


if __name__ == "__main__":
    main()
