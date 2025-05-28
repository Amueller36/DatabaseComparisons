import pandas as pd

# --- Configuration ---
INPUT_CSV_FILE  = "realtor-data.zip.csv"
OUTPUT_CSV_FILE = "transformed_real_estate_data.csv"

# --- Conversion Factors ---
ACRE_TO_SQM  = 4046.8564224
SQFT_TO_SQM  = 0.09290304

def transform_real_estate_data(df: pd.DataFrame) -> pd.DataFrame:
    # 1) Spalten umbenennen + neue Spalten anlegen
    df = df.rename(columns={
        "acre_lot":   "lot_size_sqm",
        "house_size": "house_size_sqm"
    }).copy()

    # 2) Numerische Konvertierung, fehlende Werte bleiben NaN
    for col, factor in [
        ("lot_size_sqm", ACRE_TO_SQM),
        ("house_size_sqm", SQFT_TO_SQM),
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce") * factor

    # 3) Datum konvertieren
    if "prev_sold_date" in df.columns:
        df["prev_sold_date"] = pd.to_datetime(
            df["prev_sold_date"], errors="coerce"
        )

    # 4) Erforderliche Spalten validieren und ungültige Zeilen entfernen
    required = ["brokered_by", "status", "price", "zip_code", "street", "city", "state"]
    # Konvertiere diese Spalten in numeric/string, damit dropna greift
    df["brokered_by"] = pd.to_numeric(df.get("brokered_by"), errors="coerce")
    df["price"]       = pd.to_numeric(df.get("price"), errors="coerce")
    df["status"]      = df.get("status").where(df["status"].notna(), None)
    # Jetzt alle Zeilen verwerfen, die in einer der required-Spalten NaN/None haben
    df = df.dropna(subset=required)

    # 5) Optional: Spalten-Typen einschränken (für Speicheroptimierung)
    dtype_casts = {
        "brokered_by":  "float64",
        "price":        "float64",
        "bed":          "float32",
        "bath":         "float32",
        "zip_code":     "Int64",    # Nullable Integer
    }
    for col, dtype in dtype_casts.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)

    # 6) Reihenfolge wiederherstellen
    #    neue Spalten lot_size_sqm/house_size_sqm an die Stelle der alten setzen
    final_cols = []
    for c in df.columns:
        if c == "lot_size_sqm":
            final_cols.append("lot_size_sqm")
        elif c == "house_size_sqm":
            final_cols.append("house_size_sqm")
        else:
            final_cols.append(c)
    df = df[final_cols]

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

    print(f"Gelesen: {len(df_orig)} Zeilen × {len(df_orig.columns)} Spalten.")
    df_trans = transform_real_estate_data(df_orig)
    print(f"Nach Entfernen ungültiger Zeilen: {len(df_trans)} Datensätze übrig.")

    df_trans.to_csv(
        OUTPUT_CSV_FILE,
        index=False,
        float_format="%.2f",
        date_format="%Y-%m-%d"
    )
    print(f"Geschrieben: {len(df_trans.columns)} Spalten nach {OUTPUT_CSV_FILE}.")

if __name__ == "__main__":
    main()
