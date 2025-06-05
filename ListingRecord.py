from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Iterator, Dict

@dataclass
class ListingRecord:
    # Pflichtfelder
    brokered_by:   float
    status:        str
    price:         float
    lot_size_sqm:  float
    street:        float
    city:          str
    state:         str
    zip_code:      int

    # optionale Felder
    bed:            Optional[int]   = None
    bath:           Optional[int]    = None
    house_size_sqm: Optional[float]    = None
    prev_sold_date: Optional[datetime] = None

    @staticmethod
    def from_csv_row(row: Dict[str, str]) -> "ListingRecord":
        def to_float(val: str) -> Optional[float]:
            try:
                return float(val) if val not in (None, "") else None
            except ValueError:
                return None

        def to_int(val: str) -> Optional[int]:
            try:
                return int(val) if val not in (None, "") else None
            except ValueError:
                return None

        def to_date(val) -> Optional[datetime]:
            """
            Convert val to a Python datetime or return None.

            • Accepts ISO strings     → datetime
            • Accepts datetime object → same datetime
            • Rejects anything <1970 or >2105 → None
            """
            MIN_YEAR = 1970
            MAX_YEAR = 2105

            if not val or str(val).strip() in ("", "NaT", "nan", "None", "NaN"):
                return None

            # Already a datetime?
            if isinstance(val, datetime):
                dt = val
            else:
                try:
                    dt = datetime.fromisoformat(str(val))
                except Exception:
                    # unparsable → treat as missing
                    return None

            # Convert tz-aware → naive UTC
            if dt.tzinfo is not None and dt.utcoffset() is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)

            # Range check
            if dt.year < MIN_YEAR or dt.year > MAX_YEAR:
                return None

            return dt

        brokered_by_val = to_float(row.get("brokered_by"))
        status_val = row.get("status", "").strip()
        price_val = to_float(row.get("price"))
        lot_size_val = to_float(row.get("lot_size_sqm"))
        street_val = to_float(row.get("street"))
        city_val = row.get("city").strip()
        state_val = row.get("state").strip()
        zip_code_val = to_int(row.get("zip_code"))

        # Validation of mandatory fields
        if brokered_by_val is None: raise ValueError(f"Missing or invalid brokered_by in row: {row}")
        if not status_val: raise ValueError(f"Missing status in row: {row}")
        if price_val is None: raise ValueError(f"Missing or invalid price in row: {row}")
        if lot_size_val is None: raise ValueError(f"Missing or invalid lot_size_sqm in row: {row}")
        if street_val is None: raise ValueError(f"Missing or invalid street in row: {row}")
        if not city_val: raise ValueError(f"Missing city in row: {row}")
        if not state_val: raise ValueError(f"Missing state in row: {row}")
        if zip_code_val is None: raise ValueError(f"Missing or invalid zip_code in row: {row}")

        return ListingRecord(
            brokered_by=brokered_by_val,
            status=status_val,
            price=price_val,
            lot_size_sqm=lot_size_val,
            street=street_val,
            city=city_val,
            state=state_val,
            zip_code=zip_code_val,
            bed=to_int(row.get("bed")),
            bath=to_int(row.get("bath")),
            house_size_sqm=to_float(row.get("house_size_sqm")),
            prev_sold_date=to_date(row.get("prev_sold_date"))
        )

def read_listings(path: str) -> Iterator[ListingRecord]:
    import csv
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                yield ListingRecord.from_csv_row(row)
            except ValueError:
                # ungültige Zeile überspringen
                continue
