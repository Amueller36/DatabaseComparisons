from dataclasses import dataclass
from datetime import datetime
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
            if not val or str(val).strip() in ("", "NaT", "nan", "None"):
                return None
            # Accept only strings or datetimes
            if isinstance(val, datetime):
                return val
            try:
                return datetime.fromisoformat(str(val))
            except Exception:
                raise ValueError("Could not convert date to iso8601 format")

        brokered_by_val = to_float(row.get("brokered_by"))
        status_val = row.get("status", "").strip()
        price_val = to_float(row.get("price"))
        lot_size_val = to_float(row.get("lot_size_sqm"))
        street_val = to_float(row.get("street"))
        city_val = row.get("city", "").strip()
        state_val = row.get("state", "").strip()
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
