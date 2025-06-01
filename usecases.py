from abc import ABC, abstractmethod
from typing import Any, Dict, List, Iterable, Optional

from ListingRecord import ListingRecord, read_listings

# --- Define Sensible Default Values for Benchmarks ---
# These can be adjusted as needed for your standard benchmark scenarios
DEFAULT_MIN_LISTINGS = 10
DEFAULT_MAX_PRICE = 250000.0
DEFAULT_BROKER_ID = "22611.00"  # Example, ensure this ID is relevant to your test data
DEFAULT_PERCENT_DELTA = 0.05 # 5% increase
DEFAULT_LIMIT = 100
DEFAULT_POSTAL_CODE = "33993" # Example, use a relevant postal code from your test data
DEFAULT_BELOW_AVG_PCT = 0.10 # 10% below average
DEFAULT_CITY = "Matlacha"      # Example, use a relevant city from your test data
DEFAULT_MIN_BEDROOMS = 2
DEFAULT_MAX_SIZE_SQM = 150.0
DEFAULT_BATCH_SIZE = 20000
# Path to your default data file for bulk import, accessible by adapters
# This could also be configured elsewhere or passed to adapters upon instantiation if needed.
# For simplicity here, adapters implementing usecase7 will need to know where to find data if 'data' is None.
DEFAULT_DATA_FILE_PATH_FOR_IMPORT = "transformed_real_estate_data.csv"

class Usecases(ABC):
    @abstractmethod
    def usecase1_filter_properties(
        self,
        min_listings: int = DEFAULT_MIN_LISTINGS,
        max_price: float = DEFAULT_MAX_PRICE
    ) -> List[Dict[str, Any]]:
        """
        Use Case 1:
        Städte mit mehr als `min_listings` Angeboten und einem Preis unter `max_price`.
        Rückgabe: Liste der passenden Immobilien-Dokumente.
        """
        ...

    @abstractmethod
    def usecase2_update_prices(
        self,
        broker_id: str = DEFAULT_BROKER_ID,
        percent_delta: float = DEFAULT_PERCENT_DELTA,
        limit: int = DEFAULT_LIMIT
    ) -> int:
        """
        Use Case 2:
        Preisänderung um `percent_delta` (z.B. 0.05 für +5%) für die
        ersten `limit` Immobilien desselben Brokers (`broker_id`).
        Rückgabe: Anzahl der aktualisierten Dokumente.
        """
        ...

    @abstractmethod
    def usecase3_add_solar_panels(self) -> int:
        """
        Use Case 3: (Already compatible)
        Neues Boolean-Feld `solar_panels` in allen Dokumenten anlegen
        und mit Zufallswerten befüllen.
        Rückgabe: Anzahl der bearbeiteten Dokumente.
        """
        ...

    @abstractmethod
    def usecase4_price_analysis(
        self,
        postal_code: str = DEFAULT_POSTAL_CODE,
        below_avg_pct: float = DEFAULT_BELOW_AVG_PCT,
        city: str = DEFAULT_CITY
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Use Case 4:
        1. Finde alle Immobilien in `postal_code`, die mindestens
           `below_avg_pct` (z.B. 0.20 für 20%) unter dem
           lokalen Durchschnittspreis/Flächeneinheit liegen.
        2. Sortiere alle Immobilien in `city` nach dem günstigsten Preis.
        Rückgabe: Dictionary mit zwei Listen:
            {
              "below_threshold": [...],
              "sorted_by_city": [...]
            }
        """
        ...

    @abstractmethod
    def usecase5_average_price_per_city(self) -> Dict[str, float]:
        """
        Use Case 5: (Already compatible)
        Berechne den Durchschnittspreis pro Flächeneinheit
        für jede Stadt.
        Rückgabe: Mapping Stadt → Durchschnittspreis.
        """
        ...

    @abstractmethod
    def usecase6_filter_by_bedrooms_and_size(
        self,
        min_bedrooms: int = DEFAULT_MIN_BEDROOMS,
        max_size: float = DEFAULT_MAX_SIZE_SQM
    ) -> List[Dict[str, Any]]:
        """
        Use Case 6:
        Finde alle Immobilien mit mehr als `min_bedrooms` Schlafzimmern
        und einer Größe von weniger als `max_size`.
        Rückgabe: Liste der passenden Dokumente.
        """
        ...

    @abstractmethod
    def usecase7_bulk_import(
        self,
        data: Iterable[ListingRecord] = read_listings(DEFAULT_DATA_FILE_PATH_FOR_IMPORT),
        batch_size: int = DEFAULT_BATCH_SIZE
    ) -> None:
        """
        Use Case 7:
        Batch-Import aller Immobilien-Daten (`data`).
        If `data` is None, the implementation should attempt to load
        a default dataset (e.g., from DEFAULT_DATA_FILE_PATH_FOR_IMPORT).
        """
        ...

    @abstractmethod
    def reset_database(self) -> None:
        """
        Use Case: (Already compatible)
        Lösche alle Einträge in der Datenbank.
        """
        ...
