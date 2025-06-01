from abc import ABC, abstractmethod
from typing import Any, Dict, List, Iterable

from ListingRecord import ListingRecord


class Usecases(ABC):
    @abstractmethod
    def usecase1_filter_properties(
        self,
        min_listings: int,
        max_price: float
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
        broker_id: str,
        percent_delta: float,
        limit: int
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
        Use Case 3:
        Neues Boolean-Feld `solar_panels` in allen Dokumenten anlegen
        und mit Zufallswerten befüllen.
        Rückgabe: Anzahl der bearbeiteten Dokumente.
        """
        ...

    @abstractmethod
    def usecase4_price_analysis(
        self,
        postal_code: str,
        below_avg_pct: float,
        city: str
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
        Use Case 5:
        Berechne den Durchschnittspreis pro Flächeneinheit
        für jede Stadt.
        Rückgabe: Mapping Stadt → Durchschnittspreis.
        """
        ...

    @abstractmethod
    def usecase6_filter_by_bedrooms_and_size(
        self,
        min_bedrooms: int,
        max_size: float
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
        data: Iterable[ListingRecord],
        batch_size: int = 1000
    ) -> None:
        """
        Use Case 7:
        Batch-Import aller Immobilien-Daten (`data`).
        (In der Implementierung kann man dann auch Einzeleinfügungen
        vs. Bulk-Insert messen.)
        """
        ...
    
    @abstractmethod
    def reset_database(self) -> None:
        """
        Lösche alle Einträge in der Datenbank.
        """
        ...
