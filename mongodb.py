from typing import List, Dict, Any, Iterable, override
import pandas as pd
import pymongo
from pymongo import MongoClient, errors, ASCENDING
from datetime import datetime
import random

from ListingRecord import ListingRecord, read_listings
from usecases import Usecases, DEFAULT_MIN_LISTINGS, DEFAULT_MAX_PRICE, DEFAULT_BROKER_ID, DEFAULT_PERCENT_DELTA, \
    DEFAULT_LIMIT, DEFAULT_POSTAL_CODE, DEFAULT_BELOW_AVG_PCT, DEFAULT_CITY, DEFAULT_MIN_BEDROOMS, DEFAULT_MAX_SIZE_SQM, \
    DEFAULT_DATA_FILE_PATH_FOR_IMPORT, DEFAULT_BATCH_SIZE
# --- Configuration ---
MONGO_URI = "mongodb://localhost:27020"
DATABASE_NAME = "real_estate_db"
COLLECTION_NAME = "listings"
CSV_FILE_PATH = "transformed_real_estate_data.csv"
BATCH_SIZE = 1000


class MongoDbAdapter(Usecases):
    def __init__(self):
        """Initialize MongoDB connection and setup collection reference."""
        try:
            self.client = MongoClient(MONGO_URI)
            self.db = self.client[DATABASE_NAME]
            self.ensure_collection_exists(COLLECTION_NAME)
            self.collection = self.db[COLLECTION_NAME]
            # Test connection
            self.client.admin.command('ping')
        except errors.ConnectionFailure as e:
            raise ConnectionError(f"Failed to connect to MongoDB: {e}")
        except Exception as e:
            raise RuntimeError(f"Failed to initialize MongoDB connection: {e}")

    def __del__(self):
        """Clean up MongoDB connection."""
        if hasattr(self, 'client'):
            self.client.close()

    def ensure_collection_exists(self, collection_name: str) -> None:
        """Ensure that a collection exists; create if not."""
        try:
            if collection_name not in self.db.list_collection_names():
                # Optional: You can set options here, e.g., capped, max size, etc.
                self.db.create_collection(collection_name)
                print(f"Collection '{collection_name}' created.")
            else:
                print(f"Collection '{collection_name}' already exists.")
        except Exception as e:
            print(f"Error ensuring collection exists: {e}")

    def usecase1_filter_properties(
        self,
        min_listings: int = DEFAULT_MIN_LISTINGS,
        max_price: float = DEFAULT_MAX_PRICE
    ) -> List[Dict[str, Any]]:
        """
        Use Case 1: Find cities with more than min_listings properties and price under max_price.
        Returns: List of matching property documents.
        """
        try:
            # First, find cities that have more than min_listings properties
            city_counts = self.collection.aggregate([
                {
                    "$group": {
                        "_id": "$address.city",
                        "count": {"$sum": 1}
                    }
                },
                {
                    "$match": {
                        "count": {"$gt": min_listings},
                        "_id": {"$ne": None}  # Exclude null cities
                    }
                }
            ])

            # Extract qualifying cities
            qualifying_cities = [doc["_id"] for doc in city_counts]

            if not qualifying_cities:
                return []

            # Find all properties in qualifying cities with price under max_price
            query = {
                "address.city": {"$in": qualifying_cities},
                "price": {"$lt": max_price, "$ne": None}
            }

            return list(self.collection.find(query))

        except Exception as e:
            print(f"Error in usecase1_filter_properties: {e}")
            return []

    def usecase2_update_prices(
        self,
        broker_id: str = DEFAULT_BROKER_ID,
        percent_delta: float = DEFAULT_PERCENT_DELTA,
        limit: int = DEFAULT_LIMIT
    ) -> int:
        """
        Use Case 2: Update prices by percent_delta for first limit properties of the same broker.
        Returns: Number of updated documents.
        """
        try:
            # Convert broker_id to int since brokered_by is stored as int
            broker_id_int = int(float(broker_id))

            # Find properties for the specific broker, sorted by _id for consistency
            properties = self.collection.find(
                {"brokered_by": broker_id_int, "price": {"$ne": None}}
            ).sort("_id", 1).limit(limit)

            property_ids = [prop["_id"] for prop in properties]

            if not property_ids:
                return 0

            # Update prices using aggregation pipeline
            multiplier = 1 + percent_delta
            update_result = self.collection.update_many(
                {"_id": {"$in": property_ids}},
                [{"$set": {"price": {"$multiply": ["$price", multiplier]}}}]
            )

            return update_result.modified_count

        except (ValueError, TypeError) as e:
            print(f"Invalid broker_id format: {e}")
            return 0
        except Exception as e:
            print(f"Error in usecase2_update_prices: {e}")
            return 0

    def usecase3_add_solar_panels(self) -> int:
        """
        Use Case 3: Add solar_panels boolean field to all documents with random values.
        Returns: Number of processed documents.
        """
        try:
            # Get all document IDs
            all_docs = self.collection.find({}, {"_id": 1})
            doc_ids = [doc["_id"] for doc in all_docs]

            if not doc_ids:
                return 0

            # Update documents in batches to avoid memory issues
            batch_size = 1000
            total_updated = 0

            for i in range(0, len(doc_ids), batch_size):
                batch_ids = doc_ids[i:i + batch_size]

                # Generate random boolean values for this batch
                bulk_ops = []
                for doc_id in batch_ids:
                    bulk_ops.append({
                        "updateOne": {
                            "filter": {"_id": doc_id},
                            "update": {"$set": {"solar_panels": random.choice([True, False])}}
                        }
                    })

                if bulk_ops:
                    result = self.collection.bulk_write(bulk_ops)
                    total_updated += result.modified_count

            return total_updated

        except Exception as e:
            print(f"Error in usecase3_add_solar_panels: {e}")
            return 0

    def usecase4_price_analysis(
        self,
        postal_code: str = DEFAULT_POSTAL_CODE,
        below_avg_pct: float = DEFAULT_BELOW_AVG_PCT,
        city: str = DEFAULT_CITY
    ) -> Dict[str, List[Dict[str, Any]]]:

        """
        Use Case 4:
        1. Find properties in postal_code that are below_avg_pct under local avg price per sqm
        2. Sort all properties in city by cheapest price
        Returns: Dictionary with "below_threshold" and "sorted_by_city" lists
        """
        try:
            postal_code_int = int(postal_code)
            result = {"below_threshold": [], "sorted_by_city": []}

            # Part 1: Find properties below threshold in postal code
            # First calculate average price per sqm for the postal code
            avg_pipeline = [
                {
                    "$match": {
                        "address.zip_code": postal_code_int,
                        "price": {"$ne": None, "$gt": 0},
                        "land_data.area_size_in_square_m": {"$ne": None, "$gt": 0}
                    }
                },
                {
                    "$addFields": {
                        "price_per_sqm": {
                            "$divide": ["$price", "$land_data.area_size_in_square_m"]
                        }
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "avg_price_per_sqm": {"$avg": "$price_per_sqm"}
                    }
                }
            ]

            avg_result = list(self.collection.aggregate(avg_pipeline))

            if avg_result:
                avg_price_per_sqm = avg_result[0]["avg_price_per_sqm"]
                threshold = avg_price_per_sqm * (1 - below_avg_pct)

                # Find properties below threshold
                below_threshold_pipeline = [
                    {
                        "$match": {
                            "address.zip_code": postal_code_int,
                            "price": {"$ne": None, "$gt": 0},
                            "land_data.area_size_in_square_m": {"$ne": None, "$gt": 0}
                        }
                    },
                    {
                        "$addFields": {
                            "price_per_sqm": {
                                "$divide": ["$price", "$land_data.area_size_in_square_m"]
                            }
                        }
                    },
                    {
                        "$match": {
                            "price_per_sqm": {"$lt": threshold}
                        }
                    }
                ]

                result["below_threshold"] = list(self.collection.aggregate(below_threshold_pipeline))

            # Part 2: Sort all properties in city by price
            city_properties = self.collection.find(
                {
                    "address.city": city,
                    "price": {"$ne": None, "$gt": 0}
                }
            ).sort("price", 1)  # 1 for ascending (cheapest first)

            result["sorted_by_city"] = list(city_properties)

            return result

        except (ValueError, TypeError) as e:
            print(f"Invalid postal_code format: {e}")
            return {"below_threshold": [], "sorted_by_city": []}
        except Exception as e:
            print(f"Error in usecase4_price_analysis: {e}")
            return {"below_threshold": [], "sorted_by_city": []}

    def usecase5_average_price_per_city(self) -> Dict[str, float]:
        """
        Use Case 5: Calculate average price per square meter for each city.
        Returns: Mapping of city → average price per sqm.
        """
        try:
            pipeline = [
                {
                    "$match": {
                        "address.city": {"$ne": None},
                        "price": {"$ne": None, "$gt": 0},
                        "land_data.area_size_in_square_m": {"$ne": None, "$gt": 0}
                    }
                },
                {
                    "$addFields": {
                        "price_per_sqm": {
                            "$divide": ["$price", "$land_data.area_size_in_square_m"]
                        }
                    }
                },
                {
                    "$group": {
                        "_id": "$address.city",
                        "avg_price_per_sqm": {"$avg": "$price_per_sqm"}
                    }
                },
                {
                    "$sort": {"_id": 1}
                }
            ]

            results = self.collection.aggregate(pipeline)
            return {doc["_id"]: doc["avg_price_per_sqm"] for doc in results}

        except Exception as e:
            print(f"Error in usecase5_average_price_per_city: {e}")
            return {}

    def usecase6_filter_by_bedrooms_and_size(
        self,
        min_bedrooms: int = DEFAULT_MIN_BEDROOMS,
        max_size: float = DEFAULT_MAX_SIZE_SQM
    ) -> List[Dict[str, Any]]:
        """
        Use Case 6: Find properties with more than min_bedrooms and size less than max_size.
        Returns: List of matching documents.
        """
        try:
            query = {
                "estate_details.bed": {"$gt": min_bedrooms, "$ne": None},
                "estate_details.house_size": {"$lt": max_size, "$ne": None}
            }

            return list(self.collection.find(query))

        except Exception as e:
            print(f"Error in usecase6_filter_by_bedrooms_and_size: {e}")
            return []
    @override
    def usecase7_bulk_import(
        self,
        data: Iterable[ListingRecord] = read_listings(DEFAULT_DATA_FILE_PATH_FOR_IMPORT),
        batch_size: int = DEFAULT_BATCH_SIZE
    ) -> None:
        """
        Use Case 7: Batch import all real estate data from ListingRecord objects.
        Handles both single inserts and bulk operations for performance comparison.
        """
        try:
            # Convert ListingRecord objects to MongoDB documents in batches
            batch = []
            total_processed = 0
            total_inserted = 0
            batch_count = 0
            self.create_indexes()

            for listing_record in data:
                try:
                    # Convert ListingRecord to MongoDB document format
                    doc = self._listing_record_to_document(listing_record)
                    batch.append(doc)

                    # Process batch when it reaches batch_size
                    if len(batch) >= batch_size:
                        batch_count += 1
                        inserted_count = self._insert_batch(batch, batch_count)
                        total_inserted += inserted_count
                        total_processed += len(batch)
                        batch = []  # Reset batch

                except Exception as e:
                    print(f"Error converting ListingRecord to document: {e}")
                    continue

            # Process remaining records in the last batch
            if batch:
                batch_count += 1
                inserted_count = self._insert_batch(batch, batch_count)
                total_inserted += inserted_count
                total_processed += len(batch)

            print(
                f"Batch import completed: {total_inserted}/{total_processed} documents successfully inserted across {batch_count} batches")

        except Exception as e:
            print(f"Error in usecase7_batch_import: {e}")

    def _listing_record_to_document(self, listing_record: ListingRecord) -> Dict[str, Any]:
        """Convert a ListingRecord to MongoDB document format matching the schema."""
        doc = {
            # Top-level fields
            "brokered_by": int(listing_record.brokered_by) if listing_record.brokered_by is not None else None,
            "status": listing_record.status,
            "price": listing_record.price,
            "prev_sold_date": listing_record.prev_sold_date,

            # Embedded estate_details
            "estate_details": {
                "bed": int(listing_record.bed) if listing_record.bed is not None else None,
                "bath": int(listing_record.bath) if listing_record.bath is not None else None,
                "house_size": listing_record.house_size_sqm
            },

            # Embedded land_data
            "land_data": {
                "area_size_in_square_m": listing_record.lot_size_sqm
            },

            # Embedded address
            "address": {
                "street": listing_record.street,
                "zip_code": listing_record.zip_code,
                "city": listing_record.city,
                "state": listing_record.state
            }
        }
        return doc

    def _insert_batch(self, batch: List[Dict[str, Any]], batch_number: int) -> int:
        """Insert a batch of documents with error handling."""
        try:
            # Option 1: Bulk insert (recommended for large datasets)
            result = self.collection.insert_many(batch, ordered=False)
            inserted_count = len(result.inserted_ids)
            print(f"Batch {batch_number}: Bulk insert successful - {inserted_count} documents inserted")
            return inserted_count

        except errors.BulkWriteError as bwe:
            successful_inserts = bwe.details.get('nInserted', 0)
            error_count = len(bwe.details.get('writeErrors', []))
            print(
                f"Batch {batch_number}: Bulk insert partial success - {successful_inserts} inserted, {error_count} errors")
            return successful_inserts

        except Exception as e:
            print(f"Batch {batch_number}: Bulk insert failed ({e}), falling back to individual inserts")
            # Fallback to individual inserts
            successful_inserts = 0
            for i, doc in enumerate(batch):
                try:
                    self.collection.insert_one(doc)
                    successful_inserts += 1
                except Exception as insert_error:
                    print(f"Batch {batch_number}, document {i + 1}: Individual insert failed - {insert_error}")

            print(
                f"Batch {batch_number}: Individual inserts completed - {successful_inserts}/{len(batch)} documents inserted")
            return successful_inserts
    def reset_database(self) -> None:
        """
        Delete all entries in the database.
        """
        try:
            self.collection.drop()
            print(f"Deleted All documents from the database")
        except Exception as e:
            print(f"Error in reset_database: {e}")

    # Additional utility methods for database management
    def get_total_count(self) -> int:
        """Get total number of documents in the collection."""
        try:
            return self.collection.count_documents({})
        except Exception as e:
            print(f"Error getting total count: {e}")
            return 0

    def create_indexes(self) -> None:
        """Create recommended indexes for better query performance."""
        try:
            indexes = [
                ("brokered_by", ASCENDING),
                ("price", ASCENDING),
                ("status", ASCENDING),
                ("estate_details.bed", ASCENDING),
                ("estate_details.house_size", ASCENDING),
                ("land_data.area_size_in_square_m", ASCENDING),
                ("address.zip_code", ASCENDING),
                ("address.city", ASCENDING),
                ("address.state", ASCENDING),
                (("address.city", ASCENDING), ("price", ASCENDING)),  # Compound index
                (("estate_details.bed", ASCENDING), ("estate_details.house_size", ASCENDING))  # Compound index
            ]


            for index in indexes:
                if isinstance(index, tuple) and len(index) == 2 and isinstance(index[1], int):
                    # Single field index
                    self.collection.create_index([(index[0], index[1])])
                elif isinstance(index, tuple):
                    # Compound index
                    self.collection.create_index(list(index))

            print("Indexes created successfully")
        except Exception as e:
            print(f"Error creating indexes: {e}")

    def close (self) -> None:
        """Close the MongoDB connection."""
        if hasattr(self, 'client'):
            self.client.close()
            print("MongoDB connection closed.")
        else:
            print("No MongoDB client to close.")
