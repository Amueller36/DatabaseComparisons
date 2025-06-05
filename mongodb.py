import logging
from typing import List, Dict, Any, Iterable, Optional  # Added Optional
import pandas as pd
import pymongo
from pymongo import MongoClient, errors, ASCENDING
from datetime import datetime
import random

from usecases import *

# --- Configuration ---
# MONGO_URI = "mongodb://localhost:27020"
MONGO_URI = "mongodb://152.53.248.27:27020"  # Make sure this is accessible
DATABASE_NAME = "real_estate_db"
COLLECTION_NAME = "listings"  # Using a new collection name to avoid conflicts
CSV_FILE_PATH = "transformed_real_estate_data.csv"
# BATCH_SIZE = 1000 # Defined in usecase7 parameters

logger = logging.getLogger(__name__)
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


class MongoDbAdapter(Usecases):
    def __init__(self):
        """Initialize MongoDB connection and setup collection reference."""
        try:
            self.client = MongoClient(MONGO_URI)
            self.db = self.client[DATABASE_NAME]
            # Test connection before ensuring collection (to fail fast if DB is down)
            self.client.admin.command('ping')
            logger.info(f"Successfully connected to MongoDB: {MONGO_URI}")
            self.ensure_collection_exists(COLLECTION_NAME)
            self.collection = self.db[COLLECTION_NAME]
        except errors.ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise ConnectionError(f"Failed to connect to MongoDB: {e}")
        except Exception as e:
            logger.error(f"Failed to initialize MongoDB connection: {e}")
            raise RuntimeError(f"Failed to initialize MongoDB connection: {e}")

    def __del__(self):
        """Clean up MongoDB connection."""
        if hasattr(self, 'client') and self.client:
            self.client.close()
            logger.info("MongoDB connection closed in __del__.")

    def ensure_collection_exists(self, collection_name: str) -> None:
        """Ensure that a collection exists; create if not."""
        try:
            if collection_name not in self.db.list_collection_names():
                self.db.create_collection(collection_name)
                logger.info(f"Collection '{collection_name}' created.")
            else:
                logger.info(f"Collection '{collection_name}' already exists.")
        except Exception as e:
            logger.error(f"Error ensuring collection exists: {e}")
            # Decide if this should raise an error or just log
            raise

    def usecase1_filter_properties(
            self,
            min_listings: int = DEFAULT_MIN_LISTINGS,
            max_price: float = DEFAULT_MAX_PRICE
    ) -> List[Dict[str, Any]]:
        """
        Use Case 1: Find city/state combinations with more than min_listings properties
        and price under max_price.
        Returns: List of matching property documents.
        """
        try:
            lookup_pipeline = [
                # Stage 1: Find qualifying city/state pairs (similar to your current first step)
                {
                    "$match": {
                        "address.city": {"$ne": None},
                        "address.state": {"$ne": None}
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "city": {"$toLower": "$address.city"},
                            "state": {"$toLower": "$address.state"}
                        },
                        "count": {"$sum": 1}
                    }
                },
                {
                    "$match": {
                        "count": {"$gt": min_listings}
                    }
                },
                # Stage 2: Use $lookup to find all original documents matching these city/state pairs
                # and also matching the price condition.
                {
                    "$lookup": {
                        "from": self.collection.name,  # Target the same collection
                        "let": {  # Define variables from the current pipeline document (the qualifying city/state)
                            "qual_city": "$_id.city",
                            "qual_state": "$_id.state"
                        },
                        "pipeline": [  # Sub-pipeline to run on the 'from' collection for each input document
                            {
                                "$match": {
                                    "$expr": {  # $expr allows use of aggregation expressions in $match
                                        "$and": [
                                            # Match city (case-insensitive using stored lowercase)
                                            {"$eq": ["$address.city", "$$qual_city"]},
                                            # Assumes address.city is stored as lowercase
                                            # Match state (case-insensitive using stored lowercase)
                                            {"$eq": ["$address.state", "$$qual_state"]},
                                            # Assumes address.state is stored as lowercase
                                            # Match price
                                            {"$lt": ["$price", max_price]},
                                            {"$ne": ["$price", None]}
                                        ]
                                    }
                                }
                            }
                            # Optionally, you can add a $project stage here if you only need specific fields
                        ],
                        "as": "matching_listings_for_city_state"  # Name of the new array field
                    }
                },
                # Stage 3: Unwind the array of listings
                {
                    "$unwind": "$matching_listings_for_city_state"
                },
                # Stage 4: Promote the actual listing document to the root
                {
                    "$replaceRoot": {"newRoot": "$matching_listings_for_city_state"}
                }
            ]
            return list(self.collection.aggregate(lookup_pipeline))

        except Exception as e:
            logger.error(f"Error in usecase1_filter_properties: {e}")
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
            broker_id_int: Optional[int] = None
            try:
                broker_id_int = int(float(broker_id))
            except (ValueError, TypeError):
                logger.warning(f"Invalid broker_id format: '{broker_id}'. Must be a number.")
                return 0

            properties_cursor = self.collection.find(
                {"brokered_by": broker_id_int, "price": {"$ne": None}}  # Ensure price is not null before multiplying
            ).sort("_id", pymongo.ASCENDING).limit(limit)  # Use pymongo.ASCENDING

            property_ids = [prop["_id"] for prop in properties_cursor]

            if not property_ids:
                logger.info(f"No properties found for broker_id {broker_id_int} to update.")
                return 0

            multiplier = 1 + percent_delta
            if multiplier < 0:  # Prevent negative prices if percent_delta is too low
                logger.warning(
                    f"Attempting to set negative prices with percent_delta {percent_delta}. Aborting update for safety.")
                return 0

            update_result = self.collection.update_many(
                {"_id": {"$in": property_ids}},
                [{"$set": {"price": {"$multiply": ["$price", multiplier]}}}]
            )

            logger.info(f"Usecase 2: Updated {update_result.modified_count} documents for broker_id {broker_id_int}.")
            return update_result.modified_count

        except Exception as e:
            logger.error(f"Error in usecase2_update_prices: {e}")
            return 0

    def usecase3_add_solar_panels(self) -> int:
        """
        Use Case 3: Add solar_panels boolean field to all documents with random values.
        Returns: Number of documents where solar_panels was set or modified.
        """
        try:
            update_result = self.collection.update_many(
                {},
                [{"$set": {"solar_panels": {"$lte": [{"$rand": {}}, 0.5]}}}]  # Sets to true ~50% of the time
            )
            return update_result.modified_count  # Or matched_count depending on what you want to report
        except Exception as e:
            logger.error(f"Error in usecase3_add_solar_panels: {e}")
            return 0

    def usecase4_price_analysis(
            self,
            postal_code: str = DEFAULT_POSTAL_CODE,
            below_avg_pct: float = DEFAULT_BELOW_AVG_PCT,
            city: str = DEFAULT_CITY  # This city should be lowercase if data is stored as lowercase
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Use Case 4:
        1. Find properties in postal_code that are below_avg_pct under local avg price per sqm
        2. Sort all properties in the given city (case-insensitive) by cheapest price
        Returns: Dictionary with "below_threshold" and "sorted_by_city" lists
        """
        try:
            postal_code_int: Optional[int] = None
            try:
                postal_code_int = int(postal_code)
            except (ValueError, TypeError):
                logger.warning(f"Invalid postal_code format: '{postal_code}'. Must be an integer.")
                return {"below_threshold": [], "sorted_by_city": []}

            result = {"below_threshold": [], "sorted_by_city": []}

            # Part 1: Find properties below threshold in postal code
            avg_pipeline = [
                {
                    "$match": {
                        "address.zip_code": postal_code_int,
                        "price": {"$ne": None, "$gt": 0},
                        "land_data.area_size_in_square_m": {"$ne": None, "$gt": 0}
                    }
                },
                {
                    "$project": {  # Use $project to ensure division is safe
                        "price_per_sqm": {
                            "$cond": [
                                {"$eq": ["$land_data.area_size_in_square_m", 0]},  # Avoid division by zero
                                None,  # Or handle as error/specific value
                                {"$divide": ["$price", "$land_data.area_size_in_square_m"]}
                            ]
                        }
                    }
                },
                {
                    "$match": {"price_per_sqm": {"$ne": None}}  # Filter out results from division by zero
                },
                {
                    "$group": {
                        "_id": None,  # Group all matching documents
                        "avg_price_per_sqm": {"$avg": "$price_per_sqm"}
                    }
                }
            ]
            avg_result_cursor = self.collection.aggregate(avg_pipeline)
            avg_data = list(avg_result_cursor)  # Convert cursor to list

            if avg_data and avg_data[0].get("avg_price_per_sqm") is not None:
                avg_price_per_sqm = avg_data[0]["avg_price_per_sqm"]
                threshold = avg_price_per_sqm * (1 - below_avg_pct)

                below_threshold_pipeline = [
                    {
                        "$match": {
                            "address.zip_code": postal_code_int,
                            "price": {"$ne": None, "$gt": 0},
                            "land_data.area_size_in_square_m": {"$ne": None, "$gt": 0}
                            # Ensure area_size_in_square_m is not zero before division
                        }
                    },
                    {
                        "$addFields": {  # Using $addFields for clarity, ensure no division by zero
                            "price_per_sqm_calc": {
                                "$cond": {
                                    "if": {"$gt": ["$land_data.area_size_in_square_m", 0]},
                                    "then": {"$divide": ["$price", "$land_data.area_size_in_square_m"]},
                                    "else": None  # Or some indicator for invalid data
                                }
                            }
                        }
                    },
                    {
                        "$match": {
                            "price_per_sqm_calc": {"$ne": None, "$lt": threshold}
                        }
                    }
                ]
                result["below_threshold"] = list(self.collection.aggregate(below_threshold_pipeline))
            else:
                logger.info(f"Usecase 4: No average price per sqm found for postal code {postal_code_int}.")

            # Part 2: Sort all properties in city by price (case-insensitive city match)
            # Assumes 'city' parameter is the non-normalized version, we convert it to lowercase for matching
            # if cities are stored as lowercase.
            normalized_city_param = city.lower()  # Normalize input parameter

            city_properties_cursor = self.collection.find(
                {
                    "address.city": normalized_city_param,  # Match against normalized city field
                    "price": {"$ne": None, "$gt": 0}  # Ensure price is valid for sorting
                }
            ).sort("price", pymongo.ASCENDING)

            result["sorted_by_city"] = list(city_properties_cursor)

            logger.info(
                f"Usecase 4: Found {len(result['below_threshold'])} below threshold, {len(result['sorted_by_city'])} sorted by city '{city}'.")
            return result

        except Exception as e:
            logger.error(f"Error in usecase4_price_analysis: {e}")
            return {"below_threshold": [], "sorted_by_city": []}

    def usecase5_average_price_per_city(self) -> Dict[str, float]:
        """
        Use Case 5: Calculate average price per square meter for each city/state combination.
        Returns: Mapping of "city, state" -> average price per sqm.
        """
        try:
            pipeline = [
                {
                    "$match": {
                        "address.city": {"$ne": None},
                        "address.state": {"$ne": None},
                        "price": {"$ne": None, "$gt": 0},
                        "land_data.area_size_in_square_m": {"$ne": None, "$gt": 0}
                        # Crucial: ensure not zero for division
                    }
                },
                {
                    "$addFields": {  # Safely calculate price_per_sqm
                        "price_per_sqm": {
                            "$cond": {
                                "if": {"$gt": ["$land_data.area_size_in_square_m", 0]},
                                "then": {"$divide": ["$price", "$land_data.area_size_in_square_m"]},
                                "else": None  # Will be filtered out if null
                            }
                        }
                    }
                },
                {
                    "$match": {"price_per_sqm": {"$ne": None}}  # Ensure price_per_sqm is valid after calculation
                },
                {
                    "$group": {
                        "_id": {  # Group by normalized city and state
                            "city": {"$toLower": "$address.city"},  # Use $toLower if not already stored as lowercase
                            "state": {"$toLower": "$address.state"}
                        },
                        "avg_price_per_sqm": {"$avg": "$price_per_sqm"}
                    }
                },
                {  # Sort by the compound _id for consistent output
                    "$sort": {"_id.state": 1, "_id.city": 1}
                }
            ]

            results_cursor = self.collection.aggregate(pipeline)
            # Format output key as "city, state"
            output_dict = {
                f"{doc['_id']['city']}, {doc['_id']['state']}": doc["avg_price_per_sqm"]
                for doc in results_cursor if
                doc['_id'] and doc['_id']['city'] and doc['_id']['state'] and doc.get("avg_price_per_sqm") is not None
            }
            logger.info(f"Usecase 5: Calculated average prices for {len(output_dict)} city/state combinations.")
            return output_dict

        except Exception as e:
            logger.error(f"Error in usecase5_average_price_per_city: {e}")
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
                "estate_details.bed": {"$gt": min_bedrooms, "$ne": None},  # Ensure 'bed' is not null
                "estate_details.house_size": {"$lt": max_size, "$ne": None}  # Ensure 'house_size' is not null
            }
            results = list(self.collection.find(query))
            logger.info(f"Usecase 6: Found {len(results)} properties matching bedrooms/size criteria.")
            return results
        except Exception as e:
            logger.error(f"Error in usecase6_filter_by_bedrooms_and_size: {e}")
            return []

    def usecase7_bulk_import(
            self,
            data: Iterable[ListingRecord] = read_listings(DEFAULT_DATA_FILE_PATH_FOR_IMPORT),
            batch_size: int = DEFAULT_BATCH_SIZE  # Parameter from Usecases
    ) -> None:  # Changed return type to None as per original Usecases interface
        """
        Use Case 7: Batch import all real estate data from ListingRecord objects.
        """
        try:
            # Ensure indexes are created before import for potential performance benefits on large imports,
            # though for initial empty collection import, it might be faster to create after.
            # For consistency, creating them if they don't exist.
            self.create_indexes()

            batch: List[Dict[str, Any]] = []
            total_processed = 0  # Number of records read from source
            total_inserted_successfully = 0  # Number of records successfully inserted
            current_batch_number = 0

            for record_idx, listing_record in enumerate(data):
                total_processed += 1
                try:
                    doc = self._listing_record_to_document(listing_record)
                    batch.append(doc)

                    if len(batch) >= batch_size:
                        current_batch_number += 1
                        inserted_in_batch = self._insert_batch(batch, current_batch_number)
                        total_inserted_successfully += inserted_in_batch
                        logger.info(
                            f"Processed batch {current_batch_number}, {inserted_in_batch}/{len(batch)} inserted. Cumulative successful inserts: {total_inserted_successfully}")
                        batch = []

                except Exception as e:  # Error converting a single record
                    logger.error(
                        f"Error converting ListingRecord at index {record_idx} to document: {e}. Skipping this record.")
                    continue  # Skip this record and continue with the next

            # Process any remaining records in the last batch
            if batch:
                current_batch_number += 1
                inserted_in_batch = self._insert_batch(batch, current_batch_number)
                total_inserted_successfully += inserted_in_batch
                logger.info(
                    f"Processed final batch {current_batch_number}, {inserted_in_batch}/{len(batch)} inserted. Cumulative successful inserts: {total_inserted_successfully}")

            logger.info(
                f"Bulk import completed. Total records from source: {total_processed}. "
                f"Total successfully inserted: {total_inserted_successfully} across {current_batch_number} batches."
            )
            # Original Usecases interface might expect an int return (e.g. count of inserted).
            # Adjusted to return None to match the override, but logging provides counts.

        except Exception as e:
            logger.error(f"Error in usecase7_bulk_import: {e}")
            # Consider if this should re-raise or just log

    def _listing_record_to_document(self, listing_record: ListingRecord) -> Dict[str, Any]:
        """Convert a ListingRecord to MongoDB document format, normalizing city/state to lowercase."""
        city_normalized = listing_record.city.lower() if listing_record.city else None
        state_normalized = listing_record.state.lower() if listing_record.state else None

        doc = {
            "brokered_by": int(listing_record.brokered_by) if listing_record.brokered_by is not None else None,
            "status": listing_record.status,
            "price": listing_record.price,  # Assumes price is float or None
            "prev_sold_date": listing_record.prev_sold_date if isinstance(listing_record.prev_sold_date,
                                                                          datetime) else None,
            "estate_details": {
                "bed": int(listing_record.bed) if listing_record.bed is not None else None,
                "bath": int(listing_record.bath) if listing_record.bath is not None else None,
                "house_size": listing_record.house_size_sqm  # Assumes float or None
            },
            "land_data": {
                "area_size_in_square_m": listing_record.lot_size_sqm  # Assumes float or None
            },
            "address": {
                "street": listing_record.street,  # Assumes string or None
                "zip_code": int(listing_record.zip_code) if listing_record.zip_code is not None else None,
                "city": city_normalized,
                "state": state_normalized
            }
            # solar_panels field will be added by usecase3
        }
        # Clean None values from nested dicts if MongoDB version requires or for cleaner data
        # For example, if estate_details has all None, it could be omitted.
        # For simplicity, allowing None values for now.
        return doc

    def _insert_batch(self, batch: List[Dict[str, Any]], batch_number: int) -> int:
        """Insert a batch of documents with error handling. Returns count of successfully inserted documents."""
        if not batch:
            return 0
        try:
            result = self.collection.insert_many(batch,
                                                 ordered=False)  # ordered=False allows valid inserts to proceed if some fail
            inserted_count = len(result.inserted_ids)
            # logger.info(f"Batch {batch_number}: Inserted {inserted_count} documents.")
            return inserted_count
        except errors.BulkWriteError as bwe:
            # bwe.details['nInserted'] gives the count of successfully inserted documents
            # before an error occurred if ordered=True, or total successful if ordered=False
            successful_inserts = bwe.details.get('nInserted', 0)
            write_errors = bwe.details.get('writeErrors', [])
            error_count = len(write_errors)
            logger.warning(
                f"Batch {batch_number}: Bulk insert encountered errors. "
                f"Successfully inserted: {successful_inserts}. Errors: {error_count}."
            )
            # Example of logging first few errors:
            # for err_idx, err_detail in enumerate(write_errors[:3]):
            #    logger.debug(f"Batch {batch_number} - Error {err_idx}: Index {err_detail.get('index')}, Code {err_detail.get('code')}, Msg: {err_detail.get('errmsg')}")
            return successful_inserts
        except Exception as e:  # Catch other potential errors during insert_many
            logger.error(
                f"Batch {batch_number}: General error during bulk insert: {e}. No documents inserted in this batch attempt via insert_many.")
            return 0  # Assume none inserted if a general error occurs

    def reset_database(self) -> None:
        """Delete all documents in the collection, then recreate the collection and indexes."""
        try:
            if self.collection is not None:
                self.collection.drop()
                logger.info(f"Collection '{self.collection.name}' dropped.")
            else:  # Should not happen if constructor is correct
                logger.warning("Collection reference is None, cannot drop. Ensuring collection exists.")

            # Re-ensure collection exists (it will be created if dropped)
            self.ensure_collection_exists(COLLECTION_NAME)
            self.collection = self.db[COLLECTION_NAME]  # Re-assign collection object

            # Recreate indexes after resetting
            self.create_indexes()
            logger.info(f"Database collection '{COLLECTION_NAME}' has been reset and indexes recreated.")

        except Exception as e:
            logger.error(f"Error in reset_database: {e}")
            # Consider re-raising if this is critical path

    def get_total_count(self) -> int:
        """Get total number of documents in the collection."""
        try:
            if self.collection is None:
                logger.warning("Collection is None in get_total_count.")
                return 0
            return self.collection.count_documents({})
        except Exception as e:
            logger.error(f"Error getting total count: {e}")
            return 0

    def create_indexes(self) -> None:
        """Create recommended indexes for better query performance."""
        if self.collection is None:
            logger.error("Cannot create indexes: collection is None.")
            return
        try:
            # Corrected index definitions
            # Single field indexes
            self.collection.create_index([("brokered_by", pymongo.ASCENDING)], background=True)
            self.collection.create_index([("price", pymongo.ASCENDING)], background=True)
            self.collection.create_index([("status", pymongo.ASCENDING)], background=True)
            self.collection.create_index([("estate_details.bed", pymongo.ASCENDING)], background=True)
            self.collection.create_index([("estate_details.house_size", pymongo.ASCENDING)], background=True)
            self.collection.create_index([("land_data.area_size_in_square_m", pymongo.ASCENDING)], background=True)
            self.collection.create_index([("address.zip_code", pymongo.ASCENDING)], background=True)
            # Indexes for normalized city/state (assuming they are stored as lowercase)
            self.collection.create_index([("address.city", pymongo.ASCENDING)], background=True)  # For normalized city
            self.collection.create_index([("address.state", pymongo.ASCENDING)],
                                         background=True)  # For normalized state

            # Compound indexes
            self.collection.create_index([("address.city", pymongo.ASCENDING), ("address.state", pymongo.ASCENDING),
                                          ("price", pymongo.ASCENDING)], background=True)

            self.collection.create_index(
                [("estate_details.bed", pymongo.ASCENDING), ("estate_details.house_size", pymongo.ASCENDING)],
                background=True)

            logger.info(f"Indexes ensured for collection '{self.collection.name}'.")
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")

    def close(self) -> None:
        """Close the MongoDB connection."""
        if hasattr(self, 'client') and self.client:
            self.client.close()
            logger.info("MongoDB connection closed.")
        else:
            logger.info("No MongoDB client to close or already closed.")




if __name__ == '__main__':
    mongodb = MongoDbAdapter()
    mongodb.reset_database()
    mongodb.usecase7_bulk_import()  # Import data from the default file
    logger.info(f'Usecase 4: {len(mongodb.usecase4_price_analysis()["below_threshold"])}')
    logger.info(f'Usecase 4: {len(mongodb.usecase4_price_analysis()["sorted_by_city"])}')
    logger.info(f"Usecase 5: {len(mongodb.usecase5_average_price_per_city())}")


    logger.info(f"Usecase 1: {len(mongodb.usecase1_filter_properties())}")
    mongodb.close()
