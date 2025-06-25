# Database Performance Benchmark Project

This project implements a comprehensive performance comparison between three different database systems using real estate data: **PostgreSQL**, **MongoDB**, and **ClickHouse**.

## Overview

We tested 3 database systems across 7 different use cases to evaluate their performance characteristics for various types of operations including filtering, aggregation, updates, schema modifications, and bulk data import.

## Database Systems

### 1. PostgreSQL (`postgres.py`)
- **Type**: Relational Database (RDBMS)
- **Version**: 17.5-alpine3.21
- **Implementation**: Normalized schema with separate tables for brokers, states, cities, zip codes, listings, estate details, land data, and addresses
- **Key Features**: ACID compliance, complex joins, foreign key constraints, indexes

### 2. MongoDB (`mongodb.py`)
- **Type**: Document Database (NoSQL)
- **Version**: 8.0.9
- **Implementation**: Denormalized document structure with embedded subdocuments
- **Key Features**: Flexible schema, aggregation pipelines, automatic sharding capabilities

### 3. ClickHouse (`clickhouse.py`)
- **Type**: Columnar Database (OLAP)
- **Version**: 25.4.2.31-alpine
- **Implementation**: Single wide table optimized for analytical queries
- **Key Features**: Columnar storage, high compression, optimized for analytical workloads

## Infrastructure

The databases are containerized using Docker Compose for consistent testing environments:

```bash
docker-compose up -d
```


## Use Cases Tested

### 1. Filter Properties (`usecase1_filter_properties`)
**Objective**: Find cities with more than a specified number of listings and properties under a maximum price
- Tests complex filtering and aggregation capabilities
- Evaluates join performance (PostgreSQL) vs. aggregation pipelines (MongoDB) vs. analytical queries (ClickHouse)

### 2. Update Prices (`usecase2_update_prices`)
**Objective**: Update prices by a percentage for a limited number of properties from the same broker
- Tests update performance and transaction handling
- Evaluates ACID compliance and concurrent modification capabilities

### 3. Add Solar Panels (`usecase3_add_solar_panels`)
**Objective**: Add a new boolean field to all records with random values
- Tests schema modification and bulk update capabilities
- Evaluates ALTER TABLE performance vs. document updates vs. columnar modifications

### 4. Price Analysis (`usecase4_price_analysis`)
**Objective**: 
- Find properties below average price per square meter in a postal code
- Sort all properties in a city by price
- Tests complex analytical queries and statistical operations

### 5. Average Price Per City (`usecase5_average_price_per_city`)
**Objective**: Calculate average price per square meter for each city
- Tests aggregation performance across large datasets
- Evaluates GROUP BY operations and mathematical computations

### 6. Filter by Bedrooms and Size (`usecase6_filter_by_bedrooms_and_size`)
**Objective**: Find properties with more than a minimum number of bedrooms and less than maximum size
- Tests multi-field filtering and range queries
- Evaluates index usage and query optimization

### 7. Bulk Import (`usecase7_bulk_import`)
**Objective**: Import large datasets efficiently using batch processing
- Tests bulk insert performance and data ingestion capabilities
- Evaluates batch processing strategies and memory management

## Project Structure

```
├── postgres.py          # PostgreSQL adapter implementation
├── mongodb.py           # MongoDB adapter implementation  
├── clickhouse.py        # ClickHouse adapter implementation
├── usecases.py          # Abstract base class defining all use cases
├── ListingRecord.py     # Data model and CSV reading utilities
├── data_conversion.py   # Data preprocessing and transformation
├── docker-compose.yaml  # Database containerization setup
├── benchmark.py         # Performance testing framework
├── requirements.txt     # Python dependencies
└── mdbs_visualization/  # Results analysis and visualization
    ├── Files/           # Raw benchmark results (JSONL format)
    ├── Files_Usecase_7/ # Usecase 7 specific results
    └── visu_script/     # Visualization and analysis scripts
        ├── visu.ipynb   # Jupyter notebook for result visualization
        ├── parse_files.py # Results parsing utilities
        └── svgs/        # Generated visualization outputs
```

## Data Model

This project uses the [USA Real Estate Dataset](https://www.kaggle.com/datasets/ahmedshahriarsakib/usa-real-estate-dataset) provided by **Ahmed Shahriar Sakib** on Kaggle

### Dataset Attributes
**Listing Information**: ID, broker, status, price, previous sold date
- **Property Details**: Bedrooms, bathrooms, house size
- **Location Data**: Street, city, state, zip code
- **Land Information**: Lot size in square meters

## Getting Started

1. **Start the databases**:
   ```bash
   docker-compose up -d
   ```

2. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Run individual use cases**:
   ```python
   from postgres import PostgresAdapter
   
   adapter = PostgresAdapter()
   adapter.usecase7_bulk_import()  # Import data
   results = adapter.usecase1_filter_properties()
   ```

4. **Run benchmarks**:
   ```bash
   python benchmark.py
   ```

5. **Analyze results**:
   Open `mdbs_visualization/visu_script/visu.ipynb` to visualize and analyze performance results.

