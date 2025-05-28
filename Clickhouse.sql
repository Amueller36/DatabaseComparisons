CREATE TABLE listings_denormalized
(
    -- Listing Core Info (from listings table)
    listing_id UInt64,
    status LowCardinality(String),
    price Float64,
    prev_sold_date Date,

    -- Broker Info (denormalized from brokers table)
    broker_id UInt64,

    -- Estate Details (denormalized from estate_details, nullable due to 0..1 relationship)
    bed Nullable(UInt8),
    bath Nullable(UInt8),
    house_size Nullable(Float64),

    -- Land Data (denormalized from land_data, was 1..1 mandatory in PG)
    area_size_in_square_m Float64,

    -- Address Info (denormalized from addresses and zip_codes tables)
    street String,
    zip_code String,
    city LowCardinality(String),
    state LowCardinality(String),

    -- New attribute for Use Case 3
    solar_panels Nullable(UInt8),

    -- Versioning column for ReplacingMergeTree
    update_timestamp DateTime DEFAULT now(),

    -- Secondary indices
    INDEX idx_price price TYPE minmax GRANULARITY 4,
    INDEX idx_house_size house_size TYPE minmax GRANULARITY 4, -- Index is on Nullable(Float64)
    INDEX idx_area_size area_size_in_square_m TYPE minmax GRANULARITY 4,
    INDEX idx_bed bed TYPE set(0) GRANULARITY 1, -- Index is on Nullable(UInt8), 0 means no limit for set size (default is 1024)
    INDEX idx_broker_id broker_id TYPE set(0) GRANULARITY 1

)
ENGINE = ReplacingMergeTree(update_timestamp)
PARTITION BY tuple()
ORDER BY (city, zip_code, broker_id, listing_id);


