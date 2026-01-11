-- 1. Main fact table 
CREATE TABLE IF NOT EXISTS app_user_visits_fact
(
    id String,
    
    -- User information
    phone_number Nullable(String),
    customer_id String,
    
    -- Status flags 
    seen UInt8 DEFAULT 0,          
    state Nullable(UInt8),          
    expired UInt8 DEFAULT 0,        
    is_deleted UInt8 DEFAULT 0,     
    is_fraud UInt8 DEFAULT 0,       
    -- Monetary values (DECIMAL for exact loyalty points)
    points Nullable(Decimal(10, 2)),
    receipt Nullable(Decimal(10, 2)),
    remaining Nullable(Decimal(10, 2)),
    
    -- code
    country_code Nullable(String),
    
    -- Business relationships
    branch_id String,
    store_id String,
    cashier_id String,
    order_id Nullable(String),
    
    -- Timestamps (millisecond precision)
    created_at DateTime64(3),
    updated_at DateTime64(3),
    expires_at Nullable(DateTime64(3)),
    
    -- Metadata
    sync_mechanism Nullable(String),
    is_bulk_points Nullable(String),
    
    -- Pipeline tracking
    _ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
-- Optimized for store performance + customer analysis
ORDER BY (store_id, customer_id, created_at, id);

-- 2. ETL checkpoint table (for incremental loading)
CREATE TABLE IF NOT EXISTS etl_checkpoint
(
    job_name String,
    last_processed_timestamp DateTime64(3),
    rows_processed UInt32 DEFAULT 0,
    status String DEFAULT 'success',
    error_message Nullable(String),
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (job_name);
