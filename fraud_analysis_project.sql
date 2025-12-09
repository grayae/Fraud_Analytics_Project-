-- create raw tables 
CREATE TABLE public.raw_customers (
    customer_id INT PRIMARY KEY,
    full_name TEXT,
    gender TEXT,
    age INT,
    state TEXT, 
    join_date DATE,
    kyc_level TEXT
);

CREATE TABLE raw_devices (
device_id TEXT PRIMARY KEY,
device_type TEXT, 
os TEXT,
first_seen DATE,
last_seen DATE
);

CREATE TABLE raw_merchants(
merchant_id INT PRIMARY KEY,
merchant_name TEXT,
category TEXT,
state TEXT,
risk_category TEXT);

CREATE TABLE raw_transactions(
transaction_id INT PRIMARY KEY,
customer_id INT,
merchant_id INT,
amount NUMERIC,
channel TEXT,
status TEXT,
txn_timestamp TIMESTAMP,
location_lat NUMERIC,
location_long NUMERIC,
device_id TEXT
);

SELECT * FROM raw_customers LIMIT 5;
SELECT * FROM raw_devices LIMIT 5;
SELECT * FROM raw_merchants LIMIT 5;
SELECT * FROM raw_transactions LIMIT 5;

-- create staging tables

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE staging.stg_customers AS
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY join_date DESC) AS rn
    FROM public.raw_customers
)
SELECT
    customer_id::int AS customer_id,
    INITCAP(REGEXP_REPLACE(TRIM(full_name), '\s+', ' ', 'g')) AS full_name,
    INITCAP(TRIM(gender)) AS gender,
    CASE WHEN age > 0 AND age < 100 THEN age::int ELSE NULL END AS age,
    INITCAP(state) AS state,
    join_date::date AS join_date,
    UPPER(kyc_level) AS kyc_level,
    EXTRACT(YEAR FROM join_date) AS join_year,
    EXTRACT(MONTH FROM join_date) AS join_month
FROM ranked 
WHERE rn = 1;

CREATE TABLE staging.stg_devices AS
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY last_seen DESC) AS rn
    FROM public.raw_devices
),
cleaned AS (
    SELECT
        device_id,
        device_type,
        os,
        first_seen,
        CASE 
            WHEN last_seen >= first_seen THEN last_seen 
            ELSE first_seen 
        END AS fixed_last_seen
    FROM ranked
    WHERE rn = 1
)
SELECT
    device_id::text AS device_id,
    INITCAP(TRIM(device_type)) AS device_type,
    INITCAP(TRIM(os)) AS os,
    first_seen::timestamp AS first_seen,
    fixed_last_seen::timestamp AS last_seen,
    (fixed_last_seen - first_seen) AS device_age_days,
    DATE(first_seen) AS first_seen_date,
    DATE(fixed_last_seen) AS last_seen_date
FROM cleaned;

CREATE TABLE staging.stg_merchants AS
WITH ranked AS (
SELECT *,
ROW_NUMBER() OVER (PARTITION BY merchant_id ORDER BY merchant_id) AS rn
FROM public.raw_merchants
)
SELECT
merchant_id::int AS merchant_id,
INITCAP(TRIM(merchant_name)) AS merchant_name,
INITCAP(TRIM(category)) AS category,
INITCAP(TRIM(state)) AS state,
INITCAP(TRIM(risk_category)) AS risk_category,
CASE WHEN INITCAP(TRIM(risk_category)) = 'High' THEN 1 ELSE 0 END AS high_risk_flag
FROM ranked
WHERE rn = 1;

CREATE TABLE staging.stg_transactions AS
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY txn_timestamp DESC) AS rn
    FROM public.raw_transactions
)
SELECT
    transaction_id::int AS transaction_id,
    customer_id::int AS customer_id,
    merchant_id::int AS merchant_id,
    device_id::text AS device_id,
    amount::numeric(12,2) AS amount,
    INITCAP(TRIM(channel)) AS channel,
    INITCAP(TRIM(status)) AS status,
    txn_timestamp::timestamp AS txn_timestamp,
    DATE(txn_timestamp) AS txn_date,
    EXTRACT(HOUR FROM txn_timestamp) AS txn_hour,
    EXTRACT(DOW FROM txn_timestamp) AS txn_day_of_week,
    location_lat::numeric(9,6) AS location_lat,
    location_long::numeric(9,6) AS location_long,
    CASE WHEN INITCAP(TRIM(status)) = 'Failed' THEN 1 ELSE 0 END AS is_failed
FROM ranked
WHERE rn = 1;

SELECT * FROM staging.stg_customers LIMIT 5;
SELECT * FROM staging.stg_devices LIMIT 5;
SELECT * FROM staging.stg_merchants LIMIT 5;
SELECT * FROM staging.stg_transactions LIMIT 5;

-- create feature engineering tables

CREATE SCHEMA IF NOT EXISTS features;

-- Feature engineering for customers 
CREATE TABLE features.fe_customers AS
WITH txn_agg AS (
    SELECT
        t.customer_id,
        COUNT(*) AS total_transactions,  
        COUNT(*) FILTER (WHERE t.txn_timestamp >= (SELECT MAX(txn_timestamp) FROM staging.stg_transactions) - INTERVAL '30 days') AS total_transactions_30d,
        MAX(t.txn_timestamp) AS last_transaction_date,
        MAX(t.amount) AS last_transaction_amount
    FROM staging.stg_transactions t
    GROUP BY t.customer_id
),
device_agg AS (
    SELECT
        customer_id,
        COUNT(DISTINCT device_id) AS device_diversity
    FROM staging.stg_transactions
    GROUP BY customer_id
)
SELECT
    c.customer_id,
    c.full_name,
    c.gender,
    c.age, 
    CASE
        WHEN c.age BETWEEN 18 AND 24 THEN '18-24'
        WHEN c.age BETWEEN 25 AND 34 THEN '25-34'
        WHEN c.age BETWEEN 35 AND 44 THEN '35-44'
        WHEN c.age BETWEEN 45 AND 54 THEN '45-54'
        WHEN c.age BETWEEN 55 AND 64 THEN '55-64'
        ELSE '65+'
    END AS age_group, 
    CURRENT_DATE - c.join_date AS customer_tenure_days, 
    COALESCE(d.device_diversity,0) AS device_diversity, 
    COALESCE(t.total_transactions_30d,0) AS total_transactions_30d, 
    t.last_transaction_amount, 
    -- txn_frequency_bucket to classify customer activity level based on total_transactions_30d
    CASE
        WHEN COALESCE(t.total_transactions_30d,0) = 0 THEN 'No Activity'
        WHEN COALESCE(t.total_transactions_30d,0) <= 5 THEN 'Low'
        WHEN COALESCE(t.total_transactions_30d,0) <= 15 THEN 'Medium'
        ELSE 'High'
    END AS txn_frequency_bucket
FROM staging.stg_customers c
LEFT JOIN txn_agg t
    ON c.customer_id = t.customer_id
LEFT JOIN device_agg d
    ON c.customer_id = d.customer_id;

-- Feature engineering for devices
CREATE TABLE features.fe_devices AS
WITH txn_agg AS (
    -- Aggregate transaction info per device
    SELECT
        t.device_id,
        COUNT(DISTINCT t.customer_id) AS unique_customers_on_device,     
        COUNT(*) AS total_txns,                                         
        COUNT(*) FILTER (WHERE EXTRACT(HOUR FROM t.txn_timestamp) BETWEEN 0 AND 6) AS night_txns,         
        COUNT(*) FILTER (WHERE EXTRACT(DOW FROM t.txn_timestamp) IN (0,6)) AS weekend_txns,              
        MAX(t.txn_timestamp) AS last_txn_date,
        MIN(t.txn_timestamp) AS first_txn_date
    FROM staging.stg_transactions t
    GROUP BY t.device_id
)
SELECT
    d.device_id::text AS device_id,
    INITCAP(TRIM(d.device_type)) AS device_type,            
    UPPER(TRIM(d.os)) AS os,                                
    d.first_seen,
    d.last_seen,
    -- device lifespan in days 
    EXTRACT(DAY FROM (COALESCE(d.last_seen, d.first_seen) - d.first_seen))::int AS device_lifespan_days,
    COALESCE(t.unique_customers_on_device,0) AS unique_customers_on_device,
    -- activity bucket based on total transactions
    CASE
        WHEN COALESCE(t.total_txns,0) = 0 THEN 'Inactive'
        WHEN COALESCE(t.total_txns,0) <= 20 THEN 'Low'
        WHEN COALESCE(t.total_txns,0) <= 100 THEN 'Medium'
        ELSE 'High'
    END AS device_activity_bucket,
    CASE WHEN COALESCE(t.unique_customers_on_device,0) > 1 THEN 1 ELSE 0 END AS reused_device_flag,
    CASE WHEN COALESCE(t.total_txns,0) > 0 THEN (t.night_txns::float / t.total_txns) ELSE 0 END AS night_usage_ratio,
    CASE WHEN COALESCE(t.total_txns,0) > 0 THEN (t.weekend_txns::float / t.total_txns) ELSE 0 END AS weekend_usage_ratio,
    COALESCE(t.total_txns,0) AS total_txns,
    t.first_txn_date,
    t.last_txn_date
FROM staging.stg_devices d
LEFT JOIN txn_agg t
    ON d.device_id = t.device_id;

-- Feature engineering for merchants
CREATE TABLE features.fe_merchants AS
WITH txn_agg AS (
    -- Aggregate transaction data per merchant
    SELECT
        m.merchant_id,
        COUNT(DISTINCT t.customer_id) AS unique_customers_count,
        SUM(CASE WHEN t.amount > 50000 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*),0) AS high_value_txn_ratio, 
        SUM(CASE WHEN t.status = 'Failed' THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*),0) AS failed_txn_ratio 
    FROM staging.stg_transactions t
    JOIN staging.stg_merchants m
        ON t.merchant_id = m.merchant_id
    GROUP BY m.merchant_id
)
SELECT
    m.merchant_id,
    INITCAP(TRIM(m.merchant_name)) AS merchant_name, 
    INITCAP(TRIM(m.state)) AS state, 
    CASE
        WHEN UPPER(m.risk_category) = 'HIGH' THEN 'High'
        WHEN UPPER(m.risk_category) = 'LOW' THEN 'Low'
        ELSE 'Medium'
    END AS validated_risk_category, 
    CASE
        WHEN UPPER(m.risk_category) = 'HIGH' THEN 'High'
        WHEN UPPER(m.risk_category) = 'LOW' THEN 'Low'
        ELSE 'Medium'
    END AS risk_bucket, 
    -- Derive geographic region from state
    CASE
        WHEN m.state IN ('Lagos','Oyo') THEN 'South West'
        WHEN m.state IN ('Abuja') THEN 'Central'
        WHEN m.state IN ('Kano','Kaduna') THEN 'North'
        ELSE 'Other'
    END AS state_region, 
    COALESCE(t.unique_customers_count,0) AS unique_customers_count,
    COALESCE(t.high_value_txn_ratio,0) AS high_value_txn_ratio,
    COALESCE(t.failed_txn_ratio,0) AS failed_txn_ratio
FROM staging.stg_merchants m
LEFT JOIN txn_agg t
    ON m.merchant_id = t.merchant_id;

-- Feature engineering for transactions
CREATE TABLE features.fe_transactions AS
WITH txn_agg AS (
    SELECT
        t.transaction_id,
        t.customer_id,
        t.device_id,
		t.merchant_id, 
        t.amount,
        t.txn_timestamp,
        INITCAP(TRIM(t.channel)) AS channel,
        INITCAP(TRIM(t.status)) AS status,
        CASE WHEN t.status = 'Failed' THEN 1 ELSE 0 END AS is_failed, 
        EXTRACT(HOUR FROM t.txn_timestamp) AS txn_hour, 
        EXTRACT(DOW FROM t.txn_timestamp) AS txn_day_of_week, 
        -- amount_bucket to flag unusually large transactions
        CASE
            WHEN t.amount < 10000 THEN 'Low'
            WHEN t.amount BETWEEN 10000 AND 50000 THEN 'Medium'
            ELSE 'High'
        END AS amount_bucket,
        -- Count transactions by the same customer in previous 24h to detect burst activity
        COUNT(*) OVER (
            PARTITION BY t.customer_id
            ORDER BY t.txn_timestamp
            RANGE BETWEEN INTERVAL '1 day' PRECEDING AND CURRENT ROW
        ) AS customer_txn_count_24h,
        -- device_switch_flag → customer uses different device within 24h
        LAG(t.device_id) OVER (PARTITION BY t.customer_id ORDER BY t.txn_timestamp) AS prev_device_id
    FROM staging.stg_transactions t
)
SELECT
    transaction_id,
    customer_id,
    device_id,
	merchant_id, 
    amount,
    txn_timestamp,
    channel,
    status,
    is_failed,
    txn_hour,
    txn_day_of_week,
    amount_bucket,
    customer_txn_count_24h,
    CASE
        WHEN prev_device_id IS NOT NULL AND prev_device_id <> device_id THEN 1
        ELSE 0
    END AS device_switch_flag
FROM txn_agg;

SELECT * FROM features.fe_customers LIMIT 5;
SELECT * FROM features.fe_devices LIMIT 5;
SELECT * FROM features.fe_merchants LIMIT 5;
SELECT * FROM features.fe_transactions LIMIT 5;

-- create analytics tables
CREATE SCHEMA IF NOT EXISTS analytics;
-- Create customers dim table
CREATE TABLE analytics.dim_customers AS
SELECT
    customer_id,
    full_name,
    gender,
    age,
    age_group,
    customer_tenure_days,
    device_diversity,
    total_transactions_30d,
    last_transaction_amount,
    txn_frequency_bucket
FROM features.fe_customers;

-- Create devices dim table
CREATE TABLE analytics.dim_devices AS
SELECT
    device_id,
    device_type,
    os,
    first_seen,
    last_seen,
    device_lifespan_days,
    unique_customers_on_device,
    device_activity_bucket,
    reused_device_flag,
    night_usage_ratio,
    weekend_usage_ratio,
    total_txns,
    first_txn_date,
    last_txn_date
FROM features.fe_devices;

-- Create merchants dim table
CREATE TABLE analytics.dim_merchants AS
SELECT
    merchant_id,
    merchant_name,
    state,
    validated_risk_category,
    risk_bucket,
    state_region,
    unique_customers_count,
    high_value_txn_ratio,
    failed_txn_ratio
FROM features.fe_merchants;

-- Create transactions fact table
CREATE TABLE analytics.fact_transactions AS
SELECT
    t.transaction_id,
    t.customer_id,
    c.full_name,
    c.gender,
    c.age,
    c.age_group,
    c.customer_tenure_days,
    c.device_diversity,
    c.total_transactions_30d,
    c.last_transaction_amount,
    c.txn_frequency_bucket,
    t.device_id,
    d.device_type,
    d.os,
    d.device_lifespan_days,
    d.reused_device_flag,
    t.merchant_id,
    m.merchant_name,
    m.validated_risk_category,
    m.risk_bucket,
    m.state_region,
    t.amount,
    t.txn_timestamp,
    t.channel,
    t.status,
    t.is_failed,
    t.txn_hour,
    t.txn_day_of_week,
    t.amount_bucket,
    t.customer_txn_count_24h,
    t.device_switch_flag
FROM features.fe_transactions t
LEFT JOIN features.fe_customers c
    ON t.customer_id = c.customer_id
LEFT JOIN features.fe_devices d
    ON t.device_id = d.device_id
LEFT JOIN features.fe_merchants m
    ON t.merchant_id = m.merchant_id;

ALTER TABLE analytics.fact_transactions
ADD PRIMARY KEY (transaction_id);

ALTER TABLE analytics.dim_customers
ADD PRIMARY KEY (customer_id);

ALTER TABLE analytics.dim_devices
ADD PRIMARY KEY (device_id);

ALTER TABLE analytics.dim_merchants
ADD PRIMARY KEY (merchant_id);

SELECT * FROM analytics.dim_customers LIMIT 5;
SELECT * FROM analytics.dim_devices LIMIT 5;
SELECT * FROM analytics.dim_merchants LIMIT 5;
SELECT * FROM analytics.fact_transactions LIMIT 5;