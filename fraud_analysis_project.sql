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
	last_seen DATE);

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
	device_id TEXT,
	amount NUMERIC,
	channel TEXT,
	status TEXT,
	txn_timestamp TIMESTAMP,
	location_lat NUMERIC,
	location_long NUMERIC
);

SELECT * FROM raw_customers LIMIT 5;
SELECT * FROM raw_devices LIMIT 5;
SELECT * FROM raw_merchants LIMIT 5;
SELECT * FROM raw_transactions LIMIT 5;

-- create staging tables
CREATE SCHEMA IF NOT EXISTS staging;

-- Create customers staging table
CREATE TABLE staging.stg_customers AS
SELECT 
    customer_id,
    INITCAP(REGEXP_REPLACE(TRIM(full_name), '\s+', ' ', 'g')) AS full_name,
    INITCAP(TRIM(gender)) AS gender,
    CASE WHEN age > 0 AND age < 100 THEN age ELSE NULL END AS age,
    INITCAP(state) AS state,
    join_date,
    UPPER(kyc_level) AS kyc_level
FROM public.raw_customers;

-- Create devices staging table
CREATE TABLE staging.stg_devices AS
SELECT
    device_id,
    INITCAP(TRIM(device_type)) AS device_type,
    INITCAP(TRIM(os)) AS os,
    first_seen AS device_first_seen,
    CASE WHEN last_seen >= first_seen THEN last_seen ELSE first_seen END AS device_last_seen
FROM public.raw_devices;

-- Create merchants staging table
CREATE TABLE staging.stg_merchants AS
SELECT
    merchant_id,
    INITCAP(TRIM(merchant_name)) AS merchant_name,
    INITCAP(TRIM(category)) AS category,
    INITCAP(TRIM(state)) AS state,
    INITCAP(TRIM(risk_category)) AS risk_category
FROM public.raw_merchants;

-- Create transactions staging table
CREATE TABLE staging.stg_transactions AS
SELECT
    transaction_id,
    customer_id,
    merchant_id,
    device_id,
    amount::numeric(12,2) AS amount,
    INITCAP(TRIM(channel)) AS channel,
    INITCAP(TRIM(status)) AS status,
    txn_timestamp,
    DATE(txn_timestamp) AS txn_date,
    EXTRACT(HOUR FROM txn_timestamp) AS txn_hour,
    EXTRACT(DOW FROM txn_timestamp) AS txn_day_of_week,
    location_lat::numeric(9,6) AS location_lat,
    location_long::numeric(9,6) AS location_long
FROM public.raw_transactions;

SELECT * FROM staging.stg_customers LIMIT 5;
SELECT * FROM staging.stg_devices LIMIT 5;
SELECT * FROM staging.stg_merchants LIMIT 5;
SELECT * FROM staging.stg_transactions LIMIT 5;

-- create feature engineering tables
CREATE SCHEMA IF NOT EXISTS features;

CREATE TABLE latest_timestamp AS
SELECT MAX(txn_timestamp) AS latest_timestamp
FROM staging.stg_transactions;

-- Feature engineering for customers 
CREATE TABLE features.fe_customers AS
WITH customer_activity AS (
    SELECT
		t.customer_id,
		COUNT(*) AS customer_total_txns,
		COUNT(DISTINCT t.merchant_id) AS unique_merchants_per_customer,
		COUNT(DISTINCT t.device_id) AS unique_devices_per_customer,
		SUM(amount) AS customer_txn_volume,
		AVG(t.amount) AS customer_avg_txn_amount,
		STDDEV(t.amount) AS customer_txn_amount_stddev,
		(ARRAY_AGG(amount ORDER BY t.txn_timestamp DESC))[1] AS customer_latest_txn_amount,
		SUM(t.amount) FILTER (WHERE t.txn_timestamp >= l.latest_timestamp - INTERVAL '30 days') AS customer_txn_volume_past_30d,
		COUNT(*) FILTER (WHERE t.amount > 25022)::FLOAT / NULLIF(COUNT(*), 0) AS customer_high_value_txn_ratio,
		COUNT(*) FILTER (WHERE t.status = 'Failed')::FLOAT/ NULLIF(COUNT(*), 0) AS customer_failed_txn_ratio,
		COUNT(*) FILTER (WHERE t.txn_timestamp >= l.latest_timestamp - INTERVAL '30 days') AS customer_txns_past_30d,
		MIN(t.txn_timestamp) AS customer_first_txn_timestamp,
		MAX(t.txn_timestamp) AS customer_latest_txn_timestamp,
		COUNT(*) FILTER (WHERE t.txn_hour BETWEEN 0 AND 6)::FLOAT / NULLIF(COUNT(*), 0) AS customer_night_txn_ratio
    FROM staging.stg_transactions t
    CROSS JOIN latest_timestamp l
    GROUP BY t.customer_id
)
SELECT
	c.customer_id,  
	COALESCE(t.customer_total_txns, 0) AS customer_total_txns,
    COALESCE(t.unique_merchants_per_customer, 0) AS unique_merchants_per_customer,
    COALESCE(t.unique_devices_per_customer, 0) AS unique_devices_per_customer,
    COALESCE(t.customer_txn_volume, 0) AS customer_txn_volume,
    COALESCE(t.customer_avg_txn_amount, 0) AS customer_avg_txn_amount,
    COALESCE(t.customer_txn_amount_stddev, 0) AS customer_txn_amount_stddev,
    COALESCE(t.customer_latest_txn_amount, 0) AS customer_latest_txn_amount,
    COALESCE(t.customer_txn_volume_past_30d, 0) AS customer_txn_volume_past_30d,
    COALESCE(t.customer_high_value_txn_ratio, 0) AS customer_high_value_txn_ratio,
    COALESCE(t.customer_failed_txn_ratio, 0) AS customer_failed_txn_ratio,
    COALESCE(t.customer_txns_past_30d, 0) AS customer_txns_past_30d,
	t.customer_first_txn_timestamp,
	t.customer_latest_txn_timestamp,
	COALESCE(t.customer_night_txn_ratio, 0) AS customer_night_txn_ratio,
	(l.latest_timestamp::date - c.join_date)::INT AS customer_tenure_days,
	(t.customer_first_txn_timestamp::date - c.join_date)::INT AS signup_to_first_txn_days,
	CASE WHEN t.customer_first_txn_timestamp IS NOT NULL THEN (l.latest_timestamp::date - t.customer_first_txn_timestamp::date)::INT ELSE NULL END AS active_customer_days,
	CASE WHEN t.customer_latest_txn_timestamp IS NOT NULL THEN (l.latest_timestamp::date - t.customer_latest_txn_timestamp::date)::INT ELSE NULL END AS days_since_last_customer_txn,
	CASE
        WHEN c.state IN ('Lagos', 'Oyo') THEN 'South West'
        WHEN c.state IN ('Abuja') THEN 'Central'
        WHEN c.state IN ('Kano', 'Kaduna') THEN 'North'
        ELSE 'Other'
    END AS customer_state_region
FROM staging.stg_customers c
LEFT JOIN customer_activity t ON c.customer_id = t.customer_id
CROSS JOIN latest_timestamp l;

-- Descriptive stats to guide age bucketing logic
SELECT
    MIN(age) AS min_age,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY age) AS q1_age,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY age) AS median_age,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY age) AS q3_age,
    MAX(age) AS max_age,
    AVG(age) AS mean_age
FROM staging.stg_customers;

-- Descriptive stats to guide transaction_frequency_bucket logic
SELECT
    MIN(customer_txns_past_30d) AS min_txns,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY customer_txns_past_30d) AS p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY customer_txns_past_30d) AS median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY customer_txns_past_30d) AS p75,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY customer_txns_past_30d) AS p90,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY customer_txns_past_30d) AS p95,
    MAX(customer_txns_past_30d) AS max_txns,
    AVG(customer_txns_past_30d) AS mean_txns
FROM features.fe_customers;

-- See the actual distribution in buckets to spot natural breaks
SELECT
    customer_txns_past_30d,
    COUNT(*) AS customer_count
FROM features.fe_customers
GROUP BY customer_txns_past_30d
ORDER BY customer_txns_past_30d;

-- Feature engineering for devices
CREATE TABLE features.fe_devices AS
WITH device_activity AS (	
    SELECT
        t.device_id,
		COUNT(*) AS device_total_txns,
        COUNT(DISTINCT t.customer_id) AS unique_customers_per_device,
        COUNT(DISTINCT t.merchant_id) AS unique_merchants_per_device,
		SUM(t.amount) AS device_txn_volume,
        AVG(t.amount) AS device_avg_txn_amount,
		STDDEV(t.amount) AS device_txn_amount_stddev,
		(ARRAY_AGG(amount ORDER BY t.txn_timestamp DESC))[1] AS device_latest_txn_amount,
		SUM(t.amount) FILTER (WHERE t.txn_timestamp >= l.latest_timestamp - INTERVAL '30 days') AS device_txn_volume_past_30d,
		COUNT(*) FILTER (WHERE t.amount > 25022)::FLOAT/ NULLIF(COUNT(*), 0) AS device_high_value_txn_ratio,
        COUNT(*) FILTER (WHERE t.status = 'Failed')::FLOAT/ NULLIF(COUNT(*), 0) AS device_failed_txn_ratio,
        COUNT(*) FILTER (WHERE t.txn_timestamp >= l.latest_timestamp - INTERVAL '30 days') AS device_txns_past_30d,
		MIN(t.txn_timestamp) AS device_first_txn_timestamp,
		MAX(t.txn_timestamp) AS device_latest_txn_timestamp,
		COUNT(*) FILTER (WHERE t.txn_hour BETWEEN 0 AND 6)::FLOAT / NULLIF(COUNT(*), 0) AS device_night_txn_ratio
    FROM staging.stg_transactions t
    CROSS JOIN latest_timestamp l
    GROUP BY t.device_id
)
SELECT
    d.device_id,
	COALESCE(t.device_total_txns, 0) AS device_total_txns,
	COALESCE(t.unique_customers_per_device, 0) AS unique_customers_per_device,
    COALESCE(t.unique_merchants_per_device, 0) AS unique_merchants_per_device,
	COALESCE(t.device_txn_volume, 0) AS device_txn_volume,
	COALESCE(t.device_avg_txn_amount, 0) AS device_avg_txn_amount,
	COALESCE(t.device_txn_amount_stddev, 0) AS device_txn_amount_stddev,
	COALESCE(t.device_latest_txn_amount, 0) AS device_latest_txn_amount,
	COALESCE(t.device_txn_volume_past_30d, 0) AS device_txn_volume_past_30d,
	COALESCE(t.device_high_value_txn_ratio, 0) AS device_high_value_txn_ratio,
	COALESCE(t.device_failed_txn_ratio, 0) AS device_failed_txn_ratio,
	COALESCE(t.device_txns_past_30d, 0) AS device_txns_past_30d,
	t.device_first_txn_timestamp,
	t.device_latest_txn_timestamp,
	COALESCE(t.device_night_txn_ratio, 0) AS device_night_txn_ratio,
	CASE WHEN COALESCE(t.unique_customers_per_device, 0) > 1 THEN 1 ELSE 0 END AS reused_device_flag,
	(l.latest_timestamp::date - t.device_latest_txn_timestamp::date)::INT AS days_since_last_device_txn,
	(d.device_last_seen - d.device_first_seen)::INT AS device_lifespan_days
FROM staging.stg_devices d
LEFT JOIN device_activity t ON d.device_id = t.device_id
CROSS JOIN latest_timestamp l;

-- Descriptive stats to guide device_activity_bucket logic
SELECT
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY device_total_txns) AS q1,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY device_total_txns) AS median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY device_total_txns) AS q3
FROM features.fe_devices
WHERE device_total_txns > 0;

-- Feature engineering for merchants
CREATE TABLE features.fe_merchants AS
WITH merchant_activity AS (
    SELECT
        t.merchant_id,
        COUNT(*) AS merchant_total_txns,
		COUNT(DISTINCT t.customer_id) AS unique_customers_per_merchant,
        COUNT(DISTINCT t.device_id) AS unique_devices_per_merchant,
        SUM(t.amount) AS merchant_txn_volume,
        AVG(t.amount) AS merchant_avg_txn_amount,
		STDDEV(t.amount) AS merchant_txn_amount_stddev,
		(ARRAY_AGG(amount ORDER BY t.txn_timestamp DESC))[1] AS merchant_latest_txn_amount,
		SUM(t.amount) FILTER (WHERE t.txn_timestamp >= l.latest_timestamp - INTERVAL '30 days') AS merchant_txn_volume_past_30d,
        COUNT(*) FILTER (WHERE t.amount > 25022)::FLOAT / NULLIF(COUNT(*), 0) AS merchant_high_value_txn_ratio,
        COUNT(*) FILTER (WHERE t.status = 'Failed')::FLOAT / NULLIF(COUNT(*), 0) AS merchant_failed_txn_ratio,
		COUNT(*) FILTER (WHERE t.txn_timestamp >= l.latest_timestamp - INTERVAL '30 days') AS merchant_txns_past_30d,
		MIN(t.txn_timestamp) AS merchant_first_txn_timestamp,
		MAX(t.txn_timestamp) AS merchant_latest_txn_timestamp,
        COUNT(*) FILTER (WHERE t.txn_hour BETWEEN 0 AND 6)::FLOAT / NULLIF(COUNT(*), 0) AS merchant_night_txn_ratio
    FROM staging.stg_transactions t
    CROSS JOIN latest_timestamp l
    GROUP BY t.merchant_id
)
SELECT
    m.merchant_id,
	COALESCE(t.merchant_total_txns, 0) AS merchant_total_txns,
	COALESCE(t.unique_customers_per_merchant, 0) AS unique_customers_per_merchant,
    COALESCE(t.unique_devices_per_merchant, 0) AS unique_devices_per_merchant,
    COALESCE(t.merchant_txn_volume, 0) AS merchant_txn_volume,
    COALESCE(t.merchant_avg_txn_amount, 0) AS merchant_avg_txn_amount,
    COALESCE(t.merchant_txn_amount_stddev, 0) AS merchant_txn_amount_stddev,
	COALESCE(t.merchant_latest_txn_amount, 0) AS merchant_latest_txn_amount,
	COALESCE(t.merchant_txn_volume_past_30d, 0) AS merchant_txn_volume_past_30d,
    COALESCE(t.merchant_high_value_txn_ratio, 0) AS merchant_high_value_txn_ratio,
    COALESCE(t.merchant_failed_txn_ratio, 0) AS merchant_failed_txn_ratio,
	COALESCE(t.merchant_txns_past_30d, 0) AS merchant_txns_past_30d,
	t.merchant_first_txn_timestamp,
	t.merchant_latest_txn_timestamp,
    COALESCE(t.merchant_night_txn_ratio, 0) AS merchant_night_txn_ratio,
	CASE WHEN m.risk_category = 'High' THEN 1 ELSE 0 END AS high_risk_merchant_flag,
    (l.latest_timestamp::date - t.merchant_latest_txn_timestamp::date)::INT AS days_since_last_merchant_txn,
   	CASE
        WHEN m.state IN ('Lagos', 'Oyo') THEN 'South West'
        WHEN m.state IN ('Abuja') THEN 'Central'
        WHEN m.state IN ('Kano', 'Kaduna') THEN 'North'
        ELSE 'Other'
    END AS merchant_state_region
FROM staging.stg_merchants m
LEFT JOIN merchant_activity t ON m.merchant_id = t.merchant_id
CROSS JOIN latest_timestamp l;

-- Feature engineering for transactions
CREATE TABLE features.fe_transactions AS
WITH detect_device_switch AS (
    SELECT
        transaction_id,
		customer_id,
		txn_timestamp,
		device_id,
		LAG(device_id) OVER (PARTITION BY customer_id ORDER BY txn_timestamp) AS previous_device_id
    FROM staging.stg_transactions 
)
SELECT
	t.transaction_id,
	CASE WHEN d.previous_device_id IS NOT NULL AND d.previous_device_id <> t.device_id THEN 1 ELSE 0 END AS device_switch_flag,
    CASE WHEN t.amount > 25022 THEN 1 ELSE 0 END AS is_high_value_flag,
	CASE WHEN t.status = 'Failed' THEN 1 ELSE 0 END AS is_failed_flag,
	CASE WHEN EXTRACT(HOUR FROM t.txn_timestamp) BETWEEN 0 AND 6 THEN 1 ELSE 0 END AS is_night_txn_flag,
	(t.amount - AVG(t.amount) OVER (PARTITION BY t.customer_id)) / NULLIF(STDDEV(t.amount) OVER (PARTITION BY t.customer_id), 0) AS amount_zscore,
	SUM(CASE WHEN t.status = 'Failed' THEN 1 ELSE 0 END) OVER (PARTITION BY t.customer_id ORDER BY t.txn_timestamp RANGE BETWEEN INTERVAL '1 day' PRECEDING AND CURRENT ROW) AS customer_failed_count_24h,
	COUNT(*) OVER (PARTITION BY t.customer_id ORDER BY t.txn_timestamp RANGE BETWEEN INTERVAL '1 day' PRECEDING AND CURRENT ROW) AS customer_txn_count_24h,
    EXTRACT(EPOCH FROM (t.txn_timestamp - LAG(t.txn_timestamp) OVER (PARTITION BY t.customer_id ORDER BY t.txn_timestamp))) / 3600 AS hours_since_last_txn
FROM staging.stg_transactions t
INNER JOIN detect_device_switch d ON t.transaction_id = d.transaction_id;

-- Descriptive stats to guide amount_bucket logic
SELECT
    MIN(amount) AS min_amount,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY amount) AS p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY amount) AS median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY amount) AS p75,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY amount) AS p90,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY amount) AS p95,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY amount) AS p99,
    AVG(amount) AS mean_amount,
    MAX(amount) AS max_amount
FROM staging.stg_transactions;

SELECT * FROM features.fe_customers LIMIT 5;
SELECT * FROM features.fe_devices LIMIT 5;
SELECT * FROM features.fe_merchants LIMIT 5;
SELECT * FROM features.fe_transactions LIMIT 5;

-- create analytics tables
CREATE SCHEMA IF NOT EXISTS analytics;

-- Create customers dim table
CREATE TABLE analytics.dim_customers AS
SELECT
    c.customer_id,
    c.full_name,
    c.gender,
    c.age,
    c.state,
	c.join_date,
    c.kyc_level,
	f.customer_total_txns,
    f.unique_merchants_per_customer,
    f.unique_devices_per_customer,
    f.customer_txn_volume,
    f.customer_avg_txn_amount,
    f.customer_txn_amount_stddev,
    f.customer_latest_txn_amount,
    f.customer_txn_volume_past_30d,
    f.customer_high_value_txn_ratio,
    f.customer_failed_txn_ratio,
	f.customer_txns_past_30d,
	f.customer_first_txn_timestamp,
	f.customer_latest_txn_timestamp,
    f.customer_night_txn_ratio,
	f.customer_tenure_days,
    f.signup_to_first_txn_days,
    f.active_customer_days,
    f.days_since_last_customer_txn,
	f.customer_state_region
FROM staging.stg_customers c
LEFT JOIN features.fe_customers f ON c.customer_id = f.customer_id;

-- Create devices dim table
CREATE TABLE analytics.dim_devices AS
SELECT
    d.device_id,
    d.device_type,
    d.os,
	d.device_first_seen, 
    d.device_last_seen, 
    f.device_total_txns,
	f.unique_customers_per_device,
    f.unique_merchants_per_device,
    f.device_txn_volume,
    f.device_avg_txn_amount,
	f.device_txn_amount_stddev,
	f.device_latest_txn_amount,
	f.device_txn_volume_past_30d,
    f.device_high_value_txn_ratio,
    f.device_failed_txn_ratio,
    f.device_txns_past_30d,
	f.device_first_txn_timestamp, 
	f.device_latest_txn_timestamp,
    f.device_night_txn_ratio,
    f.reused_device_flag,
    f.days_since_last_device_txn,
    f.device_lifespan_days
FROM staging.stg_devices d
LEFT JOIN features.fe_devices f ON d.device_id = f.device_id;

-- Create merchants dim table
CREATE TABLE analytics.dim_merchants AS
SELECT
    m.merchant_id,
    m.merchant_name,
    m.category,
    m.state,
    m.risk_category,
	f.merchant_total_txns,
	f.unique_customers_per_merchant,
    f.unique_devices_per_merchant,
    f.merchant_txn_volume,
    f.merchant_avg_txn_amount,
    f.merchant_txn_amount_stddev,
	f.merchant_latest_txn_amount,
	f.merchant_txn_volume_past_30d,
    f.merchant_high_value_txn_ratio,
    f.merchant_failed_txn_ratio,
    f.merchant_txns_past_30d,
	f.merchant_first_txn_timestamp,
    f.merchant_latest_txn_timestamp,
    f.merchant_night_txn_ratio,
    f.high_risk_merchant_flag,
    f.days_since_last_merchant_txn,
	f.merchant_state_region
FROM staging.stg_merchants m
LEFT JOIN features.fe_merchants f ON m.merchant_id = f.merchant_id;

-- Create transactions fact table
CREATE TABLE analytics.fact_transactions AS
SELECT
    t.transaction_id,
    t.customer_id,
    t.merchant_id,
    t.device_id,
    t.amount,
    t.channel,
    t.status,
    t.txn_timestamp,
	t.txn_date,
	t.txn_hour,
    t.txn_day_of_week,
    f.device_switch_flag,
    f.is_high_value_flag,
    f.is_failed_flag,
    f.is_night_txn_flag,
    f.amount_zscore,
    f.customer_failed_count_24h,
    f.customer_txn_count_24h,
    f.hours_since_last_txn
FROM staging.stg_transactions t
INNER JOIN features.fe_transactions f ON t.transaction_id = f.transaction_id;

SELECT * FROM analytics.dim_customers LIMIT 5;
SELECT * FROM analytics.dim_devices LIMIT 5;
SELECT * FROM analytics.dim_merchants LIMIT 5;
SELECT * FROM analytics.fact_transactions LIMIT 5;

-- Add primary keys
ALTER TABLE analytics.fact_transactions ADD PRIMARY KEY (transaction_id);
ALTER TABLE analytics.dim_customers ADD PRIMARY KEY (customer_id);
ALTER TABLE analytics.dim_devices ADD PRIMARY KEY (device_id);
ALTER TABLE analytics.dim_merchants ADD PRIMARY KEY (merchant_id);

-- Add foreign keys
ALTER TABLE analytics.fact_transactions ADD CONSTRAINT fk_fact_customer FOREIGN KEY (customer_id) REFERENCES analytics.dim_customers(customer_id);
ALTER TABLE analytics.fact_transactions ADD CONSTRAINT fk_fact_device FOREIGN KEY (device_id) REFERENCES analytics.dim_devices(device_id);
ALTER TABLE analytics.fact_transactions ADD CONSTRAINT fk_fact_merchant FOREIGN KEY (merchant_id) REFERENCES analytics.dim_merchants(merchant_id);

