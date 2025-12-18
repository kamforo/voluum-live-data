-- Voluum Visit Data Schema for Supabase
-- Run this in Supabase SQL Editor

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Main visits table
CREATE TABLE visits (
    id BIGSERIAL PRIMARY KEY,
    visit_id TEXT UNIQUE,
    click_id TEXT,
    campaign_id TEXT NOT NULL,
    campaign_name TEXT,
    offer_id TEXT,
    offer_name TEXT,
    lander_id TEXT,
    lander_name TEXT,
    traffic_source_id TEXT,
    traffic_source_name TEXT,

    -- Timing
    visit_timestamp TIMESTAMPTZ NOT NULL,
    click_timestamp TIMESTAMPTZ,
    conversion_timestamp TIMESTAMPTZ,

    -- Geo
    country_code TEXT,
    country_name TEXT,
    region TEXT,
    city TEXT,

    -- Device
    device_type TEXT,
    os TEXT,
    os_version TEXT,
    browser TEXT,
    browser_version TEXT,

    -- Connection
    isp TEXT,
    connection_type TEXT,
    ip TEXT,

    -- Metrics
    cost DECIMAL(12,6) DEFAULT 0,
    revenue DECIMAL(12,6) DEFAULT 0,
    profit DECIMAL(12,6) DEFAULT 0,
    is_click BOOLEAN DEFAULT FALSE,
    is_conversion BOOLEAN DEFAULT FALSE,

    -- Custom variables (v1-v10 from Voluum)
    custom_var_1 TEXT,
    custom_var_2 TEXT,
    custom_var_3 TEXT,
    custom_var_4 TEXT,
    custom_var_5 TEXT,
    custom_var_6 TEXT,
    custom_var_7 TEXT,
    custom_var_8 TEXT,
    custom_var_9 TEXT,
    custom_var_10 TEXT,

    -- External IDs
    external_id TEXT,
    sub_id TEXT,

    -- Metadata
    raw_data JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),

    -- Indexes will use these
    CONSTRAINT visits_timestamp_check CHECK (visit_timestamp IS NOT NULL)
);

-- Conversions table (separate for faster conversion queries)
CREATE TABLE conversions (
    id BIGSERIAL PRIMARY KEY,
    visit_id TEXT REFERENCES visits(visit_id),
    click_id TEXT,
    conversion_id TEXT UNIQUE,
    campaign_id TEXT NOT NULL,
    campaign_name TEXT,
    offer_id TEXT,
    offer_name TEXT,

    -- Timing
    conversion_timestamp TIMESTAMPTZ NOT NULL,
    visit_timestamp TIMESTAMPTZ,
    time_to_convert INTERVAL,

    -- Geo
    country_code TEXT,

    -- Revenue
    revenue DECIMAL(12,6) DEFAULT 0,
    payout DECIMAL(12,6) DEFAULT 0,

    -- Transaction
    transaction_id TEXT,
    conversion_type TEXT,  -- sale, lead, etc.

    -- Metadata
    raw_data JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Sync state tracking
CREATE TABLE sync_state (
    id SERIAL PRIMARY KEY,
    sync_type TEXT UNIQUE NOT NULL,  -- 'visits', 'conversions'
    last_sync_timestamp TIMESTAMPTZ,
    last_sync_id TEXT,
    records_synced BIGINT DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Initialize sync state
INSERT INTO sync_state (sync_type, last_sync_timestamp) VALUES
    ('visits', NOW() - INTERVAL '1 day'),
    ('conversions', NOW() - INTERVAL '1 day')
ON CONFLICT (sync_type) DO NOTHING;

-- Hourly aggregates for faster pattern queries
CREATE TABLE hourly_stats (
    id BIGSERIAL PRIMARY KEY,
    hour_timestamp TIMESTAMPTZ NOT NULL,
    campaign_id TEXT NOT NULL,
    campaign_name TEXT,
    country_code TEXT,
    device_type TEXT,

    visits INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    conversions INTEGER DEFAULT 0,
    revenue DECIMAL(12,6) DEFAULT 0,
    cost DECIMAL(12,6) DEFAULT 0,
    profit DECIMAL(12,6) DEFAULT 0,

    -- Calculated metrics
    ctr DECIMAL(8,4),
    cr DECIMAL(8,4),
    epc DECIMAL(12,6),

    created_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(hour_timestamp, campaign_id, country_code, device_type)
);

-- =====================
-- INDEXES
-- =====================

-- Visits indexes
CREATE INDEX idx_visits_timestamp ON visits(visit_timestamp DESC);
CREATE INDEX idx_visits_campaign ON visits(campaign_id, visit_timestamp DESC);
CREATE INDEX idx_visits_country ON visits(country_code, visit_timestamp DESC);
CREATE INDEX idx_visits_device ON visits(device_type, visit_timestamp DESC);
CREATE INDEX idx_visits_conversion ON visits(is_conversion) WHERE is_conversion = TRUE;
CREATE INDEX idx_visits_created ON visits(created_at DESC);

-- Conversions indexes
CREATE INDEX idx_conversions_timestamp ON conversions(conversion_timestamp DESC);
CREATE INDEX idx_conversions_campaign ON conversions(campaign_id, conversion_timestamp DESC);
CREATE INDEX idx_conversions_visit ON conversions(visit_id);

-- Hourly stats indexes
CREATE INDEX idx_hourly_timestamp ON hourly_stats(hour_timestamp DESC);
CREATE INDEX idx_hourly_campaign ON hourly_stats(campaign_id, hour_timestamp DESC);

-- =====================
-- RETENTION POLICY
-- =====================

-- Function to delete old data
CREATE OR REPLACE FUNCTION cleanup_old_data(retention_days INTEGER DEFAULT 90)
RETURNS TABLE(visits_deleted BIGINT, conversions_deleted BIGINT, stats_deleted BIGINT)
LANGUAGE plpgsql
AS $$
DECLARE
    v_deleted BIGINT;
    c_deleted BIGINT;
    s_deleted BIGINT;
    cutoff_date TIMESTAMPTZ;
BEGIN
    cutoff_date := NOW() - (retention_days || ' days')::INTERVAL;

    -- Delete old visits
    WITH deleted AS (
        DELETE FROM visits
        WHERE visit_timestamp < cutoff_date
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted FROM deleted;

    -- Delete old conversions
    WITH deleted AS (
        DELETE FROM conversions
        WHERE conversion_timestamp < cutoff_date
        RETURNING 1
    )
    SELECT COUNT(*) INTO c_deleted FROM deleted;

    -- Delete old hourly stats
    WITH deleted AS (
        DELETE FROM hourly_stats
        WHERE hour_timestamp < cutoff_date
        RETURNING 1
    )
    SELECT COUNT(*) INTO s_deleted FROM deleted;

    RETURN QUERY SELECT v_deleted, c_deleted, s_deleted;
END;
$$;

-- Schedule cleanup daily at 3 AM UTC (requires pg_cron)
-- SELECT cron.schedule('cleanup-old-data', '0 3 * * *', 'SELECT cleanup_old_data(90)');

-- =====================
-- USEFUL VIEWS
-- =====================

-- Daily summary view
CREATE OR REPLACE VIEW daily_summary AS
SELECT
    DATE(visit_timestamp) as date,
    campaign_name,
    country_code,
    COUNT(*) as visits,
    SUM(CASE WHEN is_click THEN 1 ELSE 0 END) as clicks,
    SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) as conversions,
    SUM(revenue) as revenue,
    SUM(cost) as cost,
    SUM(profit) as profit,
    ROUND(SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END)::DECIMAL /
          NULLIF(SUM(CASE WHEN is_click THEN 1 ELSE 0 END), 0) * 100, 2) as cr
FROM visits
WHERE visit_timestamp > NOW() - INTERVAL '30 days'
GROUP BY DATE(visit_timestamp), campaign_name, country_code
ORDER BY date DESC, revenue DESC;

-- Hourly pattern view (for anomaly detection)
CREATE OR REPLACE VIEW hourly_patterns AS
SELECT
    EXTRACT(HOUR FROM visit_timestamp) as hour_of_day,
    EXTRACT(DOW FROM visit_timestamp) as day_of_week,
    campaign_name,
    country_code,
    AVG(revenue) as avg_revenue,
    STDDEV(revenue) as stddev_revenue,
    COUNT(*) as sample_count
FROM visits
WHERE visit_timestamp > NOW() - INTERVAL '30 days'
GROUP BY
    EXTRACT(HOUR FROM visit_timestamp),
    EXTRACT(DOW FROM visit_timestamp),
    campaign_name,
    country_code
HAVING COUNT(*) > 10;

-- Real-time conversion rate monitoring
CREATE OR REPLACE VIEW realtime_cr AS
SELECT
    campaign_name,
    country_code,
    COUNT(*) FILTER (WHERE visit_timestamp > NOW() - INTERVAL '1 hour') as visits_1h,
    COUNT(*) FILTER (WHERE is_conversion AND visit_timestamp > NOW() - INTERVAL '1 hour') as conv_1h,
    COUNT(*) FILTER (WHERE visit_timestamp > NOW() - INTERVAL '24 hours') as visits_24h,
    COUNT(*) FILTER (WHERE is_conversion AND visit_timestamp > NOW() - INTERVAL '24 hours') as conv_24h,
    ROUND(
        COUNT(*) FILTER (WHERE is_conversion AND visit_timestamp > NOW() - INTERVAL '1 hour')::DECIMAL /
        NULLIF(COUNT(*) FILTER (WHERE visit_timestamp > NOW() - INTERVAL '1 hour'), 0) * 100, 2
    ) as cr_1h,
    ROUND(
        COUNT(*) FILTER (WHERE is_conversion AND visit_timestamp > NOW() - INTERVAL '24 hours')::DECIMAL /
        NULLIF(COUNT(*) FILTER (WHERE visit_timestamp > NOW() - INTERVAL '24 hours'), 0) * 100, 2
    ) as cr_24h
FROM visits
WHERE visit_timestamp > NOW() - INTERVAL '24 hours'
GROUP BY campaign_name, country_code
HAVING COUNT(*) > 50
ORDER BY visits_24h DESC;

-- =====================
-- ROW LEVEL SECURITY (optional)
-- =====================

-- Enable RLS
ALTER TABLE visits ENABLE ROW LEVEL SECURITY;
ALTER TABLE conversions ENABLE ROW LEVEL SECURITY;

-- Policy for service role (full access)
CREATE POLICY "Service role has full access to visits" ON visits
    FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Service role has full access to conversions" ON conversions
    FOR ALL USING (auth.role() = 'service_role');
