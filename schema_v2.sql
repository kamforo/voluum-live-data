-- Voluum Visit Data Schema v2 for Supabase
-- 3 tables: live_visits, live_clicks, conversions
-- Joined by click_id

-- =====================================================
-- LIVE VISITS TABLE
-- Individual visit records from /report/live/visits
-- =====================================================
CREATE TABLE IF NOT EXISTS live_visits (
    id BIGSERIAL PRIMARY KEY,
    click_id TEXT UNIQUE NOT NULL,
    external_id TEXT,

    -- Campaign
    campaign_id TEXT NOT NULL,
    campaign_name TEXT,

    -- Traffic Source
    traffic_source_id TEXT,
    traffic_source_name TEXT,

    -- Offer
    offer_id TEXT,
    offer_name TEXT,
    affiliate_network_id TEXT,
    affiliate_network_name TEXT,

    -- Lander
    lander_id TEXT,
    lander_name TEXT,

    -- Timing
    visit_timestamp TIMESTAMPTZ,

    -- Geo
    country_code TEXT,
    country_name TEXT,
    region TEXT,
    city TEXT,

    -- Device
    device TEXT,
    device_name TEXT,
    brand TEXT,
    model TEXT,
    os TEXT,
    os_version TEXT,
    browser TEXT,
    browser_version TEXT,

    -- Connection
    connection_type TEXT,
    isp TEXT,
    mobile_carrier TEXT,
    ip TEXT,

    -- Custom Variables
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

    -- Request info
    referrer TEXT,
    user_agent TEXT,

    -- Metadata
    raw_data JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- =====================================================
-- LIVE CLICKS TABLE
-- Individual click records from /report/live/clicks
-- =====================================================
CREATE TABLE IF NOT EXISTS live_clicks (
    id BIGSERIAL PRIMARY KEY,
    click_id TEXT UNIQUE NOT NULL,
    external_id TEXT,

    -- Campaign
    campaign_id TEXT NOT NULL,
    campaign_name TEXT,

    -- Offer
    offer_id TEXT,
    offer_name TEXT,

    -- Lander
    lander_id TEXT,
    lander_name TEXT,

    -- Timing
    click_timestamp TIMESTAMPTZ,

    -- Geo
    country_code TEXT,
    country_name TEXT,

    -- Device
    device TEXT,
    os TEXT,
    browser TEXT,

    -- Connection
    ip TEXT,

    -- Metadata
    raw_data JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- =====================================================
-- CONVERSIONS TABLE
-- Historical conversion data from /report/conversions
-- =====================================================
CREATE TABLE IF NOT EXISTS conversions (
    id BIGSERIAL PRIMARY KEY,
    click_id TEXT NOT NULL,
    external_id TEXT,
    transaction_id TEXT,

    -- Campaign
    campaign_id TEXT NOT NULL,
    campaign_name TEXT,

    -- Offer
    offer_id TEXT,
    offer_name TEXT,
    affiliate_network_id TEXT,
    affiliate_network_name TEXT,

    -- Timing
    postback_timestamp TIMESTAMPTZ,
    visit_timestamp TIMESTAMPTZ,

    -- Geo
    country_code TEXT,
    country_name TEXT,

    -- Revenue
    revenue DECIMAL(12,6) DEFAULT 0,
    payout DECIMAL(12,6) DEFAULT 0,
    cost DECIMAL(12,6) DEFAULT 0,
    profit DECIMAL(12,6) DEFAULT 0,

    -- Device
    device TEXT,
    os TEXT,
    browser TEXT,

    -- Connection
    connection_type TEXT,
    isp TEXT,
    ip TEXT,

    -- Custom Variables (from click)
    custom_var_1 TEXT,
    custom_var_2 TEXT,
    custom_var_3 TEXT,
    custom_var_4 TEXT,
    custom_var_5 TEXT,

    -- Metadata
    raw_data JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(click_id, postback_timestamp)
);

-- =====================================================
-- SYNC STATE TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS sync_state (
    id SERIAL PRIMARY KEY,
    sync_type TEXT UNIQUE NOT NULL,
    last_sync_timestamp TIMESTAMPTZ,
    last_click_id TEXT,
    records_synced BIGINT DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Initialize sync state
INSERT INTO sync_state (sync_type, last_sync_timestamp) VALUES
    ('live_visits', NOW() - INTERVAL '1 hour'),
    ('live_clicks', NOW() - INTERVAL '1 hour'),
    ('conversions', NOW() - INTERVAL '1 day')
ON CONFLICT (sync_type) DO NOTHING;

-- =====================================================
-- CAMPAIGN TRACKING TABLE
-- Track which campaigns to sync for live data
-- =====================================================
CREATE TABLE IF NOT EXISTS tracked_campaigns (
    id SERIAL PRIMARY KEY,
    campaign_id TEXT UNIQUE NOT NULL,
    campaign_name TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    last_synced TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- =====================================================
-- INDEXES
-- =====================================================

-- Live visits indexes
CREATE INDEX IF NOT EXISTS idx_live_visits_click_id ON live_visits(click_id);
CREATE INDEX IF NOT EXISTS idx_live_visits_campaign ON live_visits(campaign_id);
CREATE INDEX IF NOT EXISTS idx_live_visits_timestamp ON live_visits(visit_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_live_visits_country ON live_visits(country_code);
CREATE INDEX IF NOT EXISTS idx_live_visits_created ON live_visits(created_at DESC);

-- Live clicks indexes
CREATE INDEX IF NOT EXISTS idx_live_clicks_click_id ON live_clicks(click_id);
CREATE INDEX IF NOT EXISTS idx_live_clicks_campaign ON live_clicks(campaign_id);
CREATE INDEX IF NOT EXISTS idx_live_clicks_timestamp ON live_clicks(click_timestamp DESC);

-- Conversions indexes
CREATE INDEX IF NOT EXISTS idx_conversions_click_id ON conversions(click_id);
CREATE INDEX IF NOT EXISTS idx_conversions_campaign ON conversions(campaign_id);
CREATE INDEX IF NOT EXISTS idx_conversions_timestamp ON conversions(postback_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_conversions_country ON conversions(country_code);

-- =====================================================
-- USEFUL VIEWS
-- =====================================================

-- Joined view: visits with their conversions
CREATE OR REPLACE VIEW visit_conversions AS
SELECT
    v.click_id,
    v.visit_timestamp,
    v.campaign_id,
    v.campaign_name,
    v.country_code,
    v.device,
    v.os,
    v.browser,
    v.isp,
    v.custom_var_1,
    v.custom_var_2,
    c.postback_timestamp as conversion_timestamp,
    c.revenue,
    c.payout,
    c.offer_name,
    c.transaction_id,
    CASE WHEN c.click_id IS NOT NULL THEN TRUE ELSE FALSE END as converted
FROM live_visits v
LEFT JOIN conversions c ON v.click_id = c.click_id;

-- Daily performance by campaign
CREATE OR REPLACE VIEW daily_campaign_performance AS
SELECT
    DATE(v.visit_timestamp) as date,
    v.campaign_id,
    v.campaign_name,
    COUNT(DISTINCT v.click_id) as visits,
    COUNT(DISTINCT c.click_id) as conversions,
    COALESCE(SUM(c.revenue), 0) as revenue,
    ROUND(COUNT(DISTINCT c.click_id)::DECIMAL / NULLIF(COUNT(DISTINCT v.click_id), 0) * 100, 2) as cr
FROM live_visits v
LEFT JOIN conversions c ON v.click_id = c.click_id
WHERE v.visit_timestamp > NOW() - INTERVAL '30 days'
GROUP BY DATE(v.visit_timestamp), v.campaign_id, v.campaign_name
ORDER BY date DESC, revenue DESC;

-- Conversion rate by country
CREATE OR REPLACE VIEW country_cr AS
SELECT
    v.country_code,
    COUNT(DISTINCT v.click_id) as visits,
    COUNT(DISTINCT c.click_id) as conversions,
    COALESCE(SUM(c.revenue), 0) as revenue,
    ROUND(COUNT(DISTINCT c.click_id)::DECIMAL / NULLIF(COUNT(DISTINCT v.click_id), 0) * 100, 2) as cr
FROM live_visits v
LEFT JOIN conversions c ON v.click_id = c.click_id
WHERE v.visit_timestamp > NOW() - INTERVAL '7 days'
GROUP BY v.country_code
HAVING COUNT(DISTINCT v.click_id) > 10
ORDER BY revenue DESC;

-- Conversion rate by device/OS
CREATE OR REPLACE VIEW device_os_cr AS
SELECT
    v.device,
    v.os,
    COUNT(DISTINCT v.click_id) as visits,
    COUNT(DISTINCT c.click_id) as conversions,
    COALESCE(SUM(c.revenue), 0) as revenue,
    ROUND(COUNT(DISTINCT c.click_id)::DECIMAL / NULLIF(COUNT(DISTINCT v.click_id), 0) * 100, 2) as cr
FROM live_visits v
LEFT JOIN conversions c ON v.click_id = c.click_id
WHERE v.visit_timestamp > NOW() - INTERVAL '7 days'
GROUP BY v.device, v.os
HAVING COUNT(DISTINCT v.click_id) > 10
ORDER BY revenue DESC;

-- =====================================================
-- RETENTION CLEANUP
-- =====================================================
CREATE OR REPLACE FUNCTION cleanup_old_data(retention_days INTEGER DEFAULT 90)
RETURNS TABLE(visits_deleted BIGINT, clicks_deleted BIGINT, conversions_deleted BIGINT)
LANGUAGE plpgsql
AS $$
DECLARE
    v_deleted BIGINT;
    c_deleted BIGINT;
    conv_deleted BIGINT;
    cutoff_date TIMESTAMPTZ;
BEGIN
    cutoff_date := NOW() - (retention_days || ' days')::INTERVAL;

    WITH deleted AS (
        DELETE FROM live_visits WHERE visit_timestamp < cutoff_date RETURNING 1
    ) SELECT COUNT(*) INTO v_deleted FROM deleted;

    WITH deleted AS (
        DELETE FROM live_clicks WHERE click_timestamp < cutoff_date RETURNING 1
    ) SELECT COUNT(*) INTO c_deleted FROM deleted;

    WITH deleted AS (
        DELETE FROM conversions WHERE postback_timestamp < cutoff_date RETURNING 1
    ) SELECT COUNT(*) INTO conv_deleted FROM deleted;

    RETURN QUERY SELECT v_deleted, c_deleted, conv_deleted;
END;
$$;
