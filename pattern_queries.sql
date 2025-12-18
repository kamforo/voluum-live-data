-- =====================================================
-- PATTERN RECOGNITION QUERIES FOR VOLUUM DATA
-- Run these in Supabase SQL Editor or via Python
-- =====================================================

-- =====================================================
-- 1. ANOMALY DETECTION
-- =====================================================

-- Detect campaigns with unusual conversion rate (2+ std devs from mean)
CREATE OR REPLACE VIEW anomaly_cr AS
WITH campaign_stats AS (
    SELECT
        campaign_id,
        campaign_name,
        AVG(CASE WHEN is_conversion THEN 1.0 ELSE 0.0 END) as avg_cr,
        STDDEV(CASE WHEN is_conversion THEN 1.0 ELSE 0.0 END) as stddev_cr,
        COUNT(*) as visits
    FROM visits
    WHERE visit_timestamp > NOW() - INTERVAL '7 days'
    GROUP BY campaign_id, campaign_name
    HAVING COUNT(*) > 100
),
recent_cr AS (
    SELECT
        campaign_id,
        AVG(CASE WHEN is_conversion THEN 1.0 ELSE 0.0 END) as current_cr
    FROM visits
    WHERE visit_timestamp > NOW() - INTERVAL '1 hour'
    GROUP BY campaign_id
    HAVING COUNT(*) > 10
)
SELECT
    cs.campaign_name,
    ROUND(cs.avg_cr * 100, 2) as avg_cr_pct,
    ROUND(rc.current_cr * 100, 2) as current_cr_pct,
    ROUND((rc.current_cr - cs.avg_cr) / NULLIF(cs.stddev_cr, 0), 2) as z_score,
    CASE
        WHEN (rc.current_cr - cs.avg_cr) / NULLIF(cs.stddev_cr, 0) > 2 THEN 'HIGH'
        WHEN (rc.current_cr - cs.avg_cr) / NULLIF(cs.stddev_cr, 0) < -2 THEN 'LOW'
        ELSE 'NORMAL'
    END as status
FROM campaign_stats cs
JOIN recent_cr rc ON cs.campaign_id = rc.campaign_id
WHERE ABS((rc.current_cr - cs.avg_cr) / NULLIF(cs.stddev_cr, 0)) > 2
ORDER BY ABS((rc.current_cr - cs.avg_cr) / NULLIF(cs.stddev_cr, 0)) DESC;


-- Detect sudden traffic drops (>50% below hourly average)
CREATE OR REPLACE VIEW traffic_drops AS
WITH hourly_avg AS (
    SELECT
        campaign_id,
        campaign_name,
        EXTRACT(HOUR FROM visit_timestamp) as hour,
        AVG(COUNT(*)) OVER (
            PARTITION BY campaign_id, EXTRACT(HOUR FROM visit_timestamp)
        ) as avg_hourly_visits
    FROM visits
    WHERE visit_timestamp > NOW() - INTERVAL '7 days'
    GROUP BY campaign_id, campaign_name, DATE_TRUNC('hour', visit_timestamp)
),
current_hour AS (
    SELECT
        campaign_id,
        COUNT(*) as current_visits
    FROM visits
    WHERE visit_timestamp > NOW() - INTERVAL '1 hour'
    GROUP BY campaign_id
)
SELECT DISTINCT
    ha.campaign_name,
    ROUND(ha.avg_hourly_visits) as avg_hourly,
    ch.current_visits,
    ROUND((1 - ch.current_visits::DECIMAL / NULLIF(ha.avg_hourly_visits, 0)) * 100, 1) as drop_pct
FROM hourly_avg ha
JOIN current_hour ch ON ha.campaign_id = ch.campaign_id
WHERE ha.hour = EXTRACT(HOUR FROM NOW())
AND ch.current_visits < ha.avg_hourly_visits * 0.5
ORDER BY drop_pct DESC;


-- =====================================================
-- 2. TIME-BASED PATTERNS
-- =====================================================

-- Best performing hours by day of week
CREATE OR REPLACE VIEW best_hours AS
SELECT
    CASE EXTRACT(DOW FROM visit_timestamp)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END as day_of_week,
    EXTRACT(HOUR FROM visit_timestamp)::INTEGER as hour,
    COUNT(*) as visits,
    SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) as conversions,
    SUM(revenue) as revenue,
    ROUND(SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END)::DECIMAL /
          NULLIF(COUNT(*), 0) * 100, 2) as cr
FROM visits
WHERE visit_timestamp > NOW() - INTERVAL '30 days'
GROUP BY EXTRACT(DOW FROM visit_timestamp), EXTRACT(HOUR FROM visit_timestamp)
HAVING COUNT(*) > 100
ORDER BY revenue DESC
LIMIT 20;


-- Hourly revenue heatmap data
CREATE OR REPLACE VIEW hourly_heatmap AS
SELECT
    EXTRACT(DOW FROM visit_timestamp)::INTEGER as day_of_week,
    EXTRACT(HOUR FROM visit_timestamp)::INTEGER as hour,
    SUM(revenue) as total_revenue,
    AVG(revenue) as avg_revenue,
    COUNT(*) as visits
FROM visits
WHERE visit_timestamp > NOW() - INTERVAL '30 days'
GROUP BY EXTRACT(DOW FROM visit_timestamp), EXTRACT(HOUR FROM visit_timestamp);


-- =====================================================
-- 3. GEO PATTERNS
-- =====================================================

-- Top performing countries with trend
CREATE OR REPLACE VIEW country_trends AS
WITH weekly AS (
    SELECT
        country_code,
        DATE_TRUNC('week', visit_timestamp) as week,
        SUM(revenue) as revenue,
        COUNT(*) as visits,
        SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) as conversions
    FROM visits
    WHERE visit_timestamp > NOW() - INTERVAL '30 days'
    GROUP BY country_code, DATE_TRUNC('week', visit_timestamp)
)
SELECT
    country_code,
    SUM(revenue) as total_revenue,
    SUM(visits) as total_visits,
    SUM(conversions) as total_conversions,
    ROUND(SUM(conversions)::DECIMAL / NULLIF(SUM(visits), 0) * 100, 2) as cr,
    -- Week over week trend
    ROUND((
        SUM(CASE WHEN week = DATE_TRUNC('week', NOW()) THEN revenue ELSE 0 END) -
        SUM(CASE WHEN week = DATE_TRUNC('week', NOW() - INTERVAL '1 week') THEN revenue ELSE 0 END)
    ) / NULLIF(SUM(CASE WHEN week = DATE_TRUNC('week', NOW() - INTERVAL '1 week') THEN revenue ELSE 0 END), 0) * 100, 1) as wow_change_pct
FROM weekly
GROUP BY country_code
HAVING SUM(visits) > 100
ORDER BY total_revenue DESC;


-- Emerging countries (growing fast)
CREATE OR REPLACE VIEW emerging_countries AS
WITH period_stats AS (
    SELECT
        country_code,
        SUM(CASE WHEN visit_timestamp > NOW() - INTERVAL '7 days' THEN revenue ELSE 0 END) as recent_revenue,
        SUM(CASE WHEN visit_timestamp BETWEEN NOW() - INTERVAL '14 days' AND NOW() - INTERVAL '7 days' THEN revenue ELSE 0 END) as previous_revenue
    FROM visits
    WHERE visit_timestamp > NOW() - INTERVAL '14 days'
    GROUP BY country_code
)
SELECT
    country_code,
    recent_revenue,
    previous_revenue,
    ROUND((recent_revenue - previous_revenue) / NULLIF(previous_revenue, 0) * 100, 1) as growth_pct
FROM period_stats
WHERE previous_revenue > 0
AND recent_revenue > previous_revenue * 1.5  -- 50%+ growth
ORDER BY growth_pct DESC
LIMIT 20;


-- =====================================================
-- 4. CAMPAIGN PERFORMANCE PATTERNS
-- =====================================================

-- Campaign momentum (trending up or down)
CREATE OR REPLACE VIEW campaign_momentum AS
WITH daily_stats AS (
    SELECT
        campaign_id,
        campaign_name,
        DATE(visit_timestamp) as date,
        SUM(revenue) as revenue,
        SUM(profit) as profit,
        COUNT(*) as visits
    FROM visits
    WHERE visit_timestamp > NOW() - INTERVAL '14 days'
    GROUP BY campaign_id, campaign_name, DATE(visit_timestamp)
),
momentum AS (
    SELECT
        campaign_id,
        campaign_name,
        SUM(CASE WHEN date > NOW() - INTERVAL '7 days' THEN revenue ELSE 0 END) as recent_revenue,
        SUM(CASE WHEN date <= NOW() - INTERVAL '7 days' THEN revenue ELSE 0 END) as previous_revenue,
        SUM(CASE WHEN date > NOW() - INTERVAL '7 days' THEN profit ELSE 0 END) as recent_profit,
        SUM(CASE WHEN date <= NOW() - INTERVAL '7 days' THEN profit ELSE 0 END) as previous_profit
    FROM daily_stats
    GROUP BY campaign_id, campaign_name
)
SELECT
    campaign_name,
    ROUND(recent_revenue, 2) as recent_7d_revenue,
    ROUND(previous_revenue, 2) as previous_7d_revenue,
    ROUND((recent_revenue - previous_revenue) / NULLIF(previous_revenue, 0) * 100, 1) as revenue_change_pct,
    ROUND(recent_profit, 2) as recent_7d_profit,
    CASE
        WHEN recent_revenue > previous_revenue * 1.2 THEN 'GROWING'
        WHEN recent_revenue < previous_revenue * 0.8 THEN 'DECLINING'
        ELSE 'STABLE'
    END as momentum
FROM momentum
WHERE (recent_revenue + previous_revenue) > 100  -- Only campaigns with meaningful revenue
ORDER BY recent_revenue DESC;


-- Campaign saturation (diminishing returns)
CREATE OR REPLACE VIEW campaign_saturation AS
WITH daily_metrics AS (
    SELECT
        campaign_id,
        campaign_name,
        DATE(visit_timestamp) as date,
        COUNT(*) as visits,
        SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) as conversions,
        SUM(revenue) / NULLIF(COUNT(*), 0) as epv
    FROM visits
    WHERE visit_timestamp > NOW() - INTERVAL '30 days'
    GROUP BY campaign_id, campaign_name, DATE(visit_timestamp)
)
SELECT
    campaign_name,
    ROUND(AVG(epv), 4) as avg_epv,
    ROUND(REGR_SLOPE(epv, EXTRACT(EPOCH FROM date)) * 86400, 6) as epv_daily_trend,
    CASE
        WHEN REGR_SLOPE(epv, EXTRACT(EPOCH FROM date)) < -0.0001 THEN 'SATURATING'
        WHEN REGR_SLOPE(epv, EXTRACT(EPOCH FROM date)) > 0.0001 THEN 'IMPROVING'
        ELSE 'STABLE'
    END as status
FROM daily_metrics
GROUP BY campaign_id, campaign_name
HAVING COUNT(*) >= 7  -- At least 7 days of data
ORDER BY avg_epv DESC;


-- =====================================================
-- 5. CONVERSION PATTERNS
-- =====================================================

-- Time to convert distribution
CREATE OR REPLACE VIEW conversion_timing AS
SELECT
    campaign_name,
    CASE
        WHEN EXTRACT(EPOCH FROM time_to_convert) < 60 THEN '< 1 min'
        WHEN EXTRACT(EPOCH FROM time_to_convert) < 300 THEN '1-5 min'
        WHEN EXTRACT(EPOCH FROM time_to_convert) < 900 THEN '5-15 min'
        WHEN EXTRACT(EPOCH FROM time_to_convert) < 3600 THEN '15-60 min'
        WHEN EXTRACT(EPOCH FROM time_to_convert) < 86400 THEN '1-24 hours'
        ELSE '> 24 hours'
    END as time_bucket,
    COUNT(*) as conversions,
    SUM(revenue) as revenue
FROM conversions
WHERE conversion_timestamp > NOW() - INTERVAL '30 days'
AND time_to_convert IS NOT NULL
GROUP BY campaign_name, 1
ORDER BY campaign_name, conversions DESC;


-- Conversion rate by device and country
CREATE OR REPLACE VIEW cr_by_segment AS
SELECT
    device_type,
    country_code,
    COUNT(*) as visits,
    SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) as conversions,
    ROUND(SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END)::DECIMAL / NULLIF(COUNT(*), 0) * 100, 2) as cr,
    SUM(revenue) as revenue,
    ROUND(SUM(revenue) / NULLIF(COUNT(*), 0), 4) as epv
FROM visits
WHERE visit_timestamp > NOW() - INTERVAL '30 days'
GROUP BY device_type, country_code
HAVING COUNT(*) > 100
ORDER BY revenue DESC;


-- =====================================================
-- 6. REAL-TIME ALERTS (use with Supabase Edge Functions)
-- =====================================================

-- Function to check for alerts
CREATE OR REPLACE FUNCTION check_alerts()
RETURNS TABLE (
    alert_type TEXT,
    campaign_name TEXT,
    message TEXT,
    severity TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Check for traffic drops
    RETURN QUERY
    SELECT
        'traffic_drop'::TEXT,
        v.campaign_name,
        format('Traffic dropped %s%% in last hour', ROUND((1 - current_visits::DECIMAL / NULLIF(avg_visits, 0)) * 100, 0)),
        'warning'::TEXT
    FROM (
        SELECT
            campaign_id,
            campaign_name,
            COUNT(*) as current_visits,
            AVG(COUNT(*)) OVER (PARTITION BY campaign_id) as avg_visits
        FROM visits
        WHERE visit_timestamp > NOW() - INTERVAL '24 hours'
        GROUP BY campaign_id, campaign_name, DATE_TRUNC('hour', visit_timestamp)
    ) v
    WHERE v.current_visits < v.avg_visits * 0.3
    LIMIT 10;

    -- Check for CR anomalies
    RETURN QUERY
    SELECT
        'cr_anomaly'::TEXT,
        a.campaign_name,
        format('CR is %s%% vs avg %s%%', a.current_cr_pct, a.avg_cr_pct),
        CASE WHEN a.status = 'HIGH' THEN 'info' ELSE 'warning' END
    FROM anomaly_cr a
    LIMIT 10;
END;
$$;


-- =====================================================
-- 7. ML FEATURE EXTRACTION
-- =====================================================

-- Features for ML model training
CREATE OR REPLACE VIEW ml_features AS
SELECT
    visit_id,
    campaign_id,

    -- Time features
    EXTRACT(HOUR FROM visit_timestamp)::INTEGER as hour,
    EXTRACT(DOW FROM visit_timestamp)::INTEGER as day_of_week,
    EXTRACT(DAY FROM visit_timestamp)::INTEGER as day_of_month,

    -- Categorical features (encode these)
    country_code,
    device_type,
    os,
    browser,
    connection_type,

    -- Historical features for this campaign
    (SELECT COUNT(*) FROM visits v2
     WHERE v2.campaign_id = visits.campaign_id
     AND v2.visit_timestamp < visits.visit_timestamp
     AND v2.visit_timestamp > visits.visit_timestamp - INTERVAL '1 hour') as campaign_visits_1h,

    (SELECT AVG(CASE WHEN is_conversion THEN 1.0 ELSE 0.0 END) FROM visits v2
     WHERE v2.campaign_id = visits.campaign_id
     AND v2.visit_timestamp < visits.visit_timestamp
     AND v2.visit_timestamp > visits.visit_timestamp - INTERVAL '24 hours') as campaign_cr_24h,

    -- Target variable
    is_conversion

FROM visits
WHERE visit_timestamp > NOW() - INTERVAL '30 days';
