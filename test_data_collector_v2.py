"""
Unit tests for VoluumLiveCollector
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta
import asyncio

from data_collector_v2 import VoluumLiveCollector


class TestTimestampParsing:
    """Tests for _parse_timestamp method"""

    @pytest.fixture
    def collector(self):
        """Create collector with mocked dependencies"""
        with patch('data_collector_v2.create_client') as mock_supabase:
            mock_supabase.return_value = MagicMock()
            collector = VoluumLiveCollector(
                supabase_url="https://test.supabase.co",
                supabase_key="test-key",
                voluum_access_id="test-id",
                voluum_access_key="test-key"
            )
            return collector

    def test_parse_voluum_format(self, collector):
        """Test parsing Voluum's AM/PM timestamp format"""
        result = collector._parse_timestamp("2025-12-18 12:52:23 AM")
        assert result == "2025-12-18T00:52:23"

    def test_parse_voluum_format_pm(self, collector):
        """Test parsing PM timestamp"""
        result = collector._parse_timestamp("2025-12-18 03:30:45 PM")
        assert result == "2025-12-18T15:30:45"

    def test_parse_iso_format(self, collector):
        """Test parsing ISO format with Z suffix"""
        result = collector._parse_timestamp("2025-12-18T10:30:00Z")
        assert "2025-12-18" in result
        assert "10:30:00" in result

    def test_parse_iso_format_with_offset(self, collector):
        """Test parsing ISO format with timezone offset"""
        result = collector._parse_timestamp("2025-12-18T10:30:00+00:00")
        assert "2025-12-18" in result

    def test_parse_empty_string(self, collector):
        """Test parsing empty string returns None"""
        result = collector._parse_timestamp("")
        assert result is None

    def test_parse_none(self, collector):
        """Test parsing None returns None"""
        result = collector._parse_timestamp(None)
        assert result is None

    def test_parse_invalid_format(self, collector):
        """Test parsing invalid format returns original string"""
        result = collector._parse_timestamp("not-a-date")
        assert result == "not-a-date"


class TestTransformVisit:
    """Tests for transform_visit method"""

    @pytest.fixture
    def collector(self):
        with patch('data_collector_v2.create_client') as mock_supabase:
            mock_supabase.return_value = MagicMock()
            return VoluumLiveCollector(
                supabase_url="https://test.supabase.co",
                supabase_key="test-key",
                voluum_access_id="test-id",
                voluum_access_key="test-key"
            )

    def test_transform_visit_full(self, collector):
        """Test transforming a complete visit record"""
        raw = {
            "clickId": "abc123",
            "externalId": "ext456",
            "campaignId": "camp789",
            "campaignName": "Test Campaign",
            "trafficSourceId": "ts001",
            "trafficSourceName": "Facebook",
            "offerId": "offer001",
            "offerName": "Test Offer",
            "affiliateNetworkId": "an001",
            "affiliateNetworkName": "Network A",
            "landerId": "lander001",
            "landerName": "Landing Page 1",
            "timestamp": "2025-12-18 10:30:00 AM",
            "countryCode": "US",
            "countryName": "United States",
            "region": "California",
            "city": "Los Angeles",
            "device": "mobile",
            "deviceName": "iPhone",
            "brand": "Apple",
            "model": "iPhone 14",
            "os": "iOS",
            "osVersion": "17.0",
            "browser": "Safari",
            "browserVersion": "17.0",
            "connectionType": "wifi",
            "isp": "Verizon",
            "mobileCarrier": "Verizon Wireless",
            "ip": "192.168.1.1",
            "customVariable1": "var1",
            "customVariable2": "var2",
            "referrer": "https://google.com",
            "userAgent": "Mozilla/5.0..."
        }

        result = collector.transform_visit(raw)

        assert result["click_id"] == "abc123"
        assert result["external_id"] == "ext456"
        assert result["campaign_id"] == "camp789"
        assert result["campaign_name"] == "Test Campaign"
        assert result["country_code"] == "US"
        assert result["device"] == "mobile"
        assert result["os"] == "iOS"
        assert result["custom_var_1"] == "var1"
        assert result["custom_var_2"] == "var2"
        assert result["raw_data"] == raw

    def test_transform_visit_minimal(self, collector):
        """Test transforming a visit with minimal data"""
        raw = {
            "clickId": "abc123",
            "campaignId": "camp789"
        }

        result = collector.transform_visit(raw)

        assert result["click_id"] == "abc123"
        assert result["campaign_id"] == "camp789"
        assert result["country_code"] is None
        assert result["device"] is None

    def test_transform_visit_missing_click_id(self, collector):
        """Test transforming a visit without click_id"""
        raw = {"campaignId": "camp789"}
        result = collector.transform_visit(raw)
        assert result["click_id"] is None


class TestTransformClick:
    """Tests for transform_click method"""

    @pytest.fixture
    def collector(self):
        with patch('data_collector_v2.create_client') as mock_supabase:
            mock_supabase.return_value = MagicMock()
            return VoluumLiveCollector(
                supabase_url="https://test.supabase.co",
                supabase_key="test-key",
                voluum_access_id="test-id",
                voluum_access_key="test-key"
            )

    def test_transform_click_full(self, collector):
        """Test transforming a complete click record"""
        raw = {
            "clickId": "click123",
            "externalId": "ext456",
            "campaignId": "camp789",
            "campaignName": "Test Campaign",
            "offerId": "offer001",
            "offerName": "Test Offer",
            "landerId": "lander001",
            "landerName": "Landing Page 1",
            "timestamp": "2025-12-18 10:35:00 AM",
            "countryCode": "US",
            "countryName": "United States",
            "device": "mobile",
            "os": "iOS",
            "browser": "Safari",
            "ip": "192.168.1.1"
        }

        result = collector.transform_click(raw)

        assert result["click_id"] == "click123"
        assert result["campaign_id"] == "camp789"
        assert result["offer_id"] == "offer001"
        assert result["country_code"] == "US"
        assert result["raw_data"] == raw


class TestTransformConversion:
    """Tests for transform_conversion method"""

    @pytest.fixture
    def collector(self):
        with patch('data_collector_v2.create_client') as mock_supabase:
            mock_supabase.return_value = MagicMock()
            return VoluumLiveCollector(
                supabase_url="https://test.supabase.co",
                supabase_key="test-key",
                voluum_access_id="test-id",
                voluum_access_key="test-key"
            )

    def test_transform_conversion_full(self, collector):
        """Test transforming a complete conversion record"""
        raw = {
            "clickId": "conv123",
            "externalId": "ext456",
            "transactionId": "txn789",
            "campaignId": "camp001",
            "campaignName": "Voluum MB - Test",
            "offerId": "offer001",
            "offerName": "Test Offer",
            "affiliateNetworkId": "an001",
            "affiliateNetworkName": "Network A",
            "postbackTimestamp": "2025-12-18 10:45:00 AM",
            "visitTimestamp": "2025-12-18 10:30:00 AM",
            "countryCode": "US",
            "countryName": "United States",
            "revenue": 2.50,
            "payout": 2.00,
            "cost": 0.50,
            "profit": 1.50,
            "device": "mobile",
            "os": "iOS",
            "browser": "Safari",
            "connectionType": "wifi",
            "isp": "Verizon",
            "ip": "192.168.1.1",
            "customVariable1": "var1"
        }

        result = collector.transform_conversion(raw)

        assert result["click_id"] == "conv123"
        assert result["transaction_id"] == "txn789"
        assert result["campaign_name"] == "Voluum MB - Test"
        assert result["revenue"] == 2.50
        assert result["payout"] == 2.00
        assert result["profit"] == 1.50
        assert result["custom_var_1"] == "var1"

    def test_transform_conversion_zero_revenue(self, collector):
        """Test conversion with zero/null revenue"""
        raw = {
            "clickId": "conv123",
            "campaignId": "camp001",
            "revenue": None,
            "payout": 0,
            "cost": None,
            "profit": None
        }

        result = collector.transform_conversion(raw)

        assert result["revenue"] == 0.0
        assert result["payout"] == 0.0
        assert result["cost"] == 0.0
        assert result["profit"] == 0.0


class TestAuthentication:
    """Tests for authentication"""

    @pytest.fixture
    def collector(self):
        with patch('data_collector_v2.create_client') as mock_supabase:
            mock_supabase.return_value = MagicMock()
            return VoluumLiveCollector(
                supabase_url="https://test.supabase.co",
                supabase_key="test-key",
                voluum_access_id="test-id",
                voluum_access_key="test-key"
            )

    @pytest.mark.asyncio
    async def test_ensure_auth_new_token(self, collector):
        """Test getting a new auth token"""
        mock_response = MagicMock()
        mock_response.json.return_value = {"token": "new-token-123"}
        mock_response.raise_for_status = MagicMock()

        with patch('httpx.AsyncClient') as mock_client:
            mock_instance = AsyncMock()
            mock_instance.post = AsyncMock(return_value=mock_response)
            mock_client.return_value.__aenter__.return_value = mock_instance

            token = await collector._ensure_auth()

            assert token == "new-token-123"
            assert collector.token == "new-token-123"
            assert collector.token_expires is not None

    @pytest.mark.asyncio
    async def test_ensure_auth_cached_token(self, collector):
        """Test using cached token when not expired"""
        collector.token = "cached-token"
        collector.token_expires = datetime.now() + timedelta(hours=1)

        token = await collector._ensure_auth()

        assert token == "cached-token"

    @pytest.mark.asyncio
    async def test_ensure_auth_expired_token(self, collector):
        """Test refreshing expired token"""
        collector.token = "old-token"
        collector.token_expires = datetime.now() - timedelta(hours=1)

        mock_response = MagicMock()
        mock_response.json.return_value = {"token": "new-token"}
        mock_response.raise_for_status = MagicMock()

        with patch('httpx.AsyncClient') as mock_client:
            mock_instance = AsyncMock()
            mock_instance.post = AsyncMock(return_value=mock_response)
            mock_client.return_value.__aenter__.return_value = mock_instance

            token = await collector._ensure_auth()

            assert token == "new-token"


class TestGetTrackedCampaigns:
    """Tests for get_tracked_campaigns method"""

    @pytest.fixture
    def collector(self):
        with patch('data_collector_v2.create_client') as mock_supabase:
            mock_supabase.return_value = MagicMock()
            return VoluumLiveCollector(
                supabase_url="https://test.supabase.co",
                supabase_key="test-key",
                voluum_access_id="test-id",
                voluum_access_key="test-key",
                campaign_filter="Voluum MB"
            )

    @pytest.mark.asyncio
    async def test_get_tracked_campaigns_filters_correctly(self, collector):
        """Test that campaigns are filtered by name and traffic"""
        mock_data = {
            "rows": [
                {"campaignId": "c1", "campaignName": "Voluum MB - US", "visits": 100},
                {"campaignId": "c2", "campaignName": "Other Campaign", "visits": 50},
                {"campaignId": "c3", "campaignName": "Voluum MB - UK", "visits": 0},
                {"campaignId": "c4", "campaignName": "Voluum MB - CA", "visits": 75},
            ]
        }

        with patch.object(collector, '_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = mock_data

            campaigns = await collector.get_tracked_campaigns()

            # Should only include Voluum MB campaigns with visits > 0
            assert len(campaigns) == 2
            assert campaigns[0]["campaign_id"] == "c1"
            assert campaigns[1]["campaign_id"] == "c4"

    @pytest.mark.asyncio
    async def test_get_tracked_campaigns_empty(self, collector):
        """Test handling empty campaign list"""
        with patch.object(collector, '_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"rows": []}

            campaigns = await collector.get_tracked_campaigns()

            assert campaigns == []


class TestSyncLiveVisits:
    """Tests for sync_live_visits method"""

    @pytest.fixture
    def collector(self):
        with patch('data_collector_v2.create_client') as mock_supabase:
            mock_supabase_instance = MagicMock()
            mock_supabase.return_value = mock_supabase_instance

            collector = VoluumLiveCollector(
                supabase_url="https://test.supabase.co",
                supabase_key="test-key",
                voluum_access_id="test-id",
                voluum_access_key="test-key"
            )
            return collector

    @pytest.mark.asyncio
    async def test_sync_live_visits_success(self, collector):
        """Test successful visit sync"""
        mock_visits = {
            "rows": [
                {"clickId": "v1", "campaignId": "c1", "timestamp": "2025-12-18 10:00:00 AM"},
                {"clickId": "v2", "campaignId": "c1", "timestamp": "2025-12-18 10:01:00 AM"},
            ]
        }

        with patch.object(collector, '_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = mock_visits

            # Mock supabase upsert
            mock_upsert = MagicMock()
            mock_upsert.execute.return_value = MagicMock()
            collector.supabase.table.return_value.upsert.return_value = mock_upsert

            count = await collector.sync_live_visits(["c1"])

            assert count == 2
            collector.supabase.table.assert_called_with("live_visits")

    @pytest.mark.asyncio
    async def test_sync_live_visits_deduplication(self, collector):
        """Test that duplicate click_ids are deduplicated"""
        mock_visits = {
            "rows": [
                {"clickId": "v1", "campaignId": "c1"},
                {"clickId": "v1", "campaignId": "c1"},  # Duplicate
                {"clickId": "v2", "campaignId": "c1"},
            ]
        }

        with patch.object(collector, '_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = mock_visits

            mock_upsert = MagicMock()
            mock_upsert.execute.return_value = MagicMock()
            collector.supabase.table.return_value.upsert.return_value = mock_upsert

            count = await collector.sync_live_visits(["c1"])

            # Should only sync 2 unique visits
            assert count == 2

    @pytest.mark.asyncio
    async def test_sync_live_visits_handles_error(self, collector):
        """Test that errors are handled gracefully"""
        with patch.object(collector, '_request', new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = Exception("API Error")

            count = await collector.sync_live_visits(["c1"])

            # Should continue and return 0 on error
            assert count == 0


class TestSyncConversions:
    """Tests for sync_conversions method"""

    @pytest.fixture
    def collector(self):
        with patch('data_collector_v2.create_client') as mock_supabase:
            mock_supabase_instance = MagicMock()
            mock_supabase.return_value = mock_supabase_instance

            return VoluumLiveCollector(
                supabase_url="https://test.supabase.co",
                supabase_key="test-key",
                voluum_access_id="test-id",
                voluum_access_key="test-key",
                campaign_filter="Voluum MB"
            )

    @pytest.mark.asyncio
    async def test_sync_conversions_filters_by_campaign(self, collector):
        """Test that conversions are filtered by campaign name"""
        mock_conversions = {
            "rows": [
                {"clickId": "c1", "campaignName": "Voluum MB - US", "postbackTimestamp": "2025-12-18T10:00:00"},
                {"clickId": "c2", "campaignName": "Other Campaign", "postbackTimestamp": "2025-12-18T10:01:00"},
                {"clickId": "c3", "campaignName": "Voluum MB - UK", "postbackTimestamp": "2025-12-18T10:02:00"},
            ]
        }

        with patch.object(collector, '_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = mock_conversions

            mock_upsert = MagicMock()
            mock_upsert.execute.return_value = MagicMock()
            collector.supabase.table.return_value.upsert.return_value = mock_upsert

            count = await collector.sync_conversions(days_back=1)

            # Should only sync Voluum MB conversions
            assert count == 2


class TestInitialization:
    """Tests for collector initialization"""

    def test_init_missing_supabase_url(self):
        """Test error when Supabase URL is missing"""
        with patch.dict('os.environ', {}, clear=True):
            with pytest.raises(ValueError, match="SUPABASE_URL"):
                VoluumLiveCollector(
                    supabase_key="test-key",
                    voluum_access_id="test-id",
                    voluum_access_key="test-key"
                )

    def test_init_missing_voluum_credentials(self):
        """Test error when Voluum credentials are missing"""
        with patch('data_collector_v2.create_client'):
            with patch.dict('os.environ', {}, clear=True):
                with pytest.raises(ValueError, match="VOLUUM_ACCESS"):
                    VoluumLiveCollector(
                        supabase_url="https://test.supabase.co",
                        supabase_key="test-key"
                    )

    def test_init_with_custom_filter(self):
        """Test initialization with custom campaign filter"""
        with patch('data_collector_v2.create_client') as mock_supabase:
            mock_supabase.return_value = MagicMock()

            collector = VoluumLiveCollector(
                supabase_url="https://test.supabase.co",
                supabase_key="test-key",
                voluum_access_id="test-id",
                voluum_access_key="test-key",
                campaign_filter="Custom Filter"
            )

            assert collector.campaign_filter == "Custom Filter"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
