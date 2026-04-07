"""Tests for new weather service features."""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

import pytest
from fastapi.testclient import TestClient

from weather.app import app

client = TestClient(app)


class TestBatchWeatherEndpoint:
    """Test batch weather request endpoint."""

    def test_batch_weather_single_location(self):
        """Test batch weather with single location."""
        payload = {
            "locations": [{"lat": 43.2567, "lon": 76.9286}],
            "units": "metric",
        }
        response = client.post("/weather/batch", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert "results" in data
        assert "errors" in data

    def test_batch_weather_multiple_locations(self):
        """Test batch weather with multiple locations."""
        payload = {
            "locations": [
                {"lat": 43.2567, "lon": 76.9286},
                {"lat": 51.5074, "lon": -0.1278},
                {"lat": 35.6762, "lon": 139.6503},
            ],
            "units": "metric",
        }
        response = client.post("/weather/batch", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert "results" in data
        assert isinstance(data["results"], list)

    def test_batch_weather_invalid_location(self):
        """Test batch weather with invalid latitude."""
        payload = {
            "locations": [{"lat": 91.0, "lon": 76.9286}],  # Invalid latitude > 90
            "units": "metric",
        }
        response = client.post("/weather/batch", json=payload)
        assert response.status_code == 422  # Validation error


class TestMetricsEndpoint:
    """Test metrics endpoint."""

    def test_get_metrics(self):
        """Test retrieving metrics."""
        response = client.get("/metrics")
        assert response.status_code == 200
        data = response.json()
        assert "metrics" in data
        assert "cache_size" in data
        assert "active_circuits" in data
        assert "requests" in data["metrics"]
        assert "cache" in data["metrics"]

    def test_metrics_have_required_fields(self):
        """Test that metrics contain required fields."""
        response = client.get("/metrics")
        metrics = response.json()["metrics"]
        assert "total" in metrics["requests"]
        assert "by_provider" in metrics["requests"]
        assert "hits" in metrics["cache"]
        assert "misses" in metrics["cache"]


class TestCacheOperations:
    """Test cache management endpoints."""

    def test_clear_cache(self):
        """Test clearing cache."""
        # First make a request to populate cache
        client.get("/weather?lat=43.2567&lon=76.9286")

        # Clear cache
        response = client.delete("/cache")
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "cleared" in data["message"].lower()

    def test_get_cache_stats(self):
        """Test retrieving cache statistics."""
        response = client.get("/cache/stats")
        assert response.status_code == 200
        data = response.json()
        assert "size" in data
        assert "hits" in data
        assert "misses" in data
        assert "hit_rate" in data
        assert "entries" in data

    def test_cache_hit_rate_calculation(self):
        """Test that cache hit rate is calculated correctly."""
        # Clear metrics and cache
        client.delete("/cache/metrics")
        client.delete("/cache")

        # Make requests to the same location
        client.get("/weather?lat=43.2567&lon=76.9286&units=metric")
        client.get("/weather?lat=43.2567&lon=76.9286&units=metric")

        # Check hit rate
        response = client.get("/cache/stats")
        stats = response.json()
        # Second request should be a cache hit
        assert stats["hits"] >= 1


class TestCircuitBreakerReset:
    """Test circuit breaker reset endpoint."""

    def test_reset_all_circuit_breakers(self):
        """Test resetting all circuit breakers."""
        response = client.post("/circuit-breaker/reset")
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "all" in data["message"].lower()

    def test_reset_specific_circuit_breaker(self):
        """Test resetting specific provider circuit breaker."""
        response = client.post("/circuit-breaker/reset?provider=mock")
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "mock" in data["message"].lower()


class TestInfoEndpoint:
    """Test service info endpoint."""

    def test_get_info(self):
        """Test retrieving service information."""
        response = client.get("/info")
        assert response.status_code == 200
        data = response.json()
        assert "service" in data
        assert "version" in data
        assert "uptime_seconds" in data
        assert "providers" in data
        assert "configuration" in data

    def test_info_contains_providers(self):
        """Test that info contains provider configuration."""
        response = client.get("/info")
        info = response.json()
        assert "current" in info["providers"]
        assert "forecast" in info["providers"]
        assert len(info["providers"]["current"]) > 0
        assert len(info["providers"]["forecast"]) > 0

    def test_info_contains_cache_config(self):
        """Test that info contains cache configuration."""
        response = client.get("/info")
        info = response.json()
        config = info["configuration"]
        assert "current_cache_ttl_seconds" in config
        assert "forecast_cache_ttl_seconds" in config
        assert "fail_window_seconds" in config
        assert "fail_threshold" in config
        assert "cooldown_seconds" in config


class TestResetMetrics:
    """Test metrics reset endpoint."""

    def test_reset_metrics(self):
        """Test resetting metrics."""
        # Make some requests
        client.get("/weather?lat=43.2567&lon=76.9286")

        # Reset metrics
        response = client.delete("/cache/metrics")
        assert response.status_code == 200
        data = response.json()
        assert "message" in data

        # Verify metrics are reset
        response = client.get("/metrics")
        metrics = response.json()["metrics"]
        assert metrics["requests"]["total"] == 0
        assert len(metrics["requests"]["by_provider"]) == 0
        assert metrics["cache"]["hits"] == 0
        assert metrics["cache"]["misses"] == 0


class TestHealthEndpoints:
    """Test existing health endpoints still work."""

    def test_health_endpoint(self):
        """Test /health endpoint."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert data["status"] == "ok"

    def test_healthz_endpoint(self):
        """Test /healthz endpoint."""
        response = client.get("/healthz")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "uptime_seconds" in data


class TestProvidersEndpoint:
    """Test providers endpoint."""

    def test_providers_endpoint(self):
        """Test /providers endpoint."""
        response = client.get("/providers")
        assert response.status_code == 200
        data = response.json()
        assert "current" in data
        assert "forecast" in data
        assert len(data["current"]) > 0
        assert len(data["forecast"]) > 0


class TestWeatherEndpoint:
    """Test weather endpoint for new fields."""

    def test_weather_has_new_fields(self):
        """Test that the weather endpoint response includes new fields."""
        response = client.get("/weather?lat=43.2567&lon=76.9286&units=metric")
        assert response.status_code == 200
        data = response.json()

        assert "current" in data
        current_weather = data["current"]

        assert "pressure" in current_weather
        assert "humidity" in current_weather
        assert "light_intensity" in current_weather

        assert isinstance(current_weather["pressure"], (int, float, type(None)))
        assert isinstance(current_weather["humidity"], (int, float, type(None)))
        # light_intensity can be None
        assert isinstance(current_weather["light_intensity"], (int, float, type(None)))

        if "forecast" in data and data["forecast"]:
            for day in data["forecast"]:
                assert "light_intensity" in day
                assert isinstance(day["light_intensity"], (int, float, type(None)))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
