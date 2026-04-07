from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import requests


class WeatherClient:
    def __init__(self, base_url: str, timeout: int = 10) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def get_weather(self, lat: float, lon: float, units: str = "metric") -> Dict[str, Any]:
        return self._get("/weather", {"lat": lat, "lon": lon, "units": units})

    def get_forecast(self, lat: float, lon: float, hours: int = 24, units: str = "metric") -> Dict[str, Any]:
        return self._get("/forecast", {"lat": lat, "lon": lon, "hours": hours, "units": units})

    def get_batch_weather(self, locations: List[Tuple[float, float]], units: str = "metric") -> Dict[str, Any]:
        """Get weather for multiple locations.
        
        Args:
            locations: List of (lat, lon) tuples
            units: Unit system (metric or imperial)
            
        Returns:
            Dict with results and errors
        """
        payload = {
            "locations": [{"lat": lat, "lon": lon} for lat, lon in locations],
            "units": units,
        }
        return self._post("/weather/batch", payload)

    def get_providers(self) -> Dict[str, Any]:
        return self._get("/providers", {})

    def get_healthz(self) -> Dict[str, Any]:
        return self._get("/healthz", {})

    def get_metrics(self) -> Dict[str, Any]:
        """Get performance and cache metrics."""
        return self._get("/metrics", {})

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return self._get("/cache/stats", {})

    def get_info(self) -> Dict[str, Any]:
        """Get service information."""
        return self._get("/info", {})

    def clear_cache(self) -> Dict[str, Any]:
        """Clear the entire cache."""
        return self._delete("/cache")

    def reset_metrics(self) -> Dict[str, Any]:
        """Reset performance metrics."""
        return self._delete("/cache/metrics")

    def reset_circuit_breaker(self, provider: Optional[str] = None) -> Dict[str, Any]:
        """Reset circuit breaker for a provider or all providers.
        
        Args:
            provider: Provider name (all if None)
            
        Returns:
            Response dict
        """
        params = {"provider": provider} if provider else {}
        return self._post("/circuit-breaker/reset", {}, params)

    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        resp = requests.get(url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, payload: Dict[str, Any], params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        resp = requests.post(url, json=payload, params=params or {}, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def _delete(self, path: str) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        resp = requests.delete(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()
