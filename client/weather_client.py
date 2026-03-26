from __future__ import annotations

from typing import Any, Dict, Optional

import requests


class WeatherClient:
    def __init__(self, base_url: str, timeout: int = 10) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def get_weather(self, lat: float, lon: float, units: str = "metric") -> Dict[str, Any]:
        return self._get("/weather", {"lat": lat, "lon": lon, "units": units})

    def get_forecast(self, lat: float, lon: float, hours: int = 24, units: str = "metric") -> Dict[str, Any]:
        return self._get("/forecast", {"lat": lat, "lon": lon, "hours": hours, "units": units})

    def get_providers(self) -> Dict[str, Any]:
        return self._get("/providers", {})

    def get_healthz(self) -> Dict[str, Any]:
        return self._get("/healthz", {})

    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        resp = requests.get(url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()
