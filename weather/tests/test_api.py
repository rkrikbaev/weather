import importlib
import os
import sys
from pathlib import Path
from typing import Generator

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def app_client() -> Generator[TestClient, None, None]:
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    # Ensure we use mock providers to avoid external API calls
    os.environ["PROVIDERS"] = "mock"
    os.environ["FORECAST_PROVIDERS"] = "mock"

    # Ensure a clean import each time
    if "app" in sys.modules:
        del sys.modules["app"]
    import app  # noqa: WPS433

    importlib.reload(app)
    client = TestClient(app.app)
    yield client


def test_healthz(app_client: TestClient) -> None:
    resp = app_client.get("/healthz")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert isinstance(body["uptime_seconds"], int)


def test_health(app_client: TestClient) -> None:
    resp = app_client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["providers"] == ["mock"]


def test_weather_mock(app_client: TestClient) -> None:
    resp = app_client.get("/weather?lat=43.2567&lon=76.9286")
    assert resp.status_code == 200
    body = resp.json()
    assert body["metadata"]["source"] == "mock"
    assert "current" in body
    assert "risk_factors" in body


def test_forecast_mock(app_client: TestClient) -> None:
    resp = app_client.get("/forecast?lat=43.2567&lon=76.9286&hours=24")
    assert resp.status_code == 200
    body = resp.json()
    assert body["metadata"]["source"] == "mock"
    assert len(body["hourly"]) == 24
    sample = body["hourly"][0]
    assert "temp" in sample
    assert "pressure" in sample
    assert "wind" in sample
    assert "clouds" in sample
    assert "visibility" in sample
    assert "humidity" in sample
