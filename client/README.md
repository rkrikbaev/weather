# Weather Client Module

Lightweight Python client for the Weather Data Aggregation & Normalization Service. It abstracts HTTP calls and returns the JSON responses from the service.

## Install

This is a local module. Use it by importing from the `client` package in this repo.

## Usage

```python
from client import WeatherClient

client = WeatherClient(base_url="http://127.0.0.1:8000")

current = client.get_weather(lat=43.2567, lon=76.9286)
forecast = client.get_forecast(lat=43.2567, lon=76.9286, hours=24)
providers = client.get_providers()
health = client.get_healthz()
```

## Methods

- `get_weather(lat, lon, units="metric")`
- `get_forecast(lat, lon, hours=24, units="metric")`
- `get_providers()`
- `get_healthz()`

## Errors

Network and HTTP errors raise `requests.HTTPError` via `resp.raise_for_status()`.

## Notes

- The client does not implement retries. If you want retries, wrap calls with your own retry logic.
- Default timeout is 10 seconds; pass a different value to `WeatherClient(timeout=...)`.
