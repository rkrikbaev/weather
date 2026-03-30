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

## Example JSON

### `/weather`

```json
{
  "metadata": {
    "source": "openweather",
    "timestamp": "2026-03-23T09:54:41Z",
    "lat": 43.2567,
    "lon": 76.9286
  },
  "current": {
    "temp_c": 18.3,
    "feels_like_c": 17.15,
    "condition": {
      "text": "broken clouds",
      "icon_url": "https://openweathermap.org/img/wn/04d@2x.png"
    },
    "wind": {
      "speed_kph": 25.2,
      "direction": 290,
      "gust_kph": null
    }
  },
  "risk_factors": {
    "uv_index": null,
    "precip_prob": null,
    "thunderstorm_prob": null
  }
}
```

### `/forecast`

```json
{
  "metadata": {
    "source": "openmeteo",
    "timestamp": "2026-03-23T09:54:41Z",
    "lat": 43.2567,
    "lon": 76.9286
  },
  "hourly": [
    {
      "timestamp": "2026-03-23T10:00:00Z",
      "temp": 18.3,
      "pressure": 1012.0,
      "wind": { "speed": 12.0, "direction": 290, "gust": 18.0 },
      "clouds": 75,
      "visibility": 10.0,
      "humidity": 55
    }
  ]
}
```

### `/providers`

```json
{
  "current": [
    {
      "name": "openmeteo",
      "enabled": true,
      "circuit_open": false,
      "circuit_open_until": null,
      "has_credentials": true,
      "last_error": null
    }
  ],
  "forecast": [
    {
      "name": "openmeteo",
      "enabled": true,
      "circuit_open": false,
      "circuit_open_until": null,
      "has_credentials": true,
      "last_error": null
    }
  ]
}
```

### `/healthz`

```json
{
  "status": "ok",
  "uptime_seconds": 533
}
```

## Errors

Network and HTTP errors raise `requests.HTTPError` via `resp.raise_for_status()`.

## Notes

- The client does not implement retries. If you want retries, wrap calls with your own retry logic.
- Default timeout is 10 seconds; pass a different value to `WeatherClient(timeout=...)`.
