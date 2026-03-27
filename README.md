# Weather Data Aggregation & Normalization Service

Python HTTP service that aggregates multiple weather providers with failover, circuit breaker, and normalization into a Common Weather Schema (CWS).

## Run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --reload --port 8000
```

## Request

```bash
curl "http://127.0.0.1:8000/weather?lat=43.2567&lon=76.9286"
curl "http://127.0.0.1:8000/weather?lat=43.2567&lon=76.9286&units=imperial"
curl "http://127.0.0.1:8000/forecast?lat=43.2567&lon=76.9286&hours=24"
curl "http://127.0.0.1:8000/healthz"
```

## Configuration

Providers are tried in order via the `PROVIDERS` env var. Defaults:

```
openweather,weatherapi,openmeteo,mock
```

Set API keys:

```bash
export OPENWEATHER_API_KEY=...
export WEATHERAPI_API_KEY=...
```

Or via JSON config file (default: `config.conf` in the working directory):

```json
{
  "providers": {
    "OPENWEATHER": {
      "api_key": "your_key",
      "base_url": "https://api.openweathermap.org/data/2.5/weather"
    },
    "WEATHERAPI": {
      "api_key": "your_key",
      "base_url": "https://api.weatherapi.com/v1/current.json"
    },
    "OPENMETEO": {
      "base_url": "https://api.open-meteo.com/v1/forecast"
    },
    "TOMORROWIO": {
      "api_key": "your_key",
      "base_url": "https://api.tomorrow.io/v4/weather/realtime",
      "forecast_base_url": "https://api.tomorrow.io/v4/weather/forecast"
    },
    "VISUALCROSSING": {
      "api_key": "your_key",
      "base_url": "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
    }
  }
}
```

To use a different file:

```bash
export WEATHER_CONFIG_PATH=/path/to/your/config.conf
```

Optional base URLs:

```bash
export OPENWEATHER_BASE_URL=...
export WEATHERAPI_BASE_URL=...
export OPENMETEO_BASE_URL=...
export TOMORROWIO_BASE_URL=...
export TOMORROWIO_FORECAST_BASE_URL=...
export VISUALCROSSING_BASE_URL=...
```

Circuit breaker and cache tuning:

```bash
export CACHE_TTL_SECONDS=60
export FAIL_WINDOW_SECONDS=60
export FAIL_THRESHOLD=3
export COOLDOWN_SECONDS=300
```

## Response (CWS)

By default, values are normalized to metric. If `units=imperial` is provided, numeric values are returned in imperial units while preserving the fixed CWS field names.

## Forecast Response

```json
{
  "metadata": { "source": "provider_name", "timestamp": "ISO-8601", "lat": 0.0, "lon": 0.0 },
  "hourly": [
    {
      "timestamp": "ISO-8601",
      "temp": 0.0,
      "pressure": 0.0,
      "wind": { "speed": 0.0, "direction": 0, "gust": 0.0 },
      "clouds": 0,
      "visibility": 0.0,
      "humidity": 0
    }
  ]
}
```

```json
{
  "metadata": { "source": "provider_name", "timestamp": "ISO-8601", "lat": 0.0, "lon": 0.0 },
  "current": {
    "temp_c": 0.0,
    "feels_like_c": 0.0,
    "condition": { "text": "string", "icon_url": "string" },
    "wind": { "speed_kph": 0.0, "direction": "degrees", "gust_kph": 0.0 }
  },
  "risk_factors": { "uv_index": 0, "precip_prob": 0.0, "thunderstorm_prob": 0.0 }
}
```



## NGINX configuration

```
server {
    listen 81 default_server;
    listen [::]:81 default_server;

    server_name _;

    access_log /var/log/nginx/weather_access.log;
    error_log /var/log/nginx/weather_error.log;

    location /weather/ {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
```