# Weather Data Aggregation & Normalization Service

Python HTTP service that aggregates multiple weather providers with failover, circuit breaker, and normalization into a Common Weather Schema (CWS).

**New in v2.0**: Batch requests, performance metrics, cache management, and more! See [NEW_FEATURES.md](NEW_FEATURES.md) for details.

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --reload --port 8000
```

## API Endpoints

### Current Weather

```bash
# Single location
curl "http://127.0.0.1:8000/weather?lat=43.2567&lon=76.9286"

# With imperial units
curl "http://127.0.0.1:8000/weather?lat=43.2567&lon=76.9286&units=imperial"
```

### Forecast

```bash
# 24 hours forecast
curl "http://127.0.0.1:8000/forecast?lat=43.2567&lon=76.9286&hours=24"

# 48 hours forecast in imperial units
curl "http://127.0.0.1:8000/forecast?lat=43.2567&lon=76.9286&hours=48&units=imperial"
```

### Batch Weather (NEW)

```bash
curl -X POST "http://127.0.0.1:8000/weather/batch" \
  -H "Content-Type: application/json" \
  -d '{
    "locations": [
      {"lat": 43.2567, "lon": 76.9286},
      {"lat": 51.5074, "lon": -0.1278}
    ],
    "units": "metric"
  }'
```

### Metrics & Monitoring (NEW)

```bash
# Performance metrics
curl "http://127.0.0.1:8000/metrics"

# Cache statistics
curl "http://127.0.0.1:8000/cache/stats"

# Service info
curl "http://127.0.0.1:8000/info"
```

### Health Check

```bash
curl "http://127.0.0.1:8000/healthz"
```

## Configuration

### Providers

Providers are tried in order via the `PROVIDERS` env var:

```bash
export PROVIDERS="openweather,weatherapi,openmeteo,mock"
```

### API Keys

Set via environment variables:

```bash
export OPENWEATHER_API_KEY=...
export WEATHERAPI_API_KEY=...
export TOMORROWIO_API_KEY=...
export VISUALCROSSING_API_KEY=...
```

Or via `config.conf`:

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

### Cache & Circuit Breaker

```bash
export CURRENT_CACHE_TTL_SECONDS=60
export FORECAST_CACHE_TTL_SECONDS=3600
export FAIL_WINDOW_SECONDS=60
export FAIL_THRESHOLD=3
export COOLDOWN_SECONDS=300
```

## Response Format (CWS)

By default, values are normalized to metric. Use `units=imperial` for imperial units.

### Current Weather Response

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

### Forecast Response

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

## Python Client

```python
from weather.client import WeatherClient

client = WeatherClient("http://localhost:8000")

# Single location
weather = client.get_weather(43.2567, 76.9286)

# Multiple locations (NEW)
locations = [(43.2567, 76.9286), (51.5074, -0.1278)]
batch = client.get_batch_weather(locations)

# Metrics (NEW)
metrics = client.get_metrics()
cache_stats = client.get_cache_stats()

# Management (NEW)
client.clear_cache()
client.reset_metrics()
client.reset_circuit_breaker()
```

## Examples

See [examples.py](examples.py) for complete examples of all features.

```bash
python examples.py
```

## New Features (v2.0)

✨ **Batch Requests** - Request weather for multiple locations in one call
📊 **Performance Metrics** - Track response times and provider statistics  
💾 **Cache Management** - View and clear cache with detailed statistics
🔌 **Circuit Breaker Control** - Manually reset circuit breaker state
ℹ️ **Service Information** - Comprehensive service metadata

See [NEW_FEATURES.md](NEW_FEATURES.md) for full documentation.

## NGINX Configuration

```nginx
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
