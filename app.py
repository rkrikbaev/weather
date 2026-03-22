import json
import os
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, HTTPException, Query

app = FastAPI(title="Weather Data Aggregation & Normalization Service")
_started_at = time.time()

# -------------------------
# Config
# -------------------------

CONFIG_PATH = os.getenv("WEATHER_CONFIG_PATH", "config.conf")
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "60"))
FAIL_WINDOW_SECONDS = int(os.getenv("FAIL_WINDOW_SECONDS", "60"))
FAIL_THRESHOLD = int(os.getenv("FAIL_THRESHOLD", "3"))
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "300"))

DEFAULT_PROVIDERS = ["openweather", "weatherapi", "openmeteo", "mock"]
PROVIDERS = [p.strip() for p in os.getenv("PROVIDERS", ",".join(DEFAULT_PROVIDERS)).split(",") if p.strip()]

# -------------------------
# In-memory state
# -------------------------

CacheKey = Tuple[Any, ...]
_cache: Dict[CacheKey, Tuple[float, Dict[str, Any]]] = {}
_failures: Dict[str, List[float]] = {}
_circuit_open_until: Dict[str, float] = {}
_config_cache: Optional[Dict[str, Any]] = None

# -------------------------
# Helpers
# -------------------------


def _now() -> float:
    return time.time()


def _load_config() -> Dict[str, Any]:
    global _config_cache  # noqa: PLW0603
    if _config_cache is not None:
        return _config_cache
    if not CONFIG_PATH:
        _config_cache = {}
        return _config_cache
    if not os.path.exists(CONFIG_PATH):
        _config_cache = {}
        return _config_cache
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
            raw = handle.read().strip()
            if not raw:
                _config_cache = {}
                return _config_cache
            _config_cache = json.loads(raw)
            if not isinstance(_config_cache, dict):
                _config_cache = {}
    except Exception:
        _config_cache = {}
    return _config_cache


def _get_provider_config(name: str) -> Dict[str, Any]:
    cfg = _load_config()
    providers = cfg.get("providers")
    if isinstance(providers, dict):
        provider_cfg = providers.get(name)
        if isinstance(provider_cfg, dict):
            return provider_cfg
    return {}


def _get_config_value(key: str) -> Optional[str]:
    cfg = _load_config()
    if key in cfg:
        val = cfg.get(key)
        return None if val is None else str(val)
    return None


def _is_circuit_open(provider: str) -> bool:
    until = _circuit_open_until.get(provider)
    return until is not None and _now() < until


def _record_failure(provider: str) -> None:
    ts = _now()
    window_start = ts - FAIL_WINDOW_SECONDS
    failures = _failures.setdefault(provider, [])
    failures.append(ts)
    # prune
    while failures and failures[0] < window_start:
        failures.pop(0)
    if len(failures) >= FAIL_THRESHOLD:
        _circuit_open_until[provider] = ts + COOLDOWN_SECONDS


def _record_success(provider: str) -> None:
    _failures.pop(provider, None)
    _circuit_open_until.pop(provider, None)


def _cache_get(key: CacheKey) -> Optional[Dict[str, Any]]:
    entry = _cache.get(key)
    if not entry:
        return None
    ts, data = entry
    if _now() - ts > CACHE_TTL_SECONDS:
        _cache.pop(key, None)
        return None
    return data


def _cache_set(key: CacheKey, data: Dict[str, Any]) -> None:
    _cache[key] = (_now(), data)


# -------------------------
# Provider clients
# -------------------------


def _fetch_openweather(lat: float, lon: float, units: str) -> Dict[str, Any]:
    ow_cfg = _get_provider_config("OPENWEATHER")
    api_key = (
        os.getenv("OPENWEATHER_API_KEY")
        or _get_config_value("OPENWEATHER_API_KEY")
        or (str(ow_cfg.get("api_key")) if ow_cfg.get("api_key") is not None else None)
    )
    base_url = (
        os.getenv("OPENWEATHER_BASE_URL")
        or _get_config_value("OPENWEATHER_BASE_URL")
        or (str(ow_cfg.get("base_url")) if ow_cfg.get("base_url") is not None else None)
        or "https://api.openweathermap.org/data/2.5/weather"
    )
    if not api_key:
        raise RuntimeError("OPENWEATHER_API_KEY is not set")
    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
        "units": "metric" if units == "metric" else "imperial",
    }
    resp = requests.get(base_url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _fetch_weatherapi(lat: float, lon: float, units: str) -> Dict[str, Any]:
    wa_cfg = _get_provider_config("WEATHERAPI")
    api_key = (
        os.getenv("WEATHERAPI_API_KEY")
        or _get_config_value("WEATHERAPI_API_KEY")
        or (str(wa_cfg.get("api_key")) if wa_cfg.get("api_key") is not None else None)
    )
    base_url = (
        os.getenv("WEATHERAPI_BASE_URL")
        or _get_config_value("WEATHERAPI_BASE_URL")
        or (str(wa_cfg.get("base_url")) if wa_cfg.get("base_url") is not None else None)
        or "https://api.weatherapi.com/v1/current.json"
    )
    if not api_key:
        raise RuntimeError("WEATHERAPI_API_KEY is not set")
    q = f"{lat},{lon}"
    params = {"key": api_key, "q": q}
    resp = requests.get(base_url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _fetch_openmeteo(lat: float, lon: float, units: str) -> Dict[str, Any]:
    om_cfg = _get_provider_config("OPENMETEO")
    base_url = (
        os.getenv("OPENMETEO_BASE_URL")
        or _get_config_value("OPENMETEO_BASE_URL")
        or (str(om_cfg.get("base_url")) if om_cfg.get("base_url") is not None else None)
        or "https://api.open-meteo.com/v1/forecast"
    )
    # Open-Meteo uses metric by default; imperial conversion handled in transformer if requested
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,apparent_temperature,wind_speed_10m,wind_direction_10m,wind_gusts_10m",
        "hourly": "relative_humidity_2m,visibility,precipitation_probability,cloud_cover,uv_index",
        "forecast_days": 1,
    }
    resp = requests.get(base_url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _fetch_openmeteo_forecast(lat: float, lon: float, units: str, hours: int) -> Dict[str, Any]:
    om_cfg = _get_provider_config("OPENMETEO")
    base_url = (
        os.getenv("OPENMETEO_BASE_URL")
        or _get_config_value("OPENMETEO_BASE_URL")
        or (str(om_cfg.get("base_url")) if om_cfg.get("base_url") is not None else None)
        or "https://api.open-meteo.com/v1/forecast"
    )
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": (
            "temperature_2m,relative_humidity_2m,visibility,cloud_cover,wind_speed_10m,"
            "wind_direction_10m,wind_gusts_10m,surface_pressure"
        ),
        "forecast_hours": hours,
    }
    resp = requests.get(base_url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _fetch_weatherapi_forecast(lat: float, lon: float, units: str, hours: int) -> Dict[str, Any]:
    wa_cfg = _get_provider_config("WEATHERAPI")
    api_key = (
        os.getenv("WEATHERAPI_API_KEY")
        or _get_config_value("WEATHERAPI_API_KEY")
        or (str(wa_cfg.get("api_key")) if wa_cfg.get("api_key") is not None else None)
    )
    base_url = (
        os.getenv("WEATHERAPI_BASE_URL")
        or _get_config_value("WEATHERAPI_BASE_URL")
        or (str(wa_cfg.get("base_url")) if wa_cfg.get("base_url") is not None else None)
        or "https://api.weatherapi.com/v1/forecast.json"
    )
    if not api_key:
        raise RuntimeError("WEATHERAPI_API_KEY is not set")
    q = f"{lat},{lon}"
    params = {"key": api_key, "q": q, "days": 1, "aqi": "no", "alerts": "no"}
    resp = requests.get(base_url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _fetch_mock(lat: float, lon: float, units: str) -> Dict[str, Any]:
    return {
        "current": {
            "temp_c": 20.0,
            "feels_like_c": 19.0,
            "condition": {"text": "Partly cloudy", "icon_url": ""},
            "wind": {"speed_kph": 10.0, "direction": 180, "gust_kph": 15.0},
        },
        "risk_factors": {"uv_index": 4, "precip_prob": 0.1, "thunderstorm_prob": None},
    }


# -------------------------
# Transformers
# -------------------------


def _to_cws(metadata_source: str, lat: float, lon: float, current: Dict[str, Any], risk: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "metadata": {
            "source": metadata_source,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "lat": lat,
            "lon": lon,
        },
        "current": current,
        "risk_factors": risk,
    }


def _transform_openweather(raw: Dict[str, Any], lat: float, lon: float, units: str) -> Dict[str, Any]:
    main = raw.get("main", {})
    wind = raw.get("wind", {})
    condition = (raw.get("weather") or [{}])[0]

    temp_val = main.get("temp")
    feels_val = main.get("feels_like")
    # OpenWeather returns wind speed in m/s for metric, mph for imperial
    if units == "imperial":
        speed_out = wind.get("speed")
        gust_out = wind.get("gust")
    else:
        speed_out = None if wind.get("speed") is None else wind.get("speed") * 3.6
        gust_out = None if wind.get("gust") is None else wind.get("gust") * 3.6

    current = {
        "temp_c": temp_val,
        "feels_like_c": feels_val,
        "condition": {
            "text": condition.get("description") or "",
            "icon_url": f"https://openweathermap.org/img/wn/{condition.get('icon','')}@2x.png" if condition.get("icon") else "",
        },
        "wind": {
            "speed_kph": speed_out,
            "direction": wind.get("deg"),
            "gust_kph": gust_out,
        },
    }
    risk = {
        "uv_index": None,
        "precip_prob": None,
        "thunderstorm_prob": None,
    }
    return _to_cws("openweather", lat, lon, current, risk)


def _transform_weatherapi(raw: Dict[str, Any], lat: float, lon: float, units: str) -> Dict[str, Any]:
    current_raw = raw.get("current", {})
    condition_raw = current_raw.get("condition", {})

    if units == "imperial":
        temp_out = current_raw.get("temp_f")
        feels_out = current_raw.get("feelslike_f")
        wind_speed_out = current_raw.get("wind_mph")
        gust_out = current_raw.get("gust_mph")
    else:
        temp_out = current_raw.get("temp_c")
        feels_out = current_raw.get("feelslike_c")
        wind_speed_out = current_raw.get("wind_kph")
        gust_out = current_raw.get("gust_kph")

    current = {
        "temp_c": temp_out,
        "feels_like_c": feels_out,
        "condition": {
            "text": condition_raw.get("text") or "",
            "icon_url": condition_raw.get("icon") or "",
        },
        "wind": {
            "speed_kph": wind_speed_out,
            "direction": current_raw.get("wind_degree"),
            "gust_kph": gust_out,
        },
    }
    risk = {
        "uv_index": current_raw.get("uv"),
        "precip_prob": None,
        "thunderstorm_prob": None,
    }
    return _to_cws("weatherapi", lat, lon, current, risk)


def _transform_openmeteo(raw: Dict[str, Any], lat: float, lon: float, units: str) -> Dict[str, Any]:
    current_raw = raw.get("current", {})
    hourly = raw.get("hourly", {})

    temp_out = current_raw.get("temperature_2m")
    feels_out = current_raw.get("apparent_temperature")

    wind_speed_kph = current_raw.get("wind_speed_10m")
    wind_gust_kph = current_raw.get("wind_gusts_10m")
    if wind_speed_kph is not None:
        wind_speed_kph = wind_speed_kph * 3.6
    if wind_gust_kph is not None:
        wind_gust_kph = wind_gust_kph * 3.6

    if units == "imperial":
        if temp_out is not None:
            temp_out = (temp_out * 9 / 5) + 32
        if feels_out is not None:
            feels_out = (feels_out * 9 / 5) + 32
        if wind_speed_kph is not None:
            wind_speed_kph = wind_speed_kph / 1.60934
        if wind_gust_kph is not None:
            wind_gust_kph = wind_gust_kph / 1.60934

    # Use first hourly values for risk factors
    precip_prob = None
    uv_index = None
    if hourly:
        pp = hourly.get("precipitation_probability")
        uv = hourly.get("uv_index")
        if isinstance(pp, list) and pp:
            precip_prob = pp[0] / 100.0
        if isinstance(uv, list) and uv:
            uv_index = uv[0]

    current = {
        "temp_c": temp_out,
        "feels_like_c": feels_out,
        "condition": {"text": "", "icon_url": ""},
        "wind": {
            "speed_kph": wind_speed_kph,
            "direction": current_raw.get("wind_direction_10m"),
            "gust_kph": wind_gust_kph,
        },
    }
    risk = {
        "uv_index": uv_index,
        "precip_prob": precip_prob,
        "thunderstorm_prob": None,
    }
    return _to_cws("openmeteo", lat, lon, current, risk)


def _transform_mock(raw: Dict[str, Any], lat: float, lon: float, units: str) -> Dict[str, Any]:
    return _to_cws("mock", lat, lon, raw["current"], raw["risk_factors"])


FETCHERS: Dict[str, Callable[[float, float, str], Dict[str, Any]]] = {
    "openweather": _fetch_openweather,
    "weatherapi": _fetch_weatherapi,
    "openmeteo": _fetch_openmeteo,
    "mock": _fetch_mock,
}

TRANSFORMERS: Dict[str, Callable[[Dict[str, Any], float, float, str], Dict[str, Any]]] = {
    "openweather": _transform_openweather,
    "weatherapi": _transform_weatherapi,
    "openmeteo": _transform_openmeteo,
    "mock": _transform_mock,
}

FORECAST_PROVIDERS = [
    p.strip()
    for p in os.getenv("FORECAST_PROVIDERS", "openmeteo,weatherapi,mock").split(",")
    if p.strip()
]

FORECAST_FETCHERS: Dict[str, Callable[[float, float, str, int], Dict[str, Any]]] = {
    "openmeteo": _fetch_openmeteo_forecast,
    "weatherapi": _fetch_weatherapi_forecast,
}


def _to_forecast_response(source: str, lat: float, lon: float, hourly: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "metadata": {
            "source": source,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "lat": lat,
            "lon": lon,
        },
        "hourly": hourly,
    }


def _transform_openmeteo_forecast(raw: Dict[str, Any], lat: float, lon: float, units: str, hours: int) -> Dict[str, Any]:
    hourly = raw.get("hourly", {})
    times = hourly.get("time") or []
    temps = hourly.get("temperature_2m") or []
    humidity = hourly.get("relative_humidity_2m") or []
    visibility = hourly.get("visibility") or []
    clouds = hourly.get("cloud_cover") or []
    wind_speed = hourly.get("wind_speed_10m") or []
    wind_dir = hourly.get("wind_direction_10m") or []
    wind_gust = hourly.get("wind_gusts_10m") or []
    pressure = hourly.get("surface_pressure") or []

    rows: List[Dict[str, Any]] = []
    count = min(hours, len(times))
    for idx in range(count):
        temp_out = temps[idx] if idx < len(temps) else None
        vis_out = visibility[idx] if idx < len(visibility) else None
        wind_speed_out = wind_speed[idx] if idx < len(wind_speed) else None
        wind_gust_out = wind_gust[idx] if idx < len(wind_gust) else None
        pressure_out = pressure[idx] if idx < len(pressure) else None

        if units == "imperial":
            if temp_out is not None:
                temp_out = (temp_out * 9 / 5) + 32
            if vis_out is not None:
                vis_out = vis_out / 1609.34
            if wind_speed_out is not None:
                wind_speed_out = wind_speed_out / 1.60934
            if wind_gust_out is not None:
                wind_gust_out = wind_gust_out / 1.60934
            if pressure_out is not None:
                pressure_out = pressure_out * 0.02953

        rows.append(
            {
                "timestamp": times[idx],
                "temp": temp_out,
                "pressure": pressure_out,
                "wind": {
                    "speed": wind_speed_out,
                    "direction": wind_dir[idx] if idx < len(wind_dir) else None,
                    "gust": wind_gust_out,
                },
                "clouds": clouds[idx] if idx < len(clouds) else None,
                "visibility": vis_out,
                "humidity": humidity[idx] if idx < len(humidity) else None,
            }
        )

    return _to_forecast_response("openmeteo", lat, lon, rows)


def _transform_weatherapi_forecast(raw: Dict[str, Any], lat: float, lon: float, units: str, hours: int) -> Dict[str, Any]:
    forecast = (raw.get("forecast") or {}).get("forecastday") or []
    hourly = forecast[0].get("hour") if forecast else []
    rows: List[Dict[str, Any]] = []
    count = min(hours, len(hourly))

    for idx in range(count):
        h = hourly[idx]
        if units == "imperial":
            temp_out = h.get("temp_f")
            pressure_out = h.get("pressure_in")
            wind_speed_out = h.get("wind_mph")
            gust_out = h.get("gust_mph")
            visibility_out = h.get("vis_miles")
        else:
            temp_out = h.get("temp_c")
            pressure_out = h.get("pressure_mb")
            wind_speed_out = h.get("wind_kph")
            gust_out = h.get("gust_kph")
            visibility_out = h.get("vis_km")

        rows.append(
            {
                "timestamp": h.get("time"),
                "temp": temp_out,
                "pressure": pressure_out,
                "wind": {
                    "speed": wind_speed_out,
                    "direction": h.get("wind_degree"),
                    "gust": gust_out,
                },
                "clouds": h.get("cloud"),
                "visibility": visibility_out,
                "humidity": h.get("humidity"),
            }
        )

    return _to_forecast_response("weatherapi", lat, lon, rows)


def _transform_mock_forecast(lat: float, lon: float, hours: int) -> Dict[str, Any]:
    rows = []
    for i in range(hours):
        rows.append(
            {
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(_now() + i * 3600)),
                "temp": 20.0,
                "pressure": 1013.0,
                "wind": {"speed": 10.0, "direction": 180, "gust": 15.0},
                "clouds": 40,
                "visibility": 10.0,
                "humidity": 55,
            }
        )
    return _to_forecast_response("mock", lat, lon, rows)


FORECAST_TRANSFORMERS: Dict[str, Callable[[Dict[str, Any], float, float, str, int], Dict[str, Any]]] = {
    "openmeteo": _transform_openmeteo_forecast,
    "weatherapi": _transform_weatherapi_forecast,
}


# -------------------------
# Main endpoint
# -------------------------


@app.get("/weather")
def get_weather(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
    units: str = Query("metric", pattern="^(metric|imperial)$"),
):
    key = ("current", lat, lon, units)
    cached = _cache_get(key)
    if cached:
        return cached

    errors = []
    for provider in PROVIDERS:
        if provider not in FETCHERS or provider not in TRANSFORMERS:
            continue
        if _is_circuit_open(provider):
            continue
        try:
            raw = FETCHERS[provider](lat, lon, units)
            data = TRANSFORMERS[provider](raw, lat, lon, units)
            _record_success(provider)
            _cache_set(key, data)
            return data
        except Exception as exc:  # noqa: BLE001
            _record_failure(provider)
            errors.append({"provider": provider, "error": str(exc)})
            continue

    raise HTTPException(
        status_code=503,
        detail={
            "message": "All upstream providers are currently unreachable.",
            "errors": errors,
        },
    )


@app.get("/forecast")
def get_forecast(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
    hours: int = Query(24, ge=1, le=240, description="Number of hours to return"),
    units: str = Query("metric", pattern="^(metric|imperial)$"),
):
    key = ("forecast", lat, lon, units, hours)
    cached = _cache_get(key)
    if cached:
        return cached

    errors = []
    for provider in FORECAST_PROVIDERS:
        if _is_circuit_open(provider):
            continue
        try:
            if provider == "mock":
                data = _transform_mock_forecast(lat, lon, hours)
            else:
                fetcher = FORECAST_FETCHERS.get(provider)
                transformer = FORECAST_TRANSFORMERS.get(provider)
                if not fetcher or not transformer:
                    continue
                raw = fetcher(lat, lon, units, hours)
                data = transformer(raw, lat, lon, units, hours)
            _record_success(provider)
            _cache_set(key, data)
            return data
        except Exception as exc:  # noqa: BLE001
            _record_failure(provider)
            errors.append({"provider": provider, "error": str(exc)})
            continue

    raise HTTPException(
        status_code=503,
        detail={
            "message": "All upstream providers are currently unreachable.",
            "errors": errors,
        },
    )


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "providers": PROVIDERS,
        "circuit_open_until": _circuit_open_until,
    }


@app.get("/healthz")
def healthz() -> Dict[str, Any]:
    return {
        "status": "ok",
        "uptime_seconds": int(_now() - _started_at),
    }
