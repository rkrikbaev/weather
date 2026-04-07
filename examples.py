#!/usr/bin/env python3
"""
Quick examples demonstrating the new weather service features.

Run with: python examples.py
"""

from weather.client import WeatherClient
import json

# Initialize client
client = WeatherClient("http://localhost:8000")


def print_section(title):
    """Print a section header."""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


def example_1_service_info():
    """Example 1: Get service information."""
    print_section("Example 1: Service Information")

    info = client.get_info()
    print(f"Service: {info['service']}")
    print(f"Version: {info['version']}")
    print(f"Uptime: {info['uptime_seconds']} seconds")
    print(f"\nAvailable Providers:")
    print(f"  Current weather: {', '.join(info['providers']['current'][:3])}...")
    print(f"  Forecast: {', '.join(info['providers']['forecast'][:3])}...")
    print(f"\nConfiguration:")
    print(f"  Current cache TTL: {info['configuration']['current_cache_ttl_seconds']}s")
    print(f"  Forecast cache TTL: {info['configuration']['forecast_cache_ttl_seconds']}s")


def example_2_batch_weather():
    """Example 2: Request weather for multiple locations."""
    print_section("Example 2: Batch Weather Request")

    locations = [
        (43.2567, 76.9286, "Almaty, Kazakhstan"),
        (51.5074, -0.1278, "London, UK"),
        (35.6762, 139.6503, "Tokyo, Japan"),
    ]

    batch_locations = [(lat, lon) for lat, lon, _ in locations]
    result = client.get_batch_weather(batch_locations, units="metric")

    print(f"Requested weather for {len(batch_locations)} locations\n")

    for i, (lat, lon, name) in enumerate(locations):
        if i < len(result["results"]):
            location = result["results"][i]
            weather = location["weather"]["current"]
            print(f"{name}")
            print(f"  Temperature: {weather['temp_c']}°C")
            print(f"  Feels like: {weather['feels_like_c']}°C")
            print(f"  Wind: {weather['wind']['speed_kph']} km/h")
            print()

    if result["errors"]:
        print(f"Errors ({len(result['errors'])}):")
        for error in result["errors"]:
            print(f"  ({error['lat']}, {error['lon']}): {error['error']}")


def example_3_cache_stats():
    """Example 3: View cache statistics."""
    print_section("Example 3: Cache Statistics")

    # Make some requests first
    print("Making requests to populate cache...\n")
    client.get_weather(43.2567, 76.9286)
    client.get_weather(51.5074, -0.1278)
    client.get_weather(43.2567, 76.9286)  # This should be a cache hit

    stats = client.get_cache_stats()

    print(f"Cache Size: {stats['size']} entries")
    print(f"Cache Hits: {stats['hits']}")
    print(f"Cache Misses: {stats['misses']}")
    print(f"Hit Rate: {stats['hit_rate']:.1%}")
    print(f"\nRecent Entries (showing first 5):")
    for i, entry in enumerate(stats['entries'][:5]):
        print(f"  {i+1}. {entry['key'][:50]}...")
        print(f"     Age: {entry['age_seconds']}s")


def example_4_metrics():
    """Example 4: View performance metrics."""
    print_section("Example 4: Performance Metrics")

    metrics = client.get_metrics()
    total_requests = metrics["metrics"]["requests"]["total"]

    print(f"Total Requests: {total_requests}\n")
    print("Provider Statistics:")

    for provider, stats in metrics["metrics"]["requests"]["by_provider"].items():
        print(f"\n  {provider.upper()}")
        print(f"    Requests: {stats['count']}")
        print(f"    Avg Response Time: {stats['avg_response_time']:.3f}s")
        print(f"    Min Response Time: {stats['min_response_time']:.3f}s")
        print(f"    Max Response Time: {stats['max_response_time']:.3f}s")

    cache_metrics = metrics["metrics"]["cache"]
    total_cache_ops = cache_metrics["hits"] + cache_metrics["misses"]
    if total_cache_ops > 0:
        hit_rate = cache_metrics["hits"] / total_cache_ops
        print(f"\nCache Metrics:")
        print(f"  Hits: {cache_metrics['hits']}")
        print(f"  Misses: {cache_metrics['misses']}")
        print(f"  Hit Rate: {hit_rate:.1%}")


def example_5_cache_management():
    """Example 5: Cache management operations."""
    print_section("Example 5: Cache Management")

    # Get initial stats
    print("Initial cache status...")
    stats_before = client.get_cache_stats()
    print(f"  Cache size: {stats_before['size']} entries")

    # Clear cache
    print("\nClearing cache...")
    response = client.clear_cache()
    print(f"  {response['message']}")

    # Get new stats
    stats_after = client.get_cache_stats()
    print(f"\nAfter clearing:")
    print(f"  Cache size: {stats_after['size']} entries")


def example_6_circuit_breaker():
    """Example 6: Circuit breaker management."""
    print_section("Example 6: Circuit Breaker Management")

    # Get provider status
    providers = client.get_providers()

    open_circuits = [p["name"] for p in providers["current"] if p["circuit_open"]]

    if open_circuits:
        print(f"Open circuit breakers: {', '.join(open_circuits)}")
        print("\nResetting circuit breakers...\n")

        for provider in open_circuits:
            response = client.reset_circuit_breaker(provider)
            print(f"  {response['message']}")
    else:
        print("No open circuit breakers detected")


def example_7_monitoring_workflow():
    """Example 7: Complete monitoring workflow."""
    print_section("Example 7: Complete Monitoring Workflow")

    print("Step 1: Get service info")
    info = client.get_info()
    print(f"  Service version: {info['version']}")
    print(f"  Uptime: {info['uptime_seconds']}s")

    print("\nStep 2: Check provider status")
    providers = client.get_providers()
    working = sum(1 for p in providers["current"] if p["enabled"] and not p["circuit_open"])
    print(f"  Working providers: {working}/{len(providers['current'])}")

    print("\nStep 3: Monitor cache performance")
    cache_stats = client.get_cache_stats()
    print(f"  Cache hit rate: {cache_stats['hit_rate']:.1%}")
    print(f"  Cache size: {cache_stats['size']} entries")

    print("\nStep 4: Check request metrics")
    metrics = client.get_metrics()
    total = metrics["metrics"]["requests"]["total"]
    print(f"  Total requests since startup: {total}")

    if total > 0 and metrics["metrics"]["requests"]["by_provider"]:
        slowest = max(
            metrics["metrics"]["requests"]["by_provider"].items(),
            key=lambda x: x[1]["avg_response_time"],
        )
        print(f"  Slowest provider: {slowest[0]} ({slowest[1]['avg_response_time']:.3f}s avg)")

    print("\nStep 5: Reset metrics for fresh monitoring")
    response = client.reset_metrics()
    print(f"  {response['message']}")


if __name__ == "__main__":
    print("\n" + "="*60)
    print("  Weather Service - New Features Examples")
    print("="*60)

    try:
        # Run all examples
        example_1_service_info()
        example_2_batch_weather()
        example_3_cache_stats()
        example_4_metrics()
        example_5_cache_management()
        example_6_circuit_breaker()
        example_7_monitoring_workflow()

        print("\n" + "="*60)
        print("  All examples completed!")
        print("="*60 + "\n")

    except ConnectionError:
        print("\n[ERROR] Could not connect to weather service")
        print("Make sure the service is running at http://localhost:8000")
        print("\nTo start the service, run:")
        print("  uvicorn app:app --reload --port 8000")
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
