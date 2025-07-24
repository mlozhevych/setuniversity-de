import time

import pandas as pd
import requests

# --- Configuration ---
BASE_URL = "http://localhost:5002/adtech"  # Base URL of the API
CAMPAIGN_ID_TO_TEST = "705d4849-4c24-11f0-9f71-0242ac120002"  # Example campaign ID
ADVERTISER_ID_TO_TEST = 3  # Example advertiser ID
NUM_REQUESTS = 10  # Number of requests to test cache hit performance
REQUEST_DELAY = 0.1  # Delay between requests in seconds (for testing purposes)


def clear_redis_cache():
    """A helper function to connect to redis and clear it (for clean tests)."""
    try:
        import redis
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.flushall()
        print("Redis cache cleared.")
    except Exception as e:
        print(f"Could not clear Redis cache: {e}")
        print("Please ensure the 'redis' Python package is installed (`pip install redis`)")
        print("And that the Redis container is running and mapped to port 6379.")


def benchmark_endpoint(endpoint_url: str, num_requests: int, delay: float):
    """
    Benchmark the performance of an API endpoint with and without caching.

    Returns:
        A tuple containing (average_time_without_cache, average_time_with_cache).
    """

    # --- 1. Тестування БЕЗ кешування ---
    # Кожен запит іде напряму в базу даних, оскільки ми очищуємо кеш перед кожним викликом.
    no_cache_times = []
    print(f"--> Тестування БЕЗ кешу: {endpoint_url}")
    for i in range(num_requests):
        clear_redis_cache()  # Гарантуємо, що кеш порожній
        try:
            start_time = time.perf_counter()
            response = requests.get(endpoint_url, timeout=10)
            end_time = time.perf_counter()

            if response.status_code != 200:
                print(f"  Помилка на запиті {i + 1}: Статус {response.status_code}. Пропускаємо...")
                continue

            no_cache_times.append((end_time - start_time) * 1000)
        except requests.exceptions.RequestException as e:
            print(f"  Помилка з'єднання на запиті {i + 1}: {e}. Пропускаємо...")
            continue
        time.sleep(delay)

    avg_no_cache = sum(no_cache_times) / len(no_cache_times) if no_cache_times else 0

    # --- 2. Тестування З кешуванням ---
    # Робимо один "прогріваючий" запит, щоб заповнити кеш, а потім вимірюємо швидкість для наступних запитів.
    with_cache_times = []
    print(f"--> Тестування З кешем: {endpoint_url}")
    clear_redis_cache()  # Очищуємо кеш перед початком тесту

    # "Прогріваючий" запит для заповнення кешу
    try:
        print("  (Прогріваючий запит для заповнення кешу...)")
        requests.get(endpoint_url, timeout=10)
    except requests.exceptions.RequestException as e:
        print(f"  Помилка на прогріваючому запиті: {e}. Тест з кешем неможливий.")
        return avg_no_cache, 0

    for i in range(num_requests):
        start_time = time.perf_counter()
        requests.get(endpoint_url)  # Цей запит має потрапити в кеш
        end_time = time.perf_counter()
        with_cache_times.append((end_time - start_time) * 1000)
        time.sleep(delay)

    avg_with_cache = sum(with_cache_times) / len(with_cache_times) if with_cache_times else 0

    return avg_no_cache, avg_with_cache


def run_full_benchmark():
    """Runs a full benchmark test for both campaign and advertiser endpoints,"""

    all_results = []

    # Тестування кінцевої точки кампанії
    campaign_url = f"{BASE_URL}/campaign/{CAMPAIGN_ID_TO_TEST}/performance"
    no_cache_c, with_cache_c = benchmark_endpoint(campaign_url, NUM_REQUESTS, REQUEST_DELAY)
    all_results.append({
        "endpoint": f"/campaign/{CAMPAIGN_ID_TO_TEST}/performance",
        "no_cache": no_cache_c,
        "with_cache": with_cache_c
    })

    print("-" * 50)

    # Тестування кінцевої точки рекламодавця
    advertiser_url = f"{BASE_URL}/advertiser/{ADVERTISER_ID_TO_TEST}/spending"
    no_cache_a, with_cache_a = benchmark_endpoint(advertiser_url, NUM_REQUESTS, REQUEST_DELAY)
    all_results.append({
        "endpoint": f"/advertiser/{ADVERTISER_ID_TO_TEST}/spending",
        "no_cache": no_cache_a,
        "with_cache": with_cache_a
    })

    # --- Вивід фінальної таблиці ---
    print("\n\n--- РЕЗУЛЬТАТИ ТЕСТУВАННЯ ПРОДУКТИВНОСТІ ---")
    # Форматування заголовка таблиці
    header = f"| {'Endpoint':<42} | {'No Cache (ms)':>15} | {'Redis Cache (ms)':>18} | {'Δ‑Improvement':>15} |"
    separator = f"|{'-' * 44}|{'-' * 17}|{'-' * 20}|{'-' * 17}|"
    print(header)
    print(separator)

    # Форматування рядків таблиці
    for res in all_results:
        endpoint = res["endpoint"]
        no_cache = res["no_cache"]
        with_cache = res["with_cache"]

        if with_cache > 0 and no_cache > 0:
            improvement = f"{(no_cache / with_cache):.1f}x"
        else:
            improvement = "N/A"

        row = f"| {endpoint:<42} | {no_cache:>15.2f} | {with_cache:>18.2f} | {improvement:>15} |"
        print(row)


def run_benchmark():
    """Runs tests and prints a comparison table."""
    results = []

    # --- Benchmark Campaign Endpoint ---
    print("\n--- Benchmarking Campaign Performance Endpoint ---")
    print(f"Testing with CAMPAIGN_ID = {CAMPAIGN_ID_TO_TEST}")

    # Test 1: Direct DB Query (Cache Miss) for Campaign
    print("\nStep 1: Clearing cache to ensure a direct DB query...")
    clear_redis_cache()

    start_time = time.perf_counter()
    response = requests.get(f"{BASE_URL}/campaign/{CAMPAIGN_ID_TO_TEST}/performance")
    end_time = time.perf_counter()

    if response.status_code == 200:
        duration_ms = (end_time - start_time) * 1000
        results.append({
            "Endpoint": f"/campaign/{CAMPAIGN_ID_TO_TEST}/performance",
            "Cache Status": "MISS (Direct DB Query)",
            "Response Time (ms)": duration_ms
        })
        print(f"Initial request (CACHE MISS) took: {duration_ms:.2f} ms")
    else:
        print(f"Error on initial request: {response.status_code}")

    # Test 2: Cached Query (Cache Hit) for Campaign
    print("\nStep 2: Making subsequent requests to get a cached response...")
    total_cached_time = 0
    for i in range(NUM_REQUESTS):
        start_time = time.perf_counter()
        requests.get(f"{BASE_URL}/campaign/{CAMPAIGN_ID_TO_TEST}/performance")
        end_time = time.perf_counter()
        duration_ms = (end_time - start_time) * 1000
        total_cached_time += duration_ms
        print(f"Request {i + 1} (CACHE HIT) took: {duration_ms:.2f} ms")

    avg_cached_time = total_cached_time / NUM_REQUESTS
    results.append({
        "Endpoint": f"/campaign/{CAMPAIGN_ID_TO_TEST}/performance",
        "Cache Status": f"HIT (Avg. of {NUM_REQUESTS} requests)",
        "Response Time (ms)": avg_cached_time
    })

    # --- Benchmark Advertiser Endpoint ---
    print("\n\n--- Benchmarking Advertiser Spending Endpoint ---")
    print(f"Testing with ADVERTISER_ID = {ADVERTISER_ID_TO_TEST}")

    # Test 3: Direct DB Query (Cache Miss) for Advertiser
    print("\nStep 1: Clearing cache to ensure a direct DB query...")
    clear_redis_cache()

    start_time_adv = time.perf_counter()
    response_adv = requests.get(f"{BASE_URL}/advertiser/{ADVERTISER_ID_TO_TEST}/spending")
    end_time_adv = time.perf_counter()

    if response_adv.status_code == 200:
        duration_ms_adv = (end_time_adv - start_time_adv) * 1000
        results.append({
            "Endpoint": f"/advertiser/{ADVERTISER_ID_TO_TEST}/spending",
            "Cache Status": "MISS (Direct DB Query)",
            "Response Time (ms)": duration_ms_adv
        })
        print(f"Initial request (CACHE MISS) took: {duration_ms_adv:.2f} ms")
    else:
        print(f"Error on initial request: {response_adv.status_code}")

    # Test 4: Cached Query (Cache Hit) for Advertiser
    print("\nStep 2: Making subsequent requests to get a cached response...")
    total_cached_time_adv = 0
    for i in range(NUM_REQUESTS):
        start_time_adv_hit = time.perf_counter()
        requests.get(f"{BASE_URL}/advertiser/{ADVERTISER_ID_TO_TEST}/spending")
        end_time_adv_hit = time.perf_counter()
        duration_ms = (end_time_adv_hit - start_time_adv_hit) * 1000
        total_cached_time_adv += duration_ms
        print(f"Request {i + 1} (CACHE HIT) took: {duration_ms:.2f} ms")

    avg_cached_time_adv = total_cached_time_adv / NUM_REQUESTS
    results.append({
        "Endpoint": f"/advertiser/{ADVERTISER_ID_TO_TEST}/spending",
        "Cache Status": f"HIT (Avg. of {NUM_REQUESTS} requests)",
        "Response Time (ms)": avg_cached_time_adv
    })

    # --- Print Combined Results Table ---
    df = pd.DataFrame(results)
    df["Response Time (ms)"] = df["Response Time (ms)"].round(2)

    print("\n\n--- COMPREHENSIVE BENCHMARKING RESULTS ---")
    print(df.to_string(index=False))

    # --- Calculate and print improvement conclusions ---
    print("\n--- Conclusions ---")
    if len(results) >= 2:
        campaign_miss_time = results[0]['Response Time (ms)']
        campaign_hit_time = results[1]['Response Time (ms)']
        if campaign_hit_time > 0:
            improvement = (campaign_miss_time / campaign_hit_time)
            print(f"Campaign Endpoint: Cached responses are ~{improvement:.1f}x faster.")

    if len(results) >= 4:
        advertiser_miss_time = results[2]['Response Time (ms)']
        advertiser_hit_time = results[3]['Response Time (ms)']
        if advertiser_hit_time > 0:
            improvement = (advertiser_miss_time / advertiser_hit_time)
            print(f"Advertiser Endpoint: Cached responses are ~{improvement:.1f}x faster.")


def main():
    try:
        run_full_benchmark()
    except requests.exceptions.ConnectionError:
        print("\nERROR: Cannot connect to the API at", BASE_URL)
        print("Please ensure the API is running and accessible.")


if __name__ == "__main__":
    main()
