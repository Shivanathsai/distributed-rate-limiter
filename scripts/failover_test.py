#!/usr/bin/env python3
"""
failover_test.py
================
Verifies the fail-open failover contract:

  When Redis becomes unavailable, the rate limiter MUST continue
  to respond with 200 (fail-open), never 500 (fail-closed).

Invoked by GitHub Actions in two steps:
  Step A — GitHub Actions kills the Redis container
  Step B — This script sends requests and asserts no 5xx responses

Usage:
    python scripts/failover_test.py --url http://localhost:8000 --mode assert-fail-open
    python scripts/failover_test.py --url http://localhost:8000 --mode assert-recovery

Exit code 0 = passed  |  Exit code 1 = failed
"""

import argparse
import asyncio
import sys
import time
from typing import List, Tuple

import httpx


async def _send_requests(
    url: str,
    total: int,
    concurrency: int,
) -> List[Tuple[int, float]]:
    """Return list of (status_code, latency_ms)."""
    sem     = asyncio.Semaphore(concurrency)
    results: List[Tuple[int, float]] = []

    async def _one(key: str) -> None:
        async with sem:
            t0 = time.perf_counter()
            try:
                async with httpx.AsyncClient() as client:
                    r = await client.post(
                        f"{url}/check",
                        params={"key": key, "limit": 10, "window_ms": 1_000},
                        timeout=3.0,
                    )
                lat = (time.perf_counter() - t0) * 1_000
                results.append((r.status_code, lat))
            except Exception as e:
                lat = (time.perf_counter() - t0) * 1_000
                print(f"    request error: {e}")
                results.append((0, lat))

    await asyncio.gather(*[_one(f"fo:{i}") for i in range(total)])
    return results


async def assert_fail_open(url: str, total: int, concurrency: int) -> bool:
    """
    After Redis has been stopped externally, send requests and verify
    the service NEVER returns 5xx — all responses must be 200 or 429.
    """
    print(f"\n  [failover] assert-fail-open — {total} requests to {url}")
    print("  Redis is DOWN — service must fail-open (200/429, never 500)")

    results = await _send_requests(url, total, concurrency)

    ok_200   = sum(1 for s, _ in results if s == 200)
    ok_429   = sum(1 for s, _ in results if s == 429)
    err_5xx  = sum(1 for s, _ in results if 500 <= s < 600)
    conn_err = sum(1 for s, _ in results if s == 0)

    print(f"\n  200 (allowed):    {ok_200:>6}")
    print(f"  429 (denied):     {ok_429:>6}")
    print(f"  5xx (ERROR):      {err_5xx:>6}  ← must be 0")
    print(f"  connection err:   {conn_err:>6}  ← must be 0")

    passed = err_5xx == 0 and conn_err == 0 and (ok_200 + ok_429) == total

    if passed:
        print("\n  ✓  Fail-open confirmed — no 5xx under Redis outage")
    else:
        print("\n  ✗  Fail-open VIOLATED — service returned 5xx when Redis was down")

    return passed


async def assert_recovery(url: str, timeout_s: int, concurrency: int) -> bool:
    """
    After Redis is restored, poll until the service returns to normal operation
    within timeout_s seconds.
    """
    print(f"\n  [failover] assert-recovery — waiting up to {timeout_s}s for Redis")

    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            async with httpx.AsyncClient() as client:
                r = await client.get(f"{url}/ready", timeout=3.0)
                if r.status_code == 200:
                    elapsed = timeout_s - (deadline - time.time())
                    print(f"  ✓  Service recovered in {elapsed:.1f}s")
                    return True
        except Exception:
            pass
        await asyncio.sleep(1)

    print(f"  ✗  Service did NOT recover within {timeout_s}s")
    return False


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url",         default="http://localhost:8000")
    parser.add_argument("--mode",        choices=["assert-fail-open", "assert-recovery"],
                        required=True)
    parser.add_argument("--total",       type=int, default=50)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--timeout",     type=int, default=30,
                        help="Recovery timeout in seconds")
    args = parser.parse_args()

    if args.mode == "assert-fail-open":
        passed = asyncio.run(assert_fail_open(args.url, args.total, args.concurrency))
    else:
        passed = asyncio.run(assert_recovery(args.url, args.timeout, args.concurrency))

    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
