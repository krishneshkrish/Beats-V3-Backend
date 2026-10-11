#!/usr/bin/env python3
"""
Beats V3 Keep-Alive Ping Daemon
Prevents Render free tier backend from spinning down due to 15-minute inactivity.
Pings the backend health and trending endpoints every 5 minutes (300 seconds).
"""

import sys
import time
import argparse
from datetime import datetime
import urllib.request
import urllib.error
import json

DEFAULT_URL = "https://beats-api.onrender.com"
DEFAULT_INTERVAL_SEC = 300  # 5 minutes

ENDPOINTS = [
    "/health",
    "/api/trending",
]


def ping_endpoint(base_url: str, endpoint: str) -> dict:
    url = f"{base_url.rstrip('/')}{endpoint}"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "BeatsV3-KeepAlive-Ping/1.0",
            "Accept": "application/json, text/plain, */*",
        },
    )
    start_time = time.time()
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            elapsed_ms = int((time.time() - start_time) * 1000)
            status = resp.status
            return {"ok": True, "status": status, "elapsed_ms": elapsed_ms, "url": url}
    except urllib.error.HTTPError as e:
        elapsed_ms = int((time.time() - start_time) * 1000)
        return {"ok": False, "status": e.code, "elapsed_ms": elapsed_ms, "url": url, "error": str(e)}
    except Exception as e:
        elapsed_ms = int((time.time() - start_time) * 1000)
        return {"ok": False, "status": 0, "elapsed_ms": elapsed_ms, "url": url, "error": str(e)}


def main():
    parser = argparse.ArgumentParser(description="Keep Render backend awake with 5-minute mock pings.")
    parser.add_argument(
        "url",
        nargs="?",
        default=DEFAULT_URL,
        help=f"Target backend URL (default: {DEFAULT_URL})",
    )
    parser.add_argument(
        "--interval",
        "-i",
        type=int,
        default=DEFAULT_INTERVAL_SEC,
        help=f"Ping interval in seconds (default: {DEFAULT_INTERVAL_SEC}s / 5 mins)",
    )

    args = parser.parse_args()
    base_url = args.url.rstrip("/")
    interval = max(60, args.interval)

    print("=" * 65)
    print(" 🚀 BEATS V3 BACKEND KEEP-ALIVE DAEMON")
    print(f" Target URL : {base_url}")
    print(f" Interval   : {interval} seconds ({interval // 60} mins)")
    print("=" * 65)
    print("Press CTRL+C anytime to stop.\n")

    iteration = 1
    while True:
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        target_ep = ENDPOINTS[(iteration - 1) % len(ENDPOINTS)]
        
        result = ping_endpoint(base_url, target_ep)
        
        if result["ok"]:
            print(f"[{now_str}] [PING #{iteration}] {result['url']} -> HTTP {result['status']} ({result['elapsed_ms']}ms) [ALIVE]")
        else:
            status_desc = f"HTTP {result['status']}" if result['status'] else "Connection Error"
            print(f"[{now_str}] [PING #{iteration}] {result['url']} -> {status_desc} ({result['elapsed_ms']}ms) - {result.get('error', 'Failed')}")

        iteration += 1
        try:
            time.sleep(interval)
        except KeyboardInterrupt:
            print("\n[Stopped] Keep-alive daemon terminated gracefully.")
            sys.exit(0)


if __name__ == "__main__":
    main()
