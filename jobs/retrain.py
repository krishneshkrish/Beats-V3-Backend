import os
import io
import sys
import math
import joblib
from datetime import datetime, timedelta, timezone
from collections import Counter, defaultdict

import numpy as np
import pandas as pd
import lightgbm as lgb
import redis
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

GENRES = ["malayalam", "tamil", "hindi", "english", "phonk", "mix"]
FEATURE_COLS = [
    "hour_sin",
    "hour_cos",
    "day_sin",
    "day_cos",
    "is_weekend",
    "genre_encoded",
    "play_count",
    "search_count",
    "user_completion_rate",
    "user_genre_malayalam",
    "user_genre_tamil",
    "user_genre_hindi",
    "user_genre_english",
    "user_genre_phonk",
    "user_genre_mix",
]


def generate_synthetic_data():
    """Generates a realistic synthetic prior dataset for model bootstrap when live data is scarce."""
    np.random.seed(42)
    n = 200
    rows = []
    labels = []

    for _ in range(n):
        hour = np.random.randint(0, 24)
        day = np.random.randint(0, 7)
        genre_idx = np.random.randint(0, len(GENRES))
        selected_genre = GENRES[genre_idx]
        play_cnt = np.random.randint(0, 500)
        search_cnt = np.random.randint(0, 100)
        comp_rate = np.random.uniform(0.4, 0.95)

        row = {
            "hour_sin": math.sin(2 * math.pi * hour / 24),
            "hour_cos": math.cos(2 * math.pi * hour / 24),
            "day_sin": math.sin(2 * math.pi * day / 7),
            "day_cos": math.cos(2 * math.pi * day / 7),
            "is_weekend": int(day in [5, 6]),
            "genre_encoded": genre_idx,
            "play_count": play_cnt,
            "search_count": search_cnt,
            "user_completion_rate": comp_rate,
        }
        for g in GENRES:
            row[f"user_genre_{g}"] = 1 if g == selected_genre else 0

        rows.append(row)
        labels.append(1 if np.random.random() < comp_rate else 0)

    return pd.DataFrame(rows)[FEATURE_COLS], np.array(labels)


def run():
    print("=== [Job: Retrain] Starting LightGBM recommender retrain ===")

    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    redis_url = os.environ.get("REDIS_URL")

    if not supabase_url or not supabase_key:
        print("[!] Warning: Missing SUPABASE_URL or SUPABASE_KEY in environment secrets. Skipping retrain job.")
        return

    if not redis_url:
        print("[!] Warning: Missing REDIS_URL in environment secrets. Skipping retrain job.")
        return

    # Auto-fix Upstash redis:// to rediss:// for SSL support
    if "upstash.io" in redis_url and redis_url.startswith("redis://"):
        redis_url = redis_url.replace("redis://", "rediss://", 1)

    try:
        supabase = create_client(supabase_url, supabase_key)
    except Exception as e:
        print(f"[!] Warning: Could not initialize Supabase client: {e}")
        return

    try:
        r = redis.Redis.from_url(redis_url, socket_timeout=15, socket_connect_timeout=15)
        r.ping()
    except Exception as e:
        print(f"[!] Warning: Could not connect to Redis ({e}). Skipping retrain job.")
        return

    # 1. Pull last 30 days play_events from Supabase
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    print(f"Fetching play events since: {cutoff}")

    try:
        ev_res = supabase.table("play_events")\
            .select("user_id, song_id, genre, completed, hour_of_day, day_of_week, played_at")\
            .gte("played_at", cutoff)\
            .limit(5000)\
            .execute()
        events = ev_res.data or []
        print(f"Found {len(events)} historical events for training.")
    except Exception as e:
        print(f"[!] Notice: Failed to query play_events: {e}")
        events = []

    if len(events) < 50:
        print(f"[i] Insufficient live events ({len(events)} < 50). Using synthetic bootstrap dataset.")
        X, y = generate_synthetic_data()
    else:
        # Build user profile affinities
        user_affinities = defaultdict(lambda: defaultdict(int))
        user_completions = defaultdict(lambda: [0, 0])
        song_meta = defaultdict(lambda: {"play_count": 0, "search_count": 0})

        for e in events:
            uid = e.get("user_id") or "anon"
            genre = (e.get("genre") or "mix").lower()
            completed = bool(e.get("completed"))
            user_affinities[uid][genre] += 1
            user_completions[uid][1 if completed else 0] += 1
            sid = e.get("song_id")
            if sid:
                song_meta[sid]["play_count"] += 1

        rows = []
        labels = []
        for e in events:
            uid = e.get("user_id") or "anon"
            comps = user_completions[uid]
            total_u = sum(comps)
            comp_rate = comps[1] / total_u if total_u > 0 else 0.5

            aff_counts = user_affinities[uid]
            total_aff = sum(aff_counts.values()) or 1
            genre_affinity = {g: aff_counts.get(g, 0) / total_aff for g in GENRES}

            hour = e.get("hour_of_day") or 12
            day = e.get("day_of_week") or 0
            g_name = e.get("genre", "")
            g_idx = GENRES.index(g_name) if g_name in GENRES else len(GENRES) - 1

            sm = song_meta.get(e.get("song_id"), {"play_count": 0, "search_count": 0})

            row = {
                "hour_sin": math.sin(2 * math.pi * hour / 24),
                "hour_cos": math.cos(2 * math.pi * hour / 24),
                "day_sin": math.sin(2 * math.pi * day / 7),
                "day_cos": math.cos(2 * math.pi * day / 7),
                "is_weekend": int(day in [5, 6]),
                "genre_encoded": g_idx,
                "play_count": sm["play_count"],
                "search_count": sm["search_count"],
                "user_completion_rate": comp_rate,
            }
            for g in GENRES:
                row[f"user_genre_{g}"] = genre_affinity.get(g, 0)

            rows.append(row)
            labels.append(1 if e.get("completed") else 0)

        X = pd.DataFrame(rows)[FEATURE_COLS]
        y = np.array(labels)

    # 2. Train LightGBM Classifier
    print(f"Training LightGBM on {len(X)} samples with {len(FEATURE_COLS)} features...")
    model = lgb.LGBMClassifier(
        n_estimators=100,
        learning_rate=0.05,
        max_depth=5,
        num_leaves=31,
        random_state=42,
        verbose=-1,
    )
    model.fit(X, y)
    print("[OK] Model training completed.")

    # 3. Serialize and save to Redis with TTL 25h (90,000s)
    try:
        import base64
        buffer = io.BytesIO()
        model_payload = {
            "model": model,
            "feature_cols": FEATURE_COLS,
            "genres": GENRES,
            "trained_at": datetime.now(timezone.utc).isoformat(),
            "sample_count": len(X),
        }
        joblib.dump(model_payload, buffer)
        model_b64 = base64.b64encode(buffer.getvalue()).decode("ascii")

        r.set("beats:model:rec", model_b64)
        print("[OK] Serialized model stored permanently in Redis key 'beats:model:rec'.")
    except Exception as e:
        print(f"[!] Warning: Failed to save model to Redis: {e}")


if __name__ == "__main__":
    run()
