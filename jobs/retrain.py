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
        user_comp = np.random.uniform(0.3, 0.9)

        row = {
            "hour_sin": math.sin(2 * math.pi * hour / 24),
            "hour_cos": math.cos(2 * math.pi * hour / 24),
            "day_sin": math.sin(2 * math.pi * day / 7),
            "day_cos": math.cos(2 * math.pi * day / 7),
            "is_weekend": int(day in [5, 6]),
            "genre_encoded": genre_idx,
            "play_count": play_cnt,
            "search_count": search_cnt,
            "user_completion_rate": user_comp,
        }

        # Simulated affinity
        for g in GENRES:
            row[f"user_genre_{g}"] = 0.5 if g == selected_genre else np.random.uniform(0.0, 0.2)

        # Completion likelihood higher with higher completion rate and affinity
        prob = 0.4 * user_comp + (0.3 if play_cnt > 100 else 0.1) + np.random.uniform(0.0, 0.2)
        label = 1 if prob > 0.5 else 0

        rows.append(row)
        labels.append(label)

    return pd.DataFrame(rows)[FEATURE_COLS], np.array(labels)


def run():
    print("=== [Job: Retrain] Starting LightGBM recommender retrain ===")

    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    redis_url = os.environ.get("REDIS_URL")

    if not supabase_url or not supabase_key:
        print("[X] Missing SUPABASE_URL or SUPABASE_KEY. Exiting.")
        sys.exit(1)

    if not redis_url:
        print("[X] Missing REDIS_URL. Exiting.")
        sys.exit(1)

    supabase = create_client(supabase_url, supabase_key)
    r = redis.Redis.from_url(redis_url)

    # 1. Pull last 30 days play_events from Supabase
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    print(f"Fetching play events since: {cutoff}")

    try:
        ev_res = supabase.table("play_events")\
            .select("user_id, song_id, genre, completed, hour_of_day, day_of_week, played_at")\
            .gte("played_at", cutoff)\
            .execute()
        events = ev_res.data or []
        print(f"Found {len(events)} events in last 30 days.")
    except Exception as e:
        print(f"[X] Error querying play_events: {e}")
        events = []

    # Check if we need synthetic bootstrapping
    use_synthetic = len(events) < 30 or len(set(e.get("completed") for e in events)) < 2

    if use_synthetic:
        print("[i] Insufficient real play events for training. Using synthetic prior dataset...")
        X, y = generate_synthetic_data()
    else:
        # Precompute user stats
        user_history = defaultdict(list)
        song_ids = set()
        for e in events:
            uid = e.get("user_id")
            if uid:
                user_history[uid].append(e)
            if e.get("song_id"):
                song_ids.add(e["song_id"])

        # Fetch song popularity
        song_meta = {}
        if song_ids:
            try:
                s_res = supabase.table("songs")\
                    .select("id, play_count, search_count")\
                    .in_("id", list(song_ids)[:500])\
                    .execute()
                for s in (s_res.data or []):
                    song_meta[s["id"]] = {
                        "play_count": s.get("play_count") or 0,
                        "search_count": s.get("search_count") or 0,
                    }
            except Exception as e:
                print(f"Error fetching song metadata: {e}")

        rows = []
        labels = []
        for e in events:
            uid = e.get("user_id")
            u_events = user_history.get(uid, [])
            total_u = len(u_events) or 1
            comp_rate = sum(1 for x in u_events if x.get("completed")) / total_u

            genre_counts = Counter(x.get("genre") for x in u_events if x.get("genre"))
            genre_affinity = {g: genre_counts.get(g, 0) / total_u for g in GENRES}

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

    r.set("beats:model:rec", model_b64, ex=90000)
    print("[OK] Serialized model stored in Redis key 'beats:model:rec' (TTL 25 hours).")


if __name__ == "__main__":
    run()
