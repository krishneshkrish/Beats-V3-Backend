import os
import sys
from datetime import datetime, timedelta, timezone
from collections import Counter, defaultdict

import redis
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()


def run():
    print("=== [Job: Hot Songs] Starting trending refresh ===")

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
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    # 1. Fetch play events from last 7 days
    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    print(f"Fetching play_events since: {cutoff}")

    try:
        res = supabase.table("play_events")\
            .select("song_id, genre")\
            .gte("played_at", cutoff)\
            .execute()
        events = res.data or []
        print(f"Found {len(events)} play events in the last 7 days.")
    except Exception as e:
        print(f"[X] Failed to query play_events: {e}")
        events = []

    global_counts = Counter()
    genre_counts = defaultdict(Counter)

    for ev in events:
        song_id = ev.get("song_id")
        genre = ev.get("genre")
        if song_id:
            global_counts[song_id] += 1
            if genre:
                genre_counts[genre][song_id] += 1

    # 2. Fallback: If cold start / no play events yet, seed hot lists from top played songs
    if not global_counts:
        print("[i] No recent play events found. Seeding hot sets from songs.play_count...")
        try:
            top_songs = supabase.table("songs")\
                .select("id, genre, play_count")\
                .eq("status", "available")\
                .order("play_count", desc=True)\
                .limit(200)\
                .execute()
            for s in (top_songs.data or []):
                sid = s["id"]
                p_count = max(int(s.get("play_count") or 1), 1)
                global_counts[sid] = p_count
                g = s.get("genre")
                if g:
                    genre_counts[g][sid] = p_count
        except Exception as e:
            print(f"[X] Fallback query error: {e}")

    # 3. Update Redis sorted sets
    pipe = r.pipeline()

    if global_counts:
        pipe.delete("beats:hot:global")
        # Redis zadd takes mapping {member: score}
        pipe.zadd("beats:hot:global", dict(global_counts))
        pipe.expire("beats:hot:global", 604800)  # 7 days TTL

    for genre, counts in genre_counts.items():
        if counts:
            g_key = f"beats:hot:genre:{genre}"
            pipe.delete(g_key)
            pipe.zadd(g_key, dict(counts))
            pipe.expire(g_key, 604800)

    pipe.execute()
    print(f"[OK] Updated Redis hot sets: {len(global_counts)} global songs across {len(genre_counts)} genres.")


if __name__ == "__main__":
    run()
