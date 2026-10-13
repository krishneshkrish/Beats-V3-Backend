import io
import math
import joblib
from collections import Counter
from datetime import datetime
import numpy as np
import pandas as pd
import pytz

IST = pytz.timezone("Asia/Kolkata")


def get_time_block(hour: int = None) -> dict:
    """
    Returns time-of-day block metadata, curated acoustic mood, and default genre preferences.
    """
    if hour is None:
        hour = datetime.now(IST).hour

    if 5 <= hour < 12:
        return {
            "key": "morning",
            "name": "Morning Flow",
            "tag": "MORNING FLOW",
            "greeting": "GOOD MORNING",
            "description": "Uplifting, acoustic, and fresh tracks to start your day",
            "default_genres": ["malayalam", "hindi", "english"],
            "mood": "calm_uplifting"
        }
    elif 12 <= hour < 17:
        return {
            "key": "afternoon",
            "name": "Afternoon Energy",
            "tag": "AFTERNOON ENERGY",
            "greeting": "GOOD AFTERNOON",
            "description": "High energy and focus beats for your midday flow",
            "default_genres": ["english", "phonk", "tamil"],
            "mood": "energetic_focus"
        }
    elif 17 <= hour < 22:
        return {
            "key": "evening",
            "name": "Evening Unwind",
            "tag": "EVENING UNWIND",
            "greeting": "GOOD EVENING",
            "description": "Trending hits and vibe tracks to wrap up the day",
            "default_genres": ["tamil", "malayalam", "hindi"],
            "mood": "vibrant_chill"
        }
    else:
        return {
            "key": "night",
            "name": "Late Night Melodies",
            "tag": "LATE NIGHT MELODIES",
            "greeting": "LATE NIGHT",
            "description": "Deep melodies, lo-fi, and soothing nocturnal sounds",
            "default_genres": ["malayalam", "tamil", "phonk", "english"],
            "mood": "nocturnal_relaxing"
        }


from saavn_resolver import TEMPLATE_SONG_MAP, is_valid_uuid, fetch_saavn_song_on_the_fly, build_ytmusic_mood_queue


async def rule_based_recs(
    user_id: str,
    limit: int,
    supabase,
    redis,
    now: datetime = None
) -> list:
    """
    Enhanced AI Recommendation Engine:
    Combines user taste profiling, artist affinity mining, time-of-day acoustic mood flow,
    and cross-genre trending discovery with anti-fatigue filtering.
    """
    if now is None:
        now = datetime.now(IST)

    hour = now.hour
    time_block = get_time_block(hour)
    preferred_genres = list(time_block["default_genres"])

    favorite_artists = []
    favorite_genres = []
    recent_played_ids = set()

    # 1. Mine user's listening profile from recent play events
    try:
        history = supabase.table("play_events")\
            .select("song_id, genre, hour_of_day, completed")\
            .eq("user_id", user_id)\
            .order("played_at", desc=True)\
            .limit(100)\
            .execute()

        events = history.data or []
        if events:
            # Robust anti-fatigue filtering (exclude last 50 played tracks regardless of completion status)
            recent_played_ids = {e["song_id"] for e in events[:50] if e.get("song_id")}

            # Genre weighting (completed events give 2x weight)
            genre_weights = Counter()
            for e in events:
                g = (e.get("genre") or "").strip().lower()
                if g:
                    genre_weights[g] += 2 if e.get("completed") else 1

            favorite_genres = [g for g, _ in genre_weights.most_common(3)]

            # Fetch artist affinity for completed tracks
            sample_song_ids = [e["song_id"] for e in events if e.get("completed") and is_valid_uuid(e.get("song_id"))][:25]
            if sample_song_ids:
                s_res = supabase.table("songs").select("artist").in_("id", sample_song_ids).execute()
                artist_counts = Counter(s.get("artist") for s in (s_res.data or []) if s.get("artist"))
                favorite_artists = [a for a, _ in artist_counts.most_common(4)]
    except Exception as e:
        print(f"Taste profile mining notice for {user_id}: {e}")

    # Combine user's mined genres with current time-block atmosphere
    blended_genres = []
    for g in favorite_genres + preferred_genres:
        if g not in blended_genres:
            blended_genres.append(g)

    import random
    recommended = []
    seen_ids = set(recent_played_ids)

    # 2. Tier 1: User's Favorite Artist Tracks (Personal Anchor - ~35%)
    artist_quota = max(3, int(limit * 0.35))
    if favorite_artists:
        for artist in favorite_artists:
            try:
                a_res = supabase.table("songs")\
                    .select("*")\
                    .eq("status", "available")\
                    .ilike("artist", f"%{artist[:20]}%")\
                    .order("play_count", desc=True)\
                    .limit(20)\
                    .execute()
                a_songs = a_res.data or []
                random.shuffle(a_songs)
                for s in a_songs:
                    if s["id"] not in seen_ids:
                        seen_ids.add(s["id"])
                        recommended.append(s)
                    if len(recommended) >= artist_quota:
                        break
            except Exception:
                pass
            if len(recommended) >= artist_quota:
                break

    # 3. Tier 2: Time-of-Day Acoustic Mood Flow (~40%)
    mood_quota = max(4, int(limit * 0.40))
    target_genres = blended_genres[:3]
    try:
        mood_res = supabase.table("songs")\
            .select("*")\
            .eq("status", "available")\
            .in_("genre", target_genres)\
            .order("play_count", desc=True)\
            .limit(60)\
            .execute()
        m_songs = mood_res.data or []
        # Dynamic variety: shuffle candidate pool to avoid static top-list looping
        random.shuffle(m_songs)
        for s in m_songs:
            if s["id"] not in seen_ids:
                seen_ids.add(s["id"])
                recommended.append(s)
            if len(recommended) >= (artist_quota + mood_quota):
                break
    except Exception as e:
        print(f"Mood query error: {e}")

    # 4. Tier 3: Cross-Genre Trending Discovery (~25%)
    try:
        trending_res = supabase.table("songs")\
            .select("*")\
            .eq("status", "available")\
            .order("play_count", desc=True)\
            .limit(80)\
            .execute()
        t_songs = trending_res.data or []
        random.shuffle(t_songs)
        for s in t_songs:
            if s["id"] not in seen_ids:
                seen_ids.add(s["id"])
                recommended.append(s)
            if len(recommended) >= limit:
                break
    except Exception as e:
        print(f"Trending discovery error: {e}")

    # 5. Fallback backfill if database has limited filtered songs
    if len(recommended) < limit:
        try:
            all_res = supabase.table("songs")\
                .select("*")\
                .eq("status", "available")\
                .limit(limit * 3)\
                .execute()
            all_songs = all_res.data or []
            random.shuffle(all_songs)
            for s in all_songs:
                if s["id"] not in seen_ids:
                    seen_ids.add(s["id"])
                    recommended.append(s)
                if len(recommended) >= limit:
                    break
        except Exception:
            pass

    return recommended[:limit]


async def radio_recs(
    seed_song_id: str,
    user_id: str,
    limit: int,
    supabase,
    redis,
    exclude_ids: list = None,
    now: datetime = None
) -> list:
    """
    Automated Up Next Queue (Authentic YouTube Music Experience).
    Extracts the exact watch playlist from YouTube Music for the seed track,
    ensuring 100% mood, vibe, tempo, and acoustic synergy.
    Every track is verified and resolved to direct 320kbps CDN stream URLs. Zero yt-dlp.
    """
    if now is None:
        now = datetime.now(IST)

    # 0. Check template ID mapping
    actual_seed_id = TEMPLATE_SONG_MAP.get(seed_song_id, seed_song_id)

    # 1. Fetch seed song details
    seed_song = None
    if is_valid_uuid(actual_seed_id):
        try:
            res = supabase.table("songs").select("*").eq("id", actual_seed_id).execute()
            if res.data:
                seed_song = res.data[0]
        except Exception as e:
            print(f"Error fetching seed song {actual_seed_id}: {e}")

    if not seed_song:
        # Search by title/slug or fetch on-the-fly
        clean_q = seed_song_id.replace("song-", "").replace("-", " ").strip()
        try:
            res = supabase.table("songs").select("*").ilike("title", f"%{clean_q}%").limit(1).execute()
            if res.data:
                seed_song = res.data[0]
        except Exception:
            pass

    if not seed_song:
        seed_song = await fetch_saavn_song_on_the_fly(clean_q, supabase)

    if not seed_song:
        # Fallback to general time-aware recs
        return await rule_based_recs(user_id, limit, supabase, redis, now)

    # 2. Build exact YouTube Music mood-matched Up Next queue with user anti-fatigue filtering
    return await build_ytmusic_mood_queue(
        seed_song=seed_song,
        limit=limit,
        client=supabase,
        redis=redis,
        exclude_ids=exclude_ids,
        user_id=user_id,
    )


async def ml_recs(
    user_id: str,
    limit: int,
    supabase,
    redis
) -> list:
    """
    Machine Learning recommendation engine.
    Loads serialized LightGBM model from Redis and ranks candidate songs based on user affinity,
    time-of-day features, and song popularity.
    """
    now = datetime.now(IST)

    # 1. Load model from Redis
    if not redis:
        return []

    model_bytes = await redis.get("beats:model:rec")
    if not model_bytes:
        return []

    try:
        import base64
        import warnings
        if isinstance(model_bytes, bytes):
            try:
                raw_bytes = base64.b64decode(model_bytes)
            except Exception:
                raw_bytes = model_bytes
        elif isinstance(model_bytes, str):
            try:
                raw_bytes = base64.b64decode(model_bytes)
            except Exception:
                raw_bytes = model_bytes.encode("latin-1")
        else:
            raw_bytes = model_bytes

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore")
            model_data = joblib.load(io.BytesIO(raw_bytes))
    except Exception as e:
        print(f"Error unpickling model from Redis: {e}")
        return []

    model = model_data.get("model")
    feature_cols = model_data.get("feature_cols", [])
    genres = model_data.get("genres", ["tamil", "malayalam", "hindi", "english", "phonk", "mix"])

    if not model:
        return []

    # 2. Mine user listening history & preferences
    history = supabase.table("play_events")\
        .select("song_id, genre, completed")\
        .eq("user_id", user_id)\
        .order("played_at", desc=True)\
        .limit(100)\
        .execute()
    events = history.data or []

    # Anti-fatigue filtering (songs listened to recently)
    recent_played_ids = {e["song_id"] for e in events[:50] if e.get("song_id")}

    # Mine favorite artists from history
    favorite_artists = set()
    history_song_ids = [e["song_id"] for e in events if e.get("song_id")][:50]
    if history_song_ids:
        try:
            artist_res = supabase.table("songs").select("artist").in_("id", history_song_ids).execute()
            for row in (artist_res.data or []):
                a = (row.get("artist") or "").split(",")[0].strip()
                if len(a) >= 3:
                    favorite_artists.add(a.lower())
        except Exception:
            pass

    genre_counts = Counter(e["genre"] for e in events if e.get("genre"))
    top_user_genres = [g for g, _ in genre_counts.most_common(3)]
    total_genres = sum(genre_counts.values()) or 1
    genre_affinity = {
        g: genre_counts.get(g, 0) / total_genres
        for g in genres
    }

    completion_rate = sum(
        1 for e in events if e.get("completed")
    ) / max(len(events), 1)

    # 3. Dynamic multi-tier candidate pool: favorite artists + top genres + global trending
    candidates_dict = {}

    # Tier 1: Songs from user's favorite artists
    for artist_name in list(favorite_artists)[:6]:
        try:
            a_songs = supabase.table("songs")\
                .select("id, title, artist, genre, play_count, search_count")\
                .eq("status", "available")\
                .ilike("artist", f"%{artist_name}%")\
                .order("play_count", desc=True)\
                .limit(15)\
                .execute()
            for s in (a_songs.data or []):
                if s["id"] not in recent_played_ids and s["id"] not in candidates_dict:
                    candidates_dict[s["id"]] = s
        except Exception:
            pass

    # Tier 2: Top songs from user's preferred genres
    for g in top_user_genres:
        try:
            g_songs = supabase.table("songs")\
                .select("id, title, artist, genre, play_count, search_count")\
                .eq("status", "available")\
                .eq("genre", g)\
                .order("play_count", desc=True)\
                .limit(30)\
                .execute()
            for s in (g_songs.data or []):
                if s["id"] not in recent_played_ids and s["id"] not in candidates_dict:
                    candidates_dict[s["id"]] = s
        except Exception:
            pass

    # Tier 3: Global popular discovery backfill
    try:
        all_pop = supabase.table("songs")\
            .select("id, title, artist, genre, play_count, search_count")\
            .eq("status", "available")\
            .order("play_count", desc=True)\
            .limit(80)\
            .execute()
        for s in (all_pop.data or []):
            if s["id"] not in recent_played_ids and s["id"] not in candidates_dict:
                candidates_dict[s["id"]] = s
    except Exception:
        pass

    candidates_list = list(candidates_dict.values())
    if not candidates_list:
        return []

    # 4. Build feature matrix
    hour = now.hour
    day = now.weekday()
    rows = []

    for song in candidates_list:
        song_genre = song.get("genre", "")
        genre_idx = genres.index(song_genre) if song_genre in genres else len(genres) - 1

        row = {
            "hour_sin": math.sin(2 * math.pi * hour / 24),
            "hour_cos": math.cos(2 * math.pi * hour / 24),
            "day_sin": math.sin(2 * math.pi * day / 7),
            "day_cos": math.cos(2 * math.pi * day / 7),
            "is_weekend": int(day in [5, 6]),
            "genre_encoded": genre_idx,
            "play_count": song.get("play_count", 0) or 0,
            "search_count": song.get("search_count", 0) or 0,
            "user_completion_rate": completion_rate,
        }

        for g in genres:
            row[f"user_genre_{g}"] = genre_affinity.get(g, 0)

        rows.append(row)

    X = pd.DataFrame(rows)
    for col in feature_cols:
        if col not in X.columns:
            X[col] = 0
    X = X[feature_cols].fillna(0)

    # 5. Predict probabilities + apply artist affinity boost
    base_scores = model.predict_proba(X)[:, 1]
    final_scores = []
    for idx, song in enumerate(candidates_list):
        score = float(base_scores[idx])
        s_artist = (song.get("artist") or "").lower()
        if any(fa in s_artist for fa in favorite_artists):
            score += 0.35
        final_scores.append(score)

    top_idx = np.argsort(final_scores)[::-1][:limit]
    top_ids = [candidates_list[i]["id"] for i in top_idx]

    # 6. Fetch full song details and preserve rank ordering
    res = supabase.table("songs")\
        .select("*")\
        .in_("id", top_ids)\
        .execute()

    if not res.data:
        return [candidates_list[i] for i in top_idx]

    id_map = {s["id"]: s for s in res.data}
    ordered_songs = [id_map[sid] for sid in top_ids if sid in id_map]
    return ordered_songs
