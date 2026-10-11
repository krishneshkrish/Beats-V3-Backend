import os
import sys
from collections import Counter
from typing import List, Dict, Any

from groq import Groq
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()


def summarize_user_taste(client: Groq, user_name: str, events: List[Dict[str, Any]], songs_map: Dict[str, Dict[str, Any]]) -> str:
    """Uses Groq llama-3.1-8b-instant to generate a 2-sentence vibe taste summary."""
    genres = [e.get("genre") for e in events if e.get("genre")]
    top_genres = [g for g, _ in Counter(genres).most_common(3)]

    completed_cnt = sum(1 for e in events if e.get("completed"))
    comp_rate = int((completed_cnt / max(len(events), 1)) * 100)

    fav_songs = []
    for e in events[:10]:
        sid = e.get("song_id")
        if sid in songs_map:
            s = songs_map[sid]
            fav_songs.append(f"{s.get('title')} by {s.get('artist')}")

    prompt = (
        f"You are a music curator for Beats streaming app.\n"
        f"User: {user_name or 'Music Lover'}\n"
        f"Total Recent Plays: {len(events)}\n"
        f"Completion Rate: {comp_rate}%\n"
        f"Top Genres: {', '.join(top_genres) if top_genres else 'Eclectic'}\n"
        f"Sample Played Songs: {'; '.join(fav_songs[:5]) if fav_songs else 'Various'}\n\n"
        f"Write an engaging, vibrant 2-sentence taste profile summary describing their music vibe, "
        f"listening personality, and mood. Return ONLY the 2-sentence summary without quotes or intro."
    )

    model_candidates = ["openai/gpt-oss-20b", "qwen/qwen3.8-27b", "llama-3.1-8b-instant"]
    response = None
    for m in model_candidates:
        try:
            response = client.chat.completions.create(
                model=m,
                messages=[
                    {"role": "system", "content": "You are a concise, insightful music taste profiler."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.7,
                max_tokens=120,
            )
            break
        except Exception as model_err:
            print(f"Model {m} failed: {model_err}")
            continue

    if not response or not response.choices:
        return f"Eclectic music lover leaning into {', '.join(top_genres) if top_genres else 'varied vibes'} with high replay affinity."

    return response.choices[0].message.content.strip()


def run():
    print("=== [Job: Taste Profile] Starting weekly taste profile summaries ===")

    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    groq_api_key = os.environ.get("GROQ_API_KEY")

    if not supabase_url or not supabase_key:
        print("[X] Missing SUPABASE_URL or SUPABASE_KEY. Exiting.")
        sys.exit(1)

    if not groq_api_key:
        print("[X] Missing GROQ_API_KEY. Exiting.")
        sys.exit(1)

    supabase = create_client(supabase_url, supabase_key)
    groq_client = Groq(api_key=groq_api_key)

    # 1. Fetch eligible users (10+ plays)
    try:
        users_res = supabase.table("users")\
            .select("id, name, play_count")\
            .gte("play_count", 10)\
            .execute()
        eligible_users = users_res.data or []
    except Exception as e:
        print(f"[X] Failed to query users: {e}")
        eligible_users = []

    # Also check if any users have 10+ play_events even if users.play_count wasn't incremented
    try:
        all_events_res = supabase.table("play_events")\
            .select("user_id")\
            .execute()
        user_event_counts = Counter(e["user_id"] for e in (all_events_res.data or []))
        additional_uids = [uid for uid, c in user_event_counts.items() if c >= 10]

        existing_ids = {u["id"] for u in eligible_users}
        for uid in additional_uids:
            if uid not in existing_ids:
                eligible_users.append({"id": uid, "name": uid, "play_count": user_event_counts[uid]})
    except Exception as e:
        print(f"Notice during play_events check: {e}")

    print(f"Found {len(eligible_users)} users eligible for taste profile generation.")
    if not eligible_users:
        print("No users with 10+ plays found yet. Done.")
        return

    updated_count = 0
    for u in eligible_users:
        uid = u["id"]
        uname = u.get("name") or uid

        # Fetch user's play events
        try:
            ev_res = supabase.table("play_events")\
                .select("song_id, genre, completed")\
                .eq("user_id", uid)\
                .order("played_at", desc=True)\
                .limit(50)\
                .execute()
            user_events = ev_res.data or []
            if not user_events:
                continue

            # Fetch metadata for songs
            song_ids = list({e["song_id"] for e in user_events if e.get("song_id")})
            songs_map = {}
            if song_ids:
                s_res = supabase.table("songs")\
                    .select("id, title, artist")\
                    .in_("id", song_ids[:30])\
                    .execute()
                for s in (s_res.data or []):
                    songs_map[s["id"]] = s

            # Generate taste summary via Groq
            summary = summarize_user_taste(groq_client, uname, user_events, songs_map)
            print(f"  User {uname}: {summary}")

            # Update Supabase users.taste_profile
            supabase.table("users")\
                .update({"taste_profile": summary})\
                .eq("id", uid)\
                .execute()

            updated_count += 1
        except Exception as e:
            print(f"  [X] Error generating taste profile for user {uid}: {e}")

    print(f"[OK] Taste profile job completed: updated {updated_count} user profiles.")


if __name__ == "__main__":
    run()
