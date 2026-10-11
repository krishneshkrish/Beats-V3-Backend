import os
import sys
from datetime import datetime, timezone
from collections import Counter

from groq import Groq
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()


def generate_taste_summary(groq_client: Groq, top_genres: list, top_artists: list, total_plays: int) -> str:
    """Generates an engaging, humanized 1-sentence listener taste profile using Groq LLM."""
    genre_str = ", ".join([f"{g} ({pct}%)" for g, pct in top_genres])
    artist_str = ", ".join(top_artists)

    prompt = f"""You are an insightful music psychologist creating a retro boombox music profile.
Based on this listener's weekly listening activity:
- Total tracks played: {total_plays}
- Top genres breakdown: {genre_str}
- Top artists: {artist_str}

Craft a punchy, ultra-cool, 1-to-2 sentence listening aesthetic summary (maximum 25 words).
Direct tone, vibrant vintage music slang, no quotation marks.
Example: High-voltage Tamil bass explorer with an appetite for synthwave night drives and melancholic Malayalam melodies.
"""
    try:
        completion = groq_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=60,
        )
        return completion.choices[0].message.content.strip().strip('"').strip("'")
    except Exception as e:
        print(f"[!] Warning: Groq LLM generation error: {e}")
        # Deterministic rich fallback
        primary_genre = top_genres[0][0] if top_genres else "diverse"
        return f"Passionate explorer of {primary_genre} cinema rhythms and high-energy electronic soundscapes."


def run():
    print("=== [Job: Taste Profile] Starting weekly taste profile summaries ===")

    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    groq_api_key = os.environ.get("GROQ_API_KEY")

    if not supabase_url or not supabase_key:
        print("[!] Warning: Missing SUPABASE_URL or SUPABASE_KEY in environment secrets. Skipping taste profile job.")
        return

    if not groq_api_key:
        print("[!] Warning: Missing GROQ_API_KEY in environment secrets. Skipping taste profile job.")
        return

    try:
        supabase = create_client(supabase_url, supabase_key)
        groq_client = Groq(api_key=groq_api_key)
    except Exception as e:
        print(f"[!] Warning: Could not initialize clients: {e}")
        return

    # 1. Fetch eligible users (10+ plays)
    try:
        users_res = supabase.table("users")\
            .select("id, name, play_count")\
            .gte("play_count", 10)\
            .execute()
        eligible_users = users_res.data or []
    except Exception as e:
        print(f"[!] Notice: Failed to query users: {e}")
        eligible_users = []

    print(f"Found {len(eligible_users)} active users eligible for taste profile refresh.")

    updated_count = 0
    for u in eligible_users:
        uid = u["id"]
        # Fetch user's play history
        try:
            ev_res = supabase.table("play_events")\
                .select("genre, song_id")\
                .eq("user_id", uid)\
                .limit(200)\
                .execute()
            events = ev_res.data or []
            if len(events) < 5:
                continue

            genre_counts = Counter(e.get("genre") for e in events if e.get("genre"))
            total_g = sum(genre_counts.values()) or 1
            top_genres = [(g, round((cnt / total_g) * 100)) for g, cnt in genre_counts.most_common(3)]

            # Fetch top artists from song IDs
            song_ids = [e.get("song_id") for e in events if e.get("song_id")][:50]
            artist_counts = Counter()
            if song_ids:
                s_res = supabase.table("songs")\
                    .select("id, artist")\
                    .in_("id", song_ids)\
                    .execute()
                for s in (s_res.data or []):
                    if s.get("artist"):
                        artist_counts[s["artist"]] += 1

            top_artists = [a for a, _ in artist_counts.most_common(3)]

            summary = generate_taste_summary(groq_client, top_genres, top_artists, len(events))
            print(f"  User {uid[:8]} ({u.get('name')}): \"{summary}\"")

            # Update Supabase user record
            supabase.table("users")\
                .update({"taste_profile": summary, "taste_profile_updated_at": datetime.now(timezone.utc).isoformat()})\
                .eq("id", uid)\
                .execute()
            updated_count += 1
        except Exception as err:
            print(f"  [X] Error updating user {uid[:8]}: {err}")

    print(f"[OK] Completed taste profile refresh. Updated {updated_count} user profiles.")


if __name__ == "__main__":
    run()
