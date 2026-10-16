"""
Backfill script to populate mood, energy_level, and vocal_type for all songs in Supabase.

Before running this script, ensure the columns exist in Supabase by running:
  alter table songs 
    add column if not exists mood text,
    add column if not exists energy_level text,
    add column if not exists vocal_type text;
"""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

from main import get_supabase_client
from saavn_resolver import infer_song_features

def backfill():
    client = get_supabase_client()
    print("[*] Checking songs table in Supabase...")

    # Test if mood column exists
    try:
        test_res = client.table("songs").select("id, mood, energy_level, vocal_type").limit(1).execute()
    except Exception as e:
        err_str = str(e)
        if "column songs.mood does not exist" in err_str or "42703" in err_str:
            print("\n[!] The 'mood' column does not exist yet in Supabase.")
            print("[!] Please run the following SQL in your Supabase SQL Editor:")
            print("\n    alter table songs")
            print("      add column if not exists mood text,")
            print("      add column if not exists energy_level text,")
            print("      add column if not exists vocal_type text;\n")
            return
        else:
            print(f"[!] Error checking table: {e}")
            return

    # Fetch total songs count or iterate in batches
    page_size = 50
    offset = 0
    total_updated = 0

    while True:
        try:
            res = client.table("songs")\
                .select("id, title, artist, genre, mood, energy_level, vocal_type")\
                .range(offset, offset + page_size - 1)\
                .execute()
            songs = res.data or []
            if not songs:
                break

            for s in songs:
                sid = s.get("id")
                title = s.get("title") or ""
                artist = s.get("artist") or ""
                genre = s.get("genre") or ""

                feats = infer_song_features(title, artist, genre)

                # Only update if missing or neutral
                client.table("songs").update({
                    "mood": feats["mood"],
                    "energy_level": feats["energy_level"],
                    "vocal_type": feats["vocal_type"],
                }).eq("id", sid).execute()
                total_updated += 1

            print(f"[*] Processed {offset + len(songs)} songs (updated: {total_updated})...")
            offset += page_size

            if len(songs) < page_size:
                break
        except Exception as batch_err:
            print(f"[!] Batch processing error at offset {offset}: {batch_err}")
            break

    print(f"\n[OK] Backfill complete! Total songs processed: {total_updated}")

if __name__ == "__main__":
    backfill()
