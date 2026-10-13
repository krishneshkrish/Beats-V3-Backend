import re
import os
import sys
import json
import base64
import html
import uuid
import httpx
from urllib.parse import quote
from typing import Optional, List, Dict, Any
from Crypto.Cipher import DES
from ytmusicapi import YTMusic
from supabase import Client

TEMPLATE_SONG_MAP = {
    "song-khalifa-1": "2c640528-2f91-4b5f-b826-e92d34c57de9",
    "song-jimikki-2": "b92b2453-8080-4a84-9c4a-9abc2a47108a",
    "song-hukum-3": "4b3a58a3-0e51-4818-b0e8-ac2410c0579e",
    "song-starboy-4": "3c328ac7-bb20-4a4e-9b60-dcb10d9628cc",
    "song-phonk-5": "ced1b2ca-a2ad-488c-9456-832ab1207c60",
    "song-mix-6": "7bbfac8e-a54a-4266-8015-ba306989274c",
    "song-malayalam-7": "0c7f3fcb-2baa-436d-9185-e5cc76b2fce9",
    "song-tamil-8": "151df8fa-ed23-4931-bebf-6e4abdec53e3",
}

def is_valid_uuid(val: Any) -> bool:
    try:
        uuid.UUID(str(val))
        return True
    except (ValueError, AttributeError, TypeError):
        return False

def decrypt_saavn_url(enc_url: str) -> str:
    cipher = DES.new(b"38346591", DES.MODE_ECB)
    raw = cipher.decrypt(base64.b64decode(enc_url.strip()))
    pad_len = raw[-1]
    if 0 < pad_len <= 8:
        raw = raw[:-pad_len]
    url = raw.decode("utf-8", errors="ignore")
    return url.replace("_96.mp4", "_320.mp4")

async def fetch_saavn_song_on_the_fly(query: str, client: Client) -> Optional[dict]:
    """Dynamically search and persist original song from JioSaavn CDN if missing from catalog."""
    try:
        q_clean = query.strip()
        url = f"https://www.jiosaavn.com/api.php?__call=search.getResults&_format=json&_marker=0&n=1&p=1&q={quote(q_clean)}"
        async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}, timeout=6.5) as hclient:
            r = await hclient.get(url)
            if r.status_code != 200:
                return None
            data = json.loads(r.text.strip())
            results = data.get("results", [])
            if not results:
                return None
            top = results[0]
            enc = top.get("encrypted_media_url")
            if not enc:
                return None
            stream_url = decrypt_saavn_url(enc)
            raw_title = html.unescape(top.get("song") or top.get("title") or q_clean)
            title = raw_title.replace("&quot;", '"').replace("&#039;", "'").strip()
            raw_artist = html.unescape(top.get("singers") or top.get("primary_artists") or "Various")
            artist = raw_artist.replace("&quot;", '"').replace("&#039;", "'").strip()
            raw_film = html.unescape(top.get("album") or "")
            film = raw_film.replace("&quot;", '"').replace("&#039;", "'").strip()
            thumb = (top.get("image") or "").replace("150x150", "500x500")
            lang = top.get("language") or "Tamil"
            genre = lang.lower() if lang.lower() in ["tamil", "hindi", "english", "malayalam", "phonk"] else "mix"
            
            record = {
                "title": title,
                "artist": artist,
                "film": film,
                "genre": genre,
                "language": lang,
                "duration": int(top.get("duration") or 180),
                "thumbnail_url": thumb,
                "source_url": stream_url,
                "source_site": "jiosaavn_cdn",
                "play_count": 5000,
                "status": "available"
            }
            try:
                res = client.table("songs").insert(record).execute()
                if res.data:
                    return res.data[0]
            except Exception as ins_err:
                if "idx_songs_source_url" in str(ins_err) or "23505" in str(ins_err):
                    existing = client.table("songs").select("*").eq("source_url", stream_url).limit(1).execute()
                    if existing.data:
                        return existing.data[0]
                raise
    except Exception as e:
        print(f"Saavn dynamic fetch notice for '{query}': {e}")
    return None

async def build_ytmusic_mood_queue(
    seed_song: dict,
    limit: int,
    client: Client,
    redis = None,
    exclude_ids: List[str] = None,
    user_id: str = "guest",
) -> List[dict]:
    """
    Builds the Up Next queue matching the exact musical mood of the seed song.
    Integrates authentic YouTube Music watch playlist recommendations,
    applies strict anti-fatigue filtering against the user's recent listening history,
    and dynamically randomizes candidate pools to prevent repeating songs in the queue.
    """
    import random
    import asyncio

    seed_id = str(seed_song.get("id", ""))
    seed_title = (seed_song.get("title") or "").strip()
    seed_artist = (seed_song.get("artist") or "").strip()
    seed_genre = (seed_song.get("genre") or "mix").strip().lower()

    excluded = {str(x).strip() for x in (exclude_ids or []) if str(x).strip()}
    if seed_id:
        excluded.add(seed_id)

    # 1. Anti-fatigue: exclude last 60 tracks played by this user so the queue NEVER loops played songs
    if user_id and user_id != "guest":
        try:
            recent_res = client.table("play_events")\
                .select("song_id")\
                .eq("user_id", user_id)\
                .order("played_at", desc=True)\
                .limit(60)\
                .execute()
            for row in (recent_res.data or []):
                sid = row.get("song_id")
                if sid:
                    excluded.add(str(sid))
        except Exception as e:
            print(f"Radio recent events exclusion error: {e}")

    queue: List[dict] = []
    seen_titles = {re.sub(r"[^a-zA-Z0-9]", "", seed_title.lower())}
    seen_ids = set(excluded)

    # 2. Authentic YouTube Music Up Next watch playlist candidates
    yt_candidates = []
    cand_cache_key = f"beats:yt_candidates:{seed_id}"
    if redis:
        try:
            cached_cands = await redis.get(cand_cache_key)
            if cached_cands:
                yt_candidates = json.loads(cached_cands)
        except Exception:
            pass

    if not yt_candidates:
        try:
            def _fetch_yt_playlist():
                try:
                    yt = YTMusic()
                    query = f"{seed_title} {seed_artist}".strip()
                    search_res = yt.search(query, filter="songs")
                    if not search_res:
                        search_res = yt.search(seed_title, filter="songs")
                    if search_res and "videoId" in search_res[0]:
                        vid = search_res[0]["videoId"]
                        wp = yt.get_watch_playlist(videoId=vid, limit=35)
                        return wp.get("tracks", [])
                except Exception as ex:
                    print(f"YTMusic watch playlist fetch notice: {ex}")
                return []

            tracks = await asyncio.to_thread(_fetch_yt_playlist)
            if tracks:
                extracted_titles = []
                for t in tracks:
                    t_title = (t.get("title") or "").strip()
                    if t_title and t_title.lower() != seed_title.lower():
                        clean_t = re.sub(r'\(.*?\)|\[.*?\]', '', t_title).strip()
                        if clean_t and len(clean_t) >= 2:
                            extracted_titles.append(clean_t)

                matched_songs = []
                matched_seen = set()
                for bt in extracted_titles[:15]:
                    try:
                        m_res = client.table("songs")\
                            .select("*")\
                            .eq("status", "available")\
                            .ilike("title", f"%{bt[:20]}%")\
                            .limit(2)\
                            .execute()
                        for s in (m_res.data or []):
                            if s["id"] not in matched_seen and str(s["id"]) not in excluded:
                                matched_seen.add(s["id"])
                                matched_songs.append(s)
                    except Exception:
                        pass

                yt_candidates = matched_songs
                if redis and yt_candidates:
                    try:
                        await redis.set(cand_cache_key, json.dumps(yt_candidates), ex=3600)
                    except Exception:
                        pass
        except Exception as yt_err:
            print(f"YTMusic candidate resolution error: {yt_err}")

    # Add YTMusic matched candidates
    if yt_candidates:
        random.shuffle(yt_candidates)
        for s in yt_candidates:
            norm = re.sub(r"[^a-zA-Z0-9]", "", (s.get("title") or "").lower())
            if s["id"] not in seen_ids and norm not in seen_titles:
                seen_ids.add(s["id"])
                seen_titles.add(norm)
                queue.append(s)
            if len(queue) >= int(limit * 0.5):
                break

    # 3. Tier 1: Match songs by same artist / band
    if len(queue) < limit and seed_artist and seed_artist != "Unknown":
        primary_artist = re.split(r"[,&|]|\sfeat\.?|\sft\.?", seed_artist, flags=re.I)[0].strip()
        if len(primary_artist) >= 3:
            try:
                artist_res = client.table("songs")\
                    .select("*")\
                    .eq("status", "available")\
                    .ilike("artist", f"%{primary_artist}%")\
                    .order("play_count", desc=True)\
                    .limit(25)\
                    .execute()
                a_songs = artist_res.data or []
                random.shuffle(a_songs)
                for s in a_songs:
                    norm = re.sub(r"[^a-zA-Z0-9]", "", (s.get("title") or "").lower())
                    if s["id"] not in seen_ids and norm not in seen_titles:
                        seen_ids.add(s["id"])
                        seen_titles.add(norm)
                        queue.append(s)
                    if len(queue) >= int(limit * 0.65):
                        break
            except Exception as e:
                print(f"Artist queue query error: {e}")

    # 4. Tier 2: Match songs from the same genre with dynamic variety
    if len(queue) < limit and seed_genre:
        try:
            genre_res = client.table("songs")\
                .select("*")\
                .eq("status", "available")\
                .eq("genre", seed_genre)\
                .order("play_count", desc=True)\
                .limit(60)\
                .execute()
            g_songs = genre_res.data or []
            random.shuffle(g_songs)
            for s in g_songs:
                norm = re.sub(r"[^a-zA-Z0-9]", "", (s.get("title") or "").lower())
                if s["id"] not in seen_ids and norm not in seen_titles:
                    seen_ids.add(s["id"])
                    seen_titles.add(norm)
                    queue.append(s)
                if len(queue) >= limit:
                    break
        except Exception as e:
            print(f"Genre queue query error: {e}")

    # 5. Tier 3: Global popular & trending backfill with dynamic variety
    if len(queue) < limit:
        try:
            pop_res = client.table("songs")\
                .select("*")\
                .eq("status", "available")\
                .order("play_count", desc=True)\
                .limit(80)\
                .execute()
            p_songs = pop_res.data or []
            random.shuffle(p_songs)
            for s in p_songs:
                norm = re.sub(r"[^a-zA-Z0-9]", "", (s.get("title") or "").lower())
                if s["id"] not in seen_ids and norm not in seen_titles:
                    seen_ids.add(s["id"])
                    seen_titles.add(norm)
                    queue.append(s)
                if len(queue) >= limit:
                    break
        except Exception as e:
            print(f"Trending queue backfill error: {e}")

    return queue[:limit]
