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

def get_canonical_title(title: str) -> str:
    """Extract canonical stripped title for deduplication (removes remixes, movies, brackets)."""
    clean = re.sub(r'\(.*?\)|\[.*?\]', '', title or '')
    clean = re.sub(r'[^a-zA-Z0-9]', '', clean).lower()
    return clean

async def fetch_saavn_song_on_the_fly(query: str, client: Client, preferred_genre: Optional[str] = None) -> Optional[dict]:
    """Dynamically search and persist original song from JioSaavn CDN if missing from catalog."""
    try:
        q_clean = query.strip()
        url = f"https://www.jiosaavn.com/api.php?__call=search.getResults&_format=json&_marker=0&n=5&p=1&q={quote(q_clean)}"
        async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}, timeout=6.5) as hclient:
            r = await hclient.get(url)
            if r.status_code != 200:
                return None
            data = json.loads(r.text.strip())
            results = data.get("results", [])
            if not results:
                return None

            top = results[0]
            if preferred_genre and preferred_genre != "mix":
                target_g = preferred_genre.strip().lower()
                for item in results:
                    item_lang = (item.get("language") or "").strip().lower()
                    if item_lang == target_g:
                        top = item
                        break

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
            raw_lang = (top.get("language") or "").strip().lower()
            lang = raw_lang.capitalize() if raw_lang else (preferred_genre.capitalize() if preferred_genre else "Tamil")
            genre = raw_lang if raw_lang in ["tamil", "hindi", "english", "malayalam", "phonk", "telugu"] else (preferred_genre.lower() if preferred_genre else "mix")
            
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
    Integrates authentic YouTube Music radio watch playlist recommendations,
    applies strict anti-fatigue filtering against the user's recent listening history,
    canonical title deduplication (eliminating remixes/versions), and strict genre retention.
    """
    import random
    import asyncio

    REGIONAL_LANGS = {"hindi", "tamil", "malayalam", "telugu", "kannada", "bengali", "punjabi"}

    seed_id = str(seed_song.get("id", ""))
    seed_title = (seed_song.get("title") or "").strip()
    seed_artist = (seed_song.get("artist") or "").strip()
    seed_genre = (seed_song.get("genre") or "mix").strip().lower()
    seed_lang = (seed_song.get("language") or seed_genre or "mix").strip().lower()
    effective_target = seed_lang if seed_lang in REGIONAL_LANGS else seed_genre

    excluded = {str(x).strip() for x in (exclude_ids or []) if str(x).strip()}
    if seed_id:
        excluded.add(seed_id)

    seen_ids = set(excluded)
    norm_seed_base = get_canonical_title(seed_title)
    seen_titles = {norm_seed_base} if norm_seed_base else set()

    # 1. Anti-fatigue: exclude last 60 tracks played by this user so the queue NEVER loops played songs
    if user_id and user_id not in ["guest", "anonymous"]:
        try:
            recent_res = client.table("play_events")\
                .select("song_id, songs(title)")\
                .eq("user_id", user_id)\
                .order("played_at", desc=True)\
                .limit(60)\
                .execute()
            for row in (recent_res.data or []):
                sid = row.get("song_id")
                if sid:
                    seen_ids.add(str(sid))
                s_obj = row.get("songs")
                if isinstance(s_obj, dict):
                    t_canon = get_canonical_title(s_obj.get("title"))
                    if t_canon:
                        seen_titles.add(t_canon)
        except Exception as e:
            print(f"Radio recent events exclusion error: {e}")

    queue: List[dict] = []
    artist_counts = {}

    def can_add_song(s: dict) -> bool:
        sid = str(s.get("id", ""))
        if not sid or sid in seen_ids:
            return False
        canon = get_canonical_title(s.get("title", ""))
        if not canon or canon in seen_titles:
            return False
        g = (s.get("genre") or "mix").strip().lower()
        if effective_target and effective_target != "mix" and g != effective_target:
            return False

        # Strict linguistic integrity:
        # If seed is Hindi, NEVER allow Tamil, Telugu, Malayalam, etc.
        s_lang = (s.get("language") or g or "").strip().lower()
        s_title = (s.get("title") or "").strip()
        s_film = (s.get("film") or "").strip()

        if seed_lang in REGIONAL_LANGS:
            if s_lang in REGIONAL_LANGS and s_lang != seed_lang:
                return False
            if g in REGIONAL_LANGS and g != seed_lang:
                return False
            other_langs = REGIONAL_LANGS - {seed_lang}
            pattern = r'\b(' + '|'.join(other_langs) + r')\b'
            if re.search(pattern, s_title, re.I) or re.search(pattern, s_film, re.I):
                return False

        # Cap max 2 tracks per primary artist in Up Next for healthy diversity
        art = re.split(r"[,&|]|\sfeat\.?|\sft\.?", s.get("artist") or "", flags=re.I)[0].strip().lower()
        if art and artist_counts.get(art, 0) >= 2:
            return False
        return True

    def add_song(s: dict):
        sid = str(s.get("id", ""))
        canon = get_canonical_title(s.get("title", ""))
        seen_ids.add(sid)
        if canon:
            seen_titles.add(canon)
        art = re.split(r"[,&|]|\sfeat\.?|\sft\.?", s.get("artist") or "", flags=re.I)[0].strip().lower()
        if art:
            artist_counts[art] = artist_counts.get(art, 0) + 1
        queue.append(s)

    # 2. Datacenter-resilient Native Mood Recommendation Pipeline
    # JioSaavn's reco engine is 100% reliable on cloud hosting (Render), never blocked,
    # and directly returns 320kbps CDN tracks matching the exact mood, tempo, and genre.
    saavn_candidates = []
    cand_cache_key = f"beats:mood_cands:{seed_id}"
    if redis:
        try:
            cached_cands = await redis.get(cand_cache_key)
            if cached_cands:
                saavn_candidates = json.loads(cached_cands)
        except Exception:
            pass

    if not saavn_candidates:
        try:
            async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}, timeout=6.0) as hclient:
                # A. Search seed song on JioSaavn to get track PIDs
                s_query = f"{seed_title} {seed_artist}".strip()
                s_url = f"https://www.jiosaavn.com/api.php?__call=search.getResults&_format=json&_marker=0&n=5&p=1&q={quote(s_query)}"
                sr = await hclient.get(s_url)
                s_results = sr.json().get("results", []) if sr.status_code == 200 else []
                pids = [res.get("id") for res in s_results if res.get("id")]

                raw_recos = []
                for pid in pids[:2]:
                    try:
                        reco_url = f"https://www.jiosaavn.com/api.php?__call=reco.getreco&_format=json&_marker=0&pid={pid}"
                        rr = await hclient.get(reco_url)
                        rdata = rr.json()
                        if isinstance(rdata, list):
                            raw_recos.extend(rdata)
                        elif isinstance(rdata, dict):
                            raw_recos.extend(rdata.get(pid, []))
                    except Exception:
                        pass
                    if len(raw_recos) >= limit:
                        break

                # B. If recos are sparse, pull genre/mood specific tracks on-the-fly
                if len(raw_recos) < limit:
                    if seed_genre == "phonk":
                        fb_q = "drift phonk"
                    elif seed_lang in REGIONAL_LANGS:
                        fb_q = f"{seed_lang} hits"
                    elif effective_target and effective_target != "mix":
                        fb_q = f"{effective_target} hits"
                    else:
                        fb_q = f"{seed_artist} songs" if seed_artist else "trending songs"

                    try:
                        fb_url = f"https://www.jiosaavn.com/api.php?__call=search.getResults&_format=json&_marker=0&n=20&p=1&q={quote(fb_q)}"
                        fr = await hclient.get(fb_url)
                        raw_recos.extend(fr.json().get("results", []) if fr.status_code == 200 else [])
                    except Exception:
                        pass

                # C. Ingest, decrypt 320kbps stream URLs, and persist into Supabase
                ingested = []
                for item in raw_recos:
                    raw_t = html.unescape(item.get("song") or item.get("title") or "")
                    title = raw_t.replace("&quot;", '"').replace("&#039;", "'").strip()
                    canon = get_canonical_title(title)
                    if not canon or canon in seen_titles:
                        continue

                    enc = item.get("encrypted_media_url")
                    if not enc:
                        continue

                    raw_item_lang = (item.get("language") or "").strip().lower()
                    album_name = html.unescape(item.get("album") or "").strip()

                    # Linguistic filter on reco candidates (e.g. reject Tamil dubs of Hindi soundtracks)
                    if seed_lang in REGIONAL_LANGS:
                        if raw_item_lang in REGIONAL_LANGS and raw_item_lang != seed_lang:
                            continue
                        other_langs = REGIONAL_LANGS - {seed_lang}
                        pattern = r'\b(' + '|'.join(other_langs) + r')\b'
                        if re.search(pattern, title, re.I) or re.search(pattern, album_name, re.I):
                            continue

                    raw_a = html.unescape(item.get("singers") or item.get("primary_artists") or "Various")
                    artist = raw_a.replace("&quot;", '"').replace("&#039;", "'").strip()
                    stream_url = decrypt_saavn_url(enc)
                    thumb = (item.get("image") or "").replace("150x150", "500x500")
                    lang = raw_item_lang if raw_item_lang else (seed_lang if seed_lang != "mix" else "mix")
                    genre = lang if lang in ["tamil", "hindi", "english", "malayalam", "phonk", "telugu"] else "mix"

                    record = {
                        "title": title,
                        "artist": artist,
                        "film": album_name,
                        "genre": genre,
                        "language": lang,
                        "duration": int(item.get("duration") or 180),
                        "thumbnail_url": thumb,
                        "source_url": stream_url,
                        "source_site": "jiosaavn_cdn",
                        "play_count": 5000,
                        "status": "available"
                    }

                    saved = None
                    try:
                        ins = client.table("songs").insert(record).execute()
                        if ins.data:
                            saved = ins.data[0]
                    except Exception:
                        try:
                            ex = client.table("songs").select("*").eq("source_url", stream_url).limit(1).execute()
                            if ex.data:
                                saved = ex.data[0]
                        except Exception:
                            pass

                    if saved:
                        ingested.append(saved)
                    if len(ingested) >= limit * 2:
                        break

                saavn_candidates = ingested
                if redis and saavn_candidates:
                    try:
                        await redis.set(cand_cache_key, json.dumps(saavn_candidates), ex=3600)
                    except Exception:
                        pass
        except Exception as saavn_err:
            print(f"Native mood pipeline notice: {saavn_err}")

    # Add native mood candidates
    if saavn_candidates:
        for s in saavn_candidates:
            if can_add_song(s):
                add_song(s)
            if len(queue) >= limit:
                break

    # Secondary: YTMusic watch playlist (graceful timeout, ignored if datacenter blocks)
    if len(queue) < limit:
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
                        wp = yt.get_watch_playlist(videoId=vid, radio=True, limit=30)
                        return wp.get("tracks", [])
                except Exception:
                    pass
                return []

            tracks = await asyncio.wait_for(asyncio.to_thread(_fetch_yt_playlist), timeout=2.5)
            if tracks:
                for t in tracks:
                    t_title = (t.get("title") or "").strip()
                    canon_t = get_canonical_title(t_title)
                    if not canon_t or canon_t in seen_titles:
                        continue
                    clean_t = re.sub(r'\(.*?\)|\[.*?\]', '', t_title).strip()
                    artists = t.get("artists", [])
                    primary_art = artists[0].get("name", "") if artists else ""
                    if len(clean_t) >= 2:
                        dyn = await fetch_saavn_song_on_the_fly(f"{clean_t} {primary_art}".strip(), client, preferred_genre=seed_genre)
                        if dyn and can_add_song(dyn):
                            add_song(dyn)
                    if len(queue) >= limit:
                        break
        except Exception:
            pass

    # 3. Tier 1: Match songs by same artist (strictly same genre, max 2 songs)
    if len(queue) < limit and seed_artist and seed_artist != "Unknown":
        primary_artist = re.split(r"[,&|]|\sfeat\.?|\sft\.?", seed_artist, flags=re.I)[0].strip()
        if len(primary_artist) >= 3:
            try:
                artist_q = client.table("songs")\
                    .select("*")\
                    .eq("status", "available")\
                    .ilike("artist", f"%{primary_artist}%")
                if effective_target and effective_target != "mix":
                    artist_q = artist_q.eq("genre", effective_target)

                artist_res = artist_q.order("play_count", desc=True).limit(30).execute()
                a_songs = artist_res.data or []
                random.shuffle(a_songs)
                for s in a_songs:
                    if can_add_song(s):
                        add_song(s)
                    if len(queue) >= limit:
                        break
            except Exception as e:
                print(f"Artist queue query error: {e}")

    # 4. Tier 2: Match songs from the SAME genre/language (strictly preserve linguistic atmosphere)
    if len(queue) < limit and effective_target and effective_target != "mix":
        try:
            genre_res = client.table("songs")\
                .select("*")\
                .eq("status", "available")\
                .eq("genre", effective_target)\
                .order("play_count", desc=True)\
                .limit(100)\
                .execute()
            g_songs = genre_res.data or []
            random.shuffle(g_songs)
            for s in g_songs:
                if can_add_song(s):
                    add_song(s)
                if len(queue) >= limit:
                    break
        except Exception as e:
            print(f"Genre queue query error: {e}")

    # 5. Tier 3: Same genre/language trending backfill
    if len(queue) < limit:
        try:
            pop_q = client.table("songs").select("*").eq("status", "available")
            if effective_target and effective_target != "mix":
                pop_q = pop_q.eq("genre", effective_target)
            pop_res = pop_q.order("play_count", desc=True).limit(100).execute()
            p_songs = pop_res.data or []
            random.shuffle(p_songs)
            for s in p_songs:
                if can_add_song(s):
                    add_song(s)
                if len(queue) >= limit:
                    break
        except Exception as e:
            print(f"Trending queue backfill error: {e}")

    return queue[:limit]
