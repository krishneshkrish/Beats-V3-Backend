import re
import os
import sys
import json
import base64
import html
import uuid
import httpx
import random
import asyncio
from datetime import datetime
from urllib.parse import quote
from typing import Optional, List, Dict, Any
from Crypto.Cipher import DES
from ytmusicapi import YTMusic
from supabase import Client
import pytz

IST = pytz.timezone("Asia/Kolkata")

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

def get_primary_artist(song: dict) -> str:
    """Extract normalized primary artist name for diversity checks."""
    art = song.get("artist") or ""
    return re.split(r"[,&|]|\sfeat\.?|\sft\.?", art, flags=re.I)[0].strip().lower()

# ─── SONG FEATURES INFERENCE ───

def infer_mood(title: str, artist: str = "", genre: str = "") -> str:
    """
    Infers mood string based on title keywords and musical context.
    Allowed values: 'cinematic' | 'romantic' | 'sad' | 'energetic' | 'dark' | 'chill' | 'neutral'
    """
    t = (title or "").lower()
    g = (genre or "").lower()
    a = (artist or "").lower()

    # 1. Dark / Gothic / Devil / Horror / Shadow
    if re.search(r'\b(dark|demon|devil|shadow|evil|doom|horror|blood|gothic|sinister|nightmare|assassin|psycho|ghost|curse|wrath|rage|death|underworld|monster|hell|wicked|venom|creepy)\b', t):
        return "dark"

    # 2. Sad / Heartbreak / Pain / Grief
    if re.search(r'\b(sad|cry|crying|tears|dard|judaai|broken|alone|lonely|heartache|pain|sorrow|breakup|goodbye|yaadein|dukh|viraham|kanneer|pirivu|miss you|void|hurt|lost|distance|alvida|dhoke|bewafa|tanhai|marham|gham|rona|judai)\b', t):
        return "sad"

    # 3. Romantic / Love / Ballad
    if re.search(r'\b(love|romantic|romance|dil|pyaar|kadhal|prema|premam|ishq|darling|sweetheart|jaan|sanam|baby|kiss|wedding|soulmate|heartbeat|nee|ennai|kanna|priya|manam|mohabbat|humsafar|deewana|anbe|azhage|uyire|thodu|shona|beloved|tera|meri|humdum|aashiqui|tum|saathiya|baahon|naina|tere|lover|valentine)\b', t):
        return "romantic"

    # 4. Chill / Lofi / Calm / Acoustic / Relaxation
    if re.search(r'\b(chill|lofi|slow|acoustic|unplugged|calm|relax|peace|sleep|breeze|rain|soft|ambient|coffee|meditation|sunset|midnight|night|easy|sweet|sukoon|quiet|cloud|lazy|hawa|raat|dream|chai|breathe|serene)\b', t):
        return "chill"

    # 5. Cinematic / Mass / Trailer / OST / Score
    if re.search(r'\b(theme|ost|score|trailer|intro|symphony|orchestra|epic|interstellar|dune|bgm|mass|interval|teaser|climax|title track|villain|entry|roar|legend|warrior|kingdom|rise|tribute|soundtrack)\b', t):
        return "cinematic"

    # 6. Energetic / Party / Gym / High Tempo / Drill / Banger
    if re.search(r'\b(party|dance|club|remix|workout|gym|beat|blast|drop|jump|edm|electro|hyper|dhol|kuthu|thara local|fire|boost|bass|drill|fast|rock|groove|energy|power|banger|macho|dhamaka|dj|anthem|carnival|bhangra|trap|hardstyle)\b', t):
        return "energetic"

    # 7. Contextual Fallbacks
    if g in ["phonk", "edm", "rock"]:
        return "energetic"
    if g in ["lofi", "acoustic", "ambient"]:
        return "chill"
    if "orchestra" in a:
        return "cinematic"

    return "neutral"



def infer_energy_level(title: str, artist: str = "", genre: str = "", mood: str = "") -> str:
    """
    Infers energy level: 'low' | 'medium' | 'high'
    """
    t = (title or "").lower()
    g = (genre or "").lower()
    m = (mood or infer_mood(title, artist, genre)).lower()

    if m == "energetic" or g in ["phonk", "rock", "edm"] or any(k in t for k in ["remix", "bass", "party", "gym", "fast", "drill", "boost"]):
        return "high"
    if m in ["sad", "chill"] or g in ["lofi", "acoustic", "ambient"] or any(k in t for k in ["acoustic", "unplugged", "slow", "lofi", "piano", "calm"]):
        return "low"
    return "medium"

def infer_vocal_type(title: str, artist: str = "") -> str:
    """
    Infers vocal type: 'instrumental' | 'vocal'
    """
    t = (title or "").lower()
    a = (artist or "").lower()
    if any(k in t for k in [
        "instrumental", "theme", "bgm", "score", "ost", "karaoke", "flute",
        "violin", "guitar", "beats", "orchestra", "without lyrics", "piano version"
    ]) or any(k in a for k in ["instrumental", "soundtrack", "orchestra"]):
        return "instrumental"
    return "vocal"

def infer_song_features(title: str, artist: str = "", genre: str = "") -> dict:
    """Convenience helper to extract mood, energy_level, and vocal_type."""
    m = infer_mood(title, artist, genre)
    e = infer_energy_level(title, artist, genre, mood=m)
    v = infer_vocal_type(title, artist)
    return {
        "mood": m,
        "energy_level": e,
        "vocal_type": v,
    }

# ─── SIMILARITY SCORING & ENERGY ARC ───

def similarity_score(seed: dict, candidate: dict) -> float:
    """
    Score 0.0-1.0 based on:
    - mood match: +0.4
    - energy match: +0.3
    - vocal_type match: +0.2
    - same language: +0.1
    """
    seed_t = seed.get("title") or ""
    seed_a = seed.get("artist") or ""
    seed_g = seed.get("genre") or ""

    cand_t = candidate.get("title") or ""
    cand_a = candidate.get("artist") or ""
    cand_g = candidate.get("genre") or ""

    seed_m = (seed.get("mood") or infer_mood(seed_t, seed_a, seed_g)).strip().lower()
    cand_m = (candidate.get("mood") or infer_mood(cand_t, cand_a, cand_g)).strip().lower()

    seed_e = (seed.get("energy_level") or infer_energy_level(seed_t, seed_a, seed_g, mood=seed_m)).strip().lower()
    cand_e = (candidate.get("energy_level") or infer_energy_level(cand_t, cand_a, cand_g, mood=cand_m)).strip().lower()

    seed_v = (seed.get("vocal_type") or infer_vocal_type(seed_t, seed_a)).strip().lower()
    cand_v = (candidate.get("vocal_type") or infer_vocal_type(cand_t, cand_a)).strip().lower()

    seed_l = (seed.get("language") or seed_g or "").strip().lower()
    cand_l = (candidate.get("language") or cand_g or "").strip().lower()

    score = 0.0
    if seed_m and cand_m and seed_m == cand_m:
        score += 0.4
    if seed_e and cand_e and seed_e == cand_e:
        score += 0.3
    if seed_v and cand_v and seed_v == cand_v:
        score += 0.2
    if seed_l and cand_l and seed_l == cand_l:
        score += 0.1

    # Attach features and score to candidate in-place
    candidate["mood"] = cand_m
    candidate["energy_level"] = cand_e
    candidate["vocal_type"] = cand_v
    candidate["similarity_score"] = round(score, 2)

    return round(score, 2)

def apply_energy_arc(songs: list, limit: int = 15) -> list:
    """
    Authentic YouTube Music Up Next energy arc:
    - Split scored songs into thirds:
        high_sim (top third): very similar to seed
        medium_sim (middle third): related but different
        discovery (bottom third): new territory
    - Arrange:
        Positions 1-4:   high_sim songs
        Positions 5-9:   alternating high + medium
        Positions 10-15: discovery songs
    - Artist diversity:
        First 5 slots (positions 1-5): max 1 song per primary artist
        Slots 6-15: max 2 songs per primary artist
    - Return arranged list capped at limit
    """
    if not songs:
        return []
    if len(songs) <= 2:
        return songs[:limit]

    # Ensure songs are sorted by similarity_score desc
    sorted_songs = sorted(songs, key=lambda s: s.get("similarity_score", 0.0), reverse=True)
    n = len(sorted_songs)

    # Split into thirds
    t1 = max(1, n // 3)
    t2 = max(t1 + 1, 2 * (n // 3)) if n >= 3 else n

    high_sim = list(sorted_songs[:t1])
    medium_sim = list(sorted_songs[t1:t2]) if t2 > t1 else []
    discovery = list(sorted_songs[t2:]) if n > t2 else []

    # Balance pools if catalogue is compact
    if not medium_sim and len(high_sim) > 2:
        sp = len(high_sim) // 2
        medium_sim = high_sim[sp:]
        high_sim = high_sim[:sp]
    if not discovery and len(medium_sim) > 2:
        sp = len(medium_sim) // 2
        discovery = medium_sim[sp:]
        medium_sim = medium_sim[:sp]

    arranged: List[dict] = []
    artist_counts: Dict[str, int] = {}
    used_ids = set()

    def try_pick(pool: list, slot_idx: int) -> Optional[dict]:
        for i, s in enumerate(pool):
            sid = str(s.get("id") or s.get("source_url") or "")
            if sid and sid in used_ids:
                continue
            art = get_primary_artist(s)
            curr = artist_counts.get(art, 0) if art else 0
            if slot_idx < 5:
                # First 5 slots: max 1 song per artist
                if curr >= 1:
                    continue
            else:
                # Slots 6-15: max 2 songs per artist
                if curr >= 2:
                    continue
            pool.pop(i)
            if sid:
                used_ids.add(sid)
            if art:
                artist_counts[art] = curr + 1
            return s
        return None

    def pick_fallback(pools: list, slot_idx: int) -> Optional[dict]:
        # 1. Try strict picking across available pools
        for p in pools:
            s = try_pick(p, slot_idx)
            if s:
                return s
        # 2. Relax slot < 5 constraint if pool is exhausted, but strictly maintain global cap of 2
        for p in pools:
            for i, s in enumerate(p):
                sid = str(s.get("id") or s.get("source_url") or "")
                if sid and sid in used_ids:
                    continue
                art = get_primary_artist(s)
                curr = artist_counts.get(art, 0) if art else 0
                if curr >= 2:
                    continue
                p.pop(i)
                if sid:
                    used_ids.add(sid)
                if art:
                    artist_counts[art] = curr + 1
                return s
        return None

    # Positions 1-4: high_sim songs
    for _ in range(4):
        if len(arranged) >= limit:
            break
        idx = len(arranged)
        s = try_pick(high_sim, idx) or pick_fallback([medium_sim, discovery], idx)
        if s:
            arranged.append(s)

    # Positions 5-9: alternating high + medium
    for i in range(5):
        if len(arranged) >= limit:
            break
        idx = len(arranged)
        primary_pool = high_sim if (i % 2 == 0) else medium_sim
        secondary_pool = medium_sim if (i % 2 == 0) else high_sim
        s = try_pick(primary_pool, idx) or try_pick(secondary_pool, idx) or pick_fallback([discovery, high_sim, medium_sim], idx)
        if s:
            arranged.append(s)

    # Positions 10-15: discovery songs
    for _ in range(6):
        if len(arranged) >= limit:
            break
        idx = len(arranged)
        s = try_pick(discovery, idx) or pick_fallback([medium_sim, high_sim], idx)
        if s:
            arranged.append(s)

    # Backfill remaining if limit not yet satisfied
    while len(arranged) < limit:
        idx = len(arranged)
        s = pick_fallback([high_sim, medium_sim, discovery], idx)
        if not s:
            break
        arranged.append(s)

    return arranged[:limit]

# ─── CATALOG FETCH WITH FEATURE INGESTION ───

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
            
            feats = infer_song_features(title, artist, genre)
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
                "status": "available",
                "mood": feats["mood"],
                "energy_level": feats["energy_level"],
                "vocal_type": feats["vocal_type"],
            }
            try:
                res = client.table("songs").insert(record).execute()
                if res.data:
                    saved = res.data[0]
                    saved.setdefault("mood", feats["mood"])
                    saved.setdefault("energy_level", feats["energy_level"])
                    saved.setdefault("vocal_type", feats["vocal_type"])
                    return saved
            except Exception as ins_err:
                err_str = str(ins_err)
                if "column" in err_str.lower() or "schema cache" in err_str.lower() or "42703" in err_str or "PGRST204" in err_str:
                    fallback_record = {k: v for k, v in record.items() if k not in ("mood", "energy_level", "vocal_type")}
                    try:
                        res = client.table("songs").insert(fallback_record).execute()
                        if res.data:
                            saved = res.data[0]
                            saved["mood"] = feats["mood"]
                            saved["energy_level"] = feats["energy_level"]
                            saved["vocal_type"] = feats["vocal_type"]
                            return saved
                    except Exception as fb_err:
                        ins_err = fb_err
                if "idx_songs_source_url" in str(ins_err) or "23505" in str(ins_err):
                    existing = client.table("songs").select("*").eq("source_url", stream_url).limit(1).execute()
                    if existing.data:
                        found = existing.data[0]
                        found.setdefault("mood", feats["mood"])
                        found.setdefault("energy_level", feats["energy_level"])
                        found.setdefault("vocal_type", feats["vocal_type"])
                        return found
                raise

    except Exception as e:
        print(f"Saavn dynamic fetch notice for '{query}': {e}")
    return None

# ─── CORE QUEUE RECOMMENDATION ENGINE ───

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
    5-Tier Architecture:
      1. YTMusic watch playlist (radio=True) [FIRST]
      2. Saavn native reco engine
      3. Semantic similarity from catalogue
      4. Same artist (recent songs only, last 2 years)
      5. Genre trending (last resort)
    
    Scores all candidates with similarity_score(), sorts by score,
    filters with anti-fatigue & linguistic constraints, and arranges via apply_energy_arc().
    """
    REGIONAL_LANGS = {"hindi", "tamil", "malayalam", "telugu", "kannada", "bengali", "punjabi"}

    seed_id = str(seed_song.get("id", ""))
    seed_title = (seed_song.get("title") or "").strip()
    seed_artist = (seed_song.get("artist") or "").strip()
    seed_genre = (seed_song.get("genre") or "mix").strip().lower()
    seed_lang = (seed_song.get("language") or seed_genre or "mix").strip().lower()
    effective_target = seed_lang if seed_lang in REGIONAL_LANGS else seed_genre

    # Ensure seed song has feature fields populated
    seed_feats = infer_song_features(seed_title, seed_artist, seed_genre)
    seed_song.setdefault("mood", seed_feats["mood"])
    seed_song.setdefault("energy_level", seed_feats["energy_level"])
    seed_song.setdefault("vocal_type", seed_feats["vocal_type"])

    excluded = {str(x).strip() for x in (exclude_ids or []) if str(x).strip()}
    if seed_id:
        excluded.add(seed_id)

    # Session context (Redis) exclusion
    if redis and user_id:
        try:
            session_key = f"beats:session:{user_id}"
            session_played = await redis.lrange(session_key, 0, -1)
            for sp in (session_played or []):
                sp_clean = str(sp).strip()
                if sp_clean:
                    excluded.add(sp_clean)
        except Exception as se_err:
            print(f"Redis session read notice: {se_err}")

    seen_ids = set(excluded)
    norm_seed_base = get_canonical_title(seed_title)
    seen_titles = {norm_seed_base} if norm_seed_base else set()

    # Anti-fatigue: exclude last 60 tracks played by this user from play_events
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

    candidate_pool: List[dict] = []
    pool_seen_keys = set()

    def add_candidate(s: dict):
        if not s:
            return
        cid = str(s.get("id") or s.get("source_url") or "")
        canon = get_canonical_title(s.get("title", ""))
        key = (cid, canon)
        if key in pool_seen_keys or cid in seen_ids or (canon and canon in seen_titles):
            return
        pool_seen_keys.add(key)
        candidate_pool.append(s)

    # ══════════════════════════════════════════════════════════
    # TIER 1: YouTube Music Watch Playlist (radio=True) [FIRST]
    # ══════════════════════════════════════════════════════════
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
                    wp = yt.get_watch_playlist(videoId=vid, radio=True, limit=25)
                    return wp.get("tracks", [])
            except Exception:
                pass
            return []

        tracks = await asyncio.wait_for(asyncio.to_thread(_fetch_yt_playlist), timeout=3.5)
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
                    if dyn:
                        add_candidate(dyn)
                if len(candidate_pool) >= limit * 2:
                    break
    except Exception as yt_err:
        print(f"Tier 1 (YTMusic watch playlist) notice: {yt_err}")

    # ══════════════════════════════════════════════════════════
    # TIER 2: Saavn Native Reco Engine
    # ══════════════════════════════════════════════════════════
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

                    feats = infer_song_features(title, artist, genre)
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
                        "status": "available",
                        "mood": feats["mood"],
                        "energy_level": feats["energy_level"],
                        "vocal_type": feats["vocal_type"],
                    }

                    saved = None
                    try:
                        ins = client.table("songs").insert(record).execute()
                        if ins.data:
                            saved = ins.data[0]
                    except Exception as ins_err:
                        err_str = str(ins_err)
                        if "column" in err_str.lower() or "schema cache" in err_str.lower() or "42703" in err_str or "PGRST204" in err_str:
                            fb_rec = {k: v for k, v in record.items() if k not in ("mood", "energy_level", "vocal_type")}
                            try:
                                ins = client.table("songs").insert(fb_rec).execute()
                                if ins.data:
                                    saved = ins.data[0]
                            except Exception:
                                pass

                        if not saved:
                            try:
                                ex = client.table("songs").select("*").eq("source_url", stream_url).limit(1).execute()
                                if ex.data:
                                    saved = ex.data[0]
                            except Exception:
                                pass

                    if saved:
                        saved.setdefault("mood", feats["mood"])
                        saved.setdefault("energy_level", feats["energy_level"])
                        saved.setdefault("vocal_type", feats["vocal_type"])
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
            print(f"Tier 2 (Saavn native reco) notice: {saavn_err}")

    for s in saavn_candidates:
        add_candidate(s)

    # ══════════════════════════════════════════════════════════
    # TIER 3: Semantic Similarity from Catalogue
    # ══════════════════════════════════════════════════════════
    try:
        cat_q = client.table("songs").select("*").eq("status", "available")
        if effective_target and effective_target != "mix":
            cat_q = cat_q.eq("genre", effective_target)
        cat_res = cat_q.order("play_count", desc=True).limit(60).execute()
        for s in (cat_res.data or []):
            add_candidate(s)
    except Exception as cat_err:
        print(f"Tier 3 (Semantic catalogue) notice: {cat_err}")

    # ══════════════════════════════════════════════════════════
    # TIER 4: Same Artist (Recent songs only, last 2 years)
    # ══════════════════════════════════════════════════════════
    if seed_artist and seed_artist != "Unknown":
        primary_artist = re.split(r"[,&|]|\sfeat\.?|\sft\.?", seed_artist, flags=re.I)[0].strip()
        if len(primary_artist) >= 3:
            try:
                current_year = datetime.now(IST).year
                cutoff_year = current_year - 2

                art_q = client.table("songs")\
                    .select("*")\
                    .eq("status", "available")\
                    .ilike("artist", f"%{primary_artist}%")
                if effective_target and effective_target != "mix":
                    art_q = art_q.eq("genre", effective_target)

                # Prioritize recent songs with year >= cutoff_year
                recent_songs = []
                try:
                    recent_res = art_q.gte("year", cutoff_year).order("play_count", desc=True).limit(20).execute()
                    recent_songs = recent_res.data or []
                except Exception:
                    recent_songs = []

                # Fallback to most recently added if year column is sparse
                if not recent_songs:
                    fallback_res = art_q.order("scraped_at", desc=True).limit(20).execute()
                    recent_songs = fallback_res.data or []

                for s in recent_songs:
                    add_candidate(s)
            except Exception as art_err:
                print(f"Tier 4 (Same artist recent) notice: {art_err}")

    # ══════════════════════════════════════════════════════════
    # TIER 5: Genre Trending (Last resort)
    # ══════════════════════════════════════════════════════════
    try:
        pop_q = client.table("songs").select("*").eq("status", "available")
        if effective_target and effective_target != "mix":
            pop_q = pop_q.eq("genre", effective_target)
        pop_res = pop_q.order("play_count", desc=True).limit(50).execute()
        for s in (pop_res.data or []):
            add_candidate(s)
    except Exception as pop_err:
        print(f"Tier 5 (Trending backfill) notice: {pop_err}")

    # ─── SCORING, SORTING & CAN_ADD_SONG FILTERING ───

    # 1. Score every candidate before filtering and store "similarity_score"
    for cand in candidate_pool:
        similarity_score(seed_song, cand)

    # 2. Sort candidates by score descending before can_add_song() filter
    candidate_pool.sort(key=lambda x: x.get("similarity_score", 0.0), reverse=True)

    # 3. Filter candidates
    artist_pool_counts: Dict[str, int] = {}

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

        # Strict linguistic integrity
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

        # Total maximum 2 tracks per primary artist in the candidate pool
        art = get_primary_artist(s)
        if art and artist_pool_counts.get(art, 0) >= 2:
            return False
        return True

    qualified_songs: List[dict] = []
    for s in candidate_pool:
        if can_add_song(s):
            sid = str(s.get("id", ""))
            canon = get_canonical_title(s.get("title", ""))
            seen_ids.add(sid)
            if canon:
                seen_titles.add(canon)
            art = get_primary_artist(s)
            if art:
                artist_pool_counts[art] = artist_pool_counts.get(art, 0) + 1
            qualified_songs.append(s)

    # ─── ENERGY ARC ARRANGEMENT & ARTIST DIVERSITY ───
    final_queue = apply_energy_arc(qualified_songs, limit=limit)
    return final_queue
