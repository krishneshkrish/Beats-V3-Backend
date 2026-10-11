import re
import os
import sys
import json
import base64
import html
import uuid
import httpx
from datetime import datetime
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager
from Crypto.Cipher import DES

# Configure UTF-8 encoding for safe output on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import redis.asyncio as aioredis
from supabase import create_client, Client
from dotenv import load_dotenv
import pytz

load_dotenv()

IST = pytz.timezone("Asia/Kolkata")

# masstamilan network request headers
STREAM_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 "
        "like Mac OS X) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/17.0 "
        "Mobile/15E148 Safari/604.1"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.masstamilan.dev/",
    "Connection": "keep-alive",
}

# Supabase Client Initialization
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
supabase: Optional[Client] = (
    create_client(SUPABASE_URL, SUPABASE_KEY)
    if SUPABASE_URL and SUPABASE_KEY
    else None
)


def get_supabase_client() -> Client:
    """Helper to retrieve active Supabase client or raise informative error."""
    global supabase
    if supabase is not None:
        return supabase
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise HTTPException(
            status_code=500,
            detail="Supabase credentials (SUPABASE_URL / SUPABASE_KEY) not configured in .env",
        )
    supabase = create_client(url, key)
    return supabase


async def warmup_catalog_cache(app: FastAPI):
    """
    Preloads top catalog items into Redis on startup so the frontend
    loads songs instantly without cold-start delays.
    """
    import asyncio
    await asyncio.sleep(0.5)
    r = getattr(app.state, "redis", None)
    if not r:
        return
    client = get_supabase_client()
    if not client:
        return

    try:
        # 1. Top global songs
        res_global = client.table("songs")\
            .select("id, title, artist, film, genre, duration, thumbnail_url, play_count")\
            .eq("status", "available")\
            .order("play_count", desc=True)\
            .limit(20)\
            .execute()
        if res_global.data:
            await r.set("beats:songs:all:20", json.dumps({"songs": res_global.data}), ex=600)
            await r.set("beats:trending:all", json.dumps({"trending": res_global.data}), ex=600)

        # 2. Top songs per genre
        for g in ["tamil", "malayalam", "hindi", "english", "mix"]:
            res_genre = client.table("songs")\
                .select("id, title, artist, film, genre, duration, thumbnail_url, play_count")\
                .eq("status", "available")\
                .eq("genre", g)\
                .order("play_count", desc=True)\
                .limit(20)\
                .execute()
            if res_genre.data:
                await r.set(f"beats:songs:{g}:20", json.dumps({"songs": res_genre.data}), ex=600)
                await r.set(f"beats:trending:{g}", json.dumps({"trending": res_genre.data}), ex=600)

        # 3. Light catalog for instant search/fuzzy matching
        res_cat = client.table("songs")\
            .select("id, title, artist, film, genre, duration, thumbnail_url, play_count, status")\
            .eq("status", "available")\
            .order("play_count", desc=True)\
            .limit(400)\
            .execute()
        if res_cat.data:
            await r.set("beats:catalog:light", json.dumps(res_cat.data), ex=600)

        print("[OK] Catalog cache warmup complete: Zero-delay startup active")
    except Exception as e:
        print(f"[!] Warmup notice: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: connect Redis
    redis_url = os.environ.get("REDIS_URL")
    if redis_url:
        try:
            app.state.redis = await aioredis.from_url(
                redis_url,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
            )
            print("[OK] Redis connected")
        except Exception as e:
            print(f"[!] Redis connection warning: {e}")
            app.state.redis = None
    else:
        print("[!] REDIS_URL not configured")
        app.state.redis = None

    # Zero-delay cache warmup in background
    import asyncio
    asyncio.create_task(warmup_catalog_cache(app))

    yield

    # Shutdown: close Redis
    if getattr(app.state, "redis", None):
        await app.state.redis.aclose()
        print("[OK] Redis disconnected")


app = FastAPI(
    title="Beats API",
    version="2.0.0",
    lifespan=lifespan,
)

_cors_origins_env = os.environ.get("ALLOWED_ORIGINS", "*")
_cors_origins = (
    [origin.strip() for origin in _cors_origins_env.split(",") if origin.strip()]
    if _cors_origins_env and _cors_origins_env != "*"
    else ["*"]
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_origin_regex=r"^https:\/\/.*\.vercel\.app$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def get_redis(request: Request):
    """Retrieve Redis instance or raise 503 if unavailable."""
    r = getattr(request.app.state, "redis", None)
    if not r:
        raise HTTPException(
            status_code=503,
            detail="Redis service unavailable. Verify REDIS_URL.",
        )
    return r


class PlayEvent(BaseModel):
    user_id: str
    song_id: str
    genre: str
    completed: bool = False
    skipped_at_pct: Optional[float] = None
    source: str = "browse"


class UserCreate(BaseModel):
    user_id: str
    name: str


# ─── 1. HEALTH CHECK ───
@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "beats-api",
        "version": "2.0.0",
        "timestamp": datetime.now(IST).isoformat(),
    }


from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup


async def refresh_song_source_url(song: dict, client: Client) -> Optional[str]:
    """Auto-fetches fresh download URL when tokenized link expires (410 Gone) and resolves permanent CDN URL."""
    thumb = song.get("thumbnail_url") or ""
    m = re.search(r"/(?:i|cover|thumb)/(.+?)\.(?:jpg|jpeg|png|webp)", thumb)
    if not m:
        return None
    slug = m.group(1)
    parsed = urlparse(thumb)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    movie_url = f"{base_url}/{slug}-songs"

    try:
        async with httpx.AsyncClient(headers=STREAM_HEADERS, timeout=12.0, follow_redirects=True) as hclient:
            mr = await hclient.get(movie_url)
            if mr.status_code != 200:
                return None
            soup = BeautifulSoup(mr.text, "html.parser")
            song_title = (song.get("title") or "").strip().lower()
            for tr in soup.find_all("tr"):
                name_el = tr.find(attrs={"itemprop": "name"}) or tr.find("a", href=lambda h: h and "-mp3-song" in h)
                if name_el and song_title in name_el.text.lower():
                    dl_a = tr.find("a", href=lambda h: h and ("d128" in h or "128" in str(h)))
                    if dl_a and dl_a.get("href"):
                        token_url = urljoin(base_url, dl_a["href"])
                        final_mp3_url = token_url
                        try:
                            dl_resp = await hclient.get(token_url)
                            if dl_resp.status_code == 200 and any(ext in str(dl_resp.url) for ext in [".mp3", "masstamilan.download", "cdn"]):
                                final_mp3_url = str(dl_resp.url)
                        except Exception:
                            pass

                        try:
                            client.table("songs").update({"source_url": final_mp3_url}).eq("id", song["id"]).execute()
                        except Exception:
                            pass
                        return final_mp3_url
    except Exception as err:
        print(f"Error auto-refreshing source URL for {song.get('title')}: {err}")
    return None


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

def is_valid_uuid(val: str) -> bool:
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
        from urllib.parse import quote
        q_clean = query.strip()
        url = f"https://www.jiosaavn.com/api.php?__call=search.getResults&_format=json&_marker=0&n=1&p=1&q={quote(q_clean)}"
        async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}, timeout=7.0) as hclient:
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
            genre = lang.lower() if lang.lower() in ["tamil", "hindi", "english", "malayalam"] else "mix"
            
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
            res = client.table("songs").insert(record).execute()
            if res.data:
                return res.data[0]
    except Exception as e:
        print(f"Saavn dynamic fetch notice for '{query}': {e}")
    return None

# ─── 2. RESOLVE STREAM URL ───
@app.get("/api/resolve/{song_id}")
async def resolve_stream(song_id: str, request: Request):
    """
    Returns a playable audio stream URL for a song.
    Handles UUIDs, legacy/template IDs, and dynamic catalog resolution.
    """
    r = getattr(request.app.state, "redis", None)
    client = get_supabase_client()

    # 0. Check template ID mapping
    original_id = song_id
    song_id = TEMPLATE_SONG_MAP.get(song_id, song_id)

    # 1. Redis cache check
    cache_key = f"beats:resolve:{song_id}"
    cached = await r.get(cache_key) if r else None
    if cached and "410" not in cached and any(ext in cached for ext in [".mp3", ".m4a", ".mp4", "masstamilan.download", "saavncdn", "audio"]):
        meta_key = f"beats:song:{song_id}"
        meta = await r.hgetall(meta_key) if r else {}
        try:
            dur_int = int((meta or {}).get("duration", 0) or 0)
        except (ValueError, TypeError):
            dur_int = 0

        playable_stream = cached if "saavncdn.com" in cached else f"{str(request.base_url).rstrip('/')}/api/stream/{song_id}"
        return {
            "song_id": song_id,
            "stream_url": playable_stream,
            "title": (meta or {}).get("title", ""),
            "artist": (meta or {}).get("artist", ""),
            "thumbnail_url": (meta or {}).get("thumbnail_url", ""),
            "genre": (meta or {}).get("genre", ""),
            "duration": dur_int,
            "cached": True,
        }

    # 2. Fetch from Supabase or resolve dynamically
    song = None
    if is_valid_uuid(song_id):
        try:
            res = client.table("songs")\
                .select("id, title, artist, film, genre, duration, thumbnail_url, source_url, status")\
                .eq("id", song_id)\
                .execute()
            if res.data:
                song = res.data[0]
        except Exception as e:
            print(f"Supabase lookup error for {song_id}: {e}")
    else:
        # Non-UUID string or search keyword: lookup by title
        clean_q = song_id.replace("song-", "").replace("-", " ").strip()
        try:
            res = client.table("songs")\
                .select("id, title, artist, film, genre, duration, thumbnail_url, source_url, status")\
                .ilike("title", f"%{clean_q}%")\
                .limit(1)\
                .execute()
            if res.data:
                song = res.data[0]
        except Exception:
            pass

    # If still not found, dynamically search and resolve via Saavn
    if not song:
        lookup_term = original_id.replace("song-", "").replace("-", " ").strip()
        song = await fetch_saavn_song_on_the_fly(lookup_term, client)

    if not song or not song.get("source_url"):
        raise HTTPException(404, "Original song stream could not be resolved")

    if song.get("status") == "unavailable":
        raise HTTPException(404, "This song is not available")

    # 3. Determine real stream URL
    source_url = song["source_url"]
    stream_url = None
    real_id = song.get("id") or song_id

    if any(k in source_url for k in ["saavncdn.com", "masstamilan.download", ".mp3", ".mp4", ".m4a"]):
        stream_url = source_url
    else:
        # Follow redirects for legacy Masstamilan / mp3chetta links
        try:
            async with httpx.AsyncClient(
                headers=STREAM_HEADERS,
                follow_redirects=True,
                timeout=15.0,
            ) as client_http:
                response = await client_http.get(source_url)
                if response.status_code >= 400 or "410" in str(response.url):
                    fresh_url = await refresh_song_source_url(song, client)
                    if fresh_url:
                        source_url = fresh_url
                        response = await client_http.get(source_url)
                
                final_url = str(response.url)
                if any(ext in final_url for ext in [".mp3", ".m4a", ".mp4", "masstamilan.download", "saavncdn", "cdn"]):
                    stream_url = final_url
                    if "masstamilan.download" in stream_url:
                        try:
                            client.table("songs").update({"source_url": stream_url}).eq("id", real_id).execute()
                        except Exception:
                            pass
                else:
                    fresh_url = await refresh_song_source_url(song, client)
                    if fresh_url:
                        stream_url = fresh_url
        except Exception as e:
            print(f"Resolve stream error for {song.get('title')}: {e}")
            fresh_url = await refresh_song_source_url(song, client)
            stream_url = fresh_url or source_url

    # 4. Cache in Redis
    if r and stream_url and "410" not in stream_url:
        await r.set(f"beats:resolve:{real_id}", stream_url, ex=72000)
        if original_id != real_id:
            await r.set(f"beats:resolve:{original_id}", stream_url, ex=72000)

        meta_key = f"beats:song:{real_id}"
        await r.hset(
            meta_key,
            mapping={
                "title": song.get("title") or "",
                "artist": song.get("artist") or "",
                "thumbnail_url": song.get("thumbnail_url") or "",
                "genre": song.get("genre") or "",
                "duration": str(song.get("duration") or 0),
            },
        )
        await r.expire(meta_key, 86400)

    # 5. Return
    playable_stream = stream_url if "saavncdn.com" in stream_url else f"{str(request.base_url).rstrip('/')}/api/stream/{real_id}"
    return {
        "song_id": real_id,
        "stream_url": playable_stream,
        "title": song.get("title"),
        "artist": song.get("artist"),
        "thumbnail_url": song.get("thumbnail_url"),
        "genre": song.get("genre"),
        "duration": song.get("duration"),
        "cached": False,
    }


# ─── 3. INVALIDATE CACHED STREAM URL ───
@app.post("/api/invalidate/{song_id}")
async def invalidate_stream(song_id: str, request: Request):
    """
    Called by frontend when Audio() throws an error.
    Clears expired cached URL so next resolve() fetches a fresh one.
    """
    r = getattr(request.app.state, "redis", None)
    if r:
        try:
            await r.delete(f"beats:resolve:{song_id}")
        except Exception:
            pass
    return {"status": "invalidated", "song_id": song_id}


def normalize_phonetic(text: str) -> str:
    """Normalize text for phonetic / transliteration comparison."""
    if not text:
        return ""
    t = text.lower()
    t = re.sub(r"[^a-z0-9]", "", t)
    # Common South Asian / English transliteration equivalencies
    t = (
        t.replace("sh", "s")
        .replace("th", "t")
        .replace("dh", "d")
        .replace("zh", "l")
        .replace("ee", "i")
        .replace("oo", "u")
        .replace("aa", "a")
    )
    return t


def generate_search_variants(q: str) -> list:
    """Generate common romanized spelling variants (e.g., amsam <-> amsham, ghilli <-> gilli)."""
    raw = q.strip().lower()
    variants = [raw]

    # gh <-> g (e.g. ghilli <-> gilli, raghu <-> ragu)
    if "gh" in raw:
        variants.append(raw.replace("gh", "g"))
    elif "g" in raw:
        variants.append(raw.replace("g", "gh"))

    # kh <-> k
    if "kh" in raw:
        variants.append(raw.replace("kh", "k"))

    # bh <-> b
    if "bh" in raw:
        variants.append(raw.replace("bh", "b"))

    # ph <-> p / f
    if "ph" in raw:
        variants.append(raw.replace("ph", "p"))
        variants.append(raw.replace("ph", "f"))

    # s <-> sh (e.g. amsam <-> amsham, siva <-> shiva)
    if "sh" in raw:
        variants.append(raw.replace("sh", "s"))
    elif "s" in raw:
        variants.append(raw.replace("s", "sh"))

    # th <-> t (e.g. karthik <-> kartik)
    if "th" in raw:
        variants.append(raw.replace("th", "t"))
    elif "t" in raw:
        variants.append(raw.replace("t", "th"))

    # dh <-> d (e.g. anirudh <-> anirud)
    if "dh" in raw:
        variants.append(raw.replace("dh", "d"))
    elif "d" in raw:
        variants.append(raw.replace("d", "dh"))

    # zh <-> l (e.g. tamizh <-> tamil)
    if "zh" in raw:
        variants.append(raw.replace("zh", "l"))

    # Double vowels
    if any(dv in raw for dv in ["aa", "ee", "oo"]):
        variants.append(
            raw.replace("aa", "a").replace("ee", "i").replace("oo", "u")
        )

    # Deduplicate while preserving order
    seen = set()
    ordered = []
    for v in variants:
        v_clean = v.strip()
        if v_clean and v_clean not in seen and len(v_clean) >= 2:
            seen.add(v_clean)
            ordered.append(v_clean)
    return ordered


async def fetch_dynamic_search_songs(query: str, client: Client, limit: int = 10) -> List[dict]:
    """
    On-demand search ingestion: when a user searches for a movie (e.g. 'Ghilli', 'Ghilli tamil',
    'Aavesham', 'Premam', etc.) or artist that is not yet fully in catalog,
    dynamically fetch the songs from CDN, persist to Supabase, and return them immediately.
    """
    try:
        from urllib.parse import quote
        q_clean = query.strip()
        url = f"https://www.jiosaavn.com/api.php?__call=search.getResults&_format=json&_marker=0&n={limit}&p=1&q={quote(q_clean)}"
        async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}, timeout=6.0) as hclient:
            r = await hclient.get(url)
            if r.status_code != 200:
                return []
            data = json.loads(r.text.strip())
            results = data.get("results", [])
            if not results:
                return []
            
            records = []
            for item in results:
                enc = item.get("encrypted_media_url")
                if not enc:
                    continue
                stream_url = decrypt_saavn_url(enc)
                if not stream_url:
                    continue
                raw_title = html.unescape(item.get("song") or item.get("title") or "")
                title = raw_title.replace("&quot;", '"').replace("&#039;", "'").strip()
                raw_artist = html.unescape(item.get("singers") or item.get("primary_artists") or "Various")
                artist = raw_artist.replace("&quot;", '"').replace("&#039;", "'").strip()
                raw_film = html.unescape(item.get("album") or "")
                film = raw_film.replace("&quot;", '"').replace("&#039;", "'").strip() or "Single"
                thumb = (item.get("image") or "").replace("150x150", "500x500")
                lang = item.get("language") or "Tamil"
                lang_lower = lang.lower()
                genre = lang_lower if lang_lower in ["tamil", "hindi", "english", "malayalam", "phonk"] else "mix"
                dur = int(item.get("duration") or 180)

                rec = {
                    "title": title,
                    "artist": artist,
                    "film": film,
                    "genre": genre,
                    "language": lang,
                    "duration": dur,
                    "thumbnail_url": thumb,
                    "source_url": stream_url,
                    "source_site": "jiosaavn_cdn",
                    "play_count": 6000,
                    "status": "available",
                }
                records.append(rec)

            if records:
                try:
                    res = client.table("songs").upsert(records, on_conflict="source_url").execute()
                    if res.data:
                        return res.data
                except Exception as ins_err:
                    print(f"Dynamic search upsert error: {ins_err}")
    except Exception as e:
        print(f"Dynamic search notice: {e}")
    return []


# ─── 4. SEARCH SONGS ───
@app.get("/api/search")
async def search_songs(
    q: str,
    limit: int = 20,
    request: Request = None,
):
    """
    Search songs by title, artist, film name, or genre.
    Features:
    - Transliteration / spelling variants (e.g. 'Amsam' matches 'Amsham')
    - Multi-word matching across fields
    - Phonetic and fuzzy fallback matching
    - Redis cached for 5 minutes
    """
    if not q or len(q.strip()) < 2:
        return {"songs": [], "count": 0}

    r = None
    try:
        r = await get_redis(request)
    except Exception:
        r = None

    client = get_supabase_client()
    clean_q = re.sub(r'[,()"]', "", q).strip().lower()

    # 1. Cache check (only return if non-empty results were cached)
    cache_key = f"beats:search:{clean_q}:{limit}"
    if r:
        try:
            cached = await r.get(cache_key)
            if cached:
                cached_data = json.loads(cached)
                if cached_data.get("count", 0) > 0:
                    return cached_data
        except Exception:
            pass

    # 2. Generate transliteration / spelling variants
    variants = generate_search_variants(clean_q)
    songs_data = []
    seen_ids = set()

    # 2.5 Ensure direct title matches are fetched first so they are never crowded out
    try:
        title_res = client.table("songs")\
            .select("id, title, artist, film, genre, duration, thumbnail_url, play_count, status")\
            .eq("status", "available")\
            .ilike("title", f"%{clean_q}%")\
            .order("play_count", desc=True)\
            .limit(50)\
            .execute()
        for s in (title_res.data or []):
            if s["id"] not in seen_ids:
                seen_ids.add(s["id"])
                songs_data.append(s)
    except Exception as e:
        print(f"Title pre-search notice: {e}")

    # 3. Fast database OR query matching all variants with expanded candidate pool
    or_clauses = []
    for v in variants[:4]:
        or_clauses.extend([
            f"title.ilike.%{v}%",
            f"artist.ilike.%{v}%",
            f"film.ilike.%{v}%",
            f"genre.ilike.%{v}%",
        ])
    or_filter = ",".join(or_clauses)
    candidate_limit = max(limit * 3, 50)

    try:
        res = client.table("songs")\
            .select(
                "id, title, artist, film, genre, "
                "duration, thumbnail_url, play_count, status"
            )\
            .eq("status", "available")\
            .or_(or_filter)\
            .order("play_count", desc=True)\
            .limit(candidate_limit)\
            .execute()
        for s in (res.data or []):
            if s["id"] not in seen_ids:
                seen_ids.add(s["id"])
                songs_data.append(s)
    except Exception as e:
        print(f"Search query error: {e}")

    # 4. Multi-word & movie/genre matching
    words = [w for w in clean_q.split() if len(w) >= 2]
    if len(words) > 1 and len(songs_data) < limit:
        try:
            genres_set = {"tamil", "malayalam", "hindi", "english", "mix", "telugu"}
            q_genres = [w for w in words if w in genres_set]
            q_terms = [w for w in words if w not in genres_set] or words

            word_clauses = []
            for term in q_terms:
                term_variants = generate_search_variants(term)
                for tv in term_variants[:4]:
                    word_clauses.extend([
                        f"film.ilike.%{tv}%",
                        f"title.ilike.%{tv}%",
                        f"artist.ilike.%{tv}%",
                    ])
            or_filter_words = ",".join(word_clauses)

            query_words = client.table("songs")\
                .select("id, title, artist, film, genre, duration, thumbnail_url, play_count, status")\
                .eq("status", "available")\
                .or_(or_filter_words)\
                .limit(50)

            if q_genres:
                query_words = query_words.eq("genre", q_genres[0])

            res_words = query_words.execute()
            for s in (res_words.data or []):
                if s["id"] not in seen_ids:
                    seen_ids.add(s["id"])
                    songs_data.append(s)
        except Exception as e:
            print(f"Multi-word query error: {e}")

    # 5. Fuzzy & Phonetic fallback matching (for typos or subtle spelling variations)
    if len(songs_data) < limit:
        try:
            import difflib

            # Try to get catalog from Redis or Supabase
            catalog = []
            if r:
                try:
                    cat_cached = await r.get("beats:catalog:light")
                    if cat_cached:
                        catalog = json.loads(cat_cached)
                except Exception:
                    pass

            if not catalog:
                all_res = client.table("songs")\
                    .select("id, title, artist, film, genre, duration, thumbnail_url, play_count, status")\
                    .eq("status", "available")\
                    .execute()
                catalog = all_res.data or []
                if r and catalog:
                    try:
                        await r.set("beats:catalog:light", json.dumps(catalog), ex=600)
                    except Exception:
                        pass

            norm_q = normalize_phonetic(clean_q)
            scored = []
            for s in catalog:
                if s["id"] in seen_ids:
                    continue
                title = s.get("title") or ""
                artist = s.get("artist") or ""
                film = s.get("film") or ""

                norm_title = normalize_phonetic(title)
                norm_film = normalize_phonetic(film)
                norm_artist = normalize_phonetic(artist)

                score = 0.0
                if norm_q and (norm_q in norm_title or norm_q in norm_film or norm_q in norm_artist):
                    score = 0.90
                else:
                    s_title = difflib.SequenceMatcher(None, clean_q, title.lower()).ratio()
                    s_film = difflib.SequenceMatcher(None, clean_q, film.lower()).ratio()
                    s_artist = difflib.SequenceMatcher(None, clean_q, artist.lower()).ratio()
                    score = max(s_title, s_film, s_artist)

                if score >= 0.65:
                    scored.append((score, s))

            scored.sort(key=lambda x: (x[0], x[1].get("play_count", 0)), reverse=True)
            for _, s in scored[: limit - len(songs_data)]:
                seen_ids.add(s["id"])
                songs_data.append(s)
        except Exception as e:
            print(f"Fuzzy matching error: {e}")

    # 6. Dynamic on-the-fly search fetch if local catalog has limited results (< 3) OR lacks title match
    has_title_match = any(clean_q in (s.get("title") or "").lower() for s in songs_data)
    if len(songs_data) < 3 or not has_title_match:
        try:
            dynamic_songs = await fetch_dynamic_search_songs(clean_q, client, limit=limit)
            for s in dynamic_songs:
                if s["id"] not in seen_ids:
                    seen_ids.add(s["id"])
                    songs_data.append(s)
        except Exception as e:
            print(f"Dynamic fetch error in search: {e}")

    # 7. Relevance-first ranking: Exact title -> Title prefix -> Title substring -> Artist -> Film -> Popularity
    def compute_relevance(song: dict):
        title = (song.get("title") or "").strip().lower()
        film = (song.get("film") or "").strip().lower()
        artist = (song.get("artist") or "").strip().lower()
        p_count = song.get("play_count") or 0

        clean_t = re.sub(r'[,()"\-:]', '', title).strip()
        clean_search = re.sub(r'[,()"\-:]', '', clean_q).strip()

        score = 0
        if title == clean_q or clean_t == clean_search:
            score = 1000
        elif title.startswith(clean_q) or clean_t.startswith(clean_search):
            score = 900
        elif clean_q in title or clean_search in clean_t:
            score = 800
        elif artist == clean_q or artist.startswith(clean_q):
            score = 700
        elif clean_q in artist:
            score = 600
        elif film == clean_q or film.startswith(clean_q):
            score = 500
        elif clean_q in film:
            score = 400
        else:
            score = 100

        return (score, p_count)

    songs_data.sort(key=compute_relevance, reverse=True)

    result = {
        "songs": songs_data[:limit],
        "count": len(songs_data[:limit]),
    }

    # Increment search count for top matching songs
    if songs_data:
        top_match = songs_data[0]
        top_id = top_match.get("id")
        if is_valid_uuid(top_id):
            try:
                client.rpc("increment_search_count", {"p_song_id": top_id}).execute()
            except Exception:
                pass

    # 6. Cache in Redis (5 mins if results found; 15s if empty)
    if r:
        try:
            cache_ttl = 300 if len(songs_data) > 0 else 15
            await r.set(cache_key, json.dumps(result), ex=cache_ttl)
        except Exception:
            pass

    return result


# ─── 5. BROWSE SONGS BY GENRE ───
@app.get("/api/songs")
async def get_songs(
    genre: Optional[str] = None,
    limit: int = 20,
    request: Request = None,
):
    """
    Get songs, optionally filtered by genre.
    Redis cached for 5 minutes.
    """
    r = getattr(request.app.state, "redis", None) if request else None
    client = get_supabase_client()

    cache_key = f"beats:songs:{genre or 'all'}:{limit}"
    if r:
        try:
            cached = await r.get(cache_key)
            if cached:
                return json.loads(cached)
        except Exception:
            pass

    query_builder = client.table("songs")\
        .select(
            "id, title, artist, film, genre, "
            "duration, thumbnail_url, play_count"
        )\
        .eq("status", "available")\
        .order("play_count", desc=True)\
        .limit(limit)

    if genre and genre != "all":
        query_builder = query_builder.eq("genre", genre)

    res = query_builder.execute()
    result = {"songs": res.data or []}

    if r:
        try:
            await r.set(cache_key, json.dumps(result), ex=300)
        except Exception:
            pass
    return result


# ─── 5B. ON-DEMAND ARTIST SCRAPER ───
@app.post("/api/scraper/artist")
async def trigger_artist_scrape(
    artist: str,
    limit: int = 15,
    request: Request = None,
):
    """
    On-demand scraper endpoint to ingest independent artists, bands, or rappers.
    """
    from scraper import scrape_artist_songs
    client = get_supabase_client()
    songs = scrape_artist_songs(artist, limit=limit, supabase=client)
    r = getattr(request.app.state, "redis", None) if request else None
    if r and songs:
        try:
            await r.delete("beats:catalog:light")
            await r.delete("beats:songs:all:20")
            await r.delete("beats:trending:all")
        except Exception:
            pass
    return {
        "status": "ok",
        "artist": artist,
        "songs_count": len(songs),
        "songs": songs,
    }


# ─── 6. RECORD PLAY EVENT ───
@app.post("/api/play-event")
async def record_play_event(
    event: PlayEvent,
    request: Request,
):
    """
    Records a play event for ML recommendations.
    Updates Redis hot counters.
    Fire and forget from frontend.
    """
    r = getattr(request.app.state, "redis", None) if request else None
    client = get_supabase_client()
    now = datetime.now(IST)
    actual_song_id = TEMPLATE_SONG_MAP.get(event.song_id, event.song_id)

    # 1. Insert play event and increment song play count (requires valid UUID)
    if is_valid_uuid(actual_song_id):
        try:
            client.table("play_events").insert({
                "user_id": event.user_id,
                "song_id": actual_song_id,
                "genre": event.genre,
                "completed": event.completed,
                "skipped_at_pct": event.skipped_at_pct,
                "source": event.source,
                "hour_of_day": now.hour,
                "day_of_week": now.weekday(),
                "played_at": now.isoformat(),
            }).execute()
        except Exception as e:
            print(f"Play event insert error: {e}")

        try:
            client.rpc(
                "increment_play_count",
                {"p_song_id": actual_song_id},
            ).execute()
        except Exception as e:
            print(f"Play count increment error: {e}")

    # 2. Redis hot counters (pipeline = one round trip)
    if r:
        try:
            async with r.pipeline(transaction=False) as pipe:
                pipe.zincrby("beats:hot:global", 1, actual_song_id)
                pipe.zincrby(
                    f"beats:hot:genre:{event.genre}",
                    1,
                    actual_song_id,
                )
                await pipe.execute()
        except Exception as r_err:
            print(f"Redis hot counter notice: {r_err}")

    # 3. Upsert user and increment user's total play count (activates ML recommendation tier)
    try:
        u_check = client.table("users").select("play_count").eq("id", event.user_id).limit(1).execute()
        current_plays = (u_check.data[0].get("play_count") or 0) if (u_check.data and len(u_check.data) > 0) else 0
        client.table("users").upsert({
            "id": event.user_id,
            "play_count": current_plays + 1,
            "last_seen": now.isoformat(),
        }, on_conflict="id").execute()
    except Exception as e:
        print(f"User upsert error: {e}")

    return {"status": "recorded"}


# ─── 7. RECOMMENDATIONS ───
@app.get("/api/recommendations")
async def get_recommendations(
    user_id: str,
    limit: int = 20,
    request: Request = None,
):
    """
    Returns personalised song recommendations based on time-of-day listening habits.
    Rule-based for new users (<50 plays).
    LightGBM for returning users (50+ plays).
    Redis cached per user per time block.
    """
    r = getattr(request.app.state, "redis", None) if request else None
    client = get_supabase_client()
    now = datetime.now(IST)

    from recommender import get_time_block
    time_block = get_time_block(now.hour)

    # Cache check (per user per time block & hour)
    cache_key = f"beats:recs:{user_id}:{time_block['key']}:{now.hour}"
    if r:
        try:
            cached = await r.get(cache_key)
            if cached:
                data = json.loads(cached)
                data["cached"] = True
                return data
        except Exception:
            pass

    # Get user play count
    user_res = client.table("users")\
        .select("play_count")\
        .eq("id", user_id)\
        .execute()

    play_count = 0
    if user_res.data:
        play_count = user_res.data[0].get("play_count", 0) or 0

    # Route to correct strategy: ML if >= 50 plays, else rule-based
    songs = []
    strategy = "rule_based"
    if play_count >= 50 and r:
        try:
            from recommender import ml_recs
            songs = await ml_recs(user_id, limit, client, r)
            if songs:
                strategy = "ml"
        except Exception as ml_err:
            print(f"ML recs notice for {user_id}: {ml_err}")

    if not songs:
        from recommender import rule_based_recs
        songs = await rule_based_recs(user_id, limit, client, r, now)
        strategy = "rule_based"

    result = {
        "songs": songs,
        "strategy": strategy,
        "cached": False,
        "time_block": time_block["key"],
        "block_name": time_block["name"],
        "block_tag": time_block["tag"],
        "greeting": time_block["greeting"],
        "block_description": time_block["description"],
    }

    # Cache 30 minutes
    if r:
        try:
            await r.set(cache_key, json.dumps(result), ex=1800)
        except Exception:
            pass
    return result


@app.get("/api/radio/{song_id}")
async def get_radio(
    song_id: str,
    user_id: str = "guest",
    limit: int = 15,
    exclude: str = "",
    request: Request = None,
):
    """
    Automated seed-based radio endpoint (YouTube Music style Up Next).
    Generates a continuous, contextual queue based on seed song similarity,
    artist matching, and time-of-day atmosphere without requiring user intervention.
    """
    r = getattr(request.app.state, "redis", None)
    client = get_supabase_client()
    now = datetime.now(IST)

    from recommender import radio_recs, get_time_block
    time_block = get_time_block(now.hour)

    actual_song_id = TEMPLATE_SONG_MAP.get(song_id, song_id)
    exclude_ids = [s.strip() for s in exclude.split(",") if s.strip()] if exclude else []

    try:
        songs = await radio_recs(
            seed_song_id=actual_song_id,
            user_id=user_id,
            limit=limit,
            supabase=client,
            redis=r,
            exclude_ids=exclude_ids,
            now=now
        )
    except Exception as e:
        print(f"Radio generation error: {e}")
        from recommender import rule_based_recs
        songs = await rule_based_recs(user_id, limit, client, r, now)

    return {
        "seed_id": song_id,
        "songs": songs,
        "time_block": time_block["key"],
        "block_name": time_block["name"],
    }


# ─── 8. USER MANAGEMENT ───
@app.post("/api/user")
async def create_user(user: UserCreate):
    client = get_supabase_client()
    client.table("users").upsert({
        "id": user.user_id,
        "name": user.name,
        "last_seen": datetime.now(IST).isoformat(),
    }, on_conflict="id").execute()
    return {"status": "ok", "user_id": user.user_id}


@app.get("/api/user/{user_id}")
async def get_user(user_id: str):
    client = get_supabase_client()
    try:
        res = client.table("users")\
            .select("*")\
            .eq("id", user_id)\
            .limit(1)\
            .execute()
        if not res.data:
            raise HTTPException(404, "User not found")
        return res.data[0]
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(404, "User not found")


@app.get("/api/user/{user_id}/stats")
async def get_user_stats(user_id: str):
    """
    Returns play event data for dashboard:
    total_songs, total_hours, top_genres, top_artists, heatmap
    """
    client = get_supabase_client()
    from datetime import timedelta
    cutoff = (datetime.now(IST) - timedelta(days=30)).isoformat()
    try:
        res = client.table("play_events")\
            .select("id, song_id, played_at, hour_of_day, day_of_week, genre, songs(duration, artist)")\
            .eq("user_id", user_id)\
            .gte("played_at", cutoff)\
            .execute()
        events = res.data or []
    except Exception as e:
        print(f"Stats join query error: {e}")
        try:
            res = client.table("play_events")\
                .select("*")\
                .eq("user_id", user_id)\
                .gte("played_at", cutoff)\
                .execute()
            events = res.data or []
        except Exception as e2:
            print(f"Stats fallback query error: {e2}")
            events = []

    total_songs = len(events)
    total_duration_sec = 0
    genre_counts = {}
    artist_counts = {}
    heatmap = {str(d): [0] * 24 for d in range(7)}

    for ev in events:
        song_info = ev.get("songs") or {}
        duration = song_info.get("duration") or 0
        total_duration_sec += duration

        genre = ev.get("genre") or "mix"
        genre_counts[genre] = genre_counts.get(genre, 0) + 1

        artist = song_info.get("artist")
        if artist:
            artist_counts[artist] = artist_counts.get(artist, 0) + 1

        dow = ev.get("day_of_week")
        hod = ev.get("hour_of_day")
        if dow is not None and hod is not None:
            try:
                d_idx = int(dow)
                h_idx = int(hod)
                if 0 <= d_idx < 7 and 0 <= h_idx < 24:
                    heatmap[str(d_idx)][h_idx] += 1
            except (ValueError, TypeError):
                pass

    total_hours = round(total_duration_sec / 3600.0, 1)

    top_genres = []
    for g, count in sorted(genre_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
        pct = round((count / total_songs) * 100) if total_songs > 0 else 0
        top_genres.append({"genre": g, "count": count, "pct": pct})

    top_artists = []
    for a, count in sorted(artist_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
        top_artists.append({"artist": a, "count": count})

    taste_profile = None
    try:
        u_res = client.table("users").select("taste_profile").eq("id", user_id).limit(1).execute()
        if u_res.data and u_res.data[0].get("taste_profile"):
            taste_profile = u_res.data[0]["taste_profile"]
    except Exception:
        pass

    return {
        "total_songs": total_songs,
        "total_hours": total_hours,
        "top_genres": top_genres,
        "top_artists": top_artists,
        "heatmap": heatmap,
        "taste_profile": taste_profile,
    }


# ─── 9. TRENDING SONGS ───
@app.get("/api/trending")
async def get_trending(
    genre: Optional[str] = None,
    limit: int = 20,
    request: Request = None,
):
    """
    Returns trending songs from Redis hot sets.
    Falls back to Supabase play_count order.
    """
    r = getattr(request.app.state, "redis", None) if request else None
    client = get_supabase_client()

    key = (
        f"beats:hot:genre:{genre}"
        if genre and genre != "all"
        else "beats:hot:global"
    )

    # Get top song IDs from Redis sorted set if connected
    if r:
        try:
            song_ids = await r.zrevrange(key, 0, limit - 1)
            if song_ids:
                uuid_re = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)
                valid_ids = [sid for sid in song_ids if uuid_re.match(sid)]
                if valid_ids:
                    res = client.table("songs")\
                        .select(
                            "id, title, artist, film, genre, "
                            "duration, thumbnail_url, play_count"
                        )\
                        .in_("id", valid_ids)\
                        .eq("status", "available")\
                        .execute()
                    if res.data:
                        id_map = {s["id"]: s for s in res.data}
                        ordered = [id_map[sid] for sid in valid_ids if sid in id_map]
                        return {"songs": ordered, "source": "redis"}
        except Exception as e:
            print(f"Trending Redis query notice: {e}")

    # Fallback: Supabase play_count
    query_builder = client.table("songs")\
        .select(
            "id, title, artist, film, genre, "
            "duration, thumbnail_url, play_count"
        )\
        .eq("status", "available")\
        .order("play_count", desc=True)\
        .limit(limit)

    if genre and genre != "all":
        query_builder = query_builder.eq("genre", genre)

    res = query_builder.execute()
    return {"songs": res.data or [], "source": "supabase"}


# ─── STREAM PROXY WITH RANGE SUPPORT (Fixes Masstamilan 403 & CORS) ───
@app.get("/api/stream/{song_id}")
async def stream_song(song_id: str, request: Request):
    r = getattr(request.app.state, "redis", None)
    client = get_supabase_client()

    song_id = TEMPLATE_SONG_MAP.get(song_id, song_id)
    cache_key = f"beats:resolve:{song_id}"
    real_url = await r.get(cache_key) if r else None

    if not real_url:
        if is_valid_uuid(song_id):
            try:
                res = client.table("songs").select("source_url").eq("id", song_id).execute()
                if res.data and res.data[0].get("source_url"):
                    real_url = res.data[0]["source_url"]
            except Exception:
                pass
        else:
            clean_q = song_id.replace("song-", "").replace("-", " ").strip()
            try:
                res = client.table("songs").select("id, source_url").ilike("title", f"%{clean_q}%").limit(1).execute()
                if res.data and res.data[0].get("source_url"):
                    real_url = res.data[0]["source_url"]
            except Exception:
                pass

    if not real_url:
        # Final on-the-fly resolve attempt
        clean_term = song_id.replace("song-", "").replace("-", " ").strip()
        fresh_rec = await fetch_saavn_song_on_the_fly(clean_term, client)
        if fresh_rec and fresh_rec.get("source_url"):
            real_url = fresh_rec["source_url"]
        else:
            raise HTTPException(404, "Song source not found")

    if not any(ext in real_url for ext in [".mp3", ".m4a", ".mp4", "masstamilan.download", "saavncdn", "audio"]):
        try:
            s_res = client.table("songs").select("*").eq("id", song_id).execute()
            if s_res.data:
                fresh_url = await refresh_song_source_url(s_res.data[0], client)
                if fresh_url:
                    real_url = fresh_url
                    if r:
                        await r.set(cache_key, real_url, ex=72000)
                else:
                    real_url = s_res.data[0].get("source_url")
        except Exception:
            pass

    # Direct fast redirect for Saavn CDN URLs with native CORS support (zero proxy delay)
    if "saavncdn.com" in real_url:
        from fastapi.responses import RedirectResponse
        return RedirectResponse(real_url, status_code=307)

    req_headers = dict(STREAM_HEADERS)

    range_header = request.headers.get("range")
    if range_header:
        req_headers["Range"] = range_header

    try:
        import urllib.request
        req = urllib.request.Request(real_url, headers=req_headers)
        upstream_resp = urllib.request.urlopen(req, timeout=15)
    except urllib.error.HTTPError as e:
        if r:
            await r.delete(cache_key)
        # Attempt automatic link refresh on 403, 404, or 410 Gone (for Masstamilan tokenized links)
        if e.code in [403, 404, 410] and "saavncdn.com" not in real_url:
            try:
                s_res = client.table("songs").select("*").eq("id", song_id).execute()
                if s_res.data:
                    fresh_url = await refresh_song_source_url(s_res.data[0], client)
                    if fresh_url:
                        async with httpx.AsyncClient(headers=STREAM_HEADERS, follow_redirects=True, timeout=12.0) as ch:
                            cr = await ch.get(fresh_url)
                            real_url = str(cr.url)
                        if r:
                            await r.set(cache_key, real_url, ex=72000)
                        req = urllib.request.Request(real_url, headers=req_headers)
                        upstream_resp = urllib.request.urlopen(req, timeout=15)
                    else:
                        raise HTTPException(e.code, f"Upstream error: {e}")
                else:
                    raise HTTPException(e.code, f"Upstream error: {e}")
            except HTTPException:
                raise
            except Exception as ref_err:
                raise HTTPException(e.code, f"Upstream error: {ref_err}")
        else:
            raise HTTPException(e.code, f"Upstream error: {e}")
    except Exception as e:
        raise HTTPException(502, f"Stream connect error: {e}")

    status_code = upstream_resp.status
    content_type = upstream_resp.headers.get("Content-Type")
    if not content_type:
        content_type = "audio/mp4" if (".mp4" in real_url or "saavncdn" in real_url) else "audio/mpeg"

    resp_headers = {
        "Content-Type": content_type,
        "Accept-Ranges": "bytes",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Range, Accept, Origin",
        "Access-Control-Expose-Headers": "Content-Range, Content-Length, Accept-Ranges",
        "Cache-Control": "public, max-age=86400",
    }
    if "Content-Length" in upstream_resp.headers:
        resp_headers["Content-Length"] = upstream_resp.headers["Content-Length"]
    if "Content-Range" in upstream_resp.headers:
        resp_headers["Content-Range"] = upstream_resp.headers["Content-Range"]

    def iter_stream():
        try:
            while True:
                chunk = upstream_resp.read(64 * 1024)
                if not chunk:
                    break
                yield chunk
        finally:
            upstream_resp.close()

    return StreamingResponse(
        iter_stream(),
        status_code=status_code,
        headers=resp_headers,
        media_type=content_type
    )


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
