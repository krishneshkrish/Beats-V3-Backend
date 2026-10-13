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

        # 2. Top songs per genre
        for g in ["tamil", "malayalam", "hindi", "english", "phonk", "mix"]:
            res_genre = client.table("songs")\
                .select("id, title, artist, film, genre, duration, thumbnail_url, play_count")\
                .eq("status", "available")\
                .eq("genre", g)\
                .order("play_count", desc=True)\
                .limit(20)\
                .execute()
            if res_genre.data:
                await r.set(f"beats:songs:{g}:20", json.dumps({"songs": res_genre.data}), ex=600)

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
                socket_connect_timeout=8,
                socket_timeout=8,
                retry_on_timeout=True,
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
@app.api_route("/health", methods=["GET", "HEAD"])
async def health():
    return {
        "status": "ok",
        "service": "beats-api",
        "version": "2.0.0",
        
        "timestamp": datetime.now(IST).isoformat(),
    }


@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {
        "status": "ok",
        "service": "beats-api",
        "message": "Beats Backend is running",
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


SEARCH_STOP_WORDS = {"movie", "film", "song", "songs", "track", "tracks", "audio", "video", "full"}


def strip_search_stopwords(q: str) -> str:
    """Removes common conversational search keywords like 'movie', 'songs'."""
    words = q.strip().split()
    filtered = [w for w in words if w.lower() not in SEARCH_STOP_WORDS]
    return " ".join(filtered) if filtered else q


def normalize_search_text(text: str) -> str:
    """Normalizes text by removing punctuation, lowercasing, and collapsing repeated characters."""
    t = re.sub(r'[^\w\s]', ' ', (text or '').lower())
    t = re.sub(r'(.)\1+', r'\1', t)
    return " ".join(t.split())


def generate_search_variants(q: str) -> list:
    """Generate comprehensive romanized spelling, phonetic, and collapsed variants."""
    raw = q.strip().lower()
    variants = [raw]

    # 1. Collapsed consecutive duplicate characters (e.g. allaaka -> alaka, loova -> lova)
    collapsed = re.sub(r'(.)\1+', r'\1', raw)
    if collapsed != raw:
        variants.append(collapsed)

    # 2. Query without conversational stopwords (e.g. 'om movie' -> 'om')
    no_stop = strip_search_stopwords(raw)
    if no_stop != raw and len(no_stop) >= 2:
        variants.append(no_stop)
        c_no_stop = re.sub(r'(.)\1+', r'\1', no_stop)
        if c_no_stop != no_stop:
            variants.append(c_no_stop)

    # 3. Common Indian phonetic transliterations
    if "gh" in raw:
        variants.append(raw.replace("gh", "g"))
    elif "g" in raw:
        variants.append(raw.replace("g", "gh"))

    if "kh" in raw:
        variants.append(raw.replace("kh", "k"))

    if "bh" in raw:
        variants.append(raw.replace("bh", "b"))

    if "ph" in raw:
        variants.append(raw.replace("ph", "p"))
        variants.append(raw.replace("ph", "f"))

    if "sh" in raw:
        variants.append(raw.replace("sh", "s"))
    elif "s" in raw:
        variants.append(raw.replace("s", "sh"))

    if "th" in raw:
        variants.append(raw.replace("th", "t"))
    elif "t" in raw:
        variants.append(raw.replace("t", "th"))

    if "dh" in raw:
        variants.append(raw.replace("dh", "d"))
    elif "d" in raw:
        variants.append(raw.replace("d", "dh"))

    if "zh" in raw:
        variants.append(raw.replace("zh", "l"))

    # Vowel and consonant duplications/reductions
    if "oo" in raw:
        variants.append(raw.replace("oo", "o"))
        variants.append(raw.replace("oo", "u"))
    if "ee" in raw:
        variants.append(raw.replace("ee", "e"))
        variants.append(raw.replace("ee", "i"))
    if "aa" in raw:
        variants.append(raw.replace("aa", "a"))
    if "ll" in raw:
        variants.append(raw.replace("ll", "l"))
    if "vv" in raw:
        variants.append(raw.replace("vv", "v"))
    if "w" in raw:
        variants.append(raw.replace("w", "v"))
    elif "v" in raw:
        variants.append(raw.replace("v", "w"))

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

    # 2.5 Ensure direct title and film matches are fetched first so they are never crowded out
    direct_queries = [clean_q]
    collapsed_q = re.sub(r'(.)\1+', r'\1', clean_q)
    if collapsed_q != clean_q and len(collapsed_q) >= 2:
        direct_queries.append(collapsed_q)
    core_q = strip_search_stopwords(clean_q)
    if core_q != clean_q and len(core_q) >= 2:
        direct_queries.append(core_q)
        c_core = re.sub(r'(.)\1+', r'\1', core_q)
        if c_core != core_q:
            direct_queries.append(c_core)

    for dq in direct_queries:
        try:
            title_res = client.table("songs")\
                .select("id, title, artist, film, genre, duration, thumbnail_url, play_count, status, source_url")\
                .eq("status", "available")\
                .ilike("title", f"%{dq}%")\
                .order("play_count", desc=True)\
                .limit(40)\
                .execute()
            for s in (title_res.data or []):
                if s["id"] not in seen_ids:
                    seen_ids.add(s["id"])
                    songs_data.append(s)
        except Exception as e:
            print(f"Title pre-search notice for '{dq}': {e}")

    # For movie titles or film name searches, also query film field and 'From ...' title patterns
    film_terms = [core_q] if core_q and len(core_q) >= 2 else ([clean_q] if len(clean_q) >= 2 else [])
    for fq in film_terms:
        try:
            film_res = client.table("songs")\
                .select("id, title, artist, film, genre, duration, thumbnail_url, play_count, status, source_url")\
                .eq("status", "available")\
                .ilike("film", f"%{fq}%")\
                .order("play_count", desc=True)\
                .limit(30)\
                .execute()
            for s in (film_res.data or []):
                if s["id"] not in seen_ids:
                    seen_ids.add(s["id"])
                    songs_data.append(s)
        except Exception:
            pass

        try:
            from_res = client.table("songs")\
                .select("id, title, artist, film, genre, duration, thumbnail_url, play_count, status, source_url")\
                .eq("status", "available")\
                .ilike("title", f"%from %{fq}%")\
                .order("play_count", desc=True)\
                .limit(30)\
                .execute()
            for s in (from_res.data or []):
                if s["id"] not in seen_ids:
                    seen_ids.add(s["id"])
                    songs_data.append(s)
        except Exception:
            pass

    # 3. Fast database OR query matching all variants with expanded candidate pool
    or_clauses = []
    for v in variants[:5]:
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
                "duration, thumbnail_url, play_count, status, source_url"
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

    # 4. Multi-word & movie/genre matching: enforce AND across query words
    words = [w for w in clean_q.split() if len(w) >= 2]
    words_no_stop = [w for w in strip_search_stopwords(clean_q).split() if len(w) >= 2]
    effective_words = words_no_stop if words_no_stop else words

    if len(effective_words) > 1 and len(songs_data) < candidate_limit:
        try:
            query_words = client.table("songs")\
                .select("id, title, artist, film, genre, duration, thumbnail_url, play_count, status")\
                .eq("status", "available")

            for term in effective_words[:3]:
                term_variants = generate_search_variants(term)
                term_clauses = []
                for tv in term_variants[:3]:
                    term_clauses.extend([
                        f"title.ilike.%{tv}%",
                        f"film.ilike.%{tv}%",
                        f"artist.ilike.%{tv}%",
                    ])
                query_words = query_words.or_(",".join(term_clauses))

            res_words = query_words.limit(50).execute()
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

            norm_q_phon = normalize_phonetic(clean_q)
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
                if norm_q_phon and (norm_q_phon in norm_title or norm_q_phon in norm_film or norm_q_phon in norm_artist):
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

    # 6. Dynamic on-the-fly search fetch if local catalog has limited results (< 3) OR lacks normalized title match
    norm_search = normalize_search_text(clean_q)
    norm_core = normalize_search_text(core_q)
    q_words = [w for w in norm_core.split() if len(w) >= 2] or [w for w in norm_search.split() if len(w) >= 2]

    has_title_match = any(
        norm_search in normalize_search_text(s.get("title") or "")
        or (norm_core and norm_core in normalize_search_text(s.get("title") or ""))
        or (q_words and all(w in normalize_search_text(s.get("title") or "") for w in q_words))
        for s in songs_data
    )
    if len(songs_data) < 3 or not has_title_match:
        try:
            dynamic_songs = await fetch_dynamic_search_songs(clean_q, client, limit=limit)
            if not dynamic_songs and core_q != clean_q and len(core_q) >= 2:
                dynamic_songs = await fetch_dynamic_search_songs(core_q, client, limit=limit)
            for s in dynamic_songs:
                if s["id"] not in seen_ids:
                    seen_ids.add(s["id"])
                    songs_data.append(s)
        except Exception as e:
            print(f"Dynamic fetch error in search: {e}")

    # 7. Relevance-first ranking: Exact/Collapsed title -> Substring -> Token intersection -> Film/Artist
    def compute_relevance(song: dict):
        title = (song.get("title") or "").strip().lower()
        film = (song.get("film") or "").strip().lower()
        artist = (song.get("artist") or "").strip().lower()
        p_count = song.get("play_count") or 0

        clean_t = re.sub(r'[,()"\-:]', '', title).strip()
        clean_search = re.sub(r'[,()"\-:]', '', clean_q).strip()

        norm_t = normalize_search_text(title)
        norm_f = normalize_search_text(film)
        norm_a = normalize_search_text(artist)

        is_short_query = len(clean_q) <= 3 or (norm_core and len(norm_core) <= 3)
        # If query is short (like 'om'), verify whole-word boundary
        has_word_title = bool(re.search(rf"\b{re.escape(clean_q)}\b", title, re.I)) or (norm_core and bool(re.search(rf"\b{re.escape(norm_core)}\b", norm_t, re.I)))
        has_word_film = bool(re.search(rf"\b{re.escape(clean_q)}\b", film, re.I)) or (norm_core and bool(re.search(rf"\b{re.escape(norm_core)}\b", norm_f, re.I)))
        has_word_artist = bool(re.search(rf"\b{re.escape(clean_q)}\b", artist, re.I)) or (norm_core and bool(re.search(rf"\b{re.escape(norm_core)}\b", norm_a, re.I)))

        score = 0
        if title == clean_q or clean_t == clean_search or norm_t == norm_search or (norm_core and norm_t == norm_core):
            score = 1000
        elif title.startswith(clean_q) or clean_t.startswith(clean_search) or norm_t.startswith(norm_search) or (norm_core and norm_t.startswith(norm_core)):
            score = 950
        elif (clean_q in title or clean_search in clean_t or norm_search in norm_t or (norm_core and norm_core in norm_t)) and (not is_short_query or has_word_title):
            score = 900
        elif q_words and all(w in norm_t for w in q_words) and (not is_short_query or has_word_title):
            score = 850
        elif (film == clean_q or norm_f == norm_search or norm_f.startswith(norm_search) or (norm_core and (norm_f == norm_core or norm_f.startswith(norm_core)))) and (not is_short_query or has_word_film):
            score = 800
        elif (clean_q in film or norm_search in norm_f or (norm_core and norm_core in norm_f)) and (not is_short_query or has_word_film):
            score = 750
        elif q_words and all(w in norm_f for w in q_words) and (not is_short_query or has_word_film):
            score = 700
        elif (artist == clean_q or norm_a == norm_search or norm_a.startswith(norm_search) or (norm_core and (norm_a == norm_core or norm_a.startswith(norm_core)))) and (not is_short_query or has_word_artist):
            score = 650
        elif (clean_q in artist or norm_search in norm_a or (norm_core and norm_core in norm_a)) and (not is_short_query or has_word_artist):
            score = 600
        elif q_words and all(w in norm_a for w in q_words) and (not is_short_query or has_word_artist):
            score = 550
        else:
            if is_short_query and not (has_word_title or has_word_film or has_word_artist):
                score = 50
            else:
                matched = sum(1 for w in q_words if w in norm_t or w in norm_f or w in norm_a) if q_words else 0
                score = 100 + int((matched / max(len(q_words), 1)) * 300)

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

        # Invalidate recommendation & trending caches so changes reflect immediately
        try:
            r_keys = await r.keys(f"beats:recs:{event.user_id}:*")
            t_keys = await r.keys("beats:trending:dyn:*")
            del_keys = (r_keys or []) + (t_keys or [])
            if del_keys:
                await r.delete(*del_keys)
        except Exception:
            pass

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

    # 0. Resolve user_id: match by ID or display name
    actual_user_id = user_id
    user_res = client.table("users")\
        .select("id, play_count")\
        .eq("id", user_id)\
        .execute()

    if not user_res.data:
        name_res = client.table("users")\
            .select("id, play_count")\
            .ilike("name", user_id)\
            .order("play_count", desc=True)\
            .limit(1)\
            .execute()
        if name_res.data:
            actual_user_id = name_res.data[0]["id"]
            user_res = name_res

    # Cache check (per user per time block & hour)
    cache_key = f"beats:recs:{actual_user_id}:{time_block['key']}:{now.hour}"
    if r:
        try:
            cached = await r.get(cache_key)
            if cached:
                data = json.loads(cached)
                data["cached"] = True
                return data
        except Exception:
            pass

    play_count = 0
    if user_res.data:
        play_count = user_res.data[0].get("play_count", 0) or 0

    # Route to correct strategy: ML if >= 50 plays, else rule-based
    songs = []
    strategy = "rule_based"
    if play_count >= 50 and r:
        try:
            from recommender import ml_recs
            songs = await ml_recs(actual_user_id, limit, client, r)
            if songs:
                strategy = "ml"
        except Exception as ml_err:
            print(f"ML recs notice for {actual_user_id}: {ml_err}")

    if not songs:
        from recommender import rule_based_recs
        songs = await rule_based_recs(actual_user_id, limit, client, r, now)
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

    # Short 3-minute cache for dynamic variety and zero-delay performance
    if r:
        try:
            await r.set(cache_key, json.dumps(result), ex=180)
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
    r = getattr(request.app.state, "redis", None) if request else None
    client = get_supabase_client()
    now = datetime.now(IST)

    from recommender import radio_recs, get_time_block
    time_block = get_time_block(now.hour)

    actual_song_id = TEMPLATE_SONG_MAP.get(song_id, song_id)
    exclude_ids = [s.strip() for s in exclude.split(",") if s.strip()] if exclude else []

    actual_user_id = user_id
    if user_id and user_id not in ["guest", "anonymous"]:
        try:
            user_res = client.table("users").select("id").eq("id", user_id).execute()
            if not user_res.data:
                name_res = client.table("users").select("id").ilike("name", user_id).limit(1).execute()
                if name_res.data:
                    actual_user_id = name_res.data[0]["id"]
        except Exception:
            pass

    try:
        songs = await radio_recs(
            seed_song_id=actual_song_id,
            user_id=actual_user_id,
            limit=limit,
            supabase=client,
            redis=r,
            exclude_ids=exclude_ids,
            now=now
        )
    except Exception as e:
        print(f"Radio generation error: {e}")
        from recommender import rule_based_recs
        songs = await rule_based_recs(actual_user_id, limit, client, r, now)

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
    total_songs, total_hours, top_genres, top_artists, heatmap, dynamic taste profile
    """
    client = get_supabase_client()
    now = datetime.now(IST)
    from datetime import timedelta
    cutoff = (now - timedelta(days=30)).isoformat()

    # Resolve UUID if user_id was passed as username
    actual_uid = user_id
    try:
        if not is_valid_uuid(user_id):
            u_lookup = client.table("users").select("id").ilike("name", user_id).limit(1).execute()
            if u_lookup.data:
                actual_uid = u_lookup.data[0]["id"]
    except Exception:
        pass

    try:
        res = client.table("play_events")\
            .select("id, song_id, played_at, hour_of_day, day_of_week, genre, songs(duration, artist)")\
            .eq("user_id", actual_uid)\
            .gte("played_at", cutoff)\
            .execute()
        events = res.data or []
    except Exception as e:
        print(f"Stats join query error: {e}")
        try:
            res = client.table("play_events")\
                .select("*")\
                .eq("user_id", actual_uid)\
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
        duration = (song_info.get("duration") if isinstance(song_info, dict) else None) or 210
        total_duration_sec += duration

        genre = (ev.get("genre") or "mix").strip().lower()
        genre_counts[genre] = genre_counts.get(genre, 0) + 1

        artist = song_info.get("artist") if isinstance(song_info, dict) else None
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
        u_res = client.table("users").select("taste_profile").eq("id", actual_uid).limit(1).execute()
        if u_res.data and u_res.data[0].get("taste_profile"):
            taste_profile = u_res.data[0]["taste_profile"]
    except Exception:
        pass

    # Dynamically personalize taste profile if not manually set
    if not taste_profile and top_genres:
        top_g = top_genres[0]["genre"].capitalize()
        sec_g = f" and {top_genres[1]['genre'].capitalize()}" if len(top_genres) > 1 else ""
        top_a = f" with high affinity for {top_artists[0]['artist']}" if top_artists else ""
        
        peak_h = 21
        max_val = 0
        for d in range(7):
            for h in range(24):
                if heatmap[str(d)][h] > max_val:
                    max_val = heatmap[str(d)][h]
                    peak_h = h
        ampm = "AM" if peak_h < 12 else "PM"
        disp_h = peak_h if (0 < peak_h <= 12) else (peak_h - 12 if peak_h > 12 else 12)
        taste_profile = f"Heavy rotation on {top_g}{sec_g} soundscapes{top_a}. Consistent listening spike around {disp_h} {ampm}."

    return {
        "total_songs": total_songs,
        "total_hours": total_hours,
        "top_genres": top_genres,
        "top_artists": top_artists,
        "heatmap": heatmap,
        "taste_profile": taste_profile,
    }


# ─── 9. DYNAMIC TRENDING SONGS ───
@app.get("/api/trending")
async def get_trending(
    genre: Optional[str] = None,
    limit: int = 20,
    request: Request = None,
):
    """
    Returns authentic, dynamic trending songs.
    Blends real listener play velocity from the past 7 days with catalog rotation,
    guaranteeing that Trending Now stays fresh, active, and never locked to old static songs.
    """
    r = getattr(request.app.state, "redis", None) if request else None
    client = get_supabase_client()
    now = datetime.now(IST)

    clean_genre = genre.strip().lower() if genre and genre != "all" else None
    cache_key = f"beats:trending:dyn:{clean_genre or 'all'}"

    # 1. Check dynamic Redis cache (short 5-min TTL)
    if r:
        try:
            cached = await r.get(cache_key)
            if cached:
                cached_data = json.loads(cached)
                return {"songs": cached_data.get("songs", []), "source": "redis_dynamic"}
        except Exception:
            pass

    # 2. Gather recent play events from last 7 days
    from datetime import timedelta
    from collections import Counter
    cutoff = (now - timedelta(days=7)).isoformat()
    recent_counts = Counter()
    try:
        ev_query = client.table("play_events").select("song_id, genre").gte("played_at", cutoff)
        if clean_genre:
            ev_query = ev_query.eq("genre", clean_genre)
        ev_res = ev_query.limit(500).execute()
        for ev in (ev_res.data or []):
            sid = ev.get("song_id")
            if sid:
                recent_counts[sid] += 1
    except Exception as e:
        print(f"Trending events query notice: {e}")

    trending_songs = []
    seen_ids = set()

    # 3. Pull top songs by recent listener velocity
    if recent_counts:
        top_recent_ids = [sid for sid, _ in recent_counts.most_common(limit)]
        try:
            s_res = client.table("songs")\
                .select("id, title, artist, film, genre, duration, thumbnail_url, play_count, status, source_url")\
                .in_("id", top_recent_ids)\
                .eq("status", "available")\
                .execute()
            id_map = {s["id"]: s for s in (s_res.data or [])}
            for sid in top_recent_ids:
                if sid in id_map and sid not in seen_ids:
                    seen_ids.add(sid)
                    trending_songs.append(id_map[sid])
        except Exception as e:
            print(f"Trending recent songs fetch notice: {e}")

    # 4. Dynamic catalog backfill with daily variety
    if len(trending_songs) < limit:
        day_seed = now.timetuple().tm_yday
        try:
            if clean_genre:
                cat_res = client.table("songs")\
                    .select("id, title, artist, film, genre, duration, thumbnail_url, play_count, status, source_url")\
                    .eq("status", "available")\
                    .eq("genre", clean_genre)\
                    .order("play_count", desc=True)\
                    .limit(60)\
                    .execute()
                candidates = cat_res.data or []
                if candidates:
                    day_offset = (day_seed * 3) % len(candidates)
                    rotated = candidates[day_offset:] + candidates[:day_offset]
                    for s in rotated:
                        if s["id"] not in seen_ids:
                            seen_ids.add(s["id"])
                            trending_songs.append(s)
                        if len(trending_songs) >= limit:
                            break
            else:
                # Balanced cross-genre blend for global trending
                genres = ["tamil", "malayalam", "hindi", "english", "phonk", "mix"]
                for g in genres:
                    g_res = client.table("songs")\
                        .select("id, title, artist, film, genre, duration, thumbnail_url, play_count, status, source_url")\
                        .eq("status", "available")\
                        .eq("genre", g)\
                        .order("play_count", desc=True)\
                        .limit(20)\
                        .execute()
                    g_cands = g_res.data or []
                    if g_cands:
                        offset = (day_seed * 2) % len(g_cands)
                        g_rot = g_cands[offset:] + g_cands[:offset]
                        for s in g_rot[:3]:
                            if s["id"] not in seen_ids:
                                seen_ids.add(s["id"])
                                trending_songs.append(s)
                            if len(trending_songs) >= limit:
                                break
                    if len(trending_songs) >= limit:
                        break
        except Exception as e:
            print(f"Trending catalog backfill notice: {e}")

    # 5. Cache dynamically in Redis for 5 minutes
    if r and trending_songs:
        try:
            await r.set(cache_key, json.dumps({"songs": trending_songs}), ex=300)
        except Exception:
            pass

    return {"songs": trending_songs[:limit], "source": "dynamic_velocity"}


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
