import os
import sys
import re
import time
import json
import base64
import html
import asyncio
import argparse
from urllib.parse import urljoin, urlparse, quote
from typing import List, Dict, Any, Optional

# Configure UTF-8 encoding for safe output on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import requests
import httpx
from bs4 import BeautifulSoup
from Crypto.Cipher import DES
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

# Sources configuration for masstamilan network & independent music
SOURCES: Dict[str, Dict[str, Any]] = {
    "tamil": {
        "base_url": "https://www.masstamilan.dev",
        "listing_paths": ["/tamil-songs?page={page}", "/tamil-singles?page={page}"],
        "language": "Tamil",
    },
    "malayalam": {
        "base_url": "https://www.mp3chetta.com",
        "listing_paths": ["/malayalam-songs?page={page}"],
        "language": "Malayalam",
    },
    "hindi": {
        "base_url": "https://www.mp3bhai.com",
        "listing_paths": ["/hindi-songs?page={page}"],
        "language": "Hindi",
    },
    "mix": {
        "base_url": "https://www.masstelugu.com",
        "listing_paths": ["/telugu-songs?page={page}"],
        "language": "Telugu",
    },
}

STREAM_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 "
        "Mobile/15E148 Safari/604.1"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

# In-memory deduplication set
seen_source_urls = set()


def get_supabase_client() -> Optional[Client]:
    """Retrieve Supabase client or return None if credentials not configured."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        return None
    try:
        return create_client(url, key)
    except Exception as e:
        print(f"[!] Could not initialize Supabase client: {e}")
        return None


def create_session(proxy: Optional[str] = None) -> requests.Session:
    """Create requests session with custom headers and optional proxy."""
    session = requests.Session()
    session.headers.update(STREAM_HEADERS)
    proxy_url = proxy or os.environ.get("SCRAPER_PROXY")
    if proxy_url:
        session.proxies.update({
            "http": proxy_url,
            "https": proxy_url,
        })
    return session


def decrypt_saavn_url(enc_url: str) -> str:
    """Decrypts JioSaavn DES-ECB encrypted media URL to direct 320kbps CDN stream."""
    try:
        cipher = DES.new(b"38346591", DES.MODE_ECB)
        raw = cipher.decrypt(base64.b64decode(enc_url.strip()))
        pad_len = raw[-1]
        if 0 < pad_len <= 8:
            raw = raw[:-pad_len]
        url = raw.decode("utf-8", errors="ignore")
        return url.replace("_96.mp4", "_320.mp4").replace("_160.mp4", "_320.mp4")
    except Exception:
        return ""


def parse_duration_to_seconds(dur_str: str) -> int:
    """Convert mm:ss or hh:mm:ss string to total seconds."""
    if not dur_str:
        return 0
    parts = str(dur_str).strip().split(":")
    try:
        if len(parts) == 1 and parts[0].isdigit():
            return int(parts[0])
        elif len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        elif len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except (ValueError, TypeError):
        return 0
    return 0


def upsert_batch(supabase: Client, batch: List[Dict[str, Any]]) -> int:
    """Upserts batch of songs into Supabase with source_url deduplication."""
    if not supabase or not batch:
        return 0
    try:
        supabase.table("songs").upsert(batch, on_conflict="source_url").execute()
        print(f"  [OK] Upserted {len(batch)} songs into Supabase")
        return len(batch)
    except Exception as e:
        print(f"  [X] Failed to upsert batch into Supabase: {e}")
        return 0


# ══════════════════════════════════════════════════════════════════════════════
# 1. ARTIST & NON-MOVIE INDEPENDENT SCRAPER
# ══════════════════════════════════════════════════════════════════════════════

def scrape_artist_songs(
    artist_name: str,
    limit: int = 20,
    supabase: Optional[Client] = None,
    proxy: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Scrapes independent artist & band catalogs directly from high-bitrate CDNs.
    Works for indie rappers, bands, and standalone artists (e.g., Vedan, Agam).
    """
    print(f"\n=======================================================")
    print(f"Scraping Artist Catalog: '{artist_name}' (Limit: {limit})")
    print(f"=======================================================")

    headers = {"User-Agent": STREAM_HEADERS["User-Agent"]}
    proxies = {"http": proxy, "https": proxy} if proxy else None
    records: List[Dict[str, Any]] = []
    seen_urls: set = set()

    # Step A: Identify artist ID via JioSaavn Artist API
    artist_id = None
    try:
        search_artist_url = (
            f"https://www.jiosaavn.com/api.php?__call=search.getArtistResults"
            f"&_format=json&_marker=0&n=8&p=1&q={quote(artist_name)}"
        )
        r = requests.get(search_artist_url, headers=headers, proxies=proxies, timeout=10)
        if r.status_code == 200:
            data = json.loads(r.text.strip())
            results = data.get("results", [])
            # Priority 1: Exact name match
            for a in results:
                if a.get("name", "").strip().lower() == artist_name.strip().lower():
                    artist_id = a.get("id")
                    break
            # Priority 2: Substring match
            if not artist_id and results:
                for a in results:
                    if artist_name.strip().lower() in a.get("name", "").strip().lower():
                        artist_id = a.get("id")
                        break
    except Exception as e:
        print(f"  [!] Artist ID search note: {e}")

    # Step B: If artist ID found, fetch top artist tracks
    if artist_id:
        try:
            det_url = (
                f"https://www.jiosaavn.com/api.php?__call=artist.getArtistPageDetails"
                f"&_format=json&_marker=0&artistId={artist_id}&n_song={limit}"
            )
            r = requests.get(det_url, headers=headers, proxies=proxies, timeout=12)
            if r.status_code == 200:
                det_data = json.loads(r.text.strip())
                top_obj = det_data.get("topSongs", {})
                top_songs = top_obj.get("songs", []) if isinstance(top_obj, dict) else (top_obj or [])
                for s in top_songs:
                    enc_url = s.get("encrypted_media_url")
                    if not enc_url:
                        continue
                    stream_url = decrypt_saavn_url(enc_url)
                    if not stream_url or stream_url in seen_urls:
                        continue
                    seen_urls.add(stream_url)

                    raw_title = html.unescape(s.get("song") or s.get("title") or "")
                    title = raw_title.replace("&quot;", '"').replace("&#039;", "'").strip()

                    raw_singers = html.unescape(s.get("singers") or s.get("primary_artists") or artist_name)
                    singers = raw_singers.replace("&quot;", '"').replace("&#039;", "'").strip()
                    # Ensure artist_name is represented if singers is blank or generic
                    if not singers or singers == "Various Artists":
                        singers = artist_name

                    raw_album = html.unescape(s.get("album") or "")
                    film = raw_album.replace("&quot;", '"').replace("&#039;", "'").strip()
                    if not film:
                        film = "Single"

                    lang = s.get("language") or "Malayalam"
                    lang_lower = lang.lower()
                    genre = lang_lower if lang_lower in ["tamil", "malayalam", "hindi", "english"] else "mix"

                    thumb = (s.get("image") or "").replace("150x150", "500x500")
                    dur = parse_duration_to_seconds(s.get("duration"))

                    record = {
                        "title": title,
                        "artist": singers,
                        "film": film,
                        "genre": genre,
                        "language": lang,
                        "duration": dur if dur > 0 else 180,
                        "thumbnail_url": thumb,
                        "source_url": stream_url,
                        "source_site": "jiosaavn_cdn",
                        "play_count": 3500,
                        "status": "available",
                    }
                    records.append(record)
                    print(f"  [Artist Discography] '{title}' by {singers} ({film})")
        except Exception as e:
            print(f"  [!] Top songs extraction error: {e}")

    # Step C: Complement with targeted search queries (especially for indie singles / rap)
    queries = [
        artist_name,
        f"{artist_name} songs",
        f"{artist_name} band" if "band" not in artist_name.lower() else artist_name,
        f"{artist_name} independent",
        f"{artist_name} rap",
    ]
    # Artist-specific canonical tracks
    if "vedan" in artist_name.lower():
        queries.extend(["Vedan Vaa", "Vedan Bhoomi", "Vedan Kuthanthram", "Vedan Voice of the voiceless"])
    elif "agam" in artist_name.lower():
        queries.extend(["Rangapura Vihara Agam", "The Inner Self Awakens Agam", "Mist of Capricorn Agam", "Malhar Jam Agam"])

    for q in queries:
        if len(records) >= limit * 2:
            break
        try:
            q_url = (
                f"https://www.jiosaavn.com/api.php?__call=search.getResults"
                f"&_format=json&_marker=0&n=5&p=1&q={quote(q)}"
            )
            r = requests.get(q_url, headers=headers, proxies=proxies, timeout=10)
            if r.status_code != 200:
                continue
            data = json.loads(r.text.strip())
            results = data.get("results", [])
            for item in results:
                enc = item.get("encrypted_media_url")
                if not enc:
                    continue
                stream_url = decrypt_saavn_url(enc)
                if not stream_url or stream_url in seen_urls:
                    continue

                raw_title = html.unescape(item.get("song") or item.get("title") or "")
                title = raw_title.replace("&quot;", '"').replace("&#039;", "'").strip()

                raw_artists = html.unescape(item.get("singers") or item.get("primary_artists") or artist_name)
                singers = raw_artists.replace("&quot;", '"').replace("&#039;", "'").strip()
                if not singers or singers == "Various":
                    singers = artist_name

                # Filter relevance: either artist in singers or artist query in title
                if (
                    artist_name.lower() not in singers.lower()
                    and artist_name.lower() not in title.lower()
                    and artist_name.lower() not in (item.get("album") or "").lower()
                ):
                    continue

                seen_urls.add(stream_url)
                raw_album = html.unescape(item.get("album") or "")
                film = raw_album.replace("&quot;", '"').replace("&#039;", "'").strip() or "Single"

                lang = item.get("language") or "Malayalam"
                lang_lower = lang.lower()
                genre = lang_lower if lang_lower in ["tamil", "malayalam", "hindi", "english"] else "mix"

                thumb = (item.get("image") or "").replace("150x150", "500x500")
                dur = parse_duration_to_seconds(item.get("duration"))

                record = {
                    "title": title,
                    "artist": singers,
                    "film": film,
                    "genre": genre,
                    "language": lang,
                    "duration": dur if dur > 0 else 180,
                    "thumbnail_url": thumb,
                    "source_url": stream_url,
                    "source_site": "jiosaavn_cdn",
                    "play_count": 4200,
                    "status": "available",
                }
                records.append(record)
                print(f"  [Artist Search] '{title}' by {singers} ({film})")
        except Exception as e:
            print(f"  [!] Query '{q}' notice: {e}")

    # Upsert into database
    if supabase and records:
        upsert_batch(supabase, records)
    elif not supabase:
        print(f"  [Dry-run] Extracted {len(records)} songs for '{artist_name}' (Supabase not configured)")

    print(f"[OK] Completed artist scrape for '{artist_name}': {len(records)} songs ingested.")
    return records


# ══════════════════════════════════════════════════════════════════════════════
# 2. ASYNC HIGH-SPEED WEB CRAWLER (MASSTAMILAN / MP3CHETTA / MP3BHAI)
# ══════════════════════════════════════════════════════════════════════════════

async def async_get_movie_urls(
    base_url: str,
    listing_path: str,
    pages: int,
    client: httpx.AsyncClient,
) -> List[str]:
    """Asynchronously collect movie/album URLs across pages."""
    movie_urls = []
    seen = set()

    for page in range(1, pages + 1):
        url = urljoin(base_url, listing_path.format(page=page))
        try:
            resp = await client.get(url, timeout=12.0)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                # Match movie and single/indie detail links
                if ("-songs" in href or "-singles" in href or "-single" in href) and not any(
                    x in href for x in ["/tamil-songs", "/malayalam-songs", "/hindi-songs", "/telugu-songs"]
                ):
                    full = urljoin(base_url, href)
                    if full not in seen:
                        seen.add(full)
                        movie_urls.append(full)
        except Exception as e:
            print(f"  [!] Error crawling listing {url}: {e}")
    return movie_urls


async def async_scrape_movie_page(
    movie_url: str,
    genre: str,
    base_url: str,
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
) -> List[Dict[str, Any]]:
    """Asynchronously scrape a single movie or independent album page."""
    records = []
    source_site = urlparse(base_url).netloc

    async with sem:
        try:
            resp = await client.get(movie_url, timeout=12.0)
            if resp.status_code != 200:
                return records

            soup = BeautifulSoup(resp.text, "html.parser")

            # 1. Film / Album Name
            film = None
            h1 = soup.find("h1")
            if h1:
                film = re.sub(
                    r"\s*(Malayalam|Tamil|Hindi|Telugu)?\s*(mp3|songs|singles?|download).*",
                    "",
                    h1.text,
                    flags=re.IGNORECASE,
                ).strip()

            # 2. Movie/Album metadata
            year = None
            language = SOURCES.get(genre, {}).get("language", "")
            fieldset = soup.find("fieldset")
            if fieldset:
                info_text = fieldset.get_text(" ", strip=True)
                y_match = re.search(r"Year:\s*(\d{4})", info_text)
                if y_match:
                    year = int(y_match.group(1))
                l_match = re.search(r"Language:\s*([A-Za-z]+)", info_text)
                if l_match:
                    language = l_match.group(1).strip()

            # 3. Poster Thumbnail
            thumbnail_url = None
            for img in soup.find_all("img"):
                src = img.get("src", "")
                if src and ("/i/" in src or "cover" in src or "thumb" in src or src.endswith((".jpg", ".png", ".webp"))):
                    thumbnail_url = urljoin(base_url, src)
                    break

            # 4. Parse songs table
            table = soup.find("table")
            if not table:
                return records

            for tr in table.find_all("tr"):
                name_el = tr.find(attrs={"itemprop": "name"}) or tr.find("a", href=lambda h: h and "-mp3-song" in h)
                if not name_el:
                    continue
                title = name_el.text.strip()
                if not title:
                    continue

                artist = "Unknown"
                artist_el = tr.find(attrs={"itemprop": "byArtist"})
                if artist_el and artist_el.text.strip():
                    artist = artist_el.text.strip()
                else:
                    m_artist = re.search(r"Artists?:\s*([^\n\r<]+)", tr.text)
                    if m_artist:
                        artist = m_artist.group(1).strip()

                duration_sec = 0
                dur_el = tr.find(attrs={"itemprop": "duration"})
                if dur_el:
                    duration_sec = parse_duration_to_seconds(dur_el.text)

                dl_a = tr.find("a", href=lambda h: h and ("d128" in h or "128" in str(h))) or tr.find(
                    "a", string=re.compile(r"128\s*kbps", re.I)
                )

                source_url = None
                if dl_a and dl_a.get("href"):
                    source_url = urljoin(base_url, dl_a["href"])

                if not source_url or source_url in seen_source_urls:
                    continue
                seen_source_urls.add(source_url)

                records.append({
                    "title": title,
                    "artist": artist,
                    "film": film or "Single",
                    "genre": genre,
                    "language": language,
                    "duration": duration_sec,
                    "thumbnail_url": thumbnail_url,
                    "source_url": source_url,
                    "source_site": source_site,
                    "year": year,
                    "status": "available",
                })
        except Exception:
            pass

    return records


async def async_crawl_genre(
    genre: str,
    pages: int,
    supabase: Optional[Client],
    proxy: Optional[str] = None,
    batch_size: int = 25,
) -> int:
    """High-speed asynchronous crawler using httpx concurrency."""
    cfg = SOURCES.get(genre)
    if not cfg:
        return 0

    base_url = cfg["base_url"]
    listing_paths = cfg.get("listing_paths", ["/tamil-songs?page={page}"])
    proxy_url = proxy or os.environ.get("SCRAPER_PROXY")
    client_kwargs = {
        "headers": STREAM_HEADERS,
        "timeout": 15.0,
        "follow_redirects": True
    }
    if proxy_url:
        client_kwargs["proxy"] = proxy_url

    print(f"\n[Fast Async Crawler] Genre: {genre.upper()} | Base: {base_url} | Pages: {pages}")

    sem = asyncio.Semaphore(8)
    total_songs = 0
    buffer: List[Dict[str, Any]] = []

    async with httpx.AsyncClient(**client_kwargs) as client:
        # Collect URLs across all listing paths (including singles)
        all_detail_urls = []
        for path in listing_paths:
            urls = await async_get_movie_urls(base_url, path, pages, client)
            all_detail_urls.extend(urls)

        print(f"  Found {len(all_detail_urls)} detail pages to scrape concurrently...")

        # Concurrently scrape detail pages
        tasks = [
            async_scrape_movie_page(u, genre, base_url, client, sem)
            for u in all_detail_urls
        ]

        for coro in asyncio.as_completed(tasks):
            songs = await coro
            if songs:
                buffer.extend(songs)
                total_songs += len(songs)

            if len(buffer) >= batch_size:
                if supabase:
                    upsert_batch(supabase, buffer)
                buffer.clear()

        if buffer and supabase:
            upsert_batch(supabase, buffer)
            buffer.clear()

    print(f"[OK] Completed {genre} crawl: {total_songs} songs extracted.")
    return total_songs


# ══════════════════════════════════════════════════════════════════════════════
# 3. CLI ENTRYPOINT
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Beats Music High-Speed Scraper & Artist Ingestion")
    parser.add_argument(
        "--artist",
        type=str,
        default=None,
        help="Specific artist or band to scrape (e.g. 'Vedan', 'Agam')",
    )
    parser.add_argument(
        "--genre",
        choices=["tamil", "malayalam", "hindi", "mix", "all"],
        default=None,
        help="Target music genre",
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=3,
        help="Number of listing pages to crawl (default: 3)",
    )
    parser.add_argument(
        "--proxy",
        type=str,
        default=None,
        help="Optional HTTP/HTTPS/SOCKS proxy",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=15,
        help="Limit of songs to extract per artist (default: 15)",
    )

    args = parser.parse_args()
    supabase = get_supabase_client()
    if not supabase:
        print("[!] Notice: Supabase credentials not found in .env. Running in dry-run mode.")

    # Mode 1: Ingest Specific Artist / Band
    if args.artist:
        scrape_artist_songs(
            artist_name=args.artist,
            limit=args.limit,
            supabase=supabase,
            proxy=args.proxy,
        )
        return

    # Mode 2: Async Genre Crawl
    target_genre = args.genre or "tamil"
    target_genres = list(SOURCES.keys()) if target_genre == "all" else [target_genre]

    async def run_all():
        total = 0
        for g in target_genres:
            cnt = await async_crawl_genre(
                genre=g,
                pages=args.pages,
                supabase=supabase,
                proxy=args.proxy,
            )
            total += cnt
        print(f"\n[Done] Finished all crawls. Total songs: {total}")

    asyncio.run(run_all())


if __name__ == "__main__":
    main()
