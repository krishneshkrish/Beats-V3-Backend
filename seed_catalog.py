import os
import sys
import base64
import html
import requests
import json
from dotenv import load_dotenv
from supabase import create_client
from Crypto.Cipher import DES

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("[X] Missing Supabase credentials")
    sys.exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def decrypt_saavn_url(enc_url: str) -> str:
    cipher = DES.new(b"38346591", DES.MODE_ECB)
    raw = cipher.decrypt(base64.b64decode(enc_url.strip()))
    pad_len = raw[-1]
    if 0 < pad_len <= 8:
        raw = raw[:-pad_len]
    url = raw.decode("utf-8", errors="ignore")
    return url.replace("_96.mp4", "_320.mp4")

QUERIES_BY_GENRE = {
    "tamil": [
        "Hukum Anirudh",
        "Badass Leo Anirudh",
        "Naa Ready Thalapathy Vijay",
        "Arabic Kuthu Beast",
        "Kaavaalaa Jailer",
        "Vikram Title Track Anirudh",
        "Jalabulajangu Don",
        "Hayoda Jawan Anirudh",
        "Dheera Dheera KGF",
        "Rowdy Baby Dhanush",
        "Enjoy Enjaami Dhee",
        "Vaathi Coming Master",
        "Kutty Story Thalapathy",
        "Chellamma Doctor Anirudh",
        "Private Party Don Anirudh",
        "Megham Karukatha Thiruchitrambalam",
        "Chilla Chilla Thunivu Anirudh",
        "Oo Solriya Oo Oo Solriya Pushpa",
        "Arabic Kuthu Anirudh",
        "Aalaporan Thamizhan Mersal"
    ],
    "hindi": [
        "Chaleya Jawan Arijit Singh",
        "Kesariya Brahmastra Arijit Singh",
        "Apna Bana Le Bhediya",
        "Jhoome Jo Pathaan Arijit",
        "Tum Hi Ho Aashiqui 2",
        "Raataan Lambiyan Shershaah",
        "Pyaar Hota Kayi Baar Hai",
        "Ghungroo War Arijit Singh",
        "Shayad Love Aaj Kal",
        "Tere Pyaar Mein Tu Jhoothi",
        "Bekhayali Kabir Singh",
        "Kabira Yeh Jawaani Hai Deewani",
        "Ilahi Ranbir Kapoor",
        "Kalank Title Track Arijit",
        "Lutt Putt Gaya Dunki Arijit"
    ],
    "english": [
        "Starboy The Weeknd",
        "Blinding Lights The Weeknd",
        "Save Your Tears The Weeknd",
        "Sunflower Post Malone Swae Lee",
        "As It Was Harry Styles",
        "Stay The Kid LAROI Justin Bieber",
        "Levitating Dua Lipa",
        "Heat Waves Glass Animals",
        "Shape of You Ed Sheeran",
        "Believer Imagine Dragons",
        "One Dance Drake",
        "Die For You The Weeknd"
    ],
    "phonk": [
        "METAMORPHOSIS INTERWORLD",
        "Murder In My Mind KORDHELL",
        "RAVE Dxrk ダーク",
        "Midnight PLAYAMANE",
        "Montagem Mysterious Game",
        "AUTOMOTIVO BIBI FOGOSA",
        "Close Eyes DVRST",
        "Live Another Day KORDHELL",
        "Scopin KSLV Noh",
        "Disaster KSLV"
    ]
}

def seed():
    print("=== Starting Supabase Catalog Seeding ===")
    total_added = 0

    for genre, queries in QUERIES_BY_GENRE.items():
        print(f"\n[*] Processing genre: {genre.upper()} ({len(queries)} tracks)...")
        for q in queries:
            try:
                url = f"https://www.jiosaavn.com/api.php?__call=search.getResults&_format=json&_marker=0&n=1&p=1&q={requests.utils.quote(q)}"
                r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=6)
                data = json.loads(r.text.strip())
                results = data.get("results", [])
                if not results:
                    print(f"  [!] No result for '{q}'")
                    continue
                top = results[0]
                enc = top.get("encrypted_media_url")
                if not enc:
                    continue

                raw_title = html.unescape(top.get("song") or top.get("title") or q)
                title = raw_title.replace("&quot;", '"').replace("&#039;", "'").strip()
                raw_artist = html.unescape(top.get("singers") or top.get("primary_artists") or "Various")
                artist = raw_artist.replace("&quot;", '"').replace("&#039;", "'").strip()
                raw_film = html.unescape(top.get("album") or "")
                film = raw_film.replace("&quot;", '"').replace("&#039;", "'").strip()
                
                thumb = (top.get("image") or "").replace("150x150", "500x500")
                duration = int(top.get("duration") or 180)
                stream_url = decrypt_saavn_url(enc)

                record = {
                    "title": title,
                    "artist": artist,
                    "film": film,
                    "genre": genre,
                    "language": top.get("language") or genre.capitalize(),
                    "duration": duration,
                    "thumbnail_url": thumb,
                    "source_url": stream_url,
                    "source_site": "jiosaavn_cdn",
                    "play_count": int(top.get("play_count") or 1000) if str(top.get("play_count") or "").isdigit() else 1000,
                    "status": "available"
                }

                # Upsert / check duplicate by title + artist
                check = supabase.table("songs").select("id").eq("title", title).eq("genre", genre).execute()
                if not check.data:
                    supabase.table("songs").insert(record).execute()
                    print(f"  [+] Added: {title} by {artist} ({genre})")
                    total_added += 1
                else:
                    print(f"  [=] Exists: {title}")

            except Exception as e:
                print(f"  [X] Error on '{q}': {e}")

    print(f"\n[DONE] Seeding complete! Successfully added {total_added} new verified tracks.")

if __name__ == "__main__":
    seed()
