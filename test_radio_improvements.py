import asyncio
import pytest
from saavn_resolver import (
    infer_mood,
    infer_energy_level,
    infer_vocal_type,
    infer_song_features,
    similarity_score,
    apply_energy_arc,
    get_primary_artist,
    build_ytmusic_mood_queue,
)

def test_infer_mood():
    assert infer_mood("Tum Hi Ho (Aashiqui 2)", "Arijit Singh", "hindi") == "romantic"
    assert infer_mood("Daku Party Remix", "Inderpal Moga", "punjabi") == "energetic"
    assert infer_mood("Channa Mereya Sad Version", "Arijit Singh", "hindi") == "sad"
    assert infer_mood("Interstellar Main Theme OST", "Hans Zimmer", "english") == "cinematic"
    assert infer_mood("Midnight Chill Lofi Beats", "Kavinsky", "english") == "chill"
    assert infer_mood("Demon Slayer Dark Phonk", "KSLV", "phonk") == "dark"
    assert infer_mood("Ordinary Song", "Unknown", "mix") == "neutral"

def test_infer_energy_level():
    assert infer_energy_level("Gym Workout Beat", mood="energetic") == "high"
    assert infer_energy_level("Slow Acoustic Lofi", mood="chill") == "low"
    assert infer_energy_level("Romantic Melody", mood="romantic") == "medium"

def test_infer_vocal_type():
    assert infer_vocal_type("Vikram Title BGM Instrumental") == "instrumental"
    assert infer_vocal_type("Arabic Kuthu", "Anirudh Ravichander") == "vocal"

def test_similarity_score():
    seed = {
        "title": "Kesariya Love Anthem",
        "artist": "Arijit Singh",
        "genre": "hindi",
        "language": "hindi",
        "mood": "romantic",
        "energy_level": "medium",
        "vocal_type": "vocal"
    }

    # Candidate 1: Perfect match (all 4 match -> 0.4 + 0.3 + 0.2 + 0.1 = 1.0)
    cand_perfect = {
        "title": "Raataan Lambiyan Romance",
        "artist": "Jubin Nautiyal",
        "genre": "hindi",
        "language": "hindi",
        "mood": "romantic",
        "energy_level": "medium",
        "vocal_type": "vocal"
    }
    score1 = similarity_score(seed, cand_perfect)
    assert score1 == 1.0
    assert cand_perfect["similarity_score"] == 1.0

    # Candidate 2: Partial match (mood + vocal + lang match, different energy -> 0.4 + 0.2 + 0.1 = 0.7)
    cand_partial = {
        "title": "Love Blast High Energy",
        "artist": "Armaan Malik",
        "genre": "hindi",
        "language": "hindi",
        "mood": "romantic",
        "energy_level": "high",
        "vocal_type": "vocal"
    }
    score2 = similarity_score(seed, cand_partial)
    assert score2 == 0.7

    # Candidate 3: Total mismatch (0.0)
    cand_mismatch = {
        "title": "Dark Demon BGM",
        "artist": "Hans Zimmer",
        "genre": "english",
        "language": "english",
        "mood": "dark",
        "energy_level": "high",
        "vocal_type": "instrumental"
    }
    score3 = similarity_score(seed, cand_mismatch)
    assert score3 == 0.0

def test_apply_energy_arc_and_artist_diversity():
    # Build 20 scored songs with different artists and scores
    songs = []
    # 4 Anirudh songs, 3 Arijit songs, and distinct other artists
    artists = [
        "Anirudh Ravichander", "Anirudh Ravichander", "Anirudh Ravichander", "Anirudh Ravichander",
        "Arijit Singh", "Arijit Singh", "Arijit Singh",
        "Sid Sriram", "Shreya Ghoshal", "AR Rahman",
        "Yuvan Shankar Raja", "Harris Jayaraj", "Santhosh Narayanan",
        "Sushin Shyam", "Jakes Bejoy", "G.V. Prakash",
        "Devi Sri Prasad", "Thaman S", "Karthik", "Vijay Prakash"
    ]

    for idx, art in enumerate(artists):
        # High similarity for top songs, degrading to lower
        sim = 1.0 - (idx * 0.04)
        songs.append({
            "id": f"song-id-{idx}",
            "title": f"Song {idx} by {art}",
            "artist": art,
            "similarity_score": round(sim, 2),
            "genre": "tamil"
        })

    arranged = apply_energy_arc(songs, limit=15)
    assert len(arranged) == 15

    # Check Artist Diversity (CHANGE 6):
    # First 5 slots: max 1 song per primary artist!
    first_5_artists = [get_primary_artist(s) for s in arranged[:5]]
    assert len(first_5_artists) == len(set(first_5_artists)), f"Duplicate artist found in first 5 slots: {first_5_artists}"

    # Overall slots 1-15: max 2 songs per primary artist!
    all_artists = [get_primary_artist(s) for s in arranged]
    from collections import Counter
    counts = Counter(all_artists)
    for art, c in counts.items():
        assert c <= 2, f"Artist {art} appears {c} times (> 2 cap)!"

    # Verify high similarity songs are prioritized in positions 1-4 compared to discovery
    avg_first_4 = sum(s["similarity_score"] for s in arranged[:4]) / 4
    avg_discovery = sum(s["similarity_score"] for s in arranged[9:15]) / len(arranged[9:15])
    assert avg_first_4 > avg_discovery, f"Expected higher similarity in positions 1-4 ({avg_first_4}) than discovery ({avg_discovery})"


def test_apply_energy_arc_edge_cases():
    # Empty
    assert apply_energy_arc([], limit=15) == []
    # 1 song
    single = [{"id": "1", "artist": "A", "similarity_score": 0.9}]
    assert apply_energy_arc(single, limit=15) == single

async def test_end_to_end_radio():
    import main
    import recommender
    client = main.get_supabase_client()
    seed_res = client.table("songs").select("*").limit(1).execute()
    assert seed_res.data, "Supabase songs table has songs"
    seed = seed_res.data[0]
    print(f"[*] Running end-to-end radio test on seed: '{seed.get('title')}' by {seed.get('artist')}")

    queue = await recommender.radio_recs(
        seed_song_id=seed["id"],
        user_id="test_user_session",
        limit=15,
        supabase=client,
        redis=None,
    )

    assert len(queue) > 0, "Queue should not be empty"
    print(f"[*] Generated queue of {len(queue)} songs:")
    for i, s in enumerate(queue):
        score = s.get("similarity_score", 0.0)
        mood = s.get("mood", "N/A")
        energy = s.get("energy_level", "N/A")
        print(f"    {i+1:2d}. {s.get('title')} ({s.get('artist')}) [score={score}, mood={mood}, energy={energy}]")

    # Assert artist diversity
    first_5 = [get_primary_artist(s) for s in queue[:5]]
    assert len(first_5) == len(set(first_5)), f"Clustered artists in first 5 slots: {first_5}"


if __name__ == "__main__":
    test_infer_mood()
    print("[PASS] test_infer_mood")
    test_infer_energy_level()
    print("[PASS] test_infer_energy_level")
    test_infer_vocal_type()
    print("[PASS] test_infer_vocal_type")
    test_similarity_score()
    print("[PASS] test_similarity_score")
    test_apply_energy_arc_and_artist_diversity()
    print("[PASS] test_apply_energy_arc_and_artist_diversity")
    test_apply_energy_arc_edge_cases()
    print("[PASS] test_apply_energy_arc_edge_cases")

    asyncio.run(test_end_to_end_radio())
    print("[PASS] test_end_to_end_radio")

    print("\nALL UNIT & INTEGRATION TESTS PASSED SUCCESSFULLY!")

