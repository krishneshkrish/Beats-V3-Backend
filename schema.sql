-- ══════════════════════════════════════════════════════════
-- Beats V3 — Supabase Clean Database Schema
-- Run this script in the Supabase SQL Editor
-- ══════════════════════════════════════════════════════════

-- 1. Songs Table
create table if not exists songs (
  id uuid primary key default gen_random_uuid(),
  title text not null,
  artist text not null,
  film text,
  genre text not null,
  language text,
  duration integer default 0,
  thumbnail_url text,
  source_url text,
  source_site text,
  year integer,
  play_count bigint default 0,
  search_count bigint default 0,
  status text default 'available',
  scraped_at timestamptz default now()
);

-- status values:
-- 'available'   → has source_url, can stream
-- 'unavailable' → no source_url

-- 2. Users Table
create table if not exists users (
  id text primary key,
  name text,
  taste_profile text,
  play_count integer default 0,
  created_at timestamptz default now(),
  last_seen timestamptz default now()
);

-- 3. Play Events Table
create table if not exists play_events (
  id uuid primary key default gen_random_uuid(),
  user_id text not null,
  song_id uuid references songs(id) on delete set null,
  played_at timestamptz default now(),
  hour_of_day integer,
  day_of_week integer,
  genre text,
  completed boolean default false,
  skipped_at_pct float,
  source text default 'browse'
);

-- 4. Indexes
create index if not exists idx_songs_genre on songs(genre);
create index if not exists idx_songs_status on songs(status);
create index if not exists idx_songs_play_count on songs(play_count desc);
create unique index if not exists idx_songs_source_url on songs(source_url);
create index if not exists idx_songs_search on songs using gin(
  to_tsvector('english',
    coalesce(title,'') || ' ' ||
    coalesce(artist,'') || ' ' ||
    coalesce(film,'')
  )
);

create index if not exists idx_play_events_user on play_events(user_id);
create index if not exists idx_play_events_song on play_events(song_id);
create index if not exists idx_play_events_time on play_events(played_at desc);

-- 5. Stored Procedures
create or replace function increment_play_count(p_song_id uuid)
returns void as $$
  update songs
  set play_count = play_count + 1
  where id = p_song_id;
$$ language sql;

create or replace function increment_search_count(p_song_id uuid)
returns void as $$
  update songs
  set search_count = search_count + 1
  where id = p_song_id;
$$ language sql;
