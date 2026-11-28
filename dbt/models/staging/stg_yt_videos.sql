-- One row per video. Light cleaning only: no business logic here.
select
    video_id,
    {{ clean_text('title') }}                       as title,
    channel_id,
    {{ clean_text('channel_title') }}               as channel_title,
    published_at,
    published_at::date                              as published_date,
    source_query,
    first_seen_at,
    -- YouTube Shorts are marked with the #shorts hashtag in the title
    (title ilike '%#shorts%')                       as is_short
from {{ source('raw', 'yt_videos') }}
