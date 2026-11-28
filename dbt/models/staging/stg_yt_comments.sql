select
    comment_id,
    video_id,
    {{ clean_text('text') }}                        as text_clean,
    like_count,
    published_at,
    published_at::date                              as published_date,
    fetched_at
from {{ source('raw', 'yt_comments') }}
