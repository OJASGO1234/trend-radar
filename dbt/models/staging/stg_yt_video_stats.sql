-- One row per video per snapshot day. like_count is NULL when the channel hides likes.
select
    video_id || '|' || snapshot_date::text  as video_day_key,   -- surrogate key for the tests
    video_id,
    snapshot_date,
    view_count,
    like_count,
    comment_count,
    fetched_at
from {{ source('raw', 'yt_video_stats') }}
