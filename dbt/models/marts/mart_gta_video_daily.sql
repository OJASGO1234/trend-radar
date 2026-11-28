-- Grain: one row per video per snapshot day.
-- views_gained = today's views minus the previous snapshot's views (LAG).
-- The first snapshot of a video has no previous one, so views_gained is NULL there.
select
    s.video_day_key,
    s.video_id,
    v.title,
    v.channel_title,
    v.is_short,
    s.snapshot_date,
    s.view_count                                                        as views,
    s.like_count                                                        as likes,
    s.comment_count                                                     as comments,
    s.view_count - lag(s.view_count) over w                             as views_gained,
    s.snapshot_date - lag(s.snapshot_date) over w                       as days_since_prev,   -- >1 = a missed collector day
    round(s.like_count::numeric / nullif(s.view_count, 0), 5)           as engagement_rate    -- likes / views
from {{ ref('stg_yt_video_stats') }} s
join {{ ref('stg_yt_videos') }} v using (video_id)
window w as (partition by s.video_id order by s.snapshot_date)
