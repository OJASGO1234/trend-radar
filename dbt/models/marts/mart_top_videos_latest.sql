-- Grain: one row per video, from its LATEST snapshot, ranked by views.
with latest as (
    select distinct on (video_id)      -- keeps the first row per video_id in the ORDER BY below
        video_id, snapshot_date, view_count, like_count, comment_count
    from {{ ref('stg_yt_video_stats') }}
    order by video_id, snapshot_date desc
)
select
    l.video_id,
    v.title,
    v.channel_title,
    v.is_short,
    v.published_at,
    l.snapshot_date                                         as latest_snapshot_date,
    l.view_count                                            as views,
    l.like_count                                            as likes,
    l.comment_count                                         as comments,
    round(l.like_count::numeric / nullif(l.view_count, 0), 5) as engagement_rate,
    rank() over (order by l.view_count desc nulls last)     as views_rank
from latest l
join {{ ref('stg_yt_videos') }} v using (video_id)
