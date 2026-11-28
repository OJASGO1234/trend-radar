-- Singular test: it FAILS if this query returns any row.
-- A video's view count should only go up. YouTube does make small downward corrections
-- (it removes spam/bot views), so we tolerate a drop of up to 100 views OR 0.5% of the
-- previous day's views, whichever is larger. A bigger drop means bad data (a collector bug,
-- wrong video_id, a half-written snapshot) and should stop the pipeline.
select
    video_id,
    snapshot_date,
    views,
    views_gained
from {{ ref('mart_gta_video_daily') }}
where views_gained < -greatest(100, 0.005 * (views - views_gained))
