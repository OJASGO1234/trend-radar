-- Spike detector. Grain: one row per hour per topic_group (EVERY hour, including quiet ones).
--
-- For each hour we compare the mention count with the previous 24 hours:
--     z = (count - trailing_mean) / trailing_stddev
-- "How many standard deviations above normal is this hour?"
-- is_spike = z >= 3 AND count >= 10  (the count floor stops tiny absolute numbers from firing)
-- We also require >= 6 hours of history: a mean/stddev from 1-2 points is meaningless.
with bounds as (
    select topic_group, min(event_hour) as first_hour, max(event_hour) as last_hour
    from {{ ref('stg_stream_events') }}
    group by topic_group
),

-- one row for every hour between first and last, so quiet hours count as 0 in the mean/stddev
-- (without this, hours with no mentions would silently vanish and the "normal" would look too high)
spine as (
    select b.topic_group, h as event_hour
    from bounds b
    cross join lateral generate_series(b.first_hour, b.last_hour, interval '1 hour') as h
),

counts as (
    select topic_group, event_hour, count(*) as mentions
    from {{ ref('stg_stream_events') }}
    group by topic_group, event_hour
),

hourly as (
    select s.topic_group, s.event_hour, coalesce(c.mentions, 0) as mentions
    from spine s
    left join counts c using (topic_group, event_hour)
),

windowed as (
    select
        *,
        avg(mentions)         over w as trailing_mean,
        stddev_samp(mentions) over w as trailing_stddev,
        count(*)              over w as history_hours
    from hourly
    -- the 24 rows BEFORE this one (excludes the current hour, or a spike would hide inside its own average)
    window w as (partition by topic_group order by event_hour rows between 24 preceding and 1 preceding)
),

scored as (
    select
        *,
        case when trailing_stddev > 0
             then (mentions - trailing_mean) / trailing_stddev end as z_score
    from windowed
)

select
    topic_group || '|' || event_hour                    as topic_hour_key,
    topic_group,
    event_hour,
    mentions,
    round(trailing_mean, 2)                             as trailing_mean,
    round(trailing_stddev, 2)                           as trailing_stddev,
    history_hours,
    round(z_score, 2)                                   as z_score,
    coalesce(history_hours >= 6 and z_score >= 3 and mentions >= 10, false) as is_spike
from scored
