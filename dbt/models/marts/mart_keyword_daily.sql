-- Grain: one row per day per topic_group per keyword.
-- matched_keywords is an array, so unnest() turns one event into one row per keyword.
select
    event_date || '|' || topic_group || '|' || keyword  as day_topic_keyword_key,
    event_date,
    topic_group,
    keyword,
    count(*)                                            as mentions
from {{ ref('stg_stream_events') }}
cross join lateral unnest(matched_keywords) as keyword
group by event_date, topic_group, keyword
