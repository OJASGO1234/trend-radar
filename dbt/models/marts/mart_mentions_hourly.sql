-- Grain: one row per hour per topic_group per source (only hours that had mentions).
select
    event_hour || '|' || topic_group || '|' || source   as hour_topic_source_key,
    event_hour,
    topic_group,
    source,
    count(*)                                            as mentions
from {{ ref('stg_stream_events') }}
group by event_hour, topic_group, source
