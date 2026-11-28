-- One row per streamed event. Adds the cleaned text, the reply flag and the hour bucket.
select
    source || ':' || event_id                                   as event_key,   -- PK is (source, event_id)
    source,
    event_id,
    event_time,
    date_trunc('hour', event_time, 'UTC')                       as event_hour,  -- bucket for hourly marts
    (event_time at time zone 'UTC')::date                       as event_date,
    {{ clean_text('text') }}                                    as text_clean,  -- newlines/tabs -> one space
    url,
    matched_keywords,
    topic_group,
    -- only Bluesky carries a reply flag (payload.raw.reply); for other sources it is false
    coalesce(source = 'bluesky' and (payload -> 'raw' ->> 'reply')::boolean, false) as is_reply,
    ingested_at
from {{ source('raw', 'stream_events') }}
