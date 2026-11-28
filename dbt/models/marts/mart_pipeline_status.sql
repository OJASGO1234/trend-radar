{{ config(materialized='view') }}
-- A VIEW, not a table: it is evaluated live on every query, so the API's /api/health is never stale.
-- A Postgres view runs with its OWNER's privileges, which is how the read-only API role can see
-- these few facts without any access to the raw schema.
select
    (select max(started_at)  from {{ source('raw', 'collector_runs') }})                           as last_collector_run_at,
    (select status           from {{ source('raw', 'collector_runs') }} order by started_at desc limit 1) as last_collector_status,
    (select max(finished_at) from {{ source('raw', 'collector_runs') }} where status = 'success')   as last_collector_success_at,
    (select max(event_time)  from {{ source('raw', 'stream_events') }})                            as latest_stream_event_time,
    (select max(ingested_at) from {{ source('raw', 'stream_events') }})                            as latest_stream_ingested_at,
    (select count(*)         from {{ source('raw', 'stream_events') }})                            as stream_events_total
