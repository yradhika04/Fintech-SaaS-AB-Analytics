with source as (
    select *
    from {{ source('fintech_saas', 'events') }}
),
deduped_events as (
    select *
    from source
    qualify row_number() over (partition by event_id order by event_ts) = 1
)
select event_id,
       user_id,
       event_name,
       event_ts,
       date(event_ts) as event_date,
       variant
from deduped_events