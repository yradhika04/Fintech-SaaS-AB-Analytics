with source as (
    select *
    from {{ source('fintech_saas', 'users') }}
)
select user_id,
       signup_date,
       country,
       device,
       acquisition_channel,
       age
from source