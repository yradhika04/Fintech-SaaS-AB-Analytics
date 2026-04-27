with source as (
    select *
    from {{ source('fintech_saas', 'subscriptions') }}
)
select user_id,
       variant,
       plan,
       mrr,
       sub_start_date,
       churned,
       churn_date
from source