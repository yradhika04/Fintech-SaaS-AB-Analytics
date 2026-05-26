with notification_counts as (
    select
        user_id,
        sum(case when event_name='notification_sent' then 1 else 0 end) as notifications_sent,
        sum(case when event_name='notification_clicked' then 1 else 0 end) as notifications_clicked
    from {{ ref('stg_fintech_saas__events') }}
    group by user_id
)
select u.user_id,
       u.variant,
       u.did_first_transaction, -- primary metric
       u.did_engage_feature, --secondary metric
       u.country,
       u.device,
       u.acquisition_channel,
       n.notifications_sent,
       n.notifications_clicked
from {{ ref('mart_core__user_journey') }} u
left join notification_counts n
on u.user_id = n.user_id