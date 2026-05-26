with subscriptions as (
    select * from {{ ref('stg_fintech_saas__subscriptions') }}
)
select user_id,
       variant,
       plan,
       mrr,
       churned,
       churn_date,
       sub_start_date,
       date_diff(date(churn_date), date(sub_start_date), day) as days_to_churn,
       case
           when churned then mrr * date_diff(date(churn_date), date(sub_start_date), month)
           else mrr * date_diff(current_date, date(sub_start_date), month)
       end as ltv
from subscriptions