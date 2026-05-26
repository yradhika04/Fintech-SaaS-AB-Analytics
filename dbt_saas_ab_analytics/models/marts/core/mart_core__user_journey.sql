with users as (
    select * from {{ ref('stg_fintech_saas__users') }}
),
experiment as (
    select * from {{ ref('stg_fintech_saas__experiment_assignments') }}
),
events as (
    select * from {{ ref('stg_fintech_saas__events') }}
),
funnel as (
    select
        user_id,
        -- add completion flags for all funnel steps
        max(case when event_name = 'email_verified' then 1 else 0 end) as did_verify_email,
        max(case when event_name = 'profile_completed' then 1 else 0 end) as did_complete_profile,
        max(case when event_name = 'kyc_submitted' then 1 else 0 end) as did_submit_kyc,
        max(case when event_name = 'kyc_approved' then 1 else 0 end) as did_get_kyc_approved,
        max(case when event_name = 'payment_method_added' then 1 else 0 end) as did_add_payment,
        max(case when event_name = 'first_transaction' then 1 else 0 end) as did_first_transaction,
        max(case when event_name = 'feature_engaged' then 1 else 0 end) as did_engage_feature,

        -- timestamps of each funnel step (null if that step was never reached)
        max(case when event_name = 'email_verified' then event_ts end) as email_verified_at,
        max(case when event_name = 'profile_completed' then event_ts end) as profile_completed_at,
        max(case when event_name = 'kyc_submitted' then event_ts end) as kyc_submitted_at,
        max(case when event_name = 'kyc_approved' then event_ts end) as kyc_approved_at,
        max(case when event_name = 'payment_method_added' then event_ts end) as payment_added_at,
        max(case when event_name = 'first_transaction' then event_ts end) as first_transaction_at,
        max(case when event_name = 'feature_engaged' then event_ts end) as feature_engaged_at

    from events
    group by user_id
)
select
    u.user_id,
    e.variant,
    u.country,
    u.device,
    u.acquisition_channel,
    u.age,
    u.signup_date,
    f.did_verify_email,
    f.did_complete_profile,
    f.did_submit_kyc,
    f.did_get_kyc_approved,
    f.did_add_payment,
    f.did_first_transaction, -- if a user reached first_transaction then they are considered 'converted'
    f.did_engage_feature,
    -- days from signup to each funnel step (null if that step was never reached)
    date_diff(date(f.email_verified_at), date(u.signup_date), day) as days_to_email_verified,
    date_diff(date(f.profile_completed_at), date(u.signup_date), day) as days_to_profile_completed,
    date_diff(date(f.kyc_submitted_at), date(u.signup_date), day) as days_to_kyc_submitted,
    date_diff(date(f.kyc_approved_at), date(u.signup_date), day) as days_to_kyc_approved,
    date_diff(date(f.payment_added_at), date(u.signup_date), day) as days_to_payment_added,
    date_diff(date(f.first_transaction_at), date(u.signup_date), day) as days_to_first_transaction,
    date_diff(date(f.feature_engaged_at), date(u.signup_date), day) as days_to_feature_engaged
from users u
join experiment e on u.user_id = e.user_id
join funnel f on u.user_id = f.user_id