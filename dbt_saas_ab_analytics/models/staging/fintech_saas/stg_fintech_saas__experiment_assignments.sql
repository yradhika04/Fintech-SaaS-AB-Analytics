with source as (
    select *
    from {{ source('fintech_saas', 'experiment_assignments') }}
)
select user_id,
       experiment_name,
       variant,
       assigned_at
from source