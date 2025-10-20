with first_touch as (
  select
    user_id,
    session_source,
    session_medium
  from {{ ref('int__acquisition_touchpoints') }}
),

user_ltv as (
select
    user_id,
    sum(purchase_revenue) as user_ltv
from
    {{ ref('stg__conversion_events') }}
group by user_id
),

cohort as (
  select
    ft.session_source,
    ft.session_medium,
    ft.user_id,
    coalesce(ul.user_ltv, 0) as user_ltv
  from first_touch ft
  left join user_ltv ul using (user_id)
)

select
  session_source::varchar as session_source,
  session_medium::varchar as session_medium,
  count(distinct user_id)::int as acquired_customers,
  sum(user_ltv)::decimal as total_purchase_value_from_cohort,
  avg(user_ltv)::decimal as average_ltv_by_channel
from cohort
group by 1,2

