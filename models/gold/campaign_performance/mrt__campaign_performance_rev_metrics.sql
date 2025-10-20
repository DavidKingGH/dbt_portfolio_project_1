with user_sessions as (
    select
        ga_session_id,
        user_id,
        -- handle missing/placeholder values
        lower(coalesce(nullif(trim(s.session_source), ''), '(not set)')) as session_source,
        lower(coalesce(nullif(trim(s.session_medium), ''), '(not set)')) as session_medium
    from
        {{ ref('stg__user_sessions') }} as s
),

orders as (
  -- one row per order. requires an order identifier; adapt column names accordingly.
  select
      ga_session_id,
      purchase_id,
      sum(purchase_revenue) as purchase_revenue
  from {{ ref('stg__conversion_events') }}
  group by ga_session_id, purchase_id
),

session_revenue as (
  select
      ga_session_id,
      sum(purchase_revenue) as session_revenue,
      count(*) as order_count
  from orders
  group by ga_session_id
),

joined as (
  select
      s.user_id,
      s.session_source,
      s.session_medium,
      coalesce(sr.session_revenue, 0) as session_revenue,
      coalesce(sr.order_count, 0)     as order_count
  from user_sessions s
  left join session_revenue sr using (ga_session_id)
)

-- aggregate to the channel level
select
    session_source::varchar as session_source,
    session_medium::varchar as session_medium,
    coalesce(sum(session_revenue), 0)::decimal as total_revenue,
    count(distinct user_id)::int as total_users,
    count(distinct case when session_revenue > 0 then user_id end)::int as converting_users,
    (case when sum(order_count) > 0 
          then sum(session_revenue) / nullif(sum(order_count),0)
          else 0 
    end)::decimal as average_order_value
from
    joined
group by
    session_source,
    session_medium