with 

max_date as (

select max(event_timestamp) as max_date
from {{ ref('fct_events')}}
),

-- events from last 30 days
events_last_30d as (

select *
from {{ ref('fct_events')}} e
cross join max_date md
where e.event_date between (md.max_date - interval '29' day) and md.max_date
),

-- first-ever *date* each user appeared
users_first_event as (

select 
    user_id,
    min(event_timestamp) as first_event_date
from {{ ref('fct_events') }}
group by 1

),

-- attach “new vs returning” label based on first_event_date
events_labeled_30d as (

select el.*,
        case
            when date(ufe.first_event_date) >= (md.max_date - interval '29' day) then 'new'
            else 'returning'
        end as customer_status
from events_last_30d el
join users_first_event ufe using(user_id) 
cross join max_date md
),

kpi_summary as (

select 

    max(max_date) as max_date,
    user_id,
   -- browse-to-buy counts
    count(distinct case when event_name = 'purchase' then user_id end) as purchasers_overall,
    count(distinct case when event_name = 'view_item' then user_id end) as browsers_overall,
    count(distinct case when event_name = 'purchase' and customer_status = 'new' then user_id end) as purchasers_new,
    count(distinct case when event_name = 'view_item' and customer_status = 'new' then user_id end) as browsers_new,
    count(distinct case when event_name = 'purchase' and customer_status = 'returning' then user_id end) as purchasers_returning,
    count(distinct case when event_name = 'view_item' and customer_status = 'returning' then user_id end) as browsers_returning,    
    
    -- high-intent users
    count(distinct case when event_name = 'begin_checkout' then user_id end) as high_intent_users_overall,

    -- counts for drop-off calculation
    count(distinct case when event_name = 'add_to_cart' then user_id end) as users_add_to_cart,
    count(distinct case when event_name = 'begin_checkout' then user_id end) as users_begin_checkout,
    count(distinct case when event_name = 'add_shipping_info' then user_id end) as users_add_shipping_info,
    count(distinct case when event_name = 'add_payment_info' then user_id end) as users_add_payment_info,
    count(distinct case when event_name = 'purchase' then user_id end) as users_purchased

from events_labeled_30d
)

select
    max_date::date as as_of_date,
    user_id
    --browse-to-buy rates
    (purchasers_overall::double) / nullif(browsers_overall,0) as browse_to_buy_overall,
    (purchasers_new::double) / nullif(browsers_new,0) as browse_to_buy_new,
    (purchasers_returning::double) / nullif(browsers_returning,0) as browse_to_buy_returning,

    -- high-intent users
    high_intent_users_overall::int as high_intent_users_overall,

    -- sequential conversion rates

    (users_begin_checkout::double) / nullif(users_add_to_cart,0) as rate_cart_to_checkout,
    (users_add_shipping_info::double) / nullif(users_begin_checkout,0) as rate_checkout_to_shipping, 
    (users_add_payment_info::double) / nullif(users_add_shipping_info,0) as rate_shipping_to_payment, 

    -- specific drop-off rate for shipping step (1 - success rate) 
    1 - (users_add_payment_info::double) / nullif(users_add_shipping_info,0) as dropoff_rate_at_shipping
from kpi_summary