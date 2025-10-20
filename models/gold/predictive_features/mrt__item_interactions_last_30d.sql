with 

report_date as (

select 
    max(event_timestamp) as as_of_timestamp
from {{ ref('fct_events')}}
),


dim_items_deduplicated as (

select * from (
    select *, row_number() over(partition by item_id order by item_category) as rn
    from {{ ref('dim_items') }}
) x where rn = 1

),

events_last_30d as (
select fe.*
from {{ ref('fct_events')}} fe
cross join report_date a 
where fe.event_timestamp <= a.as_of_timestamp
    and fe.event_timestamp >= a.as_of_timestamp - interval '29' day
),

item_interactions_last_30d as (
select
    fe.user_id, 
    fe.ga_session_id,
    fe.event_id,
    fe.event_name, 
    fe.event_timestamp, 
    bfei.item_id, 
    bfei.price, 
    bfei.quantity,
    coalesce(did.item_category, 'unknown') as item_category
from events_last_30d fe
join {{ ref('fct_event_items')}} bfei using(event_id)
left join dim_items_deduplicated did on did.item_id = bfei.item_id
),

sessions_with_purchase_30d as (
  select distinct ga_session_id
  from events_last_30d
  where event_name = 'purchase'
),

last_item_category as (
  select
    user_id,
    item_category as last_carted_category_last_30d,
    row_number() over (partition by user_id order by event_timestamp desc, event_id desc) as rn
  from item_interactions_last_30d
  where event_name = 'add_to_cart'
),

user_product_interest_features_last_30d as (
select 
    user_id, 

    -- counts 
    count(distinct case when event_name = 'add_to_cart' then item_id end) as items_carted,
    count(distinct case when event_name = 'view_item' then item_id end) as items_viewed,
    count(distinct case when event_name = 'view_item' then item_category end) as item_category_viewed,
    
    -- abandoned cart value: 
     sum(
        case when ii.event_name = 'add_to_cart' and sp.ga_session_id is null
        then coalesce(ii.price,0) * coalesce(ii.quantity,1) -- assume quantity is 1 if null
        else 0
        end
    ) as abandoned_cart_value_last_30d,

    -- avg price of items carted
      avg(case when event_name = 'add_to_cart' then price end) as avg_price_of_items_carted_last_30d


from item_interactions_last_30d ii
left join sessions_with_purchase_30d sp on ii.ga_session_id = sp.ga_session_id
group by user_id
),

final as (

select
    a.as_of_timestamp::date as as_of_date,
    du.user_id::varchar as user_id,
    case when pf.items_viewed > 0
         then (pf.items_carted::double) / pf.items_viewed
         else null 
    end as cart_to_view_ratio_last_30d, 
    pf.abandoned_cart_value_last_30d::decimal as abandoned_cart_value_last_30d,
    pf.avg_price_of_items_carted_last_30d::decimal as avg_price_of_items_carted_last_30d,
    ic.last_carted_category_last_30d::varchar as last_carted_category_last_30d,
    (case when pf.item_category_viewed > 0
         then (pf.items_viewed::double) / item_category_viewed 
         else null 
    end)::double as views_per_category_last_30d
from {{ ref('dim_users') }} du
cross join report_date a
left join user_product_interest_features_last_30d pf on du.user_id = pf.user_id 
left join last_item_category ic on du.user_id = ic.user_id 
and ic.rn = 1
)

select * from final