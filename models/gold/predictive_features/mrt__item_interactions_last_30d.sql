with 

dim_items_deduplicated as (

select * from (
    select *, row_number() over(partition by item_id order by item_category) as rn
    from {{ ref('dim_items') }}
) where rn = 1

),

report_date as (

select 
    max(event_timestamp)::date as as_of_date
from {{ ref('fct_events')}}
),

item_interactions_last_30d as (
select
    bfe.user_id, 
    bfe.ga_session_id, 
    bfe.event_name, 
    bfe.event_timestamp, 
    bfei.item_id, 
    bfei.price, 
    bfei.quantity,
    coalesce(did.item_category, 'unknown') as item_category,
    a.as_of_date
from {{ ref('fct_events')}}      bfe
join {{ ref('fct_event_items')}} bfei on bfe.event_id = bfei.event_id
left join dim_items_deduplicated did on did.item_id = bfei.item_id
cross join report_date a
where bfe.event_timestamp <= a.as_of_date 
and bfe.event_timestamp >= a.as_of_date - interval '29' day
),

sessions_with_checkout_30d as (
select 
    distinct fe.ga_session_id
from {{ ref('fct_events')}} fe
cross join report_date a 
where fe.event_name = 'begin_checkout'
    and fe.event_timestamp <= a.as_of_date
    and fe.event_timestamp >= a.as_of_date - interval '29' day
),

int_last_item_category as (
  select
    user_id,
    item_category as last_carted_category_last_30d,
    row_number() over (partition by user_id order by event_timestamp desc) as rn
  from item_interactions_last_30d
  where event_name = 'add_to_cart'
),

int_user_product_interest_features_last_30d as (
select 
    max(as_of_date) as as_of_date,
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
left join sessions_with_checkout_30d sp on ii.ga_session_id = sp.ga_session_id
group by user_id
),

final as (

select
    pf.as_of_date::date as as_of_date,
    pf.user_id,
    (items_carted::double) / nullif(items_viewed,0) as cart_to_view_ratio_last_30d, 
    abandoned_cart_value_last_30d::decimal as abandoned_cart_value_last_30d,
    avg_price_of_items_carted_last_30d::decimal as avg_price_of_items_carted_last_30d,
    last_carted_category_last_30d::varchar as last_carted_category_last_30d,
    (items_viewed::double) / nullif(item_category_viewed,0) as specialist_ratio_last_30d
from int_user_product_interest_features_last_30d pf 
left join int_last_item_category ic on ic.user_id = pf.user_id 
and ic.rn = 1
)

select * from final