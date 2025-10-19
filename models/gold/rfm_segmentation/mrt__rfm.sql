WITH

base as (

SELECT 
    event_id as purchase_id,
    user_id,
    order_date as purchase_date, 
    revenue
FROM {{ ref('stg__user_orders')}}

), 

dataset_max_date as (

select 
    max(purchase_date) as as_of_date
from base

),

last_purchase_date as (

select
    user_id,
    max(purchase_date) as max_purchase_date
from base
group by 1
), 

win_365 as (

select
    b.user_id,
    b.purchase_id,
    b.revenue,
    d.as_of_date
from base b
cross join dataset_max_date d
where b.purchase_date >  d.as_of_date - interval '{{ var("rfm_window", "365") }}' day
and b.purchase_date <= d.as_of_date

),

rfm as (
select
    lpd.user_id, 
    -- recency = days since last purchase (from all history)
    date_diff('day', lpd.max_purchase_date, md.as_of_date)::int as recency, 
    -- frequency/monetary = windowed metrics
    COUNT(distinct w.purchase_id)::int as frequency,
    COALESCE(SUM(w.revenue)::decimal,0.0) as monetary,
    md.as_of_date::date as as_of_date,
    lpd.max_purchase_date::date as max_purchase_date
from last_purchase_date lpd
cross join dataset_max_date md
left join win_365 w using (user_id, as_of_date)
group by 1,2,5,6
)

select *
from rfm