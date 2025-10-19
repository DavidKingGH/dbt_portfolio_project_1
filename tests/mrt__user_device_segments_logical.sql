with base as (
select *
from {{ ref('mrt__user_device_segments') }}
),

seg_a as (

select 
  user_id, 
  dominant_device,
  spend_quartile, 
  total_revenue
from base
), 

seg_b as (
select 
  user_id, 
  dominant_device,
  spend_quartile, 
  total_revenue
from base
),

violations as (
  -- for any device bucket, a Q4 (supposedly low) user must not outspend any Q1 (supposedly top) user
  select b.user_id as q4_user_id, a.user_id as q1_user_id, a.dominant_device
  from seg_a a
  join seg_b b
    on a.dominant_device = b.dominant_device
   and a.spend_quartile = 1
   and b.spend_quartile = 4
   and b.total_revenue > a.total_revenue
)

select * from violations