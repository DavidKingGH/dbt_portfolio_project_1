with base as (
select *
from {{ ref('mrt_user_device_segments') }}
),

violations as (
  -- for any device bucket, a Q4 (supposedly low) user must not outspend any Q1 (supposedly top) user
  select b.user_id as q4_user_id, a.user_id as q1_user_id, a.dominant_device
  from seg a
  join seg b
    on a.dominant_device = b.dominant_device
   and a.cart_value_quartile = 1
   and b.cart_value_quartile = 4
   and b.total_revenue > a.total_revenue
)

select * from violations