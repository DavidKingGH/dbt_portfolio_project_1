with source as (
SELECT *
FROM {{ source('ga4_ecommerce_data', 'fact_event_items')}}
),

renamed_and_casted as (
select
	event_id,
	user_pseudo_id as user_uid,
	event_ts as event_timestamp,
	CAST(strptime(event_date,'%Y%m%d') AS date) as event_date,
	item_id,
	item_name,
	quantity,
	price,
	price_in_usd,
	item_revenue_in_usd,
	item_revenue,
	coupon,
	affiliation,
	location_id,
	item_list_id,
	item_list_name,
	item_list_index,
	promotion_id,
	promotion_name,
	creative_name,
	creative_slot,
	loaded_at
from source
)

select *
from renamed_and_casted