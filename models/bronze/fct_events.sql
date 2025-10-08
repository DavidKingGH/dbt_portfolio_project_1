with source as (

{% if target.name == 'prod' %}
    select * from {{ source('bronze', 'fact_events') }}
{% else %}
    select * from read_parquet('ga4_data/fct/fact_events/*.parquet')
{% endif %}

),

renamed_and_casted as (
select
	event_id,
	CAST(strptime(event_date, '%Y%m%d') AS date) as event_date,
	event_ts as event_timestamp,
	event_name,
	user_pseudo_id as user_id,
	ga_session_id,
	page_location,
	page_referrer,
	engagement_time_msec,
	gclid,
	utm_source,
	utm_medium,
	utm_campaign,
	utm_term,
	utm_content,
	transaction_id,
	event_value_in_usd,
	source,
	medium,
	campaign_name,
	continent,
	country,
	region,
	city,
	device_category,
	operating_system,
	browser,
	mobile_brand_name
from source
)

select *
from renamed_and_casted