with source as (

{% if target.name == 'prod' %}
    select * from {{ source('bronze', 'dim_items') }}
{% else %}
    select * from read_parquet('ga4_data/dim/dim_items/*.parquet')
{% endif %}

),

renamed_and_casted as (
SELECT

    cast(item_id as integer) as item_id,
	
    item_name,
	item_brand,
	item_variant,
	item_category,
	item_category2,
	item_category3,
	item_category4,
	item_category5

FROM source
)

select *
from renamed_and_casted 