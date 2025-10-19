with source as (

select * from {{ source('bronze', 'dim_items') }}

),

renamed_and_casted as (
SELECT

    item_id::varchar as item_id,
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