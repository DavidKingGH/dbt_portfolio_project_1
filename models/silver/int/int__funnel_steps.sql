select

    user_id, 
    ga_session_id, 

    -- on-site events

    MAX(CASE WHEN event_name = 'view_item' THEN 1 ELSE 0 END) as did_view_item, 
    MAX(CASE WHEN event_name = 'add_to_cart' THEN 1 ELSE 0 END) as did_add_to_cart,
    MAX(CASE WHEN event_name = 'begin_checkout' THEN 1 ELSE 0 END) as did_begin_checkout,
    MAX(CASE WHEN event_name = 'add_shipping_info' THEN 1 ELSE 0 END) as did_add_shipping_info,
    MAX(CASE WHEN event_name = 'add_payment_info' THEN 1 ELSE 0 END) as did_add_payment_info,
    MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) as did_purchase,

from {{ ref('fct_events')}}

where event_name IN (
        'view_item', 'add_to_cart', 'begin_checkout', 
        'add_shipping_info', 'add_payment_info', 'purchase'
    )

group by 1,2
