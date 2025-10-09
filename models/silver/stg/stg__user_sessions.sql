-- grain: one row per user per session

WITH ranked_events AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY user_id, ga_session_id ORDER BY event_timestamp ASC) as session_event_rank
    FROM
        {{ ref('fct_events') }}
)


select 
    -- ID and timestamp info
    user_id, 
    ga_session_id,
    MIN(event_timestamp) as session_start_timestamp, 
    MAX(event_timestamp) as session_end_timestamp, 

    -- Conditionally aggregate to get the value from the *first* event
    max(case when session_event_rank = 1 then device_category end) as device_category,
    MAX(CASE WHEN session_event_rank = 1 THEN source          END) AS session_source,
    MAX(CASE WHEN session_event_rank = 1 THEN medium          END) AS session_medium,
    MAX(CASE WHEN session_event_rank = 1 THEN campaign_name   END) AS session_campaign,
    MAX(CASE WHEN session_event_rank = 1 THEN page_location   END) AS landing_page,

    -- Session-level info 
    MAX(country) as country,
    
    -- Event count
    count(*) as event_count

from ranked_events
group by user_id, ga_session_id