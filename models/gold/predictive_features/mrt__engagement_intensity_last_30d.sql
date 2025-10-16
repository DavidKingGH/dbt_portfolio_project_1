with 

report_date as (

select
    max(event_timestamp) as as_of_date
from {{ ref('fct_events')}}
),


user_last_session as (
select
    user_id,
    max(session_start_timestamp) as last_session_timestamp
from {{ ref('stg__user_sessions')}}
group by 1
),


sessions_last_30d as (
    select
        user_id,
        ga_session_id,
        session_start_timestamp,
        session_end_timestamp,
        event_count,
        datediff(
            'hour',
            lag(session_start_timestamp, 1) over (partition by user_id order by session_start_timestamp),
            session_start_timestamp
        ) as hours_since_previous_session
    from {{ ref('stg__user_sessions') }}
    where session_start_timestamp >= (select as_of_date from report_date) - interval '29' day
      and session_start_timestamp <= (select as_of_date from report_date)
),

user_features_last_30d as (

select
    user_id, 
    coalesce(count(distinct date_trunc('day', session_start_timestamp)),0) as unique_active_days_last_30d,
    coalesce(count(distinct ga_session_id),0) as user_sessions_last_30d,
    coalesce(avg(datediff('sec', session_start_timestamp, session_end_timestamp)),0) as avg_session_duration_last_30d_secs,
    median(hours_since_previous_session) as median_inter_session_gap_hours,
    coalesce(sum(event_count),0) as event_count_last_30d
from sessions_last_30d us
where session_start_timestamp <= (select as_of_date from report_date)
    and session_start_timestamp > (select as_of_date from report_date) - interval '29' days
    and session_end_timestamp is not null
group by user_id
    
),

final as (
select
    -- grain
    rd.as_of_date::date as as_of_date,
    uls.user_id::varchar as user_id, 
    
    -- recency 
    datediff('day', uls.last_session_timestamp, rd.as_of_date)::int as days_since_last_session,
    
    -- 30d intensity metrics
    w.user_sessions_last_30d::int as user_sessions_last_30d,
    w.avg_session_duration_last_30d_secs::double as avg_session_duration_secs,
    w.median_inter_session_gap_hours,
    w.event_count_last_30d::int as event_count_last_30d,
    coalesce(w.event_count_last_30d::double / nullif(w.user_sessions_last_30d, 0), 0.0)::double as avg_events_per_session_last_30d
from user_last_session uls
cross join report_date rd
left join user_features_last_30d w on uls.user_id = w.user_id
)

select *
from final

