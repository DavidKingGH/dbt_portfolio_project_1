with

report_date as (
  select max(event_timestamp) as as_of_date
  from {{ ref('fct_events') }}
),

user_sessions_all as (
  select
    us.user_id,
    us.ga_session_id,
    us.session_start_timestamp,
    us.session_end_timestamp,
    us.event_count,
    lag(us.session_start_timestamp) over (
      partition by us.user_id
      order by us.session_start_timestamp
    ) as prev_session_start
  from {{ ref('stg__user_sessions') }} us
),

user_last_session as (
  select user_id, max(session_start_timestamp) as last_session_timestamp
  from user_sessions_all
  group by 1
),

-- 30d windowed aggregates 
user_features_last_30d as (
  select
    us.user_id,
    count(distinct date_trunc('day', us.session_start_timestamp))           as unique_active_days_last_30d,
    count(distinct us.ga_session_id)                                        as user_sessions_last_30d,
    avg(datediff('second', us.session_start_timestamp, us.session_end_timestamp)) 
                                                                            as avg_session_duration_last_30d_secs,
    median(datediff('hour', us.prev_session_start, us.session_start_timestamp))
                                                                            as median_inter_session_gap_hours,
    sum(us.event_count)                                                     as event_count_last_30d
  from user_sessions_all us
  cross join report_date rd
  where us.session_start_timestamp >= rd.as_of_date - interval '30' day
    and us.session_start_timestamp <= rd.as_of_date
    and us.session_end_timestamp is not null
  group by us.user_id
),

-- drive from dim_users to keep never-active users (true zeros)
final as (
  select
    rd.as_of_date::date                                      as as_of_date,
    du.user_id::varchar                                      as user_id,

    -- recency: NULL when never-active 
    datediff('day', uls.last_session_timestamp, rd.as_of_date)::int 
                                                              as days_since_last_session,

    -- 30d intensity metrics
    coalesce(w.user_sessions_last_30d, 0)::int               as user_sessions_last_30d,
    coalesce(w.unique_active_days_last_30d, 0)::int          as unique_active_days_last_30d,
    coalesce(w.avg_session_duration_last_30d_secs, 0)::double
                                                              as avg_session_duration_last_30d_secs,
    w.median_inter_session_gap_hours                          as median_inter_session_gap_hours,
    coalesce(w.event_count_last_30d, 0)::int                 as event_count_last_30d,

    -- average events per session: sum / sessions
    case 
      when coalesce(w.user_sessions_last_30d, 0) > 0
        then (w.event_count_last_30d::double) / w.user_sessions_last_30d
      else null
    end                                                      as avg_events_per_session_last_30d
  from {{ ref('dim_users') }} du
  cross join report_date rd
  left join user_last_session        uls on du.user_id = uls.user_id
  left join user_features_last_30d   w   on du.user_id = w.user_id
)

select * from final;
