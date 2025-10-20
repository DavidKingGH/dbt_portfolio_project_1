WITH user_sessions AS (
SELECT
    user_id,
    lower(nullif(trim(session_source), '')) as session_source,
    lower(nullif(trim(session_medium), '')) as session_medium,
    ga_session_id,
    session_start_timestamp
FROM
    {{ ref('stg__user_sessions') }}
),

normalized as (
  select
    user_id,
    coalesce(session_source, '(not set)') as session_source_norm,
    coalesce(session_medium, '(not set)') as session_medium_norm,
    ga_session_id,
    session_start_timestamp
  from user_sessions
),

ranked as (
  select
    *,
    row_number() over (
      partition by user_id
      order by session_start_timestamp asc, ga_session_id asc
    ) as rn_any,
    row_number() over (
      partition by user_id
      order by
        case when session_medium_norm in ('(not set)','(none)','direct') then 1 else 0 end,
        session_start_timestamp asc,
        ga_session_id asc
    ) as rn_non_direct_first
  from normalized
)

select
  user_id,
  -- choose rn_non_direct_first = 1 | switch to rn_any for "first session no matter what", 
  session_source_norm as session_source,
  session_medium_norm as session_medium
from ranked
where rn_non_direct_first = 1