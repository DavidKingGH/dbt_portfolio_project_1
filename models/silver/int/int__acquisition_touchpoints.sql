WITH ranked_sessions AS (
    SELECT
        user_id,
        session_source,
        session_medium,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY session_start_timestamp ASC) as user_session_rank
    FROM
        {{ ref('stg__user_sessions') }}

)

SELECT
    user_id,
    session_source,
    session_medium
FROM
    ranked_sessions
WHERE
    user_session_rank = 1