WITH ranked_sessions AS (
    SELECT
        user_id,
        session_source,
        session_medium,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY session_start_timestamp ASC) as user_session_rank
    FROM
        {{ ref('stg__user_sessions') }}
    WHERE
        session_source IS NOT NULL 
        AND session_source != '(direct)'
        AND session_medium IS NOT NULL
        AND session_medium != '(none)'
        AND session_medium != '(not set)'
)

SELECT
    user_id,
    session_source,
    session_medium
FROM
    ranked_sessions
WHERE
    user_session_rank = 1