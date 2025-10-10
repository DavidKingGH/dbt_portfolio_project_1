SELECT

    user_id,
    ga_session_id,
    session_start_timestamp AS touchpoint_timestamp,
    session_source AS touchpoint_source,
    session_medium AS touchpoint_medium,
    session_campaign AS touchpoint_campaign


FROM
    {{ ref('stg__user_sessions') }}

WHERE
        session_source IS NOT NULL 
        AND session_source != '(direct)'
        AND session_medium IS NOT NULL
        AND session_medium != '(none)'
        AND session_medium != '(not set)'
