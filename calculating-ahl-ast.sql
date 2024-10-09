WITH call_metrics AS (
    SELECT
        c.call_id,
        parseDateTimeBestEffort(c.call_start_datetime) AS call_start_time,
        parseDateTimeBestEffort(c.agent_assigned_datetime) AS agent_assigned_time,
        parseDateTimeBestEffort(c.call_end_datetime) AS call_end_time,

        dateDiff('second', agent_assigned_time, call_end_time) AS handle_time_seconds,
        dateDiff('second', call_start_time, agent_assigned_time) AS waiting_time_seconds,

        COALESCE(r.primary_call_reason, 'Unknown') AS primary_call_reason,
        COALESCE(ss.silence_percent_average, 0) AS silence_percent_average,
        COALESCE(ss.average_sentiment, 0) AS average_sentiment
    FROM
        calls 
    LEFT JOIN
        reason r ON c.call_id = r.call_id
    LEFT JOIN
        sentiment_statistics ss ON c.call_id = ss.call_id
)

SELECT
    COUNT(c.call_id) AS total_calls,
    SUM(handle_time_seconds) AS total_handle_time_seconds,
    SUM(waiting_time_seconds) AS total_waiting_time_seconds,
    AVG(handle_time_seconds) AS avg_handle_time_seconds,
    AVG(waiting_time_seconds) AS avg_speed_to_answer_seconds,
    primary_call_reason,
    AVG(silence_percent_average) AS avg_silence_percent,
    AVG(average_sentiment) AS avg_customer_sentiment
FROM
    call_metrics
GROUP BY
    primary_call_reason
ORDER BY
    avg_handle_time_seconds DESC
