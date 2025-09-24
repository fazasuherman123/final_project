WITH dates AS (
    SELECT
        day AS date,
        EXTRACT(YEAR FROM day) AS year,
        EXTRACT(MONTH FROM day) AS month,
        EXTRACT(DAY FROM day) AS day,
        EXTRACT(DAYOFWEEK FROM day) AS weekday
    FROM UNNEST(GENERATE_DATE_ARRAY('2025-07-01', '2025-12-31', INTERVAL 1 DAY)) AS day
)
SELECT * FROM dates
