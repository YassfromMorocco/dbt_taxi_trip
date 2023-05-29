WITH trip AS(
SELECT *,
DATE_TRUNC('HOUR', "TRIP_START") AS "TRUNCATED_TO_HOUR"
FROM {{ref('staging_trip')}})

SELECT * FROM trip AS t
LEFT JOIN {{ref('staging_weather')}} AS w
ON t.TRUNCATED_TO_HOUR = w.TIMESTAMP