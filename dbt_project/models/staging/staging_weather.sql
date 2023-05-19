WITH stage_table AS(

    SELECT *,
    TO_TIMESTAMP(DATETIME) as TIMESTAMP
    FROM {{source('nyc_weather_raw','NYC_WEATHER')}}
    
)

SELECT * FROM stage_table