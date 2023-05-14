WITH stage_table AS(

    SELECT *,
    TO_TIMESTAMP(TPEP_PICKUP_DATETIME) as trip_start,
    TO_TIMESTAMP(TPEP_DROPOFF_DATETIME) as trip_end
    FROM {{source('uber_raw','UBER_YELLOW_TAXI_TRIPS')}}
    
)

SELECT * FROM stage_table