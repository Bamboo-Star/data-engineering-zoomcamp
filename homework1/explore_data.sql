-----------------------------------------------------------------
------------ ## Question 3. Trip Segmentation Count

SELECT
    SUM(CASE WHEN trip_distance <= 1 THEN 1 ELSE 0 END) AS up_to_1_mile,
    SUM(CASE WHEN trip_distance > 1 AND trip_distance <= 3 THEN 1 ELSE 0 END) AS between_1_and_3_miles,
    SUM(CASE WHEN trip_distance > 3 AND trip_distance <= 7 THEN 1 ELSE 0 END) AS between_3_and_7_miles,
    SUM(CASE WHEN trip_distance > 7 AND trip_distance <= 10 THEN 1 ELSE 0 END) AS between_7_and_10_miles,
    SUM(CASE WHEN trip_distance > 10 THEN 1 ELSE 0 END) AS over_10_miles
FROM green_taxi_trips
WHERE CAST(lpep_pickup_datetime as DATE) >= DATE '2019-10-01' 
    AND CAST(lpep_dropoff_datetime as DATE) < DATE '2019-11-01';


-----------------------------------------------------------------
------------ ## Question 4. Longest trip for each day

SELECT CAST(lpep_pickup_datetime as DATE) as pickup_date,
    MAX(trip_distance) as max_distance
FROM green_taxi_trips
GROUP BY pickup_date
ORDER BY max_distance desc;


-----------------------------------------------------------------
------------ ## Question 5. Three biggest pickup zones

SELECT tz."Zone", COUNT(*) as n_trips
FROM green_taxi_trips gtt
LEFT JOIN 
    taxi_zone tz ON gtt."PULocationID" = tz."LocationID"
WHERE CAST(gtt.lpep_pickup_datetime as DATE) = DATE '2019-10-18'
GROUP BY tz."Zone" 
HAVING SUM(gtt.total_amount) > 13000;



-----------------------------------------------------------------
------------ ## Question 6. Largest tip

SELECT dotz."Zone", MAX(gtt.tip_amount) as largest_tip
FROM green_taxi_trips gtt
LEFT JOIN 
    taxi_zone putz ON gtt."PULocationID" = putz."LocationID"
LEFT JOIN 
    taxi_zone dotz ON gtt."DOLocationID" = dotz."LocationID"
WHERE putz."Zone" = 'East Harlem North'
    AND CAST(lpep_pickup_datetime as DATE) >= DATE '2019-10-01' 
    AND CAST(lpep_dropoff_datetime as DATE) < DATE '2019-11-01'
GROUP BY dotz."Zone"
ORDER BY largest_tip desc;