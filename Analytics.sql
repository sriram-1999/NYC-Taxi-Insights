--Average fare amount for different Vendor IDs

SELECT VendorID, AVG(fare_amount) FROM `data-analytics-taxi.dataset.fact_table` 
GROUP BY VendorID;

--Average tip amount based on Payment Type

SELECT 
  b.payment_type_name,AVG(a.tip_amount) 
FROM 
  `dataset.fact_table` a 
JOIN  
  `dataset.payment_type_dim` b 
ON 
  a.payment_type_id = b.payment_type_id
WHERE
  b.payment_type_name IS NOT NULL
GROUP BY 
b.payment_type_name;

  
  
  --TOP 10 PICKUP LOCATIONS BASED ON THE NUMBER OF TRIPS
SELECT a.PULocationID,b.Borough, b.Zone, COUNT(*) as Trip_Count
FROM 
`dataset.pickup_location_dim` a
LEFT JOIN 
`dataset.location_map` b 
ON
a.PULocationID=b.LocationID

GROUP BY PULocationID,Borough,Zone
ORDER BY Trip_Count DESC
LIMIT 10;


--FIND TOTAL NUMBER OF TRIPS BASED ON PASSENGER COUNT
SELECT COUNT(*) AS Trip_Count, passenger_count
FROM `dataset.passenger_count_dim`

GROUP BY passenger_count
ORDER BY Trip_Count desc;


-- --FIND AVERAGE FARE AMOUNT BY HOUR OF THE DAY
select AVG(fare_amount) as Average_fare, pick_hour
from `dataset.datetime_dim` a left join `dataset.fact_table`b
on a.datetime_id=b.datetime_id

group by pick_hour
order by pick_hour desc;

--COMBINE ALL RELEVANT COLUMNS FOR UPLOADING IT INTO TABLEAU
CREATE TABLE `dataset.data_insights` as
(
SELECT 
f.trip_id,
f.VendorID,
pickup_locationdetails.PULocationID as pickup_location_ID,
pickup_locationdetails.Borough as pickup_borough,
pickup_locationdetails.Zone as pickup_location,
dropoff_locationdetails.DOLocationID as drop_location_ID,
dropoff_locationdetails.Borough as drop_borough,
dropoff_locationdetails.zone as drop_location,
d.tpep_pickup_datetime as pickup_date,
d.tpep_dropoff_datetime as drop_date,
p.passenger_count as passenger_count,
t.trip_distance,
r.rate_code_name as rate_code,
pay.payment_type_name as payment_type,
f.fare_amount,
f.extra,
f.mta_tax,
f.tip_amount,
f.tolls_amount,
f.improvement_surcharge,
f.total_amount

FROM `dataset.fact_table` f
JOIN `dataset.datetime_dim` d  ON f.datetime_id=d.datetime_id
JOIN `dataset.passenger_count_dim` p  ON p.passenger_count_id=f.passenger_count_id  
JOIN `dataset.trip_distance_dim` t  ON t.trip_distance_id=f.trip_distance_id  
JOIN `dataset.rate_code_dim` r ON r.rate_code_id=f.rate_code_id  
JOIN `dataset.pickup_location_dim` pick ON pick.pickup_location_id=f.pickup_location_id
JOIN `dataset.dropoff_location_dim` drop ON drop.dropoff_location_id=f.dropoff_location_id
JOIN `dataset.payment_type_dim` pay ON pay.payment_type_id=f.payment_type_id

JOIN(
(Select
 a.PULocationID,
 a.pickup_location_id,
 b.Borough, 
 b.Zone 
 FROM 
`dataset.pickup_location_dim` a
LEFT JOIN 
`dataset.location_map` b 
ON
a.PULocationID=b.LocationID))
 AS pickup_locationdetails ON pickup_locationdetails.pickup_location_id = pick.pickup_location_id

JOIN(
  SELECT
  d.DOLocationID,
  d.dropoff_location_id,
  b.Borough,
  b.zone 
  FROM 
  `dataset.dropoff_location_dim`d
  LEFT JOIN `dataset.location_map`b
  ON 
  d.DOLocationID=B.LocationID )
  AS dropoff_locationdetails
  ON f.dropoff_location_id=dropoff_locationdetails.dropoff_location_id

);


