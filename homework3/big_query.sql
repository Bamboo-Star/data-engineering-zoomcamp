-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `ny_taxi.yellow_tripdata_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://terraform-introduction-449023-terra-bucket/yellow_tripdata_2024-*.parquet']
);


-- Create a materialized table from external table
CREATE OR REPLACE TABLE ny_taxi.yellow_tripdata_materialized AS
SELECT * FROM ny_taxi.yellow_tripdata_external;


-- Question 1: What is count of records for the 2024 Yellow Taxi Data?
SELECT COUNT(VendorID) 
FROM ny_taxi.yellow_tripdata_materialized;


-- Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
SELECT COUNT(DISTINCT PULocationID) 
FROM ny_taxi.yellow_tripdata_external;

SELECT COUNT(DISTINCT PULocationID) 
FROM ny_taxi.yellow_tripdata_materialized;


-- Question 3: Why are the estimated number of Bytes different by retrieving 1 v.s. 2 columns?
SELECT PULocationID 
FROM ny_taxi.yellow_tripdata_materialized;

SELECT PULocationID, DOLocationID 
FROM ny_taxi.yellow_tripdata_materialized;


-- Question 4: How many records have a fare_amount of 0?
SELECT COUNT(VendorID) 
FROM ny_taxi.yellow_tripdata_materialized 
WHERE fare_amount=0;


-- Question 5: Create a new table to filter based on tpep_dropoff_datetime and order the results by VendorID
CREATE OR REPLACE TABLE ny_taxi.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM ny_taxi.yellow_tripdata_materialized;


-- Question 6
SELECT DISTINCT VendorID 
FROM ny_taxi.yellow_tripdata_materialized 
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' and '2024-03-15';

SELECT DISTINCT VendorID 
FROM ny_taxi.yellow_tripdata_partitioned_clustered 
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' and '2024-03-15';


-- Question 9
SELECT COUNT(*) FROM ny_taxi.yellow_tripdata_materialized;