This file contains homework answers, simple codes and explanations.


### Question 1:
- 20,332,093


### Question 2:
- 0 MB for the External Table and 155.12 MB for the Materialized Table


### Question 3:
- BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

The estimated Bytes exactly double since the number of columns to query doubles.


### Question 4:
- 8,333

### Question 5:
- Partition by tpep_dropoff_datetime and Cluster on VendorID


### Question 6:
- 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

The estimated Bytes of partitioned table is almost 1/12 of the non-partitioned one because only 1/12 of the dates are queried.


### Question 7: 
- GCP Bucket


### Question 8:
- False


## (Bonus: Not worth points) Question 9:
No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
- 0 B. Materialized tables have metadata that stores precomputed results. `SELECT count(*)` does not proceed any data but directly returns the 'Number of rows' metadata that also displays in the table 'DETAILS'->'Storage Info' part.