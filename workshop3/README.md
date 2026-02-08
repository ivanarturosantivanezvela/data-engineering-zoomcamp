CREATE OR REPLACE EXTERNAL TABLE `kestra-demo-486117.zoomcampdemo.yellow_tripdata_ext`
  OPTIONS (
    format = 'PARQUET',
    uris = ['gs://workshop3zoomcamp/yellow_tripdata_*.parquet']);

CREATE OR REPLACE TABLE `kestra-demo-486117.zoomcampdemo.yellow_tripdata_normal`
AS
SELECT *
FROM `kestra-demo-486117.zoomcampdemo.yellow_tripdata_ext`

-- Pregunta 1
SELECT COUNT(*)
FROM kestra-demo-486117.zoomcampdemo.yellow_tripdata_ext

-- Pregunta 2
SELECT Distinct(PULocationID)
FROM kestra-demo-486117.zoomcampdemo.yellow_tripdata_ext
SELECT Distinct(PULocationID)
FROM kestra-demo-486117.zoomcampdemo.yellow_tripdata_normal

--Pregunta 3
SELECT Distinct(PULocationID)
FROM kestra-demo-486117.zoomcampdemo.yellow_tripdata_normal

SELECT Distinct(PULocationID, DOLocationID)
FROM kestra-demo-486117.zoomcampdemo.yellow_tripdata_normal

--Pregunta 4
SELECT COUNT(*) FROM kestra-demo-486117.zoomcampdemo.yellow_tripdata_normal where fare_amount = 0

--Pregunta 5
CREATE TABLE `kestra-demo-486117.zoomcampdemo.yellow_tripdata_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT *
FROM `kestra-demo-486117.zoomcampdemo.yellow_tripdata_normal`;

--Pregunta 6
SELECT DISTINCT VendorID
FROM `kestra-demo-486117.zoomcampdemo.yellow_tripdata_normal`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT VendorID
FROM `kestra-demo-486117.zoomcampdemo.yellow_tripdata_optimized`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';