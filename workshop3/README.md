CREATE OR REPLACE EXTERNAL TABLE
  `kestra-demo-486117.zoomcampdemo.yellow_tripdata_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://workshop3zoomcamp/yellow_tripdata_*.parquet']
);