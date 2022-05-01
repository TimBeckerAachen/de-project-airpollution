create .eenv file

setup env var for google

docker-compose build
docker-compose up airflow-init
docker-compose up

gcloud auth application-default login
terraform init
terraform plan
terraform apply

gcloud auth activate-service-account --key-file=/.google/credentials/airpollution-348907-9b63fa676541.json

gcloud auth login --cred-file=/home/tim/.google/credentials/airpollution-348907-9b63fa676541.json
gcloud config set project airpollution-348907
gsutil cp code/process_input_data.py gs://spark_cluster_bucket_airpollution-348907/code/process_input_data.py

gcloud dataproc jobs submit pyspark --cluster=airpollution-spark-cluster --project=airpollution-348907 --region=europe-west6 --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar gs://spark_cluster_bucket_airpollution-348907/code/process_input_data.py -- --input_airpollution_data=gs://data_lake_airpollution-348907/airpollution/Measurement_info.parquet --input_item_info=gs://data_lake_airpollution-348907/airpollution/Measurement_item_info.parquet --input_station_info=gs://data_lake_airpollution-348907/airpollution/Measurement_station_info.parquet --output=airpollution_data_all.report