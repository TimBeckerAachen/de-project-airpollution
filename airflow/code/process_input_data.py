#!/usr/bin/env python
# coding: utf-8

import argparse

import pyspark
from pyspark.sql import SparkSession


parser = argparse.ArgumentParser()

parser.add_argument('--input_airpollution_data', required=True)
parser.add_argument('--input_item_info', required=True)
parser.add_argument('--input_station_info', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_airpolution_data = args.input_airpollution_data
input_item_info = args.input_item_info
input_station_info = args.input_station_info
output = args.output


spark = SparkSession.builder \
    .appName('air-pollution') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'spark_cluster_bucket_airpollution-348907')

df_airpollution = spark.read.parquet(input_airpolution_data)
df_item = spark.read.parquet(input_item_info)
df_station = spark.read.parquet(input_station_info)

print('done reading input files')

df_join = df_airpollution.join(df_item, on=['item_code'], how='outer')
df_join_all = df_join.join(df_station, on=['station_code'], how='outer')

df_join_all.write.format('bigquery') \
    .option('table', output) \
    .mode('overwrite') \
    .save()
