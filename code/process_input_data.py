#!/usr/bin/env python
# coding: utf-8

import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


parser = argparse.ArgumentParser()

parser.add_argument('--input_airpollution_data', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_airpolution_data = args.input_airpollution_data
output = args.output


spark = SparkSession.builder \
    .appName('air-pollution') \
    .getOrCreate()

# spark = SparkSession.builder \
#     .master("local[*]") \
#     .appName('test') \
#     .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'spark_cluster_bucket_airpollution-348907')

df_airpollution = spark.read.parquet(input_airpolution_data)
# df_airpollution = spark.read.parquet('/home/tim/play/DE-project/Measurement_info.parquet')
print('reading done')

# df_airpollution = df_airpollution \
#     .withColumnRenamed('Station code', 'station_code')


df_airpollution.write.format('bigquery') \
    .option('table', output) \
    .save()

# df_airpollution.write.parquet('zones')


