from __future__ import print_function
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext, HiveContext
import sys
import pyspark.sql.functions as F
from pyspark.sql.functions import hour, udf, year
from pyspark.sql.types import StringType
import csv

yellow_data = sc.textFile("/user/ls1908/project/data/y/*", 1)
broken_yellow = yellow_data.mapPartitions(lambda x: csv.reader(x))
broken_yellow_without_labels = broken_yellow.filter(lambda x: len(x) > 0).filter(lambda x: 'endor' not in str(x[0]))
yellow_len_19 = broken_yellow_without_labels.filter(lambda x: len(x)==19)
yellow_len_18 = broken_yellow_without_labels.filter(lambda x: len(x)==18)
potential_17ers = yellow_len_19.filter(lambda x:  str(x[17])=='').filter(lambda x: str(x[18])=='')
yellow_18_corrected = yellow_len_18.map(lambda x: [x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13],x[14],x[15],x[16], "null", x[17], "null", "null"])
yellow_17_corrected = potential_17ers.map(lambda x: [x[0], x[1], x[2], x[3], x[4], 'null', 'null', x[5], x[6], 'null', 'null', x[9], x[10], x[11], x[12], x[13], x[14], x[16], x[17], x[7], x[8]])
yellow_19_corrected = yellow_len_19.filter(lambda x: str(x[18]) != '').map(lambda x: [x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13],x[14],x[15],x[16], x[17], x[18], "null", "null"])
cols_19 = ('VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'pickup_longitude', 'pickup_latitude', 'RateCodeID', 'store_and_fwd_flag', 'dropoff_longitude', 'dropoff_latitude', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount',  'PULocationID', 'DOLocationID')
total_yellow_data = sc.union([yellow_18_corrected, yellow_19_corrected, yellow_17_corrected])
schema_sd = spark.createDataFrame(total_yellow_data, cols_19)
schema_sd.createOrReplaceTempView("yc")



# yellow cabs by date

query_res = spark.sql("select date(tpep_pickup_datetime) as pickup_date, count(*) as num_rides from yc group by 1")
query_res_rdd = query_res.rdd.map(lambda x: str(x.pickup_date) + "\t" + str(x.num_rides))
header = sc.parallelize(["Pickup_date\tCount"])
header.union(query_res_rdd).saveAsTextFile("yellow_cabs_daily_nicer.out")


# Difference in pickup date and dropoff date for yellow cabs
query_res = spark.sql("select datediff(to_date(tpep_dropoff_datetime),to_date(tpep_pickup_datetime)) as date_delta, count(*) as num_rides from yc group by 1")
query_res_rdd = query_res.rdd.map(lambda x: str(x.date_delta) + "\t" + str(x.num_rides))
header = sc.parallelize(["Date_delta\tCount"])
header.union(query_res_rdd).saveAsTextFile("yellow_cabs_dropoff_pickup_delta.out")



# difference in pickup and dropoff date by year for yellow cabs

query_res = spark.sql("select year(tpep_pickup_datetime) as pickup_year, year(tpep_dropoff_datetime) as dropoff_year, datediff(to_date(tpep_dropoff_datetime),to_date(tpep_pickup_datetime)) as date_delta, count(*) as num_rides from yc group by 1, 2, 3")
query_res_rdd = query_res.rdd.map(lambda x: str(x.pickup_year) + "\t" + str(x.dropoff_year) + "\t" + str(x.date_delta) + "\t" + str(x.num_rides))
header = sc.parallelize(["Pickup_year\tDropoff_Year\tDate_delta\tCount"])
header.union(query_res_rdd).saveAsTextFile("yellow_cabs_dropoff_pickup_delta_with_years.out")


