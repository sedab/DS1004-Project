from __future__ import print_function
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext, HiveContext
import sys
from csv import reader
import pyspark.sql.functions as F
from pyspark.sql.functions import hour, udf, year
from pyspark.sql.types import StringType
import csv

uber_2015 = sc.textFile("/user/mmd378/uber-raw-data-janjune-15.csv", 1)
bruber_2015 = uber_2015.mapPartitions(lambda x: csv.reader(x))
header = bruber_2015.take(1)[0]
raw_data = bruber_2015.filter(lambda x: x[0] !='Dispatching_base_num')

schema_sd = spark.createDataFrame(raw_data, header)
schema_sd.createOrReplaceTempView("uber2015")


query_res = spark.sql("select to_date(Pickup_Date)  as pickup_date, count(*) as num_rides from uber2015 group by 1")
query_res_rdd = query_res.rdd.map(lambda x: str(x.pickup_date) + "\t" +  str(x.num_rides))
header = sc.parallelize(["pickup_date\tCount"])
header.union(query_res_rdd).saveAsTextFile("uber_2015_daily.out")
