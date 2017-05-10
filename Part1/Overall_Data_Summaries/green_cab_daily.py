from __future__ import print_function
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext, HiveContext
import sys
import pyspark.sql.functions as F
from pyspark.sql.functions import hour, udf, year
from pyspark.sql.types import StringType
import csv

green_columns = ['VendorID', 'lpep_pickup_datetime', 'Lpep_dropoff_datetime',  'Store_and_fwd_flag', 'RateCodeID', 'Pickup_longitude', 'Pickup_latitude', 'Dropoff_longitude', 'Dropoff_latitude', 'Passenger_count', 'Trip_distance', 'Fare_amount','Extra','MTA_tax', 'Tip_amount', 'Tolls_amount', 'Ehail_fee', 'improvement_surcharge', 'Total_amount', 'Payment_type', 'Trip_type',  'PULocationID', 'DOLocationID'] 
green_data = sc.textFile("/user/ls1908/project/data/g/*", 1)
broken_green = green_data.mapPartitions(lambda x: csv.reader(x))
broken_green_no_labels = broken_green.filter(lambda x: len(x) > 0).filter(lambda x: 'endor' not in x[0])
gd_2013 = broken_green_no_labels.filter(lambda x: '2013' in x[1])
gd_2014 = broken_green_no_labels.filter(lambda x: '2014' in x[1])
gd_2015 = broken_green_no_labels.filter(lambda x: '2015' in x[1])
gd_2016 = broken_green_no_labels.filter(lambda x: '2016' in x[1])
gd_2016_1_6 = gd_2016.filter(lambda x: int(x[1].split("-")[1])<7)
gd_2016_7_12 = gd_2016.filter(lambda x: int(x[1].split("-")[1])>6)
fixed_2013 = gd_2013.map(lambda x: [x[0], x[1] ,x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13], x[14], x[15], x[16], "null", x[17], x[18], x[19],"null","null"])
fixed_2014 = gd_2014.map(lambda x: [x[0], x[1] ,x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13], x[14], x[15], x[16], "null", x[17], x[18], x[19],"null","null"])
fixed_2015 = gd_2015.map(lambda x: [x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10],x[11],x[12],x[13],x[14],x[15],x[16],x[17],x[18],x[19],x[20],"null","null"])
fixed_2016_1_6 = gd_2016_1_6.map(lambda x: [x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10],x[11],x[12],x[13],x[14],x[15],x[16],x[17],x[18],x[19],x[20],"null","null"])
fixed_2016_7_12 = gd_2016_7_12.map(lambda x: [x[0],x[1],x[2],x[3],x[4],"null","null","null","null",x[7],x[8],x[9],x[10],x[11],x[12],x[13],x[14],x[15],x[16],x[17],x[18],x[5],x[6]])
total_data = sc.union([fixed_2013, fixed_2014, fixed_2016_1_6, fixed_2016_7_12, fixed_2015 ])
schema_sd = spark.createDataFrame(total_data, green_columns)
schema_sd.createOrReplaceTempView("gc")



# green cabs by date

query_res = spark.sql("select date(lpep_pickup_datetime) as pickup_date, count(*) as num_rides from gc group by 1")
query_res_rdd = query_res.rdd.map(lambda x: str(x.pickup_date) + "\t" + str(x.num_rides))
header = sc.parallelize(["Pickup_date\tCount"])
header.union(query_res_rdd).saveAsTextFile("green_cabs_daily_nicer.out")



# Difference in pickup date and dropoff date for green cabs 
query_res = spark.sql("select datediff(to_date(lpep_pickup_datetime),to_date(lpep_dropoff_datetime)) as date_delta, count(*) as num_rides from gc group by 1")
query_res_rdd = query_res.rdd.map(lambda x: str(x.date_delta) + "\t" + str(x.num_rides))
header = sc.parallelize(["Date_delta\tCount"])
header.union(query_res_rdd).saveAsTextFile("green_cabs_dropoff_pickup_delta.out")


# difference in pickup and dropoff date by year for green cabs

query_res = spark.sql("select year(lpep_pickup_datetime) as pickup_year, year(Lpep_dropoff_datetime) as dropoff_year, datediff(to_date(lpep_dropoff_datetime),to_date(lpep_pickup_datetime)) as date_delta, count(*) as num_rides from gc group by 1, 2, 3")
query_res_rdd = query_res.rdd.map(lambda x: str(x.pickup_year) + "\t" + str(x.dropoff_year) + "\t" + str(x.date_delta) + "\t" + str(x.num_rides))
header = sc.parallelize(["Pickup_year\tDropoff_Year\tDate_delta\tCount"])
header.union(query_res_rdd).saveAsTextFile("green_cabs_dropoff_pickup_delta_with_years.out")

