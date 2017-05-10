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


# Look at hourly trip data for green cabs 
query_res = spark.sql("select to_date(lpep_pickup_datetime) as pickup_date, hour(lpep_pickup_datetime) as pickup_hour, count(*) as num_rides from gc where to_date(lpep_pickup_datetime)BETWEEN '2014-02-01' AND '2014-02-28' group by 1, 2")
query_res_rdd = query_res.rdd.map(lambda x: str(x.pickup_date) + "\t" + str(x.pickup_hour) + "\t" + str(x.num_rides))
header = sc.parallelize(["Pickup_date\tpickup_hour\tCount"])
header.union(query_res_rdd).saveAsTextFile("green_cabs_hourly_rides_feb_2014.out")

### GREEN CAB DATA OVERALL
query_res = spark.sql("select date_format(lpep_pickup_datetime, 'EEEE')  as day_of_week, hour(lpep_pickup_datetime) as pickup_hour, count(*) as num_rides from gc group by 1, 2")
query_res_rdd = query_res.rdd.map(lambda x: str(x.day_of_week) + "\t" + str(x.pickup_hour) + "\t" + str(x.num_rides))
header = sc.parallelize(["Pickup_dayofweek\tpickup_hour\tCount"])
header.union(query_res_rdd).saveAsTextFile("green_cabs_hourly_rides_day_of_week.out")

### GREEN CAB DATA BY YEAR
query_res = spark.sql("select date_format(lpep_pickup_datetime, 'EEEE')  as day_of_week, year(lpep_pickup_datetime) as pickup_year, hour(lpep_pickup_datetime) as pickup_hour, count(*) as num_rides from gc group by 1, 2, 3")
query_res_rdd = query_res.rdd.map(lambda x: str(x.day_of_week)+ "\t" + str(x.pickup_year) + "\t" + str(x.pickup_hour) + "\t" + str(x.num_rides))
header = sc.parallelize(["Pickup_dayofweek\tpickup_year\tpickup_hour\tCount"])
header.union(query_res_rdd).saveAsTextFile("green_cabs_hourly_rides_day_of_week_by_year.out")



### GREEN CAB DATA BY hour by date
query_res = spark.sql("select date_format(lpep_pickup_datetime, 'EEEE')  as day_of_week, to_date(lpep_pickup_datetime) as pickup_date, hour(lpep_pickup_datetime) as pickup_hour, count(*) as num_rides from gc group by 1, 2, 3")
query_res_rdd = query_res.rdd.map(lambda x: str(x.day_of_week)+ "\t" + str(x.pickup_date) + "\t" + str(x.pickup_hour) + "\t" + str(x.num_rides))
header = sc.parallelize(["Pickup_dayofweek\tpickup_date\tpickup_hour\tCount"])
header.union(query_res_rdd).saveAsTextFile("green_cabs_hourly_rides_day_of_week_by_date.out")

