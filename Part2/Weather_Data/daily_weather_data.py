from __future__ import print_function
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext, HiveContext
import sys
import pyspark.sql.functions as F
from pyspark.sql.functions import hour, udf, year
from pyspark.sql.types import StringType
import csv

hourly_weather_data = sc.textFile("/user/mmd378/hourly_weather_data_2013_through_2016.csv", 1)
broken_hours = hourly_weather_data.mapPartitions(lambda x: csv.reader(x))
weather_columns =broken_hours.take(1)[0]
broken_hours.map(lambda x: (x[1], 1)).reduceByKey(lambda x,y: x+y).take(20)
#[('NY CITY CENTRAL PARK NY US', 38586), ('STATION_NAME', 1), ('JFK INTERNATIONAL AIRPORT NY US', 45249), ('LA GUARDIA AIRPORT NY US', 46339)]
weather_data_no_label = broken_hours.filter(lambda x: x[1] != 'STATION_NAME')
schema_sd = spark.createDataFrame(weather_data_no_label, weather_columns)
schema_sd.createOrReplaceTempView("wd")

# Number of days with data for each station
query_res = spark.sql("select split(DATE, ' ')[0] as day_of, STATION_NAME, count(*) as total_freq from wd group by 1,2")
query_res_rdd = query_res.rdd.map(lambda x: str(x.day_of) + "\t" +str(x.STATION_NAME)+'\t'+ str(x.total_freq))
header = sc.parallelize(["Date\tWeather_Station\tCount"])
header.union(query_res_rdd).saveAsTextFile("weather_data_by_day_by_station.out")

# Max precipitation per day by weather station
query_res = spark.sql("select split(DATE, ' ')[0] as day_of, STATION_NAME, max(DAILYPrecip) as MAX_PRECIP, count(*) as total_freq from wd where REPORTTPYE='SOD' group by 1,2")
query_res_rdd = query_res.rdd.map(lambda x: str(x.day_of) + "\t" +str(x.STATION_NAME)+'\t'+str(x.MAX_PRECIP) +'\t'+ str(x.total_freq))
header = sc.parallelize(["Date\tWeather_Station\tmax_daily_precip\tCount"])
header.union(query_res_rdd).saveAsTextFile("weather_data_by_day_by_station_with_precip2.out")

# max precipitation per day across weather stations
query_res = spark.sql("select split(DATE, ' ')[0] as day_of, max(DAILYPrecip) as MAX_PRECIP, count(*) as total_freq from wd where REPORTTPYE='SOD' group by 1")
query_res_rdd = query_res.rdd.map(lambda x: str(x.day_of) + '\t'+str(x.MAX_PRECIP) +'\t'+ str(x.total_freq))
header = sc.parallelize(["Date\tmax_daily_precip\tCount"])
header.union(query_res_rdd).saveAsTextFile("weather_data_by_day_max_precip.out")

# Average precipitation per day across weather stations
query_res = spark.sql("select split(DATE, ' ')[0] as day_of, AVG(DAILYPrecip) as AVG_PRECIP, count(*) as total_freq from wd where REPORTTPYE='SOD'and DAILYPrecip !='T' group by 1")
query_res_rdd = query_res.rdd.map(lambda x: str(x.day_of) + '\t'+str(x.AVG_PRECIP) +'\t'+ str(x.total_freq))
header = sc.parallelize(["Date\tAVG_daily_precip\tCount"])
header.union(query_res_rdd).saveAsTextFile("weather_data_by_day_avg_precip_no_T.out")

# Average precipitation per day across weather stations by station
query_res = spark.sql("select split(DATE, ' ')[0] as day_of, STATION_NAME, CASE WHEN DAILYPrecip != 'T' then DAILYPrecip else 0 END as AVG_PRECIP, count(*) as total_freq from wd where REPORTTPYE='SOD' group by 1,2,3")
query_res_rdd = query_res.rdd.map(lambda x: str(x.day_of) + '\t'+str(x.STATION_NAME) + '\t'+str(x.AVG_PRECIP) +'\t'+ str(x.total_freq))
header = sc.parallelize(["Date\tstation_name\tAVG_daily_precip\tCount"])
header.union(query_res_rdd).saveAsTextFile("weather_data_by_day_by_station_avg_precip_no_T.out")

# Average temperature per day across weather stations
query_res = spark.sql("select split(DATE, ' ')[0] as day_of, AVG(DAILYAverageDryBulbTemp) as AVG_TEMP, count(*) as total_freq from wd where REPORTTPYE='SOD'and DAILYAverageDryBulbTemp !='T' group by 1")
query_res_rdd = query_res.rdd.map(lambda x: str(x.day_of) + '\t'+str(x.AVG_TEMP) +'\t'+ str(x.total_freq))
header = sc.parallelize(["Date\tAVG_daily_temp\tCount"])
header.union(query_res_rdd).saveAsTextFile("weather_data_by_day_avg_temp_no_T.out")

# Average snowfall per day across weather stations
query_res = spark.sql("select split(DATE, ' ')[0] as day_of, AVG(DAILYSnowfall) as AVG_Snowfall, count(*) as total_freq from wd where REPORTTPYE='SOD'and DAILYSnowfall !='T' group by 1")
query_res_rdd = query_res.rdd.map(lambda x: str(x.day_of) + '\t'+str(x.AVG_snowfall) +'\t'+ str(x.total_freq))
header = sc.parallelize(["Date\tAVG_daily_snowfallp\tCount"])
header.union(query_res_rdd).saveAsTextFile("weather_data_by_day_avg_snowfall_no_T.out")
