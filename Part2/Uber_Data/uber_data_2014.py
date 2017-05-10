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

uber_2015 = sc.textFile("/user/mmd378/uber-raw-data-*14.csv", 1)
bruber_2015 = uber_2015.mapPartitions(lambda x: csv.reader(x))
header = bruber_2015.take(1)[0]
raw_data = bruber_2015.filter(lambda x: x[0] !='Dispatching_base_num').filter(lambda x: 'Date' not in x[0])
counters = raw_data.map(lambda x: (x[0].split(" ")[0],1)).reduceByKey(lambda x,y: x+y)
counters.saveAsTextFile("uber_2014_daily.out")
