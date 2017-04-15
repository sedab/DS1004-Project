import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SQLContext
import csv
import argparse
import os
from datetime import datetime as dt

# /user/ls1908/project/data/g/*
"""
green_columns = ['VendorID', 'lpep_pickup_datetime', 'Lpep_dropoff_datetime',  'Store_and_fwd_flag', 'RateCodeID', 'Pickup_longitude', 'Pickup_latitude', 'Dropoff_longitude', 'Dropoff_latitude', 'Passenger_count', 'Trip_distance', 'Fare_amount','Extra','MTA_tax', 'Tip_amount', 'Tolls_amount', 'Ehail_fee', 'improvement_surcharge', 'Total_amount', 'Payment_type', 'Trip_type',  'PULocationID', 'DOLocationID'] 
"""

def gd_processing(sc, path_to_data):

    # load all green cab data
    green_data = sc.textFile(path_to_data, 1)
    # process CSV lines
    broken_green = green_data.mapPartitions(lambda x: csv.reader(x))

    #green_data = sc.textFile("/user/ls1908/project/data/g/*", 1)
    #broken_green = green_data.mapPartitions(lambda x: csv.reader(x))
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

    return total_data

