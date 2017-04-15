import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SQLContext
import csv
import argparse
import os
from datetime import datetime as dt

# data file will process yellow taxi data into one dataframe object

def yc_processing(sc, path_to_data):

    # load all yellow cab data
    #yellow_data = sc.textFile("data/yellow_tripdata_2014-08.csv", 1)
    yellow_data = sc.textFile(path_to_data,1)
    # process CSV lines
    broken_yellow = yellow_data.mapPartitions(lambda x: csv.reader(x))


    # filter out rows that are just file column names
    broken_yellow_without_labels = broken_yellow.filter(lambda x: len(x) > 0).filter(lambda x: 'endor' not in str(x[0]))


    #split into pre 2015 and post 2015 based on line length
    yellow_len_19 = broken_yellow_without_labels.filter(lambda x: len(x)==19)
    yellow_len_18 = broken_yellow_without_labels.filter(lambda x: len(x)==18)
    potential_17ers = yellow_len_19.filter(lambda x:  str(x[17])=='').filter(lambda x: str(x[18])=='')

    yellow_18_corrected = yellow_len_18.map(lambda x: [x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13],x[14],x[15],x[16], "null", x[17], "null", "null"])
    yellow_17_corrected = potential_17ers.map(lambda x: [x[0], x[1], x[2], x[3], x[4], 'null', 'null', x[5], x[6], 'null', 'null', x[9], x[10], x[11], x[12], x[13], x[14], x[16], x[17], x[7], x[8]])

    yellow_19_corrected = yellow_len_19.filter(lambda x: str(x[18]) != '').map(lambda x: [x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13],x[14],x[15],x[16], x[17], x[18], "null", "null"])

    total_yellow_data = sc.union([yellow_18_corrected, yellow_19_corrected, yellow_17_corrected])



    return total_yellow_data


"""
def map_a_column(path_to_data, col_func, col_loc, filename=None):
    sc = SparkContext()
    sq = SQLContext(sc)
    print("TEST!")
    print(col_func)
    print(col_func(25))
    print("TEST DONE")
    total_data = yc_processing(sc, sq, path_to_data)
    mapped_data = total_data.map(lambda x: col_func(x[col_loc]
                                                   )
                                )
    print("SAMPLE DATA OUTPUT: \n")
    print(mapped_data.take(20))
    #if filename:
    #    print("Saving Mapped Data to file: {0}".format(filename))
    #    mapped_data.write.csv(filename)
    sc.stop()
    return


def main():    
    yellow_cols = ['VendorID', 
                   'tpep_pickup_datetime', 
                   'tpep_dropoff_datetime', 
                   'passenger_count', 
                   'trip_distance', 
                   'pickup_longitude', 
                   'pickup_latitude', 
                   'RateCodeID', 
                   'store_and_fwd_flag', 
                   'dropoff_longitude', 
                   'dropoff_latitude', 
                   'payment_type', 
                   'fare_amount', 
                   'extra', 
                   'mta_tax', 
                   'tip_amount', 
                   'tolls_amount', 
                   'improvement_surcharge', 
                   'total_amount',  
                   'PULocationID',
                   'DOLocationID'
                  ]

    sc = SparkContext()
    sq = SQLContext(sc)

    
    
    
    
    
    column_to_process = sys.argv[1]
    data_to_process = sys.argv[2]
    single_or_spark = sys.argv[3]
    try:
        single_data = sys.argv[3]
        
    if sys.argv == "spark":
        total_data = yc_processing(sc, sq)
        for col in column_to_test:
            
    print("Right now we're just testing")
    print("Checking Vends")
    vends = total_data.map(lambda x: yt.check_passenger_count(x[3]))
    print(vends.take(20))
    print("Checking Times")
    times = total_data.map(lambda x: yt.check_tpep_pickup_datetime(x[1]))
    print(times.take(20))
    
    print("Checking IDs")
    ids = total_data.map(lambda x: yt.check_PULocationID(x[20]))
    print(ids.take(20))    
    
    taxes = total_data.map(lambda x: yt.check_mta_tax(x[14]))
    print(taxes.take(20))
    
    tolls = total_data.map(lambda x: yt.check_tolls_amount(x[15]))
    print(tolls.take(20))
    
    
    longs = total_data.map(lambda x: yt.check_dropoff_longitude(x[5]))
    print(longs.take(20))
    
    lats = total_data.map(lambda x: yt.check_dropoff_latitude(x[6]))
    print(lats.take(20))
    
    sc.stop()
    return

if __name__ == '__main__':
    main()
"""
