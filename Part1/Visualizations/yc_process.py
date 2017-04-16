import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SQLContext
import csv
import argparse
import os

# data file will process yellow taxi data into one dataframe object

def yc_processing(sc, sq):
    cols_19 = ('VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_dis$


    # load all yellow cab data
    yellow_data = sc.textFile("/user/ls1908/project/data/y/*", 1)

    # process CSV lines
    broken_yellow = yellow_data.mapPartitions(lambda x: csv.reader(x))


    # filter out rows that are just file column names
    broken_yellow_without_labels = broken_yellow.filter(lambda x: len(x) > 0).filter(lambda x: 'endor' n$


    #split into pre 2015 and post 2015 based on line length
    yellow_len_19 = broken_yellow_without_labels.filter(lambda x: len(x)==19)
    yellow_len_18 = broken_yellow_without_labels.filter(lambda x: len(x)==18)
    potential_17ers = yellow_len_19.filter(lambda x:  str(x[17])=='').filter(lambda x: str(x[18])=='')

    yellow_18_corrected = yellow_len_18.map(lambda x: [x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x$
    yellow_17_corrected = potential_17ers.map(lambda x: [x[0], x[1], x[2], x[3], x[4], 'null', 'null', x$

    yellow_19_corrected = yellow_len_19.filter(lambda x: str(x[18]) != '').map(lambda x: [x[0], x[1], x[$

    total_yellow_data = sc.union([yellow_18_corrected, yellow_19_corrected, yellow_17_corrected])

   #total_mapped = total_yellow_data.map(lambda x: ["{0}={1}".format(cols_19[i],x[i]) for i in range(le$

    schema_sd = sq.createDataFrame(total_yellow_data, schema=cols_19)
    schema_sd.registerTempTable("yc")



    # creating location schema
    loc = sc.textFile("/user/ls1908/project/taxi_zone_lookup.csv", 1)
    loc = loc.mapPartitions(lambda l: csv.reader(l))
    loc = loc.filter(lambda l: 'ocation' not in str(l[0]))
    cols_names_loc = ('LocationID',     'Borough',	'Zone', 'service_zone')
    schema_loc = sq.createDataFrame(loc, cols_names_loc)
    schema_loc.registerTempTable("loc")



    return

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--out_file', type=str, default='sql_result.out',
                    help='output file for writing sql query results')
    parser.add_argument('--query', type=str, default='select count(*) from yc',
                    help='sql query to run')
    args = parser.parse_args()

    sc = SparkContext()
    sq = SQLContext(sc)
    yc_processing(sc, sq)
    print("running query")
    sql_query = sq.sql(args.query)
    print("storing file to: {0}".format(args.out_file))
    #sc.parallelize(sql_query.rdd.collect()).saveAsTextFile(args.out_file)
    sql_query.write.csv(args.out_file)
    sc.stop()
    return

if __name__ == '__main__':
    main()


                                                                                                       
