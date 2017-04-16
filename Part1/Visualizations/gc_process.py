import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SQLContext
import csv
import argparse
import os

# data file will process yellow taxi data into one dataframe object

def gc_processing(sc, sq):
    green_columns = ['VendorID', 'lpep_pickup_datetime', 'Lpep_dropoff_datetime',  'Store_and_fwd_flag',$
    green_data = sc.textFile("/user/ls1908/project/data/g/*", 1)
    broken_green = green_data.mapPartitions(lambda x: csv.reader(x))
    broken_green_no_labels = broken_green.filter(lambda x: len(x) > 0).filter(lambda x: 'endor' not in x$
    gd_2013 = broken_green_no_labels.filter(lambda x: '2013' in x[1])
    gd_2014 = broken_green_no_labels.filter(lambda x: '2014' in x[1])
    gd_2015 = broken_green_no_labels.filter(lambda x: '2015' in x[1])
    gd_2016 = broken_green_no_labels.filter(lambda x: '2016' in x[1])
    gd_2016_1_6 = gd_2016.filter(lambda x: int(x[1].split("-")[1])<7)
    gd_2016_7_12 = gd_2016.filter(lambda x: int(x[1].split("-")[1])>6)

    fixed_2013 = gd_2013.map(lambda x: [x[0], x[1] ,x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10$
    fixed_2014 = gd_2014.map(lambda x: [x[0], x[1] ,x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10$

    fixed_2015 = gd_2015.map(lambda x: [x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10],x[11],x[$

    fixed_2016_1_6 = gd_2016_1_6.map(lambda x: [x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10],$
    fixed_2016_7_12 = gd_2016_7_12.map(lambda x: [x[0],x[1],x[2],x[3],x[4],"null","null","null","null",x$

    total_data = sc.union([fixed_2013, fixed_2014, fixed_2016_1_6, fixed_2016_7_12, fixed_2015 ])

    schema_sd = sq.createDataFrame(total_data, schema=green_columns)
    schema_sd.createOrReplaceTempView("gc")

    return

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--out_file', type=str, default='sql_result.out',
                    help='output file for writing sql query results')
    parser.add_argument('--query', type=str, default='select count(*) from gc',
                    help='sql query to run')
    args = parser.parse_args()

    sc = SparkContext()
    sq = SQLContext(sc)
    gc_processing(sc, sq)
    print("running query")
    sql_query = sq.sql(args.query)
    print("storing file to: {0}".format(args.out_file))
    #sc.parallelize(sql_query.rdd.collect()).saveAsTextFile(args.out_file)
    sql_query.write.csv(args.out_file)
    sc.stop()
    return

if __name__ == '__main__':
    main()
                                                   

