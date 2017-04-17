"""
ONLY APPLIES TO GREEN CAB DATA
"""

import sys
import check_yellow_data as yd
import check_green_data as gd
from pyspark import SparkContext
from datetime import datetime as dt

def check_Lpep_dropoff_datetime(input_datapoint):
    try:
        dto = dt.strptime(input_datapoint, '%Y-%m-%d %H:%M:%S')
        #dto.year, dto.month, dto.day, dto.minute, dto.second, dto.hour        
        if dto.year in [2013, 2014,2015,2016]:
            base_type="DATETIME"
            semantic_type="Timestamp"
            qual_type="VALID"
        else:
            base_type="DATETIME"
            semantic_type="Timestamp"
            qual_type="INVALID/OUTLIER"
    except:
        if input_datapoint=='':
            base_type="TEXT"
            semantic_type="Empty Value"
            qual_type="NULL"
        else:
            base_type=type(input_datapoint)
            semantic_type="Unknown"
            qual_type="INVALID"
    
    return [input_datapoint, base_type, semantic_type, qual_type]


def main():
    # input data (point or path) is sys.argv[1]
    # if sys.argv[2] is passed, it will be a path to green data
  
    try:
        check_green_data_flag = sys.argv[2]
        sc = SparkContext()
        g_data = gd.gd_processing(sc, sys.argv[1])
        mapped_g_data = g_data.map(lambda x: check_Lpep_dropoff_datetime(x[2]))
        print("SAMPLE GREEN CAB DATA OUTPUT: \n")
        print(mapped_g_data.take(20))
        
        print("saving all outputs to files")
        mapped_y_data.saveAsTextFile("lpep_dropoff_datetime_y.out")
        mapped_g_data.saveAsTextFile("lpep_dropoff_datetime_g.out")
        sc.stop()

    except:
        words = str(sys.argv[1]).split("|")
        for word in words:
            print(check_Lpep_dropoff_datetime(word))
    return


if __name__ == '__main__':
    main()