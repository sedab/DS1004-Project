"""
This file processes the "extra" column in the green and yellow cab data
August 2013 - Dec 2016

"""

import sys
import check_yellow_data as yd
import check_green_data as gd
from pyspark import SparkContext


def check_extra(input_datapoint):
    try:
        flip = float(input_datapoint)        
        if flip in [0.5, 1]:
            base_type="FLOAT"
            semantic_type="Currency"
            qual_type="VALID"
        elif flip < 0:
            base_type="FLOAT"
            semantic_type="Currency"
            qual_type="INVALID"
        elif flip == 0:
            base_type="FLOAT"
            semantic_type="Integer"
            qual_type="NULL"            
        else:
            base_type="FLOAT"
            semantic_type="Currency"
            qual_type="INVALID/OUTLIER" 
    except:
        if input_datapoint=="":
            base_type="TEXT"
            semantic_type="No Entry"
            qual_type="NULL"
        else:
            base_type=type(input_datapoint)
            semantic_type="Invalid Tax Entry"
            qual_type="INVALID"
    return [input_datapoint, base_type, semantic_type, qual_type]



def main():
    # input data (point or path) is sys.argv[1]
    # if sys.argv[2] is passed, it will be a path to green data
  
    try:
        green_data_path = sys.argv[2]
        sc = SparkContext()

        y_data = yd.yc_processing(sc, sys.argv[1])
        mapped_y_data = y_data.map(lambda x: check_extra(x[13]))
        print("SAMPLE YELLOW CAB DATA OUTPUT: \n")
        print(mapped_y_data.take(20))
        
        g_data = gd.gd_processing(sc, green_data_path)
        mapped_g_data = g_data.map(lambda x: check_extra(x[12]))
        print("SAMPLE GREEN CAB DATA OUTPUT: \n")
        print(mapped_g_data.take(20))
        
        print("saving all outputs to files")
        mapped_y_data.saveAsTextFile("extra_y.out")
        mapped_g_data.saveAsTextFile("extra_g.out")
        sc.stop()

    except:
        words = str(sys.argv[1]).split("|")
        for word in words:
            print(check_extra(word))
    return


if __name__ == '__main__':
    main()

