"""
Check data quality in Trip_type column of Green Cab Data
Note this column does not exist in 2013-2016 yellow cab data
"""

import sys
import check_green_data as gd
from pyspark import SparkContext


def check_Trip_type(input_datapoint):
    try:
        intinp = int(input_datapoint)
        if intinp in [1,2,3,4,5,6]:
            base_type="INTEGER"
            semantic_type="Rate code"
            qual_type="VALID"
        elif intinp==99:
            base_type="INTEGER"
            semantic_type="Integer"
            qual_type="NULL"
        elif intinp==0:
            base_type="INTEGER"
            semantic_type="Integer"
            qual_type="NULL"
        else:
            base_type="INTEGER"
            semantic_type="Integer"
            qual_type="INVALID"      
    except:
        base_type=type(input_datapoint)
        semantic_type="Invalid Ratecode ID"
        qual_type="Invalid"
    return [input_datapoint, base_type, semantic_type, qual_type]


def main():
    # input data (point or path) is sys.argv[1]
    # if sys.argv[2] is passed, it will be a path to green data
  
    try:
        check_green_data_flag = sys.argv[2]
        sc = SparkContext()
        g_data = gd.gd_processing(sc, sys.argv[1])
        mapped_g_data = g_data.map(lambda x: check_Trip_type(x[20]))
        print("SAMPLE GREEN CAB DATA OUTPUT: \n")
        print(mapped_g_data.take(20))
        #if filename:
        #    print("Saving Mapped Data to file: {0}".format(filename))
        #    mapped_data.write.csv(filename)
        sc.stop()

    except:
        words = str(sys.argv[1]).split("|")
        for word in words:
            print(check_Trip_type(word))
    return


if __name__ == '__main__':
    main()