"""
Check data quality in Ehail_fee column of Green Cab Data
Note this column does not exist in 2013-2016 yellow cab data
"""
import sys
import check_green_data as gd
from pyspark import SparkContext

def check_Ehail_fee(input_datapoint):
    try:
        flip = float(input_datapoint)
        if flip < 5:
            base_type = "FLOAT"
            semantic_type= "Currency"
            qual_type="VALID"
        elif flip == 0:
            base_type = "INT"
            semantic_type= "Integer"
            qual_type="NULL"
    except:
        if input_datapoint == "":
            base_type = "TEXT"
            semantic_type = "No Entry"
            qual_type = "NULL"
        else:
            base_type=str(type(input_datapoint))
            semantic_type="Unknown"
            qual_type="INVALID"
    return [input_datapoint, base_type, semantic_type, qual_type]

import sys
import check_green_data as gd
from pyspark import SparkContext



def main():
    # input data (point or path) is sys.argv[1]
    # if sys.argv[2] is passed, it will be a path to green data
  
    try:
        check_green_data_flag = sys.argv[2]
        sc = SparkContext()
        
        g_data = gd.gd_processing(sc, sys.argv[1])
        mapped_g_data = g_data.map(lambda x: check_Ehail_fee(x[16]))
        print("SAMPLE GREEN CAB DATA OUTPUT: \n")
        print(mapped_g_data.take(20))
        
        print("saving all outputs to files")
        mapped_y_data.saveAsTextFile("Ehail_fee_y.out")
        mapped_g_data.saveAsTextFile("Ehail_fee_g.out")
        sc.stop()

    except:
        words = str(sys.argv[1]).split("|")
        for word in words:
            print(check_Ehail_fee(word))
    return


if __name__ == '__main__':
    main()
