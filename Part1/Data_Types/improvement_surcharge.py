"""
Check the improvement_surcharge column in green and yellow cab data
August 2013 - December 2016

"""
import sys
import check_yellow_data as yd
import check_green_data as gd
from pyspark import SparkContext


def check_improvement_surcharge(input_datapoint):
    try:
        flip = float(input_datapoint)
    except:
        if input_datapoint=="":
            base_type="TEXT"
            semantic_type="No Entry"
            qual_type="Null"
    return [input_datapoint, base_type, semantic_type, qual_type]


def main():
    # input data (point or path) is sys.argv[1]
    # if sys.argv[2] is passed, it will be a path to green data
  
    try:
        green_data_path = sys.argv[2]
        sc = SparkContext()

        y_data = yd.yc_processing(sc, sys.argv[1])
        mapped_y_data = y_data.map(lambda x: check_improvement_surcharge(x[17]))
        print("SAMPLE YELLOW CAB DATA OUTPUT: \n")
        print(mapped_y_data.take(20))
        
        g_data = gd.gd_processing(sc, green_data_path)
        mapped_g_data = g_data.map(lambda x: check_improvement_surcharge(x[17]))
        print("SAMPLE GREEN CAB DATA OUTPUT: \n")
        print(mapped_g_data.take(20))
        #if filename:
        #    print("Saving Mapped Data to file: {0}".format(filename))
        #    mapped_data.write.csv(filename)
        sc.stop()

    except:
        words = str(sys.argv[1]).split("|")
        for word in words:
            print(check_improvement_surcharge(word))
    return


if __name__ == '__main__':
    main()
