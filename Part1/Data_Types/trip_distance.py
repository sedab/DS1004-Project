import sys
import check_yellow_data as yd
import check_green_data as gd
from pyspark import SparkContext


def check_trip_distance(input_datapoint):
    try:
        flip = float(input_datapoint)
        if flip==0:
            base_type="FLOAT"
            semantic_type="Distance"
            qual_type="NULL"
        elif flip < 0:
            base_type="FLOAT"
            semantic_type="Distance"
            qual_type="INVALID"
        elif flip > 1000:
            base_type="FLOAT"
            semantic_type="Distance"
            qual_type="INVALID"
        else:  
            base_type="FLOAT"
            semantic_type="Distance"
            qual_type="VALID"
            
    except:
        if input_datapoint=="":
            base_type="TEXT"
            semantic_type="No Entry"
            qual_type="NULL"
        
    return [input_datapoint, base_type, semantic_type, qual_type]


def main():
    # input data (point or path) is sys.argv[1]
    # if sys.argv[2] is passed, it will be a path to green data
  
    try:
        green_data_path = sys.argv[2]
        sc = SparkContext()

        y_data = yd.yc_processing(sc, sys.argv[1])
        mapped_y_data = y_data.map(lambda x: check_trip_distance(x[4]))
        print("SAMPLE YELLOW CAB DATA OUTPUT: \n")
        print(mapped_y_data.take(20))
        
        g_data = gd.gd_processing(sc, green_data_path)
        mapped_g_data = g_data.map(lambda x: check_trip_distance(x[10]))
        print("SAMPLE GREEN CAB DATA OUTPUT: \n")
        print(mapped_g_data.take(20))
        
        print("saving all outputs to files")
        mapped_y_data.saveAsTextFile("trip_distance_y.out")
        mapped_g_data.saveAsTextFile("trip_distance_g.out")
        sc.stop()

    except:
        words = str(sys.argv[1]).split("|")
        for word in words:
            print(check_trip_distance(word))
    return


if __name__ == '__main__':
    main()
