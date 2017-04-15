import sys
import check_yellow_data as yd
import check_green_data as gd
from pyspark import SparkContext


def check_passenger_count(input_datapoint):
    if input_datapoint in ['1','2','3','4','5','6']:
        base_type = "INT"
        semantic_type= "Number of Passengers"
        qual_type="VALID"
    elif input_datapoint=='0':
        base_type = "Integer"
        semantic_type= "Number of Passengers"
        qual_type="NULL"
    else:
        base_type=type(input_datapoint)
        semantic_type="Unknown"
        qual_type="Invalid"
    return [input_datapoint, base_type, semantic_type, qual_type]



def main():
    # input data (point or path) is sys.argv[1]
    # if sys.argv[2] is passed, it will be a path to green data
  
    try:
        green_data_path = sys.argv[2]
        sc = SparkContext()

        y_data = yd.yc_processing(sc, sys.argv[1])
        mapped_y_data = y_data.map(lambda x: check_passenger_count(x[0]))
        print("SAMPLE YELLOW CAB DATA OUTPUT: \n")
        print(mapped_y_data.take(20))
        
        g_data = gd.gd_processing(sc, green_data_path)
        mapped_g_data = g_data.map(lambda x: check_passenger_count(x[0]))
        print("SAMPLE GREEN CAB DATA OUTPUT: \n")
        print(mapped_g_data.take(20))
        #if filename:
        #    print("Saving Mapped Data to file: {0}".format(filename))
        #    mapped_data.write.csv(filename)
        sc.stop()

    except:
        words = str(sys.argv[1]).split("|")
        for word in words:
            print(check_passenger_count(word))
    return


if __name__ == '__main__':
    main()