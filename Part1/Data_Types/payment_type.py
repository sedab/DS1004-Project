import sys
import check_yellow_data as yd
import check_green_data as gd
from pyspark import SparkContext


def check_payment_type(input_datapoint):
    try:
        intinp = int(input_datapoint)
        if  intinp in range(1,5):
            base_type="INTEGER"
            semantic_type="Code of Payment Type"
            qual_type="VALID"
        elif intinp==0:
            base_type="INTEGER"
            semantic_type="INTEGER"
            qual_type="NULL"
     
    except:
        if input_datapoint=='':
            base_type="TEXT"
            semantic_type="no code provided"
            qual_type="NULL"       
        else:
            base_type=type(input_datapoint)
            semantic_type="Invalid Flag"
            qual_type="INVALID"

    return [input_datapoint, base_type, semantic_type, qual_type]


def main():
    # input data (point or path) is sys.argv[1]
    # if sys.argv[2] is passed, it will be a path to green data
  
    try:
        green_data_path = sys.argv[2]
        sc = SparkContext()

        y_data = yd.yc_processing(sc, sys.argv[1])
        mapped_y_data = y_data.map(lambda x: check_payment_type(x[11]))
        print("SAMPLE YELLOW CAB DATA OUTPUT: \n")
        print(mapped_y_data.take(20))
        
        g_data = gd.gd_processing(sc, green_data_path)
        mapped_g_data = g_data.map(lambda x: check_payment_type(x[19]))
        print("SAMPLE GREEN CAB DATA OUTPUT: \n")
        print(mapped_g_data.take(20))
        
        print("saving all outputs to files")
        mapped_y_data.saveAsTextFile("payment_type_y.out")
        mapped_g_data.saveAsTextFile("payment_type_g.out")
        sc.stop()

    except:
        words = str(sys.argv[1]).split("|")
        for word in words:
            print(check_payment_type(word))
    return


if __name__ == '__main__':
    main()
