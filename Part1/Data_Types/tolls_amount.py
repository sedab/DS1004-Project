### Check data quality in vendorID (or vendor_id) column
import sys
import check_yellow_data as yd
import check_green_data as gd
from pyspark import SparkContext


def check_tolls_amount(input_datapoint):
    try:
        flip = float(input_datapoint)
        if flip in [5.33, 5.50, 5.51, 5.52, 5.53, 5.54, 5.55, 5.56, 5.57,  5.58, 5.59]:
            base_type="FLOAT"
            semantic_type="Toll"
            qual_type="VALID"
        elif flip==0:
            base_type="FLOAT"
            semantic_type="No Toll"
            qual_type="VALID"  
        else:
            base_type="FLOAT"
            semantic_type="Currency"
            qual_type="INVALID"
    except:
        if input_datapoint=="":
            base_type="TEXT"
            semantic_type="No Entry"
            qual_type="NULL"
        else:
            base_type=type(input_datapoint)
            semantic_type="Tolls"
            qual_type="INVALID"
            
        
    return [input_datapoint, base_type, semantic_type, qual_type]



def main():
    # input data (point or path) is sys.argv[1]
    # if sys.argv[2] is passed, it will be a path to green data
  
    try:
        green_data_path = sys.argv[2]
        sc = SparkContext()

        y_data = yd.yc_processing(sc, sys.argv[1])
        mapped_y_data = y_data.map(lambda x: check_tolls_amount(x[16]))
        print("SAMPLE YELLOW CAB DATA OUTPUT: \n")
        print(mapped_y_data.take(20))
        
        #print(y_data.map(lambda x: (x[16], 1)).reduceByKey(lambda x,y: x+y).take(20)) 
        g_data = gd.gd_processing(sc, green_data_path)
        mapped_g_data = g_data.map(lambda x: check_tolls_amount(x[15]))
        print("SAMPLE GREEN CAB DATA OUTPUT: \n")
        print(mapped_g_data.take(20))
        #if filename:
        #    print("Saving Mapped Data to file: {0}".format(filename))
        #    mapped_data.write.csv(filename)
        sc.stop()

    except:
        words = str(sys.argv[1]).split("|")
        for word in words:
            print(check_tolls_amount(word))
    return


if __name__ == '__main__':
    main()
