### Check data quality in vendorID (or vendor_id) column
import sys
import check_yellow_data as yd
import check_green_data as gd
from pyspark import SparkContext

def check_VendorID(input_datapoint):
    """
    Applies to first column in processed data 
    """
    if input_datapoint in ['VTS', 'CMT']:
        base_type="TEXT"
        semantic_type="Acronym"
        qual_type="VALID"
    elif input_datapoint in [""]:
        base_type="TEXT"
        semantic_type="Empty string"
        qual_type="NULL"
    else:
        try:
            intinp = int(input_datapoint)
            if intinp in [1, 2]:
                base_type = "INTEGER"
                semantic_type= "Code"
                qual_type="VALID"
            elif intinp in [3]:
                base_type = "INTEGER"
                semantic_type= "Code"
                qual_type="NULL"
        except:
            base_type=str(type(input_datapoint))
            semantic_type="Unknown"
            qual_type="INVALID"
    return [input_datapoint, base_type, semantic_type, qual_type]



def main():
    # input data (point or path) is sys.argv[1]
    # if sys.argv[2] is passed, it will be a path to green data
  
    try:
        green_data_path = sys.argv[2]
        sc = SparkContext()

        y_data = yd.yc_processing(sc, sys.argv[1])
        mapped_y_data = y_data.map(lambda x: check_VendorID(x[0]))
        print("SAMPLE YELLOW CAB DATA OUTPUT: \n")
        print(mapped_y_data.take(20))
        
        g_data = gd.gd_processing(sc, green_data_path)
        mapped_g_data = g_data.map(lambda x: check_VendorID(x[0]))
        print("SAMPLE GREEN CAB DATA OUTPUT: \n")
        print(mapped_g_data.take(20))
        
        print("saving all outputs to files")
        mapped_y_data.saveAsTextFile("VendorID_y.out")
        mapped_g_data.saveAsTextFile("VendorID_g.out")
        sc.stop()

    except:
        words = str(sys.argv[1]).split("|")
        for word in words:
            print(check_VendorID(word))
    return


if __name__ == '__main__':
    main()
