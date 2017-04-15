"""
WRITTEN IN PYTHON 3
FUNCTIONS TO DETERMINE DATATYPES FOR YELLOW DATA COLUMNS
USE IN check_yellow_data.py file 
"""

import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SQLContext
import csv
import os
from datetime import datetime as dt


def check_VendorID(input_datapoint):
    """
    Applies to first column in processed data 
    """
    if input_datapoint in ['VTS', 'CMT']:
        base_type="TEXT"
        semantic_type="Depreciated Version of encoding Vendor IDs"
        qual_type="Valid"
    elif input_datapoint in [""]:
        base_type="TEXT"
        semantic_type="String"
        qual_type="NULL"
    else:
        try:
            intinp = int(input_datapoint)
            if intinp in [1, 2]:
                base_type = "INT"
                semantic_type= "Integer"
                qual_type="VALID"
            elif intinp in [3]:
                base_type = "INT"
                semantic_type= "Integer"
                qual_type="NULL"
        except:
            base_type=str(type(input_datapoint))
            semantic_type="Unknown"
            qual_type="INVALID"
    return [input_datapoint, base_type, semantic_type, qual_type]


def check_tpep_pickup_datetime(input_datapoint):
    # sample valid datapoint:  '2015-07-01 00:01:10'
    try:
        dto = dt.strptime(input_datapoint, '%Y-%m-%d %H:%M:%S')
        #dto.year, dto.month, dto.day, dto.minute, dto.second, dto.hour        
        if dto.year in [2013, 2014,2015,2016]:
            base_type="TIMESTAMP"
            semantic_type="Timestamp"
            qual_type="Valid"
        else:
            base_type="timestamp"
            semantic_type="Timestamp"
            qual_type="Invalid/Outlier"
    except:
        if input_datapoint=='':
            base_type="TEXT"
            semantic_type="Empty Value"
            qual_type="Null"
        else:
            base_type=type(input_datapoint)
            semantic_type="Unknown"
            qual_type="Invalid"
    
    return [input_datapoint, base_type, semantic_type, qual_type]


def check_tpep_dropoff_datetime(input_datapoint):
    try:
        dto = dt.strptime(input_datapoint, '%Y-%m-%d %H:%M:%S')
        #dto.year, dto.month, dto.day, dto.minute, dto.second, dto.hour        
        if dto.year in [2013, 2014,2015,2016]:
            base_type="timestamp"
            semantic_type="Timestamp"
            qual_type="Valid"
        else:
            base_type="timestamp"
            semantic_type="Timestamp"
            qual_type="Invalid/Outlier"
    except:
        if input_datapoint=='':
            base_type="TEXT"
            semantic_type="Empty Value"
            qual_type="Null"
        else:
            base_type=type(input_datapoint)
            semantic_type="Unknown"
            qual_type="Invalid"
    
    return [input_datapoint, base_type, semantic_type, qual_type]

def check_passenger_count(input_datapoint):
    if input_datapoint in ['1','2','3','4','5','6']:
        base_type = "Integer"
        semantic_type= "Number of Passengers"
        qual_type="Valid"
    elif input_datapoint=='0':
        base_type = "Integer"
        semantic_type= "Number of Passengers"
        qual_type="Null"
    else:
        base_type=type(input_datapoint)
        semantic_type="Unknown"
        qual_type="Invalid"
    return [input_datapoint, base_type, semantic_type, qual_type]

def check_trip_distance(input_datapoint):
    try:
        flip = float(input_datapoint)
        if flip==0:
            base_type="Integer"
            semantic_type="No Distance Recorded"
            qual_type="Null"  
            
    except:
        if input_datapoint=="":
            base_type="TEXT"
            semantic_type="No Entry"
            qual_type="Null"
        
    return [input_datapoint, base_type, semantic_type, qual_type]

def check_pickup_longitude(input_datapoint):
    try:
        flip = float(input_datapoint)
        if -77 <= flip <=  -73:
            base_type="Float"
            semantic_type="Longitude"
            qual_type="Valid"  
        elif flip==0:
            base_type="Float"
            semantic_type="No Longitude Provided"
            qual_type="Null"  
        else:
            base_type="Float"
            semantic_type="Longitude out of taxi range"
            qual_type="Invalid/Outlier"  
    except:
        if input_datapoint in ["", "null"]:
            base_type="TEXT"
            semantic_type="No Entry"
            qual_type="Null"
        else:
            base_type=type(input_datapoint)
            semantic_type="Invalid Flag"
            qual_type="Invalid"
        
    return [input_datapoint, base_type, semantic_type, qual_type]


def check_pickup_latitude(input_datapoint):
    try:
        flip = float(input_datapoint)
        if 37 <= flip <=  42:
            base_type="Float"
            semantic_type="Latitude"
            qual_type="Valid"  
        elif flip==0:
            base_type="Float"
            semantic_type="No Latitude Provided"
            qual_type="Null"  
        else:
            base_type="Float"
            semantic_type="Latitude out of taxi range"
            qual_type="Invalid/Outlier"  
    except:
        if input_datapoint in ["", "null"]:
            base_type="TEXT"
            semantic_type="No Entry"
            qual_type="Null"
        else:
            base_type=type(input_datapoint)
            semantic_type="Invalid Flag"
            qual_type="Invalid"
        
    return [input_datapoint, base_type, semantic_type, qual_type]



def check_RateCodeID(input_datapoint):
    if int(input_datapoint) in [1,2,3,4,5,6]:
        base_type="Integer"
        semantic_type="Type of Rate: "
        qual_type="Valid"
    elif int(input_datapoint)==99:
        base_type="Integer"
        semantic_type="Ratecode not input"
        qual_type="Null"
    elif int(input_datapoint)==0:
        base_type="Integer"
        semantic_type="Ratecode not input- prior to 2015"
        qual_type="Null"
    else:
        base_type=type(input_datapoint)
        semantic_type="Invalid Ratecode ID"
        qual_type="Invalid"
    return [input_datapoint, base_type, semantic_type, qual_type]


def check_store_and_fwd_flag(input_datapoint):
    if input_datapoint in ["Y", "N"]:
        base_type="TEXT"
        semantic_type="Boolean Indication of delay in data transmission"
        qual_type="Valid"
    elif input_datapoint== "":
        base_type = "TEXT"
        semantic_type="Missing Value"
        qual_type="Null"
    else:
        base_type=type(input_datapoint)
        semantic_type="Invalid Flag"
        qual_type="Invalid"

    return [input_datapoint, base_type, semantic_type, qual_type]


def check_dropoff_longitude(input_datapoint):
    try:
        flip = float(input_datapoint)
        if -77 <= flip <=  -73:
            base_type="Float"
            semantic_type="Longitude"
            qual_type="Valid"  
        elif flip==0:
            base_type="Float"
            semantic_type="No Longitude Provided"
            qual_type="Null"  
        else:
            base_type="Float"
            semantic_type="Longitude out of taxi range"
            qual_type="Invalid/Outlier"  
    except:
        if input_datapoint in ["", "null"]:
            base_type="TEXT"
            semantic_type="No Entry"
            qual_type="Null"
        else:
            base_type=type(input_datapoint)
            semantic_type="Invalid Flag"
            qual_type="Invalid"
        
    return [input_datapoint, base_type, semantic_type, qual_type]


def check_dropoff_latitude(input_datapoint):
    #37 and 42, 
    try:
        flip = float(input_datapoint)
        if 37 <= flip <=  42:
            base_type="Float"
            semantic_type="Latitude"
            qual_type="Valid"  
        elif flip==0:
            base_type="Float"
            semantic_type="No Latitude Provided"
            qual_type="Null"  
        else:
            base_type="Float"
            semantic_type="Latitude out of taxi range"
            qual_type="Invalid/Outlier"  
    except:
        if input_datapoint in ["", "null"]:
            base_type="TEXT"
            semantic_type="No Entry"
            qual_type="Null"
        else:
            base_type=type(input_datapoint)
            semantic_type="Invalid Flag"
            qual_type="Invalid"
        
    return [input_datapoint, base_type, semantic_type, qual_type]


def check_payment_type(input_datapoint):
    if str(input_datapoint) in ['1', '2', '3', '4']:
        base_type="Integer"
        semantic_type="Code of Payment Type"
        qual_type="Valid"
    elif input_datapoint=='':
        base_type="TEXT"
        semantic_type="no code provided"
        qual_type="Null"       
    else:
        base_type=type(input_datapoint)
        semantic_type="Invalid Flag"
        qual_type="Invalid"

    return [input_datapoint, base_type, semantic_type, qual_type]


def check_fare_amount(input_datapoint):
    try:
        flip = float(input_datapoint)
        if 2.25 <= flip <= 120:
            base_type="Float"
            semantic_type="Fare Amount"
            qual_type="Valid"  
        elif flip == 0:
            base_type="Float"
            semantic_type="Fare Amount"
            qual_type="Null"  
        elif flip < 2.25:
            base_type="Float"
            semantic_type="Too Small Fare Amount"
            qual_type="Invalid"  
        else:
            base_type="Float"
            semantic_type="Too Large Fare Amount"
            qual_type="Invalid"             
    except:
        if input_datapoint=="":
            base_type="TEXT"
            semantic_type="No Entry"
            qual_type="Null"
        else:
            base_type=type(input_datapoint)
            semantic_type="Invalid Flag"
            qual_type="Invalid"
        
    return [input_datapoint, base_type, semantic_type, qual_type]

def check_extra(input_datapoint):
    try:
        flip = float(input_datapoint)        
        if flip in [0.5, 1]:
            base_type="Float"
            semantic_type="Extra Charges"
            qual_type="Valid"
        elif flip < 0:
            base_type="Float"
            semantic_type="Negative Charge"
            qual_type="Invalid"
        elif flip == 0:
            base_type="Float"
            semantic_type="No Extra Charge"
            qual_type="Null"            
        else:
            base_type="Float"
            semantic_type="Incorrect Charge"
            qual_type="Invalid/Outlier" 
    except:
        if input_datapoint=="":
            base_type="TEXT"
            semantic_type="No Entry"
            qual_type="Null"
        else:
            base_type=type(input_datapoint)
            semantic_type="Invalid Tax Entry"
            qual_type="Invalid"

    return [input_datapoint, base_type, semantic_type, qual_type]


def check_mta_tax(input_datapoint):
    try:
        flip = float(input_datapoint)        
        if flip in [0.5, 1]:
            base_type="Float"
            semantic_type="MTA Tax Cost"
            qual_type="Valid"
        elif flip < 0:
            base_type="Float"
            semantic_type="Negative MTA Tax Cost"
            qual_type="Invalid"
        else:
            base_type="Float"
            semantic_type="Incorrect MTA Tax Cost"
            qual_type="Invalid/Outlier" 
    except:
        if input_datapoint=="":
            base_type="TEXT"
            semantic_type="No Entry"
            qual_type="Null"
        else:
            base_type=type(input_datapoint)
            semantic_type="Invalid Tax Entry"
            qual_type="Invalid"
    return [input_datapoint, base_type, semantic_type, qual_type]

def check_tip_amount(input_datapoint):
    try:
        flip = float(input_datapoint)
        if flip==0:
            base_type="Integer"
            semantic_type="No or Cash Tip"
            qual_type="Null" 
        elif flip < 0:
            base_type="Float"
            semantic_type="Negative Tip"
            qual_type="Invalid"

        else:
            base_type="Float"
            semantic_type="Incorrect Tip Amount"
            qual_type="Invalid/Outlier" 

    except:
        if input_datapoint=="":
            base_type="TEXT"
            semantic_type="No Entry"
            qual_type="Null"
            
    return [input_datapoint, base_type, semantic_type, qual_type]



def check_tolls_amount(input_datapoint):
    try:
        flip = float(input_datapoint)
        if flip in [5.50]:
            base_type="Float"
            semantic_type="Tolls"
            qual_type="Valid"
        elif flip==0:
            base_type="Float"
            semantic_type="No Toll"
            qual_type="Valid"  
        else:
            base_type="Float"
            semantic_type="Tolls"
            qual_type="Invalid"
    except:
        if input_datapoint=="":
            base_type="TEXT"
            semantic_type="No Entry"
            qual_type="Null"
        else:
            base_type=type(input_datapoint)
            semantic_type="Tolls"
            qual_type="Invalid"
            
        
    return [input_datapoint, base_type, semantic_type, qual_type]


def check_improvement_surcharge(input_datapoint):
    try:
        flip = float(input_datapoint)
    except:
        if input_datapoint=="":
            base_type="TEXT"
            semantic_type="No Entry"
            qual_type="Null"
    return [input_datapoint, base_type, semantic_type, qual_type]


def check_total_amount(input_datapoint):
    try:
        flip = float(input_datapoint)
    except:
        if input_datapoint=="":
            base_type="TEXT"
            semantic_type="No Entry"
            qual_type="Null"
        
    return [input_datapoint, base_type, semantic_type, qual_type]

def check_PULocationID(input_datapoint):
    if input_datapoint in [str(x) for x in range(1,266)]:
        base_type="Integer"
        semantic_type="Pickup Location ID"
        qual_type="Valid"  
    
    elif input_datapoint in ["null", ""]:
        base_type="TEXT"
        semantic_type="Older data without code"
        qual_type="Null"  
    else:
        base_type=type(input_datapoint)
        semantic_type="Invalid Flag"
        qual_type="Invalid"
 
    return [input_datapoint, base_type, semantic_type, qual_type]


def check_DOLocationID(input_dataline, locidix):
    if input_datapoint in [str(x) for x in range(1,266)]:
        base_type="Integer"
        semantic_type="Pickup Location ID"
        qual_type="Valid"  
    
    elif input_datapoint in ["null", ""]:
        base_type="TEXT"
        semantic_type="Older data without code"
        qual_type="Null"  
    else:
        base_type=type(input_datapoint)
        semantic_type="Invalid Flag"
        qual_type="Invalid"

    return [input_datapoint, base_type, semantic_type, qual_type]

