
# Data Visualizations
This folder contains datapoints and scripts for visualizing the related data.

Useda visuzlize_2.r and vizUber.r to plot the data for figures 47-63, excluding 56.


For Figure 56, please plot "tip and total amounts per date yellow cab.out" via excel.

`pull_green_data.py` and `pull_yellow_data.py` generate the datafiles used by the R scripts. These scripts contain several data generation scripts:

`pull_green_data.py`:
- Look at hourly trip data for green cabs 
- GREEN CAB DATA OVERALL
- GREEN CAB DATA BY YEAR
- GREEN CAB DATA BY hour by date

`pull_yellow_data.py`:
- look at hourly trip data from the drop on 2/22/2014
- YELLOW CAB DATA OVERALL
- YELLOW CAB DATA BY YEAR
- YELLOW CAB DATA BY hour by date


Please first edit these scripts to direct to the correct data file on HDFS, then run using spark2-submit on NYU's HPC dumbo.
`spark2-submit pull_green_data.py`
`spark2-submit pull_yellow_data.py`

