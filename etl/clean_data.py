
import os
from constants import months
from constants import years
from constants import hdfs_folder_path, hdfs_cleaned_data_folder_path
import subprocess

#calls the pig command for each month and year
def clean_data():
	for year in years:
		for month in months:
				clean_folder_path = hdfs_cleaned_data_folder_path+year+"_"+month
				hdfs_file_check = "hdfs dfs -test -e "+clean_folder_path+"; echo $?"
				is_present = int(subprocess.Popen(hdfs_file_check,shell=True,stdout=subprocess.PIPE).communicate()[0][0])
				# if is_present == 0:
				# 	continue
				input_file = year+"_"+month
				output_file = year+"_"+month
				# print("Running for "+year+"_"+month)
				os.system("pig -f data_clean.pig "+
							"-param input_file='"+hdfs_folder_path+input_file+"'"+
							" -param output_file='"+hdfs_cleaned_data_folder_path+output_file+"'")
			
if __name__== "__main__":
  clean_data()