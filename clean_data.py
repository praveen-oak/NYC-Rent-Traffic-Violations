
import os
from constants import months
from constants import years
from constants import hdfs_folder_path, hdfs_cleaned_data_folder_path

#calls the pig command for each month and year
def clean_data():
	for year in years:
		for month in months:
				input_file = year+"_"+month
				output_file = year+"_"+month
				try:
					os.system("hdfs dfs -rmr "+hdfs_cleaned_data_folder_path+output_file)
				except:
					pass
				os.system("pig -f data_clean.pig "+
							"-param input_file='"+hdfs_folder_path+input_file+"'"+
							" -param output_file='"+hdfs_cleaned_data_folder_path+output_file+"'")
			
if __name__== "__main__":
  clean_data()