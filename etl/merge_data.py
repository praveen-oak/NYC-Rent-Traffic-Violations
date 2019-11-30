import os
from constants import months
from constants import years
import subprocess
from constants import hdfs_cleaned_data_folder_path, hdfs_merged_data_folder_path, hdfs_merged_csv_folder_path


#scipt to clean the dataset loaded from the internet and clean and merge the data for different years

#main routine
def merge_all_data():

	os.chdir("../hadoopjobs/")
	os.system("./runner.sh")
	temp = 0
	for year in years:
		for month in months:
			month_int = int(month)-1
			year_int = int(year) - 2010
			index = year_int*12 + month_int + 3
			clean_data = hdfs_cleaned_data_folder_path+year+'_'+month+'/'
			merge_data = hdfs_merged_data_folder_path+year+'_'+month+'/'
			hdfs_file_check = "hdfs dfs -test -e "+merge_data+"; echo $?"
			is_present = int(subprocess.Popen(hdfs_file_check,shell=True,stdout=subprocess.PIPE).communicate()[0][0])
			if is_present == 0:
				continue
			median_rent_file = '/home/ppo208/project/etl/median_rent.csv'
			zone_lookup_file = '/home/ppo208/project/etl/zone_lookup_mapped.csv'
			# print("Running for "+year+"_"+month)
			os.system('hadoop jar merge.jar MergeData '+clean_data+' '+merge_data+'  '+median_rent_file+' '+zone_lookup_file+' '+str(index)+' &')



def merge_hadoop_files():
	hdfs_file_check = "hdfs dfs -test -e "+hdfs_merged_csv_folder_path+"; echo $?"
	is_present = int(subprocess.Popen(hdfs_file_check,shell=True,stdout=subprocess.PIPE).communicate()[0][0])
	if is_present == 1:
		os.system("hdfs dfs -mkdir "+hdfs_merged_csv_folder_path)

	for year in years:
		for month in months:
			merged_csv = '/user/ppo208/project/final_data/'+year+'_'+month+'.csv'
			merge_data = hdfs_merged_data_folder_path+year+"_"+month+"/"
			hdfs_file_check = "hdfs dfs -test -e "+merged_csv+"; echo $?"
			is_present = int(subprocess.Popen(hdfs_file_check,shell=True,stdout=subprocess.PIPE).communicate()[0][0])
			if is_present == 0:
				continue
			else:
				string = "hadoop fs -cat "+hdfs_merged_data_folder_path+year+"_"+month+"/* | hadoop fs -put - "+hdfs_merged_csv_folder_path+year+"_"+month+".csv"
				# print(string)
				try:
					os.system(string)
				except:
					print("Exception for "+year+"_"+month)
					pass



if __name__== "__main__":
  merge_all_data()
  merge_hadoop_files()
