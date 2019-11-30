from __future__ import print_function
import os
import calendar
import time
import requests
from constants import months
from constants import years
from constants import hdfs_folder_path
from constants import temp_folder
from constants import base_url
import subprocess

#scipt to take the taxi dataset for the last 10 years from the web and upload to hdfs

#function that takes a local file and uploads it to hdfs
def upload_file_to_hdfs(local_file_path, local_filename, hadoop_filename):
	print("Uploading file to hdfs folder = "+hdfs_folder_path)
	os.chdir(local_file_path)
	os.system("hdfs dfs -put "+temp_folder+local_filename+" "+hdfs_folder_path)
	os.system("hdfs dfs -mv "+hdfs_folder_path+local_filename+" "+hdfs_folder_path+hadoop_filename)

#function that downloads a file from the link in chunks and stores it in a temp folder
def download_file(url, local_filename):
	response = requests.get(url, stream=True)
	print("Downloading url "+url)
	file_size = float(response.headers['Content-length'])
	print("File size is "+str(file_size/(1024*1024))+"MB.")
	print("File path is "+local_filename)
	downloaded = 0
	fp = open(local_filename, 'w')
	for chunk in response.iter_content(chunk_size=8192):
		downloaded = downloaded + 8192
		percent = int(round(downloaded/file_size, 2)*100)
		print("%d percent downloaded " %(percent), end="\r")
		fp.write(chunk)
		fp.flush
	fp.close()

#function that takes a file and moves the data to a given hdfs file
def process_link(url, hdfs_filename):
	temp_filename = str(calendar.timegm(time.gmtime()))
	local_filename = download_file(url, temp_folder+temp_filename)
	upload_file_to_hdfs(temp_folder, temp_filename, hdfs_filename)
	#delete file from system
	os.system("rm -rf "+temp_folder+temp_filename)
	print("Successfully processed data in link "+url+" to hdfs ")

#main routine
def download_all_data():
	# os.system("hdfs dfs -rmr "+hdfs_folder_path)
	# os.system("hdfs dfs -mkdir "+hdfs_folder_path)

	for year in years:
		for month in months:
			raw_folder_path = hdfs_folder_path+year+"_"+month
			hdfs_file_check = "hdfs dfs -test -e "+raw_folder_path+"; echo $?"
			is_present = int(subprocess.Popen(hdfs_file_check,shell=True,stdout=subprocess.PIPE).communicate()[0][0])
			if is_present == 0:
				continue
			print("running for "+year+"_"+month)
			url = base_url+year+"-"+month+".csv"
			hdfs_filename = year+"_"+month
			process_link(url, hdfs_filename)


if __name__== "__main__":
  download_all_data()


