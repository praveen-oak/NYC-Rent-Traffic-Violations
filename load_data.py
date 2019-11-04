from __future__ import print_function
import os
import calendar
import time
import requests


hdfs_folder_path = "/user/ppo208/project"
temp_folder = "/tmp/ppo208/"
def upload_file_to_hdfs(local_file_path, local_filename, hadoop_filename):
	print("Uploading file to hdfs folder = "+hdfs_folder_path)
	os.chdir(local_file_path)
	os.system("hdfs dfs -put "+temp_folder+local_filename+" "+hdfs_folder_path)
	os.system("hdfs dfs -mv "+hdfs_folder_path+"/"+local_filename+" "+hdfs_folder_path+"/"+hadoop_filename)

def download_file(url, local_filename):
	response = requests.get(url, stream=True)
	print("Downloading url "+url)
	file_size = float(response.headers['Content-length'])
	print("File size is "+str(file_size/(1024*1024))+"MB.")
	print("File path is "+local_filename)
	downloaded = 0
	fp = open(local_filename, 'wb')
	for chunk in response.iter_content(chunk_size=8192):
		downloaded = downloaded + 8192
		percent = int(round(downloaded/file_size, 2)*100)
		print("%d percent downloaded " %(percent), end="\r")
		fp.write(chunk)
		fp.flush
	fp.close()

def process_link(url, hdfs_filename):
	temp_filename = str(calendar.timegm(time.gmtime()))
	local_filename = download_file(url, temp_folder+temp_filename)
	upload_file_to_hdfs(temp_folder, temp_filename, hdfs_filename)

if __name__== "__main__":
  process_link("https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2009-01.csv", "Jan_2009.csv")


