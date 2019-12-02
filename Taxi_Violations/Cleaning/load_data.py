from __future__ import print_function
import os
import requests
import subprocess

years = ['2017', '2018']
months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
hdfs_folder_path = '/user/ak7380/TaxiData/'
temp_folder = '/scratch/ak7380/TaxiData/'
base_url = 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_'

#function that takes a local file and uploads it to hdfs
def upload_file_to_hdfs(local_file_path, local_filename, hdfs_folder_path, hadoop_filename):
	print("Uploading file to hdfs folder = "+hdfs_folder_path)
	os.chdir(local_file_path)
	os.system("hdfs dfs -put "+local_file_path+local_filename+" "+hdfs_folder_path)

#function that downloads a file from the link in chunks and stores it in a temp folder
def download_file(url, local_filename):
	response = requests.get(url, stream=True, verify=False)
	print("Downloading url "+url)
	length = False
	if(response.headers.get('Content-length') != None):
		length = True
	if(length):
		file_size = float(response.headers.get('Content-length'))
		print("File size is "+str(file_size/(1024*1024))+"MB.")
	print("File path is "+local_filename)
	downloaded = 0
	fp = open(local_filename, 'w')
	for chunk in response.iter_content(chunk_size=8192):
		downloaded = downloaded + 8192
		if(length):
			percent = int(round(downloaded/file_size, 2)*100)
			print("%d percent downloaded " %(percent), end="\r")
		else:
			print("Downloading", end="\r")
		fp.write(chunk)
		fp.flush
	fp.close()

#function that takes a file and moves the data to a given hdfs file
def process_link(url, temp_folder, hdfs_folder_path, hdfs_filename):
	download_file(url, temp_folder+hdfs_filename)
	upload_file_to_hdfs(temp_folder, hdfs_filename, hdfs_folder_path, hdfs_filename)
	print("Successfully processed data in link "+url+" to hdfs ")

#main routine
def download_all_data():
	process_link('https://nycopendata.socrata.com/api/views/nc67-uf89/rows.csv?accessType=DOWNLOAD', '/scratch/ak7380/Violations/', '/user/ak7380/Violations/', 'violations.csv')
	for year in years:
		for month in months:
			print("running for "+year+"_"+month)
			url = base_url+year+"-"+month+".csv"
			hdfs_filename = year+"_"+month+".csv"
			process_link(url, temp_folder, hdfs_folder_path, hdfs_filename)

if __name__== "__main__":
  download_all_data()
