#!/bin/bash
#
# Syntax: extract_pace_data.sh YYYYMMDD YYYYMMDD
# e.g.	extract_pace_data.sh 20160101 20160107
#

# Set input variables
args=("$@")

echo "Extracting Ethan Diagnostics data for specified date range..."

date_from=${args[0]}
date_to=${args[1]}


if [ $# -eq 0 ] ; then
        echo "Default date range... extract for yesterday"

        date_from="$(date -d "-1 day" +"%Y%m%d")"
        date_to="$(date -d "-8 days" +"%Y%m%d")"
fi


echo $date_from
echo $date_to

i=$date_from


# Prepare empty file for concatenation
echo "rm all_Diagnostics_data.csv.gz"
rm all_Diagnostics_data.csv.gz

echo "touch all_Diagnostics_data.csv"
touch all_Diagnostics_data.csv

echo "gzip all_Diagnostics_data.csv"
gzip all_Diagnostics_data.csv


# Loop over target date range
while [ $(($i)) -le $(($date_to)) ]
do

	echo "Extracting for date $i"

	# List files in target date directory
	echo "hdfs dfs -du -h /datasets/stb_diagnostics/pace/test/p=$i/"
	hdfs dfs -du -h /datasets/stb_diagnostics/pace/test/p=$i/


	# Get data from hdfs for target date
	echo "hdfs dfs -get /datasets/stb_diagnostics/pace/test/p=$i/Ethan_BDC_STB_Parameters_*.csv.gz"
	hdfs dfs -get /datasets/stb_diagnostics/pace/test/p=$i/Ethan_BDC_STB_Parameters_*.csv.gz

	# echo "gunzip -v ./Ethan_BDC_STB_Parameters_*.csv.gz"
	# gunzip -v ./Ethan_BDC_STB_Parameters_*.csv.gz


	# Concatenate .gz files
	echo "Ethan_BDC_STB_Parameters_*.csv.gz >> all_Diagnostics_data.csv.gz"
	cat Ethan_BDC_STB_Parameters_*.csv.gz >> all_Diagnostics_data.csv.gz

	# Clean up
	echo "rm Ethan_BDC_STB_Parameters_*.csv"
	rm Ethan_BDC_STB_Parameters_*.csv.gz	

	i=$(($i+1))

done


# SFTP file over to ETL server

# echo "Copying all_Diagnostics_data.csv.gz to ETL server..."
# sshpass -p "1NY>vNB1WpKU" sftp sk_cu_tr@dcslopsketl01 <<< "cd /Decision_Sciences/tanghoi/Diagnostics/
# pwd
# put all_Diagnostics_data.csv.gz"

echo "gunzip all_Diagnostics_data.csv.gz"
gunzip all_Diagnostics_data.csv.gz

echo "Copying all_Diagnostics_data.csv.gz to ETL server..."
sshpass -p "1NY>vNB1WpKU" sftp sk_cu_tr@dcslopsketl01 <<< "cd /Decision_Sciences/tanghoi/Diagnostics/
pwd
put all_Diagnostics_data.csv"

echo "rm all_Diagnostics_data_$i.csv"
rm all_Diagnostics_data_$i.csv

