#!/bin/bash
#
# Syntax: extract_pace_data.sh YYYYMMDD
# e.g.	extract_pace_data.sh 20160101
#

# Set input variables
args=("$@")

echo "Extracting Ethan Diagnostics data for specified date..."

date_from=${args[0]}


if [ $# -eq 0 ] ; then
        echo "Default date range... extract for yesterday"

        date_from="$(date -d "-1 day" +"%Y%m%d")"
fi


echo $date_from
echo $date_to

i=$date_from


# Clean up in case the previous run failed
echo "rm Ethan_BDC_STB_Parameters_*.csv"
rm Ethan_BDC_STB_Parameters_*.csv

echo "rm Ethan_BDC_STB_Parameters_*.csv.gz"
rm Ethan_BDC_STB_Parameters_*.csv.gz



echo "Extracting for date $i"

# List files in target date directory
echo "hdfs dfs -du -h /datasets/stb_diagnostics/pace/bdc/p=$i/"
hdfs dfs -du -h /datasets/stb_diagnostics/pace/bdc/p=$i/


# Get data from hdfs for target date
echo "hdfs dfs -get /datasets/stb_diagnostics/pace/bdc/p=$i/Ethan_BDC_STB_Parameters_*.csv.gz"
hdfs dfs -get /datasets/stb_diagnostics/pace/bdc/p=$i/Ethan_BDC_STB_Parameters_*.csv.gz


for line in Ethan_BDC_STB_Parameters_*.csv.gz; do
        echo "Working on file: $line"

        echo "gunzip $line"
        gunzip "$line"

        echo "Copying ${line/.gz/} to ETL server..."
        sshpass -p "1NY>vNB1WpKU" sftp sk_cu_tr@dcslopsketl01 <<< "cd /Decision_Sciences/tanghoi/Diagnostics/
        pwd
        put ${line/.gz/}"

        echo "rm ${line/.gz/}"
        rm ${line/.gz/}

        echo ""
done

