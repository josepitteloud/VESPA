#!/bin/bash
#                                            '#                                 
#                                            '#                                 
#                                            '#                                 
#                                            '#                                 
#                                            '#                                 
#                                            '#                                 
#              '##                           '#                                 
#              ###                           '#                                 
#             .###                           '#                                 
#             .###                           '#                                 
#     .:::.   .###       ::         ..       '#       .                   ,:,   
#   ######### .###     #####       ###.      '#      '##  ########`     ########
#  ########## .###    ######+     ####       '#      '##  #########'   ########'
# ;#########  .###   +#######     ###;       '#      '##  ###    ###.  ##       
# ####        .###  '#### ####   '###        '#      '##  ###     ###  ##       
# '####+.     .### ;####  +###:  ###+        '#      '##  ###      ##  ###`     
#  ########+  .###,####    #### .###         '#      '##  ###      ##. ;#####,  
#  `######### .###`####    `########         '#      '##  ###      ##.  `######`
#     :######`.### +###.    #######          '#      '##  ###      ##      .####
#         ###'.###  ####     ######          '#      '##  ###     ;##         ##
#  `'':..+###:.###  .####    ,####`          '#      '##  ###    `##+         ##
#  ########## .###   ####.    ####           '#      '##  ###   +###   ;,    +##
#  #########, .###    ####    ###:           '#      '##  #########    ########+
#  #######;   .##:     ###+  '###            '#      '##  '######      ;######, 
#                            ###'            '#                                 
#                           ;###             '#                                 
#                           ####             '#                                 
#                          :###              '#                                 
#                                            '#                                 
#                                            '#                                 
#                                            '#                                 
#                                            '#                                 
#                                            '#                                 
#                                            '#                                 
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# V346_daily_SkyQ_NSS_volumes
# 2016-11-29
# 
# Environment:
# bash script to be run on the prod hdfs cluster
# http://uphad4j0.bskyb.com:8888/impala/
# 
# Function: 
# Extract from the PA raw JSON logs any records associated with Voice Search.
# Dump into new json, then call the import_json.py python script to parse and convert the json file into csv (pipe-delimited)
# 
# Dependencies:
# import_json.py
# 
# ------------------------------------------------------------------------------
# 
# 
#
# Syntax: extractVoiceSearchJSON.sh YYYY MM DD
# e.g.	extractVoiceSearchJSON.sh 2016 01 01
#

date

# # Set input variables
# args=("$@")

# echo "Extracting Product Analytics Voice Search data for specified date..."

# YYYY=${args[0]}
# MM=${args[1]}
# DD=${args[2]}


# if [ $# -eq 0 ] ; then

#         echo "Default date range... extract for yesterday"

#         YYYY="$(date -d "-1 day" +"%Y")"
#         MM="$(date -d "-1 day" +"%m")"
#         DD="$(date -d "-1 day" +"%d")"

# fi


# echo "Extracting Voice Search data for... $YYYY $MM $DD"

# cmd="hdfs dfs -du -h /data/raw/ethan/stb-pa/year=$YYYY/month=$MM/day=$DD/"
# echo $cmd
# $cmd

# # Pull PA data for date of interest
# cmd="hdfs dfs -get /data/raw/ethan/stb-pa/year=$YYYY/month=$MM/day=$DD/ TMP_ProductAnalytics_HDFS_extract"
# echo $cmd
# $cmd




#######################################################################
#
# Voice Search
#
#######################################################################

# Run Spark script to filter for Voice Search actions in hdfs
  spark-shell \
    --packages \
      com.databricks:spark-csv_2.10:1.4.0 \
    -i /home/tanghoiy/bin/extractVoiceSearchJSON.scala

# sqoop command for export into Netezza - ensure target table name is correct first!
sqoop export \
--connect jdbc:netezza://10.137.15.3/ETHAN_PA_PROD \
--username BD_SMI \
--password B1gData3DM \
--direct \
--export-dir /user/tanghoiy/PA_tactical \
--table PA_VOICE_SEARCH_RAW \
--num-mappers 8 \
--input-fields-terminated-by '\\' \
-- --nz-maxerrors 1


# # Clean up hdfs dump folder
# cmd="hdfs dfs -rm -r /user/tanghoiy/PA_tactical"
# echo $cmd
# $cmd


#######################################################################
#
# Signposting
#
#######################################################################

# # Filter for json records corresponding to Voice Search actions only
# cmd="grep -hr __signpost TMP_ProductAnalytics_HDFS_extract"
# echo $cmd  "> SignpostingExtract_$YYYY$MM$DD.json"
# $cmd > SignpostingExtract_$YYYY$MM$DD.json

# # Make a copy of json extract onto the dev cluster 
# cmd="scp SignpostingExtract_$YYYY$MM$DD.json tanghoiy@client01.prod.bigdata.bskyb.com:~/data/"
# echo $cmd
# $cmd

# # Parse json file and dump into csv
# cmd="python /home/tanghoiy/bin/import_Signpost_json.py SignpostingExtract_$YYYY$MM$DD.json"
# echo $cmd
# $cmd

# # Copy csv to dev cluster
# cmd="scp TMP_SignpostingExtract.csv tanghoiy@client01.prod.bigdata.bskyb.com:~/data/"
# echo $cmd
# $cmd

# # Truncate target Netezza BATCH table first as a precaution
# echo "Truncate target Netezza batch table first"
# ssh tanghoiy@client01.prod.bigdata.bskyb.com "nzodbcsql -h 10.137.15.3 -u bd_smi -p B1gData3DM -d ETHAN_PA_PROD -q \"TRUNCATE TABLE ETHAN_PA_PROD..TMP_PA_Signposting_EXTRACT_BATCH;\""

# # Use nzload to push csv into Netezza
# echo "Use nzload to push csv into Netezza"
# ssh tanghoiy@client01.prod.bigdata.bskyb.com "nzload -host 10.137.15.3 -u bd_smi -pw B1gData3DM -db ETHAN_PA_PROD -t TMP_PA_Signposting_EXTRACT_BATCH -delim '|' -df /home/tanghoiy/data/TMP_SignpostingExtract.csv -skiprows 1 -maxErrors 0"

# # Insert batch records into final historical table
# echo "Insert batch records into final historical table"
# ssh tanghoiy@client01.prod.bigdata.bskyb.com "nzodbcsql -h 10.137.15.3 -u bd_smi -p B1gData3DM -d ETHAN_PA_PROD -q \"INSERT INTO ETHAN_PA_PROD..PA_Signposting_RAW SELECT * FROM ETHAN_PA_PROD..TMP_PA_Signposting_EXTRACT_BATCH;\""

# # Clean up
# echo "Clean up..."
# cmd="rm TMP_SignpostingExtract.csv"
# echo $cmd
# $cmd


#######################################################################
#
# Recommendations
#
#######################################################################

# # Filter for json records corresponding to Voice Search actions only
# cmd="grep -hr __Receng TMP_ProductAnalytics_HDFS_extract"
# echo $cmd  "> RecengExtract_$YYYY$MM$DD.json"
# $cmd > RecengExtract_$YYYY$MM$DD.json

# # Make a copy of json extract onto the dev cluster 
# cmd="scp RecengExtract_$YYYY$MM$DD.json tanghoiy@client01.prod.bigdata.bskyb.com:~/data/"
# echo $cmd
# $cmd

# # Parse json file and dump into csv
# cmd="python /home/tanghoiy/bin/import_Recs_json.py RecengExtract_$YYYY$MM$DD.json"
# echo $cmd
# $cmd

# # Copy csv to dev cluster
# cmd="scp TMP_RecsExtract.csv tanghoiy@client01.prod.bigdata.bskyb.com:~/data/"
# echo $cmd
# $cmd

# # Truncate target Netezza BATCH table first as a precaution
# echo "Truncate target Netezza batch table first"
# ssh tanghoiy@client01.prod.bigdata.bskyb.com "nzodbcsql -h 10.137.15.3 -u bd_smi -p B1gData3DM -d ETHAN_PA_PROD -q \"TRUNCATE TABLE ETHAN_PA_PROD..TMP_PA_Recs_EXTRACT_BATCH;\""

# # Use nzload to push csv into Netezza
# echo "Use nzload to push csv into Netezza"
# ssh tanghoiy@client01.prod.bigdata.bskyb.com "nzload -host 10.137.15.3 -u bd_smi -pw B1gData3DM -db ETHAN_PA_PROD -t TMP_PA_Recs_EXTRACT_BATCH -delim '|' -df /home/tanghoiy/data/TMP_RecsExtract.csv -skiprows 1 -maxErrors 0"

# # Insert batch records into final historical table
# echo "Insert batch records into final historical table"
# ssh tanghoiy@client01.prod.bigdata.bskyb.com "nzodbcsql -h 10.137.15.3 -u bd_smi -p B1gData3DM -d ETHAN_PA_PROD -q \"INSERT INTO ETHAN_PA_PROD..PA_Recs_RAW SELECT * FROM ETHAN_PA_PROD..TMP_PA_Recs_EXTRACT_BATCH;\""

# # Clean up
# echo "Clean up..."
# cmd="rm TMP_RecsExtract.csv"
# echo $cmd
# $cmd


#######################################################################
#
# Clean up
#
#######################################################################

date

# # Remove temporary dump directory
# cmd="rm -r TMP_ProductAnalytics_HDFS_extract"
# echo $cmd
# $cmd
