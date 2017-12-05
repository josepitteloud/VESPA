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

# Set input variables
args=("$@")

echo "Extracting Product Analytics Voice Search data for specified date..."

YYYY=${args[0]}
MM=${args[1]}
DD=${args[2]}


if [ $# -eq 0 ] ; then

        echo "Default date range... extract for yesterday"

        YYYY="$(date -d "-1 day" +"%Y")"
        MM="$(date -d "-1 day" +"%m")"
        DD="$(date -d "-1 day" +"%d")"

fi


echo "Extracting Voice Search data for... $YYYY $MM $DD"

cmd="hdfs dfs -du -h /data/raw/ethan/stb-pa/year=$YYYY/month=$MM/day=$DD/"
echo $cmd
$cmd

cmd="hdfs dfs -get /data/raw/ethan/stb-pa/year=$YYYY/month=$MM/day=$DD/ TMP_VoiceSearchData"
echo $cmd
$cmd

cmd="grep -hr Voice-Search TMP_VoiceSearchData"
echo $cmd  "> VoiceSearchExtract_$YYYY$MM$DD.json"
$cmd > VoiceSearchExtract_$YYYY$MM$DD.json

# Remove temporary dump directory
cmd="rm -r TMP_VoiceSearchData"
echo $cmd
$cmd

# Make a copy of json extract onto the dev cluster 
cmd="scp VoiceSearchExtract_$YYYY$MM$DD.json tanghoiy@client01.prod.bigdata.bskyb.com:~/data/"
echo $cmd
$cmd

# Parse json file and dump into csv
cmd="python /home/tanghoiy/bin/import_Voice_Search_json.py VoiceSearchExtract_$YYYY$MM$DD.json"
echo $cmd
$cmd

# Copy csv to dev cluster
cmd="scp TMP_VoiceSearchExtract.csv tanghoiy@client01.prod.bigdata.bskyb.com:~/data/"
echo $cmd
$cmd

# Truncate target Netezza table first as a precaution
echo "Truncate target Netezza batch table first"
ssh tanghoiy@client01.prod.bigdata.bskyb.com "nzodbcsql -h 10.137.15.3 -u bd_smi -p B1gData3DM -d ETHAN_PA_PROD -q \"TRUNCATE TABLE ETHAN_PA_PROD..TMP_VOICE_SEARCH_EXTRACT_BATCH;\""

# Use nzload to push csv into Netezza
echo "Use nzload to push csv into Netezza"
ssh tanghoiy@client01.prod.bigdata.bskyb.com "nzload -host 10.137.15.3 -u bd_smi -pw B1gData3DM -db ETHAN_PA_PROD -t TMP_VOICE_SEARCH_EXTRACT_BATCH -delim '|' -df /home/tanghoiy/data/TMP_VoiceSearchExtract.csv -skiprows 1 -maxErrors 0"

# Insert batch records into final historical table
echo "Insert batch records into final historical table"
ssh tanghoiy@client01.prod.bigdata.bskyb.com "nzodbcsql -h 10.137.15.3 -u bd_smi -p B1gData3DM -d ETHAN_PA_PROD -q \"INSERT INTO ETHAN_PA_PROD..PA_VOICE_SEARCH_RAW SELECT * FROM ETHAN_PA_PROD..TMP_VOICE_SEARCH_EXTRACT_BATCH;\""

# Clean up
echo "Clean up..."
cmd="rm TMP_VoiceSearchExtract.csv"
echo $cmd
$cmd

date
