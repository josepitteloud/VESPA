#!/usr/bin/env python
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
# import_json.py
# 2016-11-29
# 
# Environment:
# python script to be run on any linux environment having Python 2.7
# 
# Function: 
# Parse a json file produced by extractVoiceSearchJSON.sh into a csv (pipe-delimited)
# 
# ------------------------------------------------------------------------------
#
# Syntax: import_json.py JSON_FILE_NAME.json
#


import sys
import json
import datetime


try:
    print sys.argv[1]
except:
    print "Target file name required! Exiting..."
    exit()

auditTimestamp = str(datetime.datetime.now())

# e.g.
# {
#     "eventLog": [
#         {
#             "action": {
#                 "error_msg": "",
#                 "id": "01605",
#                 "oldQuery": "",
#                 "query": "Rick stein",
#                 "suggestions": 10
#             },
#             "ref": {
#                 "id": "guide://voice-search"
#             },
#             "timems": "1480205231801",
#             "trigger": {
#                 "id": "userInput",
#                 "input": "Voice-Search",
#                 "remote": {
#                     "batterylevel": 31,
#                     "conntype": "BT",
#                     "deviceid": 4,
#                     "hwrev": "264.1",
#                     "make": "ruwido austria",
#                     "model": "2772-503",
#                     "name": "P037ruwido Sky Remote",
#                     "swrev": "1.0.59"
#                 }
#             }
#         }
#     ],
#     "source": {
#         "serialNumber": "32B0560488006529A"
#     }
# }

i = 0

iof_out = open('TMP_VoiceSearchExtract.csv','w')

with open(sys.argv[1],'r') as f:

    for l in f:

        i = i + 1

        j = json.loads(l)

        if i == 1:

            # Write header line
            iof_out.write(
                "source/serialNumber" + "|"
                + "eventLog/action/asrConfidenceLevel" + "|"
                + "eventLog/action/error_msg" + "|"
                + "eventLog/action/id" + "|"
                + "eventLog/action/oldQuery" + "|"
                + "eventLog/action/query" + "|"
                + "eventLog/action/suggestions" + "|"
                + "eventLog/ref/id" + "|"
                + "eventLog/timems" + "|"
                + "eventLog/trigger/id" + "|"
                + "eventLog/trigger/input" + "|"
                + "eventLog/trigger/remote/batterylevel" + "|"
                + "eventLog/trigger/remote/conntype" + "|"
                + "eventLog/trigger/remote/deviceid" + "|"
                + "eventLog/trigger/remote/hwrev" + "|"
                + "eventLog/trigger/remote/make" + "|"
                + "eventLog/trigger/remote/model" + "|"
                + "eventLog/trigger/remote/name" + "|"
                + "eventLog/trigger/remote/swrev" + "|"
                + "auditTimestamp"
                + "\n"
                )

        # Write parsed json to csv file (ugly nested try/except - let's keep this for now)
        try:
            
            # Check for asrConfidencenceLevel key (all Voice Search json message should contain this after 29th Nov 2016, but this will ensure we can also parse older messages that do not contain this key)
            asrConfidenceLevel = ''
            if 'asrConfidenceLevel' in j['eventLog'][0]['action']:
                asrConfidenceLevel = j['eventLog'][0]['action']['asrConfidenceLevel']


            iof_out.write (
                j['source']['serialNumber'] + "|"
                + asrConfidenceLevel + "|"
                + j['eventLog'][0]['action']['error_msg'] + "|"
                + j['eventLog'][0]['action']['id'] + "|"
                + j['eventLog'][0]['action']['oldQuery'] + "|"
                + j['eventLog'][0]['action']['query'] + "|"
                + str(j['eventLog'][0]['action']['suggestions']) + "|"
                + j['eventLog'][0]['ref']['id'] + "|"
                + str(j['eventLog'][0]['timems']) + "|"
                + j['eventLog'][0]['trigger']['id'] + "|"
                + j['eventLog'][0]['trigger']['input'] + "|"
                + str(j['eventLog'][0]['trigger']['remote']['batterylevel']) + "|"
                + j['eventLog'][0]['trigger']['remote']['conntype'] + "|"
                + str(j['eventLog'][0]['trigger']['remote']['deviceid']) + "|"
                + str(j['eventLog'][0]['trigger']['remote']['hwrev']) + "|"
                + j['eventLog'][0]['trigger']['remote']['make'] + "|"
                + j['eventLog'][0]['trigger']['remote']['model'] + "|"
                + j['eventLog'][0]['trigger']['remote']['name'] + "|"
                + j['eventLog'][0]['trigger']['remote']['swrev'] + "|"
                + auditTimestamp
                + "\n"
                )
        except:
            pass

iof_out.close()

print (str(i) + " lines parsed from json to csv.")

exit()


# -- Netezza SQL for creating the target table to house the above csv...
# 
# CREATE TABLE PA_Voice_Search_RAW    (
#                                             "source/serialNumber"   VARCHAR(17)
#                                         ,   "eventLog/action/asrConfidenceLevel" INTEGER
#                                         ,   "eventLog/action/error_msg" VARCHAR(512)
#                                         ,   "eventLog/action/id"    VARCHAR(128)
#                                         ,   "eventLog/action/oldQuery"  VARCHAR(2048)
#                                         ,   "eventLog/action/query"  VARCHAR(2048)
#                                         ,   "eventLog/action/suggestions"   INTEGER
#                                         ,   "eventLog/ref/id"   VARCHAR(2048)
#                                         ,   "eventLog/timems"   BIGINT
#                                         ,   "eventLog/trigger/id"   VARCHAR(512)
#                                         ,   "eventLog/trigger/input"    VARCHAR(512)
#                                         ,   "eventLog/trigger/remote/batterylevel"  INTEGER
#                                         ,   "eventLog/trigger/remote/conntype"  VARCHAR(512)
#                                         ,   "eventLog/trigger/remote/deviceid"  VARCHAR(512)
#                                         ,   "eventLog/trigger/remote/hwrev" VARCHAR(512)
#                                         ,   "eventLog/trigger/remote/make"  VARCHAR(512)
#                                         ,   "eventLog/trigger/remote/model" VARCHAR(512)
#                                         ,   "eventLog/trigger/remote/name"  VARCHAR(512)
#                                         ,   "eventLog/trigger/remote/swrev" VARCHAR(512)
#                                         ,   "auditTimestamp"  TIMESTAMP
#                                     )
# DISTRIBUTE ON ("eventLog/timems", "source/serialNumber")
# ;

