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
# import_Recs_json.py
# 2017-01-26
# 
# Environment:
# python script to be run on any linux environment having Python 2.7
# 
# Function: 
# Parse a Rec Engine json file into a csv (pipe-delimited) for loading into
# Netezza
# 
# ------------------------------------------------------------------------------
#
# Syntax: import_Recs_json.py JSON_FILE_NAME.json
#

#######################################################################
# Initialise
#######################################################################

# Load libraries
import sys
import json
import datetime
import urlparse

# Check for target intput file
try:
    print sys.argv[1]
except:
    print "Target file name required! Exiting..."
    exit()

# Generate audit timestamp
auditTimestamp = str(datetime.datetime.now())

# e.g. JSON record to be parsed
# {
#     "source": {
#         "serialNumber": "32B0560488023031F"
#     },
#     "eventLog": [
#         {
#             "action": {
#                 "id": "01400"
#             },
#             "dest": "guide://interim?__Receng=newseries&breadcrumb=Bones&date=20170120&eventId=Efe2-256&ippv=false&oppv=false&sid=4066&startTime=1484946000&tilePosition=6&type=tvguide&uuid=04671945-f761-4dad-8c50-9d625dab321c",
#             "orig": "guide://ondemand/asset/EVOD09?__signpost=New%20series%20for%20you&bookmark=true&dcnid=guide%3A%2F%2Fpvr%2Fpvod%3Ffilter%3Dboth&ondemand_context=qms&selectedProgramId=022A2A47&toppicks=true",
#             "timems": "1484933755736",
#             "trigger": {
#                 "id": "userInput",
#                 "input": "KeyEvent:Key_SelectKeyReleased",
#                 "remote": {
#                     "batterylevel": 100,
#                     "conntype": "BT",
#                     "deviceid": 4,
#                     "hwrev": "264.1",
#                     "make": "ruwido austria",
#                     "model": "2772-503",
#                     "name": "P092ruwido Sky Remote",
#                     "swrev": "1.0.59"
#                 }
#             }
#         }
#     ]
# }

# {
#     "source": {
#         "serialNumber": "32B05604880034742"
#     },
#     "eventLog": [
#         {
#             "action": {
#                 "asset": {
#                     "UUID": "dd66f291-840a-4f6d-915e-feb62c038e65",
#                     "airing": {
#                         "date": "2017-01-24",
#                         "ev": "Efeb-b7f",
#                         "sk": "4075"
#                     },
#                     "assetid": "P290128af",
#                     "providerid": "N/A",
#                     "seasonUUID": "f3701fc4-bfc5-4ee5-a9cd-3095f72fe17c",
#                     "type": "pvr"
#                 },
#                 "duration": 3900,
#                 "id": "02000"
#             },
#             "ref": {
#                 "id": "guide://interim?__Receng=newseries&breadcrumb=Case&date=20170124&eventId=Efeb-b7f&ippv=false&oppv=false&sid=4075&startTime=1485295200&tilePosition=2&type=tvguide&uuid=dd66f291-840a-4f6d-915e-feb62c038e65"
#             },
#             "timems": "1484951768851",
#             "trigger": {
#                 "id": "userInput",
#                 "input": "record",
#                 "remote": {
#                     "batterylevel": 100,
#                     "conntype": "BT",
#                     "deviceid": 4,
#                     "hwrev": "264.1",
#                     "make": "ruwido austria",
#                     "model": "2772-503",
#                     "name": "P027ruwido Sky Remote",
#                     "swrev": "1.0.59"
#                 }
#             }
#         }
#     ]
# }


#######################################################################
# Start here
#######################################################################


i = 0

# Open output file for writing
iof_out = open('TMP_RecsExtract.csv','w')

# Parse JSON file line by line
with open(sys.argv[1],'r') as f:

    for l in f:

        i = i + 1

        j = json.loads(l)

        if i == 1:

            # Write header line
            iof_out.write(
                "source/serialNumber" + "|"
                + "source/trial" + "|"
                + "eventLog/action/id" + "|"
                + "eventLog/dest" + "|"
                + "eventLog/orig" + "|"
                + "eventLog/0/action/asset/UUID" + "|"
                + "eventLog/0/action/asset/assetid" + "|"
                + "eventLog/0/action/asset/providerid" + "|"
                + "eventLog/0/action/asset/seasonUUID" + "|"
                + "eventLog/0/action/asset/type" + "|"
                + "eventLog/0/action/asset/airing/date" + "|"
                + "eventLog/0/action/asset/airing/ev" + "|"
                + "eventLog/0/action/asset/airing/sk" + "|"
                + "eventLog/0/action/duration" + "|"
                + "eventLog/0/ref/id" + "|"
                + "eventLog/0/Receng" + "|"
                + "eventLog/timems" + "|"
                + "eventLog/trigger/id" + "|"
                + "eventLog/trigger/input" + "|"
                + "eventLog/trigger/remote/conntype" + "|"
                + "eventLog/trigger/remote/deviceid" + "|"
                + "auditTimestamp"
                + "\n"
                )

        # Write parsed json to csv file
        try:
            
            # Check for trial group key (all json messages should contain this if the viewing card has this entitlement set). Otherwise, they are not part of the trial.
            trialGroup = ''
            if 'trial' in j['source']:
                trialGroup = j['source']['trial']


            # Switch between 01400 json and everything else
            dest = ''
            orig = ''
            assetUUID       = ''
            assetAssetid    = ''
            assetProviderid = ''
            assetSeasonuuid = ''
            assetType       = ''
            assetAiringDate = ''
            assetAiringEv   = ''
            assetAiringSk   = ''
            duration        = ''
            refID           = ''

            if j['eventLog'][0]['action']['id'] == '01400':
                dest                    = j['eventLog'][0]['dest']
                orig                    = j['eventLog'][0]['orig']

                # Parse the destination URI
                parsed = urlparse.urlparse(j['eventLog'][0]['dest'])

            else:
                assetUUID               = j['eventLog'][0]['action']['asset']['UUID']
                assetAssetid            = j['eventLog'][0]['action']['asset']['assetid']
                assetProviderid         = j['eventLog'][0]['action']['asset']['providerid']
                assetSeasonuuid         = j['eventLog'][0]['action']['asset']['seasonUUID']
                assetType               = j['eventLog'][0]['action']['asset']['type']
                assetAiringDate         = j['eventLog'][0]['action']['asset']['airing']['date']
                assetAiringEv           = j['eventLog'][0]['action']['asset']['airing']['ev']
                assetAiringSk           = j['eventLog'][0]['action']['asset']['airing']['sk']
                duration                = j['eventLog'][0]['action']['duration']
                refID                   = j['eventLog'][0]['ref']['id']

                # Parse the URI
                parsed = urlparse.urlparse(j['eventLog'][0]['ref']['id'])


            # Parse the URI query parameters into a dictionary
            qs_dict = urlparse.parse_qs(parsed.query)


            # Write parsed record to output file
            iof_out.write (
                j['source']['serialNumber'] + "|"
                + trialGroup + "|"
                + j['eventLog'][0]['action']['id'] + "|"
                + dest + "|"
                + orig + "|"
                + assetUUID + "|"
                + assetAssetid + "|"
                + assetProviderid + "|"
                + assetSeasonuuid + "|"
                + assetType + "|"
                + assetAiringDate + "|"
                + assetAiringEv + "|"
                + assetAiringSk + "|"
                + duration + "|"
                + refID + "|"
                + qs_dict['__Receng'][0] + "|"
                + str(j['eventLog'][0]['timems']) + "|"
                + j['eventLog'][0]['trigger']['id'] + "|"
                + j['eventLog'][0]['trigger']['input'] + "|"
                + j['eventLog'][0]['trigger']['remote']['conntype'] + "|"
                + str(j['eventLog'][0]['trigger']['remote']['deviceid']) + "|"
                + auditTimestamp
                + "\n"
                )

        except:
            pass

# Close output file
iof_out.close()


#######################################################################
# Finish
#######################################################################

print (str(i) + " lines parsed from json to csv.")

exit()


# -- Netezza SQL for creating the target tables to house the above csv...
# 
# CREATE TABLE PA_Recs_RAW    (
#                                     "source/serialNumber"   VARCHAR(17)
#                                 ,   "source/trial"    VARCHAR(128)
#                                 ,   "eventLog/action/id"    VARCHAR(128)
#                                 ,   "eventLog/dest"    VARCHAR(8192)
#                                 ,   "eventLog/orig"    VARCHAR(8192)
#                                 ,   "eventLog/0/action/asset/UUID"  VARCHAR(128)
#                                 ,   "eventLog/0/action/asset/assetid"   VARCHAR(128)
#                                 ,   "eventLog/0/action/asset/providerid"    VARCHAR(128)
#                                 ,   "eventLog/0/action/asset/seasonUUID"    VARCHAR(128)
#                                 ,   "eventLog/0/action/asset/type"  VARCHAR(128)
#                                 ,   "eventLog/0/action/asset/airing/date"   DATE
#                                 ,   "eventLog/0/action/asset/airing/ev" VARCHAR(128)
#                                 ,   "eventLog/0/action/asset/airing/sk" INT
#                                 ,   "eventLog/0/action/duration"    INT
#                                 ,   "eventLog/0/ref/id" VARCHAR(8192)
#                                 ,   "eventLog/0/Receng" VARCHAR(8192)
#                                 ,   "eventLog/timems"   BIGINT
#                                 ,   "eventLog/trigger/id"   VARCHAR(512)
#                                 ,   "eventLog/trigger/input"    VARCHAR(512)
#                                 ,   "eventLog/trigger/remote/conntype"  VARCHAR(512)
#                                 ,   "eventLog/trigger/remote/deviceid"  VARCHAR(512)
#                                 ,   "auditTimestamp"  TIMESTAMP
#                             )
# DISTRIBUTE ON ("eventLog/timems", "source/serialNumber")
# ;
# 
# SELECT    *
# INTO    TMP_PA_Recs_EXTRACT_BATCH
# FROM    PA_Recs_RAW
# WHERE   1 <> 1
# ;

