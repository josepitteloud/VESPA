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
# import_Signpost_json.py
# 2017-01-26
# 
# Environment:
# python script to be run on any linux environment having Python 2.7
# 
# Function: 
# Parse a Signposting json file into a csv (pipe-delimited) for loading into
# Netezza
# 
# ------------------------------------------------------------------------------
#
# Syntax: import_Signpost_json.py JSON_FILE_NAME.json
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

# e.g. JSON record to the parsed
# {
#     "source": {
#         "serialNumber": "32D0010487029153C",
#         "trial": "SKYQ_TRIAL_B"
#     },
#     "eventLog": [
#         {
#             "action": {
#                 "id": "01400"
#             },
#             "dest": "guide://qms-programme-details/interim?assetLocation=qms&breadcrumb=Hercules&queryHex=2f7365617263682f76312f736b79717374622f746f707069636b732f343130312f332f757365722f70726f6772616d6d652f35666363326432332d633564622d343330652d626632342d3766343339343234653533373f7372633d73766f64267372633d637570&searchInputString=TOP_PICKS_REFERRER&sectionTitle=Top%20Picks&source=online&tilePosition=13&title=Top%20Picks&uuid=5fcc2d23-c5db-430e-bf24-7f439424e537&uuidType=programme",
#             "orig": "guide://ondemand/asset/EVOD09?__signpost=Sky%20Cinema%20highlights&bookmark=true&dcnid=guide%3A%2F%2Fpvr%2Fpvod%3Ffilter%3Dboth&ondemand_context=qms&selectedProgramId=1BE6E6B7&toppicks=true",
#             "timems": "1484935272046",
#             "trigger": {
#                 "id": "userInput",
#                 "input": "KeyEvent:Key_SelectKeyPressed",
#                 "remote": {
#                     "conntype": "IR",
#                     "deviceid": 4294967293
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
iof_out = open('TMP_SignpostingExtract.csv','w')

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
                + "eventLog/dest/tilePosition" + "|"
                + "eventLog/orig" + "|"
                + "eventLog/orig/signpost" + "|"
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

            # Parse the orig URI
            parsedOrig = urlparse.urlparse(j['eventLog'][0]['orig'])

            # Parse the dest URI
            parsedDest = urlparse.urlparse(j['eventLog'][0]['dest'])

            # Parse the URI query parameters into dictionaries
            qs_dict_orig = urlparse.parse_qs(parsedOrig.query)
            qs_dict_dest = urlparse.parse_qs(parsedDest.query)

            # Write parsed record to output file
            iof_out.write (
                j['source']['serialNumber'] + "|"
                + trialGroup + "|"
                + j['eventLog'][0]['action']['id'] + "|"
                + j['eventLog'][0]['dest'] + "|"
                + qs_dict_dest['tilePosition'][0] + "|"
                + j['eventLog'][0]['orig'] + "|"
                + qs_dict_orig['__signpost'][0] + "|"
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
# CREATE TABLE PA_Signposting_RAW    (
#                                             "source/serialNumber"   VARCHAR(17)
#                                         ,   "source/trial"    VARCHAR(128)
#                                         ,   "eventLog/action/id"    VARCHAR(128)
#                                         ,   "eventLog/dest"    VARCHAR(8192)
#                                         ,   "eventLog/dest/tilePosition"    INT
#                                         ,   "eventLog/orig"    VARCHAR(8192)
#                                         ,   "eventLog/orig/signpost"    VARCHAR(256)
#                                         ,   "eventLog/timems"   BIGINT
#                                         ,   "eventLog/trigger/id"   VARCHAR(512)
#                                         ,   "eventLog/trigger/input"    VARCHAR(512)
#                                         ,   "eventLog/trigger/remote/conntype"  VARCHAR(512)
#                                         ,   "eventLog/trigger/remote/deviceid"  VARCHAR(512)
#                                         ,   "auditTimestamp"  TIMESTAMP
#                                     )
# DISTRIBUTE ON ("eventLog/timems", "source/serialNumber")
# ;
# 
# SELECT    *
# INTO    TMP_PA_Signposting_EXTRACT_BATCH
# FROM    PA_Signposting_RAW
# WHERE   1 <> 1
# ;

