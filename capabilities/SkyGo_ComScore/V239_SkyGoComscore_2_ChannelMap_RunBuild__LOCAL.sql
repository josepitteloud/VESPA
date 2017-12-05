/*
                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$     ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$      ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$=      ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$       ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$        ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=        ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$         ODD   ZDDDDDDDN
                                      $$$           .      $DDZ
                                     $$$                  ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES


        V239 - Oneview Comscore (SkyGo) - Channel Mapping Build Control(for live stream events)


###############################################################################
# Created between:   08/11/2016
# Created by:        Alan Barber (ABA)
# Description:
#
# List of steps:
# --------------
#
# code_location_A01    ...
#                      ...
# ----------------------------------------------------------
# code_location_B01    ...
# ----------------------------------------------------------
#
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - VESPA_ANALYSTS.channel_map_prod_service_key_attributes
#     - COMSCORE_UNION_VIEW (based on 13months comscore data)
#     -
#
# Table created or re-generated
# ------------------------------------------------------------------------------
#     - VESPA_Comscore_SkyGo_Channel_Mapping_tbl
#
#
# Temporary generated tables
#     - 
#     - 
#     - 
#
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 08/11/2016  ABA   Initial version
# 
# 
# 
# 
#                   
#
###############################################################################*/


CREATE or REPLACE procedure barbera.vespa_comscore_skygo_channelmap_build(
                        IN @data_run_date       DATE  --run this as NULL if a complete refresh is required
                                                                   ) AS
BEGIN

    EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Refreshing channel mapping information'
    EXEC barbera.vespa_comscore_skygo_channelmap_refresh @data_run_date

    IF (@@error = 0)  EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'>     > Channel mapping update [Successfull]'
    ELSE EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'>     > Channel mapping update [FAILED('||@@error||')]'

    /* code_location_G02 *************************************************************************
     *****                                                                                      **
     *****                Make manual adjustments to channel mapping info                       **
     *****                                                                                      **
     *********************************************************************************************/

    EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Making manual channelmap updates'
    EXEC barbera.vespa_comscore_skygo_channelmap_manual_updates

    IF (@@error = 0)
        BEGIN
            EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'>     > Channel mapping manual update [Successfull]'
              --update channels_mapped flag in audit_run_tbl
            execute('UPDATE barbera.VESPA_Comscore_SkyGo_audit_run_tbl
                        SET channels_mapped = 1
                      where data_date = '''||@data_run_date||'''
                     commit ')
        END
    ELSE
        BEGIN
            EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'>     > Channel mapping manual update [FAILED('||@@error||')]'
              --update channels_mapped flag in audit_run_tbl
            execute('UPDATE barbera.VESPA_Comscore_SkyGo_audit_run_tbl
                        SET channels_mapped = 0
                      where data_date = '''||@data_run_date||'''
                     commit ')
        END

END; 

