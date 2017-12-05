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


        V239 - Oneview Comscore (SkyGo) - Channel Mapping (for live stream events)


###############################################################################
# Created between:   16/08/2014 - 31/09/2014
# Created by:        Alan Barber (ABA)
# Description:
#
# List of steps:
# --------------
#
# code_location_A01    Extract station_name, and channel_id information already existing
#                      within the Comscore Union View (1Yr of data)
# ----------------------------------------------------------
# code_location_B01    Clean-up and drop temporary tables
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
#     - VESPA_Comscore_SkyGo_RawChannels_tmp
#     - VESPA_Comscore_SkyGo_channel_lookup_a_tmp
#     - VESPA_Comscore_SkyGo_channel_lookup_b_tmp
#
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 26/09/2014  ABA   Initial version
# 05/11/2014  ABA   Fix to ensure that the most recent service key is used
# 10/12/2014  ABA   Added drop statements for VESPA_Comscore_SkyGo_channel_lookup_b_tmp and ..._a_tmp
# 07/01/2015  ABA   Added some indexes to VESPA_Comscore_SkyGo_Channel_Mapping_tbl
# 08/01/2015  ABA   @data_run_date added to list of proc parameters and used in
#                   code + modifications to build of VESPA_Comscore_SkyGo_Channel_Mapping_tbl 
# 09/11/2016  ABA   updated the mapping algorithm to match more intelligently
#
###############################################################################*/


CREATE or REPLACE procedure barbera.vespa_comscore_skygo_channelmap_refresh(
                        IN @data_run_date       DATE  --run this as NULL if a complete refresh is required
                                                                   ) AS
BEGIN

--temp
--declare @data_run_date       DATE
--set @data_run_date = '2015-01-18'

    declare @load_file_name varchar(22)
    declare @xsql           varchar(8000)


    --if running for a specfied run day, then specify the expected file name for the day's data
    IF @data_run_date is NOT NULL
        BEGIN
            set @load_file_name = 'Comscore_'||dateformat(cast(@data_run_date as date), 'YYYYMMDD')||'.gz'
        END


    /* code_location_A01 ***********************************************************
     *****                                                                        **
     *****       Extract station_name, and channel_id information already         **
     *****       existing within the Comscore Union View (1Yr of data)            **
     *****                                                                        **
     *******************************************************************************/


    IF object_id('barbera.VESPA_Comscore_SkyGo_RawChannels_tmp') IS NOT NULL
        BEGIN
            drop table barbera.VESPA_Comscore_SkyGo_RawChannels_tmp
        END

    set @xsql = '
                select c.ns_st_st station_name,
                       c.ns_st_ci channel_id
                  into barbera.VESPA_Comscore_SkyGo_RawChannels_tmp
                  from barbera.COMSCORE_UNION_VIEW c
                 where c.sg_vs_sc = ''lin''   --restrict to linear channels, but could be expanded if service key required for all station names
                 ###AND_CLAUSE###
                 group by c.ns_st_st, c.ns_st_ci
                 order by c.ns_st_st --station_name
                commit '


    IF @data_run_date is NOT NULL
          BEGIN
             execute( replace(@xsql, '###AND_CLAUSE###', 'and c.cb_source_file = '''||@load_file_name||'''') )
          END
       ELSE
          BEGIN --used when @run_data_date is NULL
             execute( replace(@xsql, '###AND_CLAUSE###', '') )    --## FIRST TIME or Re-FRESH  is unrestricted (to allow a fresh build)
          END


    --create channel look-up for all channels (not just SkyGo broadcast)
    IF object_id('barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl') IS NULL
        BEGIN
            CREATE TABLE barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl(
                        station_name       varchar(70)          NULL,
                        channel_name       varchar(200)         NULL,
                        service_key        int                  NULL
                )
            commit

            --add indexes on service_key, station_name
            create index VESPA_Comscore_skygo_cm_servkey_idx on barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl(service_key)
            create index VESPA_Comscore_skygo_cm_stname_idx  on barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl(station_name)
            commit
        END


    --delete tmp tables if existing
    IF object_id('barbera.VESPA_Comscore_SkyGo_channel_lookup_a_tmp') IS NOT NULL  BEGIN drop table barbera.VESPA_Comscore_SkyGo_channel_lookup_a_tmp END
    IF object_id('barbera.VESPA_Comscore_SkyGo_channel_lookup_b_tmp') IS NOT NULL  BEGIN drop table barbera.VESPA_Comscore_SkyGo_channel_lookup_b_tmp END
    IF object_id('barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl_tmp') IS NOT NULL  BEGIN drop table barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl_tmp END
    IF object_id('barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl_dups') IS NOT NULL  BEGIN drop table barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl_dups END
    commit

    --identify the service_key that represents the parent channel
    select channel_name,
           vespa_name,
           parent_service_key,
           service_key,
           max(effective_to) max_effective_to,
           max(effective_to) over(partition by vespa_name ) vespa_channel_max
      into barbera.VESPA_Comscore_SkyGo_channel_lookup_a_tmp
      from VESPA_ANALYSTS.channel_map_prod_service_key_attributes
     where parent_service_key = service_key
     group by channel_name, vespa_name, parent_service_key, service_key, effective_to


    --add index to VESPA_Comscore_SkyGo_channel_lookup_a_tmp to see if it is faster to build
    create DTTM index VESPA_Comscore_SkyGo_channel_lookup_a_vespa_idx on barbera.VESPA_Comscore_SkyGo_channel_lookup_a_tmp(vespa_channel_max)
    commit


    -- take the most recent service_key
    select channel_name, vespa_name, parent_service_key, service_key
      into barbera.VESPA_Comscore_SkyGo_channel_lookup_b_tmp
      from barbera.VESPA_Comscore_SkyGo_channel_lookup_a_tmp
     where max_effective_to = vespa_channel_max
       and vespa_channel_max = '2999-12-31 00:00:00.000000'
     group by channel_name, vespa_name, parent_service_key, service_key


    select cm.station_name, sk.channel_name, sk.vespa_name, sk.service_key
      into barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl_tmp
      from barbera.VESPA_Comscore_SkyGo_RawChannels_tmp cm
            LEFT JOIN
           barbera.VESPA_Comscore_SkyGo_channel_lookup_b_tmp sk
            ON lower(trim(cm.station_name)) = lower(trim(sk.channel_name))
            OR (lower(trim(cm.station_name)) = lower(trim(sk.vespa_name))
                 AND
                sk.service_key = cast(cm.channel_id as INTEGER)
                )
            OR (sk.service_key = cast(cm.channel_id as INTEGER)
                 AND
                cm.station_name = cm.channel_id
                 )
     where coalesce(sk.service_key, 99999) >= 1000
       and coalesce(cast(cm.channel_id as INTEGER), 99999) >= 1000
     group by cm.station_name, sk.channel_name, sk.vespa_name, sk.service_key
    commit

    --need to tidy up this table to make sure there are no duplicate station_names
    DELETE FROM barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl_tmp x
            from barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl_tmp a
           where a.channel_name IS NOT NULL
            AND x.channel_name IS NULL
            AND x.station_name = a.station_name
    commit  

    


    --combine with the master Channel Mapping table
    -- [to save time delete if in the tmp list already and then insert new records,
    --  assuming this will be run daily going forward - just means master holds the most recent mapping info]
    DELETE FROM barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl m
      from barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl_tmp t
     where m.station_name = t.station_name

    INSERT into barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl
       select station_name, vespa_name, service_key
         from barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl_tmp
    commit




    /* code_location_B01 ***********************************************************
     *****                                                                        **
     *****         Clean-up and drop temporary tables                             **
     *****                                                                        **
     *******************************************************************************/

    drop table barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl_tmp
    drop table barbera.VESPA_Comscore_SkyGo_RawChannels_tmp
    drop table barbera.VESPA_Comscore_SkyGo_channel_lookup_a_tmp
    drop table barbera.VESPA_Comscore_SkyGo_channel_lookup_b_tmp
    
    commit



END;


--------------------------------------------------------------------------------
--#####################     END     ############################################
--##############################################################################
--##############################################################################
--##############################################################################





