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
# Created between:   29/09/2014
# Created by:        Alan Barber (ABA)
# Description:
#
# List of steps:
# --------------
#
# code_location_A01     Create the temporary table used for the manual inserts
# code_location_A02     Make the manual mappings
# ----------------------------------------------------------
# code_location_B01     Update the existing SkyGo Comscore channel mapping info with the manual updates
# ----------------------------------------------------------
# code_location_C01     Secondary Update of Comscore channel mapping info from manually controlled table
#
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - VESPA_SkyGo_Channel_Mapping_tbl
#     -
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 29/09/2014  ABA   Initial version
# 05/11/2014  ABA   modified to match station_name using lower()
# 28/11/2014  ABA   added the ability to add channel mapping info from a secondary table
#                   vespa_shared.VESPA_Comscore_SkyGO_channel_mapping_adjustments_info
#
###############################################################################*/



CREATE or REPLACE procedure barbera.vespa_comscore_skygo_channelmap_manual_updates() AS
BEGIN


    /* code_location_A01 ***********************************************************
     *****                                                                        **
     *****       Create the temporary table used for the manual inserts           **
     *****                                                                        **
     *******************************************************************************/

    IF object_id('barbera.Comscore_missing_channel_mapping_info_tmp') IS NOT NULL
        BEGIN
                DROP TABLE barbera.Comscore_missing_channel_mapping_info_tmp
        END


    create table barbera.Comscore_missing_channel_mapping_info_tmp (
        station_name varchar(48),
        service_key  integer    NULL,
        channel_name varchar(48)
    )




    /* code_location_A02 ***********************************************************
     *****                                                                        **
     *****       Make the manual mappings                                         **
     *****                                                                        **
     *******************************************************************************/

insert into barbera.Comscore_missing_channel_mapping_info_tmp values('$channel.Name',           null,   'unknown')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sample',                  null,   'unknown')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('unknown',                 null,   'unknown')

--insert into Comscore_missing_channel_mapping_info_tmp values('xXxXxXxXxXx',             null,   'unknown')


    /* code_location_B01 ***********************************************************
     *****                                                                        **
     *****       Update the existing SkyGo Comscore channel mapping               **
     *****       info with the manual updates                                     **
     *****                                                                        **
     *******************************************************************************/


    update barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl a
       set a.channel_name = m.channel_name,
           a.service_key = m.service_key
      from barbera.Comscore_missing_channel_mapping_info_tmp m
     where lower(a.station_name) = lower(m.station_name)
      and a.channel_name IS NULL --just update missing info with the over-ride

    drop table barbera.Comscore_missing_channel_mapping_info_tmp

    commit




    /* code_location_C01 ***********************************************************
     *****                                                                        **
     *****       A secondary update to SkyGo Comscore channel mapping             **
     *****       from a table in vespa_analysts so we can make further              **
     *****       adjustments outside the production run.                          **
     *****                                                                        **
     *******************************************************************************/

    IF object_id('vespa_analysts.VESPA_Comscore_SkyGO_channel_mapping_adjustments_info') IS NOT NULL
        BEGIN
            UPDATE barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl a
               set a.channel_name = m.channel_name,
                   a.service_key = m.service_key
              from vespa_analysts.VESPA_Comscore_SkyGO_channel_mapping_adjustments_info m
             where lower(a.station_name) = lower(m.station_name)
               and a.channel_name IS NULL --just update missing info with the over-ride
            commit

            --the above does not load channels that are *ONLY* represented by the manual lookup, 
            --so we do an additional insert for those
            INSERT into barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl
            select m.station_name, m.channel_name, m.service_key
              from vespa_analysts.VESPA_Comscore_SkyGO_channel_mapping_adjustments_info m
                     LEFT JOIN
                   barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl cm
                     ON lower(m.station_name) = lower(cm.station_name)
             where cm.station_name is NULL
            commit

        END

    
 
    
        
    /*Remove any duplicates that may have crept in*/
    IF object_id('barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl_dups') IS NOT NULL  
        BEGIN 
            drop table barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl_dups commit
        END

    --remove any dup service_keys that may have crept in
    select a.*, dense_rank() over(partition by station_name order by service_key, len(channel_name), channel_name) rank
      into barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl_dups
      from barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl a
    commit

    DELETE FROM barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl x   --check this is the right place to delete
            from barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl_dups a
     WHERE cast(x.service_key as INTEGER) = cast(a.service_key as INTEGER)
       AND a.rank = 2
    commit

    drop table barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl_dups
    commit

END;


--------------------------------------------------------------------------------
--#####################     END     ############################################
--##############################################################################
--##############################################################################
--##############################################################################


/*
select top 1000 *
from VESPA_ANALYSTS.channel_map_prod_service_key_attributes
where lower(vespa_name) like '%sky%'
and effective_to = '2999-12-31 00:00:00.000000'

*/


