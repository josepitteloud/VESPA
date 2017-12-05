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



CREATE or REPLACE procedure vespa_comscore_skygo_channelmap_manual_updates() AS
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
        service_key  integer    DEFAULT NULL,
        channel_name varchar(48)
    )




    /* code_location_A02 ***********************************************************
     *****                                                                        **
     *****       Make the manual mappings                                         **
     *****                                                                        **
     *******************************************************************************/



insert into barbera.Comscore_missing_channel_mapping_info_tmp values('$channel.Name',           null,   'unknown')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('4oD',                     null,   'unknown')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Baby TV',                 3645,   'BabyTV')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('British Eurosport 2',     1726,   'Eurosport UK')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Cartoon Netwrk',          5601,   'Cartoon Network')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Channel 4',               1621,   'Channel 4 London')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Channel 5',               1800,   'Channel 5')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('ComedyCentral',           2510,   'Comedy Central')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Disney Chnl',             1881,   'Disney Channel')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Disney Movies',           1838,   'Sky Movies Disney')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Eurosport',               1726,   'Eurosport UK')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Eurosport 2',             1841,   'Eurosport2 UK')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('FOX',                     1305,   'FX')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('GOLD',                    2304,   'Gold')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('ITV',                     6000,   'ITV1 London')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('ITV2',                    6240,   'ITV 2')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('ITV3',                    6260,   'ITV 3')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('ITV4',                    6272,   'ITV 4')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('ITVBe',                   6508,   'ITV Be')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('ITV Be',                  6508,   'ITV Be')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('More4',                   3340,   'More 4')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Nat Geo',                 1806,   'National Geographic')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('National Geographic Channel',     1806, 'National Geographic')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Nick Jr',                 1857,   'Nick Jr.')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('NickJr',                  1857,   'Nick Jr.')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('STV',                     6210,   'ITV1 STV Grampian')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sample',                  null,   'unknown')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky Action',              1001,   'Sky Movies Action & Adventure')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky Aliens',              1807,   'Sky Movies SciFi/Horror')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky Arts',                null,   'unknown')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky Comedy',              1002,   'Sky Movies Comedy')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky Disney',              1838,   'Sky Movies Disney')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky DramaRom',            1816,   'Sky Movies Drama & Romance')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky Movies Drama and Romance',            1816,   'Sky Movies Drama & Romance')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky Family',              1808,   'Sky Movies Family')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky LivingIt',            2207,   'Sky Livingit')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky Movies',              null,   'unknown')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky Movies Action',       1001,   'Sky Movies Action & Adventure')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky Movies Action and Adventure', 1001,   'Sky Movies Action & Adventure')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky Movies Aliens',               1807,   'Sky Movies SciFi/Horror')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky Movies Sci-Fi & Horror',      1807,   'Sky Movies SciFi/Horror')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky Movies Superheroes',          1815,   'Sky Movies Superheroes')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky Premiere',            1409,   'Sky Movies Premiere')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky ScFi/Horror',         1807,   'Sky Movies SciFi/Horror')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky Select',              1811,   'Sky Movies Select')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky Showcase',            1814,   'Sky Movies Showcase')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky Sports',              null,   'unknown')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky Sports02',            1302,   'Sky Sports 2')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky Sports Formula 1',    1306,   'Sky Sports F1')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky Sports News',         1314,   'Sky Sports News HQ')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky Spts News',           1314,   'Sky Sports News HQ')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky Sp NewsHQ',           1314,   'Sky Sports News HQ')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky Sports News HQ',      1314,   'Sky Sports News HQ')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Sky Thriller',            1818,   'Sky Movies Crime & Thriller')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('Star Plus',               1771,   'STAR Plus')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('abc studios',             null,   'unknown')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('alibi',                   2303,   'Alibi')
insert into barbera.Comscore_missing_channel_mapping_info_tmp values('unknown',                 null,   'unknown')

--insert into Comscore_missing_channel_mapping_info_tmp values('xXxXxXxXxXx',             null,   'unknown')


    /* code_location_B01 ***********************************************************
     *****                                                                        **
     *****       Update the existing SkyGo Comscore channel mapping               **
     *****       info with the manual updates                                     **
     *****                                                                        **
     *******************************************************************************/


    update VESPA_Comscore_SkyGo_Channel_Mapping_tbl a
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
     *****       from a table in vespa_Shared so we can make further              **
     *****       adjustments outside the production run.                          **
     *****                                                                        **
     *******************************************************************************/

    IF object_id('vespa_shared.VESPA_Comscore_SkyGO_channel_mapping_adjustments_info') IS NOT NULL
        BEGIN
            update VESPA_Comscore_SkyGo_Channel_Mapping_tbl a
               set a.channel_name = m.channel_name,
                   a.service_key = m.service_key
              from vespa_shared.VESPA_Comscore_SkyGO_channel_mapping_adjustments_info m
             where lower(a.station_name) = lower(m.station_name)
               and a.channel_name IS NULL --just update missing info with the over-ride
            commit
        END




END


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


