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
# Created:      01/10/2014
# Created by:   Alan Barber (ABA)
# Description:
#
# List of steps:
# --------------
#
# code_location_A01
# code_location_A02
# ----------------------------------------------------------
# code_location_B01
# ----------------------------------------------------------
# code_location_C01
# ----------------------------------------------------------
# code_location_D01
# ----------------------------------------------------------
# code_location_E01
# ----------------------------------------------------------
# code_location_F01
# ----------------------------------------------------------
# code_location_G01  Clean-up tables that not required
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - VESPA_ANALYSTS.channel_map_prod_service_key_attributes (used to build VESPA_Comscore_SkyGo_Channel_Mapping_tbl)
#     - VESPA_Comscore_SkyGo_Channel_Mapping_tbl (based on 12months comscore data, and VESPA_ANALYSTS.channel_map_prod_service_key_attributes)
#     - VESPA_Comscore_SkyGo_linear_channel_mapping_audit_tbl
#     - VESPA_Comscore_SkyGo_[YYYYMM]
#     - VESPA_PROGRAMME_SCHEDULE
#
# => Temporary Tables:
#     - VESPA_Comscore_linear_base_tmp
#     - Comscore_lin_account_list_tmp
#     - vespa_comscore_skygo_programme_schedule_tmp
#     - #Comscore_skygo_mapping_aggregated_tmp
#     - #Comscore_skygo_mapping_summary_tmp
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 26/09/2014  ABA   Initial version
# 10/10/2014  ABA   Changed table names to production naming convention
# 11/11/2014  ABA   Added run_datetime to VESPA_Comscore_SkyGo_linear_channel_mapping_audit_tbl
#                   so that proc can be run and logged multiple times on the same data date
# 11/12/2014  ABA   Section F01 to drop temporary tables at the end of the proc
# 16/12/2014  ABA   Swapped last two columns of channel audit table (total events/accounts) to correct order
# 07/01/2015  ABA   Added some indexes to the temporary list of accounts that is built
# 12/01/2015  ABA   Added new section to isolate the vespa_programme_schedule period of interest, before use in the 'batch' section
#
###############################################################################*/


CREATE or REPLACE procedure vespa_comscore_skygo_linear_split(
                IN @data_run_date       DATE
                ) AS
BEGIN

--temp
--declare @data_run_date       DATE
--set @data_run_date = '2014-11-14'



    declare @xsql varchar(8000)
    declare @batch_size  INTEGER
    declare @iterations  INTEGER
    declare @iteration   INTEGER
    declare @sam_ac_low  INTEGER
    declare @sam_ac_high INTEGER
    declare @monthly_table_name     varchar(72)



 /* code_location_A01 ***********************************************************
     *****                                                                     **
     *****   Copy the first row of the current aggregate event table so        **
     *****   we can build, then overwrite using the same format later.         **
     *****                                                                     **
     ****************************************************************************/

    SET @monthly_table_name  =  'VESPA_Comscore_SkyGo_'||dateformat(@data_run_date, 'YYYYMM')


    IF object_id('barbera.VESPA_Comscore_linear_base_tmp') IS NOT NULL
        BEGIN
            drop table barbera.VESPA_Comscore_linear_base_tmp
        END

    --set the dimensions of the staging table from the format of the final aggregated view
    SET @xsql = 'select top 1 t.* into barbera.VESPA_Comscore_linear_base_tmp from '||@monthly_table_name||' t commit'
    execute(@xsql)
    truncate table barbera.VESPA_Comscore_linear_base_tmp
    commit



 /* code_location_B01 ***********************************************************
     *****                                                                     **
     *****   Create a list of accounts that have viewed linear content         **
     *****   and that have not been split into programme instances yet         **
     *****                                                                     **
     ****************************************************************************/

    set @xsql = '
        --create list of accounts to iterate through for the day
        IF object_id(''barbera.Comscore_lin_account_list_tmp'') IS NOT NULL
            BEGIN
                drop table barbera.Comscore_lin_account_list_tmp
            END

        select sam_profileid, dense_rank() over(order by sam_profileid) rank
          into barbera.Comscore_lin_account_list_tmp
          from '||@monthly_table_name||' v
         where v.data_date_local = '''||@data_run_date||'''
           and v.stream_context = ''lin''
           and v.linear_instance_flag = 0
         group by sam_profileid

         commit'

    execute(@xsql)

    --add index to sam_profileid, rank
    create HNG index Comscore_lin_account_list_samprofile_idx  on Comscore_lin_account_list_tmp(sam_profileid)
    create HNG index Comscore_lin_account_list_rank_idx on Comscore_lin_account_list_tmp(rank)
    commit



    /* code_location_C01 ***********************************************************
     *****                                                                     **
     *****     Extract section of the programme schedule that we will use      **
     *****                                                                     **
     ****************************************************************************/

    IF object_id('barbera.vespa_comscore_skygo_programme_schedule_tmp') IS NOT NULL
        BEGIN
            drop table barbera.vespa_comscore_skygo_programme_schedule_tmp
        END

    SELECT service_key,
           broadcast_start_date_time_utc,
           broadcast_end_date_time_utc,
           programme_instance_duration,
           programme_name,
           genre_description,
           sub_genre_description,
           dk_programme_instance_dim,
           broadcast_start_date_time_local,
           broadcast_end_date_time_local
      into vespa_comscore_skygo_programme_schedule_tmp
      from VESPA_PROGRAMME_SCHEDULE
     where cast(broadcast_start_date_time_utc as date) = @data_run_date
        or cast(broadcast_end_date_time_utc as date)   = @data_run_date
    commit

    --add indexes
    create dttm index vespa_comscore_skygo_programme_schedule_start_idx on vespa_comscore_skygo_programme_schedule_tmp(broadcast_start_date_time_utc)
    create dttm index vespa_comscore_skygo_programme_schedule_end_idx on vespa_comscore_skygo_programme_schedule_tmp(broadcast_end_date_time_utc)
    create LF index vespa_comscore_skygo_programme_schedule_servicekey_idx on vespa_comscore_skygo_programme_schedule_tmp(service_key)
    commit



 /* code_location_D01 ***********************************************************
     *****                                                                     **
     *****   Batch-up (prepare) the code so it can run over a batch of         **
     *****   accounts, identifying linear broadcast content from the EPG       **
     *****   coinsiding with the linear SkyGo usage                            **
     *****                                                                     **
     ****************************************************************************/

    SET @batch_size = 50000

    set @xsql = '
        INSERT into barbera.VESPA_Comscore_linear_base_tmp
        select v.account_number, v.cb_key_household,
               v.sam_profileid,
               v.ns_ap_device,
               v.platform_name,
               v.platform_version,
               v.stream_context,
               v.station_name,
               v.channel_id,
               s.service_key,
               v.vod_asset_id, v.ad_asset_id,
               s.dk_programme_instance_dim,
               s.broadcast_start_date_time_utc,
               s.broadcast_end_date_time_utc,
               s.broadcast_start_date_time_local,
               s.broadcast_end_date_time_local,
               case when v.server_event_start_utc >= s.broadcast_start_date_time_utc then v.server_event_start_utc
                    else s.broadcast_start_date_time_utc
                end programme_instance_start_utc,
               case when v.server_event_end_utc <= s.broadcast_end_date_time_utc then v.server_event_end_utc
                    else s.broadcast_end_date_time_utc
                end programme_instance_end_utc,
               case when v.server_start_local_time >= s.broadcast_start_date_time_local then v.server_start_local_time
                    else s.broadcast_start_date_time_local
                end programme_instance_start_local,
               case when v.server_end_local_time <= s.broadcast_end_date_time_local then v.server_end_local_time
                    else s.broadcast_end_date_time_local
                end programme_instance_end_local,
               v.viewing_event_start_utc,
               v.viewing_event_end_utc,
               v.viewing_event_start_utc_raw,
               v.viewing_event_end_utc_raw,
               v.viewing_event_start_local,
               v.viewing_event_end_local,
               v.daylight_savings_start_flag,
               v.daylight_savings_end_flag,
               v.server_event_start_utc,
               v.server_event_end_utc,
               v.server_event_start_utc_raw,
               v.server_event_end_utc_raw,
               v.server_start_local_time,
               v.server_end_local_time,
               v.connection_type_start,
               v.connection_type_end,
               s.genre_description,
               s.sub_genre_description,
               v.ad_flag,
               v.aggr_event_id,
               v.event_count,
               v.erroneous_data_suspected_flag,
               v.view_continuing_flag,
               v.view_continues_next_day_flag,
               1 linear_instance_flag,
               s.programme_instance_duration as content_duration,
               s.programme_name,
               datediff(ss,
                        case when v.server_event_start_utc >= s.broadcast_start_date_time_utc then v.server_event_start_utc else s.broadcast_start_date_time_utc end,
                        case when v.server_event_end_utc <= s.broadcast_end_date_time_utc then v.server_event_end_utc else s.broadcast_end_date_time_utc end
               ) as duration_viewed, -- calculated using the timestamps, not accurate using ns_st_pt for linear viewing
               case when coalesce(content_duration, 0) =0 then null
                    else cast(round((duration_viewed*1.0/content_duration*1.0),2) as decimal(5,3))
                end percentage_viewed,
               cast(''###data_run_date###'' as date) data_date_local,
               today() load_date
          from vespa_comscore_skygo_programme_schedule_tmp s,
               VESPA_Comscore_SkyGo_Channel_Mapping_tbl cm,
               ###monthly_table_name### v,
               barbera.Comscore_lin_account_list_tmp a
          where v.data_date_local = ''###data_run_date###''
            and a.sam_profileid = v.sam_profileid
            and a.rank between ###XXX### and ###YYY###
            and s.service_key = cm.service_key
            and v.station_name = cm.station_name
            and v.stream_context = ''lin''
            and v.linear_instance_flag = 0
            --and v.duration_viewed > 0
            and (s.broadcast_start_date_time_utc <= v.server_event_end_utc
                 and s.broadcast_end_date_time_utc >= v.server_event_start_utc )
          commit
                '

    select @iterations = cast(ceiling((max(rank)*1.0/@batch_size*1.0)) as integer)
      from barbera.Comscore_lin_account_list_tmp

    EXEC VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> '||@iterations||' required'

    SET @iteration = 1
    SET @sam_ac_low  = @iteration
    SET @sam_ac_high = @batch_size



 /* code_location_D02 ***********************************************************
     *****                                                                     **
     *****   Iterate through the batches identifying linear broadcast          **
     *****   content from the EPG                                              **
     *****                                                                     **
     ****************************************************************************/

    --work through the list of accounts, creating the programme instances
    WHILE (@iteration <= @iterations)
        BEGIN
            EXEC VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Inserting linear instances '||@iteration||'(of'||@iterations||')'
            execute(
              replace(
                replace(
                  replace(
                    replace(@xsql,
                      '###data_run_date###', @data_run_date),
                      '###monthly_table_name###', @monthly_table_name),
                      '###XXX###', @sam_ac_low),
                      '###YYY###', @sam_ac_high)
            )
            SET @iteration   = @iteration   + 1
            SET @sam_ac_low  = @sam_ac_low  + @batch_size
            SET @sam_ac_high = @sam_ac_high + @batch_size
        END

    create index VESPA_Comscore_linear_base_tmp_aggr_event_id_idx on barbera.VESPA_Comscore_linear_base_tmp(aggr_event_id)




    /* code_location_E01 ***********************************************************
     *****                                                                        **
     *****         Record audit stats for channel mapping of                      **
     *****         linear events                                                  **
     *****                                                                        **
     *******************************************************************************/

    SET @xsql = '
      select V.data_date_local, V.aggr_event_id AggrView, V.station_name AggrView_Station, L.aggr_event_id Lin, L.station_name Lin_Station, V.sam_profileid
        into barbera.Comscore_skygo_mapping_summary_tmp
        from '||@monthly_table_name||' V
                LEFT JOIN
             barbera.VESPA_Comscore_linear_base_tmp L
                ON V.aggr_event_id = L.aggr_event_id
        where V.stream_context = ''lin''
          and V.data_date_local = cast('''||@data_run_date||''' as date)
       group by V.data_date_local, V.aggr_event_id, V.station_name, L.aggr_event_id, L.station_name, V.sam_profileid
     '

    execute(@xsql)


    --load this into a channels mapped audit history table
    select data_date_local,
           AggrView_Station station_name,
           Lin_Station mapping_name,
           count(distinct sam_profileid) accounts,
           count() events,
           case when Lin_Station is null then 1 else 0 end error,
           dense_rank() over(partition by station_name order by error) rank,
           case when error = 1 AND rank = 2 then 1 else 0 end unknown_error,
           case when error = 1 AND unknown_error = 0 then 1 else 0 end mapping_error
      into barbera.Comscore_skygo_mapping_aggregated_tmp
      from barbera.Comscore_skygo_mapping_summary_tmp
     group by data_date_local, station_name, Lin_Station
     order by station_name



    INSERT into VESPA_Comscore_SkyGo_linear_channel_mapping_audit_tbl
    select data_date_local,
           dateformat(now(), 'YYYY-MM-DD HH:mm:SS'), --run_datetime
           station_name,
           max(case when mapping_error = 0 AND unknown_error = 0 then events else null end) events_mapped,
           max(case when mapping_error = 0 AND unknown_error = 0 then accounts else null end) accounts_mapped,
           max(case when mapping_error = 1 then events else null end) events_not_mapped,
           max(case when mapping_error = 1 then accounts else null end) accounts_not_mapped,
           max(case when unknown_error = 1 then events else null end) event_unknown_errors,
           max(case when unknown_error = 1 then accounts else null end) account_unknown_errors,
           sum(events) total_events,
           sum(accounts) total_accounts
      from barbera.Comscore_skygo_mapping_aggregated_tmp
     group by data_date_local, station_name
     order by station_name

    commit



    /* code_location_F01 ***********************************************************
     *****                                                                        **
     *****         Re-write the *programme instance* events over the              **
     *****         existing linear *channel* events                               **
     *****                                                                        **
     *******************************************************************************/

    set @xsql = '
             DELETE FROM ###monthly_table_name###
               FROM ###monthly_table_name### a, barbera.VESPA_Comscore_linear_base_tmp b
              WHERE a.aggr_event_id = b.aggr_event_id

             commit
           '
    execute(replace(@xsql, '###monthly_table_name###', @monthly_table_name))

    --then insert
    set @xsql = '
        INSERT INTO ###monthly_table_name###
          select *
            from barbera.VESPA_Comscore_linear_base_tmp
            where duration_viewed > 0
        commit
     '
    execute(replace(@xsql, '###monthly_table_name###', @monthly_table_name))

    ---->  [stats written to audit table completed in the main procedure]



  /* code_location_G01 *************************************************************
     *****                                                                        **
     *****         Clean-up tables that not required                              **
     *****                                                                        **
     *******************************************************************************/

    drop table barbera.vespa_comscore_skygo_programme_schedule_tmp
    drop table barbera.VESPA_Comscore_linear_base_tmp
    drop table barbera.Comscore_lin_account_list_tmp
    drop table barbera.Comscore_skygo_mapping_summary_tmp
    drop table barbera.Comscore_skygo_mapping_aggregated_tmp


END



--------------------------------------------------------------------------------
--#####################     END     ############################################
--##############################################################################
--##############################################################################
--##############################################################################


