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


        V239 - Oneview Comscore (SkyGo) - Aggregated Event View


###############################################################################
# Created between:   06/01/2015 - 07/01/2015
# Created by:        Alan Barber (ABA)
# Description:       Builds universe information by combining the day specified to the
#                    existing universe, or if NULL specified - then builds afresh using all
#                    raw Comscore data in the union view
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
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - sk_prod.COMSCORE_UNION_VIEW
#     - sk_prod.VESPA_Comscore_SkyGo_SAV_summary_tbl
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 07/01/2015  ABA   Initial version
#
#
###############################################################################*/


create or replace procedure vespa_comscore_skygo_universe_build(
                IN @data_run_date            date
                ) AS
BEGIN

   --declare @data_run_date date
   --set @data_run_date = '2015-01-01'

   declare @load_file_name varchar(22)
   declare @xsql varchar(8000)

   --if running for a specfied run day, then specify the expected file name for the day's data
   IF @data_run_date is NOT NULL
      BEGIN
         set @load_file_name = 'Comscore_'||dateformat(cast(@data_run_date as date), 'YYYYMMDD')||'.gz'
      END

   --drop the tmp table before we create it
   IF object_id('VESPA_Comscore_SkyGo_universe_day_tmp') IS NOT NULL
      BEGIN
         drop table VESPA_Comscore_SkyGo_universe_day_tmp
      END

   --dynamic SQL for stats associated with either the day specfied, or ALL days if NULL specified
   set @xsql = '
                   select cast('''||@data_run_date||''' as date) local_data_date,
                          c.sam_profileid,
                          sav.account_number,
                          sav.cb_key_household,
                          coalesce(sav.exclude_flag, 0)                                                  exclude_flag,       --default to 0 (dont exclude)
                          coalesce(sav.aggregate_flag, 0)                                                aggregate_flag,     --default to 0 (dont aggregate)
                          max(cast(ns_utc as date))                                                      last_received_date,
                          cast(dateformat(min(ns_ts), ''YYYY-MM-DD HH:MM:SS'') as datetime)              first_event_utc,    --local client time-stamp
                          cast(dateformat(max(ns_ts), ''YYYY-MM-DD HH:MM:SS'') as datetime)              latest_event_utc,   --local client time-stamp
                          cast(dateformat(min(ns_utc), ''YYYY-MM-DD HH:MM:SS'') as datetime)             first_data_utc,     --timestamp recived at the Comscore server
                          cast(dateformat(max(ns_utc), ''YYYY-MM-DD HH:MM:SS'') as datetime)             latest_data_utc,    --timestamp recived at the Comscore server
                          cast(max(case when sg_vs_sc = ''lin'' then ns_utc else null end) as datetime)  latest_lin_data_utc,
                          cast(max(case when sg_vs_sc = ''vod'' then ns_utc else null end) as datetime)  latest_vod_data_utc,
                          cast(max(case when sg_vs_sc = ''dvod'' then ns_utc else null end) as datetime) latest_dvod_data_utc,
                          max(cb_data_date)                                                              last_load_date,
                          cast(max(ns_utc) as date)                                                      event_last_load_date
                     into VESPA_Comscore_SkyGo_universe_day_tmp
                     from COMSCORE_UNION_VIEW c                                 --using the union view
                             left join VESPA_Comscore_SkyGo_SAV_summary_tbl sav --combine with SAV
                                 on c.sam_profileid = sav.sam_profileid
                    ###WHERE_CLAUSE###
                    group by c.sam_profileid, sav.account_number, sav.cb_key_household, sav.exclude_flag, sav.aggregate_flag

                   commit '

   IF @data_run_date is NOT NULL
      BEGIN
         execute( replace(@xsql, '###WHERE_CLAUSE###', 'where c.cb_source_file = '''||@load_file_name||'''') )
      END
   ELSE
      BEGIN --used when @run_data_date is NULL
         execute( replace(@xsql, '###WHERE_CLAUSE###', '') )    --## FIRST TIME or Re-FRESH  is unrestricted (to allow a fresh build)
      END


   --need to create the universe table if it doesn't exist <VESPA_Comscore_SkyGo_universe_tbl> before we use it
   IF object_id('VESPA_Comscore_SkyGo_universe_tbl') IS NULL
      BEGIN
         create table VESPA_Comscore_SkyGo_universe_tbl(
                sam_profileid           BIGINT         DEFAULT NULL,
                account_number           VARCHAR(14)    DEFAULT NULL,
                cb_key_household         BIGINT         DEFAULT NULL,
                exclude_flag             SMALLINT       DEFAULT NULL,
                aggregate_flag           SMALLINT       DEFAULT NULL,
                last_received_date       DATE           DEFAULT NULL,
                first_event_utc          DATETIME       DEFAULT NULL,
                latest_event_utc         DATETIME       DEFAULT NULL,
                first_data_utc           DATETIME       DEFAULT NULL,
                latest_data_utc          DATETIME       DEFAULT NULL,
                latest_lin_data_utc      DATETIME       DEFAULT NULL,
                latest_vod_data_utc      DATETIME       DEFAULT NULL,
                latest_dvod_data_utc     DATETIME       DEFAULT NULL,
                last_load_date           DATE           DEFAULT NULL,
                event_last_load_date     DATE           DEFAULT NULL,
                last_expected_event_date DATE           DEFAULT NULL,
                days_since_last_data     INTEGER        DEFAULT NULL)
         commit
         create index VESPA_Comscore_universe_sam_idx on VESPA_Comscore_SkyGo_universe_tbl(sam_profileid)
         commit
      END


   --- now combine with existing universe stats to update the table
   IF object_id('VESPA_Comscore_SkyGo_universe_tmp') IS NOT NULL
      BEGIN
         drop table VESPA_Comscore_SkyGo_universe_tmp
      END

   select coalesce(u.sam_profileid, d.sam_profileid)                               sam_profileid,
          coalesce(u.account_number, d.account_number)                             account_number,
          coalesce(u.cb_key_household, d.cb_key_household)                         cb_key_household,
          coalesce(d.exclude_flag, u.exclude_flag)                                 exclude_flag,   --the processing day in preference
          coalesce(d.aggregate_flag, u.aggregate_flag)                             aggregate_flag, --the processing day in preference
          case when coalesce(u.last_received_date, d.last_received_date) > coalesce(d.last_received_date, u.last_received_date) then coalesce(u.last_received_date, d.last_received_date) else coalesce(d.last_received_date, u.last_received_date) end                 AS last_received_date,
          case when coalesce(u.first_event_utc, d.first_event_utc) < coalesce(d.first_event_utc, u.first_event_utc)             then coalesce(u.first_event_utc, d.first_event_utc) else coalesce(d.first_event_utc, u.first_event_utc) end                                         AS first_event_utc,
          case when coalesce(u.latest_event_utc, d.latest_event_utc) > coalesce(d.latest_event_utc, u.latest_event_utc)         then coalesce(u.latest_event_utc, d.latest_event_utc) else coalesce(d.latest_event_utc, u.latest_event_utc) end                                 AS latest_event_utc,
          case when coalesce(u.first_data_utc, d.first_data_utc) < coalesce(d.first_data_utc, u.first_data_utc)                 then coalesce(u.first_data_utc, d.first_data_utc) else coalesce(d.first_data_utc, u.first_data_utc) end                                                 AS first_data_utc,
          case when coalesce(u.latest_data_utc, d.latest_data_utc) > coalesce(d.latest_data_utc, u.latest_data_utc)             then coalesce(u.latest_data_utc, d.latest_data_utc) else coalesce(d.latest_data_utc, u.latest_data_utc) end                                         AS latest_data_utc,
          case when coalesce(u.latest_lin_data_utc, d.latest_lin_data_utc) > coalesce(d.latest_lin_data_utc, u.latest_lin_data_utc) then coalesce(u.latest_lin_data_utc, d.latest_lin_data_utc) else coalesce(d.latest_lin_data_utc, u.latest_lin_data_utc) end         AS latest_lin_data_utc,
          case when coalesce(u.latest_vod_data_utc, d.latest_vod_data_utc) > coalesce(d.latest_vod_data_utc, u.latest_vod_data_utc) then coalesce(u.latest_vod_data_utc, d.latest_vod_data_utc) else coalesce(d.latest_vod_data_utc, u.latest_vod_data_utc) end         AS latest_vod_data_utc,
          case when coalesce(u.latest_dvod_data_utc, d.latest_dvod_data_utc) > coalesce(d.latest_dvod_data_utc, u.latest_dvod_data_utc) then coalesce(u.latest_dvod_data_utc, d.latest_dvod_data_utc) else coalesce(d.latest_dvod_data_utc, u.latest_dvod_data_utc) end AS latest_dvod_data_utc,
          case when coalesce(u.last_load_date, d.last_load_date) > coalesce(d.last_load_date, u.last_load_date)                 then coalesce(u.last_load_date, d.last_load_date) else coalesce(d.last_load_date, u.last_load_date) end                                                 AS last_load_date,
          case when coalesce(u.event_last_load_date, d.event_last_load_date) > coalesce(d.event_last_load_date, u.event_last_load_date) then coalesce(u.event_last_load_date, d.event_last_load_date) else coalesce(d.event_last_load_date, u.event_last_load_date) end AS event_last_load_date,
          cast(dateadd(dd, -1, today()) as date)                                   last_expected_event_date,
          datediff(dd, event_last_load_date, last_expected_event_date)             days_since_last_data
     into VESPA_Comscore_SkyGo_universe_tmp
     from VESPA_Comscore_SkyGo_universe_tbl u      -- existing universe
            FULL OUTER JOIN
          VESPA_Comscore_SkyGo_universe_day_tmp d  -- day we are appending
            ON u.sam_profileid = d.sam_profileid
   commit


   --Copy the new combined universe information into the universe_tbl
   TRUNCATE table VESPA_Comscore_SkyGo_universe_tbl

   INSERT into VESPA_Comscore_SkyGo_universe_tbl
      select  *
        from VESPA_Comscore_SkyGo_universe_tmp

   DROP table VESPA_Comscore_SkyGo_universe_tmp

   commit



END

--------------
--------------











---------------
-- run the proc

declare @data_run_date date
    set @data_run_date = null --'2015-01-01'

exec vespa_comscore_skygo_universe_build @data_run_date




select top 1000 *
  from VESPA_Comscore_SkyGo_universe_tbl




---------------------------------------------------------
-- this can be run separately for the rolling stats
   declare @last_expected_event_date date

    select @last_expected_event_date = max(data_date)-1
      from barbera.vespa_comscore_skygo_audit_run_tbl

    select count(distinct ns_ap_device)                                             device_types_in_yr,
           count(distinct case when sg_vs_sc = 'lin' then ns_st_ci else null end)   unique_lin_channels_in_yr,
           count(distinct case when sg_vs_sc = 'vod' then ns_st_ci else null end)   unique_vod_assets_in_yr,
           count(distinct case when sg_vs_sc = 'dvod' then ns_st_ci else null end)  unique_dvod_assets_in_yr,
           count( distinct case when cast(ns_utc as date) > dateadd(dd, -7, @last_expected_event_date) then cast(ns_utc as date) else null end) days_active_rolling_week,
           count( distinct case when cast(ns_utc as date) > dateadd(mm, -1, @last_expected_event_date) then cast(ns_utc as date) else null end) days_active_rolling_month,
           count( distinct case when cast(ns_utc as date) > dateadd(qq, -1, @last_expected_event_date) then cast(ns_utc as date) else null end) days_active_rolling_quarter,
           count( distinct case when cast(ns_utc as date) > dateadd(yy, -1, @last_expected_event_date) then cast(ns_utc as date) else null end) days_active_rolling_year
      into VESPA_Comscore_SkyGo_universe_rolling_tmp
      from COMSCORE_UNION_VIEW c --using the union view
              left join VESPA_Comscore_SkyGo_SAV_summary_tbl sav --combine with SAV
                 on c.sam_profileid = sav.sam_profileid
     group by c.sam_profileid, sav.account_number, sav.cb_key_household, sav.exclude_flag, sav.aggregate_flag
    commit
--1:4:14 to run





----------------
----------------
--update the universe table to the new format
drop table VESPA_Comscore_SkyGo_universe_tbl_tmp
select cast(sam_profileid as BIGINT),
                cast(account_number           as VARCHAR(14)) account_number,
                cast(cb_key_household         as BIGINT)      cb_key_household,
                cast(exclude_flag             as SMALLINT)    exclude_flag,
                cast(aggregate_flag           as SMALLINT)    aggregate_flag,
                cast(last_received_date       as DATE)        last_received_date,
                cast(first_event_utc          as DATETIME)    first_event_utc,
                cast(latest_event_utc         as DATETIME)    latest_event_utc,
                cast(first_data_utc           as DATETIME)    first_data_utc,
                cast(latest_data_utc          as DATETIME)    latest_data_utc,
                cast(latest_lin_data_utc      as DATETIME)    latest_lin_data_utc,
                cast(latest_vod_data_utc      as DATETIME)    latest_vod_data_utc,
                cast(latest_dvod_data_utc     as DATETIME)    latest_dvod_data_utc,
                cast(last_load_date           as DATE)        last_load_date,
                cast(event_last_load_date     as DATE)        event_last_load_date,
                cast(last_expected_event_date as DATE)        last_expected_event_date,
                cast(days_since_last_data     as INTEGER)     days_since_last_data
  into VESPA_Comscore_SkyGo_universe_tbl_tmp
  from VESPA_Comscore_SkyGo_universe_tbl
commit

--copy to tmp to the real universe table
drop table VESPA_Comscore_SkyGo_universe_tbl
select *
into VESPA_Comscore_SkyGo_universe_tbl
from VESPA_Comscore_SkyGo_universe_tbl_tmp

drop table VESPA_Comscore_SkyGo_universe_tbl_tmp
commit






