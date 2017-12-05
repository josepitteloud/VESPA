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


create or replace procedure barbera.vespa_comscore_skygo_universe_build(
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
   IF object_id('barbera.VESPA_Comscore_SkyGo_universe_day_tmp') IS NOT NULL
      BEGIN
         drop table barbera.VESPA_Comscore_SkyGo_universe_day_tmp
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
                     into barbera.VESPA_Comscore_SkyGo_universe_day_tmp
                     from barbera.COMSCORE_UNION_VIEW c                                 --using the union view
                             left join barbera.VESPA_Comscore_SkyGo_SAV_summary_tbl sav --combine with SAV
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
   IF object_id('barbera.VESPA_Comscore_SkyGo_universe_tbl') IS NULL
      BEGIN
         create table barbera.VESPA_Comscore_SkyGo_universe_tbl(
                sam_profileid           BIGINT          NULL,
                account_number           VARCHAR(14)    NULL,
                cb_key_household         BIGINT         NULL,
                exclude_flag             SMALLINT       NULL,
                aggregate_flag           SMALLINT       NULL,
                last_received_date       DATE           NULL,
                first_event_utc          DATETIME       NULL,
                latest_event_utc         DATETIME       NULL,
                first_data_utc           DATETIME       NULL,
                latest_data_utc          DATETIME       NULL,
                latest_lin_data_utc      DATETIME       NULL,
                latest_vod_data_utc      DATETIME       NULL,
                latest_dvod_data_utc     DATETIME       NULL,
                last_load_date           DATE           NULL,
                event_last_load_date     DATE           NULL,
                last_expected_event_date DATE           NULL,
                days_since_last_data     INTEGER        NULL)
         commit
         create index VESPA_Comscore_universe_sam_idx on barbera.VESPA_Comscore_SkyGo_universe_tbl(sam_profileid)
         commit
      END


   --- now combine with existing universe stats to update the table
   IF object_id('barbera.VESPA_Comscore_SkyGo_universe_tmp') IS NOT NULL
      BEGIN
         drop table barbera.VESPA_Comscore_SkyGo_universe_tmp
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
     into barbera.VESPA_Comscore_SkyGo_universe_tmp
     from barbera.VESPA_Comscore_SkyGo_universe_tbl u      -- existing universe
            FULL OUTER JOIN
          barbera.VESPA_Comscore_SkyGo_universe_day_tmp d  -- day we are appending
            ON u.sam_profileid = d.sam_profileid
   commit


   --Copy the new combined universe information into the universe_tbl
   TRUNCATE table barbera.VESPA_Comscore_SkyGo_universe_tbl

   INSERT into barbera.VESPA_Comscore_SkyGo_universe_tbl
      select  *
        from barbera.VESPA_Comscore_SkyGo_universe_tmp

   DROP table barbera.VESPA_Comscore_SkyGo_universe_tmp

   commit



END;





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


        V239 - Oneview Comscore (SkyGo) - Account Inclusion, Run and Day stats


###############################################################################
# Created between:   06/08/2014 - 02/09/2014
# Created by:        Alan Barber (ABA)
# Description:       Prepares information before the build of the aggregated data view
#
# List of steps:
# --------------
#
# code_location_A01 Create information to control how accounts will be treated,
#                   identify exclusions, aggregations and normal accounts
# code_location_A02 Create summary of accounts, and SamProfileIDs
# ----------------------------------------------------------
# code_location_B01 Populate <VESPA_Comscore_SkyGo_audit_run_tbl> table with dates
#                   where data should be available
# code_location_B02 Populate <VESPA_Comscore_SkyGo_audit_stats_tbl> table with
#                   statistics, and indicate status for running the rest
#                   of the build.
# ----------------------------------------------------------
# code_location_C01 Tidy-up tables that are no longer required
#
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Required tables/objects:
#     - CUST_SINGLE_ACCOUNT_VIEW (sk_prod)
#     - SAM_REGISTRANT           (sk_prod)
#
# => Modified tables/objects:
#     - VESPA_Comscore_SkyGo_audit_run_tbl
#     - VESPA_Comscore_SkyGo_audit_stats_tbl
#     - VESPA_Comscore_SkyGo_SAV_summary_tbl
#     - VESPA_Comscore_SkyGo_SAV_account_type_tbl
#
# => Temporary tables/objects:
#     - V239_dates_array_tmp
#     - Vespa_Comscore_audit_counts_tmp
#     - #V239_audit_tmp
#     - #V239_audit_tmp2
#     - #V239_audit_tmp3
#
#################################################################################
# Result/testing
# ------------------------------------------------------------------------------
#   select * from VESPA_Comscore_SkyGo_audit_run_tbl
#   select * from VESPA_Comscore_SkyGo_audit_stats_tbl
#
#   select *
#     from VESPA_Comscore_SkyGo_audit_run_tbl r,
#          VESPA_Comscore_SkyGo_audit_stats_tbl s
#    where r.data_date = s.data_date
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 06/08/2014  ABA   Initial version
# 03/09/2014  ABA   Tidyed formatting
# 12/09/2014  ABA   1. Changed to pull out stats relating to local-time day at the server
#                   2. Re-wrote stats section as pulling out incorrect row count
# 20/10/2014  ABA   Added in the rejects count to the stats table from the Comscore_rejects ETL table
# 07/11/2014  ABA   Added @run_date parameter to procedure call
# 07/11/2014  ABA   Added hard check to see if the day's file has been loaded at all before anything happens
# 09/12/2014  ABA   Renaming '#Vespa_Comscore_audit_counts_tmp' to 'Vespa_Comscore_audit_counts_tmp' and dropping at proc end
# 10/12/2014  ABA   Added drop statement at script start to clear temporary tables if required
# 11/12/2014  ABA   Replaced section that collates rows and account stats from Comscore_union_view (in section B01)
# 11/12/2014  ABA   Ammended the process for updating the run and audit table stats
# 15/01/2015  ABA   Handled additional fields in the audit run tbl to allow for monitoring build progress
#
###############################################################################*/



CREATE or REPLACE procedure barbera.vespa_comscore_skygo_event_view_prep(
                IN @data_run_date            date,
                IN @lower_limit         integer,
                IN @stddev_over_rows    integer,
                IN @weighting_rows      integer
                ) AS
BEGIN


------------
/*
    DECLARE @data_run_date date
    --DECLARE @run_date date
    DECLARE @suppress_stats_universe_build bit
    DECLARE @lower_limit integer
    DECLARE @stddev_over_rows integer
    DECLARE @weighting_rows integer

    --set the date we are running this for
    SET @data_run_date = '2014-12-06'
    SET @suppress_stats_universe_build = 0

    --set variables for the monitoring stats (calculating expected data volumes from the underlaying data)
    SET @lower_limit = 200000 -- floor at which an alert is triggered regardless of other stats (number of expected accounts)
    SET @stddev_over_rows = 7 -- how many days the stddev of accounts is calculated over
    SET @weighting_rows = 4   -- how many days are used to exponentially weight the moving average

*/

-------------


    declare @days        integer
    declare @sqlx        varchar(128)
    declare @load_count  integer
    declare @load_file_name varchar(24)




   /* code_location_A01 ***********************************************************
    *****                                                                        **
    *****      Has the day's file been loaded                                    **
    *****                                                                        **
    *******************************************************************************/

    set @load_file_name = 'Comscore_'||dateformat(cast(@data_run_date as date), 'YYYYMMDD')||'.gz'

    commit
    --if this is 0 then we have no file loaded
    select @load_count = count(1)
      from barbera.Comscore_union_view
     where cb_source_file = @load_file_name

    --raise error
    IF @load_count = 0
        BEGIN
             RAISERROR 18001 'No File <'||@load_file_name||'> has been loaded into raw data table Comscore_'||dateformat(cast(@data_run_date as date), 'YYYYMM')
        END
    ELSE
        BEGIN

   IF object_id('barbera.V239_dates_array_tmp') IS NOT NULL   BEGIN  drop table barbera.V239_dates_array_tmp  END





   /* code_location_A01 ***********************************************************
    *****                                                                        **
    *****      Create information to control how accounts will be treated        **
    *****                                                                        **
    *******************************************************************************/

   IF object_id('barbera.VESPA_Comscore_SkyGo_SAV_account_type_tbl') IS NOT NULL
       TRUNCATE TABLE barbera.VESPA_Comscore_SkyGo_SAV_account_type_tbl
   ELSE RAISERROR 18002 'Table <barbera.VESPA_Comscore_SkyGo_SAV_account_type_tbl> does not exist'

   --create table that shows account types
   INSERT into barbera.VESPA_Comscore_SkyGo_SAV_account_type_tbl(acct_type_code, acct_sub_type, uniqid)
   select acct_type_code,  acct_sub_type,
          dense_rank() over(order by acct_type_code, acct_sub_type) rank
     from sk_prod.CUST_SINGLE_ACCOUNT_VIEW
   group by acct_type_code, acct_sub_type
   commit

   --identify accounts to completely exclude from all further manipulation
   update barbera.VESPA_Comscore_SkyGo_SAV_account_type_tbl
      set exclude_flag = 1
    where acct_sub_type in (
           'Monitoring',
           'Test/development',
           'Internal office/ test cards',
           'Investigation and Compliance',
           'Regulatory',
           'Contact Centre',
           'CA Sky Channels')

   --identify accounts where we don't want to identify the customer viewing
   --  use a list to show who we are going to include
   --** THIS IS THE LIST THAT WOULD most likely BE ALTERED BY A CHANGE REQUEST
   update barbera.VESPA_Comscore_SkyGo_SAV_account_type_tbl
      set aggregate_flag = 1
    where acct_type_code = 'NON'
      and exclude_flag != 1
      and coalesce(acct_sub_type,'unknown') not in (
                'Normal',
                'unknown',
                'NDS',
                'Competition Winner',
                'CA Cancelled Accounts',
                'CA Home Use',
                'DSO Help Scheme Customer',
                'Guest Billable',
                'Guest Priority Chargeable',
                'Manually Invoiced',
                'Priority Chargeable Installation Customers',
                'Sky Guest List',
                'Ex-Staff',
                'Royal Household',
                'Senior Staff',
                'Staff',
                'VIP',
                'VVIP')

   --if not excluded or aggregated then treat as a normal account
   update barbera.VESPA_Comscore_SkyGo_SAV_account_type_tbl
      set normal_flag = 1
    where --acct_type_code = 'STD'
          exclude_flag != 1
      and aggregate_flag !=1

   --just check that only one column has been indicated
   -- **ALERT if check_flag > 1  for any row
   update barbera.VESPA_Comscore_SkyGo_SAV_account_type_tbl
      set check_flag = (exclude_flag+aggregate_flag+normal_flag)

   commit


   /* code_location_A02 ***********************************************************
    *****                                                                        **
    *****   Create summary of accounts, and SamProfileIDs                        **
    *****                                                                        **
    *******************************************************************************/

   IF object_id('barbera.VESPA_Comscore_SkyGo_SAV_summary_tbl') IS NOT NULL
       TRUNCATE TABLE barbera.VESPA_Comscore_SkyGo_SAV_summary_tbl
   ELSE RAISERROR 18003 'Table <barbera.VESPA_Comscore_SkyGo_SAV_summary_tbl> does not exist'

   --join with SAM_Registrant, and SAV
   INSERT into barbera.VESPA_Comscore_SkyGo_SAV_summary_tbl
   select uniqid, account_number, samprofileid, cb_key_household, type_id, acct_type_code, acct_sub_type, exclude_flag, aggregate_flag, normal_flag
   from (
     select dense_rank() over(order by r.account_number, r.samprofileid, r.cb_key_household) uniqid,
            dense_rank() over(partition by r.account_number, r.samprofileid order by sav.acct_status_dt desc, cb_row_id desc) rank,
            r.account_number, r.samprofileid, r.cb_key_household,
            t.uniqid type_id,
            t.acct_type_code,
            t.acct_sub_type,
            t.exclude_flag,
            t.aggregate_flag,
            t.normal_flag
       from SAM_REGISTRANT r
      INNER JOIN sk_prod.CUST_SINGLE_ACCOUNT_VIEW sav
         ON r.account_number = sav.account_number
      INNER JOIN barbera.VESPA_Comscore_SkyGo_SAV_account_type_tbl t
         ON sav.acct_type_code = t.acct_type_code
        AND sav.acct_sub_type = t.acct_sub_type
      WHERE r.samprofileid is not null) a
    where a.rank = 1
   commit


   /* code_location_B01 ***********************************************************
    *****                                                                        **
    *****    Populate <VESPA_Comscore_SkyGo_audit_run_tbl> table with dates      **
    *****    where data should be available                                      **
    *******************************************************************************/

   -- we need a list of all the days between 2014-08-01 and now()
   -- there isn't any data before 2014-08-01 so use this as the reference date
   -- ideally we'd set min date from COMSCORE_UNION_VIEW, or @run_date - 30

   SET @days = datediff(dd,'2014-08-01',dateformat(now(),'YYYY-MM-DD'))+1


   IF object_id('barbera.V239_dates_array_tmp') IS NOT NULL
       BEGIN
           DROP TABLE barbera.V239_dates_array_tmp
       END

   create table barbera.V239_dates_array_tmp(
           uniqid          integer         not null identity,
           data_date       date            not null
   )



   -- dynamic sql for the insert
   set @sqlx = 'INSERT into barbera.V239_dates_array_tmp(data_date) values(dateadd(dd, ###days###, ''2014-08-01''))'


   -- execute dynamic insert statement - filling table with dates
   WHILE @days >= 0
     BEGIN
           execute(replace(@sqlx, '###days###', @days))
           SET @days = @days-1
     END
   commit


   IF object_id('barbera.Vespa_Comscore_audit_counts_tmp') IS NOT NULL
       BEGIN  drop table barbera.Vespa_Comscore_audit_counts_tmp  END


   --left join to this date list using the Comscore data_date
   SELECT d.data_date,
          coalesce(c.samprofiles, 0) samprofiles,
          coalesce(c.imported_rows, 0) imported_rows
     into barbera.Vespa_Comscore_audit_counts_tmp
     from barbera.V239_dates_array_tmp d
            LEFT JOIN
          (select cast(substring(cb_source_file,10,8) as date) data_date,
                  count(distinct c.sam_profileid) samprofiles,
                  count(1) imported_rows
             from COMSCORE_UNION_VIEW c
            group by data_date) c
       ON d.data_date = c.data_date
    ORDER BY d.data_date
   commit


 /*  -- remove the days we have not already processed, or have part processed
   DELETE VESPA_Comscore_SkyGo_audit_run_tbl
    where build_success = 0  -- catches aborted runs
       or basic_view_created = 0 --this is set to 0 at the start of the build
*/

   --insert (into final audit run table) new data_dates where date does not exist
   INSERT into barbera.VESPA_Comscore_SkyGo_audit_run_tbl (data_date)
        select a.data_date
          from barbera.Vespa_Comscore_audit_counts_tmp a
                 left outer join
               barbera.VESPA_Comscore_SkyGo_audit_run_tbl r
            on a.data_date = r.data_date
         where r.data_date is null
         order by a.data_date asc
   commit

   -- reset run details for the day we are processing
   UPDATE barbera.VESPA_Comscore_SkyGo_audit_run_tbl
      SET QA_status = 'unknown',
          build_started = 1,
          basic_view_created = 0,
          channels_mapped = 0,
          linear_events_split = 0,
          build_completed = 0,
          build_success = 0,
          build_completed_date = cast('0001-01-01 00:00:00' as datetime),
          build_duration_secs = -99
    where data_date = dateformat(cast(@data_run_date as date), 'YYYY-MM-DD')
   commit


   /* code_location_B02 ***********************************************************
    *****                                                                        **
    *****    Populate <VESPA_Comscore_SkyGo_audit_stats_tbl> table with statistics,    **
    *****    and indicate status for running the rest of the build.              **
    *****                                                                        **
    *******************************************************************************/


   --insert (into final audit_stats table) new data_dates where date does not exist
   INSERT into barbera.VESPA_Comscore_SkyGo_audit_stats_tbl (data_date)
        select a.data_date
          from barbera.Vespa_Comscore_audit_counts_tmp a
                 left outer join
               barbera.VESPA_Comscore_SkyGo_audit_stats_tbl r
            on a.data_date = r.data_date
         where r.data_date is null
         order by a.data_date asc
   commit


   --update stats audit table for all dates in the raw comscore_union_view
   UPDATE barbera.VESPA_Comscore_SkyGo_audit_stats_tbl s
      SET samprofiles = a.samprofiles,
          imported_rows = a.imported_rows
     FROM barbera.Vespa_Comscore_audit_counts_tmp a
    WHERE s.data_date = a.data_date

    commit


   --insert the reject row details
   UPDATE barbera.VESPA_Comscore_SkyGo_audit_stats_tbl x
      SET rejected_rows = sample_count
     FROM (select cb_data_date, count(1) sample_count
             from COMSCORE_REJECTS
            group by cb_data_date) a
    WHERE a.cb_data_date = x.data_date
    commit


   --make sure tables are dropped before use
   IF object_id('barbera.V239_audit_tmp') IS NOT NULL   BEGIN  drop table barbera.V239_audit_tmp  commit  END
   IF object_id('barbera.V239_audit_tmp2') IS NOT NULL  BEGIN  drop table barbera.V239_audit_tmp2 commit  END
   IF object_id('barbera.V239_audit_tmp3') IS NOT NULL  BEGIN  drop table barbera.V239_audit_tmp3 commit  END


   --rank dates in processing order
   select a.*,
          dense_rank() over(order by data_date) rank
   into barbera.V239_audit_tmp
   from barbera.Vespa_Comscore_audit_counts_tmp a


   --where the core stats are calculated: Part 1
   select dateformat(a.data_date,'YYYY-MM-DD') data_date,
          a.samprofiles,
          case when a.imported_rows = 1 then 0 else a.imported_rows end imported_rows, --fix to give true 0 reading when no rows
          a.rank,
          EXP_WEIGHTED_AVG (samprofiles, @weighting_rows) over(order by rank asc) exp_w
   into barbera.V239_audit_tmp2
   from barbera.V239_audit_tmp a
   order by rank asc


   --where the core stats are calculated: Part 2
   select a.*,
          cast(round(stddev(a.exp_w) OVER(ORDER BY rank asc
                            ROWS BETWEEN @stddev_over_rows PRECEDING and CURRENT ROW ),0) as integer)
                           AS exp_w_sd,
          exp_w - exp_w_sd AS exp_neg,
          case when (rank = 1) then 'ok'
               when samprofiles < @lower_limit then '<limit'
               when samprofiles < exp_neg then 'low'
               else 'ok' end status
   into barbera.V239_audit_tmp3
     from barbera.V239_audit_tmp2 a


    --update the stats info
    update barbera.VESPA_Comscore_SkyGo_audit_stats_tbl x
       SET samprofiles = coalesce(s.samprofiles, -99),
           sam_trigger_level = coalesce(s.exp_neg, -99),
           imported_rows = coalesce(s.imported_rows, -99),
           status = coalesce(s.status,'unknown')
      from barbera.V239_audit_tmp3 s
     where dateformat(s.data_date, 'YYYY-MM-DD') = dateformat(x.data_date, 'YYYY-MM-DD')
    commit


    --update the success of the load info
    update barbera.VESPA_Comscore_SkyGo_audit_run_tbl x
       SET x.qa_status = case when s.imported_rows <= 0 then 'no file' else coalesce(s.status,'unknown') end
      from barbera.V239_audit_tmp3 s, barbera.VESPA_Comscore_SkyGo_audit_run_tbl a
     where dateformat(s.data_date, 'YYYY-MM-DD') = dateformat(a.data_date, 'YYYY-MM-DD')
       AND dateformat(x.data_date, 'YYYY-MM-DD') = dateformat(a.data_date, 'YYYY-MM-DD')

    commit



   /* code_location_C01 ***********************************************************
    *****                                                                        **
    *****    Tidy-up tables that are no longer required                          **
    *****                                                                        **
    *******************************************************************************/

   drop table barbera.V239_dates_array_tmp
   drop table barbera.V239_audit_tmp
   drop table barbera.V239_audit_tmp2
   drop table barbera.V239_audit_tmp3
   drop table barbera.Vespa_Comscore_audit_counts_tmp
   commit

    END

END;

-- *****************                                    ************************
-- ##################o             E N D              o#########################
-- ####################o                            o###########################
-- #############################################################################



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
# 07/11/2016  ABA   Added the ability to trigger a rebuild of the underlying viewing data (from raw) by triggering the execution of 
#                   the vespa_comscore_skygo_event_view_create procedure
# 18/01/2017  ABA   Changed the programme schedule extract to use the local broadcast time to match to data_date as this uses local time not UTC
#
###############################################################################*/


CREATE or REPLACE procedure barbera.vespa_comscore_skygo_linear_split(
                IN @data_run_date       DATE,
                IN @build_level         SMALLINT -- default should be 0 (just split out channel events into programmes)
                                                 -- 1 = refresh channel mapping before splitting channels
                                                 -- 2 = rebuild channel events and refresh channel mapping (full rebuild) before splitting
                ) AS
BEGIN

--temp
--declare @data_run_date       DATE
--declare @build_level         smallint
--set @data_run_date = '2016-07-01'
--set @build_level = 1



    declare @xsql               varchar(10000)
    declare @batch_size         INTEGER
    declare @iterations         INTEGER
    declare @iteration          INTEGER
    declare @sam_ac_low         INTEGER
    declare @sam_ac_high        INTEGER
    declare @monthly_table_name varchar(300)



/* code_location_A00 ***********************************************************
     *****                                                                     **
     *****   Trigger the rebuild of viewing data if requested by input         **
     *****   parameter, and update of channel mapping                          **
     *****                                                                     **
     ****************************************************************************/

    IF @build_level = 2
        BEGIN
           EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Complete rebuild of viewing data requested.' 
           EXEC barbera.vespa_comscore_skygo_event_view_create @data_run_date
           IF @@error = 0   EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Rebuild of viewing data completed [Successfully]' 
           ELSE             EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Rebuild of viewing data [FAILED('||@error||')] when executing linear split' 
        END

    /* code_location_G01 *************************************************************************
     *****                                                                                      **
     *****                      Create Channel Mapping Information                              **
     *****                                                                                      **
     *********************************************************************************************/

    IF @build_level > 0
        BEGIN 
            EXEC barbera.vespa_comscore_skygo_channelmap_build @data_run_date
        END    


 /* code_location_A01 ***********************************************************
     *****                                                                     **
     *****   Copy the first row of the current aggregate event table so        **
     *****   we can build, then overwrite using the same format later.         **
     *****                                                                     **
     ****************************************************************************/

    SET @monthly_table_name  =  'barbera.VESPA_Comscore_SkyGo_'||dateformat(@data_run_date, 'YYYYMM')


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
    create HNG index Comscore_lin_account_list_samprofile_idx  on barbera.Comscore_lin_account_list_tmp(sam_profileid)
    create HNG index Comscore_lin_account_list_rank_idx on barbera.Comscore_lin_account_list_tmp(rank)
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
      into barbera.vespa_comscore_skygo_programme_schedule_tmp
      from VESPA_PROGRAMME_SCHEDULE
     where cast(broadcast_start_date_time_local as date) = @data_run_date
        or cast(broadcast_end_date_time_local as date)   = @data_run_date
    commit

    --add indexes
    create dttm index vespa_comscore_skygo_programme_schedule_start_idx on barbera.vespa_comscore_skygo_programme_schedule_tmp(broadcast_start_date_time_utc)
    create dttm index vespa_comscore_skygo_programme_schedule_end_idx on barbera.vespa_comscore_skygo_programme_schedule_tmp(broadcast_end_date_time_utc)
    create LF index vespa_comscore_skygo_programme_schedule_servicekey_idx on barbera.vespa_comscore_skygo_programme_schedule_tmp(service_key)
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
               v.stream_id,     -- added [2016-07-29] primarily for reconcilliation work
               v.session_id,    -- added [2016-07-29] primarily for reconcilliation work
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
          from barbera.vespa_comscore_skygo_programme_schedule_tmp s,
               barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl cm,
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

    EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> '||@iterations||' required'

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
            EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Inserting linear instances '||@iteration||'(of'||@iterations||')'
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
    
    IF object_id('barbera.Comscore_skygo_mapping_summary_tmp') IS NOT NULL 
       BEGIN 
            drop table barbera.Comscore_skygo_mapping_summary_tmp commit 
       END 
 
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


    IF object_id('barbera.Comscore_skygo_mapping_aggregated_tmp') IS NOT NULL 
       BEGIN 
            drop table barbera.Comscore_skygo_mapping_aggregated_tmp commit 
       END 

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



    INSERT into barbera.VESPA_Comscore_SkyGo_linear_channel_mapping_audit_tbl
    select data_date_local,
           dateformat(now(), 'YYYY-MM-DD HH:mm:SS'), --run_datetime
           coalesce(station_name,'') as station_name,
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

    IF object_id('barbera.vespa_comscore_skygo_programme_schedule_tmp') IS NOT NULL 
       BEGIN drop table barbera.vespa_comscore_skygo_programme_schedule_tmp commit END 

    IF object_id('barbera.VESPA_Comscore_linear_base_tmp') IS NOT NULL 
       BEGIN drop table barbera.VESPA_Comscore_linear_base_tmp commit END 

    IF object_id('barbera.Comscore_lin_account_list_tmp') IS NOT NULL 
       BEGIN drop table barbera.Comscore_lin_account_list_tmp commit END 

    IF object_id('barbera.Comscore_skygo_mapping_summary_tmp') IS NOT NULL 
       BEGIN drop table barbera.Comscore_skygo_mapping_summary_tmp commit END 

    IF object_id('barbera.Comscore_skygo_mapping_aggregated_tmp') IS NOT NULL 
       BEGIN drop table barbera.Comscore_skygo_mapping_aggregated_tmp commit END 

END;



--------------------------------------------------------------------------------
--#####################     END     ############################################
--##############################################################################
--##############################################################################
--##############################################################################


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
# Created between:   06/08/2014 - 30/10/2014
# Created by:        Alan Barber (ABA)
# Description:
#
# List of steps:
# --------------
#
# code_location_A01  First prepare the information about this load and identify if ok to load
# code_location_A02  Prepare Universe information
# ----------------------------------------------------------
# code_location_B01  Halt the build process and raise error if the raw data looks wrong
# ----------------------------------------------------------
# code_location_C01  Start the conversion of raw data into event aggregate.
#                    First - extract the local day's period keys from VESPA_CALENDAR
# code_location_C02  Extract the required fields from the day's data (from COMSCORE_UNION_VIEW)
#                    handling DQ error codes with conversions
#                    Add a unique ID to each row, which will also order events
# code_location_C03  Form the context of each event by creating the event transitions
#                    using functions lag() and lead()
#                    Identify certain events (including 'orphaned events'), content starts,
#                    and event series start with flags
# code_location_C04  Identify the context of the Orphaned events, and events that signify
#                    the end of content or event series. Also apply fixes to the assetID and
#                    accumulated playing time (ns_st_pt) if required
# code_location_C05  Apply midnight cross-over rules to extend viewing to/from midnight
#                    add local-time fields. Remove orphaned-end events
# code_location_D01  Build an aggregate table using new_event_series = 1, end_event_series = 1,
#                    continuing_content_from = 1, content_continuing_to = 1
#                    Incorporate additional lead/lag context/transistions for requiring fields
# code_location_D02  Compress the prepared start/end event rows into a single 'aggregated' row
#                    Apply rules for allocating the event's play duration
# code_location_E01  Insert the aggregated events into the monthly table. If the start
#                    of a new month, create a new table.
#                    1. Prepare executable/dynamic SQL statement
# code_location_E02  2. Execute the dynamic SQL statement
# code_location_F01  Update basic stats... links back with file/raw data stats
# code_location_G01  Create Channel Mapping Information
# code_location_G02  Make manual adjustments to channel mapping info
# code_location_H01  Split Linear Events into Programme Instances
# code_location_H02  Record stats around splitting linear events into Programme Instances
# code_location_I01  Scan the day's records and make corrections to missing or incorrect data
# code_location_J01  Refresh union view  -- VESPA_Comscore_SkyGo_Union_View
# code_location_K01  Log completion of build procs
# code_location_L01  drop tables no longer requiered
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - sk_prod.VESPA_CALENDAR
#     - VESPA_Comscore_SAV_summary_tbl
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 30/10/2014  ABA   Initial version
# 07/11/2014  ABA   Added @run_date parameter to 'vespa_comscore_skygo_event_view_prep' procedure call
# 13/11/2014  ABA   Added code in union_view build to re-rank available months so the view builds correctly
# 27/11/2014  ABA   Added row_id to the universe_tbl in preparation for splitting the extraction of viewing data
#                   for large quantities... but then commented out, as temp table memory issues
# 09/12/2014  ABA   Code section B01 now referencing 'data_date' field rather than it's previous name
# 09/12/2014  ABA   Added 'drop table #Comscore_skygo_day_stats_tmp' command to F01 & H02: following update statement
# 11/12/2014  ABA   now resets basic_view_created = 0 after failed run
# 11/12/2014  ABA   Section K01 to drop temporary tables at the end of the proc
# 22/12/2014  ABA   Added ability to run when data volume is low -  IF @status not in ('ok','low') then raise error
# 22/12/2014  ABA   Wrapped the remained of script inside BEGIN/END statement for "IF @status not in ('ok','low')" so not run if failure
# 07/01/2015  ABA   Added execution of a new proc to build universe information (replaces previous code)
# 07/01/2015  ABA   Added some indexes to monthly tables when they are first built
# 08/01/2015  ABA   Added indexes to VESPA_CALENDAR_section_tmp on utc columns
#                   Changed use of VESPA_CALENDAR to use VESPA_CALENDAR_section_tmp instead
#                   Call to EXEC 'vespa_comscore_skygo_channelmap_refresh' now includes: @data_run_date
# 13/01/2015  ABA   Set erroneous_data_flag = 1  when  view_continuing_flag = 1  but first event starts way after midnight
# 15/08/2016  ABA   Midnight x-over calculation: added rule to deal with occurences of client before, but server timestamps after midnight cut-off. No rounds to the correct hour (rather than an hour too early!)
#
###############################################################################*/



CREATE or REPLACE procedure barbera.vespa_comscore_skygo_event_view_create(
                IN @data_run_date                 DATE) AS
BEGIN



            /* code_location_C01 *************************************************************************
             *****                                                                                      **
             *****      Start the conversion of raw data into event aggregate.                          **
             *****      First - extract the local day's period keys from VESPA_CALENDAR                 **
             *****                                                                                      **
             *********************************************************************************************/

DECLARE @xsql               varchar(10000)
DECLARE @monthly_table_name varchar(300)


/*
 create or replace VIEW barbera.comscore_union_view as
        select * from comscore_201607
          union all
        select * from comscore_201608
          union all
        select * from sk_prod.comscore_201609  --X
          union all
        select * from comscore_201610
          union all
        select * from comscore_201611
          union all
        select * from comscore_201612
commit
*/

--declare @data_run_date       DATE
--set @data_run_date = '2016-06-01'

            IF object_id('barbera.VESPA_CALENDAR_section_tmp') IS NOT NULL  BEGIN drop table barbera.VESPA_CALENDAR_section_tmp END

            select cast(c.utc_day_date as date) utc_day_date,
                   cast(c.utc_time_hours as integer) utc_time_hours,
                   c.local_day_date, c.local_time_hours,
                   c.daylight_savings_flag
              into barbera.VESPA_CALENDAR_section_tmp -- create a tempory table just with the hours we are interested in for this load, so that the inner joins on these only
              FROM VESPA_CALENDAR c
             where local_day_date = dateformat(cast(@data_run_date as date), 'YYYY-MM-DD')
             order by c.utc_day_date, c.utc_time_hours

            --add indexes to VESPA_CALENDAR_section_tmp on utc columns
            create DATE index VESPA_CALENDAR_section_tmp_dayutc_idx on barbera.VESPA_CALENDAR_section_tmp(utc_day_date)
            create LF index VESPA_CALENDAR_section_tmp_timeutc_idx on barbera.VESPA_CALENDAR_section_tmp(utc_time_hours)
            commit



            /* code_location_C02 *************************************************************************
             *****                                                                                      **
             *****      extract the required fields from the day's data (from COMSCORE_UNION_VIEW)      **
             *****      handling DQ error codes with conversions                                        **
             *****      Add a unique ID to each row, which will also order events                       **
             *****                                                                                      **
             *********************************************************************************************/

            --Create table for the day. Extract by server-side local-time. Cut-down dataset to required fields
             --drop tmp table before we start if exists
            IF object_id('barbera.V239_comscore_event_tmp5') IS NOT NULL
                BEGIN
                    DROP TABLE barbera.V239_comscore_event_tmp5
                END

            EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Extracting raw Comscore data'

            select dense_rank() over(order by coalesce(cast(t.sam_profileid as varchar), t.ns_st_id), ns_ap_device, ns_ts_raw, ns_st_ec asc, ns_utc_raw, cb_row_id asc) uniqid,
                   t.cb_row_id,
                   ns_ap_ec,
                   case when ns_ap_pfm = 'unknown' then sg_vs_dt else ns_ap_pfm end as ns_ap_pfm,   --correction especially for playstaion where ns_ap_pfm is unknown
                   ns_ap_pfv,
                   ns_radio,
                   agent,
                   case when ns_st_ev like 'ad_%' then 1 else ns_st_ad end          as ns_st_ad,
                   ns_st_bp, ns_st_bt,
                   ns_st_ca,
                   case when ns_st_cl >= 0 then ns_st_cl else null end              as ns_st_cl,  --handling DQ error codes
                   ns_st_cn, ns_st_ec,
                   case when ns_st_el >= 0 then ns_st_el else null end              as ns_st_el,  --handling DQ error codes
                   ns_st_ep,
                   ns_st_ge, ns_st_hc, ns_st_id,
                   ns_vid, --added 2016-07-26 for reconciliation work with activity capture
                   ns_st_it, ns_st_li, ns_st_pa, ns_st_pb, ns_st_pc, ns_st_pe,
                   ns_st_pl, ns_st_pn,
                   ns_st_po,
                   lead(ns_st_po) over(partition by sam_profileid, ns_ap_device, ns_st_id, ns_st_ad order by ns_ts_raw, ns_st_ec)   as next_ns_st_po,
                   ns_st_pp,
                   case when ns_st_pr in ('preroll','midroll') then null else ns_st_pr end       as ns_st_pr, --cleansing programme name of preroll entry
                   ns_st_sp, ns_st_sq, ns_st_st, ns_st_tp, ns_st_ty,
                   case when ns_st_ci = 'unknown' then null else ns_st_ci end       as ns_st_ci, --cleanse unknowns from assetID
                   case when sg_vs_sc in ('vod','lin','dvod') then sg_vs_sc else 'unknown' end sg_vs_sc,
                   t.sam_profileid,
                   ns_ap_device,
                   ns_utc,
                   ns_utc_raw,
                   -- dateadd(hh, c.daylight_savings_flag, ns_utc) ns_server_local_time,
                   time_of_day, --no longer required as calculating datetime version above
                   ns_ts_raw,
                   ns_ts,
                   ns_st_ev,
                   case when ns_st_pt_raw >= 0 then ns_st_pt_raw else null end ns_st_pt_raw, --handling DQ error codes
                   case when ns_st_pt >= 0     then ns_st_pt     else null end ns_st_pt,  --handling DQ error codes
                   case when ns_st_ev like '%play' and ns_st_ad = 1 then (next_ns_st_po - ns_st_po) else null end               as ad_play_duration_raw --for correcting ns_st_pt during ad events
              into barbera.V239_comscore_event_tmp5
              from barbera.COMSCORE_UNION_VIEW t
/*                     INNER JOIN barbera.VESPA_CALENDAR_section_tmp c -- using just with the hours we are interested in for this load, so that the inner joins is efficient
                    on cast(dateformat(t.ns_utc,'YYYY-MM-DD') as date) = c.utc_day_date
                   and cast(dateformat(t.ns_utc,'hh') as Integer)      = c.utc_time_hours
*/             --where t.sam_profileid is not null --later we need to include these, but the data is not good enough yet
                 where t.cb_source_file = 'Comscore_'||dateformat(cast(@data_run_date as date), 'YYYYMMDD')||'.gz'
            commit



            /*******************************************************************
             * update station name (ns_st_st) filling in 'unknown' station names
             * using identified (sk) code and Channel Mapping data
             *******************************************************************/

            create unique index sk_list_tmp_row_idx on barbera.V239_comscore_event_tmp5(cb_row_id)

            IF object_id('barbera.sk_list_tmp') IS NOT NULL
                BEGIN
                    DROP TABLE barbera.sk_list_tmp commit
                END

            select cb_row_id, cast(ns_st_ci as INTEGER) as service_key, ns_st_st, ns_st_pl
              into sk_list_tmp
              from barbera.COMSCORE_UNION_VIEW c
             where c.cb_source_file = 'Comscore_'||dateformat(cast(@data_run_date as date), 'YYYYMMDD')||'.gz'
               and ns_st_st = 'unknown'
               and ns_st_cu like '%/'||coalesce(ns_st_ci,'____')||'/%'   --confirm if the service key is in the CDN resource descriptor
            commit

            create unique index sk_list_tmp_row_idx on sk_list_tmp(cb_row_id)
            create HG index sk_list_tmp_sk_idx on sk_list_tmp(service_key)    


            IF object_id('barbera.sk_list_vespa_name_tmp') IS NOT NULL
                BEGIN
                    DROP TABLE barbera.sk_list_vespa_name_tmp commit
                END 
            
            select sk.*, cm.vespa_name
              into sk_list_vespa_name_tmp
              from sk_list_tmp sk
                     left join
                   VESPA_ANALYSTS.channel_map_prod_service_key_attributes cm
                     on sk.service_key = cm.service_key
             where cast(@data_run_date as date) between effective_from and effective_to
            commit

            create unique index sk_list_vespa_name_tmp_row_idx on sk_list_vespa_name_tmp(cb_row_id)


            --make updates
            UPDATE barbera.V239_comscore_event_tmp5 x
               SET ns_st_st = coalesce(sk.vespa_name, 'unknown')
              from sk_list_vespa_name_tmp sk
             where x.cb_row_id = sk.cb_row_id
            commit

            -- try an additional update where the playlist title used for a linear stream is the channel name
            UPDATE barbera.V239_comscore_event_tmp5 x
               SET x.ns_st_st = sk.ns_st_pl
              from sk_list_vespa_name_tmp sk
             where x.cb_row_id = sk.cb_row_id
               and lower(sk.ns_st_pl) = lower(sk.vespa_name)
               and coalesce(sk.ns_st_st, 'unknown') = 'unknown'
               and x.ns_st_st = 'unknown'
            commit

            drop table sk_list_tmp              commit
            drop table sk_list_vespa_name_tmp   commit



            /*******************************************************************
             * Continue preparing data
             *
             *******************************************************************/
            IF object_id('barbera.V239_comscore_event_tmp4') IS NOT NULL
                BEGIN
                    DROP TABLE barbera.V239_comscore_event_tmp4
                END

            --correct ns_st_pt for ad play events
            --add the server local-time
            select uniqid, cb_row_id,
                   ns_ap_ec, ns_ap_pfm, ns_ap_pfv,
                   ns_radio, agent, ns_st_ad, ns_st_bp, ns_st_bt, ns_st_ca,
                   ns_st_cl, ns_st_cn, ns_st_ec, ns_st_el, ns_st_ep,
                   ns_st_ge, ns_st_hc, ns_st_id,
                   ns_vid, --added 2016-07-26 for reconciliation work with activity capture
                   ns_st_it, ns_st_li, ns_st_pa, ns_st_pb, ns_st_pc, ns_st_pe,
                   ns_st_pl, ns_st_pn, ns_st_po, ns_st_pp, ns_st_pr,
                   ns_st_sp, ns_st_sq, ns_st_st, ns_st_tp, ns_st_ty,
                   ns_st_ci, sg_vs_sc,
                   sam_profileid,
                   ns_ap_device, ns_utc, ns_utc_raw, time_of_day,
                   ns_ts_raw, ns_ts,
                   case when ns_st_ev like 'ad_%' then substring(ns_st_ev, 4) else ns_st_ev end as ns_st_ev,  -- transformation for stripping out ad_
                   ns_st_pt_raw, 
                   --ns_st_pt,
                   case when ns_st_ad = 1 then
                                sum(ad_play_duration_raw/1000) over(partition by sam_profileid, ns_ap_device, ns_st_id, ns_st_ad order by ns_ts_raw, ns_st_ec 
                                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                        else ns_st_pt
                    end as ns_st_pt,
                   dateadd(hh, c.daylight_savings_flag, a.ns_utc) ns_server_local_time
              into barbera.V239_comscore_event_tmp4
              from barbera.V239_comscore_event_tmp5 a
        INNER JOIN barbera.VESPA_CALENDAR_section_tmp c -- using just with the hours we are interested in for this load, so that the inner joins is efficient
                on cast(dateformat(a.ns_utc,'YYYY-MM-DD') as date) = c.utc_day_date
               and cast(dateformat(a.ns_utc,'hh') as Integer)      = c.utc_time_hours
            commit



            /* code_location_C03 *************************************************************************
             *****                                                                                      **
             *****      Form the context of each event by creating the event transitions                **
             *****      using functions lag() and lead()                                                **
             *****      Identify certain events (including 'orphaned events'), content starts,          **
             *****      and event series start with flags                                               **
             *****                                                                                      **
             *********************************************************************************************/

            --drop tmp table before we start if exists
            IF object_id('barbera.V239_comscore_event_tmp3') IS NOT NULL
                BEGIN
                    DROP TABLE barbera.V239_comscore_event_tmp3
                END

            EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Applying universe restriction, and preparing view'

            -- create the low-level event view [this had a temp table memory issue so added an index to the universe, may cause problems in the future if greater volume of data is received]
            select uniqid,
                   t.cb_row_id,
                   s.aggregate_flag,
                   ns_ap_ec, ns_ap_pfm, ns_ap_pfv,
                   lag(ns_radio) over(partition by t.sam_profileid, ns_ap_device  order by uniqid) prev_ns_radio,
                   ns_radio,
                   agent,
                   lag(ns_st_ad) over(partition by t.sam_profileid, ns_ap_device  order by uniqid) prev_ns_st_ad, --account, device, ip (for location if more than one person using the same account)?
                   ns_st_ad,
                   lead(ns_st_ad) over(partition by t.sam_profileid, ns_ap_device  order by uniqid) next_ns_st_ad,
                   ns_st_bp, 

ns_st_bt, --add in lead/lag on clip accum buffering time here

                   ns_st_ca, ns_st_cl, ns_st_cn, ns_st_ec,
                   ns_st_el, ns_st_ep,
                   ns_st_ge, ns_st_hc, ns_st_id,
                   ns_vid, --added 2016-07-26 for reconciliation work with activity capture
                   ns_st_it, ns_st_li, ns_st_pa, ns_st_pb, ns_st_pc, ns_st_pe,
                   ns_st_pl, ns_st_pn, ns_st_po, ns_st_pp, ns_st_pr,
                   ns_st_sp, ns_st_sq,
                   lag(ns_st_st) over(partition by t.sam_profileid, ns_ap_device  order by uniqid) prev_ns_st_st, --added 2015-11-04
                   ns_st_st,
                   ns_st_tp, ns_st_ty,
                   lag(ns_st_ci) over(partition by t.sam_profileid, ns_ap_device  order by uniqid) prev_ns_st_ci, --account, device, ip (for location if more than one person using the same account)?
                   ns_st_ci,
                   lead(ns_st_ci) over(partition by t.sam_profileid, ns_ap_device  order by uniqid) next_ns_st_ci,
                   time_of_day, -- don't need this as using the derived 'ns_server_local_time' below
                   sg_vs_sc,
                   --not using the aggregate flag
                   --case when s.aggregate_flag = 1 then null else s.account_number end as account_number,
                   --case when s.aggregate_flag = 1 then null else s.cb_key_household end as cb_key_household,
                   s.account_number,
                   s.cb_key_household,
                   t.sam_profileid,
                   ns_ap_device,
                   ns_utc,
                   ns_utc_raw,
                   ns_server_local_time,
                   lag(ns_ts_raw) over(partition by t.sam_profileid, ns_ap_device  order by uniqid) prev_ns_ts_raw,
                   ns_ts_raw,
                   lag(ns_ts) over(partition by t.sam_profileid, ns_ap_device  order by uniqid) prev_ns_ts,
                   ns_ts,
                   lead(ns_ts) over(partition by t.sam_profileid, ns_ap_device  order by uniqid) next_ns_ts,
                   lag(ns_st_ev) over(partition by t.sam_profileid, ns_ap_device  order by uniqid) prev_ns_st_ev,
                   ns_st_ev,
                   lead(ns_st_ev) over(partition by t.sam_profileid, ns_ap_device  order by uniqid) next_ns_st_ev,
                   ns_st_pt_raw,
                   lag(ns_st_pt) over(partition by t.sam_profileid, ns_ap_device order by uniqid) prev_ns_st_pt,
                   case when t.ns_st_pt = 0 AND t.ns_st_pt_raw between 501 and 999 then 1               --bring back short events > half-second, < second [round to a second]
                        else t.ns_st_pt
                    end ns_st_pt,
                   lead(ns_st_pt) over(partition by t.sam_profileid, ns_ap_device order by uniqid) next_ns_st_pt,
                   floor((ns_ts_raw - prev_ns_ts_raw)/1000) event_secs,

                    --new content/channel
                   case when prev_ns_st_ev is null and ns_st_ev = 'keep-alive' and datediff(ss,'00:00:00', dateformat(ns_server_local_time, 'HH:MM:SS')) /*20mins*/ < 1200 then 0
                        when prev_ns_st_ev is null and ns_st_ev = 'end'  then 0
                        when prev_ns_st_ev = 'play' AND ns_st_ev = 'end' then 0
                        when coalesce(prev_ns_st_ad, -1) != coalesce(ns_st_ad, -1) then 1 --moved this to before linear condition (2015-11-04)

                        when sg_vs_sc = 'lin' AND (ns_st_st = 'unknown' OR prev_ns_st_st = 'unknown') then 0 --treat as we dont know whats happening, continue blindly  <--added 2015-11-06

                        when sg_vs_sc = 'lin' AND prev_ns_st_st != ns_st_st then 1 --basically a new channel is being viewed  <-- added 2015-11-04

                        when sg_vs_sc != 'lin' AND (ns_st_ci = 'unknown' OR prev_ns_st_ci = 'unknown') then 0 --treat as we dont know whats happening, continue blindly  <--added 2015-11-06

                        when sg_vs_sc != 'lin' AND prev_ns_st_ci != ns_st_ci then 1 --general condition for VOD/DVOD content changing
                        else 0 end new_content, -- this doesn't work too well when we want to take the max(accumulated_playtime) later in the code

                    --continuing content/channel
                   case when prev_ns_st_ev is null and ns_st_ev = 'keep-alive' and datediff(ss,'00:00:00', dateformat(ns_server_local_time, 'HH:MM:SS')) /*20mins*/ < 1200 then 1
                        when prev_ns_st_ev is null and ns_st_ev = 'keep-alive' and prev_ns_st_ci = ns_st_ci then 1
                        when prev_ns_st_ev is null and ns_st_ev = 'end' then 1
                        else 0 end continuing_content_from,

                    --content_continuing_to
                   case when ns_st_ev in ('play','keep-alive') and next_ns_st_ev is null and datediff(ss,'00:00:00', dateformat(ns_server_local_time, 'HH:MM:SS')) /*days_secs*/ >= 85200 then 1
                        else 0 end content_continuing_to,

                    --new event series
                   case when coalesce(prev_ns_st_ev, 'end') = 'end' AND ns_st_ev = 'play' then 1
                        when sg_vs_sc != 'lin' AND prev_ns_st_ev = 'play' AND ns_st_ev = 'play' AND datediff(ss, prev_ns_ts, ns_ts) > 20*60  then 1 -- and diff>20mins
                        when new_content = 1 then 1
                        --when prev_ns_radio not in ('none','unknown') AND ns_radio not in ('none','unknown') AND prev_ns_radio != ns_radio then 1   --connection_type has changed, so we need to split the event - NOT POSSIBLE AT THE MOMENT using ns_st_pt                      
                        --additonal rules for linear                            
                        when sg_vs_sc = 'lin' AND prev_ns_st_ev = 'pause'                   AND ns_st_ev = 'play'         then 1
                        when sg_vs_sc = 'lin' AND coalesce(prev_ns_st_ev, 'end') = 'end'    AND ns_st_ev = 'keep-alive'   then 1  
                        when sg_vs_sc = 'lin' AND coalesce(prev_ns_st_ev, 'play') in ('play', 'keep-alive') AND ns_st_ev = 'keep-alive' 
                                              AND datediff(ss, prev_ns_ts, ns_ts) > 20*60+1                               then 1

   --[don't think this is right, not setting up to catch "play -> pause -> end", but "pause -> play -> end"]
   --removing rule [2015-11-06]          when sg_vs_sc = 'lin' AND prev_ns_st_ev = 'pause' AND ns_st_ev in ('play','end') then 1 --having 'end' in this list forces 'pause','end','null' sequences to be a single row event starting and ending with the one entry, these are then removed (as end is a none event)

                        else 0
                    end new_event_series, --this is better at recognising events

                   --mimick the rule for new_event_series that looks at the secs difference between events. Flag if greater than 20mins+1sec
                   case when sg_vs_sc = 'lin' AND coalesce(prev_ns_st_ev, 'play') in ('play', 'keep-alive') AND ns_st_ev = 'keep-alive' 
                                              AND datediff(ss, prev_ns_ts, ns_ts) > 20*60+1                               then 1
                        else 0 
                    end as keep_alive_cap_flag, -- [added 2017-03-24] 
                    --play event
                   case when ns_st_ev = 'play'  then 1
                        when ns_st_ev = 'keep-alive'  then 1
                        else 0 end play_event,
                    --keep-alive event
                   case when ns_st_ev = 'keep-alive'  then 1
                        else 0 end keep_alive_event,
                    --play duration
                   case when prev_ns_st_ev = 'play' and event_secs > 0 then event_secs
                        when prev_ns_st_ev = 'keep-alive' and event_secs > 0 then event_secs
                        else 0 end play_duration,
                    --pause event
                   case when ns_st_ev = 'pause'  then 1
                        else 0 end pause_event,
                    --pause duration
                   case when prev_ns_st_ev = 'pause' and ns_st_ev = 'end' then 1
                        when prev_ns_st_ev = 'pause' and ns_st_ev = 'play' then 1
                        else 0
                    end pause_duration,
                    --end event
                   case when prev_ns_st_ev = 'end' and ns_st_ev != 'play' then 1
                        when prev_ns_st_ev = 'pause' and ns_st_ev = 'end' then 1
                        when ns_st_ev = 'end' then 1
                        --when pause_ev = 0 and play_ev = 0 and prev_ns_st_ev is not null then 1
                        else 0
                    end end_ev,
                   case when prev_ns_st_ev = 'pause' AND ns_st_ev = 'end' AND content_continuing_to != 1 then 1
                        when prev_ns_st_ev = 'end' AND ns_st_ev = 'pause' AND content_continuing_to != 1 then 1
                        else 0
                    end orphaned_end,     --  'pause' -> 'end' events are considered unnecessary, so marked as 'orphaned'
                     --lag_new_content_group
                    row_number() over(order by t.sam_profileid, ns_ap_device, ns_st_ci, ns_ts, ns_st_ec) row,                    --not required
                    row_number() over(partition by t.sam_profileid, ns_ap_device, ns_st_ci order by ns_ts, ns_st_ec) group_row,  --not required
                    count(1) over(partition by t.sam_profileid, ns_ap_device, ns_st_ci) event_count   --replaced later in preference for the difference in unique IDs
               into barbera.V239_comscore_event_tmp3
               from barbera.V239_comscore_event_tmp4 t
                     INNER JOIN barbera.VESPA_Comscore_SkyGo_universe_tbl s -- the universe table (limit using row_id in batches for large volumes of data)
                     --LEFT JOIN barbera.VESPA_Comscore_SkyGo_universe_tbl s
                     on t.sam_profileid = s.sam_profileid
              where s.exclude_flag != 1
                    --coalesce(s.exclude_flag, 0) != 1   --this makes the query way too slow, so we will only use known accounts
              order by t.sam_profileid, ns_ap_device, ns_ts,/* ns_st_ci,*/ ns_st_ec asc

             commit



        /* code_location_C04 *************************************************************************
         *****      Identify the context of the Orphaned events, and events that signify            **
         *****      the end of content or event series. Also apply fixes to the assetID and         **
         *****      accumulated playing time (ns_st_pt) if required                                 **
         *********************************************************************************************/

        IF object_id('barbera.V239_comscore_event_tmp2') IS NOT NULL
            BEGIN
                drop table barbera.V239_comscore_event_tmp2
            END

        SELECT uniqid, cb_row_id,
               aggregate_flag,
               ns_ap_ec, ns_ap_pfm, ns_ap_pfv,
               ns_radio, agent,
               prev_ns_st_ad,
               case when prev_ns_st_ci = ns_st_ci and prev_ns_st_ad != ns_st_ad then prev_ns_st_ad else ns_st_ad end ns_st_ad, --corrects for cases where the ad_flag is not set
               next_ns_st_ad,
               ns_st_bp, --playlist buffering

ns_st_bt,  --clip buffering
--if prev_bt < bt AND prev_ev = 'pause' then bt-prev_bt else 0 end buffering  --this buffering should be added to a live offset duration

ns_st_ca, ns_st_cl, ns_st_cn, ns_st_ec,
               ns_st_el, ns_st_ep, ns_st_ge, ns_st_hc, ns_st_id,
               ns_vid, --added 2016-07-26 for reconciliation work with activity capture
               ns_st_it,
               ns_st_li, ns_st_pa, ns_st_pb, ns_st_pc, ns_st_pe, ns_st_pl,
               ns_st_pn, ns_st_po, ns_st_pp, ns_st_pr, ns_st_sp, ns_st_sq,
               ns_st_st, ns_st_tp, ns_st_ty, prev_ns_st_ci,
               --ns_st_ci,
               next_ns_st_ci,
               time_of_day, --   <-- not required anymore
               ns_server_local_time,
               sg_vs_sc, account_number, cb_key_household, sam_profileid,
               ns_ap_device,
               ns_utc, ns_utc_raw,
               prev_ns_ts, ns_ts_raw, ns_ts, next_ns_ts,
               prev_ns_st_ev, ns_st_ev, next_ns_st_ev,
               ns_st_pt_raw, prev_ns_st_pt, ns_st_pt,
               next_ns_st_pt,
               event_secs,
               --lag(orphaned_end) over(partition by sam_profileid, ns_ap_device  order by ns_ts, ns_st_ec) prev_orphaned_end,
               lag(orphaned_end) over(partition by sam_profileid, ns_ap_device  order by uniqid) prev_orphaned_end,
               orphaned_end,
               --lead(orphaned_end) over(partition by sam_profileid, ns_ap_device  order by ns_ts, ns_st_ec) next_orphaned_end,
               lead(orphaned_end) over(partition by sam_profileid, ns_ap_device  order by uniqid) next_orphaned_end,
               new_content, continuing_content_from, content_continuing_to,
               case when coalesce(prev_ns_st_ev, 'end') = 'end' 
                         and ns_st_ev = 'keep-alive'   -- [added 2017-02-24] (basically if the event starts with a keep-alive, start a new event series)
                         and datediff(ss,'00:00:00', dateformat(ns_server_local_time, 'HH:MM:SS')) /*20mins*/ > 1201 -- [added 2017-03-01] (trigger new series if NOT continuing from previous day)
                         then 1
                    when prev_orphaned_end = 1 then 1 --this is so when an orphaned end event is removed, we treat the following row as a new event series
                    else e.new_event_series
                end new_event_series,
               new_event_series as old_new_event_series,
               keep_alive_cap_flag,
               play_event, keep_alive_event, play_duration, pause_event, pause_duration,
               end_ev,
               row, group_row, event_count,
               --e.new_content,
               case --when ns_st_pt = 0 AND prev_ns_st_ev = 'play' AND ns_st_ev = 'play' then
                    when ns_st_pt = 0 then prev_ns_st_pt
                    else ns_st_pt
                end fake_ns_st_pt,--used in cases where it's the last event for the account on the day (pulls forward the accumulated play time)
               --replace asset_id when it's just changed but is an end event
               case when e.prev_ns_st_ci != e.ns_st_ci AND e.ns_st_ev = 'end' /*and e.next_ns_st_ev is null*/ then e.prev_ns_st_ci
                    else e.ns_st_ci
                end ns_st_ci,
               case when ns_st_ev = 'end' and next_ns_st_ev is null then 1
                    when ns_st_ev = 'pause' AND next_ns_st_ev is null then 1
                    when ns_st_ev = 'play' AND next_ns_st_ev = 'end' AND content_continuing_to = 0 AND coalesce(ns_st_ci, 'un') = coalesce(next_ns_st_ci, 'un') then 0    --as this event is not the end event
                    when content_continuing_to = 1 then 0
                    else lead(e.new_content) over(partition by sam_profileid, ns_ap_device order by uniqid)
                end end_content,
               case when prev_ns_st_ev is null AND ns_st_ev = 'end' AND continuing_content_from = 1 then 1
                    when ns_st_ev = 'end' AND next_ns_st_ev is null AND content_continuing_to != 1 then 1
                    when ns_st_ev = 'pause' AND coalesce(next_ns_st_ev, 'end') = 'end' AND content_continuing_to != 1 then 1
                    when ns_st_ev = 'play' AND next_ns_st_ev is null AND content_continuing_to != 1 then 1
                    when ns_st_ev = 'play' AND next_ns_st_ev = 'end' AND content_continuing_to != 1 then 0    --as next event is the end event, and not this one
                    when content_continuing_to = 1 then 0
                    --#####
                    --need to expand this for linear viewing... to split up events...
                    when sg_vs_sc='lin' AND prev_ns_st_ev = 'play' AND ns_st_ev in ('pause','end') then 1 --[added 2016-12-21]  pause added [2017-03-07]
                    when sg_vs_sc='lin' AND prev_ns_st_ev = 'keep-alive' AND ns_st_ev in ('pause','end') then 1 --[added 2017-03-02]    pause added [2017-03-07] 
                    --when sg_vs_sc='lin' AND ns_st_ev  = 'keep-alive' AND coalesce(next_ns_st_ev, 'end') = 'end' then 1 --[added 2017-03-10] <-- NOT CORRECT
                    -- sequence: end  -->  keep-alive  -->  play   (is not being captured - but not valid anyway)  if keepalive play transition is greater than 20mins then we could 
                    --                                              make it an orphan and strip out, just incase it makes it through
                    when next_orphaned_end = 1 then 1 --- required as the next step removes orphaned events leaving end_event_series as 0 (no ending for the event)
                    when keep_alive_cap_flag = 1 then 1 -- the event is being capped so should act as the end of an event series
                    else lead(e.new_event_series) over(partition by sam_profileid, ns_ap_device order by uniqid) --this doesnt always get picked up for some reason

                end end_event_series
               into barbera.V239_comscore_event_tmp2
           from barbera.V239_comscore_event_tmp3 e
          order by uniqid
        --drop table V239_comscore_event_tmp2
        commit



        /* code_location_C05 *************************************************************************
         *****    Apply midnight cross-over rules to extend viewing to/from midnight                **
         *****    add local-time fields. Remove orphaned-end events                                 **
         *****                                                                                      **
         *********************************************************************************************/

        IF object_id('barbera.V239_comscore_event_tmp') IS NOT NULL
            BEGIN
                drop table barbera.V239_comscore_event_tmp
            END

        SELECT e.uniqid, e.cb_row_id, aggregate_flag,
             ns_ap_ec, ns_ap_pfm, ns_ap_pfv,
             case when viewing_start_client_utc is null then null
                  else ns_radio
              end ns_radio_start,
             case when viewing_end_client_utc is null then null
                  else ns_radio
              end ns_radio_end,
             agent, prev_ns_st_ad, ns_st_ad, next_ns_st_ad, ns_st_bp, ns_st_bt,
             ns_st_ca, ns_st_cl, ns_st_cn,
             ns_st_ec,
             ns_st_el, ns_st_ep, ns_st_ge, ns_st_hc, ns_st_id, 
             ns_vid, --added 2016-07-26 for reconciliation work with activity capture
             ns_st_it, ns_st_li,
             ns_st_pa, ns_st_pb, ns_st_pc, ns_st_pe, ns_st_pl, ns_st_pn, ns_st_po, ns_st_pp, ns_st_pr, ns_st_sp, ns_st_sq,
             ns_st_st, ns_st_tp, ns_st_ty,
             prev_ns_st_ci, next_ns_st_ci,
             sg_vs_sc,
             account_number, cb_key_household, sam_profileid, ns_ap_device,            
             --ns_ts is based on utc at the client. This means that we need to adjust the cut-off according to daylight savings (summer is -1hr from midnight, winter is midnight)
             case when new_event_series = 1 AND end_event_series = 1 then prev_ns_ts  --if there is only one row of data explaining the event then we need to use the previous event timestamp, otherwise event will have 0 duration
                  when new_event_series = 1 then ns_ts
                  when continuing_content_from = 1 AND DATEFLOOR(hh, ns_ts) = DATEFLOOR(hh, ns_utc)
                            then DATEFLOOR(hh, ns_ts) -- <---- test this, not sure it works  [added line below 2016-08-15 to deal with client ts before, but server after cut-off occurences]
                  when continuing_content_from = 1 AND dateadd(hh, 1, DATEFLOOR(hh, ns_ts)) = DATEFLOOR(hh, ns_utc)
                            then dateadd(hh, 1, DATEFLOOR(hh, ns_ts))   -- <--- this could lead to negative durations which is what we want as we should subtract from previous day
                  else null
              end viewing_start_client_utc,
             case when content_continuing_to = 1 then DATECEILING(hh, ns_ts)
                  --next rule used for capping  
                  when end_event_series = 1 AND keep_alive_cap_flag = 1 then /*cap*/ dateadd(ss, 20*60, prev_ns_ts) --used to cap keep-alive events to 20mins
                  when end_event_series = 1 then ns_ts
                  else null
              end viewing_end_client_utc,
             dateadd(hh, c_ts.daylight_savings_flag, viewing_start_client_utc) viewing_start_client_local,
             dateadd(hh, c_ts.daylight_savings_flag, viewing_end_client_utc)  viewing_end_client_local,
             case when viewing_start_client_utc is null then null
                  else c_ts.daylight_savings_flag
              end daylight_savings_flag_start,
             case when viewing_end_client_utc is null then null
                  else c_ts.daylight_savings_flag
              end daylight_savings_flag_end,
             prev_ns_ts,
             ns_ts,
             next_ns_ts,
             ns_ts_raw,
             ns_ts_raw - truncnum(ns_ts_raw,-3) client_millis,                  --may not need this
             ns_utc_raw - truncnum(ns_utc_raw,-3) server_millis,                --may not need this
             datediff(ms, viewing_start_client_utc, ns_ts) start_day_ts_diff,   --may not need this
             datediff(ms, ns_ts, viewing_end_client_utc) end_day_ts_diff,       --may not need this
             datediff(ms, viewing_start_client_utc, ns_utc) start_day_utc_diff, --may not need this
             datediff(ms, ns_utc, viewing_end_client_utc) end_day_utc_diff,     --may not need this
             case when viewing_start_client_utc is null then null
                  --when start_day_ts_diff > 0 then ns_ts_raw - start_day_ts_diff - client_millis
                  when continuing_content_from = 1 OR keep_alive_cap_flag = 1 then datediff(ms, '1970-01-01 00:00:00.000000', viewing_start_client_utc) --ok to not use milliseconds as rounding to hour
                  else ns_ts_raw
              end viewing_start_client_utc_raw,
             case when viewing_end_client_utc is null then null
                  --when end_day_ts_diff > 0 then ns_ts_raw + end_day_ts_diff - client_millis
                  when content_continuing_to = 1 OR keep_alive_cap_flag = 1 then datediff(ms, '1970-01-01 00:00:00.000000', viewing_end_client_utc) --ok to not use milliseconds as rounding to hour
                  else ns_ts_raw
              end viewing_end_client_utc_raw,
             -- UTC at Comscore server
             case when viewing_start_client_utc is null then null
             --adjust for continuing from
                  when continuing_content_from = 1 then DATEFLOOR(hh, ns_utc)
                  else ns_utc
              end server_start_utc,
             case when viewing_end_client_utc is null then null
                  when content_continuing_to = 1 then DATECEILING(hh, ns_utc)
                  else ns_utc
              end server_end_utc,
             --raw server_utc milliseconds
             case when viewing_start_client_utc is null then null
                  --when start_day_utc_diff > 0 then ns_utc_raw - start_day_utc_diff - server_millis                    -- [removed 2016-08-16]
                  when continuing_content_from = 1 then datediff(ms, '1970-01-01 00:00:00.000000', server_start_utc)    -- [added 2016-08-16:  reset to midnight/11pm for continuing events]
                  else ns_utc_raw
              end server_start_utc_raw,
             case when viewing_end_client_utc is null then null
                  --when end_day_utc_diff > 0 then ns_utc_raw + end_day_utc_diff - server_millis                    -- [removed 2016-08-16]
                  when content_continuing_to = 1 then datediff(ms, '1970-01-01 00:00:00.000000', server_end_utc)    -- [added 2016-08-16:  reset to midnight/11pm for continuing events]
                  else ns_utc_raw
              end server_end_utc_raw,
             --Comscore local-time [ based on time_of_day] (used to determine file cut-off point)
             case when viewing_start_client_utc is null then null
                  when continuing_content_from = 1 then
                            dateadd(hh, c_utc.daylight_savings_flag, server_start_utc)
                  else ns_server_local_time   -- better than time_of_day provided by Comscore
              end server_start_local_time,
             case when viewing_end_client_utc is null then null
                  when content_continuing_to = 1 then
                            dateadd(hh, c_utc.daylight_savings_flag, server_end_utc)
                  else ns_server_local_time   -- better than time_of_day provided by Comscore
              end server_end_local_time,
             prev_ns_st_ev, ns_st_ev, next_ns_st_ev,

             --adjust all the accumulated play times to midnight if continuing or continued event
             --might be best to do this in one of the next sections??
              prev_ns_st_pt,
              ns_st_pt, ns_st_pt_raw, --if continuing: pt+millis, if continued: remove time before midnight
              next_ns_st_pt,
             case when viewing_start_client_utc is not null then uniqid --used to use ns_st_ec but is very unreliable so better to use self-generated uniqid
                  else null
              end start_uniqid,
             case when viewing_end_client_utc is not null then uniqid
                  else null
              end end_uniqid,
             event_secs, --can be worked out more accurately using the raw milliseconds now....  done in next query
             new_content, continuing_content_from, content_continuing_to,
             -- need a column to flag erroneous data suspected
             case when lag(e.end_event_series) over(partition by sam_profileid, ns_ap_device order by uniqid) = 1 AND old_new_event_series = 0 then 1                  
                  -- we could also flag: case when prev_ns_st_ev is null and ns_st_ev = 'keep-alive' then 1    --when an event starts with keep-alive... seems ok for most cases though
                  -- this was added as a rule into identifying an event_start on 2017-02-24
                  else 0
              end erroneous_data_suspected_flag,             
             --fix any missing start events if the end has been recognised but not the next start
             --new_event_series,
             case when lag(e.end_event_series) over(partition by sam_profileid, ns_ap_device order by uniqid) = 1 AND new_event_series = 0 then 1
                  else new_event_series
              end new_event_series,
             keep_alive_cap_flag, 
             play_event, play_duration, pause_event, pause_duration, end_ev,
             row, group_row,
             event_count,
             fake_ns_st_pt,
             ns_st_ci,
             end_content,
             end_event_series             
        into barbera.V239_comscore_event_tmp
        from barbera.V239_comscore_event_tmp2 e,
             --barbera.VESPA_CALENDAR_section_tmp c_ts,   <-- commented out as restricted to local server day records and client-side timestamps can fall outside this range
             VESPA_CALENDAR c_ts,                         --  <-- [change applied 2015-11-09, now includes client-side events that fall outside local_data_date range]
             barbera.VESPA_CALENDAR_section_tmp c_utc
       where cast(ns_ts as date) = c_ts.utc_day_date
         and cast(dateformat(ns_ts,'hh') as Integer) = c_ts.utc_time_hours
         and cast(ns_utc as date) = c_utc.utc_day_date
         and cast(dateformat(ns_utc,'hh') as Integer) = c_utc.utc_time_hours
         and orphaned_end != 1
        commit



        /* code_location_C06 *************************************************************************
         *****                                                                                      **
         *****    1. Removal of events that are non-events                                          **
         *****                                                                                      **         
         *********************************************************************************************/

      --1st remove any event that is an end event (ns_st_ev = 'end') as these events are handled in context of the remaining events
        DELETE barbera.V239_comscore_event_tmp t
         where t.new_event_series = 1
           and t.end_event_series = 1
           and t.sg_vs_sc = 'lin'
           and coalesce(t.prev_ns_st_ev, 'end') = 'end'
        commit


--Q: What happens to longer events that end with a keep-alive that needs capping - this would not occure on one line?

   --make amendment here to single row events: make sure start and end are correct? capped etc.??  
    --note that we have not changed server times here. We need to swap from using server time to connect linear viewing to use CORRECTED client time






        /* code_location_D01 *************************************************************************
         *****                                                                                      **
         *****      Build an aggregate table using new_event_series = 1, end_event_series = 1,      **
         *****      continuing_content_from = 1, content_continuing_to = 1                          **
         *****      Incorporate additional lead/lag context/transistions for requiring fields       **
         *********************************************************************************************/

        IF object_id('barbera.V239_comscore_view2') IS NOT NULL
            BEGIN
                drop table barbera.V239_comscore_view2
            END

        select uniqid,
               account_number,
               cb_key_household,
               sam_profileid,
               aggregate_flag,
               ns_ap_device,
               ns_ap_pfm platform_name,
               ns_ap_pfv platform_version,
               ns_st_id, --added this 2016-01-07 so we can split linear channel events, as viewing of a new channel usually uses a different streamID
               ns_vid, --added 2016-07-26 for reconciliation work with activity capture
               sg_vs_sc stream_context,
               ns_st_st station_name,
               case when sg_vs_sc = 'lin' then ns_st_ci else null end channel_id,
               case when sg_vs_sc like '%vod' AND ns_st_ad = 0 then ns_st_ci else null end vod_asset_id,
               case when ns_st_ad = 1 then ns_st_ci else null end ad_asset_id,
               e.prev_ns_st_ev, e.ns_st_ev,
               e.continuing_content_from, e.content_continuing_to, e.end_content,
               erroneous_data_suspected_flag,
               new_event_series, end_event_series,
               keep_alive_cap_flag,
               e.next_ns_st_pt,
               e.fake_ns_st_pt,
               case when e.content_continuing_to = 1 and e.ns_st_pt = 0 then fake_ns_st_pt      --don't really need this as we work out the time to midnight anyway
                    else e.ns_st_pt
                end ns_st_pt,
               ns_ts_raw,
               ns_ts,
               daylight_savings_flag_start,
               daylight_savings_flag_end,
               viewing_start_client_utc_raw,
               viewing_end_client_utc_raw,
               viewing_start_client_utc,
               viewing_end_client_utc,
               viewing_start_client_local,
               viewing_end_client_local,
               server_start_utc,
               server_end_utc,
               server_start_utc_raw,
               server_end_utc_raw,
               server_start_local_time,
               server_end_local_time,
               ns_radio_start,
               ns_radio_end,
               --altered to use uniqid to order
               lag(daylight_savings_flag_start) over(partition by sam_profileid, ns_ap_device order by uniqid) prev_daylight_savings_flag_start,
               lead(daylight_savings_flag_end) over(partition by sam_profileid, ns_ap_device order by uniqid) next_daylight_savings_flag_end,
               lag(viewing_start_client_utc) over(partition by sam_profileid, ns_ap_device order by uniqid) prev_viewing_start_client_utc,
               lead(viewing_end_client_utc) over(partition by sam_profileid, ns_ap_device order by uniqid) next_viewing_end_client_utc,
               lag(viewing_start_client_utc_raw) over(partition by sam_profileid, ns_ap_device order by uniqid) prev_viewing_start_client_utc_raw,
               lead(viewing_end_client_utc_raw) over(partition by sam_profileid, ns_ap_device order by uniqid) next_viewing_end_client_utc_raw,
               lag(viewing_start_client_local) over(partition by sam_profileid, ns_ap_device order by uniqid) prev_viewing_start_client_local,
               lead(viewing_end_client_local) over(partition by sam_profileid, ns_ap_device order by uniqid) next_viewing_end_client_local,
               lag(server_start_utc) over(partition by sam_profileid, ns_ap_device order by uniqid) prev_server_start_utc,
               lead(server_end_utc) over(partition by sam_profileid, ns_ap_device order by uniqid) next_server_end_utc,
               lag(server_start_utc_raw) over(partition by sam_profileid, ns_ap_device order by uniqid) prev_server_start_utc_raw,
               lead(server_end_utc_raw) over(partition by sam_profileid, ns_ap_device order by uniqid) next_server_end_utc_raw,
               lag(server_start_local_time) over(partition by sam_profileid, ns_ap_device order by uniqid) prev_server_start_local_time,
               lead(server_end_local_time) over(partition by sam_profileid, ns_ap_device order by uniqid) next_server_end_local_time,
               lag(ns_radio_start) over(partition by sam_profileid, ns_ap_device order by uniqid) prev_ns_radio_start,
               lead(ns_radio_end) over(partition by sam_profileid, ns_ap_device order by uniqid) next_ns_radio_end,
               ns_st_ge genre,
               ns_st_ad ad_flag,
               null unique_duration,
               ns_st_ec, --event counter for playlist
               start_uniqid,
               end_uniqid,
               lag(start_uniqid) over(partition by sam_profileid, ns_ap_device order by uniqid) prev_uniqid_start,
               lead(end_uniqid) over(partition by sam_profileid, ns_ap_device order by uniqid) next_uniqid_end,
               case when sg_vs_sc = 'lin' then null              -- we don't know the linear content length yet
                    when coalesce(ns_st_el, 0) > 0  AND  e.ns_st_ev != 'end'  then ns_st_el -- episode length
                    when ns_st_cl > 0  AND  e.ns_st_ev != 'end'  then ns_st_cl              -- clip clength (there are often multiple clips in an episode, but it's the best 2nd choice)
                    else null
                end content_duration,                            -- episode more reliable for length of total clips (sometimes end events dont hold correct info
               --play_events,
               case when new_event_series = 1 or continuing_content_from = 1 and ns_st_pr is not null then ns_st_pr
                    when end_event_series = 1 or content_continuing_to = 1 and sg_vs_sc like '%vod' then ns_st_pr   --  <--- careful with this, just incase it introduces mis-reads of the data  2016-07-25
                    else null
                end programme_name, --this is required for linear where the start and end programme could be different
               case when new_event_series = 1 or content_continuing_to = 1 then 0
                    else ns_st_bt
                end buffering_duration
          into barbera.V239_comscore_view2
          from barbera.V239_comscore_event_tmp e
         where new_event_series = 1
            or end_event_series = 1
            or continuing_content_from = 1
            or content_continuing_to = 1
        order by sam_profileid,
                ns_ap_device,
                ns_ts,
                ns_st_ec
        commit



        /* code_location_D02 *************************************************************************
         *****                                                                                      **
         *****    Compress the prepared start/end event rows into a single 'aggregated' row         **
         *****    Apply rules for allocating the event's play duration                              **
         *********************************************************************************************/

        IF object_id('barbera.V239_comscore_view') IS NOT NULL
            BEGIN
                drop table barbera.V239_comscore_view
            END

        --create 'final' table view
        select e.uniqid,
               e.account_number,
               e.cb_key_household,
               e.sam_profileid,
               e.aggregate_flag,
               e.ns_ap_device,
               e.platform_name,
               e.platform_version,
               e.stream_context,

               --e.station_name, [fix 'unknown' station name  <-- applied 2015-11-06]  further adjustment made partitioning by ns_st_id applied 2016-01-07
               lag(e.station_name) over(partition by sam_profileid, ns_ap_device, ns_st_id order by uniqid) first_station_name,
               lead(e.station_name) over(partition by sam_profileid, ns_ap_device, ns_st_id order by uniqid) second_station_name,
               case when e.station_name = 'unknown' AND e.station_name != first_station_name then first_station_name
                    when e.station_name = 'unknown' AND e.station_name != second_station_name then second_station_name
                    else e.station_name
                end station_name,

               --e.channel_id, [fix 'unknown' channel_id   <-- applied 2015-11-06]  further adjustment made partitioning by ns_st_id applied 2016-01-07
               lag(e.channel_id) over(partition by sam_profileid, ns_ap_device, ns_st_id order by uniqid) first_channel_id,
               lead(e.channel_id) over(partition by sam_profileid, ns_ap_device, ns_st_id order by uniqid) second_channel_id,  -- not being split properly - reading lines from entries we shouldn't be
               case when e.channel_id = 'unknown' AND e.channel_id != first_channel_id then first_channel_id
                    when e.channel_id = 'unknown' AND e.channel_id != second_channel_id then second_channel_id
                    else e.channel_id
                end channel_id,

               e.vod_asset_id, e.ad_asset_id,
               e.ns_st_id,
               e.ns_vid,
               e.prev_ns_st_ev, e.ns_st_ev,
               e.continuing_content_from, e.content_continuing_to,
               e.erroneous_data_suspected_flag,
               e.keep_alive_cap_flag,
               e.new_event_series, e.end_content, e.next_ns_st_pt, e.ns_st_pt,
               coalesce(e.viewing_start_client_utc, e.prev_viewing_start_client_utc) viewing_event_start_utc,
               coalesce(e.viewing_end_client_utc, e.next_viewing_end_client_utc) viewing_event_end_utc,
               coalesce(e.viewing_start_client_utc_raw, e.prev_viewing_start_client_utc_raw) viewing_event_start_client_utc_raw,
               coalesce(e.viewing_end_client_utc_raw, e.next_viewing_end_client_utc_raw) viewing_event_end_client_utc_raw,
               coalesce(e.viewing_start_client_local, e.prev_viewing_start_client_local) viewing_event_start_client_local,
               coalesce(e.viewing_end_client_local, e.next_viewing_end_client_local) viewing_event_end_client_local,
               coalesce(e.daylight_savings_flag_start, e.prev_daylight_savings_flag_start) daylight_savings_flag_start,
               coalesce(e.daylight_savings_flag_end, e.next_daylight_savings_flag_end) daylight_savings_flag_end,
               coalesce(e.server_start_utc, e.prev_server_start_utc) server_start_utc,                          --  Comscore utc
               coalesce(e.server_end_utc, e.next_server_end_utc) server_end_utc,                                --  Comscore utc
               coalesce(e.server_start_utc_raw, e.prev_server_start_utc_raw) server_start_utc_raw,              --  Comscore utc RAW (milliseconds)
               coalesce(e.server_end_utc_raw, e.next_server_end_utc_raw) server_end_utc_raw,                    --  Comscore utc RAW (milliseconds)
               coalesce(e.server_start_local_time, e.prev_server_start_local_time) server_start_local_time,     --  Comscore local-time
               coalesce(e.server_end_local_time, e.next_server_end_local_time) server_end_local_time,           --  Comscore local-time
               coalesce(e.ns_radio_start, e.prev_ns_radio_start) connection_type_start,                         --  connection_type
               coalesce(e.ns_radio_end, e.next_ns_radio_end) connection_type_end,                               --  connection_type
               coalesce(e.start_uniqid, e.prev_uniqid_start) uniqid_start,                                --  event counter using uniqid
               coalesce(e.end_uniqid, e.next_uniqid_end) uniqid_end,                                      --  event counter using uniqid

               --e.genre, [fix 'unknown' channel_id   <-- applied 2015-11-06]  further adjustment made partitioning by ns_st_id applied 2016-01-07
               lag(e.genre) over(partition by sam_profileid, ns_ap_device, ns_st_id order by uniqid) first_genre,
               lead(e.genre) over(partition by sam_profileid, ns_ap_device, ns_st_id order by uniqid) second_genre,
               case when e.genre = 'unknown' AND e.genre != first_genre then first_genre
                    when e.genre = 'unknown' AND e.genre != second_genre then second_genre
                    else e.genre
                end genre,

               ad_flag,
               unique_duration,
               datediff(ss, viewing_event_start_utc, viewing_event_end_utc) event_duration,                           --  better to convert this to use the raw start/end
               floor((viewing_event_end_client_utc_raw - viewing_event_start_client_utc_raw)/1000) event_sec_precise, --  <-- I think use this for event_secs
               content_duration,
               programme_name, buffering_duration,
                --need to work out the duration of viewing before midnight if event_continuing
               case when continuing_content_from = 1 AND end_event_series = 1 then datediff(ss, viewing_event_start_utc, ns_ts)  --  period of content after midnight (to subtract from accumulated clip playing time)
                    when continuing_content_from = 1 then -(ns_st_pt - datediff(ss, viewing_event_start_utc, ns_ts))  --  period of content *before* midnight (to subtract from accumulated clip playing time)
                    when content_continuing_to = 1 then  datediff(ss, viewing_event_start_utc, viewing_event_end_utc) --  period *upto* midnight to use as accumulated clip playing time
                    
                    --for linear stream (where ns_st_pt doesnt really work)
                    when stream_context = 'lin' AND end_event_series = 1 then datediff(ss, viewing_event_start_utc, viewing_event_end_utc)

                    --for VOD (where we can use ns_st_pt)
                    when new_event_series = 1 then 0                                                                  --  don't use any surplus accumulated play-times at the start of the series
                    when end_event_series = 1 AND e.ns_st_pt = 0 then fake_ns_st_pt
                    when end_content = 1 AND e.ns_st_pt = 0 AND prev_ns_st_ev = 'pause' AND ns_st_ev = 'play' then fake_ns_st_pt
                    when end_content = 1 AND e.ns_st_pt = 0 then next_ns_st_pt                                        --  use fix if playtime is attributed to the next event incorrectly
                    else ns_st_pt
                end play_duration
          into barbera.V239_comscore_view
          from barbera.V239_comscore_view2 e
         --where not(coalesce(prev_ns_st_ev, 'play') = 'end' and ns_st_ev = 'end') --fix for taking out a whole load of continuous end events...
         order by sam_profileid,
                  ns_ap_device,
                  ns_ts,
                  ns_st_ec
        commit



        /* code_location_E01 *************************************************************************
         *****                                                                                      **
         *****     Insert the aggregated events into the monthly table. If the start                **
         *****     of a new month, create a new table.                                              **
         *****                                                                                      **
         *****     1. Prepare executable/dynamic SQL statement                                      **
         *****                                                                                      **
         *********************************************************************************************/
/*commit
declare @monthly_table_name varchar(300)
declare @xsql varchar(10000)
declare @data_run_date       DATE
SET @data_run_date = '2015-08-05'
*/

        SET @monthly_table_name = 'barbera.VESPA_Comscore_SkyGo_'||dateformat(@data_run_date, 'YYYYMM')

        EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Using ['||@monthly_table_name||']'


        SET @xsql = ' ###insert_into_table###
                   select 
                   account_number               as account_number,
                   cb_key_household             as cb_key_household,
                   sam_profileid                as sam_profileid,
                   ns_ap_device                 as ns_ap_device,
                   platform_name                as platform_name,
                   platform_version             as platform_version,
                   max(stream_context)          as stream_context,
                   max(station_name)            as station_name,
                   max(channel_id)              as channel_id,
                   null                         as service_key,
                   max(vod_asset_id)            as vod_asset_id,
                   max(ad_asset_id)             as ad_asset_id,
                   cast(min(ns_st_id)  as varchar(24))  as stream_id,                           --added 2016-07-26 for reconciliation work with Activity Capture
                   cast(min(ns_vid) as varchar(48))     as session_id,                          --added 2016-07-26 for reconciliation work with Activity Capture
                   cast(null as bigint)         as dk_programme_instance_dim,
                   cast(null as datetime)       as broadcast_start_date_time_utc,
                   cast(null as datetime)       as broadcast_end_date_time_utc,
                   cast(null as datetime)       as broadcast_start_date_time_local,
                   cast(null as datetime)       as broadcast_end_date_time_local,

                   -- the client side viewing event times require adjusting if the
                   -- duration spans more than one day to use the server-side timestamps
                   max(viewing_event_start_utc)             as programme_instance_start_utc,
                   max(viewing_event_end_utc)               as programme_instance_end_utc,
                   max(viewing_event_start_client_local)    as programme_instance_start_local,
                   max(viewing_event_end_client_local)      as programme_instance_end_local,
                   viewing_event_start_utc,
                   viewing_event_end_utc,
                   viewing_event_start_client_utc_raw   as viewing_event_start_utc_raw,
                   viewing_event_end_client_utc_raw     as viewing_event_end_utc_raw,
                   viewing_event_start_client_local     as viewing_event_start_local,
                   viewing_event_end_client_local       as viewing_event_end_local,
                   max(daylight_savings_flag_start)     as daylight_savings_start_flag,
                   max(daylight_savings_flag_end)       as daylight_savings_end_flag,
                   max(server_start_utc)                as server_event_start_utc,
                   max(server_end_utc)                  as server_event_end_utc,
                   max(server_start_utc_raw)            as server_event_start_utc_raw,
                   max(server_end_utc_raw)              as server_event_end_utc_raw,
                   max(server_start_local_time)         as server_start_local_time,
                   max(server_end_local_time)           as server_end_local_time,
                   max(connection_type_start)           as connection_type_start,
                   max(connection_type_end)             as connection_type_end,
                   max(genre)                           as genre_description,
                   cast(null as varchar(20))            as sub_genre_description,
                   max(ad_flag)                         as ad_flag,
                   min(cast((dateformat(cast(''###data_run_date###'' as date), ''YYYYMMDD'')||uniqid) as bigint)) aggr_event_id, --unique identifier for the event
                   (max(uniqid_end) - min(uniqid_start))+1 as event_count, -- +1 as inclusive
                   max(erroneous_data_suspected_flag)   as erroneous_data_suspected_flag,
                   max(continuing_content_from)         as view_continuing_flag,
                   max(content_continuing_to)           as view_continues_next_day_flag,
                   0                                    as linear_instance_flag, --as not split by programme yet
                   max(content_duration)                as content_duration, -- <--- picks up wrong duration sometimes
                   max(programme_name)                  as programme_name,    --  <--- preroll descriptors should be stripped from programme name before this point
                   case
                        --require fix for durations that have been affected by client-side clock change making the duration really long..
                        when sum(play_duration) > content_duration AND event_count <= 2 then content_duration
                        else sum(play_duration)
                    end                                 as duration_viewed, -- alternative with capping applied if only two events are defining aggregate, ie play/end
                   case when coalesce(content_duration, 0) = 0 then cast(null as decimal(5,3))
                        else cast(round((duration_viewed*1.0/content_duration*1.0),2) as decimal(5,3))
                    end                                 as percentage_viewed,
                   cast(''###data_run_date###'' as date) data_date_local,
                   today() load_date
            ###into_table###             --into VESPA_Comscore_SkyGo_YYYYMM       
             from barbera.V239_comscore_view            
            group by account_number, cb_key_household, sam_profileid, ns_ap_device, platform_name, platform_version,
                     viewing_event_start_utc, viewing_event_end_utc, viewing_event_start_utc_raw, viewing_event_end_utc_raw,
                     viewing_event_start_local, viewing_event_end_local
            order by account_number, cb_key_household, sam_profileid, ns_ap_device, platform_name, platform_version, viewing_event_start_utc
            commit


          --update the audit_run table with success...
            UPDATE barbera.VESPA_Comscore_SkyGo_audit_run_tbl
               SET basic_view_created = 1
             where data_date = dateformat(cast(''###data_run_date###'' as date), ''YYYY-MM-DD'')
            commit
            '



        /* code_location_E02 *************************************************************************
         *****                                                                                      **
         *****     2. Execute the dynamic SQL statement                                             **
         *****                                                                                      **
         *********************************************************************************************/

        IF object_id(@monthly_table_name) IS NOT NULL
            BEGIN
                --delete any previous runs of the same load if they exist..
                execute('delete '||@monthly_table_name||' where data_date_local = cast('''||@data_run_date||''' as date)')

                execute (
                    replace(
                      replace(
                        replace(@xsql, '###insert_into_table###', 'INSERT into '||@monthly_table_name),
                                       '###into_table###', ''),
                                       '###data_run_date###', @data_run_date)
                )
                commit
                EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Inserting into existing view<'||@monthly_table_name||'>'
                commit
            END
         ELSE
            BEGIN
                execute (
                  replace(
                    replace(
                        replace(@xsql, '###insert_into_table###', ''),
                                       '###into_table###', 'into '||@monthly_table_name),
                                       '###data_run_date###', @data_run_date)
                )
                commit

                --add indexes to new table
                execute('
                    create index '||@monthly_table_name||'_aggr_event_id_idx on '||@monthly_table_name||'(aggr_event_id)
                    --more indexes added
                    create index '||@monthly_table_name||'_data_date_local_idx on '||@monthly_table_name||'(data_date_local)
                    create index '||@monthly_table_name||'_sam_profileid_idx on '||@monthly_table_name||'(sam_profileid)
                    create index '||@monthly_table_name||'_station_name_idx on '||@monthly_table_name||'(station_name)
                    create DTTM index '||@monthly_table_name||'_server_startutc_idx on '||@monthly_table_name||'(server_event_start_utc)
                    create DTTM index '||@monthly_table_name||'_server_endutc_idx on '||@monthly_table_name||'(server_event_end_utc)
                    ')

                EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Creating new view<'||@monthly_table_name||'>'
                commit
            END

        execute('grant select on '||@monthly_table_name||' to vespa_group_low_security')

        execute('grant select on '||@monthly_table_name||' to public')  --temporary

        EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Completed build of basic view'



        /* code_location_F01 *************************************************************************
         *****                                                                                      **
         *****       Update basic stats... links back with file/raw data stats                      **
         *****                                                                                      **
         *********************************************************************************************/

            SET @xsql = '
                        SELECT sum(case when stream_context = ''vod'' then 1 else 0 end)  aggr_vod_events,
                               sum(case when stream_context = ''dvod'' then 1 else 0 end) aggr_dvod_events,
                               sum(case when stream_context = ''lin'' then 1 else 0 end)  aggr_lin_events
                          into barbera.Comscore_skygo_day_stats_tmp
                          from '||@monthly_table_name||' a
                         where a.data_date_local = '''||@data_run_date||'''
                         group by a.data_date_local

                        UPDATE barbera.VESPA_Comscore_SkyGo_audit_stats_tbl s
                           SET s.aggr_vod_events  = a.aggr_vod_events,
                               s.aggr_dvod_events = a.aggr_dvod_events,
                               s.aggr_lin_events  = a.aggr_lin_events
                          from barbera.Comscore_skygo_day_stats_tmp a
                         WHERE s.data_date = '''||@data_run_date||'''

                          drop table barbera.Comscore_skygo_day_stats_tmp

                         commit
                        '
            EXECUTE(@xsql)


END;




--------------------------------------------------------------------------------
--#####################     END     ############################################
--##############################################################################
--##############################################################################
--##############################################################################




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
# Created between:   06/08/2014 - 30/10/2014
# Created by:        Alan Barber (ABA)
# Description:
#
# List of steps:
# --------------
#
# code_location_A01  First prepare the information about this load and identify if ok to load
# code_location_A02  Prepare Universe information
# ----------------------------------------------------------
# code_location_B01  Halt the build process and raise error if the raw data looks wrong
# ----------------------------------------------------------
# code_location_C01  Start the conversion of raw data into event aggregate.
#                    First - extract the local day's period keys from VESPA_CALENDAR
# code_location_C02  Extract the required fields from the day's data (from COMSCORE_UNION_VIEW)
#                    handling DQ error codes with conversions
#                    Add a unique ID to each row, which will also order events
# code_location_C03  Form the context of each event by creating the event transitions
#                    using functions lag() and lead()
#                    Identify certain events (including 'orphaned events'), content starts,
#                    and event series start with flags
# code_location_C04  Identify the context of the Orphaned events, and events that signify
#                    the end of content or event series. Also apply fixes to the assetID and
#                    accumulated playing time (ns_st_pt) if required
# code_location_C05  Apply midnight cross-over rules to extend viewing to/from midnight
#                    add local-time fields. Remove orphaned-end events
# code_location_D01  Build an aggregate table using new_event_series = 1, end_event_series = 1,
#                    continuing_content_from = 1, content_continuing_to = 1
#                    Incorporate additional lead/lag context/transistions for requiring fields
# code_location_D02  Compress the prepared start/end event rows into a single 'aggregated' row
#                    Apply rules for allocating the event's play duration
# code_location_E01  Insert the aggregated events into the monthly table. If the start
#                    of a new month, create a new table.
#                    1. Prepare executable/dynamic SQL statement
# code_location_E02  2. Execute the dynamic SQL statement
# code_location_F01  Update basic stats... links back with file/raw data stats
# code_location_G01  Create Channel Mapping Information
# code_location_G02  Make manual adjustments to channel mapping info
# code_location_H01  Split Linear Events into Programme Instances
# code_location_H02  Record stats around splitting linear events into Programme Instances
# code_location_I01  Scan the day's records and make corrections to missing or incorrect data
# code_location_J01  Refresh union view  -- VESPA_Comscore_SkyGo_Union_View
# code_location_K01  Log completion of build procs
# code_location_L01  drop tables no longer requiered
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - sk_prod.VESPA_CALENDAR
#     - VESPA_Comscore_SAV_summary_tbl
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 30/10/2014  ABA   Initial version
# 07/11/2014  ABA   Added @run_date parameter to 'vespa_comscore_skygo_event_view_prep' procedure call
# 13/11/2014  ABA   Added code in union_view build to re-rank available months so the view builds correctly
# 27/11/2014  ABA   Added row_id to the universe_tbl in preparation for splitting the extraction of viewing data
#                   for large quantities... but then commented out, as temp table memory issues
# 09/12/2014  ABA   Code section B01 now referencing 'data_date' field rather than it's previous name
# 09/12/2014  ABA   Added 'drop table #Comscore_skygo_day_stats_tmp' command to F01 & H02: following update statement
# 11/12/2014  ABA   now resets basic_view_created = 0 after failed run
# 11/12/2014  ABA   Section K01 to drop temporary tables at the end of the proc
# 22/12/2014  ABA   Added ability to run when data volume is low -  IF @status not in ('ok','low') then raise error
# 22/12/2014  ABA   Wrapped the remained of script inside BEGIN/END statement for "IF @status not in ('ok','low')" so not run if failure
# 07/01/2015  ABA   Added execution of a new proc to build universe information (replaces previous code)
# 07/01/2015  ABA   Added some indexes to monthly tables when they are first built
# 08/01/2015  ABA   Added indexes to VESPA_CALENDAR_section_tmp on utc columns
#                   Changed use of VESPA_CALENDAR to use VESPA_CALENDAR_section_tmp instead
#                   Call to EXEC 'vespa_comscore_skygo_channelmap_refresh' now includes: @data_run_date
# 13/01/2015  ABA   Set erroneous_data_flag = 1  when  view_continuing_flag = 1  but first event starts way after midnight
# 15/08/2016  ABA   Midnight x-over calculation: added rule to deal with occurences of client before, but server timestamps after midnight cut-off. No rounds to the correct hour (rather than an hour too early!)
# 07/11/2016  ABA   Made the run_build procedure, and viewing create script seperate. 
# 16/11/2016  ABA   Changed round the order of execution code to trigger channel mapping before the raw events are manipulated
#
###############################################################################*/


CREATE or REPLACE procedure barbera.vespa_comscore_skygo_run_build(
                IN @data_run_date                 DATE,
                IN @suppress_universe_build       BIT,
                IN @lower_limit                   INTEGER,
                IN @stddev_over_rows              INTEGER,
                IN @weighting_rows                INTEGER) AS
BEGIN

    commit
    DECLARE @status             varchar(24)
    DECLARE @xsql               varchar(10000)
    DECLARE @monthly_table_name varchar(300)

    DECLARE @build_start_datetime datetime
        SET @build_start_datetime = dateformat(now(),'YYYY-MM-DD HH:mm:ss')


--tempory fix.. as Olive is shocking sometimes
 create or replace VIEW barbera.comscore_union_view as
        select * from sk_prod.comscore_201508
          union all
        select * from comscore_201607
          union all
        select * from comscore_201608
          union all
        select * from sk_prod.comscore_201609  --X
          union all
        select * from comscore_201610
          union all
        select * from comscore_201611
          union all
        select * from comscore_201612
commit


    /* code_location_A01 ***********************************************************
     *****                                                                        **
     *****     First prepare the information about this load and identify         **
     *****     if ok to load                                                      **
     *****                                                                        **
     *******************************************************************************/

    UPDATE barbera.VESPA_Comscore_SkyGo_audit_run_tbl
       SET build_started = 1
     where data_date = dateformat(cast(@data_run_date as date), 'YYYY-MM-DD')
    commit

    EXEC barbera.VESPA_Comscore_SkyGo_log 'Build Start <'||@data_run_date||'>'

    EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Preparing stats'


----------------------
/*
commit
    DECLARE @data_run_date date
    DECLARE @suppress_stats_universe_build bit
    DECLARE @lower_limit integer
    DECLARE @stddev_over_rows integer
    DECLARE @weighting_rows integer

    --set the date we are running this for
    SET @data_run_date = '2015-01-02'
    SET @suppress_stats_universe_build = 0

    --set variables for the monitoring stats (calculating expected data volumes from the underlaying data)
    SET @lower_limit = 200000 -- floor at which an alert is triggered regardless of other stats (number of expected accounts)
    SET @stddev_over_rows = 7 -- how many days the stddev of accounts is calculated over
    SET @weighting_rows = 4   -- how many days are used to exponentially weight the moving average
------
*/


    EXEC barbera.vespa_comscore_skygo_event_view_prep @data_run_date, @lower_limit, @stddev_over_rows, @weighting_rows

    if @@error = 18001 BEGIN return raiserror 18001 'No File Supplied' END


    /* code_location_A02 *************************************************************************
     *****                                                                                      **
     *****      Prepare Universe information                                                    **
     *****                                                                                      **
     *********************************************************************************************/
    IF @suppress_universe_build != 1
        BEGIN
            EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Creating universe information'

            exec barbera.vespa_comscore_skygo_universe_build @data_run_date

/*
            IF object_id('VESPA_Comscore_SkyGo_universe_tbl') IS NOT NULL
                BEGIN
                    drop table VESPA_Comscore_SkyGo_universe_tbl
                END

            select --dense_rank() over(order by c.sam_profileid asc) row_id,
                   c.sam_profileid,
                   sav.account_number,
                   sav.cb_key_household,
                   coalesce(sav.exclude_flag, 0) exclude_flag,       --default to 0 (don't exclude)
                   coalesce(sav.aggregate_flag, 0) aggregate_flag,     --default to 0 (don't aggregate)
                   count(distinct ns_ap_device) device_types_in_yr,
                   count(distinct case when sg_vs_sc = 'lin' then ns_st_ci else null end)   unique_lin_channels_in_yr,
                   count(distinct case when sg_vs_sc = 'vod' then ns_st_ci else null end)   unique_vod_assets_in_yr,
                   count(distinct case when sg_vs_sc = 'dvod' then ns_st_ci else null end)  unique_dvod_assets_in_yr,
                   max(cast(ns_utc as date)) last_received_date,
                   dateformat(min(ns_ts), 'YYYY-MM-DD HH:MM:SS') first_event_utc,  --local client time-stamp
                   dateformat(max(ns_ts), 'YYYY-MM-DD HH:MM:SS') latest_event_utc, --local client time-stamp
                   dateformat(min(ns_utc), 'YYYY-MM-DD HH:MM:SS') first_data_utc,  --timestamp recived at the Comscore server
                   dateformat(max(ns_utc), 'YYYY-MM-DD HH:MM:SS') latest_data_utc, --timestamp recived at the Comscore server
                   cast(max(case when sg_vs_sc = 'lin' then ns_utc else null end) as date)  latest_lin_data_utc,
                   cast(max(case when sg_vs_sc = 'vod' then ns_utc else null end) as date)  latest_vod_data_utc,
                   cast(max(case when sg_vs_sc = 'dvod' then ns_utc else null end) as date)  latest_dvod_data_utc,
                   max(cb_data_date) last_load_date,
                   cast(max(ns_utc) as date) event_last_load_date,
                   cast(dateadd(dd, -1, today()) as date) last_expected_event_date,
                   datediff(dd, event_last_load_date, last_expected_event_date) days_since_last_data,
                   count( distinct case when cast(ns_utc as date) > dateadd(dd, -7, last_expected_event_date) then cast(ns_utc as date) else null end) days_active_rolling_week,
                   count( distinct case when cast(ns_utc as date) > dateadd(mm, -1, last_expected_event_date) then cast(ns_utc as date) else null end) days_active_rolling_month,
                   count( distinct case when cast(ns_utc as date) > dateadd(qq, -1, last_expected_event_date) then cast(ns_utc as date) else null end) days_active_rolling_quarter,
                   count( distinct case when cast(ns_utc as date) > dateadd(yy, -1, last_expected_event_date) then cast(ns_utc as date) else null end) days_active_rolling_year
              into VESPA_Comscore_SkyGo_universe_tbl
              from COMSCORE_UNION_VIEW c --using the union view
                   left join VESPA_Comscore_SkyGo_SAV_summary_tbl sav --combine with SAV
                     on c.sam_profileid = sav.sam_profileid
             group by c.sam_profileid, sav.account_number, sav.cb_key_household, sav.exclude_flag, sav.aggregate_flag

            commit

            create index VESPA_Comscore_universe_sam_idx on VESPA_Comscore_SkyGo_universe_tbl(sam_profileid)
            commit
*/

        END
     ELSE
        BEGIN
            EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Suppressed Universe Build'
        END -- end @supress_stats_universe_build if = 1



    /* code_location_B01 *************************************************************************
     *****                                                                                      **
     *****      Halt the build process and raise error if the raw data looks wrong              **
     *****                                                                                      **
     *********************************************************************************************/

    --check/QA to see if we can run the process for the required day
    SELECT @status = qa_status
      from barbera.VESPA_Comscore_SkyGo_audit_run_tbl r
    where data_date = dateformat(cast(@data_run_date as date), 'YYYY-MM-DD')

    IF @status not in ('ok','low')
        BEGIN
            -- set processed information to flag issue
            UPDATE barbera.VESPA_Comscore_SkyGo_audit_run_tbl
               SET basic_view_created = -1
             where data_date = dateformat(cast(@data_run_date as date), 'YYYY-MM-DD')

            EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Process failed['||@status||']'

            RAISERROR 50001 'data volume is '||@status
        END
    ELSE
        BEGIN
            --reset any previous attempts
            UPDATE barbera.VESPA_Comscore_SkyGo_audit_run_tbl
               SET basic_view_created = 0
             where data_date = dateformat(cast(@data_run_date as date), 'YYYY-MM-DD')
            commit


            /* code_location_G01 *************************************************************************
             *****                                                                                      **
             *****                      Create Channel Mapping Information                              **
             *****                                                                                      **
             *********************************************************************************************/

            EXEC barbera.vespa_comscore_skygo_channelmap_build  @data_run_date   

            


            /* code_location_C01 *************************************************************************
             *****                                                                                      **
             *****      Start the conversion of raw data into event aggregate.                          **
             *****      First - extract the local day's period keys from VESPA_CALENDAR                 **
             *****                                                                                      **
             *********************************************************************************************/

--DECLARE @xsql               varchar(10000)
--DECLARE @monthly_table_name varchar(300)
--declare @data_run_date       DATE
--set @data_run_date = '2016-06-01'

            EXEC barbera.vespa_comscore_skygo_event_view_create @data_run_date



            --Apply Channel mapping logic to clean up the view thats just been created
            SET @monthly_table_name = 'barbera.VESPA_Comscore_SkyGo_'||dateformat(@data_run_date, 'YYYYMM')
            execute('UPDATE '||@monthly_table_name||' v
                        SET v.station_name = cm.channel_name,
                            v.service_key  = cm.service_key
                       FROM barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl cm
                      WHERE v.station_name = cm.station_name
                     commit ')




            /* code_location_H01 *************************************************************************
             *****                                                                                      **
             *****             Split Linear Events into Programme Instances                             **
             *****                                                                                      **
             *********************************************************************************************/

 --params for running code from this point...
--this small section (to the end of the proc) can be run to manually run the conversion of linear instances
/*DECLARE @data_run_date date
DECLARE @xsql               varchar(8000)
DECLARE @monthly_table_name varchar(48)
SET @data_run_date = '2015-01-18'

*/
SET @monthly_table_name = 'VESPA_Comscore_SkyGo_'||dateformat(@data_run_date, 'YYYYMM')

            EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Splitting linear viewing events'

            --iterates through the aggregated linear events, splitting each one into programme instances
            EXEC barbera.vespa_comscore_skygo_linear_split @data_run_date, 0

            EXEC barbera.VESPA_Comscore_SkyGo_log ' <'||@data_run_date||'> proc complete with error code <'||@@error||'>'

            EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Completed linear instance derivations'

            --update linear_events_split flag in audit_run_tbl
            execute('
              Update barbera.VESPA_Comscore_SkyGo_audit_run_tbl
                 SET linear_events_split = 1
               WHERE data_date = '''||@data_run_date||'''
              commit
             ')



            /* code_location_H02 *************************************************************************
             *****                                                                                      **
             *****        Record stats around splitting linear events into Programme Instances          **
             *****                                                                                      **
             *********************************************************************************************/


            EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Updating linear stats'

            SET @xsql = '
                        SELECT sum(case when linear_instance_flag = 1 then 1 else 0 end)  linear_event_instances,
                               sum(case when linear_instance_flag = 0 then 1 else 0 end) aggr_lin_not_split
                          into barbera.Comscore_skygo_day_stats_tmp_b
                          from '||@monthly_table_name||' a
                         where a.data_date_local = '''||@data_run_date||'''
                           and stream_context = ''lin''
                         group by a.data_date_local

                        UPDATE barbera.VESPA_Comscore_SkyGo_audit_stats_tbl s
                           SET s.linear_event_instances  = a.linear_event_instances,
                               s.aggr_lin_not_split = a.aggr_lin_not_split
                          from barbera.Comscore_skygo_day_stats_tmp_b a
                        WHERE s.data_date = '''||@data_run_date||'''

                          drop table barbera.Comscore_skygo_day_stats_tmp_b

                        commit
                        '
            EXECUTE(@xsql)


          /* code_location_I01 ***************************************************************************
           *****                                                                                      **
           *****       Scan the day's records and make corrections to missing or incorrect data       **
           *****                                                                                      **
           *********************************************************************************************/


          -- we should log these updates/deletes for data quality purposes


          EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Applying adjustments to impossible viewing events'

          --set duration from programme_instance start/end if it is found to be negative - and start/end are available
          SET @xsql = 'UPDATE '||@monthly_table_name||'
                          SET duration_viewed = datediff(ss, programme_instance_start_utc, programme_instance_end_utc)
                        where coalesce(duration_viewed, -1) < 0
                          and programme_instance_start_utc is not null
                          and programme_instance_end_utc is not null
                          and data_date_local = '''||@data_run_date||'''
                       commit'
          EXECUTE(@xsql)

          --make sure that all viewing durations are capped to the calculated viewing period
          SET @xsql = 'UPDATE '||@monthly_table_name||'
                          SET duration_viewed = datediff(ss, programme_instance_start_utc, programme_instance_end_utc)
                        where datediff(ss, programme_instance_start_utc, programme_instance_end_utc) < duration_viewed
                          and programme_instance_start_utc is not null
                          and programme_instance_end_utc is not null
                          and data_date_local = '''||@data_run_date||'''
                       commit'
          EXECUTE(@xsql)

          --remove anything that that doesn't have a start or end time. Later we could try and rescue these but the volumes are low (circa 0.1%)
          SET @xsql = 'DELETE '||@monthly_table_name||'
                        where programme_instance_start_utc is null
                           or programme_instance_end_utc is null
                       commit'
          EXECUTE(@xsql)


          --set erroneous data flag when view_continuing_flag is 1 (as the first event of the day was an 'end' event) but the end event
          --    happend after the first hour of the day leading to a start hour > 0 (which is not possible for a view_continuing event)
          SET @xsql = 'UPDATE '||@monthly_table_name||'
                          SET erroneous_data_suspected_flag = 1
                        where view_continuing_flag = 1
                          and datepart(hh, viewing_event_start_local) != 0  --client clock could be wrong, but this makes it erroneous anyway..
                       commit'
          EXECUTE(@xsql)


          --set erroneous data flag when long duration events are suspected due to client-side clock changes [flag all viewing where duration_viewed > 86400]
          SET @xsql = 'UPDATE '||@monthly_table_name||'
                          SET erroneous_data_suspected_flag = 1
                        where duration_viewed > (24*3600)
                       commit'
          EXECUTE(@xsql)



        /* code_location_J01 *************************************************************************
         *****                                                                                      **
         *****       Refresh union view  -- VESPA_Comscore_SkyGo_Union_View                         **
         *****                                                                                      **
         *********************************************************************************************/
        
        EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Refreshing union view'
 
        declare @current_month varchar(6)
        declare @months        integer
        declare @max_rank      integer
        declare @month_exists  bit

        IF object_id('barbera.Comscore_SkyGo_union_months_tmp')   IS NOT NULL   BEGIN  drop table barbera.Comscore_SkyGo_union_months_tmp    END
        IF object_id('barbera.Comscore_SkyGo_union_months_tmp_b') IS NOT NULL   BEGIN  drop table barbera.Comscore_SkyGo_union_months_tmp_b  END


        --create months list that should be available
        select dateformat(data_date, 'YYYYMM') months, dense_rank() over(order by months desc) rank, null new_rank   -- order by months desc. changed [2016-07-29]
          into barbera.Comscore_SkyGo_union_months_tmp
          from barbera.VESPA_Comscore_SkyGo_audit_run_tbl
         where build_completed = 1                            --only add the months in this list that have built correctly. Changed [2016-07-29] to use build_completed rather than build_success
         group by dateformat(data_date, 'YYYYMM')
        commit 

        --verify that the months are available
        /*
        select @months = max(rank)
          from barbera.Comscore_SkyGo_union_months_tmp
         where rank <= 13    --- restrict the maximum number of months in this view
        */

        

        --iterate through each month, if the table doesn't exist, delete it from the list
        set @months = 1

        select @max_rank = max(rank)
         from barbera.Comscore_SkyGo_union_months_tmp

        while(@months<=@max_rank) -- Method changed to count up (rather than down) to 13 [2016-07-29]
            BEGIN
                select @current_month = months
                  from barbera.Comscore_SkyGo_union_months_tmp
                 where rank = @months

                IF object_id('barbera.VESPA_Comscore_SkyGo_'||@current_month) IS NULL 
                        AND @current_month IS NOT NULL -- this condition deletes the month entry if the @current_month didnt exists - not all ranks exist
                    BEGIN
                        delete barbera.Comscore_SkyGo_union_months_tmp
                         where months = @current_month
                        commit
                    END
                SET @months = @months + 1  --limit to 13 months. Method changed to count up (rather than down) to 13 [2016-07-29]
            END


        

        --see if month exists in the listing
        select @month_exists = max(case when dateformat(@data_run_date, 'YYYYMM') = months then 1 else 0 end)  --modified to use max [2016-07-29]
          from barbera.Comscore_SkyGo_union_months_tmp
 
        --if the month doesn't exist, add it.. This happens when loading records for a new month
        IF @month_exists = 0
          BEGIN
            --set location of last record
            select @max_rank = max(rank)
              from barbera.Comscore_SkyGo_union_months_tmp

            INSERT into barbera.Comscore_SkyGo_union_months_tmp(months, rank, new_rank)
                 values(dateformat(@data_run_date, 'YYYYMM') , @max_rank+1, null )
            commit     
          END
          


        --as some months may have been missing (so deleted), give the order a new_rank
        select months, rank, dense_rank() over(order by months) new_rank
          into barbera.Comscore_SkyGo_union_months_tmp_b
          from barbera.Comscore_SkyGo_union_months_tmp
         group by months, rank
        commit
 
        --create union view of all the verified months
        select @months = count(1)
          from barbera.Comscore_SkyGo_union_months_tmp_b
        
        IF @months > 13 
          BEGIN  
             SET @months = 13 --limit to 13 months. Cap applied here rather than earlier [2017-02-27]
          END
  
        IF @months > 0
            BEGIN                
                declare @union_view_str varchar(1000)
                set @union_view_str = 'create or replace view barbera.VESPA_Comscore_SkyGo_Union_View as ('

                --iterate through each month, if the table doesn't exist delete it from the list
                while(@months>0)
                    BEGIN
                        select @current_month = months
                          from barbera.Comscore_SkyGo_union_months_tmp_b
                         where new_rank = @months

                        IF object_id('barbera.VESPA_Comscore_SkyGo_'||@current_month)   IS NOT NULL
                            BEGIN
                                SET @union_view_str = @union_view_str||' select * from barbera.VESPA_Comscore_SkyGo_'||@current_month
                                SET @months = @months - 1
                             END
                        ELSE
                            BEGIN
                               SET @months = @months - 1
                            END
                        IF @months > 0
                            BEGIN
                               SET @union_view_str = @union_view_str||' union all '
                            END

                    END
                set @union_view_str = @union_view_str||')     '

                execute(@union_view_str)
                commit

            END




        /* code_location_K01 *************************************************************************
         *****                                                                                      **
         *****                       Log that the build has completed                               **
         *****                                                                                      **
         *********************************************************************************************/

        EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Completed Build'



        /* code_location_L01 *************************************************************************
         *****                                                                                      **
         *****                       Drop tables no longer required                                 **
         *****                                                                                      **
         *********************************************************************************************/


        IF object_id('barbera.VESPA_CALENDAR_section_tmp')          IS NOT NULL   BEGIN  drop table barbera.VESPA_CALENDAR_section_tmp          commit  END    
        IF object_id('barbera.V239_comscore_event_tmp5')            IS NOT NULL   BEGIN  drop table barbera.V239_comscore_event_tmp5            commit  END
        IF object_id('barbera.V239_comscore_event_tmp4')            IS NOT NULL   BEGIN  drop table barbera.V239_comscore_event_tmp4            commit  END
        IF object_id('barbera.V239_comscore_event_tmp3')            IS NOT NULL   BEGIN  drop table barbera.V239_comscore_event_tmp3            commit  END
        IF object_id('barbera.V239_comscore_event_tmp2')            IS NOT NULL   BEGIN  drop table barbera.V239_comscore_event_tmp2            commit  END
        IF object_id('barbera.V239_comscore_event_tmp')             IS NOT NULL   BEGIN  drop table barbera.V239_comscore_event_tmp             commit  END
        IF object_id('barbera.V239_comscore_view2')                 IS NOT NULL   BEGIN  drop table barbera.V239_comscore_view2                 commit  END
        IF object_id('barbera.V239_comscore_view')                  IS NOT NULL   BEGIN  drop table barbera.V239_comscore_view                  commit  END
        IF object_id('barbera.Comscore_SkyGo_union_months_tmp_b')   IS NOT NULL   BEGIN  drop table barbera.Comscore_SkyGo_union_months_tmp_b   commit  END
        IF object_id('barbera.Comscore_SkyGo_union_months_tmp')     IS NOT NULL   BEGIN  drop table barbera.Comscore_SkyGo_union_months_tmp     commit  END
        
    END --end of wrapped @status not in ('ok','low') check




    /* code_location_M01 ********************************************************************
     *****                                                                                 **
     *****                       Record run success                                        **
     *****                                                                                 **
     ****************************************************************************************/

    DECLARE @build_end_datetime datetime
    DECLARE @build_success      BIT
        SET @build_end_datetime = now()

    UPDATE barbera.VESPA_Comscore_SkyGo_audit_run_tbl
       SET build_completed = 1
     where data_date = dateformat(cast(@data_run_date as date), 'YYYY-MM-DD')

     UPDATE barbera.VESPA_Comscore_SkyGo_audit_run_tbl
       SET build_completed_date = @build_end_datetime
     where data_date = dateformat(cast(@data_run_date as date), 'YYYY-MM-DD')

    UPDATE barbera.VESPA_Comscore_SkyGo_audit_run_tbl
       SET build_duration_secs = datediff(ss, @build_start_datetime, @build_end_datetime)
     where data_date = dateformat(cast(@data_run_date as date), 'YYYY-MM-DD')

    select @build_success = case when build_started       = 1
                                  and basic_view_created  = 1
                                  and channels_mapped     = 1
                                  and linear_events_split = 1
                                  and build_completed     = 1
                                 then 1 else 0
                             end
      from barbera.VESPA_Comscore_SkyGo_audit_run_tbl
     where data_date = dateformat(cast(@data_run_date as date), 'YYYY-MM-DD')

    UPDATE barbera.VESPA_Comscore_SkyGo_audit_run_tbl
       SET build_success = @build_success
     where data_date = dateformat(cast(@data_run_date as date), 'YYYY-MM-DD')

    commit

END; 




--------------------------------------------------------------------------------
--#####################     END     ############################################
--##############################################################################
--##############################################################################
--##############################################################################




