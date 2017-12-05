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
# 11/12/2014  ABA   Added local schema
#
###############################################################################*/



CREATE or REPLACE procedure vespa_comscore_skygo_event_view_prep(
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
      from Comscore_union_view
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

   IF object_id('VESPA_Comscore_SkyGo_SAV_account_type_tbl') IS NOT NULL
       TRUNCATE TABLE VESPA_Comscore_SkyGo_SAV_account_type_tbl
   ELSE RAISERROR 18002 'Table <VESPA_Comscore_SkyGo_SAV_account_type_tbl> does not exist'

   --create table that shows account types
   INSERT into VESPA_Comscore_SkyGo_SAV_account_type_tbl(acct_type_code, acct_sub_type, uniqid)
   select acct_type_code,  acct_sub_type,
          dense_rank() over(order by acct_type_code, acct_sub_type) rank
     from CUST_SINGLE_ACCOUNT_VIEW
   group by acct_type_code, acct_sub_type
   commit

   --identify accounts to completely exclude from all further manipulation
   update VESPA_Comscore_SkyGo_SAV_account_type_tbl
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
   update VESPA_Comscore_SkyGo_SAV_account_type_tbl
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
   update VESPA_Comscore_SkyGo_SAV_account_type_tbl
      set normal_flag = 1
    where --acct_type_code = 'STD'
          exclude_flag != 1
      and aggregate_flag !=1

   --just check that only one column has been indicated
   -- **ALERT if check_flag > 1  for any row
   update VESPA_Comscore_SkyGo_SAV_account_type_tbl
      set check_flag = (exclude_flag+aggregate_flag+normal_flag)

   commit


   /* code_location_A02 ***********************************************************
    *****                                                                        **
    *****   Create summary of accounts, and SamProfileIDs                        **
    *****                                                                        **
    *******************************************************************************/

   IF object_id('VESPA_Comscore_SkyGo_SAV_summary_tbl') IS NOT NULL
       TRUNCATE TABLE VESPA_Comscore_SkyGo_SAV_summary_tbl
   ELSE RAISERROR 18003 'Table <VESPA_Comscore_SkyGo_SAV_summary_tbl> does not exist'

   --join with SAM_Registrant, and SAV
   INSERT into VESPA_Comscore_SkyGo_SAV_summary_tbl
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
      INNER JOIN CUST_SINGLE_ACCOUNT_VIEW sav
         ON r.account_number = sav.account_number
      INNER JOIN VESPA_Comscore_SkyGo_SAV_account_type_tbl t
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


   -- remove the days we have not already processed
   DELETE VESPA_Comscore_SkyGo_audit_run_tbl
    where basic_view_created <= 0  --  '<' catches aborted runs



   --insert (into final audit run table) new data_dates where date does not exist
   INSERT into VESPA_Comscore_SkyGo_audit_run_tbl (data_date)
        select a.data_date
          from barbera.Vespa_Comscore_audit_counts_tmp a
                 left outer join
               VESPA_Comscore_SkyGo_audit_run_tbl r
            on a.data_date = r.data_date
         where r.data_date is null
         order by a.data_date asc
   commit



   /* code_location_B02 ***********************************************************
    *****                                                                        **
    *****    Populate <VESPA_Comscore_SkyGo_audit_stats_tbl> table with statistics,    **
    *****    and indicate status for running the rest of the build.              **
    *****                                                                        **
    *******************************************************************************/


   --insert (into final audit_stats table) new data_dates where date does not exist
   INSERT into VESPA_Comscore_SkyGo_audit_stats_tbl (data_date)
        select a.data_date
          from barbera.Vespa_Comscore_audit_counts_tmp a
                 left outer join
               VESPA_Comscore_SkyGo_audit_stats_tbl r
            on a.data_date = r.data_date
         where r.data_date is null
         order by a.data_date asc
   commit


   --update stats audit table for all dates in the raw comscore_union_view
   UPDATE VESPA_Comscore_SkyGo_audit_stats_tbl s
      SET samprofiles = a.samprofiles,
          imported_rows = a.imported_rows
     FROM barbera.Vespa_Comscore_audit_counts_tmp a
    WHERE s.data_date = a.data_date

    commit


   --insert the reject row details
   UPDATE VESPA_Comscore_SkyGo_audit_stats_tbl x
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
   from Vespa_Comscore_audit_counts_tmp a


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
    update VESPA_Comscore_SkyGo_audit_stats_tbl x
       SET samprofiles = coalesce(s.samprofiles, -99),
           sam_trigger_level = coalesce(s.exp_neg, -99),
           imported_rows = coalesce(s.imported_rows, -99),
           status = coalesce(s.status,'unknown')
      from barbera.V239_audit_tmp3 s
     where dateformat(s.data_date, 'YYYY-MM-DD') = dateformat(x.data_date, 'YYYY-MM-DD')
    commit


    --update the success of the load info
    update VESPA_Comscore_SkyGo_audit_run_tbl x
       SET x.qa_status = case when s.imported_rows <= 0 then 'no file' else coalesce(s.status,'unknown') end
      from barbera.V239_audit_tmp3 s, VESPA_Comscore_SkyGo_audit_run_tbl a
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

END

-- *****************                                    ************************
-- ##################o             E N D              o#########################
-- ####################o                            o###########################
-- #############################################################################



