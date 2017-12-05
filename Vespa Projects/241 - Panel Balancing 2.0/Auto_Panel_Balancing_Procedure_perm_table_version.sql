-----------------------------------------
--Auto Panel Balance - Procedural Version
-----------------------------------------

/*
Code is stored in the repository here: Vespa\Vespa Projects\120 - Panel Balancing
Insight Collation is here: http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=120&Source=http%3A%2F%2Frtci%2FLists%2FRTCI%2520IC%2520dev%2FMy%2520Projects%2Easpx

Structure for the main PanBal procedure:

Section A - Setup
  Create tables
  Create variables
  Assign initial values to variables

Section B - Fill the accounts lists
  Fill list1,2,3 and 4
  Fill #segment_THIs

Section C - The Main Loop
  Fill #panel_segmentation
  fill panbal_all_aggregated_results
  Update Good_Household_Index, +1, -1 and incr_diff and decr_diff
  Fill PanBal_traffic_lights
  Update THIs with incr_diff for matching values and decr_diff for unmatching values
  Add new accounts into working panel

Section D - Create amends table
  Count boxes for each account
  Find accounts to add to panel 12 from panels 6/7
  check for <750k boxes
  Create list of rq for list1
  Delete those with the worst rq
  Remove the ones we've added fom list3 (we have already deleted the ones we've added from list4)
  Add accounts to alt. panels, to make 50% in each segment (if poss)
  Add accounts to virtual panels if required and available
  Recreate initial list4
  Add accounts to add to panels 6/7, then eventually panel 12
  Drop tables excep PanBal_amends

The values for variables
@max_imbalance
@max_rq
@precision
are customisable

when you are happy with the results then run the procedure PanBal_drop_tables
This will drop all tables except PanBal_Amends, which lists all of the enablements and disablements that need to happen, as well as any warnings e.g. not enough accounts
*/

drop procedure panbal;
create procedure PanBal as begin

     -------------------
     --Section A - Setup
     -------------------

  --prerequisites
--  execute waterfall
--  execute VirtPan
--  execute panbal_segmentation

     --create tables
     create table PanBal_all_aggregated_results (
          aggregation_variable                               varchar(30)
         ,variable_value                                     varchar(60)
         ,Sky_Base_Households                                int
         ,Panel_Households                                   int
         ,Good_Household_Index                               double default 0
         ,GHIplus1                                           double default 0
         ,GHIminus1                                          double default 0
         ,incr_diff                                          double default 0
         ,decr_diff                                          double default 0
         ,primary key (aggregation_variable, variable_value)
     )
     create lf index idx1 on PanBal_all_aggregated_results(variable_value)
     create lf index idx2 on PanBal_all_aggregated_results(aggregation_variable)

     create table #panel_households(
          aggregation_variable                               varchar(30)
         ,Panel_Households                                   int
     )

     create table PanBal_list1(
          account_number                                     varchar(30)
         ,segment_id                                 int
         ,rq                                                 double
     )

     create table PanBal_list2(
          account_number                                     varchar(30)
         ,segment_id                                 int
         ,rq                                                 double
     )

     create table PanBal_list3(
          account_number                                     varchar(30)
         ,segment_id                                 int
         ,rq                                                 double
         ,thi                                                double
     )
     create hg index idx1 on PanBal_list3(segment_id)
     create hg index idx2 on PanBal_list3(thi)

     create table PanBal_list4(
          account_number                                     varchar(30)
         ,segment_id                                 int
         ,rq                                                 double
         ,thi                                                double
     )
     create hg index idx1 on PanBal_list4(thi)
     create hg index idx3 on PanBal_list4(segment_id)

     create table PanBal_Panel(
          account_number                                     varchar(30)
         ,segment_id                                 int
     )
     create hg index idx1 on PanBal_Panel(segment_id)

     create table PanBal_Scaling_Segment_Profiling (
          segment_id                                 int
         ,Sky_Base_Households                                int
         ,Panel_households                                   int
         ,Acceptably_reliable_households                     int
         ,Acceptably_reporting_index                         decimal(6,2)  default null
         ,primary key (segment_id)
     )
--     create hng index idx1 on PanBal_Scaling_Segment_Profiling(Panel_households)

     create table PanBal_traffic_lights(
          variable_name                                      varchar(30)
         ,imbalance_rating                                   decimal(6,2)
     )

     create table PanBal_results ( --for testing
          imbalance                                          double --the highest variable
         ,tot_imb                                            double --total across all variables
         ,records                                            int    --in the panel
         ,tim                                                datetime
     )

     create table PanBal_amends(
          account_number                                     varchar(30)
         ,movement                                           varchar(60)
     )
     grant select on PanBal_amends to vespa_group_low_security

     create table #descrs(
          segment_id                                 int
         ,descrs                                             double
     )

     create table #panel_segmentation(
          segment_id                                 int
         ,Panel_Households                                   int
     )
     create unique hg index fake_pk on #panel_segmentation (segment_id)

     create table #new_adds(
          account_number                                     varchar(50)
         ,segment_id                                 int
         ,thi                                                double
     )
     create hg index idx1 on #new_adds(thi)

     create table #segment_THIs(segment_id int
                               ,adsmbl             bit    default 0
                               ,region             varchar(30)
                               ,hhcomp             varchar(30)
                               ,tenure             varchar(30)
                               ,package            varchar(30)
                               ,mr                 bit    default 0
                               ,hd                 bit    default 0
                               ,pvr                bit    default 0
                               ,valseg             varchar(30)
                               ,mosaic             varchar(30)
                               ,fss                varchar(30)
                               ,onnet              bit    default 0
                               ,skygo              bit    default 0
                               ,st                 bit    default 0
                               ,bb                 bit    default 0
                               ,thi                double default 0
     )
     create hg index idx1 on #segment_THIs(thi)
     create lf index idx2 on #segment_THIs(region)
     create lf index idx3 on #segment_THIs(hhcomp)
     create lf index idx4 on #segment_THIs(tenure)
     create lf index idx5 on #segment_THIs(package)
     create lf index idx6 on #segment_THIs(valseg)
     create lf index idx7 on #segment_THIs(mosaic)
     create lf index idx8 on #segment_THIs(fss)
     create hg index idx9 on #segment_THIs(segment_id)

     create table #list1_rq(
          account_number                                     varchar(30)
         ,rq                                                 double
         ,boxes                                              int
     )

     create table #temp(
          account_number                                     varchar(30)
         ,boxes                                              int
     )

     create table #vespa_accounts(
          account_number                                     varchar(30)
         ,panel                                              tinyint
         ,rq                                                 double
         ,segment_id                                 int         default 0
     )

     --create variables
     declare @total_sky_base                        int
     declare @panel_reporters                       int
     declare @cow                                   int
     declare @temp                                  int
     declare @imbalance                             double
     declare @tot_imb                               double
     declare @precision                             int
     declare @max_imbalance                         int
     declare @max_rq                                double
     declare @virtuals                              int
     declare @continue                              int
/*
     create variable @total_sky_base                        int;
     create variable @panel_reporters                       int;
     create variable @cow                                   int;
     create variable @temp                                  int;
     create variable @imbalance                             double;
     create variable @tot_imb                               double;
     create variable @precision                             int;
     create variable @max_imbalance                         int;
     create variable @max_rq                                double;
     create variable @virtuals                              int;
     create variable @continue                              int;
*/
     --set starting variables
     set @imbalance = 100
     set rowcount 0 -- just in case it's been set to something

     --these 3 variables can be changed as required
     set @max_imbalance = 30 -- needs to be less than 20 to get a green light
     set @max_rq = 0.8       -- reporting quality should aim be at least 0.8
     set @precision = 500    -- this defines how accurately we try to find a solution - ideally it will be 1, but the lower the number,
                               --the longer the code will take to run.
     set @continue = 8       -- if the code was interrupted, then this is the number of times the main loop will be skipped, and records added directly from list 4

     -------------------------------------
     --Section B - Fill the accounts lists
     -------------------------------------


       insert into #vespa_accounts(account_number
                                  ,panel
                                  ,rq)
       select account_number
             ,panel_id_vespa
             ,min(coalesce(case when reporting_quality > 1 then 1 else reporting_quality end, 1))
         from vespa_analysts.vespa_single_box_view
        where status_vespa='Enabled'
     group by account_number
             ,panel_id_vespa

       update #vespa_accounts as bas
          set bas.segment_id = lkp.segment_id
         from panbal_segment_snapshots as lkp
        where bas.account_number = lkp.account_number

       delete from #vespa_accounts
        where segment_id is null

     --List 1 - Unacceptable panel 12 accounts
       insert into PanBal_list1(account_number
                               ,rq
                               ,segment_id
                               )
       select account_number
             ,rq
             ,segment_id
         from #vespa_accounts
        where panel = 12
          and rq < @max_rq

     --List 2 Acceptable panel 12 accounts
       insert into PanBal_list2(account_number
                               ,rq
                               ,segment_id
                               )
       select account_number
             ,rq
             ,segment_id
         from #vespa_accounts
        where panel = 12
          and rq >= @max_rq

     --List 3 - alternate day panel accounts
       insert into PanBal_list3
        select account_number
             ,segment_id
             ,rq
             ,0
         from #vespa_accounts as vac
        where panel <> 12
          and vac.rq >= @max_rq

     --List 4 - waterfall accounts
       insert into PanBal_list4
       select bas.account_number
             ,segment_id
             ,0
             ,0
         from vespa_analysts.waterfall_base       as bas
              inner join panbal_segment_snapshots as snp on bas.account_number = snp.account_number
        where knockout_level = 9999
     group by bas.account_number
             ,segment_id

       select account_number
             ,100 * sum(cast(expected_cbcks as double) - cast(missing_cbcks as double)) / sum(cast(expected_cbcks as double)) as cbck_rate
         into #cbck_rate
         from vespa_analysts.Waterfall_scms_callback_data
        where expected_cbcks > 0
     group by account_number

     commit
     create hg index idx1 on #cbck_rate(account_number)

       update PanBal_list4 as bas
          set bas.rq = cbc.cbck_rate
         from #cbck_rate as cbc
        where bas.account_number = cbc.account_number

       select account_number
             ,segment_id
         into #Scaling_weekly_sample
         from panbal_segment_snapshots

       insert into PanBal_Scaling_Segment_Profiling (
              segment_id
             ,Sky_Base_Households
       )
       select segment_id
             ,count(1) as Sky_Base_Households
         from #Scaling_weekly_sample as sws
     group by sws.segment_id

       select @total_sky_base = sum(Sky_Base_Households) from PanBal_Scaling_Segment_Profiling

       insert into PanBal_Panel(account_number
                               ,segment_id
             )
       select account_number
             ,segment_id
         from PanBal_list2

     --we only need to work out THIs once for each segment that we have any accounts in list3 or list4
       insert into #segment_THIs(segment_id)
       select distinct(segment_id)
         from (select distinct(segment_id) as segment_id
                 from PanBal_list3
                union
               select distinct(segment_id)
                 from PanBal_list4) as sub

       update #segment_THIs as bas
          set bas.adsmbl  = seg.adsmbl
             ,bas.region  = seg.region
             ,bas.hhcomp  = seg.hhcomp
             ,bas.tenure  = seg.tenure
             ,bas.package = seg.package
             ,bas.mr      = seg.mr
             ,bas.hd      = seg.hd
             ,bas.pvr     = seg.pvr
             ,bas.valseg  = seg.valseg
             ,bas.mosaic  = seg.mosaic
             ,bas.fss     = seg.fss
             ,bas.onnet   = seg.onnet
             ,bas.skygo   = seg.skygo
             ,bas.st      = seg.st
             ,bas.bb      = seg.bb
         from PanBal_segments_lookup as seg
        where bas.segment_id = seg.segment_id

       update PanBal_Scaling_Segment_Profiling as bas
          set Panel_Households             = 0

     --count boxes for every account
       select distinct (ccs.account_number)
             ,count(distinct card_subscriber_id) as boxes
         into #sky_box_count
         from sk_prod.CUST_CARD_SUBSCRIBER_LINK as ccs
              inner join sk_prod.cust_single_account_view as sav on ccs.account_number = sav.account_number
        where effective_to_dt = '9999-09-09'
          and cust_active_dtv = 1
     group by ccs.account_number

     ---------------------------
     --Section C - The Main Loop
     ---------------------------
     while (@imbalance >= @max_imbalance)
          begin

               truncate table #panel_segmentation

               --reqd for traffic lights
                 insert into #panel_segmentation(segment_id
                                                ,Panel_Households)
                 select segment_id
                       ,count(1)
                   from PanBal_Panel
               group by segment_id

                 update PanBal_Scaling_Segment_Profiling as bas
                    set Panel_Households             = seg.Panel_Households
                   from #panel_segmentation as seg
                  where bas.segment_id       = seg.segment_id

               truncate table PanBal_all_aggregated_results

               --create the traffic light report
                 insert into PanBal_all_aggregated_results(aggregation_variable
                                                          ,variable_value
                                                          ,Sky_Base_Households
                                                          ,Panel_Households)
                 select 'adsmbl' -- Name of variable being profiled
                       ,ssl.adsmbl
                       ,sum(base_accounts)
                       ,sum(Panel_households)
                   from PanBal_Scaling_Segment_Profiling as ssp
                        inner join PanBal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
               group by ssl.adsmbl

                 insert into PanBal_all_aggregated_results(aggregation_variable
                                                          ,variable_value
                                                          ,Sky_Base_Households
                                                          ,Panel_Households)
                 select 'region'
                       ,ssl.region
                       ,sum(base_accounts)
                       ,sum(Panel_households)
                   from PanBal_Scaling_Segment_Profiling as ssp
                        inner join PanBal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
               group by ssl.region

                 insert into PanBal_all_aggregated_results(aggregation_variable
                                                          ,variable_value
                                                          ,Sky_Base_Households
                                                          ,Panel_Households)
                 select 'hhcomp'
                       ,ssl.hhcomp
                       ,sum(base_accounts)
                       ,sum(Panel_households)
                   from PanBal_Scaling_Segment_Profiling as ssp
                        inner join panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
               group by ssl.hhcomp

                 insert into PanBal_all_aggregated_results(aggregation_variable
                                                          ,variable_value
                                                          ,Sky_Base_Households
                                                          ,Panel_Households)
                 select 'tenure'
                       ,ssl.tenure
                       ,sum(base_accounts)
                       ,sum(Panel_households)
                   from PanBal_Scaling_Segment_Profiling as ssp
                        inner join panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
               group by ssl.tenure

                 insert into PanBal_all_aggregated_results(aggregation_variable
                                                          ,variable_value
                                                          ,Sky_Base_Households
                                                          ,Panel_Households)
                 select 'package'
                       ,ssl.package
                       ,sum(base_accounts)
                       ,sum(Panel_households)
                   from PanBal_Scaling_Segment_Profiling as ssp
                        inner join panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
               group by ssl.package

                 insert into PanBal_all_aggregated_results(aggregation_variable
                                                          ,variable_value
                                                          ,Sky_Base_Households
                                                          ,Panel_Households)
                 select 'mr'
                       ,ssl.mr
                       ,sum(base_accounts)
                       ,sum(Panel_households)
                   from PanBal_Scaling_Segment_Profiling as ssp
                        inner join panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
               group by ssl.mr

                 insert into PanBal_all_aggregated_results(aggregation_variable
                                                          ,variable_value
                                                          ,Sky_Base_Households
                                                          ,Panel_Households)
                 select 'hd'
                       ,ssl.hd
                       ,sum(base_accounts)
                       ,sum(Panel_households)
                   from PanBal_Scaling_Segment_Profiling as ssp
                        inner join panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
               group by ssl.hd

                 insert into PanBal_all_aggregated_results(aggregation_variable
                                                          ,variable_value
                                                          ,Sky_Base_Households
                                                          ,Panel_Households)
                 select 'pvr'
                       ,ssl.pvr
                       ,sum(base_accounts)
                       ,sum(Panel_households)
                   from PanBal_Scaling_Segment_Profiling as ssp
                        inner join panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
               group by ssl.pvr

                 insert into PanBal_all_aggregated_results(aggregation_variable
                                                          ,variable_value
                                                          ,Sky_Base_Households
                                                          ,Panel_Households)
                 select 'valseg'
                       ,ssl.valseg
                       ,sum(base_accounts)
                       ,sum(Panel_households)
                   from PanBal_Scaling_Segment_Profiling as ssp
                        inner join panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
               group by ssl.valseg

                 insert into PanBal_all_aggregated_results(aggregation_variable
                                                          ,variable_value
                                                          ,Sky_Base_Households
                                                          ,Panel_Households)
                 select 'mosaic'
                       ,ssl.mosaic
                       ,sum(base_accounts)
                       ,sum(Panel_households)
                   from PanBal_Scaling_Segment_Profiling as ssp
                        inner join panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
               group by ssl.mosaic

                 insert into PanBal_all_aggregated_results(aggregation_variable
                                                          ,variable_value
                                                          ,Sky_Base_Households
                                                          ,Panel_Households)
                 select 'fss'
                       ,ssl.fss
                       ,sum(base_accounts)
                       ,sum(Panel_households)
                   from PanBal_Scaling_Segment_Profiling as ssp
                        inner join panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
               group by ssl.fss

                 insert into PanBal_all_aggregated_results(aggregation_variable
                                                          ,variable_value
                                                          ,Sky_Base_Households
                                                          ,Panel_Households)
                 select 'onnet'
                       ,ssl.onnet
                       ,sum(base_accounts)
                       ,sum(Panel_households)
                   from PanBal_Scaling_Segment_Profiling as ssp
                        inner join panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
               group by ssl.onnet

                 insert into PanBal_all_aggregated_results(aggregation_variable
                                                          ,variable_value
                                                          ,Sky_Base_Households
                                                          ,Panel_Households)
                 select 'skygo'
                       ,ssl.skygo
                       ,sum(base_accounts)
                       ,sum(Panel_households)
                   from PanBal_Scaling_Segment_Profiling as ssp
                        inner join panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
               group by ssl.skygo

                 insert into PanBal_all_aggregated_results(aggregation_variable
                                                          ,variable_value
                                                          ,Sky_Base_Households
                                                          ,Panel_Households)
                 select 'st'
                       ,ssl.st
                       ,sum(base_accounts)
                       ,sum(Panel_households)
                   from PanBal_Scaling_Segment_Profiling as ssp
                        inner join panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
               group by ssl.st

                 insert into PanBal_all_aggregated_results(aggregation_variable
                                                          ,variable_value
                                                          ,Sky_Base_Households
                                                          ,Panel_Households)
                 select 'bb'
                       ,ssl.bb
                       ,sum(base_accounts)
                       ,sum(Panel_households)
                   from PanBal_Scaling_Segment_Profiling as ssp
                        inner join panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
               group by ssl.bb

                 --if any values are Unknown, then we don't want to balance these
                 delete from PanBal_all_aggregated_results
                  where variable_value in ('Non-scalable', 'NS', 'U', 'Not Defined', 'D) Unknown', 'Unknown')
                     or sky_base_households < 1000
                     or sky_base_households is null

               truncate table #panel_households

                 insert into #panel_households(aggregation_variable
                                              ,Panel_Households
                        )
                 select aggregation_variable
                       ,sum(panel_households)
                   from PanBal_all_aggregated_results
                group by aggregation_variable

               --index value for each variable value
                 update PanBal_all_aggregated_results as bas
                    set Good_Household_Index = 100.0 * bas.Panel_households       * @total_sky_base / Sky_Base_Households / hsh.Panel_Households
                   from #panel_households as hsh
                  where bas.aggregation_variable = hsh.aggregation_variable

               --what the Good Household Index (GHI) would be if we added 1 account
                 update PanBal_all_aggregated_results as bas
                    set GHIplus1             = 100.0 * (bas.Panel_households + 1) * @total_sky_base / Sky_Base_Households / (hsh.Panel_Households + 1)
                   from #panel_households as hsh
                  where bas.aggregation_variable = hsh.aggregation_variable

               --what the Good Household Index (GHI) would be if we added 1 account to a different variable value
                 update PanBal_all_aggregated_results as bas
                    set GHIminus1            = 100.0 * bas.Panel_households       * @total_sky_base / Sky_Base_Households / (hsh.Panel_Households + 1)
                   from #panel_households as hsh
                  where bas.aggregation_variable = hsh.aggregation_variable

               --the difference (squared) between GHI and GHI+1
                 update PanBal_all_aggregated_results as bas
                    set incr_diff = ((Good_Household_Index-100) * (Good_Household_Index-100) - (GHIplus1-100) * (GHIplus1-100))

               --the difference (squared) between GHI and GHI-1
                 update PanBal_all_aggregated_results as bas
                    set decr_diff = ((Good_Household_Index-100) * (Good_Household_Index-100) - (GHIminus1-100) * (GHIminus1-100))

               truncate table PanBal_traffic_lights

                 insert into PanBal_traffic_lights
                 select aggregation_variable
                       ,sqrt(avg((Good_Household_Index - 100) * (Good_Household_Index - 100))) as imbalance_rating
                   from PanBal_all_aggregated_results
               group by aggregation_variable

                 select @imbalance = max(imbalance_rating) from PanBal_traffic_lights

               --if the panel is not balanced, then continue
               if (@imbalance >= @max_imbalance)
                    begin

                           --calculate the THIs
                           update #segment_THIs as bas
                              set thi = incr_diff
                             from PanBal_all_aggregated_results as agg
                            where bas.adsmbl = cast(agg.variable_value as bit)
                              and aggregation_variable = 'adsmbl'

                           update #segment_THIs as bas
                              set thi = thi + incr_diff
                             from PanBal_all_aggregated_results as agg
                            where bas.region = agg.variable_value
                              and aggregation_variable = 'region'

                           update #segment_THIs as bas
                              set thi = thi + incr_diff
                             from PanBal_all_aggregated_results as agg
                            where bas.hhcomp = agg.variable_value
                              and aggregation_variable = 'hhcomp'

                           update #segment_THIs as bas
                              set thi = thi + incr_diff
                             from PanBal_all_aggregated_results as agg
                            where bas.tenure = agg.variable_value
                              and aggregation_variable = 'tenure'

                           update #segment_THIs as bas
                              set thi = thi + incr_diff
                             from PanBal_all_aggregated_results as agg
                            where bas.package = agg.variable_value
                              and aggregation_variable = 'package'

                           update #segment_THIs as bas
                              set thi = thi + incr_diff
                             from PanBal_all_aggregated_results as agg
                            where bas.mr = cast(agg.variable_value as bit)
                              and aggregation_variable = 'mr'

                           update #segment_THIs as bas
                              set thi = thi + incr_diff
                             from PanBal_all_aggregated_results as agg
                            where bas.hd = cast(agg.variable_value as bit)
                              and aggregation_variable = 'hd'

                           update #segment_THIs as bas
                              set thi = thi + incr_diff
                             from PanBal_all_aggregated_results as agg
                            where bas.pvr = cast(agg.variable_value as bit)
                              and aggregation_variable = 'pvr'

                           update #segment_THIs as bas
                              set thi = thi + incr_diff
                             from PanBal_all_aggregated_results as agg
                            where bas.valseg = agg.variable_value
                              and aggregation_variable = 'valseg'

                           update #segment_THIs as bas
                              set thi = thi + incr_diff
                             from PanBal_all_aggregated_results as agg
                            where bas.mosaic = agg.variable_value
                              and aggregation_variable = 'mosaic'

                           update #segment_THIs as bas
                              set thi = thi + incr_diff
                             from PanBal_all_aggregated_results as agg
                            where bas.fss = agg.variable_value
                              and aggregation_variable = 'fss'

                           update #segment_THIs as bas
                              set thi = thi + incr_diff
                             from PanBal_all_aggregated_results as agg
                            where bas.onnet = cast(agg.variable_value as bit)
                              and aggregation_variable = 'onnet'

                           update #segment_THIs as bas
                              set thi = thi + incr_diff
                             from PanBal_all_aggregated_results as agg
                            where bas.skygo = cast(agg.variable_value as bit)
                              and aggregation_variable = 'skygo'

                           update #segment_THIs as bas
                              set thi = thi + incr_diff
                             from PanBal_all_aggregated_results as agg
                            where bas.st = cast(agg.variable_value as bit)
                              and aggregation_variable = 'st'

                           update #segment_THIs as bas
                              set thi = thi + incr_diff
                             from PanBal_all_aggregated_results as agg
                            where bas.bb = cast(agg.variable_value as bit)
                              and aggregation_variable = 'bb'

                         truncate table #descrs

                           insert into #descrs(segment_id, descrs)
                           select segment_id
                                 ,sum(decr_diff)
                             from #segment_THIs                            as bas
                                  cross join PanBal_all_aggregated_results as agg
                            where bas.adsmbl          <> cast(agg.variable_value as bit)
                              and aggregation_variable   = 'adsmbl'
                         group by segment_id

                           update #segment_THIs as bas
                              set thi = thi + descrs
                             from #descrs as dsc
                            where bas.segment_id = dsc.segment_id

                         truncate table #descrs

                           insert into #descrs(segment_id, descrs)
                           select segment_id
                                 ,sum(decr_diff)
                             from #segment_THIs                            as bas
                                  cross join PanBal_all_aggregated_results as agg
                            where bas.region          <> agg.variable_value
                              and aggregation_variable   = 'region'
                         group by segment_id

                           update #segment_THIs as bas
                              set thi = thi + descrs
                             from #descrs as dsc
                            where bas.segment_id = dsc.segment_id

                         truncate table #descrs

                           insert into #descrs(segment_id, descrs)
                           select segment_id
                                 ,sum(decr_diff)
                             from #segment_THIs                            as bas
                                  cross join PanBal_all_aggregated_results as agg
                            where bas.hhcomp          <> agg.variable_value
                              and aggregation_variable   = 'hhcomp'
                         group by segment_id

                           update #segment_THIs as bas
                              set thi = thi + descrs
                             from #descrs as dsc
                            where bas.segment_id = dsc.segment_id

                         truncate table #descrs

                           insert into #descrs(segment_id, descrs)
                           select segment_id
                                 ,sum(decr_diff)
                             from #segment_THIs                            as bas
                                  cross join PanBal_all_aggregated_results as agg
                            where bas.tenure          <> agg.variable_value
                              and aggregation_variable   = 'tenure'
                         group by segment_id

                           update #segment_THIs as bas
                              set thi = thi + descrs
                             from #descrs as dsc
                            where bas.segment_id = dsc.segment_id

                         truncate table #descrs

                           insert into #descrs(segment_id, descrs)
                           select segment_id
                                 ,sum(decr_diff)
                             from #segment_THIs                            as bas
                                  cross join PanBal_all_aggregated_results as agg
                            where bas.package          <> agg.variable_value
                              and aggregation_variable   = 'package'
                         group by segment_id

                           update #segment_THIs as bas
                              set thi = thi + descrs
                             from #descrs as dsc
                            where bas.segment_id = dsc.segment_id

                         truncate table #descrs

                           insert into #descrs(segment_id, descrs)
                           select segment_id
                                 ,sum(decr_diff)
                             from #segment_THIs                            as bas
                                  cross join PanBal_all_aggregated_results as agg
                            where bas.mr          <> cast(agg.variable_value as bit)
                              and aggregation_variable   = 'mr'
                         group by segment_id

                           update #segment_THIs as bas
                              set thi = thi + descrs
                             from #descrs as dsc
                            where bas.segment_id = dsc.segment_id

                         truncate table #descrs

                           insert into #descrs(segment_id, descrs)
                           select segment_id
                                 ,sum(decr_diff)
                             from #segment_THIs                            as bas
                                  cross join PanBal_all_aggregated_results as agg
                            where bas.hd          <> cast(agg.variable_value as bit)
                              and aggregation_variable   = 'hd'
                         group by segment_id

                           update #segment_THIs as bas
                              set thi = thi + descrs
                             from #descrs as dsc
                            where bas.segment_id = dsc.segment_id

                         truncate table #descrs

                           insert into #descrs(segment_id, descrs)
                           select segment_id
                                 ,sum(decr_diff)
                             from #segment_THIs                            as bas
                                  cross join PanBal_all_aggregated_results as agg
                            where bas.pvr          <> cast(agg.variable_value as bit)
                              and aggregation_variable   = 'pvr'
                         group by segment_id

                           update #segment_THIs as bas
                              set thi = thi + descrs
                             from #descrs as dsc
                            where bas.segment_id = dsc.segment_id

                         truncate table #descrs

                           insert into #descrs(segment_id, descrs)
                           select segment_id
                                 ,sum(decr_diff)
                             from #segment_THIs                            as bas
                                  cross join PanBal_all_aggregated_results as agg
                            where bas.valseg          <> agg.variable_value
                              and aggregation_variable   = 'valseg'
                         group by segment_id

                           update #segment_THIs as bas
                              set thi = thi + descrs
                             from #descrs as dsc
                            where bas.segment_id = dsc.segment_id

                         truncate table #descrs

                           insert into #descrs(segment_id, descrs)
                           select segment_id
                                 ,sum(decr_diff)
                             from #segment_THIs                            as bas
                                  cross join PanBal_all_aggregated_results as agg
                            where bas.mosaic          <> agg.variable_value
                              and aggregation_variable   = 'mosaic'
                         group by segment_id

                           update #segment_THIs as bas
                              set thi = thi + descrs
                             from #descrs as dsc
                            where bas.segment_id = dsc.segment_id

                         truncate table #descrs

                           insert into #descrs(segment_id, descrs)
                           select segment_id
                                 ,sum(decr_diff)
                             from #segment_THIs                            as bas
                                  cross join PanBal_all_aggregated_results as agg
                            where bas.fss          <> agg.variable_value
                              and aggregation_variable   = 'fss'
                         group by segment_id

                           update #segment_THIs as bas
                              set thi = thi + descrs
                             from #descrs as dsc
                            where bas.segment_id = dsc.segment_id

                         truncate table #descrs

                           insert into #descrs(segment_id, descrs)
                           select segment_id
                                 ,sum(decr_diff)
                             from #segment_THIs                            as bas
                                  cross join PanBal_all_aggregated_results as agg
                            where bas.onnet          <> cast(agg.variable_value as bit)
                              and aggregation_variable   = 'onnet'
                         group by segment_id

                           update #segment_THIs as bas
                              set thi = thi + descrs
                             from #descrs as dsc
                            where bas.segment_id = dsc.segment_id

                         truncate table #descrs

                           insert into #descrs(segment_id, descrs)
                           select segment_id
                                 ,sum(decr_diff)
                             from #segment_THIs                            as bas
                                  cross join PanBal_all_aggregated_results as agg
                            where bas.skygo          <> cast(agg.variable_value as bit)
                              and aggregation_variable   = 'skygo'
                         group by segment_id

                           update #segment_THIs as bas
                              set thi = thi + descrs
                             from #descrs as dsc
                            where bas.segment_id = dsc.segment_id

                         truncate table #descrs

                           insert into #descrs(segment_id, descrs)
                           select segment_id
                                 ,sum(decr_diff)
                             from #segment_THIs                            as bas
                                  cross join PanBal_all_aggregated_results as agg
                            where bas.st          <> cast(agg.variable_value as bit)
                              and aggregation_variable   = 'st'
                         group by segment_id

                           update #segment_THIs as bas
                              set thi = thi + descrs
                             from #descrs as dsc
                            where bas.segment_id = dsc.segment_id

                         truncate table #descrs

                           insert into #descrs(segment_id, descrs)
                           select segment_id
                                 ,sum(decr_diff)
                             from #segment_THIs                            as bas
                                  cross join PanBal_all_aggregated_results as agg
                            where bas.bb          <> cast(agg.variable_value as bit)
                              and aggregation_variable   = 'bb'
                         group by segment_id

                           update #segment_THIs as bas
                              set thi = thi + descrs
                             from #descrs as dsc
                            where bas.segment_id = dsc.segment_id

                         --update THI for alternate day panels
                           update PanBal_list3 as bas
                              set bas.thi = thi.thi * rq
                             from #segment_THIs as thi
                            where bas.segment_id = thi.segment_id

                         truncate table #new_adds

                           insert into #new_adds
                           select li3.account_number
                                 ,li3.segment_id
                                 ,thi
                             from panbal_list3 as li3
                                  left join panbal_panel as bas on li3.account_number = bas.account_number
                            where thi > 0
                              and bas.account_number is null

                           select @cow = count(1)
                             from #new_adds

                         if (@cow >= @precision and @continue = 0) --if there are a reasonable amount that can be added
                              begin
                                        set rowcount @precision -- only show this many lines in all queries

                                          insert into panbal_panel(
                                                 account_number
                                                ,segment_id
                                          )
                                          select account_number
                                                ,segment_id
                                            from #new_adds
                                        order by thi desc

                                        set rowcount 0 --back to normal
                              end
                         else
                              begin
                                  --update THI for waterfall list
                                    update PanBal_list4 as bas
                                       set bas.thi = thi.thi * rq
                                      from #segment_THIs as thi
                                     where bas.segment_id = thi.segment_id

                                    delete from panbal_panel where account_number in (select account_number from panbal_list3)

                                       set rowcount @precision

                                    insert into panbal_panel(account_number
                                                            ,segment_id)
                                    select li4.account_number
                                          ,li4.segment_id
                                      from panbal_list4 as li4
                                     where thi > 0
                                  order by thi desc

                                       set rowcount 0
                                    delete from panbal_list4 where account_number in (select account_number from panbal_panel)
                                        if (@continue > 0) set @continue = @continue - 1
                              end --begin
                         --end if
                    end --begin
               --end if

               select @tot_imb = sum((100-good_household_index) * (100-good_household_index)) from PanBal_all_aggregated_results

               insert into PanBal_results(imbalance, tot_imb, records, tim)
               select @continue * 100 + @imbalance, @tot_imb, count(1), now() from panbal_panel

               if (@imbalance < @max_imbalance) --it's balanced, but there still may not be enough boxes
                    begin
                         --we need at least 700k boxes dialling back per day
                         select @cow = sum((case when rq is null or rq > 1 then 1 else rq end) * boxes)
                           from panbal_panel as bas
                                inner join #sky_box_count as sbc on bas.account_number = sbc.account_number
                                left join #vespa_accounts as vac on bas.account_number = vac.account_number
                                where rq is not null

                         select @temp = sum(boxes)
                           from panbal_panel as bas
                                inner join #sky_box_count as sbc on bas.account_number = sbc.account_number
                                left join #vespa_accounts as vac on bas.account_number = vac.account_number
                          where vac.rq is null

                         select @cow = @cow + coalesce(@temp, 0)

                         select @cow = @cow + sum((case when rq is null then 1 when rq > 1 then 1 else rq end) * boxes)
                           from panbal_list1 as li1
                                inner join #sky_box_count as sbc on li1.account_number = sbc.account_number

                         if (@cow < 700000)
                              begin --add some more from the waterfall
                                  --update THI for waterfall list
                                    update PanBal_list4 as bas
                                       set bas.thi = thi.thi * rq
                                      from #segment_THIs as thi
                                     where bas.segment_id = thi.segment_id

                                       set rowcount @precision

                                    insert into panbal_panel(account_number
                                                            ,segment_id)
                                    select li4.account_number
                                          ,li4.segment_id
                                      from panbal_list4 as li4
                                     where thi > 0
                                  order by thi desc

                                       set rowcount 0

                                    delete from panbal_list4 where account_number in (select account_number from panbal_panel)

                                   set @imbalance = @max_imbalance
                              end
                         --end if
                    end --begin
               --end if
               commit
          end --begin
     --end while

     -----------------------------------
     --Section D - fill the amends table
     -----------------------------------

     truncate table PanBal_Amends

     --accounts to add to panel 12 from panels 6/7
       insert into PanBal_Amends(account_number, movement)
       select bas.account_number
             ,'Account to add to panel 12 from panel 6 or 7'
         from panbal_panel as bas
              inner join panbal_list3 as li3 on bas.account_number = li3.account_number

     --we need between 700k and 750k boxes dialling back per day
       select @cow = sum((case when rq is null or rq > 1 then 1 else rq end) * boxes)
         from panbal_panel as bas
              inner join #sky_box_count as sbc on bas.account_number = sbc.account_number
              left join #vespa_accounts as vac on bas.account_number = vac.account_number
              where rq is not null

       select @temp = sum(boxes)
         from panbal_panel as bas
              inner join #sky_box_count as sbc on bas.account_number = sbc.account_number
              left join #vespa_accounts as vac on bas.account_number = vac.account_number
        where vac.rq is null

       select @cow = @cow + coalesce(@temp, 0)

       select @cow = @cow + sum((case when rq is null then 1 when rq > 1 then 1 else rq end) * boxes)
         from panbal_list1 as li1
              left join #sky_box_count as sbc on li1.account_number = sbc.account_number

     if (@cow < 700000)
          begin
                 insert into PanBal_amends(movement)
                 select 'Not enough boxes with good rq - ' || @cow
          end
     else if (@cow > 750000)
          begin
                 insert into #list1_rq(account_number, rq, boxes)
                 select sbv.account_number
                       ,coalesce(rq, 0) as rq
                       ,count(subscriber_id) as boxes
                   from panbal_list1 as li1
                        left join #vespa_accounts as acc on li1.account_number = acc.account_number
               group by sbv.account_number
                       ,rq

               create table #temp(account_number varchar(30), boxes int)

               while (@cow > 750000)
                    begin
                               set rowcount @precision

                            insert into #temp
                            select account_number
                                  ,boxes * rq
                              from #list1_rq
                          order by rq
                                  ,account_number

                               set rowcount 0

                            select @cow = sum((case when rq > 1 then 1 else rq end) * boxes)
                              from panbal_panel as bas
                                   inner join #vespa_accounts as acc on bas.account_number = acc.account_number
                                   inner join #sky_box_count as sbc on bas.account_number = sbc.account_number

                            select @cow = @cow + sum((case when rq > 1 then 1 else reporting_quality end) * boxes)
                              from panbal_list1 as li1
                                   inner join #sky_box_count as sbc on li1.account_number = sbc.account_number

                            select @cow = @cow - sum(rq) from #temp

                            delete from #list1_rq where account_number in (select account_number from #temp)

                    end
               --end while

            insert into PanBal_amends(account_number, movement)
            select account_number
                  ,'Account to remove from panel 12'
              from #temp

          end --begin
     --end if

     --now need to remove the ones we've added fom list3 (we have already deleted the ones we've added from list4)
       delete from PanBal_list3 where account_number in (select account_number from PanBal_panel)

     --accounts to add to alt. panels, to make 50% in each segment (if poss)
       select sum(case when pan.account_number is null then 0 else 1 end)                                    as vespa
             ,sum(case when alt.account_number is not null and pan.account_number is null then 1 else 0 end) as alt
             ,bss.segment_id
         into #panels
         from panbal_segment_snapshots                      as bss
              left join panbal_panel                        as pan on bss.account_number = pan.account_number
              left join panbal_list3                        as alt on bss.account_number = alt.account_number
     group by bss.segment_id

       select segment_id
             ,vespa - (alt*2) as reqd
         into #reqd
         from #panels
        where reqd > 0

       select wat.account_number
             ,wat.segment_id
             ,rank() over (partition by wat.segment_id order by cbck_rate desc, vp1 desc) as rnk
             ,vp1
             ,boxes
         into #available
         from PanBal_list4                                              as wat
              inner join #cbck_rate                                     as cbr on wat.account_number = cbr.account_number
              inner join #sky_box_count                                 as sbc on wat.account_number = sbc.account_number
              left join vespa_analysts.vespa_broadcast_reporting_vp_map as vir on wat.account_number = vir.account_number
              left join panbal_panel                                    as bas on wat.account_number = bas.account_number
        where bas.account_number is null

       insert into PanBal_amends(account_number, movement)
       select account_number
             ,'Account to add to Panel 6 or 7 as segment backup'
         from #available        as ava
              inner join  #reqd as req on ava.segment_id = req.segment_id
        where rnk <= reqd

       select @virtuals = sum(boxes) --count boxes on the virtual panel on the new panel
         from panbal_panel                                               as bas
              inner join vespa_analysts.vespa_broadcast_reporting_vp_map as vir on bas.account_number = vir.account_number
              inner join #sky_box_count                                  as sbc on bas.account_number = sbc.account_number
        where vp1 = 1

       select @virtuals = @virtuals + sum(boxes) --add on the remaining accounts left on list1
         from panbal_list1                                               as bas
              inner join vespa_analysts.vespa_broadcast_reporting_vp_map as vir on bas.account_number = vir.account_number
              inner join #sky_box_count                                  as sbc on bas.account_number = sbc.account_number
              left join panbal_amends                                    as ame on bas.account_number = ame.account_number
        where vp1 = 1
          and ame.account_number is null
          and rq > 0

       select @virtuals = @virtuals + sum(boxes) --add on the remaining accounts left on list3
         from panbal_list3                                               as bas
              inner join vespa_analysts.vespa_broadcast_reporting_vp_map as vir on bas.account_number = vir.account_number
              inner join #sky_box_count                                  as sbc on bas.account_number = sbc.account_number
              left join panbal_amends                                    as ame on bas.account_number = ame.account_number
        where vp1 = 1
          and ame.account_number is null

     --we are still missing the alt. panel accounts with unacceptable reporting:
       select @virtuals = @virtuals + sum(boxes)
         from #vespa_accounts as bas
              inner join #sky_box_count as sbc on bas.account_number = sbc.account_number
        where panel <> 12
          and rq > 0
          and rq < @max_rq
          and segment_id is not null

     if (@virtuals < 300000) --do we need any more on the channel 4 panel?
          begin
               truncate table #list1_rq
                 insert into #list1_rq
                 select li4.account_number
                       ,rq
                       ,boxes
                   from panbal_list4                                               as li4
                        inner join #sky_box_count                                  as sbc on li4.account_number = sbc.account_number
                        inner join vespa_analysts.vespa_broadcast_reporting_vp_map as vir on li4.account_number = vir.account_number
                  where vp1 = 1

               while (@virtuals < 300000)
                    begin
                         truncate table #temp

                           insert into #temp
                           select top 1000 account_number
                                 ,boxes
                             from #list1_rq
                         order by rq,account_number

                           set @virtuals = @virtuals + (select sum(boxes) from #temp)

                           select @cow = count(1) from #list1_rq

                               if (@cow = 0) begin

                                        set @cow = 30000 - @virtuals

                                     insert into PanBal_amends(account_number, movement)
                                     select null
                                           ,@cow || ' more boxes needed on the virtual panel'

                                        set @virtuals = 300000
                              end

                           delete from #list1_rq where account_number in (select account_number from #temp)

                           insert into PanBal_amends(account_number, movement)
                           select account_number
                                 ,'Account to add to panel 6/7 for virtual panel req.'
                             from #temp

                    end
               --end while
               commit

          end --begin
     --end if

     --recreate list4
     truncate table panbal_list4

       insert into PanBal_list4
       select bas.account_number
             ,segment_id
             ,0
             ,0
         from vespa_analysts.waterfall_base                                 as bas
              inner join panbal_segment_snapshots as bss on bas.account_number = bss.account_number
        where knockout_level = 9999
     group by bas.account_number
             ,segment_id

     --New accounts to add to alternate day panels
       insert into PanBal_Amends(account_number, movement)
       select bas.account_number
             ,'Account to add to panels 6/7, eventually for panel 12'
         from panbal_panel as bas
              inner join panbal_list4 as li4 on bas.account_number = li4.account_number

--        insert into PanBal_Changes(
--               date_of_change
--              ,Alt_to_Daily
--              ,New_to_Alt
--              ,New_to_Daily_via_Alt
--              ,Daily_to_Alt
--        )
--        select now()
--              ,sum(case when movement = 'Account to add to panel 12 from panel 6 or 7'          then 1 else 0 end)
--              ,sum(case when movement = 'Account to add to Panel 6 or 7 as segment backup'      then 1 else 0 end)
--              ,sum(case when movement = 'Account to add to panels 6/7, eventually for panel 12' then 1 else 0 end)
--              ,sum(case when movement = 'Account to remove from panel 12'                       then 1 else 0 end)
--          from PanBal_amends

end; --procedure

     --drop tables
drop procedure PanBal_drop_tables;
create procedure PanBal_drop_tables as begin
     drop table panbal_list1
     drop table panbal_list2
     drop table panbal_list3
     drop table panbal_list4
     drop table PanBal_all_aggregated_results
     drop table PanBal_Scaling_Segment_Profiling
     drop table PanBal_traffic_lights
     drop table PanBal_results
     drop table PanBal_panel
     drop table Panbal_Amends
end; --procedure




---


