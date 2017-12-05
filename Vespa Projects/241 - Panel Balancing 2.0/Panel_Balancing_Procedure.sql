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
  fill #PanBal_all_aggregated_results
  Update Good_Household_Index, +1, -1 and incr_diff and decr_diff
  Fill #PanBal_traffic_lights
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
  Drop tables excep panbal_amends

The values for variables
@max_imbalance
@max_rq
@precision
are customisable

when you are happy with the results then run the procedure PanBal_drop_tables
This will drop all tables except panbal_amends, which lists all of the enablements and disablements that need to happen, as well as any warnings e.g. not enough accounts
*/

drop procedure panbal;
create procedure PanBal as begin

                   --------------------
                   -- Section A - Setup
                   --------------------

                -- prerequisites: (in this order)
                -- execute waterfall
                -- execute VirtPan
                -- execute panbal_segmentation
                -- panbal_sav

          truncate table panbal_progress
                -- create tables
            create table PanBal_all_aggregated_results (
                   aggregation_variable                               varchar(30) default null
                  ,variable_value                                     varchar(60) default null
                  ,Sky_Base_Households                                int
                  ,Panel_Households                                   decimal(10,2)
                  ,Good_Household_Index                               double default 0
                  ,GHIplus1                                           double default 0
                  ,GHIminus1                                          double default 0
                  ,incr_diff                                          double default 0
                  ,decr_diff                                          double default 0
                  ,primary key (aggregation_variable, variable_value)
            )

            create table PanBal_traffic_lights(
                   variable_name                                      varchar(30)
                  ,imbalance_rating                                   decimal(10,2)
            )

            create table panbal_results (
                   imbalance                                          double --the highest variable
                  ,tot_imb                                            double --total across all variables
                  ,records                                            int    --in the panel
                  ,tim                                                datetime
            )
            grant select on Panbal_results to vespa_group_low_security

            create table panbal_amends(
                   account_number                                     varchar(30)
                  ,movement                                           varchar(60)
            )
             grant select on panbal_amends to vespa_group_low_security

            create table PanBal_bestsofar(
                   account_number                                     varchar(30)
                  ,segment_id                                         int
            )
             grant select on PanBal_bestsofar to vespa_group_low_security

            create table #PanBal_all_aggregated_results (
                   aggregation_variable                               varchar(30) default null
                  ,variable_value                                     varchar(60) default null
                  ,Sky_Base_Households                                int
                  ,Panel_Households                                   decimal(10,2)
                  ,Good_Household_Index                               double default 0
                  ,GHIplus1                                           double default 0
                  ,GHIminus1                                          double default 0
                  ,incr_diff                                          double default 0
                  ,decr_diff                                          double default 0
                  ,primary key (aggregation_variable, variable_value)
            )

            create table #panel_households(
                   aggregation_variable                               varchar(30)
                  ,Panel_Households                                   decimal(10,2)
                  ,sky_base_households                                int
            )

            create table #panbal_list2(
                   account_number                                     varchar(30)
                  ,segment_id                                         int
                  ,rq                                                 double
            )

            create table #panbal_list3(
                   account_number                                     varchar(30)
                  ,segment_id                                         int
                  ,rq                                                 double
                  ,thi                                                double
            )

            create table #panbal_list4(
                   account_number                                     varchar(30)
                  ,segment_id                                         int
                  ,rq                                                 double
                  ,thi                                                double
            )

            create table #PanBal_panel(
                 account_number                                       varchar(30)
                ,segment_id                                           int
            )

            create table #PanBal_Scaling_Segment_Profiling (
                   segment_id                                         int
                  ,Sky_Base_Households                                int
                  ,Panel_households                                   decimal(10,2)
                  ,primary key (segment_id)
            )

            create table #PanBal_traffic_lights(
                   variable_name                                      varchar(30)
                  ,imbalance_rating                                   decimal(6,2)
            )

            create table #descrs(
                   segment_id                                         int
                  ,descrs                                             double
            )

            create table #panel_segmentation(
                   segment_id                                         int
                  ,Panel_Households                                   decimal(10,2)
            )

            create table #new_adds(
                   account_number                                     varchar(50)
                  ,segment_id                                         int
                  ,thi                                                double
            )

            create table #segment_THIs(segment_id int
                                      ,adsmbl             varchar(30) default null
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
                                      ,bb_capable         varchar(3)
                                      ,thi                double default 0
            )

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
                ,segment_id                                         int         default 0
            )

            commit
            create hg index idx1 on #segment_THIs(thi)
            create lf index idx2 on #segment_THIs(region)
            create lf index idx3 on #segment_THIs(hhcomp)
            create lf index idx4 on #segment_THIs(tenure)
            create lf index idx5 on #segment_THIs(package)
            create lf index idx6 on #segment_THIs(valseg)
            create lf index idx7 on #segment_THIs(mosaic)
            create lf index idx8 on #segment_THIs(fss)
            create hg index idx1 on #PanBal_panel(segment_id)
            create hg index idx2 on #PanBal_panel(account_number)
            create unique hg index fake_pk on #panel_segmentation (segment_id)
            create hg index idx1 on #new_adds(thi)
            create hg index idx9 on #segment_THIs(segment_id)
            create lf index idx1 on #PanBal_all_aggregated_results(variable_value)
            create lf index idx2 on #PanBal_all_aggregated_results(aggregation_variable)
            create hg index idx1 on #panbal_list3(segment_id)
            create hg index idx2 on #panbal_list3(thi)
            create hg index idx1 on #panbal_list4(thi)
            create hg index idx3 on #panbal_list4(segment_id)
            create hg index idx4 on #panbal_list4(account_number)
            create unique hg index idx1 on #vespa_accounts(account_number)

                -- create variables
           declare @total_sky_base                        int
           declare @panel_reporters                       int
           declare @cow                                   int
           declare @accounts_remaining                    int
           declare @temp                                  int
           declare @imbalance                             double
           declare @prev_imbalance                        double
           declare @tot_imb                               double
           declare @precision                             int
           declare @max_imbalance                         int
           declare @prev_tot_imb                          int
           declare @max_rq                                double
           declare @virtuals                              int
           declare @continue                              int
           declare @ta                                    double
           declare @now                                   datetime
           declare @minimum                               int
           declare @shortrun int
       /*
                -- for running outside of the procedure
            create hg index idx1 on #segment_THIs(thi);
            create lf index idx2 on #segment_THIs(region);
            create lf index idx3 on #segment_THIs(hhcomp);
            create lf index idx4 on #segment_THIs(tenure);
            create lf index idx5 on #segment_THIs(package);
            create lf index idx6 on #segment_THIs(valseg);
            create lf index idx7 on #segment_THIs(mosaic);
            create lf index idx8 on #segment_THIs(fss);
            create hg index idx1 on #PanBal_panel(segment_id);
            create hg index idx2 on #PanBal_panel(account_number);
            create unique hg index fake_pk on #panel_segmentation (segment_id);
            create hg index idx1 on #new_adds(thi);
            create hg index idx9 on #segment_THIs(segment_id);
            create lf index idx1 on #PanBal_all_aggregated_results(variable_value);
            create lf index idx2 on #PanBal_all_aggregated_results(aggregation_variable);
            create hg index idx1 on #panbal_list3(segment_id);
            create hg index idx2 on #panbal_list3(thi);
            create hg index idx1 on #panbal_list4(thi);
            create hg index idx3 on #panbal_list4(segment_id);
            create hg index idx4 on #panbal_list4(account_number);
            create unique hg index idx1 on #vespa_accounts(account_number);
            create variable @total_sky_base                        int;
            create variable @panel_reporters                       int;
            create variable @cow                                   int;
            create variable @accounts_remaining                    int;
            create variable @temp                                  int;
            create variable @imbalance                             double;
            create variable @prev_imbalance                        double;
            create variable @tot_imb                               double;
            create variable @precision                             int;
            create variable @max_imbalance                         int;
            create variable @prev_tot_imb                          int;
            create variable @max_rq                                double;
            create variable @virtuals                              int;
            create variable @continue                              int;
            create variable @ta                                    double;
            create variable @now                                   datetime;
            create variable @minimum                               int;
            create variable @shortrun int;
       */

                -- set starting variables
               set @imbalance = 100
               set @prev_imbalance = 200
               set rowcount 0 -- just in case it's been set to something

                -- these 3 variables can be changed as required
               set @max_imbalance = 15 -- needs to be less than 20 to get a green light
               set @max_rq = 0.8       -- reporting quality should aim be at least 0.8
               set @precision = 0      -- this defines how accurately we try to find a solution - ideally it will be 1, but the lower the number,
                                       --the longer the code will take to run.
               set @continue = 0       -- if the code was interrupted, then this is the number of times the main loop will be skipped, and records added directly from list 4
               set @tot_imb = 100000
               set @prev_tot_imb = 1000000
               set @accounts_remaining = 1000000
               set @now = now()
               set @cow = 0           -- the count of boxes on the daily panels
               set @minimum = 900000  -- minimum number of boxes required to return per day
               set @shortrun =1

                   --------------------------------------
                   -- Section B - Fill the accounts lists
                   --------------------------------------
            insert into #vespa_accounts(account_number
                                       ,panel
                                       ,rq)
            select sav.account_number
                  ,panel
                  ,min(rq)
              from vespa_analysts.panbal_sav as sav
             where panel is not null
          group by sav.account_number
                  ,panel

                -- check for any accounts missing from panbal_weekly_sampla
            delete from #vespa_accounts where account_number in (
                      select ves.account_number
                        from #vespa_accounts as ves
                             left join greenj.PanBal_segment_snapshots as snp on ves.account_number = snp.account_number
                       where snp.account_number is null
                 )

            update #vespa_accounts as bas
               set bas.segment_id = lkp.segment_id
              from greenj.PanBal_segment_snapshots as lkp
             where bas.account_number = lkp.account_number

            delete from #vespa_accounts
             where segment_id is null

                -- List 2 daily panel accounts
            insert into #panbal_list2(account_number
                                    ,rq
                                    ,segment_id
                                    )
            select account_number
                  ,rq
                  ,segment_id
              from #vespa_accounts
             where panel in (11, 12)

                -- List 3 - alternate day panel accounts
            insert into #panbal_list3
             select account_number
                  ,segment_id
                  ,rq
                  ,0
              from #vespa_accounts as vac
             where panel in (5, 6, 7)
               and vac.rq >= @max_rq

                -- List 4 - waterfall accounts
            insert into #panbal_list4(
                   account_number
                  ,segment_id
                  ,rq
                  ,thi
            )
            select bas.account_number
                  ,segment_id
                  ,1
                  ,0
              from vespa_analysts.panbal_sav           as bas
             where panel is null
          group by bas.account_number
                  ,segment_id

                -- check for any accounts missing from panbal_weekly_sampla
            delete from #panbal_list4 where account_number in (
                      select ves.account_number
                        from #panbal_list4 as ves
                             left join panbal_weekly_sample as snp on ves.account_number = snp.account_number
                       where snp.account_number is null
                 )

            select account_number
                  ,sum((expected_cbcks - missing_cbcks) / (1.0* expected_cbcks)) as cbck_rate
              into #cbck_rate
              from vespa_analysts.Waterfall_scms_callback_data
             where expected_cbcks > 0
          group by account_number

            commit
            create unique hg index idx1 on #cbck_rate(account_number)

            update #panbal_list4 as bas
               set bas.rq = cbc.cbck_rate
              from #cbck_rate as cbc
             where bas.account_number = cbc.account_number

            select account_number
                  ,segment_id
              into #Scaling_weekly_sample
              from greenj.PanBal_segment_snapshots

            insert into #PanBal_Scaling_Segment_Profiling (
                   segment_id
                  ,Sky_Base_Households
            )
            select segment_id
                  ,count(1) as Sky_Base_Households
              from #Scaling_weekly_sample as sws
          group by sws.segment_id

            select @total_sky_base = sum(Sky_Base_Households) from #PanBal_Scaling_Segment_Profiling

                -- start the panel off with just the acceptable daily panel accounts
            insert into #PanBal_panel(account_number
                                    ,segment_id
                  )
            select account_number
                  ,segment_id
              from #panbal_list2

                -- we only need to work out THIs once for each segment that we have any accounts in list3 or list4
            insert into #segment_THIs(segment_id)
            select distinct(segment_id)
              from (select distinct(segment_id) as segment_id
                      from #panbal_list3
                     union
                    select distinct(segment_id)
                      from #panbal_list4) as sub

            update #segment_THIs as bas
               set bas.adsmbl   = seg.adsmbl
                  ,bas.region   = seg.region
                  ,bas.hhcomp   = seg.hhcomp
                  ,bas.tenure   = seg.tenure
                  ,bas.package  = seg.package
                  ,bas.mr       = seg.mr
                  ,bas.hd       = seg.hd
                  ,bas.pvr      = seg.pvr
                  ,bas.valseg   = seg.valseg
                  ,bas.mosaic   = seg.mosaic
                  ,bas.fss      = seg.fss
                  ,bas.onnet    = seg.onnet
                  ,bas.skygo    = seg.skygo
                  ,bas.st       = seg.st
                  ,bas.bb       = seg.bb
                  ,bas.bb_capable = seg.bb_capable
              from greenj.panbal_segments_lookup as seg
             where bas.segment_id = seg.segment_id

            update #PanBal_Scaling_Segment_Profiling as bas
               set Panel_Households             = 0

                   ----------------------------
                   -- Section C - The Main Loop
                   ----------------------------
             while ((@imbalance >= @max_imbalance or @cow < @minimum) and @shortrun > 0) begin

    ---------------------------------------------------------------
    -- Comment this incrementor out to trigger a full balancing run
     set @shortrun = @shortrun - 1
    ---------------------------------------------------------------


                    truncate table #panel_segmentation

                          -- reqd for traffic lights
                      insert into #panel_segmentation(segment_id
                                                     ,Panel_Households)
                      select bas.segment_id
                            ,sum(case when sav.rq is null then case when cbck_rate is null then 1
                                                                    else cbck_rate
                                                               end
                                      when sav.rq > 1 then 1
                                      else sav.rq
                                 end)
                        from #PanBal_panel as bas
                             inner join vespa_analysts.panbal_sav as sav on bas.account_number = sav.account_number
                    group by bas.segment_id

                      update #PanBal_Scaling_Segment_Profiling as bas
                         set Panel_Households     = 0

                      update #PanBal_Scaling_Segment_Profiling as bas
                         set Panel_Households     = seg.Panel_Households
                        from #panel_segmentation as seg
                       where bas.segment_id       = seg.segment_id

                    truncate table #PanBal_all_aggregated_results

                          -- create the traffic light report
                      insert into #PanBal_all_aggregated_results(aggregation_variable
                                                                ,variable_value
                                                                ,Sky_Base_Households
                                                                ,Panel_Households)
                      select 'adsmbl' -- Name of variable being profiled
                            ,cast(ssl.adsmbl as varchar)
                            ,sum(sky_base_households)
                            ,sum(Panel_households)
                        from #PanBal_Scaling_Segment_Profiling as ssp
                             inner join greenj.panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
                    group by ssl.adsmbl
                       union
                      select 'region'
                            ,cast(ssl.region as varchar)
                            ,sum(sky_base_households)
                            ,sum(Panel_households)
                        from #PanBal_Scaling_Segment_Profiling as ssp
                             inner join greenj.panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
                    group by ssl.region
                       union
                      select 'hhcomp'
                            ,cast(ssl.hhcomp as varchar)
                            ,sum(sky_base_households)
                            ,sum(Panel_households)
                        from #PanBal_Scaling_Segment_Profiling as ssp
                             inner join greenj.panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
                    group by ssl.hhcomp
                       union
                      select 'tenure'
                            ,cast(ssl.tenure as varchar)
                            ,sum(sky_base_households)
                            ,sum(Panel_households)
                        from #PanBal_Scaling_Segment_Profiling as ssp
                             inner join greenj.panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
                    group by ssl.tenure
                       union
                      select 'package'
                            ,cast(ssl.package as varchar)
                            ,sum(sky_base_households)
                            ,sum(Panel_households)
                        from #PanBal_Scaling_Segment_Profiling as ssp
                             inner join greenj.panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
                    group by ssl.package
                       union
                      select 'mr'
                            ,cast(ssl.mr as varchar)
                            ,sum(sky_base_households)
                            ,sum(Panel_households)
                        from #PanBal_Scaling_Segment_Profiling as ssp
                             inner join greenj.panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
                    group by ssl.mr
                       union
                      select 'hd'
                            ,cast(ssl.hd as varchar)
                            ,sum(sky_base_households)
                            ,sum(Panel_households)
                        from #PanBal_Scaling_Segment_Profiling as ssp
                             inner join greenj.panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
                    group by ssl.hd
                       union
                      select 'pvr'
                            ,cast(ssl.pvr as varchar)
                            ,sum(sky_base_households)
                            ,sum(Panel_households)
                        from #PanBal_Scaling_Segment_Profiling as ssp
                             inner join greenj.panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
                    group by ssl.pvr
                       union
                      select 'valseg'
                            ,cast(ssl.valseg as varchar)
                            ,sum(sky_base_households)
                            ,sum(Panel_households)
                        from #PanBal_Scaling_Segment_Profiling as ssp
                             inner join greenj.panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
                    group by ssl.valseg
                       union
                      select 'mosaic'
                            ,cast(ssl.mosaic as varchar)
                            ,sum(sky_base_households)
                            ,sum(Panel_households)
                        from #PanBal_Scaling_Segment_Profiling as ssp
                             inner join greenj.panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
                    group by ssl.mosaic
                       union
                      select 'fss'
                            ,cast(ssl.fss as varchar)
                            ,sum(sky_base_households)
                            ,sum(Panel_households)
                        from #PanBal_Scaling_Segment_Profiling as ssp
                             inner join greenj.panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
                    group by ssl.fss
                       union
                      select 'onnet'
                            ,cast(ssl.onnet as varchar)
                            ,sum(sky_base_households)
                            ,sum(Panel_households)
                        from #PanBal_Scaling_Segment_Profiling as ssp
                             inner join greenj.panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
                    group by ssl.onnet
                       union
                      select 'skygo'
                            ,cast(ssl.skygo as varchar)
                            ,sum(sky_base_households)
                            ,sum(Panel_households)
                        from #PanBal_Scaling_Segment_Profiling as ssp
                             inner join greenj.panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
                    group by ssl.skygo
                       union
                      select 'st'
                            ,cast(ssl.st as varchar)
                            ,sum(sky_base_households)
                            ,sum(Panel_households)
                        from #PanBal_Scaling_Segment_Profiling as ssp
                             inner join greenj.panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
                    group by ssl.st
                       union
                      select 'bb'
                            ,cast(ssl.bb as varchar)
                            ,sum(sky_base_households)
                            ,sum(Panel_households)
                        from #PanBal_Scaling_Segment_Profiling as ssp
                             inner join greenj.panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
                    group by ssl.bb
                        union
                       select 'bb_capable'
                             ,cast(ssl.bb_capable as varchar)
                            ,sum(sky_base_households)
                             ,sum(Panel_households)
                         from #PanBal_Scaling_Segment_Profiling as ssp
                              inner join greenj.panbal_segments_lookup as ssl on ssp.segment_id = ssl.segment_id
                     group by ssl.bb_capable

                          -- if any values are Unknown, then we don't want to balance these
                      delete from #PanBal_all_aggregated_results
                       where variable_value in ('Non-scalable', 'NS', 'U', 'Not Defined', 'D) Unknown', 'Unknown', 'No Panel')
                          or sky_base_households < 1000
                          or sky_base_households is null

                    truncate table #panel_households

                      insert into #panel_households(aggregation_variable
                                                   ,Panel_Households
                                                   ,sky_base_households
                             )
                      select aggregation_variable
                            ,sum(panel_households)
                            ,sum(sky_base_households)
                        from #PanBal_all_aggregated_results
                    group by aggregation_variable

                      update #PanBal_all_aggregated_results as bas
                         set Good_Household_Index = 100.0 *  bas.Panel_households      * hsh.Sky_Base_Households / bas.Sky_Base_Households /  hsh.Panel_Households      --index value for each variable value
                            ,GHIplus1             = 100.0 * (bas.Panel_households + 1) * hsh.Sky_Base_Households / bas.Sky_Base_Households / (hsh.Panel_Households + 1) --what the Good Household Index (GHI) would be if we added 1 account
                            ,GHIminus1            = 100.0 *  bas.Panel_households      * hsh.Sky_Base_Households / bas.Sky_Base_Households / (hsh.Panel_Households + 1) --what the Good Household Index (GHI) would be if we added 1 account to a different variable value
                        from #panel_households as hsh
                       where bas.aggregation_variable = hsh.aggregation_variable

                          -- the difference (squared) between GHI and GHI+1
                      update #PanBal_all_aggregated_results as bas
                         set incr_diff = ((Good_Household_Index-100) * (Good_Household_Index-100) - (GHIplus1-100) * (GHIplus1-100))

                          -- the difference (squared) between GHI and GHI-1
                      update #PanBal_all_aggregated_results as bas
                         set decr_diff = ((Good_Household_Index-100) * (Good_Household_Index-100) - (GHIminus1-100) * (GHIminus1-100))

                    truncate table #PanBal_traffic_lights

                      insert into #PanBal_traffic_lights
                      select aggregation_variable
                            ,sqrt(avg((Good_Household_Index - 100) * (Good_Household_Index - 100))) as imbalance_rating
                        from #PanBal_all_aggregated_results
                    group by aggregation_variable

                      select @imbalance = max(imbalance_rating) from #PanBal_traffic_lights

                          -- if we are short of the number of boxes required then we will need to continue
                      select @cow = sum(case when rq is null then case when cbck_rate is null then 1 else cbck_rate end when rq > 1 then 1 else rq end * boxes)
                        from #PanBal_panel as bas
                             inner join vespa_analysts.panbal_sav as sav on bas.account_number = sav.account_number

                          -- if the panel is not balanced, then continue
                          if (@imbalance >= @max_imbalance or @cow < @minimum) begin

                                    -- calculate the THIs
                                update #segment_THIs as bas
                                   set thi = incr_diff
                                  from #PanBal_all_aggregated_results as agg
                                 where bas.adsmbl = agg.variable_value
                                   and aggregation_variable = 'adsmbl'

                                update #segment_THIs as bas
                                   set thi = thi + incr_diff
                                  from #PanBal_all_aggregated_results as agg
                                 where bas.region = agg.variable_value
                                   and aggregation_variable = 'region'

                                update #segment_THIs as bas
                                   set thi = thi + incr_diff
                                  from #PanBal_all_aggregated_results as agg
                                 where bas.hhcomp = agg.variable_value
                                   and aggregation_variable = 'hhcomp'

                                update #segment_THIs as bas
                                   set thi = thi + incr_diff
                                  from #PanBal_all_aggregated_results as agg
                                 where bas.tenure = agg.variable_value
                                   and aggregation_variable = 'tenure'

                                update #segment_THIs as bas
                                   set thi = thi + incr_diff
                                  from #PanBal_all_aggregated_results as agg
                                 where bas.package = agg.variable_value
                                   and aggregation_variable = 'package'

                                update #segment_THIs as bas
                                   set thi = thi + incr_diff
                                  from #PanBal_all_aggregated_results as agg
                                 where bas.mr = cast(agg.variable_value as bit)
                                   and aggregation_variable = 'mr'

                                update #segment_THIs as bas
                                   set thi = thi + incr_diff
                                  from #PanBal_all_aggregated_results as agg
                                 where bas.hd = cast(agg.variable_value as bit)
                                   and aggregation_variable = 'hd'

                                update #segment_THIs as bas
                                   set thi = thi + incr_diff
                                  from #PanBal_all_aggregated_results as agg
                                 where bas.pvr = cast(agg.variable_value as bit)
                                   and aggregation_variable = 'pvr'

                                update #segment_THIs as bas
                                   set thi = thi + incr_diff
                                  from #PanBal_all_aggregated_results as agg
                                 where bas.valseg = agg.variable_value
                                   and aggregation_variable = 'valseg'

                                update #segment_THIs as bas
                                   set thi = thi + incr_diff
                                  from #PanBal_all_aggregated_results as agg
                                 where bas.mosaic = agg.variable_value
                                   and aggregation_variable = 'mosaic'

                                update #segment_THIs as bas
                                   set thi = thi + incr_diff
                                  from #PanBal_all_aggregated_results as agg
                                 where bas.fss = agg.variable_value
                                   and aggregation_variable = 'fss'

                                update #segment_THIs as bas
                                   set thi = thi + incr_diff
                                  from #PanBal_all_aggregated_results as agg
                                 where bas.onnet = cast(agg.variable_value as bit)
                                   and aggregation_variable = 'onnet'

                                update #segment_THIs as bas
                                   set thi = thi + incr_diff
                                  from #PanBal_all_aggregated_results as agg
                                 where bas.skygo = cast(agg.variable_value as bit)
                                   and aggregation_variable = 'skygo'

                                update #segment_THIs as bas
                                   set thi = thi + incr_diff
                                  from #PanBal_all_aggregated_results as agg
                                 where bas.st = cast(agg.variable_value as bit)
                                   and aggregation_variable = 'st'

                                update #segment_THIs as bas
                                   set thi = thi + incr_diff
                                  from #PanBal_all_aggregated_results as agg
                                 where bas.bb = cast(agg.variable_value as bit)
                                   and aggregation_variable = 'bb'

                                update #segment_THIs as bas
                                   set thi = thi + incr_diff
                                  from #PanBal_all_aggregated_results as agg
                                 where bas.bb_capable = agg.variable_value
                                   and aggregation_variable = 'bb_capable'

                              truncate table #descrs

                                insert into #descrs(segment_id, descrs)
                                select segment_id
                                      ,sum(decr_diff)
                                  from #segment_THIs                            as bas
                                       cross join #PanBal_all_aggregated_results as agg
                                 where bas.adsmbl          <> agg.variable_value
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
                                       cross join #PanBal_all_aggregated_results as agg
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
                                       cross join #PanBal_all_aggregated_results as agg
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
                                       cross join #PanBal_all_aggregated_results as agg
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
                                       cross join #PanBal_all_aggregated_results as agg
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
                                       cross join #PanBal_all_aggregated_results as agg
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
                                       cross join #PanBal_all_aggregated_results as agg
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
                                       cross join #PanBal_all_aggregated_results as agg
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
                                       cross join #PanBal_all_aggregated_results as agg
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
                                       cross join #PanBal_all_aggregated_results as agg
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
                                       cross join #PanBal_all_aggregated_results as agg
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
                                       cross join #PanBal_all_aggregated_results as agg
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
                                       cross join #PanBal_all_aggregated_results as agg
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
                                       cross join #PanBal_all_aggregated_results as agg
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
                                       cross join #PanBal_all_aggregated_results as agg
                                 where bas.bb          <> cast(agg.variable_value as bit)
                                   and aggregation_variable   = 'bb'
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
                                       cross join #PanBal_all_aggregated_results as agg
                                 where bas.bb_capable          <> agg.variable_value
                                   and aggregation_variable   = 'bb_capable'
                              group by segment_id

                                update #segment_THIs as bas
                                   set thi = thi + descrs
                                  from #descrs as dsc
                                 where bas.segment_id = dsc.segment_id

                                    -- update THI for alternate day panels
                                update #panbal_list3 as bas
                                   set bas.thi = thi.thi * rq
                                  from #segment_THIs as thi
                                 where bas.segment_id = thi.segment_id

                              truncate table #new_adds

                                insert into #new_adds
                                select li3.account_number
                                      ,li3.segment_id
                                      ,thi
                                  from #panbal_list3           as li3
                                       left join #PanBal_panel as bas on li3.account_number = bas.account_number
                                 where thi > 0
                                   and bas.account_number is null

                                select @accounts_remaining = count(1)
                                  from #new_adds

                                    if (@accounts_remaining >= @precision and @continue = 0 and @tot_imb + 20 <= @prev_tot_imb and ((@imbalance - (@prev_imbalance - @imbalance) * (@accounts_remaining / @precision)) < @max_imbalance)) begin --if there are a reasonable amount that can be added, and the target can be reached

                                             set rowcount @precision -- only show this many lines in all queries

                                          insert into #PanBal_panel(
                                                 account_number
                                                ,segment_id
                                          )
                                          select account_number
                                                ,segment_id
                                            from #new_adds
                                        order by thi desc

                                             set rowcount 0 --back to normal
                                             set @prev_imbalance = @imbalance

                                   end
                                  else
                                 begin
                                              -- save best results so far, before overwriting them
                                        truncate table panbal_bestsofar

                                          insert into panbal_bestsofar
                                          select account_number
                                                ,segment_id
                                            from #panbal_panel

                                              -- update THI for waterfall list
                                          update #panbal_list4 as bas
                                             set bas.thi = thi.thi * rq
                                            from #segment_THIs as thi
                                           where bas.segment_id = thi.segment_id

                                              -- remove all the alt.day accounts from the panel
                                          delete from #PanBal_panel where account_number in (select account_number from #panbal_list3)

                                             set rowcount @precision

                                              -- add some from the waterfall
                                          insert into #PanBal_panel(account_number
                                                                   ,segment_id)
                                          select li4.account_number
                                                ,li4.segment_id
                                            from #panbal_list4 as li4
                                           where thi > 0
                                        order by thi desc

                                             set rowcount 0

                                              -- remove the ones we have just added from the waterfall list
                                          delete from #panbal_list4 where account_number in (select account_number from #PanBal_panel)

                                              if (@continue > 0) set @continue = @continue - 1
                                             set @prev_imbalance = 100
                                   end --if

                                select @accounts_remaining = @accounts_remaining + count(account_number) from #panbal_list4

                         end --if

                         set @prev_tot_imb = @tot_imb
                      select @tot_imb = sum((100-good_household_index) * (100-good_household_index)) from #PanBal_all_aggregated_results

                      insert into panbal_results(imbalance
                                                ,tot_imb
                                                ,records
                                                ,tim)
                      select (@continue * 100) + @imbalance
                            ,@tot_imb
                            ,count(1)
                            ,now()
                        from #PanBal_panel

                      insert into panbal_progress(variable_name
                                                 ,imbalance_rating)
                      select variable_name
                            ,imbalance_rating
                        from #panbal_traffic_lights

                      commit
               end --while

                   ----------------------------------------------------------
                   -- Section D - fill the amends table and additional checks
                   ----------------------------------------------------------

                -- accounts to add to panel 12 from alt. panels
            insert into panbal_amends(account_number, movement)
            select bas.account_number
                  ,'Account to add to panel 11/ 12 from panel 6 or 7'
              from #PanBal_panel as bas
                   inner join #panbal_list3 as li3 on bas.account_number = li3.account_number

                -- we need between 900k and 1M boxes dialling back per day
            select @cow = sum(case when rq is null then case when cbck_rate is null then 1 else cbck_rate end when rq > 1 then 1 else rq end) * boxes
              from #PanBal_panel as bas
                   inner join vespa_analysts.panbal_sav as sav on bas.account_number = sav.account_number

                if (@cow > 1000000) begin

                       while (@cow > 1000000) begin
                              truncate table #temp

                                   set rowcount @precision

                                insert into #temp
                                select pan.account_number
                                      ,boxes * case when sav.rq is null then case when cbck_rate is null then 1 else cbck_rate end when sav.rq > 1 then 1 else sav.rq end
                                  from #panbal_panel as pan
                                       inner join vespa_analysts.panbal_sav as sav on pan.account_number = sav.account_number
                              order by sav.rq
                                      ,pan.account_number

                                   set rowcount 0

                                delete from #panbal_panel where account_number in (select account_number from #temp)

                               select @cow = sum(case when rq is null then case when cbck_rate is null then 1 else cbck_rate end when rq > 1 then 1 else rq end) * boxes
                                 from #PanBal_panel as bas
                                      inner join vespa_analysts.panbal_sav as sav on bas.account_number = sav.account_number

                                insert into panbal_amends(account_number, movement)
                                select account_number
                                      ,'Account to remove from panel 11/ 12'
                                  from #temp

                         end --while
               end --if

                -- now need to remove the ones we've added fom list3 (we have already deleted the ones we've added from list4)
            delete from #panbal_list3 where account_number in (select account_number from #PanBal_panel)

                -- accounts to add to alt. panels, to make 50% in each segment (if poss)
            select sum(case when pan.account_number is null then 0 else 1 end)                                    as vespa
                  ,sum(case when alt.account_number is not null and pan.account_number is null then 1 else 0 end) as alt
                  ,bss.segment_id
              into #panels
              from greenj.PanBal_segment_snapshots                       as bss
                   left join #PanBal_panel                        as pan on bss.account_number = pan.account_number
                   left join #panbal_list3                        as alt on bss.account_number = alt.account_number
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
              from #panbal_list4                                             as wat
                   inner join vespa_analysts.panbal_sav                      as sav on wat.account_number = sav.account_number
                   left join vespa_analysts.vespa_broadcast_reporting_vp_map as vir on wat.account_number = vir.account_number
                   left join #PanBal_panel                                   as bas on wat.account_number = bas.account_number
             where bas.account_number is null

            insert into panbal_amends(account_number, movement)
            select account_number
                  ,'Account to add to Panel 6 or 7 as segment backup'
              from #available        as ava
                   inner join  #reqd as req on ava.segment_id = req.segment_id
             where rnk <= reqd

            select @virtuals = sum(boxes) --count boxes on the virtual panel on the new panel
              from #PanBal_panel                                              as bas
                   inner join vespa_analysts.vespa_broadcast_reporting_vp_map as vir on bas.account_number = vir.account_number
                   inner join vespa_analysts.panbal_sav                       as sav on bas.account_number = sav.account_number
             where vp1 = 1

            select @virtuals = @virtuals + sum(boxes) --add on the remaining accounts left on list3
              from #panbal_list3                                              as bas
                   inner join vespa_analysts.vespa_broadcast_reporting_vp_map as vir on bas.account_number = vir.account_number
                   inner join vespa_analysts.panbal_sav                       as sav on bas.account_number = sav.account_number
                   left join panbal_amends                                    as ame on bas.account_number = ame.account_number
             where vp1 = 1
               and ame.account_number is null

                -- we are still missing the alt. panel accounts with unacceptable reporting:
            select @virtuals = @virtuals + sum(boxes)
              from vespa_analysts.panbal_sav
             where panel <> 12
               and rq > 0
               and rq < @max_rq
               and segment_id is not null

                if (@virtuals < 300000) begin --do we need any more on the channel 4 panel?

                    truncate table #list1_rq
                      insert into #list1_rq
                      select li4.account_number
                            ,case when rq is null then case when cbck_rate is null then 1 else cbck_rate end when rq > 1 then 1 else rq end
                            ,boxes
                        from #panbal_list4                                              as li4
                             inner join vespa_analysts.panbal_sav                       as sav on li4.account_number = sav.account_number
                             inner join vespa_analysts.vespa_broadcast_reporting_vp_map as vir on li4.account_number = vir.account_number
                       where vp1 = 1

                       while (@virtuals < 300000) begin
                              truncate table #temp

                                insert into #temp
                                select top 1000 account_number
                                      ,boxes
                                  from #list1_rq
                              order by rq
                                      ,account_number

                                   set @virtuals = @virtuals + (select sum(boxes) from #temp)

                                select @cow = count(1) from #list1_rq

                                    if (@cow = 0) begin

                                             set @cow = 30000 - @virtuals

                                          insert into panbal_amends(account_number, movement)
                                          select null
                                                ,@cow || ' more boxes needed on the virtual panel'

                                             set @virtuals = 300000
                                   end

                                delete from #list1_rq where account_number in (select account_number from #temp)

                                insert into panbal_amends(account_number, movement)
                                select account_number
                                      ,'Account to add to panel 6/7 for virtual panel req.'
                                  from #temp

                         end --while
                      commit

               end --if

                -- check TA coverage - we need at least 25% from enabled accounts on all panels
            select @ta = sum(ta_propensity)
              from vespa_analysts.panbal_sav as sav
                   left join panbal_amends  as pan on sav.account_number = pan.account_number
             where panel is not null
                or pan.account_number is not null

            select @ta = @ta / sum(ta_propensity)  from vespa_analysts.SkyBase_TA_scores

             while (@ta < .25) begin
                         set rowcount @precision

                    truncate table #temp

                      insert into #temp(account_number)
                      select li4.account_number
                        from #panbal_list4 as li4
                             inner join vespa_analysts.panbal_sav as sav on li4.account_number = sav.account_number
                    order by case when rq is null then case when cbck_rate is null then 1 else cbck_rate end when rq > 1 then 1 else rq end * ta_propensity desc

                         set rowcount 0

                      delete from #panbal_list4 where account_number in (select account_number from #temp)

                      insert into panbal_amends(account_number, movement)
                      select account_number
                            ,'Account to add to panels 6/7 for TA coverage'
                        from #temp

                      select @ta = sum(ta_propensity)
                            ,@virtuals = count(1)
                        from vespa_analysts.panbal_sav as sav
                             left join panbal_amends  as pan on sav.account_number = pan.account_number
                       where panel is not null
                          or pan.account_number is not null

                      select @ta = @ta / @virtuals

               end --while

                -- check TA coverage - we also need at least 12% from accounts returning data on all panels
            select @ta = sum(ta_propensity)
              from vespa_analysts.panbal_sav as sav
                   left join panbal_amends  as pan on sav.account_number = pan.account_number
             where rq >= 0.5
               and (panel is not null or pan.account_number is not null)

            select @ta = @ta / sum(ta_propensity) from vespa_analysts.SkyBase_TA_scores

             while (@ta < .12) begin
                         set rowcount @precision

                    truncate table #temp

                      insert into #temp(account_number)
                      select li4.account_number
                        from #panbal_list4 as li4
                             inner join vespa_analysts.panbal_sav as sav on li4.account_number = sav.account_number
                    order by case when sav.rq is null then case when cbck_rate is null then 1 else cbck_rate end when sav.rq > 1 then 1 else sav.rq end * ta_propensity desc

                         set rowcount 0

                      delete from #panbal_list4 where account_number in (select account_number from #temp)

                      insert into panbal_amends(account_number, movement)
                      select account_number
                            ,'Account to add to panels 6/7 for TA coverage'
                        from #temp

                      select @ta = sum(ta_propensity)
                            ,@virtuals = count(1)
                        from vespa_analysts.panbal_sav as sav
                             left join panbal_amends  as pan on sav.account_number = pan.account_number
                       where panel is not null
                          or pan.account_number is not null

                      select @ta = @ta / @virtuals

               end --while

                -- recreate list4
          truncate table #panbal_list4

            insert into #panbal_list4(
                   account_number
                  ,segment_id
                  ,rq
                  ,thi
            )
            select bas.account_number
                  ,segment_id
                  ,1
                  ,0
              from vespa_analysts.panbal_sav           as bas
             where panel is null
          group by bas.account_number
                  ,segment_id

                -- New accounts to add to alternate day panels
            insert into panbal_amends(account_number, movement)
            select bas.account_number
                   ,'Account to add to panels 6/7, eventually for panel 11/ 12'
              from #PanBal_panel as bas
                   inner join #panbal_list4 as li4 on bas.account_number = li4.account_number

          truncate table panbal_all_aggregated_results
          truncate table panbal_traffic_lights
            insert into panbal_all_aggregated_results select * from #panbal_all_aggregated_results
            insert into panbal_traffic_lights         select * from #panbal_traffic_lights

     end; --PanBal procedure

--Procedure drop tables
    drop procedure PanBal_drop_tables;
  create procedure PanBal_drop_tables as begin
              drop table panbal_results
              drop table panbal_amends
              drop table panbal_bestsofar
              drop table panbal_traffic_lights
              drop table panbal_all_aggregated_results
     end; --procedure
;

commit;

      -- results
  select * from panbal_results
  select * from panbal_traffic_lights;
  select * from panbal_all_aggregated_results;
  select movement,count(1) from panbal_amends group by movement;


/*
insert into vespa_analysts.panel_movements_log(
         account_number
        ,card_subscriber_id
        ,requested_enablement_dt
        ,requested_enablement_route
        ,last_ca_callback_route
        ,multiroom
        ,requested_movement_type)
  select bas.account_number
        ,card_subscriber_id
        ,@now
        ,'KQ'
        ,'Unknown' as last_ca_callback_route
        ,0
        ,'Panel balancing'
    from panbal_amends as bas
         left join sk_prod.sk_prod.cust_card_subscriber_link as stb on bas.account_number = stb.account_number
   where current_flag = 'Y'
;

      -- find last On Demand download date by box
  create table #dl_by_box(
         card_id             varchar(30)
        ,service_instance_id varchar(30)
        ,subscriber_id       int
        ,max_dt              date)
;

  insert into #dl_by_box(
         card_id
        ,max_dt)
  select card_id
        ,max(last_modified_dt) as max_dt
    from panbal_amends as bas
         inner join sk_prod.CUST_ANYTIME_PLUS_DOWNLOADS as apd on bas.account_number = apd.account_number
group by card_id
;

  commit;
  create unique hg index uhcar on #dl_by_box(card_id);

  update #dl_by_box as bas
     set bas.service_instance_id = cid.service_instance_id
    from sk_prod.cust_card_issue_dim as cid
   where bas.card_id = left(cid.card_id, 8)
     and card_status = 'Enabled'
;

  update #dl_by_box as bas
     set bas.subscriber_id = csi.si_external_identifier
    from sk_prod.cust_service_instance as csi
   where bas.service_instance_id = csi.service_instance_id
     and effective_to_dt = '9999-09-09'
;

      -- if there has been an on demand download in the last 6 months (by box)
  update vespa_analysts.panel_movements_log as bas
     set last_ondemand_download_dt = max_dt
    from #dl_by_box as dls
   where cast(bas.card_subscriber_id as int) = dls.subscriber_id
     and requested_enablement_dt = now()
;

  update vespa_analysts.panel_movements_log as bas
     set source = panel
        ,bas.rq = sav.rq
        ,ca_callback_rate = cbck_rate
    from vespa_analysts.panbal_sav as sav
   where bas.account_number = sav.account_number
     and requested_enablement_dt = now()
;

  update vespa_analysts.panel_movements_log as bas
     set destination = case when knockout_level_bb = 9999 then 11 else 12 end
    from vespa_analysts.waterfall_base as wat
   where bas.account_number = wat.account_number
     and requested_enablement_dt = now()
;

  update vespa_analysts.panel_movements_log as bas
     set multiroom = mr
    from panbal_weekly_sample as sam
   where bas.account_number = sam.account_number
     and requested_enablement_dt = now()
;

  update vespa_analysts.panel_movements_log as bas
     set bas.ca_callback_day         = scm.cbk_day
        ,last_CA_callback_dt = date(substr(date_time_received,7,4) || '-' || substr(date_time_received,4,2) || '-' || left(date_time_received,2))
        ,request_created_dt  = cast(case when cbk_day is null then '2014-07-15' else '2014-0' || case when cast(cbk_day as int) >= 15 then '8-' else '7-' end || right('0' || cbk_day, 2) end as date)
    from vespa_analysts.Waterfall_SCMS_callback_data as scm
   where bas.account_number = scm.account_number
     and requested_enablement_dt = now()
;
*/




