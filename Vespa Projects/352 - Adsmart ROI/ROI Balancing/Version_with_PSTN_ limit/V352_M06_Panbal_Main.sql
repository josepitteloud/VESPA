/*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

-----------------------------------------------------------------------------------

**Project Name:                         Panel Balancing
**Analyst:                              Jonathan Green
**Contributions From:                   Hoi Yu Tang, Leonardo Ripoli, Jason Thompson, Jose Loureda
**Stakeholder:                          Jose Loureda
**Project Code (Insight Collation):     V306, V352
**SharePoint Folder:                    http://sp-department.bskyb.com/sites/IQSKY/SIG/Insight%20Collation%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FIQSKY%2FSIG%2FInsight%20Collation%20Documents%2F01%20Analysis%20Requests%2FV306%20-%20Foundation%20-%20Platform%20Maintenance%2FPhase%202%2FPanel%20Balancing

**Business Brief:

Panel Balancing is a regular exercise to ensure that the viewing panel is as
representative of the wider Sky customer base as possible as the latter evolves over
time. Balancing is also crucial in ensuring that key KPI and contractual obligations
of account coverage are maintained.




**Module:                              M06_Main

This module is the central loop of the balancing process. After initialising all of the tables which are used within this module, it calculates the imbalance for each variable in the traffic_lights table.
If any of the variables has an imbalance higher than the lmit then another loop of the process is induced.
A loop of the code adds a number of accounts to the panel (set by @precision) to minimise the maximum imbalance.

*/


  create or replace procedure V352_M06_Main
         @max_imbalance int
        ,@min_b         int --boxes to return per day
        ,@precision     int --the number of accounts to add at a time
        ,@country       bit
        ,@maxpstn       int --the maximum number of PSTN accounts
      as begin

                   --------------------
                   -- Section A - Setup
                   --------------------
            create table temp_PanBal_all_aggregated_results(
                   aggregation_variable                              int null
                  ,variable_value                                    varchar(60) default null
                  ,Sky_Base_Households                               int null
                  ,Panel_Households                                  decimal(10,2) null
                  ,Good_Household_Index                              double default 0 null
                  ,GHIplus1                                          double default 0 null
                  ,GHIminus1                                         double default 0 null
                  ,incr_diff                                         double default 0 null
                  ,decr_diff                                         double default 0 null
                   )

            commit
            create lf index lfagg on temp_PanBal_all_aggregated_results(aggregation_variable)
            create lf index lfvar on temp_PanBal_all_aggregated_results(variable_value)

            create table temp_panel_households(
                   aggregation_variable                              int null
                  ,Panel_Households                                  decimal(10,2) null
                  ,sky_base_households                               int null
                   )

            create table temp_primary_panel_pool(
                   account_number                                    varchar(30) null
                  ,segment_id                                        int null
                  ,rq                                                double null
                   )

            create table temp_PanBal_panel(
                   account_number                                    varchar(30) null
                  ,segment_id                                        int null
                   )

            create table temp_PanBal_Scaling_Segment_Profiling (
                   segment_id                                        int null
                  ,Sky_Base_Households                               int null
                  ,Panel_households                                  decimal(10,2) default 0 null
                  ,primary key (segment_id)
                   )

            create table temp_PanBal_traffic_lights(
                   variable_name                                     int null
                  ,imbalance_rating                                  decimal(6,2) null
                   )

            create table temp_descrs(
                   segment_id                                        int null
                  ,descrs                                            double null
                   )

            create table temp_panel_segmentation(
                   segment_id                                        int null
                  ,Panel_Households                                  decimal(10,2) null
                   )

            create table temp_new_adds(
                   account_number                                    varchar(50) null
                  ,segment_id                                        int null
                  ,thi                                               double null
                   )

            create table temp_segment_THIs(
                   segment_id                                        int null
                  ,thi                                               double    default 0 null
                   )

            commit
            create unique hg index uhseg on temp_segment_THIs(segment_id)

            create table temp_panbal_segments_lookup_normalised(
                   segment_id                                        int null
                  ,aggregation_variable                              tinyint null
                  ,value                                             varchar(30) null
                   )

            commit
            create hg index hgseg on temp_panbal_segments_lookup_normalised(segment_id)
            create hg index lfagg on temp_panbal_segments_lookup_normalised(aggregation_variable)
            create hg index lfval on temp_panbal_segments_lookup_normalised(value)

            create table temp_lookup(
                   segment_id                                       int null
                  ,diff                                             double null
                   )

            commit
            create unique hg index uhseg on temp_lookup(segment_id)

                -- declarations
           declare @total_sky_base                                  int
           declare @panel_reporters                                 int
           declare @cow                                             int     default 0 -- the count of boxes on the daily panels
           declare @accounts_remaining                              int     default 1000000
           declare @temp                                            int
           declare @imbalance                                       double  default 1000
           declare @prev_imbalance                                  double  default 2000
           declare @tot_imb                                         double  default 100000
           declare @prev_tot_imb                                    int     default 1000000
           declare @counter                                         int
           declare @vars                                            tinyint
           declare @from_waterfall                                  tinyint default 0
           declare @records_out                                     int     default 1 -- are there any accounts that can be added?
           declare @reached                                         double  default 1000
           declare @started                                         double  default 2000
           declare @pstn_accounts                                   int     default 0

                -- set starting variables
               set rowcount 0

                   --------------------------------------
                   -- Section B - Fill the accounts lists
                   --------------------------------------

            select @pstn_accounts = count(distinct account_number)
              from panbal_sav
             where (panel = 12 and @country = 0)
                or (panel = 7  and @country = 1)

                -- primary panel list
            insert into temp_primary_panel_pool
                  (account_number
                  ,rq
                  ,segment_id
                  )
            select sav.account_number
                  ,rq
                  ,segment_id
                  ,segment_id
              from panbal_sav as sav
             where (panel in (2, 7) and @country = 1)
                or (panel in (10, 11, 12) and @country = 0)

                -- pstn panel accounts
          truncate table pstn_panel_pool

            insert into pstn_panel_pool
            select sav.account_number
                  ,segment_id
                  ,rq
                  ,0
              from panbal_sav as sav
             where (panel = 6 and @country = 0)

                -- secondary panel accounts
          truncate table secondary_panel_pool

            insert into secondary_panel_pool
            select sav.account_number
                  ,segment_id
                  ,rq
                  ,0
              from panbal_sav as sav
             where panel in (5, 15)
               and @country = 0

                -- waterfall accounts
          truncate table waterfall_pool

            insert into waterfall_pool
                  (account_number
                  ,segment_id
                  ,rq
                  ,thi
                  )
            select account_number
                  ,segment_id
                  ,1
                  ,0
              from panbal_sav
             where panel is null
               and bb_panel = 1
          group by account_number
                  ,segment_id

                -- waterfall PSTN accounts
          truncate table pstn_waterfall_pool

            insert into pstn_waterfall_pool
                  (account_number
                  ,segment_id
                  ,rq
                  ,thi
                  )
            select account_number
                  ,segment_id
                  ,1
                  ,0
              from panbal_sav
             where panel is null
               and bb_panel = 0
          group by account_number
                  ,segment_id

            insert into temp_PanBal_Scaling_Segment_Profiling
                  (segment_id
                  ,Sky_Base_Households
                  )
            select segment_id
                  ,sum(case when (fin_currency_code = 'GBP' and @country = 0) or (fin_currency_code = 'EUR' and @country = 1) then 1 else 0 end) as Sky_Base_Households
              from PanBal_segment_snapshots as seg
                   inner join cust_single_account_view as sav on seg.account_number = sav.account_number
          group by segment_id

            select @total_sky_base = sum(Sky_Base_Households) from temp_PanBal_Scaling_Segment_Profiling

               -- add any accounts first from the table panbal_additions
           insert into temp_PanBal_panel
                 (account_number
                 ,segment_id
                 )
           select pan.account_number
                 ,segment_id
             from secondary_panel_pool as pan
                  inner join panbal_additions as ads on pan.account_number = ads.account_number
            union
           select pan.account_number
                 ,segment_id
             from waterfall_pool as pan
                  inner join panbal_additions as ads on pan.account_number = ads.account_number

               -- for ROI add all BB accounts
           insert into temp_PanBal_panel
                 (account_number
                 ,segment_id
                 )
           select pan.account_number
                 ,segment_id
             from waterfall_base as pan
                  inner join panbal_sav as sav on sav.account_number= pan.account_number
            where knockout_level_roi = 9999
              and @country = 1

           delete from secondary_panel_pool
            where account_number in (select account_number from panbal_additions)

           delete from waterfall_pool
            where account_number in (select account_number from temp_PanBal_panel)

                -- start the panel off with just the acceptable viewing panel accounts
            insert into temp_PanBal_panel
                  (account_number
                  ,segment_id
                  )
            select account_number
                  ,segment_id
              from temp_primary_panel_pool

                -- we only need to work out THIs once for each segment that we have any accounts in list3 or list4
            insert into temp_segment_THIs(segment_id)
            select distinct(segment_id)
              from (select segment_id as segment_id
                      from secondary_panel_pool
                     union
                    select segment_id
                      from waterfall_pool
                     union
                    select segment_id
                      from pstn_panel_pool
                     union
                    select segment_id
                      from pstn_waterfall_pool
                   ) as sub

            insert into temp_panbal_segments_lookup_normalised(
                   segment_id
                  ,aggregation_variable
                  ,value
                   )
            select segment_id
                  ,aggregation_variable
                  ,value
              from panbal_segments_lookup_normalised

            select @vars = count(distinct aggregation_variable) from temp_panbal_segments_lookup_normalised

          truncate table panbal_results

                   ----------------------------
                   -- Section C - The Main Loop
                   ----------------------------
             while ((@imbalance >= @max_imbalance or @cow < @min_b) and @records_out > 0) begin

                    truncate table temp_panel_segmentation

                          -- reqd for traffic lights
                      insert into temp_panel_segmentation(segment_id
                            ,Panel_Households)
                      select bas.segment_id
                            ,sum(case when sav.rq is null then 0
                                      when sav.rq > 1 then 1
                                      else sav.rq
                                 end)
                        from temp_PanBal_panel as bas
                             inner join panbal_sav as sav on bas.account_number = sav.account_number
                    group by bas.segment_id

                      update temp_PanBal_Scaling_Segment_Profiling
                         set Panel_Households     = 0

                      update temp_PanBal_Scaling_Segment_Profiling as bas
                         set Panel_Households     = seg.Panel_Households
                        from temp_panel_segmentation as seg
                       where bas.segment_id       = seg.segment_id

                    truncate table temp_PanBal_all_aggregated_results

                          -- create the traffic light report
                      insert into temp_PanBal_all_aggregated_results(
                             aggregation_variable
                            ,variable_value
                            ,Sky_Base_Households
                            ,Panel_Households
                             )
                      select ssl.aggregation_variable
                            ,value
                            ,sum(sky_base_households)
                            ,sum(Panel_households)
                        from temp_PanBal_Scaling_Segment_Profiling as ssp
                             inner join temp_panbal_segments_lookup_normalised as ssl on ssp.segment_id = ssl.segment_id
                    group by aggregation_variable
                            ,value

                          -- if any values are Unknown, then we don't want to balance these
                      delete from temp_PanBal_all_aggregated_results
                       where variable_value in ('Non-scalable', 'NS', 'U', 'Not Defined', 'E) Unknown', 'Unknown', 'No Panel')
                          or sky_base_households < 1000
                          or sky_base_households is null

                    truncate table temp_panel_households

                      insert into temp_panel_households(
                             aggregation_variable
                            ,Panel_Households
                            ,sky_base_households
                             )
                      select aggregation_variable
                            ,sum(panel_households)
                            ,sum(sky_base_households)
                        from temp_PanBal_all_aggregated_results
                    group by aggregation_variable

                      update temp_PanBal_all_aggregated_results as bas
                         set Good_Household_Index = 100.0 *  bas.Panel_households               * hsh.Sky_Base_Households / bas.Sky_Base_Households /  hsh.Panel_Households      --index value for each variable value
                            ,GHIplus1             = 100.0 * (bas.Panel_households + @precision) * hsh.Sky_Base_Households / bas.Sky_Base_Households / (hsh.Panel_Households + @precision) --what the Good Household Index (GHI) would be if we added 1 account
                            ,GHIminus1            = 100.0 *  bas.Panel_households               * hsh.Sky_Base_Households / bas.Sky_Base_Households / (hsh.Panel_Households + @precision) --what the Good Household Index (GHI) would be if we added 1 account to a different variable value
                        from temp_panel_households as hsh
                       where bas.aggregation_variable = hsh.aggregation_variable

                          -- the difference (squared) between GHI and GHI+1
                      update temp_PanBal_all_aggregated_results
                         set incr_diff = ((Good_Household_Index-100) * (Good_Household_Index-100) - (GHIplus1-100) * (GHIplus1-100))

                          -- the difference (squared) between GHI and GHI-1
                      update temp_PanBal_all_aggregated_results
                         set decr_diff = ((Good_Household_Index-100) * (Good_Household_Index-100) - (GHIminus1-100) * (GHIminus1-100))

                    truncate table temp_PanBal_traffic_lights

                      insert into temp_PanBal_traffic_lights
                      select aggregation_variable
                            ,sqrt(avg((Good_Household_Index - 100) * (Good_Household_Index - 100))) as imbalance_rating
                        from temp_PanBal_all_aggregated_results
                    group by aggregation_variable

                      select @imbalance = max(imbalance_rating) from temp_PanBal_traffic_lights

                          -- if we are short of the number of boxes required then we will need to continue
                      select @cow = sum(rq * boxes)
                        from temp_PanBal_panel as bas
                             inner join panbal_sav as sav on bas.account_number = sav.account_number

                          -- if the panel is not balanced, then continue
                          if (@imbalance >= @max_imbalance or @cow < @min_b)
                       begin

                                    -- calculate the THIs
                                update temp_segment_THIs
                                   set thi = 0

                              truncate table temp_lookup

                                insert into temp_lookup(
                                       segment_id
                                      ,diff
                                       )
                                select segment_id
                                      ,sum(case when lkp.value = agg.variable_value then incr_diff else decr_diff end) as diff
                                  from temp_PanBal_all_aggregated_results as agg
                                       inner join temp_panbal_segments_lookup_normalised as lkp on lkp.aggregation_variable = agg.aggregation_variable
                              group by segment_id

                                update temp_segment_THIs as bas
                                   set thi = thi + diff
                                  from temp_lookup as lkp
                                 where bas.segment_id = lkp.segment_id

                                    -- update THI for marketing panels
                                update secondary_panel_pool as bas
                                   set bas.thi = thi.thi * rq
                                  from temp_segment_THIs as thi
                                 where bas.segment_id = thi.segment_id

                                update pstn_panel_pool as bas
                                   set bas.thi = thi.thi * rq
                                  from temp_segment_THIs as thi
                                 where bas.segment_id = thi.segment_id

                              truncate table temp_new_adds

                                    if (@pstn_accounts + @precision > @maxpstn) begin
                                          insert into temp_new_adds
                                          select li3.account_number
                                                ,li3.segment_id
                                                ,thi
                                            from secondary_panel_pool        as li3
                                                 left join temp_PanBal_panel as bas on li3.account_number = bas.account_number
                                           where bas.account_number is null
                                   end
                                  else begin
                                          insert into temp_new_adds
                                          select li3.account_number
                                                ,li3.segment_id
                                                ,thi
                                            from pstn_panel_pool             as li3
                                                 left join temp_PanBal_panel as bas on li3.account_number = bas.account_number
                                           where bas.account_number is null
                                   end

                                select @accounts_remaining = count(1)
                                  from temp_new_adds

                                    if (@from_waterfall = 2)
                                 begin
                                              if (@started - ((@reached - @max_imbalance) / 2) < @imbalance)
                                           begin
                                                       set @from_waterfall = 1
                                             end
                                            else
                                           begin
                                                       set @from_waterfall = 0
                                             end
                                   end
                                    if (@from_waterfall = 1)
                                 begin
                                              -- update THI for waterfall list
                                              if (@pstn_accounts + @precision > @maxpstn) begin
                                                    update waterfall_pool as bas
                                                       set bas.thi = thi.thi * rq
                                                      from temp_segment_THIs as thi
                                                     where bas.segment_id = thi.segment_id

                                                        -- are there any that can be added?
                                                    select @records_out = count(1)
                                                      from waterfall_pool
                                                     where thi > 0

                                                       set rowcount @precision

                                                                  -- add some from the waterfall
                                                              insert into temp_PanBal_panel(account_number
                                                                                       ,segment_id)
                                                              select li4.account_number
                                                                    ,li4.segment_id
                                                                from waterfall_pool as li4
                                                               where thi > 0
                                                            order by thi desc

                                                       set rowcount 0

                                             end
                                            else begin
                                                    update pstn_waterfall_pool as bas
                                                       set bas.thi = thi.thi * rq
                                                      from temp_segment_THIs as thi
                                                     where bas.segment_id = thi.segment_id

                                                    select @records_out = count(1)
                                                      from pstn_waterfall_pool
                                                     where thi > 0

                                                       set rowcount @precision

                                                              insert into temp_PanBal_panel(account_number
                                                                                       ,segment_id)
                                                              select li4.account_number
                                                                    ,li4.segment_id
                                                                from pstn_waterfall_pool as li4
                                                               where thi > 0
                                                            order by thi desc

                                                       set rowcount 0
                                                       set @pstn_accounts = @pstn_accounts + @precision

                                             end

                                              -- remove the ones we have just added from the waterfall list
                                          delete from waterfall_pool where account_number in (select account_number from temp_PanBal_panel)

                                             set @prev_imbalance = 3000
                                             set @prev_tot_imb = 100000
                                             set @from_waterfall = 2
                                             set @imbalance = 2000
                                   end
                                  else
                                 begin

                                              if (@accounts_remaining >= @precision and (((@imbalance - (@prev_imbalance - @imbalance) * (@accounts_remaining / @precision)) < @max_imbalance))  or @cow < @min_b) 
                                              -- if there are a reasonable amount that can be added, and the target can be reached
                                           begin
                                                       set rowcount @precision -- only show this many lines in all queries

                                                              insert into temp_PanBal_panel(
                                                                     account_number
                                                                    ,segment_id
                                                              )
                                                              select account_number
                                                                    ,segment_id
                                                                from temp_new_adds
                                                            order by thi desc
 
                                                       set rowcount 0 --back to normal

                                                        if (@pstn_accounts + @precision <= @maxpstn) begin
                                                                 set @pstn_accounts = @pstn_accounts + @precision
                                                       end

                                                       set @prev_imbalance = @imbalance
                                                       set @prev_tot_imb = @tot_imb

                                             end
                                            else
                                           begin
                                                       set @from_waterfall = 1
                                                        -- remove all marketing panel accounts from the panel
                                                    delete from temp_PanBal_panel where account_number in (select account_number from secondary_panel_pool)
                                                    delete from temp_PanBal_panel where account_number in (select account_number from pstn_panel_pool)
                                                    select @pstn_accounts = count(pan.account_number)
                                                      from temp_panbal_panel as pan
                                                           inner join panbal_sav as sav on pan.account_number = sav.account_number
                                                     where panel in (6, 12)

                                                       set @reached = @imbalance
                                                       set @imbalance = 3000
                                             end --if
                                   end --if
                         end

                      select @tot_imb = sum((100-good_household_index) * (100-good_household_index)) from temp_PanBal_all_aggregated_results


                      insert into panbal_results(imbalance
                                                ,tot_imb
                                                ,records
                                                ,from_waterfall
                                                ,tim)
                      select @imbalance
                            ,@tot_imb
                            ,count(1)
                            ,@from_waterfall
                            ,now()
                        from temp_PanBal_panel

                      commit

                          if @started = 3000 begin
                                   set @started = @imbalance
                         end

               end --while

          truncate table panbal_amends
                if (@records_out = 0) begin
                      insert into panbal_amends(
                             account_number
                            ,movement)
                      select 0
                            ,'Not enough accounts available to find a balance'
               end

               -- accounts to add to panel 12 from panel 6
            insert into panbal_amends(
                   account_number
                  ,movement)
            select bas.account_number
                  ,'Account to add to primary panels from secondary panels'
              from temp_PanBal_panel as bas
                   inner join secondary_panel_pool as li3 on bas.account_number = li3.account_number
             union
            select bas.account_number
                  ,'Account to add to primary panels from secondary panels'
              from temp_PanBal_panel as bas
                   inner join pstn_panel_pool as li3 on bas.account_number = li3.account_number
             union
            select bas.account_number
                  ,'Account to add to primary panels from secondary panels (from adds table)'
              from temp_PanBal_panel as bas
                   inner join panbal_additions as ads on bas.account_number = ads.account_number

          truncate table panbal_panel
            insert into panbal_panel(
                   account_number
                  ,segment_id
                   )
            select account_number
                  ,segment_id
              from temp_PanBal_panel
            commit

              drop table temp_PanBal_all_aggregated_results
              drop table temp_panel_households
              drop table temp_primary_panel_pool
              drop table temp_PanBal_traffic_lights
              drop table temp_descrs
              drop table temp_panel_segmentation
              drop table temp_new_adds
              drop table temp_segment_THIs
              drop table temp_panbal_segments_lookup_normalised
              drop table temp_lookup
              drop table temp_PanBal_Scaling_Segment_Profiling
              drop table temp_PanBal_panel
     end; --V352_M06_Main
 commit;

 grant execute on V352_M06_Main to vespa_group_low_security;
 commit;


---
/*
--Test
              drop table temp_PanBal_all_aggregated_results;
              drop table temp_panel_households;
              drop table temp_primary_panel_pool;
              drop table temp_PanBal_traffic_lights;
              drop table temp_descrs;
              drop table temp_panel_segmentation;
              drop table temp_new_adds;
              drop table temp_segment_THIs;
              drop table temp_panbal_segments_lookup_normalised;
              drop table temp_lookup;
              drop table temp_PanBal_Scaling_Segment_Profiling;
              drop table temp_PanBal_panel;

           create variable @total_sky_base                                  int;
           create variable @panel_reporters                                 int;
           create variable @cow                                             int     default 0; -- the count of boxes on the daily panels
           create variable @accounts_remaining                              int     default 1000000;
           create variable @temp                                            int;
           create variable @imbalance                                       double  default 1000;
           create variable @prev_imbalance                                  double  default 2000;
           create variable @tot_imb                                         double  default 100000;
           create variable @prev_tot_imb                                    int     default 1000000;
           create variable @counter                                         int;
           create variable @vars                                            tinyint;
           create variable @from_waterfall                                  tinyint default 0;
           create variable @records_out                                     int     default 1; -- are there any accounts that can be added?
           create variable @reached                                         double  default 1000;
           create variable @started                                         double  default 2000;
           create variable @pstn_accounts                                   int     default 0;

create variable          @max_imbalance int=162;
create variable         @min_b         int=0; --boxes to return per day
create variable         @precision     int=100; --the number of accounts to add at a time
create variable         @country       bit=1;
create variable         @maxpstn       int=10000; --the maximum number of PSTN accounts
/*

