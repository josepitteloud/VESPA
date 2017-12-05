--------------------------------------
--Auto Panel Balance------------------
--------------------------------------

-------
--Setup
-------

--create tables
create table PanBal_all_aggregated_results (
     aggregation_variable                               varchar(30)
    ,scaling_or_not                                     bit
    ,variable_value                                     varchar(60)
    ,Sky_Base_Households                                int
    ,Panel_Households                                   int
    ,Good_Household_Index                               double
    ,GHIplus1                                           double
    ,GHIminus1                                          double
    ,incr_diff                                          double
    ,decr_diff                                          double
    ,primary key (aggregation_variable, variable_value)
);
create hg index idx1 on PanBal_all_aggregated_results(variable_value);

create table PanBal_list1(
     account_number                                     varchar(30)
    ,scaling_segment_id                                 int
);

create table PanBal_list2(
     account_number                                     varchar(30)
    ,scaling_segment_id                                 int
);

create table PanBal_list3(
     account_number                                     varchar(30)
    ,scaling_segment_id                                 int
    ,rq                                                 double
    ,thi                                                double
);
create hg index idx1 on PanBal_list3(scaling_segment_id);
create hg index idx2 on PanBal_list3(thi);

create table PanBal_list4(
     account_number                                     varchar(30)
    ,scaling_segment_id                                 int
    ,rq                                                 double
    ,thi                                                double
);
create hg index idx1 on PanBal_list4(thi);
create hg index idx3 on PanBal_list4(scaling_segment_id);

create table PanBal_Panel(
     account_number                                     varchar(30)
    ,scaling_segment_id                                 int
);
create hg index idx1 on PanBal_Panel(scaling_segment_id);

create table PanBal_Scaling_Segment_Profiling (
     scaling_segment_id                                 int
    ,Sky_Base_Households                                int
    ,Panel_households                                   int
    ,Acceptably_reliable_households                     int
    ,Acceptably_reporting_index                         decimal(6,2)  default null
    ,primary key (scaling_segment_id)
);
create hg index idx1 on PanBal_Scaling_Segment_Profiling(Panel_households);

create table PanBal_traffic_lights(
     variable_name                                      varchar(30)
    ,imbalance_rating                                   decimal(6,2)
);

create table PanBal_results ( --for testing
     imbalance                                          double --the highest variable
    ,tot_imb                                            double --total across all variables
    ,records                                            int    --in the panel
    ,tim                                                datetime
);

create table PanBal_amends(
     account_number                                     varchar(30)
    ,movement                                           varchar(60)
);

create table #descrs(
     scaling_segment_id                                 int
    ,descrs                                             double
);

create table #panel_segmentation(
     scaling_segment_id                                 int
    ,Panel_Households                                   int
);
create unique index fake_pk on #panel_segmentation (scaling_segment_id);

create table #new_adds(
     account_number                                     varchar(50)
    ,scaling_segment_id                                 int
    ,thi                                                double
);
create hg index idx1 on #new_adds(thi);

create table #segment_THIs(
     scaling_segment_id                                 int
    ,universe                                           varchar(50)
    ,isba_tv_region                                     varchar(50)
    ,hhcomposition                                      varchar(50)
    ,tenure                                             varchar(50)
    ,package                                            varchar(50)
    ,boxtype                                            varchar(50)
    ,thi                                                double      default 0
);

create table #list1_rq(
     account_number                                     varchar(30)
    ,rq                                                 double
    ,boxes                                              int
);

create table #temp(
     account_number                                     varchar(30)
    ,boxes                                              int
);

--create variables
create variable @total_sky_base        int;
create variable @recent_profiling_date date;
create variable @panel_reporters       int;
create variable @cow                   int;
create variable @imbalance             decimal(6,2);
create variable @tot_imb               double;
create variable @precision             int;
create variable @max_imbalance         int;
create variable @max_rq                double;
create variable @virtuals              int;

--set starting variables
set @imbalance = 100;
set rowcount 0; -- just in case it's been set to something

--these 3 variables can be changed as required
set @max_imbalance = 20;
set @max_rq = 0.8;
set @precision = 5000; -- this defines how accurately we try to find a solution - ideally it will be 1, but the lower the number,
                       --the longer the code will take to run.

select @recent_profiling_date = max(profiling_date) from vespa_analysts.SC2_Sky_base_segment_snapshots;
commit

-------------------------
--Fill the accounts lists
-------------------------
execute vespa_analysts.waterfall;

--List 2 - Acceptable panel 12 accounts
  insert into PanBal_list2
  select bas.account_number
        ,scaling_segment_id
    from vespa_analysts.vespa_panman_all_households as bas
         left join vespa_analysts.ConsentIssue_05_Revised_Consent_Info as exc on bas.account_number = exc.account_number -- list from Tony Kinnaird of accounts that should not be usec due to consent issues
   where panel='VESPA'
     and reporting_quality >= .8
     and scaling_segment_ID is not null
     and exc.account_number is null
;

--List 1 unacceptable panel 12 accounts
  insert into PanBal_list1
  select bas.account_number
        ,bas.scaling_segment_id
    from vespa_analysts.vespa_panman_all_households as bas
         left join PanBal_list2 as li2 on bas.account_number = li2.account_number
         left join vespa_analysts.ConsentIssue_05_Revised_Consent_Info as exc on bas.account_number = exc.account_number
   where li2.account_number is null
     and bas.panel = 'VESPA'
     and bas.scaling_segment_ID is not null
     and exc.account_number is null
;

--List 3 - alternate day panel accounts
  insert into PanBal_list3
   select bas.account_number
        ,scaling_segment_id
        ,case when reporting_quality > 1 then 1 else reporting_quality end
        ,0
    from vespa_analysts.vespa_panman_all_households as bas
         left join vespa_analysts.ConsentIssue_05_Revised_Consent_Info as exc on bas.account_number = exc.account_number
   where panel <> 'VESPA'
     and reporting_quality >= .8
     and scaling_segment_ID is not null
     and exc.account_number is null
;

--List 4 - waterfall accounts
  insert into PanBal_list4
  select bas.account_number
        ,scaling_segment_id
        ,0
        ,0
    from vespa_analysts.waterfall_base                                 as bas
         inner join vespa_analysts.SC2_Sky_base_segment_snapshots      as bss on bas.account_number = bss.account_number
         left join vespa_analysts.ConsentIssue_05_Revised_Consent_Info as exc on bas.account_number = exc.account_number
   where knockout_level = 9999
     and profiling_date = @recent_profiling_date
     and exc.account_number is null
group by bas.account_number
        ,scaling_segment_id
;

  select account_number
        ,100 * sum(cast(expected_cbcks as double) - cast(missing_cbcks as double)) / sum(cast(expected_cbcks as double)) as cbck_rate
    into #cbck_rate
    from Waterfall_scms_callback_data
   where expected_cbcks >0
group by account_number
;

commit;
create hg index idx1 on #cbck_rate(account_number);

  update PanBal_list4 as bas
     set bas.rq = cbc.cbck_rate
    from #cbck_rate as cbc
   where bas.account_number = cbc.account_number
; --

  select account_number
        ,scaling_segment_id
    into #Scaling_weekly_sample
    from vespa_analysts.SC2_Sky_base_segment_snapshots
   where profiling_date = @recent_profiling_date
;

  insert into  PanBal_Scaling_Segment_Profiling (
         scaling_segment_id
        ,Sky_Base_Households
  )
  select scaling_segment_id
        ,count(1) as Sky_Base_Households
    from #Scaling_weekly_sample as sws
group by sws.scaling_segment_ID
;

  select @total_sky_base = sum(Sky_Base_Households) from PanBal_Scaling_Segment_Profiling;

  insert into PanBal_Panel(account_number
                          ,scaling_segment_id
        )
  select account_number
        ,scaling_segment_id
    from PanBal_list2
;

--we only need to work out THIs once for each segment that we have any accounts in list3 or list4
  insert into #segment_THIs(scaling_segment_id)
  select distinct(scaling_segment_id)
    from (select distinct(scaling_segment_id) as scaling_segment_id
            from PanBal_list3
           union
          select distinct(scaling_segment_id)
            from PanBal_list4) as sub
;

  update #segment_THIs as bas
     set bas.universe       = seg.universe
        ,bas.isba_tv_region = seg.isba_tv_region
        ,bas.hhcomposition  = seg.hhcomposition
        ,bas.tenure         = seg.tenure
        ,bas.package        = seg.package
        ,bas.boxtype        = seg.boxtype
    from vespa_analysts.SC2_Segments_lookup as seg
   where bas.scaling_segment_id = seg.scaling_segment_id
;

---------------
--The Main Loop
---------------
while (@imbalance >= @max_imbalance)
     begin

          truncate table #panel_segmentation

          --reqd for traffic lights
            insert into #panel_segmentation
            select scaling_segment_id
                  ,count(1) as Panel_Households
              from PanBal_Panel
          group by scaling_segment_ID

            update PanBal_Scaling_Segment_Profiling as bas
               set Panel_Households             = ps.Panel_Households
              from #panel_segmentation as ps
             where bas.scaling_segment_id       = ps.scaling_segment_id

          truncate table PanBal_all_aggregated_results

          --create the traffic light report
            insert into PanBal_all_aggregated_results
            select 'UNIVERSE' -- Name of variable being profiled
                  ,1          -- Whether the variable is used for scaling or not
                  ,ssl.universe
                  ,sum(Sky_Base_Households)
                  ,sum(Panel_households)
                  ,0          --Good Household Index
                  ,0          --GHIplus1 (if we add 1 account to this variable value)
                  ,0          --GHIminus1 (if we add 1 account to a different value within this category)
                  ,0          --incr_diff, the difference between GHI and GHI+1 (variance from 100 squared)
                  ,0          --decr_diff, the difference between GHI-1 and GHI (variance from 100 squared)
              from PanBal_Scaling_Segment_Profiling as ssp
                   inner join vespa_analysts.SC2_Segments_lookup_v2_0 as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
          group by ssl.universe

            insert into PanBal_all_aggregated_results
            select 'REGION'
                  ,1
                  ,ssl.isba_tv_region
                  ,sum(Sky_Base_Households)
                  ,sum(Panel_households)
                  ,0
                  ,0
                  ,0
                  ,0
                  ,0
              from PanBal_Scaling_Segment_Profiling as ssp
                   inner join vespa_analysts.SC2_Segments_lookup_v2_0 as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
          group by ssl.isba_tv_region

            insert into PanBal_all_aggregated_results
            select 'HHCOMP'
                  ,1
                  ,ssl.hhcomposition
                  ,sum(Sky_Base_Households)
                  ,sum(Panel_households)
                  ,0
                  ,0
                  ,0
                  ,0
                  ,0
              from PanBal_Scaling_Segment_Profiling as ssp
                   inner join vespa_analysts.SC2_Segments_lookup_v2_0 as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
          group by ssl.hhcomposition

            insert into PanBal_all_aggregated_results
            select 'PACKAGE'
                  ,1
                  ,ssl.package
                  ,sum(Sky_Base_Households)
                  ,sum(Panel_households)
                  ,0
                  ,0
                  ,0
                  ,0
                  ,0
              from PanBal_Scaling_Segment_Profiling as ssp
                   inner join vespa_analysts.SC2_Segments_lookup_v2_0 as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
          group by ssl.package

            insert into PanBal_all_aggregated_results
            select 'TENURE'
                  ,1
                  ,ssl.tenure
                  ,sum(Sky_Base_Households)
                  ,sum(Panel_households)
                  ,0
                  ,0
                  ,0
                  ,0
                  ,0
              from PanBal_Scaling_Segment_Profiling as ssp
                   inner join vespa_analysts.SC2_Segments_lookup_v2_0 as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
          group by ssl.tenure

            insert into PanBal_all_aggregated_results
            select 'BOXTYPE'
                  ,1
                  ,ssl.boxtype
                  ,sum(Sky_Base_Households)
                  ,sum(Panel_households)
                  ,0
                  ,0
                  ,0
                  ,0
                  ,0
              from PanBal_Scaling_Segment_Profiling as ssp
                   inner join vespa_analysts.SC2_Segments_lookup_v2_0 as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
          group by ssl.boxtype

            select @panel_reporters = sum(panel_households) from PanBal_all_aggregated_results where aggregation_variable='UNIVERSE' --or anything

          --index value for each variable value
            update PanBal_all_aggregated_results as bas
               set Good_Household_Index = 100 * (Panel_households)     * @total_sky_base / convert(float, Sky_Base_Households) / @panel_reporters

          --what the Good Household Index (GHI) would be if we added 1 account
            update PanBal_all_aggregated_results as bas
               set GHIplus1             = 100 * (Panel_households + 1) * @total_sky_base / convert(float, Sky_Base_Households) / (@panel_reporters +1)

          --what the Good Household Index (GHI) would be if we added 1 account to a different variable value
            update PanBal_all_aggregated_results as bas
               set GHIminus1             = 100 * (Panel_households)     * @total_sky_base / convert(float, Sky_Base_Households) / (@panel_reporters +1)

          --the difference (squared) between GHI and GHI+1
            update PanBal_all_aggregated_results as bas
               set incr_diff = ((Good_Household_Index-100) * (Good_Household_Index-100) - (GHIplus1-100) * (GHIplus1-100))

          --the difference (squared) between GHI and GHI-1
            update PanBal_all_aggregated_results as bas
               set decr_diff = ((Good_Household_Index-100) * (Good_Household_Index-100) - (GHIminus1-100) * (GHIminus1-100))


            delete from PanBal_all_aggregated_results where variable_value like '%Unknown'

          truncate table PanBal_traffic_lights

            insert into PanBal_traffic_lights
            select case aggregation_variable
                    when 'UNIVERSE'         then 'Universe'
                    when 'REGION'           then 'Region'
                    when 'HHCOMP'           then 'Household composition'
                    when 'PACKAGE'          then 'Package'
                    when 'TENURE'           then 'Tenure'
                    when 'BOXTYPE'          then 'Box type'
                    else 'FAIL!'
                   end as variable_name
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
                       where bas.universe = agg.variable_value
                         and aggregation_variable = 'UNIVERSE'

                      update #segment_THIs as bas
                         set thi = thi + incr_diff
                        from PanBal_all_aggregated_results as agg
                       where bas.isba_tv_region = agg.variable_value
                         and aggregation_variable = 'REGION'

                      update #segment_THIs as bas
                         set thi = thi + incr_diff
                        from PanBal_all_aggregated_results as agg
                       where bas.hhcomposition = agg.variable_value
                         and aggregation_variable = 'HHCOMP'

                      update #segment_THIs as bas
                         set thi = thi + incr_diff
                        from PanBal_all_aggregated_results as agg
                       where bas.package = agg.variable_value
                         and aggregation_variable = 'PACKAGE'

                      update #segment_THIs as bas
                         set thi = thi + incr_diff
                        from PanBal_all_aggregated_results as agg
                       where bas.tenure = agg.variable_value
                         and aggregation_variable = 'TENURE'

                      update #segment_THIs as bas
                         set thi = thi + incr_diff
                        from PanBal_all_aggregated_results as agg
                       where bas.boxtype = agg.variable_value
                         and aggregation_variable = 'BOXTYPE'

                    truncate table #descrs

                      insert into #descrs(scaling_segment_id, descrs)
                      select scaling_segment_id
                            ,sum(decr_diff)
                        from #segment_THIs                            as bas
                             cross join PanBal_all_aggregated_results as agg
                       where bas.universe          <> agg.variable_value
                         and aggregation_variable   = 'UNIVERSE'
                    group by scaling_segment_id

                      update #segment_THIs as bas
                         set thi = thi + descrs
                        from #descrs as dsc
                       where bas.scaling_segment_id = dsc.scaling_segment_id

                    truncate table #descrs

                      insert into #descrs(scaling_segment_id, descrs)
                      select scaling_segment_id
                            ,sum(decr_diff)
                        from #segment_THIs                            as bas
                             cross join PanBal_all_aggregated_results as agg
                       where bas.isba_tv_region          <> agg.variable_value
                         and aggregation_variable   = 'REGION'
                    group by scaling_segment_id

                      update #segment_THIs as bas
                         set thi = thi + descrs
                        from #descrs as dsc
                       where bas.scaling_segment_id = dsc.scaling_segment_id

                    truncate table #descrs

                      insert into #descrs(scaling_segment_id, descrs)
                      select scaling_segment_id
                            ,sum(decr_diff)
                        from #segment_THIs                            as bas
                             cross join PanBal_all_aggregated_results as agg
                       where bas.hhcomposition          <> agg.variable_value
                         and aggregation_variable   = 'HHCOMP'
                    group by scaling_segment_id

                      update #segment_THIs as bas
                         set thi = thi + descrs
                        from #descrs as dsc
                       where bas.scaling_segment_id = dsc.scaling_segment_id

                    truncate table #descrs

                      insert into #descrs(scaling_segment_id, descrs)
                      select scaling_segment_id
                            ,sum(decr_diff)
                        from #segment_THIs                            as bas
                             cross join PanBal_all_aggregated_results as agg
                       where bas.package          <> agg.variable_value
                         and aggregation_variable   = 'PACKAGE'
                    group by scaling_segment_id

                      update #segment_THIs as bas
                         set thi = thi + descrs
                        from #descrs as dsc
                       where bas.scaling_segment_id = dsc.scaling_segment_id

                    truncate table #descrs

                      insert into #descrs(scaling_segment_id, descrs)
                      select scaling_segment_id
                            ,sum(decr_diff)
                        from #segment_THIs                            as bas
                             cross join PanBal_all_aggregated_results as agg
                       where bas.tenure          <> agg.variable_value
                         and aggregation_variable   = 'TENURE'
                    group by scaling_segment_id

                      update #segment_THIs as bas
                         set thi = thi + descrs
                        from #descrs as dsc
                       where bas.scaling_segment_id = dsc.scaling_segment_id

                    truncate table #descrs

                      insert into #descrs(scaling_segment_id, descrs)
                      select scaling_segment_id
                            ,sum(decr_diff)
                        from #segment_THIs                            as bas
                             cross join PanBal_all_aggregated_results as agg
                       where bas.boxtype          <> agg.variable_value
                         and aggregation_variable   = 'BOXTYPE'
                    group by scaling_segment_id

                      update #segment_THIs as bas
                         set thi = thi + descrs
                        from #descrs as dsc
                       where bas.scaling_segment_id = dsc.scaling_segment_id

                    --update THI for alternate day panels
                      update PanBal_list3 as bas
                         set bas.thi = thi.thi * rq
                        from #segment_THIs as thi
                       where bas.scaling_segment_id = thi.scaling_segment_id

                    truncate table #new_adds

                      insert into #new_adds
                      select li3.account_number
                            ,li3.scaling_segment_id
                            ,thi
                        from panbal_list3 as li3
                             left join panbal_panel as bas on li3.account_number = bas.account_number
                       where thi > 0
                         and bas.account_number is null

                      select @cow = count(1)
                        from #new_adds

                    if (@cow >= @precision) --if there are a reasonable amount that can be added
                         begin
                                   set rowcount @precision -- only show this many lines in all queries

                                insert into panbal_panel(
                                       account_number
                                      ,scaling_segment_id
                                )
                                select account_number
                                      ,scaling_segment_id
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
                                where bas.scaling_segment_id = thi.scaling_segment_id

                               delete from panbal_panel where account_number in (select account_number from panbal_list3)

                                  set rowcount @precision

                               insert into panbal_panel(account_number
                                                       ,scaling_segment_id)
                               select li4.account_number
                                     ,li4.scaling_segment_id
                                 from panbal_list4 as li4
                                where thi > 0
                             order by thi desc

                                  set rowcount 0

                               delete from panbal_list4 where account_number in (select account_number from panbal_panel)

                         end --begin
                    --end if
               --end begin
          end --if

          select @tot_imb = sum((100-good_household_index) * (100-good_household_index)) from PanBal_all_aggregated_results

          insert into PanBal_results(imbalance, tot_imb, records, tim) select @imbalance, @tot_imb, count(1), now() from panbal_panel

          commit
     end; --begin
--end while

--count boxes for every account
  select distinct (ccs.account_number)
        ,count(distinct card_subscriber_id) as boxes
    into #sky_box_count
    from sk_prod.CUST_CARD_SUBSCRIBER_LINK as ccs
         inner join sk_prod.cust_single_account_view as sav on ccs.account_number = sav.account_number
   where effective_to_dt = '9999-09-09'
     and cust_active_dtv = 1
group by ccs.account_number
;
create unique hg index idx1 on #sky_box_count(account_number);

truncate table PanBal_Amends;

--accounts to add to panel 12 from panels 6/7
  insert into PanBal_Amends(account_number, movement)
  select bas.account_number
        ,'Account to add to panel 12 from panel 6 or 7'
    from panbal_panel as bas
         inner join panbal_list3 as li3 on bas.account_number = li3.account_number
;

--check for <750k boxes
  select @cow = sum(boxes)
    from panbal_panel as bas
         inner join #sky_box_count as sbc on bas.account_number = sbc.account_number
;

  select @cow = @cow + sum(boxes)
    from panbal_list1 as li1
         inner join #sky_box_count as sbc on li1.account_number = sbc.account_number
;

if (@cow > 750000)
     begin
            insert into #list1_rq(account_number, rq, boxes)
            select sbv.account_number
                  ,max(reporting_quality) as rq
                  ,count(subscriber_id) as boxes
              from vespa_analysts.vespa_single_box_view as sbv
                   inner join panbal_list1 as li1 on sbv.account_number = li1.account_number
             where status_vespa = 'Enabled'
          group by sbv.account_number

          create table #temp(account_number varchar(30), boxes int)

          while (@cow > 750000)
               begin
                       insert into #temp
                       select top 1000 account_number
                             ,boxes
                         from #list1_rq
                     order by rq,account_number

                       select @cow = sum(boxes)
                         from panbal_panel as bas
                              inner join #sky_box_count as sbc on bas.account_number = sbc.account_number

                       select @cow = @cow + sum(boxes)
                         from panbal_list1 as li1
                              inner join #sky_box_count as sbc on li1.account_number = sbc.account_number

                       select @cow = @cow - sum(boxes) from #temp

                       delete from #list1_rq where account_number in (select account_number from #temp)

               end
          --end while

       insert into PanBal_amends(account_number, movement)
       select account_number
             ,'Account to remove from panel 12'
         from #temp

     end --begin
--end if


select movement,count(1) from panbal_amends group by movement


--now need to remove the ones we've added fom list3 (we have already deleted the ones we've added from list4)
  delete from PanBal_list3 where account_number in (select account_number from PanBal_panel)
;

--accounts to add to alt. panels, to make 50% in each segment (if poss)
  select sum(case when pan.account_number is null then 0 else 1 end)                                    as vespa
        ,sum(case when alt.account_number is not null and pan.account_number is null then 1 else 0 end) as alt
        ,bss.scaling_segment_id
    into #panels
    from vespa_analysts.SC2_Sky_base_segment_snapshots as bss
         left join panbal_panel                        as pan on bss.account_number = pan.account_number
         left join panbal_list3                        as alt on bss.account_number = alt.account_number
group by bss.scaling_segment_id
;

  select scaling_segment_id
        ,vespa - (alt*2) as reqd
    into #reqd
    from #panels
   where reqd > 0
;

  select wat.account_number
        ,wat.scaling_segment_id
        ,rank() over (partition by wat.scaling_segment_id order by cbck_rate desc, vp1 desc) as rnk
        ,vp1
        ,boxes
    into #available
    from PanBal_list4                                              as wat
         inner join #cbck_rate                                     as cbr on wat.account_number = cbr.account_number
         inner join #sky_box_count                                 as sbc on wat.account_number = sbc.account_number
         left join vespa_analysts.vespa_broadcast_reporting_vp_map as vir on wat.account_number = vir.account_number
         left join panbal_panel                                    as bas on wat.account_number = bas.account_number
   where bas.account_number is null
;

  insert into PanBal_amends(account_number, movement)
  select account_number
        ,'Account to add to Panel 6 or 7 as segment backup'
    from #available        as ava
         inner join  #reqd as req on ava.scaling_segment_id = req.scaling_segment_id
   where rnk <= reqd
;

  select @virtuals = sum(boxes) --count boxes on the virtual panel on the new panel
    from panbal_panel                                               as bas
         inner join vespa_analysts.vespa_broadcast_reporting_vp_map as vir on bas.account_number = vir.account_number
         inner join #sky_box_count                                  as sbc on bas.account_number = sbc.account_number
   where vp1 = 1
;

  select @virtuals = @virtuals + sum(boxes) --add on the remaining accounts left on list1
    from panbal_list1                                               as bas
         inner join vespa_analysts.vespa_broadcast_reporting_vp_map as vir on bas.account_number = vir.account_number
         inner join #sky_box_count                                  as sbc on bas.account_number = sbc.account_number
         left join panbal_amends                                    as ame on bas.account_number = ame.account_number
   where vp1 = 1
     and ame.account_number is null
;

  select @virtuals = @virtuals + sum(boxes) --add on the remaining accounts left on list3
    from panbal_list3                                               as bas
         inner join vespa_analysts.vespa_broadcast_reporting_vp_map as vir on bas.account_number = vir.account_number
         inner join #sky_box_count                                  as sbc on bas.account_number = sbc.account_number
         left join panbal_amends                                    as ame on bas.account_number = ame.account_number
   where vp1 = 1
     and ame.account_number is null
;

--we are still missing the alt. panel accounts with unacceptable reporting:
  select @virtuals = @virtuals + sum(boxes)
    from vespa_analysts.vespa_panman_all_households as bas
         left join vespa_analysts.ConsentIssue_05_Revised_Consent_Info as exc on bas.account_number = exc.account_number
   where panel <> 'VESPA'
     and reporting_quality < .8
     and scaling_segment_ID is not null
     and exc.account_number is null

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

                          if (@cow = 0)
                              begin

                                insert into PanBal_amends(account_number, movement)
                                select null
                                      ,300000 - @virtuals || ' more boxes needed on the virtual panel'

                                   set @virtuals = 300000

                              end
                          --end if

                      delete from #list1_rq where account_number in (select account_number from #temp)

                      insert into PanBal_amends(account_number, movement)
                      select account_number
                            ,'Account to add to panel 6/7 for virtual panel req.'
                        from #temp

               end
          --end while


     end --begin
--end if

--recreate list4
truncate table panbal_list4;

  insert into PanBal_list4
  select bas.account_number
        ,scaling_segment_id
        ,0
        ,0
    from vespa_analysts.waterfall_base                                 as bas
         inner join vespa_analysts.SC2_Sky_base_segment_snapshots      as bss on bas.account_number = bss.account_number
         left join vespa_analysts.ConsentIssue_05_Revised_Consent_Info as exc on bas.account_number = exc.account_number
   where knockout_level = 9999
     and profiling_date = @recent_profiling_date
     and exc.account_number is null
group by bas.account_number
        ,scaling_segment_id
;

--New accounts to add to alternate day panels
  insert into PanBal_Amends(account_number, movement)
  select bas.account_number
        ,'Account to add to panels 6/7, eventually for panel 12'
    from panbal_panel as bas
         inner join panbal_list4 as li4 on bas.account_number = li4.account_number
;


--drop tables
drop table panbal_list1;
drop table panbal_list2;
drop table panbal_list3;
drop table panbal_list4;
drop table PanBal_all_aggregated_results;
drop table PanBal_Scaling_Segment_Profiling;
drop table PanBal_traffic_lights;
drop table PanBal_results;
drop table PanBal_panel;
--just leave us with Panbal_Amends






