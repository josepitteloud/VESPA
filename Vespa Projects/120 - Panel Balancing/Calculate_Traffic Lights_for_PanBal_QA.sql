--First run PanMan 00 - persistent table creation.sql if required
--Note line 119-120 amend to fudge RQ for new accounts

-- sample preparation
drop table Traffic_Light;

select subscriber_id
      ,account_number
      ,panel
      ,reporting_quality
  into Traffic_Light
  from vespa_analysts.vespa_single_box_view
 where panel in ('VESPA', 'ALT6', 'ALT7')
   and status_vespa='Enabled'
;
commit;

create hg index idx1 on Traffic_Light(account_number);

update Traffic_Light a
   set panel = 'VESPA'
  from greenj.panbal_amends b
 where a.account_number = b.account_number
   and b.movement in ('Account to add to panel 12 from panel 6 or 7',
                      'Account to add to panels 6/7, eventually for panel 12');
commit;

insert into Traffic_Light
       (subscriber_id,account_number,Panel)
select 0
      ,account_number
      ,'VESPA'
  from greenj.panbal_amends
 where movement in ('Account to add to panel 12 from panel 6 or 7',
                    'Account to add to panels 6/7, eventually for panel 12')
   and account_number not in (select account_number
                                from Traffic_Light);
commit;

delete from Traffic_Light where account_number in (select account_number from greenj.panbal_amends where movement = 'Account to remove from panel 12');

delete from traffic_light where panel like 'ALT%';
commit;

if object_id(    'PanMan_make_report') is not null
   drop procedure PanMan_make_report;

commit;



create procedure  PanMan_make_report
   @profiling_thursday         date    = null
as
begin

     -- ****************** A01: SETTING UP THE LOGGER ******************
/*
     create variable @PanMan_logging_ID      bigint;
     create variable @Refresh_identifier     varchar(40);
     create variable @run_Identifier         varchar(20);
     create variable @recent_profiling_date date;
     create variable @TrafficLights_stdCount integer;
     create variable @QA_catcher             integer;
     create variable @exe_status       integer;
     create variable @profiling_thursday date;
     create variable @total_sky_base                 int
*/
     DECLARE @PanMan_logging_ID      bigint
     DECLARE @Refresh_identifier     varchar(40)
     declare @run_Identifier         varchar(20)
     declare @recent_profiling_date date
     DECLARE @TrafficLights_stdCount integer
     DECLARE @QA_catcher             integer
     declare @exe_status       integer

     set @TrafficLights_stdCount = 11


     -- ****************** A02: TABLE RESETS ******************

     execute PanMan_clear_transients
     commit

     -- ****************** A03: TIME BOUNDS ******************

     -- Might not use this for a whole lot, given how much gets done in the midway scaling tables
     if @profiling_thursday is null
     begin
         execute vespa_analysts.Regulars_Get_report_end_date @profiling_thursday output -- A Saturday
         set @profiling_thursday    = dateadd(day, -8, @profiling_thursday) -- Thursday of SAV flip data, but we're now profiling from the beginning of the period.
     end
     commit


     -- ****************** A04: DEPENDENCY COMPLETENESS CHECK ******************

     select @recent_profiling_date = max(profiling_date)
       from vespa_analysts.SC2_Sky_base_segment_snapshots

     select account_number,scaling_segment_id
       into #Scaling_weekly_sample
       from vespa_analysts.SC2_Sky_base_segment_snapshots
      where profiling_date = @recent_profiling_date

     -- ****************** B02: INDEXING PANELS AGAINST THE SKY BASE ******************

       insert into Vespa_PanMan_all_households (
              account_number
             ,hh_box_count       -- not directly used? but might be interesting
             ,most_recent_enablement
             ,reporting_categorisation
             ,reporting_quality
             ,panel
       )
       select account_number
             ,count(1)
             ,'9999-09-09'
             ,case
--                   when min(reporting_quality) >= 0.8                                         then 'Acceptable'
                   when min(coalesce(reporting_quality,1)) >= 0.8                                         then 'Acceptable'
                                                                                           else 'Other'
                   end
             ,min(reporting_quality)  -- Used much later in the box selection bit, but may as well build it now
             ,min(panel)              -- This guy should be unique per account, we test for that coming off SBV
         from Traffic_Light
     group by account_number

     commit

     update Vespa_PanMan_all_households
        set scaling_segment_id = tws.scaling_segment_id
      from Vespa_PanMan_all_households
           inner join #Scaling_weekly_sample as tws on Vespa_PanMan_all_households.account_number = tws.account_number

     update Vespa_PanMan_all_households
        set non_scaling_segment_id = tsnss.non_scaling_segment_id
       from Vespa_PanMan_all_households
            inner join vespa_analysts.Vespa_PanMan_this_weeks_non_scaling_segmentation as tsnss on Vespa_PanMan_all_households.account_number = tsnss.account_number

     update Vespa_PanMan_all_households as bas
        set non_scaling_segment_id = nss.non_scaling_segment_id
      from vespa_analysts.Vespa_PanMan_this_weeks_non_scaling_segmentation as nss
      where bas.account_number = nss.account_number

     commit
       select scaling_segment_id
--             ,non_scaling_segment_id
         --  Doesn't include scaling_segment_name, we stitch that in later
             ,count(1) as Sky_Base_Households
         into #sky_base_segmentation
         from #Scaling_weekly_sample as tws
--              inner join vespa_analysts.Vespa_PanMan_this_weeks_non_scaling_segmentation as tsnss on tws.account_number = tsnss.account_number
        where tws.scaling_segment_ID is not null
        --and tsnss.non_scaling_segment_id is not null -- That annoying case of the region 'Eire' guy which is Ireland and therefore shouldn't be in the Vespa dataset, but whatever
     group by tws.scaling_segment_ID
     --, tsnss.non_scaling_segment_id
     -- It has to go into a temp table because we duplicate all these number for each panel

     commit

     -- We need control totals for each panel later, but right now we need this to duplicate the sky base numbers for each panel...
       select panel
             ,count(1) as panel_reporters
         into #panel_totals
         from Vespa_PanMan_all_households
        where reporting_categorisation = 'Acceptable'
     group by panel

     commit
--deleted non-scaling index
     insert into Vespa_PanMan_Scaling_Segment_Profiling (
            panel
           ,scaling_segment_id
--           ,non_scaling_segment_id
       --  Doens't include scaling_segment_name, we stitch that in later
           ,Sky_Base_Households
     )
     select pt.panel
           ,sb.*
       from #sky_base_segmentation as sb
            cross join #panel_totals as pt -- want all the combinations of stuff

     commit

     -- Now with the marks in plac we can group things into segments: even though we have the scaling
     -- and non-scaling segments on the all households table, that's only for boxes on a panel and here
     -- we need the whole sky base. Good thing we've already got that built then, and added into the
     -- main table for each panel.
       select panel
             ,scaling_segment_id
--             ,non_scaling_segment_id
             ,count(1) as Panel_Households
             ,sum(case when reporting_categorisation = 'Acceptable'       then 1 else 0 end) as Acceptably_reliable_households
             ,sum(case when reporting_categorisation = 'Unreliable'       then 1 else 0 end) as Unreliable_households
             ,sum(case when reporting_categorisation = 'Zero reporting'   then 1 else 0 end) as Zero_reporting_households
             ,sum(case when reporting_categorisation = 'Recently enabled' then 1 else 0 end) as Recently_enabled_households
         into #panel_segmentation
         from Vespa_PanMan_all_households as hr
        where scaling_segment_ID is not null
        --and non_scaling_segment_id is not null -- That annoying case of the region 'Eire' guy which is Ireland and therefore shouldn't be in the Vespa dataset, but whatever
     group by panel, scaling_segment_ID
     --, non_scaling_segment_id

     commit
     create unique index fake_pk on #panel_segmentation (panel, scaling_segment_id)

     -- Now with the totals built for each panel, we can throw them into the table with the Sky base:
     update Vespa_PanMan_Scaling_Segment_Profiling
        set Panel_Households                = ps.Panel_Households
           ,Acceptably_reliable_households = ps.Acceptably_reliable_households
           ,Unreliable_households          = ps.Unreliable_households
           ,Zero_reporting_households      = ps.Zero_reporting_households
           ,Recently_enabled_households    = ps.Recently_enabled_households
       from Vespa_PanMan_Scaling_Segment_Profiling
            inner join #panel_segmentation as ps on Vespa_PanMan_Scaling_Segment_Profiling.panel                    = ps.panel
                                                and Vespa_PanMan_Scaling_Segment_Profiling.scaling_segment_id       = ps.scaling_segment_id
--                                                and Vespa_PanMan_Scaling_Segment_Profiling.non_scaling_segment_id   = ps.non_scaling_segment_id

     commit
     drop table #sky_base_segmentation
     drop table #panel_segmentation
     -- execute logger_add_event @PanMan_logging_ID, 4, 'B02: Ongoing (Profiler built)'
     commit

     -- Patch in the scaling segment name from the lookup...
     update Vespa_PanMan_Scaling_Segment_Profiling
        set scaling_segment_name = ssl.scaling_segment_name
       from Vespa_PanMan_Scaling_Segment_Profiling
            inner join vespa_analysts.SC2_Segments_lookup_v2_1 as ssl on Vespa_PanMan_Scaling_Segment_Profiling.scaling_segment_ID = ssl.scaling_segment_ID

     update Vespa_PanMan_Scaling_Segment_Profiling
        set non_scaling_segment_name = nssl.non_scaling_segment_name
       from Vespa_PanMan_Scaling_Segment_Profiling
            inner join Vespa_PanMan_non_scaling_segments_lookup as nssl on Vespa_PanMan_Scaling_Segment_Profiling.non_scaling_segment_ID = nssl.non_scaling_segment_ID

     commit

     -- We do need the indices in-database though, since we make decisions based on them etc.
     declare @total_sky_base                 int
     -- With the new normalised structures, panel totals just go into a table...

     -- We need the size of the sky base for indexing calculations
     select @total_sky_base     = sum(Sky_Base_Households)
       from Vespa_PanMan_Scaling_Segment_Profiling
      where panel = 'VESPA'

     commit

     -- Now simplified because we'll only be dividing by things in cases where we've got
     -- the appropriate panel stuff in the table:
     update Vespa_PanMan_Scaling_Segment_Profiling
       set Acceptably_reporting_index         = -- *sigh* there's no GREATEST / LEAST operator in this DB...
             case when 200 < 100 * (Acceptably_reliable_households)   * @total_sky_base / convert(float, Sky_Base_Households) / pt.panel_reporters then 200
                  else       100 * (Acceptably_reliable_households)   * @total_sky_base / convert(float, Sky_Base_Households) / pt.panel_reporters
             end
       from Vespa_PanMan_Scaling_Segment_Profiling
            inner join #panel_totals as pt on Vespa_PanMan_Scaling_Segment_Profiling.panel = pt.panel
     -- Not dropping #panel_totals here because we still need it for the single variable summaries

     -- Still... What are we pulling out to report this? One graph for Vespa Live, one for
     -- each alternate...

     -- execute logger_add_event @PanMan_logging_ID, 3, 'B02: Complete! (Indexing panels)', coalesce(@QA_catcher, -1)
     commit

  -- execute logger_add_event @PanMan_logging_ID, 4, 'B02-1 DML command status: '||@@error

     -- ****************** B03: AGGREGATING TO VARIABLE VIEWS ******************

     -- So this is the bit that's specific from one scaling build to the next; we need
     -- individual variables here. And because we don't want to introduce any bias from
     -- how we calculated the indices on the segments above, we'll do it from the account
     -- level stuff. But we still have to join in the lookups to get the IDs across the
     -- variables we want:

     -- (Now with improved normalisation, helped by the merging of reliable & somewhat
     -- reliable into one category...)

       insert into Vespa_PanMan_all_aggregated_results
       select ssp.panel
             ,'UNIVERSE' -- Name of variable being profiled
             ,1          -- Whether the variable is used for scaling or not (determintes the results sheet pull)
             ,ssl.universe
             ,sum(Sky_Base_Households)
             ,sum(Panel_households)
             ,sum(Acceptably_reliable_households)
             ,sum(Unreliable_households)
             ,sum(Zero_reporting_households)
             ,sum(Recently_enabled_households)
             ,null
         from Vespa_PanMan_Scaling_Segment_Profiling as ssp
              inner join vespa_analysts.SC2_Segments_lookup_v2_1 as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
     group by ssp.panel, ssl.universe

     commit

       insert into Vespa_PanMan_all_aggregated_results
       select ssp.panel
             ,'REGION'   -- Name of variable being profiled
             ,1          -- Whether the variable is used for scaling or not (determintes the results sheet pull)
             ,ssl.isba_tv_region
             ,sum(Sky_Base_Households)
             ,sum(Panel_households)
             ,sum(Acceptably_reliable_households)
             ,sum(Unreliable_households)
             ,sum(Zero_reporting_households)
             ,sum(Recently_enabled_households)
             ,null
         from Vespa_PanMan_Scaling_Segment_Profiling as ssp
              inner join vespa_analysts.SC2_Segments_lookup_v2_1 as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
     group by ssp.panel, ssl.isba_tv_region

     commit

     insert into Vespa_PanMan_all_aggregated_results
       select ssp.panel
             ,'HHCOMP'
             ,1
             ,ssl.hhcomposition
             ,sum(Sky_Base_Households)
             ,sum(Panel_households)
             ,sum(Acceptably_reliable_households)
             ,sum(Unreliable_households)
             ,sum(Zero_reporting_households)
             ,sum(Recently_enabled_households)
             ,null
         from Vespa_PanMan_Scaling_Segment_Profiling as ssp
              inner join vespa_analysts.SC2_Segments_lookup_v2_1 as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
     group by ssp.panel, ssl.hhcomposition

     commit

       insert into Vespa_PanMan_all_aggregated_results
       select ssp.panel
             ,'PACKAGE'
             ,1
             ,ssl.package
             ,sum(Sky_Base_Households)
             ,sum(Panel_households)
             ,sum(Acceptably_reliable_households)
             ,sum(Unreliable_households)
             ,sum(Zero_reporting_households)
             ,sum(Recently_enabled_households)
             ,null
         from Vespa_PanMan_Scaling_Segment_Profiling as ssp inner join vespa_analysts.SC2_Segments_lookup_v2_1 as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
     group by ssp.panel, ssl.package

     commit

       insert into Vespa_PanMan_all_aggregated_results
       select ssp.panel
             ,'TENURE'
             ,1
             ,ssl.tenure
             ,sum(Sky_Base_Households)
             ,sum(Panel_households)
             ,sum(Acceptably_reliable_households)
             ,sum(Unreliable_households)
             ,sum(Zero_reporting_households)
             ,sum(Recently_enabled_households)
             ,null
         from Vespa_PanMan_Scaling_Segment_Profiling as ssp
              inner join vespa_analysts.SC2_Segments_lookup_v2_1 as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
     group by ssp.panel, ssl.tenure

     commit

       insert into Vespa_PanMan_all_aggregated_results
       select ssp.panel
             ,'BOXTYPE'
             ,1
             ,ssl.boxtype
             ,sum(Sky_Base_Households)
             ,sum(Panel_households)
             ,sum(Acceptably_reliable_households)
             ,sum(Unreliable_households)
             ,sum(Zero_reporting_households)
             ,sum(Recently_enabled_households)
             ,null
         from Vespa_PanMan_Scaling_Segment_Profiling as ssp
              inner join vespa_analysts.SC2_Segments_lookup_v2_1 as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
     group by ssp.panel, ssl.boxtype

     commit

     -- Then other things that we're not scaling by, but we'd still like for panel balance:
       insert into Vespa_PanMan_all_aggregated_results
       select ssp.panel
             ,'VALUESEG'
             ,0          -- indicates we're not scaling by this, because these variables are pulled onto a different sheet
             ,nssl.value_segment
             ,sum(Sky_Base_Households)
             ,sum(Panel_households)
             ,sum(Acceptably_reliable_households)
             ,sum(Unreliable_households)
             ,sum(Zero_reporting_households)
             ,sum(Recently_enabled_households)
             ,null
         from Vespa_PanMan_Scaling_Segment_Profiling as ssp
              inner join Vespa_PanMan_non_scaling_segments_lookup as nssl on ssp.non_scaling_segment_ID = nssl.non_scaling_segment_ID
     group by ssp.panel, nssl.value_segment

     commit

       insert into Vespa_PanMan_all_aggregated_results
       select ssp.panel
             ,'MOSAIC'
             ,0
             ,nssl.Mosaic_segment -- Special treatment for the MOSAIC segment names gets handled at the end
             ,sum(Sky_Base_Households)
             ,sum(Panel_households)
             ,sum(Acceptably_reliable_households)
             ,sum(Unreliable_households)
             ,sum(Zero_reporting_households)
             ,sum(Recently_enabled_households)
             ,null
         from Vespa_PanMan_Scaling_Segment_Profiling as ssp
              inner join Vespa_PanMan_non_scaling_segments_lookup as nssl on ssp.non_scaling_segment_ID = nssl.non_scaling_segment_ID
     group by ssp.panel, nssl.Mosaic_segment

     commit

       insert into Vespa_PanMan_all_aggregated_results
       select ssp.panel
             ,'FINANCIALSTRAT'
             ,0
             ,nssl.Financial_strategy_segment
             ,sum(Sky_Base_Households)
             ,sum(Panel_households)
             ,sum(Acceptably_reliable_households)
             ,sum(Unreliable_households)
             ,sum(Zero_reporting_households)
             ,sum(Recently_enabled_households)
             ,null
         from Vespa_PanMan_Scaling_Segment_Profiling as ssp
              inner join Vespa_PanMan_non_scaling_segments_lookup as nssl on ssp.non_scaling_segment_ID = nssl.non_scaling_segment_ID
     group by ssp.panel, nssl.Financial_strategy_segment

     commit

       insert into Vespa_PanMan_all_aggregated_results
       select ssp.panel
             ,'ONNET'
             ,0
             ,case when nssl.is_OnNet = 1 then '1.) OnNet' else '2.) OffNet' end
             ,sum(Sky_Base_Households)
             ,sum(Panel_households)
             ,sum(Acceptably_reliable_households)
             ,sum(Unreliable_households)
             ,sum(Zero_reporting_households)
             ,sum(Recently_enabled_households)
             ,null
         from Vespa_PanMan_Scaling_Segment_Profiling as ssp
              inner join Vespa_PanMan_non_scaling_segments_lookup as nssl on ssp.non_scaling_segment_ID = nssl.non_scaling_segment_ID
     group by ssp.panel, nssl.is_OnNet

     commit

       insert into Vespa_PanMan_all_aggregated_results
       select ssp.panel
             ,'SKYGO'
             ,0
             ,case when nssl.uses_sky_go = 1 then '1.) Uses Sky Go' else '2.) No Sky Go' end
             ,sum(Sky_Base_Households)
             ,sum(Panel_households)
             ,sum(Acceptably_reliable_households)
             ,sum(Unreliable_households)
             ,sum(Zero_reporting_households)
             ,sum(Recently_enabled_households)
             ,null
         from Vespa_PanMan_Scaling_Segment_Profiling as ssp
              inner join Vespa_PanMan_non_scaling_segments_lookup as nssl on ssp.non_scaling_segment_ID = nssl.non_scaling_segment_ID
     group by ssp.panel, nssl.uses_sky_go

     commit

     -- Okay, now all of that is done, we can patch the index calculations into
     -- the whole lot at once (the variables got calculated further up when we
     -- did indices for each segment):
     update Vespa_PanMan_all_aggregated_results
        set Good_Household_Index = case when 200 < 100 * (Acceptable_Households) * @total_sky_base / convert(float, Sky_Base_Households) / pt.panel_reporters then 200
                                        else       100 * (Acceptable_Households) * @total_sky_base / convert(float, Sky_Base_Households) / pt.panel_reporters
                                   end
       from Vespa_PanMan_all_aggregated_results
            inner join #panel_totals as pt on Vespa_PanMan_all_aggregated_results.panel = pt.panel

     -- execute logger_add_event @PanMan_logging_ID, 4, 'B03: Ongoing. (Balance indexing)'

     commit

     -- Oh, and also, we want to tack the full names onto the Experian 3rd party
     -- things...

     update Vespa_PanMan_all_aggregated_results
       set variable_value = case variable_value
           when '00' then '00: Families'
           when '01' then '01: Extended family'
           when '02' then '02: Extended household'
           when '03' then '03: Pseudo family'
           when '04' then '04: Single male'
           when '05' then '05: Single female'
           when '06' then '06: Male homesharers'
           when '07' then '07: Female homesharers'
           when '08' then '08: Mixed homesharers'
           when '09' then '09: Abbreviated male families'
           when '10' then '10: Abbreviated female families'
           when '11' then '11: Multi-occupancy dwelling'
           else 'U: Unclassified HHComp' end
     where aggregation_variable = 'HHCOMP'

     -- execute logger_add_event @PanMan_logging_ID, 4, 'B03: Ongoing. (HHComposition update)'

     commit

     update Vespa_PanMan_all_aggregated_results
        set variable_value = case variable_value
         when 'A' then 'A: Alpha Territory'
         when 'B' then 'B: Professional Rewards'
         when 'C' then 'C: Rural Solitude'
         when 'D' then 'D: Small Town Diversity'
         when 'E' then 'E: Active Retirement'
         when 'F' then 'F: Suburban Mindsets'
         when 'G' then 'G: Careers and Kids'
         when 'H' then 'H: New Homemakers'
         when 'I' then 'I: Ex-Council Community'
         when 'J' then 'J: Claimant Cultures'
         when 'K' then 'K: Upper Floor Living'
         when 'L' then 'L: Elderly Needs'
         when 'M' then 'M: Industrial Heritage'
         when 'N' then 'N: Terraced Melting Pot'
         when 'O' then 'O: Liberal Opinions'
         else 'U: Unknown MOSAIC' end
     where aggregation_variable = 'MOSAIC'

     -- execute logger_add_event @PanMan_logging_ID, 4, 'B03: Ongoing. (MOSAIC update)'

     commit

     update Vespa_PanMan_all_aggregated_results
        set variable_value = case variable_value
         when 'A' then 'A: Successful Start'
         when 'B' then 'B: Happy Housemates'
         when 'C' then 'C: Surviving Singles'
         when 'D' then 'D: On The Breadline'
         when 'E' then 'E: Flourishing Families'
         when 'F' then 'F: Credit Hungry Families'
         when 'G' then 'G: Gilt Edged Lifestyles'
         when 'H' then 'H: Mid Life Affluence'
         when 'I' then 'I: Modest Mid Years'
         when 'J' then 'J: Advancing Status'
         when 'K' then 'K: Ageing Workers'
         when 'L' then 'L: Wealthy Retirement'
         when 'M' then 'M: Elderly Deprivation'
         else 'U: Unknown FSS' end
     where aggregation_variable = 'FINANCIALSTRAT'

     -- execute logger_add_event @PanMan_logging_ID, 4, 'B03: Ongoing. (Financial strategy update)'

     commit

       select panel -- it gets denormalised in the extraction query though...
             ,case aggregation_variable
               when 'UNIVERSE'         then 'Universe'
               when 'REGION'           then 'Region'
               when 'HHCOMP'           then 'Household composition'
               when 'PACKAGE'          then 'Package'
               when 'TENURE'           then 'Tenure'
               when 'BOXTYPE'          then 'Box type'
               when 'VALUESEG'         then 'Value segment'
               when 'MOSAIC'           then 'MOSAIC'
               when 'FINANCIALSTRAT'   then 'FSS'
               when 'ONNET'            then 'OnNet / Offnet'
               when 'SKYGO'            then 'Sky Go users'
               else 'FAIL!'
              end as variable_name
             ,case aggregation_variable
               when 'UNIVERSE'         then 1
               when 'REGION'           then 2
               when 'HHCOMP'           then 3
               when 'PACKAGE'          then 4
               when 'TENURE'           then 5
               when 'BOXTYPE'          then 6
               when 'VALUESEG'         then 7
               when 'MOSAIC'           then 8
               when 'FINANCIALSTRAT'   then 9
               when 'ONNET'            then 10
               when 'SKYGO'            then 11
               else -1
              end as sequencer -- so the results go out into the excel thing in the right order
             ,sqrt(avg(
               (Good_Household_Index - 100) * (Good_Household_Index - 100)           )) as imbalance_rating
         into Vespa_PanMan_09_traffic_lights
         from Vespa_PanMan_all_aggregated_results
      where (aggregation_variable = 'BOXTYPE')
         or (aggregation_variable = 'FINANCIALSTRAT' and variable_value <> 'U: Unknown FSS')
         or (aggregation_variable = 'HHCOMP' and variable_value <> 'U: Unclassified HHComp')
         or (aggregation_variable = 'MOSAIC' and variable_value <> 'U: Unknown MOSAIC')
         or (aggregation_variable = 'ONNET')
         or (aggregation_variable = 'PACKAGE')
         or (aggregation_variable = 'REGION' and variable_value <> 'Not Defined')
         or (aggregation_variable = 'SKYGO')
         or (aggregation_variable = 'TENURE' and variable_value <> 'D) Unknown')
         or (aggregation_variable = 'UNIVERSE')
         or (aggregation_variable = 'VALUESEG')
     group by panel, aggregation_variable

end; --PanMan_make_report

commit;

-- And somethign else to clean up the junk that was built:
if object_id('PanMan_clear_transients') is not null
   drop procedure PanMan_clear_transients;

commit;

create procedure PanMan_clear_transients
as
begin
    -- For some reason, these guys needed the explicit schema references while inside a
    -- proc that was called by a different user. Weird.
    -- ##32## - are we recasting this so that the schema is automatically detected?
    delete from Vespa_PanMan_all_households
    delete from Vespa_PanMan_Scaling_Segment_Profiling
    delete from Vespa_PanMan_this_weeks_non_scaling_segmentation
    delete from Vespa_PanMan_all_aggregated_results
    delete from Vespa_PanMan_panel_redundancy_calculations
    if object_id( 'vespa_PanMan_02_vespa_panel_overall') is not null
        drop table vespa_PanMan_02_vespa_panel_overall
    if object_id( 'vespa_PanMan_03_panel_6_overall') is not null
        drop table vespa_PanMan_03_panel_6_overall
    if object_id( 'vespa_PanMan_04_panel_7_overall') is not null
        drop table vespa_PanMan_04_panel_7_overall
    if object_id( 'Vespa_PanMan_08_ordered_weightings') is not null
        drop table Vespa_PanMan_08_ordered_weightings
    if object_id( 'vespa_PanMan_09_traffic_lights') is not null
        drop table vespa_PanMan_09_traffic_lights
    if object_id( 'vespa_PanMan_11_panel_4_discontinuations') is not null
        drop table vespa_PanMan_11_panel_4_discontinuations
    if object_id( 'vespa_PanMan_12_panel_6_imports') is not null
        drop table vespa_PanMan_12_panel_6_imports
    if object_id( 'vespa_PanMan_13_panel_7_imports') is not null
        drop table vespa_PanMan_13_panel_7_imports
    if object_id( 'vespa_PanMan_42_vespa_panel_single_box_HHs') is not null
        drop table vespa_PanMan_42_vespa_panel_single_box_HHs
    if object_id( 'vespa_PanMan_43_vespa_panel_dual_box_HHs') is not null
        drop table vespa_PanMan_43_vespa_panel_dual_box_HHs
    if object_id( 'vespa_PanMan_44_vespa_panel_multi_box_HHs') is not null
        drop table vespa_PanMan_44_vespa_panel_multi_box_HHs
    if object_id( 'Vespa_PanMan_this_weeks_non_scaling_segmentation_bugfix') is not null
        drop table Vespa_PanMan_this_weeks_non_scaling_segmentation_bugfix
end;

commit;





PanMan_make_report;

select * from Vespa_PanMan_09_traffic_lights
order by panel,sequencer
;





