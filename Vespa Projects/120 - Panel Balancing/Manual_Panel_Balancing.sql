--------------------------------------
--Jon---------------------------------
--------------------------------------
/*
truncate table Vespa_PanMan_all_households;
drop table Vespa_PanMan_orig;
drop table vespa_panman_remainder;
drop table waterfall_base;
--drop table Vespa_PanMan_orig_old
--drop table Vespa_PanMan_remainder_old
alter table Vespa_PanMan_orig rename Vespa_PanMan_orig_old
alter table Vespa_PanMan_remainder rename Vespa_PanMan_remainder_old
*/
  select bas.account_number
        ,scaling_segment_id
        ,reporting_quality
        ,panel
        ,non_scaling_segment_id
        ,reporting_categorisation
    into Vespa_PanMan_orig
    from vespa_analysts.vespa_panman_all_households as bas
         left join disablements as dis on bas.account_number = dis.account_number
   where panel='VESPA'
     and dis.account_number is null
; --571,869

  select bas.account_number
        ,scaling_segment_id
        ,reporting_quality
        ,panel
        ,non_scaling_segment_id
        ,reporting_categorisation
    into vespa_panman_remainder
    from vespa_analysts.vespa_panman_all_households as bas
         left join disablements as dis on bas.account_number = dis.account_number
   where panel <>'VESPA'
     and reporting_quality>.8
     and dis.account_number is null
; --303,190

  select bas.account_number
    into greenj.waterfall_base
    from vespa_analysts.waterfall_base                        as bas
         left join vespa_analysts.Vespa_PanMan_all_households as pan on bas.account_number = pan.account_number
         left join disablements as dis on bas.account_number = dis.account_number
   where knockout_level in (9999)
     and pan.account_number is null
     and dis.account_number is null
;--637,595

--Pass 1 - Add 2+ boxes and new tenure from alt. panels
truncate table Vespa_PanMan_all_households;

drop table #adds;

  select account_number
        ,max(reporting_quality) as rq
    into #adds
    from vespa_panman_remainder as pah
         inner join vespa_analysts.SC2_Segments_lookup as lkp on pah.scaling_segment_id = lkp.scaling_segment_id
   where (universe not like 'A%' and left(tenure,1) in ('A', 'B','C'))
or left(tenure,1)='A'
group by account_number
order by rq desc
; --

  insert into Vespa_PanMan_all_households(account_number
                                         ,scaling_segment_id
                                         ,reporting_quality
                                         ,panel
                                         ,non_scaling_segment_id
                                         ,reporting_categorisation
        )
  select bas.*
    from Vespa_PanMan_remainder as bas
         inner join #adds on bas.account_number = #adds.account_number
; --20270

--Pass 2 - The unacceptable boxes are not important for traffic light analysis, so the only thing we can do to imporoive the universe score is to remove some single boxers. This would only improve the score from 65.5 to 64.6
--so let's woork on the other variables.



--multi box and new tenure from waterfall
drop table #new_adds;

  select sc2.account_number
        ,sc2.scaling_segment_id
    into #new_adds
    from vespa_analysts.SC2_Sky_base_segment_snapshots as sc2
         inner join waterfall_base                     as nep on sc2.account_number     = nep.account_number
         inner join vespa_analysts.SC2_Segments_lookup as lkp on sc2.scaling_segment_id = lkp.scaling_segment_id
   where universe not like 'A%' and tenure like 'A%' and boxtype not like 'J%'
group by sc2.account_number
        ,sc2.scaling_segment_id--29267
; --

  insert into #new_adds
  select top 2000 sc2.account_number
        ,sc2.scaling_segment_id
    from vespa_analysts.SC2_Sky_base_segment_snapshots as sc2
         inner join waterfall_base                     as nep on sc2.account_number     = nep.account_number
         inner join vespa_analysts.SC2_Segments_lookup as lkp on sc2.scaling_segment_id = lkp.scaling_segment_id
   where universe like 'B%' and tenure like 'C%' and boxtype like 'F%'
group by sc2.account_number
        ,sc2.scaling_segment_id
order by sc2.account_number
; --

  insert into #new_adds
  select top 1000 sc2.account_number
        ,sc2.scaling_segment_id
    from vespa_analysts.SC2_Sky_base_segment_snapshots as sc2
         inner join waterfall_base                     as nep on sc2.account_number     = nep.account_number
         inner join vespa_analysts.SC2_Segments_lookup as lkp on sc2.scaling_segment_id = lkp.scaling_segment_id
   where universe like 'B%' and tenure like 'C%' and boxtype like 'K%'
group by sc2.account_number
        ,sc2.scaling_segment_id
order by sc2.account_number
; --

  insert into #new_adds
  select top 2508 sc2.account_number
        ,sc2.scaling_segment_id
    from vespa_analysts.SC2_Sky_base_segment_snapshots as sc2
         inner join waterfall_base                     as nep on sc2.account_number     = nep.account_number
         inner join vespa_analysts.SC2_Segments_lookup as lkp on sc2.scaling_segment_id = lkp.scaling_segment_id
   where universe like 'B%' and tenure like 'D%' and boxtype like 'L%'
group by sc2.account_number
        ,sc2.scaling_segment_id
order by sc2.account_number
; --

  insert into #new_adds
  select top 492 sc2.account_number
        ,sc2.scaling_segment_id
    from vespa_analysts.SC2_Sky_base_segment_snapshots as sc2
         inner join waterfall_base                     as nep on sc2.account_number     = nep.account_number
         inner join vespa_analysts.SC2_Segments_lookup as lkp on sc2.scaling_segment_id = lkp.scaling_segment_id
   where universe like 'B%' and tenure like 'D%' and boxtype like 'M%'
group by sc2.account_number
        ,sc2.scaling_segment_id
order by sc2.account_number
; --

  insert into #new_adds
  select top 4944 sc2.account_number
        ,sc2.scaling_segment_id
    from vespa_analysts.SC2_Sky_base_segment_snapshots as sc2
         inner join waterfall_base                     as nep on sc2.account_number     = nep.account_number
         inner join vespa_analysts.SC2_Segments_lookup as lkp on sc2.scaling_segment_id = lkp.scaling_segment_id
   where universe like 'C%' and tenure like 'C%' and boxtype not like 'J%'
group by sc2.account_number
        ,sc2.scaling_segment_id --7494
order by sc2.account_number
; --

  insert into #new_adds
  select top 8000 sc2.account_number
        ,sc2.scaling_segment_id
    from vespa_analysts.SC2_Sky_base_segment_snapshots as sc2
         inner join waterfall_base                     as nep on sc2.account_number     = nep.account_number
         inner join vespa_analysts.SC2_Segments_lookup as lkp on sc2.scaling_segment_id = lkp.scaling_segment_id
   where universe like 'A%' and tenure like 'C%' and boxtype like 'C%'
group by sc2.account_number
        ,sc2.scaling_segment_id
order by sc2.account_number
; --

  insert into #new_adds
  select top 12000 sc2.account_number
        ,sc2.scaling_segment_id
    from vespa_analysts.SC2_Sky_base_segment_snapshots as sc2
         inner join waterfall_base                     as nep on sc2.account_number     = nep.account_number
         inner join vespa_analysts.SC2_Segments_lookup as lkp on sc2.scaling_segment_id = lkp.scaling_segment_id
   where universe like 'A%' and tenure like 'C%' and boxtype like 'D%'
group by sc2.account_number
        ,sc2.scaling_segment_id
order by sc2.account_number
; --

  insert into #new_adds
  select top 12000 sc2.account_number
        ,sc2.scaling_segment_id
    from vespa_analysts.SC2_Sky_base_segment_snapshots as sc2
         inner join waterfall_base                     as nep on sc2.account_number     = nep.account_number
         inner join vespa_analysts.SC2_Segments_lookup as lkp on sc2.scaling_segment_id = lkp.scaling_segment_id
   where universe like 'A%' and tenure like 'D%' and boxtype like 'A%'
group by sc2.account_number
        ,sc2.scaling_segment_id
order by sc2.account_number
; --

  insert into #new_adds
  select top 3000 sc2.account_number
        ,sc2.scaling_segment_id
    from vespa_analysts.SC2_Sky_base_segment_snapshots as sc2
         inner join waterfall_base                     as nep on sc2.account_number     = nep.account_number
         inner join vespa_analysts.SC2_Segments_lookup as lkp on sc2.scaling_segment_id = lkp.scaling_segment_id
   where universe like 'A%' and tenure like 'D%' and boxtype like 'C%'
group by sc2.account_number
        ,sc2.scaling_segment_id
order by sc2.account_number
; --

  insert into #new_adds
  select top 2000 sc2.account_number
        ,sc2.scaling_segment_id
    from vespa_analysts.SC2_Sky_base_segment_snapshots as sc2
         inner join waterfall_base                     as nep on sc2.account_number     = nep.account_number
         inner join vespa_analysts.SC2_Segments_lookup as lkp on sc2.scaling_segment_id = lkp.scaling_segment_id
   where universe like 'A%' and tenure like 'D%' and boxtype like 'D%'
group by sc2.account_number
        ,sc2.scaling_segment_id
order by sc2.account_number
; --

  insert into #new_adds
  select top 8000 sc2.account_number
        ,sc2.scaling_segment_id
    from vespa_analysts.SC2_Sky_base_segment_snapshots as sc2
         inner join waterfall_base                     as nep on sc2.account_number     = nep.account_number
         inner join vespa_analysts.SC2_Segments_lookup as lkp on sc2.scaling_segment_id = lkp.scaling_segment_id
   where universe like 'A%' and tenure like 'D%' and boxtype like 'D%'
group by sc2.account_number
        ,sc2.scaling_segment_id
order by sc2.account_number
; --
  insert into #new_adds
  select top 7000 sc2.account_number
        ,sc2.scaling_segment_id
    from vespa_analysts.SC2_Sky_base_segment_snapshots as sc2
         inner join waterfall_base                     as nep on sc2.account_number     = nep.account_number
         inner join vespa_analysts.SC2_Segments_lookup as lkp on sc2.scaling_segment_id = lkp.scaling_segment_id
   where universe like 'A%' and tenure like 'D%' and boxtype like 'D%'
group by sc2.account_number
        ,sc2.scaling_segment_id
order by sc2.account_number
; --
  insert into #new_adds
  select top 4000 sc2.account_number
        ,sc2.scaling_segment_id
    from vespa_analysts.SC2_Sky_base_segment_snapshots as sc2
         inner join waterfall_base                     as nep on sc2.account_number     = nep.account_number
         inner join vespa_analysts.SC2_Segments_lookup as lkp on sc2.scaling_segment_id = lkp.scaling_segment_id
   where universe like 'A%' and tenure like 'D%' and boxtype like 'D%'
group by sc2.account_number
        ,sc2.scaling_segment_id
order by sc2.account_number
; --
  insert into #new_adds
  select top 3500 sc2.account_number
        ,sc2.scaling_segment_id
    from vespa_analysts.SC2_Sky_base_segment_snapshots as sc2
         inner join waterfall_base                     as nep on sc2.account_number     = nep.account_number
         inner join vespa_analysts.SC2_Segments_lookup as lkp on sc2.scaling_segment_id = lkp.scaling_segment_id
   where universe like 'A%' and tenure like 'D%' and boxtype like 'D%'
group by sc2.account_number
        ,sc2.scaling_segment_id
order by sc2.account_number
; --
  insert into #new_adds
  select top 900 sc2.account_number
        ,sc2.scaling_segment_id
    from vespa_analysts.SC2_Sky_base_segment_snapshots as sc2
         inner join waterfall_base                     as nep on sc2.account_number     = nep.account_number
         inner join vespa_analysts.SC2_Segments_lookup as lkp on sc2.scaling_segment_id = lkp.scaling_segment_id
   where universe like 'A%' and tenure like 'D%' and boxtype like 'D%'
group by sc2.account_number
        ,sc2.scaling_segment_id
order by sc2.account_number
; --
  insert into #new_adds
  select top 611 sc2.account_number
        ,sc2.scaling_segment_id
    from vespa_analysts.SC2_Sky_base_segment_snapshots as sc2
         inner join waterfall_base                     as nep on sc2.account_number     = nep.account_number
         inner join vespa_analysts.SC2_Segments_lookup as lkp on sc2.scaling_segment_id = lkp.scaling_segment_id
   where universe like 'A%' and tenure like 'D%' and boxtype like 'D%'
group by sc2.account_number
        ,sc2.scaling_segment_id
order by sc2.account_number
; --
  insert into #new_adds
  select top 500 sc2.account_number
        ,sc2.scaling_segment_id
    from vespa_analysts.SC2_Sky_base_segment_snapshots as sc2
         inner join waterfall_base                     as nep on sc2.account_number     = nep.account_number
         inner join vespa_analysts.SC2_Segments_lookup as lkp on sc2.scaling_segment_id = lkp.scaling_segment_id
   where universe like 'A%' and tenure like 'D%' and boxtype like 'D%'
group by sc2.account_number
        ,sc2.scaling_segment_id
order by sc2.account_number
; --
  insert into #new_adds
  select top 500 sc2.account_number
        ,sc2.scaling_segment_id
    from vespa_analysts.SC2_Sky_base_segment_snapshots as sc2
         inner join waterfall_base                     as nep on sc2.account_number     = nep.account_number
         inner join vespa_analysts.SC2_Segments_lookup as lkp on sc2.scaling_segment_id = lkp.scaling_segment_id
   where universe like 'A%' and tenure like 'D%' and boxtype like 'D%'
group by sc2.account_number
        ,sc2.scaling_segment_id
order by sc2.account_number
; --
  insert into #new_adds
  select top 223 sc2.account_number
        ,sc2.scaling_segment_id
    from vespa_analysts.SC2_Sky_base_segment_snapshots as sc2
         inner join waterfall_base                     as nep on sc2.account_number     = nep.account_number
         inner join vespa_analysts.SC2_Segments_lookup as lkp on sc2.scaling_segment_id = lkp.scaling_segment_id
   where universe like 'A%' and tenure like 'D%' and boxtype like 'D%'
group by sc2.account_number
        ,sc2.scaling_segment_id
order by sc2.account_number
; --

  insert into #new_adds
  select top 10000 account_number,pah.scaling_segment_id
    from vespa_panman_remainder as pah
         inner join vespa_analysts.SC2_Segments_lookup as lkp on pah.scaling_segment_id = lkp.scaling_segment_id
   where universe like 'A%'
     and tenure like 'C%'
     and boxtype like 'C%'
group by account_number,pah.scaling_segment_id
order by account_number
;

  insert into #new_adds
  select account_number,pah.scaling_segment_id
    from vespa_panman_remainder as pah
         inner join vespa_analysts.SC2_Segments_lookup as lkp on pah.scaling_segment_id = lkp.scaling_segment_id
   where (universe like 'A%' and tenure like 'C%' and boxtype like 'D%')
      or (universe like 'B%' and tenure like 'D%' and boxtype like 'F%')
      or (universe like 'B%' and tenure like 'D%' and boxtype like 'I%')
      or (universe like 'B%' and tenure like 'D%' and boxtype like 'K%')
      or (universe like 'B%' and tenure like 'D%' and boxtype like 'L%')
      or (universe like 'B%' and tenure like 'D%' and boxtype like 'M%')
      or (universe like 'C%' and tenure like 'D%' and boxtype like 'F%')
      or (universe like 'C%' and tenure like 'D%' and boxtype like 'I%')
      or (universe like 'C%' and tenure like 'D%' and boxtype like 'K%')
      or (universe like 'C%' and tenure like 'D%' and boxtype like 'L%')
group by account_number,pah.scaling_segment_id
order by account_number
;

  insert into Vespa_PanMan_all_households(account_number
                                         ,reporting_categorisation
        )
  select account_number
        ,'Acceptable'
    from #new_adds
;


--make them all acceptable
  update Vespa_PanMan_all_households
     set panel = 'VESPA'
       ,reporting_categorisation = 'Acceptable'
;

--add the originals
  insert into Vespa_PanMan_all_households
  select * from Vespa_PanMan_orig
;

create variable @recent_profiling_date date;

  select @recent_profiling_date = max(profiling_date)
    from vespa_analysts.SC2_Sky_base_segment_snapshots
;

  select account_number,scaling_segment_id
    into #Scaling_weekly_sample
    from vespa_analysts.SC2_Sky_base_segment_snapshots
   where profiling_date = @recent_profiling_date
;--9,480,053

       select scaling_segment_id
             ,non_scaling_segment_id
             ,count(1) as Sky_Base_Households
         into #sky_base_segmentation
         from #Scaling_weekly_sample as tws
              inner join Vespa_PanMan_this_weeks_non_scaling_segmentation as tsnss on tws.account_number = tsnss.account_number
        where tws.scaling_segment_ID is not null and tsnss.non_scaling_segment_id is not null -- That annoying case of the region 'Eire' guy which is Ireland and therefore shouldn't be in the Vespa dataset, but whatever
     group by tws.scaling_segment_ID, tsnss.non_scaling_segment_id
;

--start here
  update Vespa_PanMan_all_households
     set scaling_segment_id = tws.scaling_segment_id
   from Vespa_PanMan_all_households
        inner join #Scaling_weekly_sample as tws on Vespa_PanMan_all_households.account_number = tws.account_number
;

  update Vespa_PanMan_all_households as bas
     set non_scaling_segment_id = nss.non_scaling_segment_id
    from Vespa_PanMan_this_weeks_non_scaling_segmentation as nss
   where bas.account_number = nss.account_number
;

drop table #panel_segmentation;
drop table #panel_totals;

    select 'VESPA' as panel
          ,scaling_segment_id
          ,non_scaling_segment_id
          ,count(1) as Panel_Households
          ,sum(case when reporting_categorisation = 'Acceptable'       then 1 else 0 end) as Acceptably_reliable_households
      into #panel_segmentation
      from Vespa_PanMan_all_households as hr
     where scaling_segment_ID is not null and non_scaling_segment_id is not null
  group by panel, scaling_segment_ID, non_scaling_segment_id
;
     commit;
     create unique index fake_pk on #panel_segmentation (panel, scaling_segment_id, non_scaling_segment_id);

       select 'VESPA' as panel
             ,count(1) as panel_reporters
         into #panel_totals
         from Vespa_PanMan_all_households
        where reporting_categorisation = 'Acceptable'
     group by panel
;

     create variable @total_sky_base                 int;

truncate table Vespa_PanMan_Scaling_Segment_Profiling;

     insert into Vespa_PanMan_Scaling_Segment_Profiling (
            panel
           ,scaling_segment_id
           ,non_scaling_segment_id
           ,Sky_Base_Households
     )
     select pt.panel
           ,sb.*
       from #sky_base_segmentation as sb
            cross join #panel_totals as pt -- want all the combinations of stuff
;

     update Vespa_PanMan_Scaling_Segment_Profiling
        set Panel_Households                = ps.Panel_Households
           ,Acceptably_reliable_households = ps.Acceptably_reliable_households
       from Vespa_PanMan_Scaling_Segment_Profiling
            inner join #panel_segmentation as ps on Vespa_PanMan_Scaling_Segment_Profiling.panel                    = ps.panel
                                                and Vespa_PanMan_Scaling_Segment_Profiling.scaling_segment_id       = ps.scaling_segment_id
                                                and Vespa_PanMan_Scaling_Segment_Profiling.non_scaling_segment_id   = ps.non_scaling_segment_id
;

truncate table Vespa_PanMan_all_aggregated_results;



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
              inner join vespa_analysts.SC2_Segments_lookup as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
     group by ssp.panel, ssl.universe
;

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
              inner join vespa_analysts.SC2_Segments_lookup as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
     group by ssp.panel, ssl.isba_tv_region
;

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
              inner join vespa_analysts.SC2_Segments_lookup as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
     group by ssp.panel, ssl.hhcomposition
;

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
         from Vespa_PanMan_Scaling_Segment_Profiling as ssp inner join vespa_analysts.SC2_Segments_lookup as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
     group by ssp.panel, ssl.package
;

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
              inner join vespa_analysts.SC2_Segments_lookup as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
     group by ssp.panel, ssl.tenure
;

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
              inner join vespa_analysts.SC2_Segments_lookup as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
     group by ssp.panel, ssl.boxtype
;

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
              inner join vespa_analysts.Vespa_PanMan_non_scaling_segments_lookup as nssl on ssp.non_scaling_segment_ID = nssl.non_scaling_segment_ID
     group by ssp.panel, nssl.value_segment
;

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
              inner join vespa_analysts.Vespa_PanMan_non_scaling_segments_lookup as nssl on ssp.non_scaling_segment_ID = nssl.non_scaling_segment_ID
     group by ssp.panel, nssl.Mosaic_segment
;

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
              inner join vespa_analysts.Vespa_PanMan_non_scaling_segments_lookup as nssl on ssp.non_scaling_segment_ID = nssl.non_scaling_segment_ID
     group by ssp.panel, nssl.Financial_strategy_segment
;

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
              inner join vespa_analysts.Vespa_PanMan_non_scaling_segments_lookup as nssl on ssp.non_scaling_segment_ID = nssl.non_scaling_segment_ID
     group by ssp.panel, nssl.is_OnNet
;

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
              inner join vespa_analysts.Vespa_PanMan_non_scaling_segments_lookup as nssl on ssp.non_scaling_segment_ID = nssl.non_scaling_segment_ID
     group by ssp.panel, nssl.uses_sky_go
;

     update Vespa_PanMan_all_aggregated_results
        set Good_Household_Index = case when 200 < 100 * (Acceptable_Households) * @total_sky_base / convert(float, Sky_Base_Households) / pt.panel_reporters then 200
                                        else       100 * (Acceptable_Households) * @total_sky_base / convert(float, Sky_Base_Households) / pt.panel_reporters
                                   end
       from Vespa_PanMan_all_aggregated_results
            inner join #panel_totals as pt on Vespa_PanMan_all_aggregated_results.panel = pt.panel
;

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
;

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
;

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
;

      select @total_sky_base     =sum(Sky_Base_Households)
        from Vespa_PanMan_Scaling_Segment_Profiling
       where panel = 'VESPA'
;

     update Vespa_PanMan_all_aggregated_results
        set Good_Household_Index =   100 * Acceptable_Households * @total_sky_base /  (Sky_Base_Households * pt.panel_reporters)
       from Vespa_PanMan_all_aggregated_results
            inner join #panel_totals as pt on Vespa_PanMan_all_aggregated_results.panel = pt.panel
;


drop table vespa_PanMan_09_traffic_lights;

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
         into vespa_PanMan_09_traffic_lights
         from Vespa_PanMan_all_aggregated_results
     group by panel, aggregation_variable
;

select * from vespa_PanMan_09_traffic_lights where panel='VESPA'
order by sequencer
;

select * from Vespa_PanMan_all_aggregated_results where aggregation_variable='UNIVERSE' and good_household_index is not null order by variable_value;
select * from Vespa_PanMan_all_aggregated_results where aggregation_variable='TENURE'   and good_household_index is not null order by variable_value;
select * from Vespa_PanMan_all_aggregated_results where aggregation_variable='BOXTYPE'  and good_household_index is not null order by variable_value;
select * from Vespa_PanMan_all_aggregated_results where aggregation_variable='VALUESEG' and good_household_index is not null order by variable_value;

--possible adds from waterfall remaining
   select count(distinct sc2.account_number)
        ,universe
        ,tenure
        ,boxtype
--        ,value_segment
    from waterfall_base                                                     as nep
         inner join vespa_analysts.SC2_Sky_base_segment_snapshots           as sc2 on sc2.account_number     = nep.account_number
         inner join vespa_analysts.SC2_Segments_lookup                      as lkp on sc2.scaling_segment_id = lkp.scaling_segment_id
         inner join Vespa_PanMan_this_weeks_non_scaling_segmentation        as nss on sc2.account_number     = nss.account_number
         left join vespa_panman_all_households                              as vah on sc2.account_number     = vah.account_number
   where vah.account_number is null
group by universe
        ,tenure
        ,boxtype
--        ,value_segment
;

--possible adds from alt.panels remaining
  select universe,tenure,boxtype
--        ,value_segment
        ,count(1)
    from vespa_panman_remainder as pah
         inner join vespa_analysts.SC2_Segments_lookup               as lkp on pah.scaling_segment_id = lkp.scaling_segment_id
         inner join Vespa_PanMan_this_weeks_non_scaling_segmentation as nss on pah.account_number     = nss.account_number
         left join vespa_panman_all_households as vah on pah.account_number = vah.account_number
where vah.account_number is null
group by universe,tenure,boxtype
        ,value_segment
;

  select universe,tenure,boxtype
        ,count(1)
    from vespa_panman_remainder as pah
         inner join vespa_analysts.SC2_Segments_lookup               as lkp on pah.scaling_segment_id = lkp.scaling_segment_id
         inner join Vespa_PanMan_this_weeks_non_scaling_segmentation as nss on pah.account_number     = nss.account_number
         left join vespa_panman_all_households as vah on pah.account_number = vah.account_number
where vah.account_number is null
group by universe,tenure,boxtype
;

  select tenure
        ,count(1)
    from vespa_panman_remainder as pah
         inner join vespa_analysts.SC2_Segments_lookup               as lkp on pah.scaling_segment_id = lkp.scaling_segment_id
         inner join Vespa_PanMan_this_weeks_non_scaling_segmentation as nss on pah.account_number     = nss.account_number
         left join vespa_panman_all_households as vah on pah.account_number = vah.account_number
where vah.account_number is null
group by tenure
;

  select universe
        ,tenure
        ,count(1)
    from vespa_panman_remainder as pah
         inner join vespa_analysts.SC2_Segments_lookup               as lkp on pah.scaling_segment_id = lkp.scaling_segment_id
         inner join Vespa_PanMan_this_weeks_non_scaling_segmentation as nss on pah.account_number     = nss.account_number
         left join vespa_panman_all_households as vah on pah.account_number = vah.account_number
   where vah.account_number is null
group by universe
        ,tenure
;

  select vah.account_number
        ,max(reporting_quality) as rq
        ,count(distinct si_external_identifier) as subs
    into #rq_by_acc
    from vespa_panman_all_households                as vah
         inner join sk_prod.cust_service_instance   as csi on vah.account_number = csi.account_number
   where effective_to_dt='9999-09-09'
group by vah.account_number
--
select count(1),sum(subs) from #rq_by_acc
--count(1) sum(#rq_by_acc.subs)
--659979 accounts
--811556 subs
--need to get down to a max. of 750,000 subs

--to see how many accounts we need to delete to get 61,556 subs to delete
  select top 150000 *
    from #rq_by_acc
   where rq is not null
order by rq
;

  select vss.account_number
        ,panel_no
    into #vss
    from sk_prod.vespa_subscriber_status as vss
         left join disablements          as dis on vss.account_number = dis.account_number
   where result = 'Enabled'
     and dis.account_number is null
group by vss.account_number
        ,panel_no
;

  select top 37000 account_number,subs
    into #todelete
    from #rq_by_acc as bas
   where rq is not null
order by rq
; --

--only need to delete the ones that actually are still on panel 12
  select bas.*
    into panbal_todelete
    from #todelete as bas
         inner join #vss on bas.account_number = #vss.account_number
   where panel_no = 12
;

  select bas.account_number
    into greenj.panbal_toadd_to_alt_panels
    from vespa_panman_all_households as bas
         left join #vss on bas.account_number = #vss.account_number
   where #vss.account_number is null
group by bas.account_number
; --57,789

  select bas.account_number
    into greenj.panbal_moveto12
    from vespa_panman_all_households as bas
         left join #vss on bas.account_number = #vss.account_number
   where panel_no in (6,7)
group by bas.account_number
; --30,665


grant select on panbal_todelete to public;
grant select on panbal_toadd_to_alt_panels to public;
grant select on panbal_moveto12 to public;
grant select on Vespa_PanMan_orig to public;
grant select on vespa_panman_remainder to public;
grant select on waterfall_base to public;
grant select on Vespa_PanMan_all_households to public;
grant select on Vespa_PanMan_Scaling_Segment_Profiling to public;
grant select on Vespa_PanMan_all_aggregated_results to public;
grant select on vespa_PanMan_09_traffic_lights to public;

---

select top 10 account_number,reporting_quality, * from vespa_analysts.vespa_single_box_view order by account_number
create hg index idx1 on panbal_todelete(account_number)
select distinct(sbv.account_number) into #temp from vespa_analysts.vespa_single_box_view as sbv left join panbal_todelete as del on sbv.account_number = del.account_number where panel = 'VESPA' and status_vespa = 'Enabled' and del.account_number is null
insert into #temp select distinct(account_number) from panbal_toadd_to_alt_panels;
insert into #temp select distinct(account_number) from panbal_moveto12;
insert into Vespa_PanMan_all_households(account_number) select distinct(account_number) from #temp
select count(1) from Vespa_PanMan_all_households
truncate table Vespa_PanMan_all_households


select top 10 * from vespa_analysts.vespa_single_box_view






select * from results


