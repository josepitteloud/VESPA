create variable @recent_profiling_date date;

  select @recent_profiling_date = max(profiling_date)
    from vespa_analysts_SC2_Sky_base_segment_snapshots
;

    select sbv.account_number
          ,panel
          ,scaling_segment_id
      into #sbv
      from vespa_analysts.vespa_single_box_view as sbv
           left join vespa_analysts.SC2_Sky_base_segment_snapshots as bss on sbv.account_number = bss.account_number
     where status_vespa = 'Enabled'
     and profiling_date = @recent_profiling_date
  group by sbv.account_number
          ,panel
          ,scaling_segment_id
; --1,417,876

create hg index idx1 on #sbv(account_number);

--check whether any accounts have more than 1 panel
select top 10 count(1) as cow,account_number from #sbv group by account_number having cow>1;
--all good


  select bss.scaling_segment_id
        ,sum(case when panel =   'VESPA'        then 1 else 0 end) as vespa
        ,sum(case when panel in ('ALT6','ALT7') then 1 else 0 end) as alt
    into #segments
    from #sbv
         left join vespa_analysts.SC2_Sky_base_segment_snapshots as bss on #sbv.account_number = bss.account_number
         left join vespa_analysts.SC2_Segments_lookup            as ssl on bss.scaling_segment_id = ssl.scaling_segment_id
   where universe like 'A%'
     and profiling_date = @recent_profiling_date
group by bss.scaling_segment_id
; --14669


--results

--no. of segments in all
  select count(distinct bss.scaling_segment_id)
    from vespa_analysts.SC2_Sky_base_segment_snapshots as bss
         left join vespa_analysts.SC2_Segments_lookup  as ssl on bss.scaling_segment_id = ssl.scaling_segment_id
   where universe like 'A%'
     and profiling_date = @recent_profiling_date
--19448

--non-empty segments across all panels
  select count(1) from #segments;
--14669

--segments empty in daily panel, with accounts in alt. panels
  select count(1) from #segments where vespa=0
--2025

  select alt,count(1) from #segments where vespa=0 group by alt;

drop table greenj.segment_fill_in

select distinct(#sbv.account_number)
  into greenj.segment_fill_in
  from #sbv
       inner join #segments as seg on #sbv.scaling_segment_id = seg.scaling_segment_id
 where vespa=0
   and panel in ('ALT6', 'ALT7')
--12,455


select * from segment_fill_in

