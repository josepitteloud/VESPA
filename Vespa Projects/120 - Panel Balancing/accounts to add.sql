
--accounts to add to alt. panels, to make 50% in each segment (if poss)
  select sum(case when pan.account_number is null then 0 else 1 end)                                    as vespa
        ,sum(case when alt.account_number is not null and pan.account_number is null then 1 else 0 end) as alt
        ,bss.scaling_segment_id
        ,non_scaling_segment_id
    into #panels
    from vespa_analysts_SC2_Sky_base_segment_snapshots as bss
         left join panbal_panel                        as pan on bss.account_number = pan.account_number
         left join panbal_list3                        as alt on bss.account_number = alt.account_number
group by bss.scaling_segment_id
        ,non_scaling_segment_id
;

  select scaling_segment_id
        ,non_scaling_segment_id
        ,vespa-(alt*2) as reqd
    into #reqd
    from #panels
   where reqd > 0
;

select sum(reqd) from #tots;


  select account_number
        ,scaling_segment_id
        ,non_scaling_segment_id
        ,rank(over scaling_segment_id, non_scaling_segment_id) as rnk
    into #available
    from PanBal_list4           as wat
         left join panbal_panel as bas on wat.account_number = bas.account_number
   where bas.account_number is null
;

  select account_number
        ,scaling_segment_id
        ,non_scaling_segment_id
    into #adds
    from #available        as ava
         inner join  #reqd as req on ava.scaling_segment_id = req.scaling_segment_id
                                 and ava.scaling_segment_id = req.scaling_segment_id
   where rnk <= reqd
;





select count(1) from panbal_list2
327574
457890
select 457890-327574
select count(1) from panbal_list4




