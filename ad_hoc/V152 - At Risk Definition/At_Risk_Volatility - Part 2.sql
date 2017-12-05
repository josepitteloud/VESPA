--1. All customers
  select case when colour <> 'Red' then 'X' else coalesce(cuscan_type,'X') end as cuscan_type
        ,sum(case when value_seg = 'Bedding In'                    then 1 else 0 end) as bedding_in
        ,sum(case when value_seg = 'Unstable'                      then 1 else 0 end) as unstable
        ,sum(case when value_seg not in ('Bedding In', 'Unstable') then 1 else 0 end) as other
        ,sum(case when value_seg is null                           then 1 else 0 end) as remainder
    from sk_prod.cust_single_account_view       as sav
         left join sk_prod.VALUE_SEGMENTS_DATA as cvs on sav.account_number = cvs.account_number
         left join atrisk_results as bas on sav.account_number = bas.account_number
   where sav.cust_active_dtv = 1
group by cuscan_type
order by cuscan_type
;

--2. Current Vespa customers
  select case when colour <> 'Red' then 'X' else coalesce(cuscan_type,'X') end as cuscan_type
        ,sum(case when value_seg = 'Bedding In'                    then 1 else 0 end) as bedding_in
        ,sum(case when value_seg = 'Unstable'                      then 1 else 0 end) as unstable
        ,sum(case when value_seg not in ('Bedding In', 'Unstable') then 1 else 0 end) as other
        ,sum(case when value_seg is null                           then 1 else 0 end) as remainder
    from ( select distinct account_number 
           from vespa_analysts.vespa_single_box_view
           where status_vespa like 'Enable%' ) as sbv
         left join sk_prod.VALUE_SEGMENTS_DATA as cvs on sbv.account_number = cvs.account_number
         left join atrisk_results                     as bas on bas.account_number = sbv.account_number
         left join vespa_analysts.accounts_to_exclude as exc on bas.account_number = exc.account_number
   where exc.account_number is null
and sbv.account_number in (select account_number from dt_callback group by account_number having max(prefix) = '')
and sbv.account_number not in (select account_number from kinnairt.DRX_595_BOXES_LIST)
group by cuscan_type
order by cuscan_type
;

-- Breakdown of CVS for At Risk on Vespa
    select value_seg
            ,count(*) as Num_HHs
    from ( select distinct account_number 
           from vespa_analysts.vespa_single_box_view
           where status_vespa like 'Enable%' ) as sbv
         left join sk_prod.VALUE_SEGMENTS_DATA as cvs on sbv.account_number = cvs.account_number
         left join vespa_analysts.accounts_to_exclude as exc on sbv.account_number = exc.account_number
   where exc.account_number is null
and sbv.account_number in (select account_number from dt_callback group by account_number having max(prefix) = '')
and sbv.account_number not in (select account_number from kinnairt.DRX_595_BOXES_LIST)
group by value_seg
order by value_seg

-- Breakdown of Cuscan for At Risk on Vespa
  select cuscan_type,rule,colour,count(*)
    from ( select distinct account_number 
           from vespa_analysts.vespa_single_box_view
           where status_vespa like 'Enable%' ) as sbv
         left join atrisk_results                     as bas on bas.account_number = sbv.account_number
         left join vespa_analysts.accounts_to_exclude as exc on sbv.account_number = exc.account_number
   where exc.account_number is null
and sbv.account_number in (select account_number from dt_callback group by account_number having max(prefix) = '')
and sbv.account_number not in (select account_number from kinnairt.DRX_595_BOXES_LIST)
group by cuscan_type,rule,colour
order by cuscan_type,colour,rule