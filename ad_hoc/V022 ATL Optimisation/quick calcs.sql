drop table #a;
select distinct(account_number),null as scaling_segment_ID into #a from sk_prod.VESPA_STB_PROG_EVENTS_20111111;

update #a set scaling_segment_ID = sdi.scaling_segment_ID
from vespa_analysts.scaling_dialback_intervals as sdi
where #a.account_number = sdi.account_number
and reporting_starts <= '2011-11-11'
and reporting_ends >= '2011-11-11'
;

select sum(weighting) from #a inner join vespa_analysts.scaling_weightings as sw
on #a.scaling_segment_ID = sw.scaling_segment_ID
where scaling_day='2011-11-11'
sum(sw.weighting)
9375063.00000012159 All weighting
sum(sw.weighting)
8788457.78360197783 Vespa weighting 11/11/11
94%

drop table #temp;
select account_number
      ,cast(0 as bit) as hd
      ,cast(0 as bit) as movies, cast(0 as bit) as vespa
into #temp
from sk_prod.cust_single_account_view as sav where cust_active_dtv=1
;


update #temp as bas
set hd = 1
from sk_prod.cust_subs_hist as csh
where bas.account_number=csh.account_number
and csh.subscription_sub_type ='DTV HD'
   AND csh.status_code in  ('AC','AB','PC')
;

    update #temp
       set movies = 1
      FROM sk_prod.cust_subs_hist AS csh
           LEFT OUTER JOIN sk_prod.cust_entitlement_lookup cel on csh.current_short_description = cel.short_description
     WHERE csh.effective_from_dt <= '2012-03-15'
       AND csh.effective_to_dt    > '2012-03-15'
       AND csh.subscription_sub_type  IN ( 'DTV Primary Viewing')
and cel.prem_movies>0
and #temp.account_number = csh.account_number
;

update #temp set vespa=1
from sk_prod.vespa_subscriber_status as vss
where #temp.account_number = vss.account_number
;

select count(1),sum(hd),sum(vespa),sum(hd * vespa) from #temp;
select count(1),sum(movies),sum(vespa),sum(movies * vespa) from #temp;




