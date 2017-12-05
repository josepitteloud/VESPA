/*

Investigate Chaneel Islands region missing from scaling

Lead: Claudio Lima
2013-10-25

*/

-- Tables in scaling
select table_name from sp_tables() where table_owner = 'vespa_analysts' and lower(table_name) like 'sc2_%' order by 1

-- Hoe many accounts in AdSmart are from Channel Islands
SELECT count(DISTINCT account_number)
FROM sk_prod.ADSMART 
WHERE region = 'Channel Islands'
-- 36,635

-- Where are these accounts currently in the panel?
select panel
        ,Reported_on_20131005
        ,count(*)
from (
select distinct ad.account_number
                ,coalesce(sbv.panel,'Not in panel') as panel
                ,case when sc2.account_number is null then 'No' else 'Yes' end as Reported_on_20131005
from sk_prod.adsmart ad
left join (select account_number,panel from vespa_analysts.vespa_single_box_view where status_vespa = 'Enabled') sbv
on ad.account_number = sbv.account_number
left join (select account_number from vespa_analysts.sc2_intervals where '2013-10-05' between reporting_starts and reporting_ends) sc2
on ad.account_number = sc2.account_number
where ad.region = 'Channel Islands'
) t
group by panel,Reported_on_20131005
order by panel,Reported_on_20131005
/*
panel,Reported_on_20131005,count()
'ALT6','No',4615
'ALT7','No',4659
'Not in panel','No',27090
'Not in panel','Yes',1
'VESPA','No',270
*/

-- Have Channel Islands accounts ever reported in the daily panel during 2013
-- and in what region have they reported
select var.isba_tv_region, count(distinct ad.account_number)
from sk_prod.adsmart ad
inner join (select distinct account_number,scaling_segment_id from vespa_analysts.sc2_intervals where reporting_starts >= '2013-01-01') sc2
on ad.account_number = sc2.account_number
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 var
on sc2.scaling_segment_id = var.scaling_segment_id
where ad.region = 'Channel Islands'
group by var.isba_tv_region


select isba_tv_region,count(*) 
from sk_prod.cust_single_account_view 
group by isba_tv_region
order by 1