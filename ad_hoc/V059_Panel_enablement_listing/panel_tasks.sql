--Q1a
--Profile of the 2 and 3+ box household universes by data return metric across all panels
select case when reporting_Quality < 0.2 then '0 - 0.2'
            when reporting_Quality >= 0.2 and reporting_Quality < 0.4 then '0.2 - 0.4'
            when reporting_Quality >= 0.4 and reporting_Quality < 0.6 then '0.4 - 0.6'
            when reporting_Quality >= 0.6 and reporting_Quality < 0.8 then '0.6 - 0.8'
            when reporting_Quality >= 0.8 then '0.8 - 1'
            else 'Unknown' end as qual
      ,sum(case when panel_id_vespa = 6  then 1 else 0 end) as panel_6
      ,sum(case when panel_id_vespa = 7  then 1 else 0 end) as panel_7
      ,sum(case when panel_id_vespa = 12 then 1 else 0 end) as panel_12
  from vespa_analysts.vespa_single_box_view as sbv
       inner join vespa_analysts.SC2_scaling_weekly_sample as sws on sbv.account_number = sws.account_number
 where status_vespa='Enabled'
   and left(universe,1) in ('B', 'C')
group by qual
order by qual
;

--Q1b
  select sbv.account_number
        ,panel_id_vespa
        ,min(reporting_quality) as min_qual
    into #temp
    from vespa_analysts.vespa_single_box_view as sbv
         inner join vespa_analysts.SC2_scaling_weekly_sample as sws on sbv.account_number = sws.account_number
   where status_vespa='Enabled'
     and left(universe,1) in ('B', 'C')
group by sbv.account_number
        ,panel_id_vespa
;

select case when min_qual < 0.2 then '0 - 0.2'
            when min_qual >= 0.2 and min_qual < 0.4 then '0.2 - 0.4'
            when min_qual >= 0.4 and min_qual < 0.6 then '0.4 - 0.6'
            when min_qual >= 0.6 and min_qual < 0.8 then '0.6 - 0.8'
            when min_qual >= 0.8 then '0.8 - 1'
            else 'Unknown' end as qual
      ,sum(case when panel_id_vespa = 6  then 1 else 0 end) as panel_6
      ,sum(case when panel_id_vespa = 7  then 1 else 0 end) as panel_7
      ,sum(case when panel_id_vespa = 12 then 1 else 0 end) as panel_12
  from #temp
group by qual
order by qual
;

--Q2
--All short tenure (less than 1 year) single box universe segments with at least 0.4 data quality return metric.
  select distinct(sbv.account_number)
    from vespa_analysts.vespa_single_box_view as sbv
         inner join vespa_analysts.SC2_scaling_weekly_sample as sws on sbv.account_number = sws.account_number
   where tenure like 'A%'
     and universe like 'A%'
     and reporting_quality >= 0.4
     and panel_id_vespa in (6, 7)
     and status_vespa='Enabled'
;--164

--Q3
--An additional 40K accounts of single box households, taken randomly across scaling segments, with at least 0.4 reporting metric.
  select sbv.account_number
        ,min(reporting_quality) as rq
    into #additional
    from vespa_analysts.vespa_single_box_view as sbv
         inner join vespa_analysts.SC2_scaling_weekly_sample as sws on sbv.account_number = sws.account_number
   where status_vespa='Enabled'
     and panel_id_vespa in (6, 7)
     and status_vespa='Enabled'
     and reporting_quality >= 0.4
     and universe like 'A%'
group by sbv.account_number
;

  select account_number
        ,rq
        ,rank() over (ORDER BY rq,account_number desc) as rnk
    into #ranked
    from #additional
;

select rq from #ranked where rnk=40000;
--.43299999833107

select account_number from #ranked where rnk<=40000;


