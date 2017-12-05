      -- expected accounts
  select count(distinct log.account_number)
        ,value
    from vespa_analysts.panel_movements_log           as log
         inner join panbal_segment_snapshots          as snp on log.account_number = snp.account_number
         inner join panbal_segments_lookup_normalised as lkp on snp.segment_id = lkp.segment_id
   where request_created_dt   = '2014-11-24'
     and aggregation_variable = 4
     and destination in (11,12)
group by value

      -- imbalance
  select *
    from panbal_metrics_hist
   where metric = 'Current / tenure'
--   where metric = 'Current / bb_capable'


      -- how many in VIQ scaled table tenure
  select adjusted_event_start_date_vespa
        ,sum(case when value = 'A) 0-10 Months' then cow end) as A
        ,sum(case when value = 'B) 10-24 Months' then cow end) as B1
        ,sum(case when value = 'B) 2-10 Years' then cow end) as B2
        ,sum(case when value = 'C) 10 Years+' then cow end) as C
    from (
            select adjusted_event_start_date_vespa
                  ,value
                  ,count(distinct viq.account_number) as cow
              from viq_viewing_data_scaling as viq
                   inner join vespa_analysts.panel_movements_log as log on viq.account_number = log.account_number
                   inner join panbal_segment_snapshots as snp on viq.account_number = snp.account_number
                   inner join panbal_segments_lookup_normalised as lkp on snp.segment_id = lkp.segment_id
             where request_created_dt = '2014-11-24'
               and aggregation_variable = 4
               and destination in (11,12)
          group by adjusted_event_start_date_vespa
                  ,value
         ) as sub
group by adjusted_event_start_date_vespa

      -- how many in VIQ scaled table bb_capable
  select adjusted_event_start_date_vespa
        ,sum(case when value = 'Yes' then cow end) as yes
        ,sum(case when value = 'Mix' then cow end) as mix
        ,sum(case when value = 'No' then cow end) as no
    from (
            select adjusted_event_start_date_vespa
                  ,value
                  ,count(distinct viq.account_number) as cow
              from viq_viewing_data_scaling as viq
                   inner join vespa_analysts.panel_movements_log as log on viq.account_number = log.account_number
                   inner join panbal_segment_snapshots as snp on viq.account_number = snp.account_number
                   inner join panbal_segments_lookup_normalised as lkp on snp.segment_id = lkp.segment_id
             where request_created_dt = '2014-11-24'
               and aggregation_variable = 16
               and destination in (11,12)
          group by adjusted_event_start_date_vespa
                  ,value
         ) as sub
group by adjusted_event_start_date_vespa



---
select top 1000 * from panbal_metrics_hist
where metric like 'Current /%'

select * from panbal_metrics_hist
where metric like 'Current Panel / tenure%'

select * from panbal_metrics_hist
where metric like 'Sky Base / tenure%'



            select adjusted_event_start_date_vespa
                  ,count(distinct viq.account_number) as cow
              from viq_viewing_data_scaling as viq
                   inner join vespa_analysts.panel_movements_log as log on viq.account_number = log.account_number
                   inner join panbal_segment_snapshots as snp on viq.account_number = snp.account_number
                   inner join panbal_segments_lookup_normalised as lkp on snp.segment_id = lkp.segment_id
             where request_created_dt = '2014-11-24'
               and aggregation_variable = 4
               and destination in (11,12)
          group by adjusted_event_start_date_vespa
                  ,value
         ) as sub
group by adjusted_event_start_date_vespa






  select account_number
        ,min(acct_first_account_activation_dt) as acct_first_account_activation_dt
    into #activation
    from cust_single_account_view
group by account_number
;

create unique hg index uhacc on #activation(account_number);

  select adjusted_event_start_date_vespa
        ,CASE WHEN datediff(day,acct_first_account_activation_dt,adjusted_event_start_date_vespa) between 0 and 304 THEN 'A) 0-10 Months'
              WHEN datediff(day,acct_first_account_activation_dt,adjusted_event_start_date_vespa) <=  730 THEN 'B) 10-24 Months'
              WHEN datediff(day,acct_first_account_activation_dt,adjusted_event_start_date_vespa) <= 3652 THEN 'B) 2-10 Years'
              WHEN datediff(day,acct_first_account_activation_dt,adjusted_event_start_date_vespa) >  3652 THEN 'C) 10 Years+'
                                     ELSE 'D) Unknown' end as tenure
        ,count(1) as cow
    from viq_viewing_data_scaling as viq
         inner join #activation as act on viq.account_number = act.account_number
   where adjusted_event_start_date_vespa >= '2014-11-01'
group by adjusted_event_start_date_vespa
        ,tenure

--VIQ table is primary panel only
select panel_id_vespa
,count()
from vespa_analysts.vespa_single_box_view as sbv
inner join viq_viewing_data_scaling as viq on sbv.account_number = viq.account_number
where status_vespa='Enabled'
and adjusted_event_start_date_vespa = '2015-01-06'
group by panel_id_vespa





select adjusted_event_start_date_vespa,count()
from viq_viewing_data_scaling
group by adjusted_event_start_date_vespa


select * from panbal_metrics


