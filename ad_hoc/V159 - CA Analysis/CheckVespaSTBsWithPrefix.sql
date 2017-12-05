

-- Ranking callbacks by most recent
select  account_number
       ,subscriber_id
       ,dt
       ,callback_seq
       ,prefix
       ,rank() over (partition by account_number,subscriber_id order by dt desc, callback_seq, prefix desc, row_id desc) as dt_callback_seq
into    dt_callback
from vespa_analysts.waterfall_callback_data
where subscriber_id is not null
and account_number is not null
-- 43978476 row(s) affected

-- remove older callbacks
delete from dt_callback where dt_callback_seq >1
-- 28239970 row(s) deleted


-- Identify STBs in VESPA with a prefix
select panel_id_vespa,enablement_prefix_delta,count(*)
from (
select ca.subscriber_id
        ,sbv.panel_id_vespa
        ,sbv.enablement_date
        ,replace(ca.prefix,'?','') as Prefix
        ,case when prefix <> '' then 1 else 0 end as has_prefix
        ,ca.dt 
        ,case 
            when datediff(day,sbv.enablement_date,ca.dt) >= 0 
            then 'Last callback after panel enablement'
            else 'Last callback before panel enablement'
        end as enablement_prefix_delta       
from dt_callback ca
inner join Vespa_analysts.vespa_single_box_view sbv 
on ca.subscriber_id = sbv.subscriber_id
where sbv.status_vespa = 'Enabled'
) t
where has_prefix = 1
group by panel_id_vespa,enablement_prefix_delta
order by 3 desc

-- List all boxes with prefix in the panel 
select subscriber_id,panel_id_vespa,prefix,reporting_quality,enablement_date
from (
select ca.subscriber_id
        ,sbv.panel_id_vespa
        ,replace(ca.prefix,'?','') as Prefix
        ,case when prefix <> '' then 1 else 0 end as has_prefix
        ,reporting_quality
        ,enablement_date
from dt_callback ca
inner join Vespa_analysts.vespa_single_box_view sbv 
on ca.subscriber_id = sbv.subscriber_id
where sbv.status_vespa like 'Enable%'
) t
where has_prefix = 1
order by 2,1
-- 

-- List also boxes in HHs on the panel that have at least one DRX 595 
select subscriber_id,panel_id_vespa
from Vespa_analysts.vespa_single_box_view 
where status_vespa like 'Enable%'
and account_number in (select account_number from kinnairt.DRX_595_BOXES_LIST)
order by 2,1

select panel_id_vespa,status_vespa,count(*)
from Vespa_analysts.vespa_single_box_view 
where account_number in (select account_number from kinnairt.DRX_595_BOXES_LIST)
and status_vespa like 'Enable%'
group by panel_id_vespa,status_vespa
order by panel_id_vespa,status_vespa

