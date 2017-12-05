--Landmark Feed UAT Rec-check

--Defect 001
select count(1) from smi_dw.smi_etl.VIEWING_BY_MINUTE_FACT
--5039138
where dk_tx_time is null
and TX_DATETIME is null
--2

--Defect 002
select count(1) from smi_dw.smi_etl.VIEWING_BY_MINUTE_FACT
where tx_day=-1

--Defect 003
select * from smi_dw.smi_etl.VIEWING_BY_MINUTE_FACT
where tx_datetime='2012-12-07 23:59:00'
and dk_broadcast_channel=8500002
;

  select count(1) as cow
        ,tx_datetime
        ,dk_broadcast_channel
        ,tx_day
    from smi_dw.smi_etl.VIEWING_BY_MINUTE_FACT
--   where tx_datetime < '2013-01-12 12:00:00'
group by tx_datetime
        ,dk_broadcast_channel
        ,tx_day
  having cow>1
order by tx_datetime desc
limit 100
;

select * from smi_dw.smi_etl.VIEWING_BY_MINUTE_FACT
where tx_datetime='2013-01-12 11:59:00'
and dk_broadcast_channel=2100008
;


---
select dk_tx_date,count(1),sum(case when tx_day=-1 then 1 else 0 end) from ADMIN.VIEWING_BY_MINUTE_FACT
group by dk_tx_date
order by dk_tx_date




select * from ADMIN.VESPA_UNIVERSE_FACT
limit 10


select max(dk_tx_date) from ADMIN.v_VIEWING_BY_MINUTE_FACT
