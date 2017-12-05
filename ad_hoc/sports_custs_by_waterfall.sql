  select account_number
    into #sports
    from cust_subs_hist as csh
         inner join cust_entitlement_lookup as cel on csh.current_short_description = cel.short_description
   where csh.status_code in ('AC')
     and csh.subscription_sub_type ='DTV Primary Viewing'
     and effective_to_dt = '9999-09-09'
     and prem_sports > 0
group by account_number
;

--BB breakdown by account
  select knockout_level_bb
        ,knockout_reason_bb
        ,count(1)
    from vespa_analysts.Waterfall_Base as bas
    inner join #sports as spo on bas.account_number = spo.account_number
group by knockout_level_bb
        ,knockout_reason_bb
order by knockout_level_bb
;

--PSTN breakdown by account
  select min(knockout_level_pstn) as level
        ,knockout_reason_pstn
        ,count(1)
    from vespa_analysts.Waterfall_Base as bas
    inner join #sports as spo on bas.account_number = spo.account_number
group by knockout_reason_pstn
order by level
;

--Mix breakdown by account
  select max(knockout_level_mix) as level
        ,knockout_reason_mix
        ,count(1)
    from vespa_analysts.Waterfall_Base as bas
    inner join #sports as spo on bas.account_number = spo.account_number
group by knockout_reason_mix
order by level
;



--rq for sports customers already on the panels
  select sbv.account_number
        ,subscriber_id
        ,case when reporting_quality > 1then 1 else reporting_quality end as rq
    into #temp
    from #results as bas
         inner join vespa_analysts.vespa_single_box_view as sbv on bas.account_number = sbv.account_number
   where knockout_level_pstn = 14
;

drop table #temp2;

  select account_number
        ,min(cast(rq * 10 as int)) as rqr
    into #temp2
    from #temp
group by account_number
;

  select count(1)
        ,rqr
    from #temp2
group by rqr
;



--TA
select sum(ta_propensity) from vespa_analysts.SkyBase_TA_scores as tas
inner join #sports as spo on tas.account_number = spo.account_number
--477952.665890468

select distinct(account_number)
into #sbv
from vespa_analysts.vespa_single_box_view
where status_vespa='Enabled'

select count(1),sum(ta_propensity) from vespa_analysts.SkyBase_TA_scores as tas
inner join #sports as spo on tas.account_number = spo.account_number
inner join #sbv on tas.account_number = #sbv.account_number
--90143.1390826172

select 90143.1390826172 / 477952.665890468
--19%

select count(1),sum(ta_propensity) from vespa_analysts.SkyBase_TA_scores as tas
inner join #sports as spo on tas.account_number = spo.account_number
inner join vespa_analysts.Waterfall_Base as wat on tas.account_number = wat.account_number
where knockout_reason_pstn = 'Potential PSTN panellist'
or knockout_reason_bb = 'Potential BB panellist'
or knockout_reason_pstn = 'Potential Mixed panellist'
--128359.842815094

select 128359.842815094 / 477952.665890468
--27%




select count(1)
from #sports as spo
inner join vespa_analysts.Waterfall_Base as wat on spo.account_number = wat.account_number
where knockout_reason_pstn = 'Potential PSTN panellist'
or knockout_reason_bb = 'Potential BB panellist'
or knockout_reason_pstn = 'Potential Mixed panellist'



select count(1) from vespa_analysts.waterfall_base



  select knockout_level_bb
        ,knockout_reason_bb
        ,count(1)
    from vespa_analysts.Waterfall_Base as bas
group by knockout_level_bb
        ,knockout_reason_bb
order by knockout_level_bb
;



select count (distinct spo.account_number)
from #sports as spo
inner join vespa_analysts.vespa_single_box_view as sbv on spo.account_number = sbv.account_number

4771725
1049125 22%
1086956 23%

select 1086956 / 4771725.0




select count()
from #sports
temp_waterfall_base

select count(),knockout_level_bb,knockout_reason_bb
from waterfall_base as wat
--inner join #sports as spo on wat.account_number = spo.account_number
group by knockout_level_bb,knockout_reason_bb


select count(),knockout_level_pstn,knockout_reason_pstn
from waterfall_base as wat
--inner join #sports as spo on wat.account_number = spo.account_number
where knockout_level_bb<9999
group by knockout_level_pstn,knockout_reason_pstn


drop table p5recon
create table p5recon(panel varchar(10), sub varchar(100))
truncate table p5recon

execute('load table p5recon(
panel'',''
,sub''\n''
)
from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/panel5reconciliation.csv''
escapes off
quotes off
')

update p5recon set sub =replace(sub,'"','')

select count(distinct sub) from p5recon as a inner join cust_card_subscriber_link as b on cast(a.sub as int) = cast(b.card_subscriber_id as int)
select a.sub from p5recon as a left join cust_card_subscriber_link as b on cast(a.sub as int) = cast(b.card_subscriber_id as int)
where b.card_subscriber_id is null

select top 10 * from p5recon
select count() from p5recon

count(distinct a.sub)
4093399
count()
4093402

select sub from
select * from panbal_run_log



select panel,count()
from panel_status as pan
inner join #sports as spo on spo.account_number = pan.account_number
group by panel


alter table panel_status add account_number varchar(30)

create hg index hgsub on panel_status(subint)

update panel_status as bas
set bas.account_number = ccs.account_number
from cust_card_subscriber_link as ccs
where cast(ccs.card_subscriber_id as int) = subint


select panel,count() from panel_status group by panel


select top 10 * from panel_status
where panel='5'

update panel_status
set sub =left(sub,len(sub)-1)
where panel<>'12'

update panel_status set subint=cast(sub as int)

update panel_status
set account_number = ccs.account_number
from cust_card_subscriber_link as ccs
where cast(ccs.card_subscriber_id as int) = subint

select panel,count(distinct account_number)
from panel_status
group by panel

select count(distinct pan.account_number)
from panel_status as pan
inner join #sports as spo on spo.account_number = pan.account_number
where panel in ('11','12')

347740


  select count(distinct stb.account_number)
    from stb_connection_fact as stb
   inner join #sports as spo on spo.account_number = stb.account_number
   where data_return_reliability_metric > 0

     and panel_id_reported in (11, 12)

1.92
1.78
select count(distinct account_number)
from panel_status where panel in ('11','12','5','6')




select knockout_level_bb,knockout_reason_bb,knockout_level_pstn,knockout_reason_pstn
,count(distinct wat.account_number)
from waterfall_base as wat
--inner join #sports as spo on wat.account_number=spo.account_number
where l14_not_vespa_panel = 1
and l08_country=1
group by knockout_level_bb,knockout_reason_bb,knockout_level_pstn,knockout_reason_pstn


count()
4154636

select count() from panel_status as pan
inner join #sports as spo on pan.account_number= spo.account_number
--3917783


select l14_not_vespa_panel,count() from waterfall_base
group by l14_not_vespa_panel

select count() from waterfall_base as bas
inner join panel_status as pan on bas.account_number = pan.account_number

select requested_movement_type,count() from vespa_analysts.panel_movements_log
where requested_movement_type like '%Disable%'
group by requested_movement_type



select top 10 * from stb_connection_fact

select max(panel_age) from stb_connection_fact



select count() from vespa_panel_status
select panel_no,count()from vespa_panel_status group by panel_no

select count() from vespa_subscriber_status
select panel_no,count()from vespa_subscriber_status group by panel_no


select count() from #sports


select count() from waterfall_base as wat inner join #sports as spo on wat.account_number = spo.account_number
4,153,567


select count() from vespa_panel_status as vps
--inner join #sports as spo on vps.account_number = spo.account_number
where panel_no in (11,12)
--2843576

select count() from waterfall_base as bas
inner join #sports as spo on bas.account_number = spo.account_number
where l08_country=1
and l14_not_vespa_panel=0


select count() from #sports as spo
inner join waterfall_base as wat on spo.account_number=wat.account_number
where l08_country=1


select knockout_level_pstn,knockout_reason_pstn
,count(distinct wat.account_number)
from waterfall_base as wat
where knockout_level_bb<9999
group by knockout_level_pstn,knockout_reason_pstn

