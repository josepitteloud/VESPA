drop table survey_data;
drop table freesat_returns;
drop table roi_returns;

      -- load survey data
  create table survey_data(
         account_number varchar(30)
        ,changing_to    varchar(30)
        ,reason         varchar(30)
        )
;

execute('
    load table survey_data(
         account_number'','',
         changing_to'','',
         reason''\n'')
   from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/leavers_survey.csv''
escapes off
 quotes off
   skip 1
       ')
;

      -- load Freesat data
  create table freesat_returns_import(
         subscriber_id bigint
        ,dt            varchar(100)
        )
;

execute('
    load table freesat_returns_import(
         subscriber_id'','',
        dt''\n'')
   from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/Freesat_returns.csv''
escapes off
 quotes off
   skip 1
');

  create table freesat_returns(
         subscriber_id bigint
        ,dt            date
        ,groupx        tinyint default 0
        )
;

  insert into freesat_returns(subscriber_id,dt)
  select subscriber_id
        ,cast(left(dt, 10) as date)
    from freesat_returns_import
   where dt not like 'dt%'
;

    drop table freesat_returns_import;


      -- load RoI data
  create table roi_returns_import(
         subscriber_id bigint
        ,dt            varchar(30)
        )
;

execute('
    load table roi_returns_import(
         subscriber_id'','',
        dt''\n'')
   from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/roi_returns.csv''
escapes off
 quotes off
       ')
;

  create table roi_returns(
         subscriber_id bigint
        ,dt            date
        )
;

  insert into roi_returns
  select subscriber_id
        ,cast(substr(dt, 7, 4) || '-' || substr(dt, 4, 2) || '-' || substr(dt, 1, 2) as date)
    from roi_returns_import
   where dt not like 'dt%'
;

drop table roi_returns_import;


      -- FreeSat results
  update freesat_returns as bas
     set groupx = case requested_movement_type when 'FreeSat panel test' then 1
                                               when 'Freesat Test Adds'  then 2 else 0 end
    from vespa_analysts.panel_movements_log as log
   where bas.subscriber_id = cast(log.card_subscriber_id as int)
;

  delete from freesat_returns
   where groupx = 0
;

  select subscriber_id
        ,min(dt) as dt
    into #min_dt
    from freesat_returns
group by subscriber_id
;

  select dt
    into #dates
    from freesat_returns
group by dt
;

  select bas.subscriber_id
        ,dte.dt
        ,sum(case when bas.subscriber_id is null then 0 else 1 end * 1.0) / max(case when dte.dt - mdt.dt > 6 then 7 else dte.dt - mdt.dt + 1 end) as rq
        ,groupx
    into #results
    from #dates                                        as dte
         inner join freesat_returns                    as bas on dte.dt between bas.dt and bas.dt + 6
         left  join #min_dt                            as mdt on mdt.subscriber_id = bas.subscriber_id
group by bas.subscriber_id
        ,dte.dt
        ,groupx
;

  select dt
        ,groupx
        ,count() as cow
    into #counts
    from freesat_returns
group by dt
        ,groupx
order by dt
;

  select res.dt
        ,avg(rq)
        ,min(cow)
    from #results as res
         inner join #counts as cow on res.dt = cow.dt
group by res.dt
;

  select changing_to
    into #changing_to_options
    from survey_data
group by changing_to
;

  select res.dt
        ,avg(rq) as rq_
        ,min(cow) as cow
        ,sum(case when changing_to = 'BT TV'        then rq else 0 end) as bt
        ,sum(case when changing_to = 'EE TV'        then rq else 0 end) as ee
        ,sum(case when changing_to = 'Free Option'  then rq else 0 end) as free
        ,sum(case when changing_to = 'Moving'       then rq else 0 end) as moving
        ,sum(case when changing_to = 'NowTV'        then rq else 0 end) as now
        ,sum(case when changing_to = 'Online'       then rq else 0 end) as online
        ,sum(case when changing_to = 'Other'        then rq else 0 end) as other
        ,sum(case when changing_to = 'Q39'          then rq else 0 end) as q39
        ,sum(case when changing_to = 'TalkTalk TV'  then rq else 0 end) as tt
        ,sum(case when changing_to = 'Virgin Media' then rq else 0 end) as virgin
        ,sum(case when changing_to = 'BT TV'        then 1  else 0 end) as bt_count
        ,sum(case when changing_to = 'EE TV'        then 1  else 0 end) as ee_count
        ,sum(case when changing_to = 'Free Option'  then 1  else 0 end) as free_count
        ,sum(case when changing_to = 'Moving'       then 1  else 0 end) as moving_count
        ,sum(case when changing_to = 'NowTV'        then 1  else 0 end) as now_count
        ,sum(case when changing_to = 'Online'       then 1  else 0 end) as online_count
        ,sum(case when changing_to = 'Other'        then 1  else 0 end) as other_count
        ,sum(case when changing_to = 'Q39'          then 1  else 0 end) as q39_count
        ,sum(case when changing_to = 'TalkTalk TV'  then 1  else 0 end) as tt_count
        ,sum(case when changing_to = 'Virgin Media' then 1  else 0 end) as virgin_count
        ,res.groupx
    into #results2
    from #results as res
         inner join #counts                              as cow on res.dt             = cow.dt
                                                               and res.groupx         = cow.groupx
         left  join vespa_analysts.vespa_single_box_view as sbv on res.subscriber_id  = sbv.subscriber_id
         left  join survey_data                          as sur on sbv.account_number = sur.account_number
group by res.dt
        ,res.groupx
;

  select dt
        ,groupx
        ,rq_
        ,cow
        ,case when bt_count = 0     then 0 else bt     / bt_count     end as bt_arq
        ,case when ee_count = 0     then 0 else ee     / ee_count     end as ee_arq
        ,case when free_count = 0   then 0 else free   / free_count   end as free_arq
        ,case when moving_count = 0 then 0 else moving / moving_count end as moving_arq
        ,case when now_count = 0    then 0 else now    / now_count    end as now_arq
        ,case when online_count = 0 then 0 else online / online_count end as online_arq
        ,case when other_count = 0  then 0 else other  / other_count  end as other_arq
        ,case when q39_count = 0    then 0 else q39    / q39_count    end as q39_arq
        ,case when tt_count = 0     then 0 else tt     / tt_count     end as tt_arq
        ,case when virgin_count = 0 then 0 else virgin / virgin_count end as virgin_arq
        ,bt_count
        ,ee_count
        ,free_count
        ,moving_count
        ,now_count
        ,online_count
        ,other_count
        ,q39_count
        ,tt_count
        ,virgin_count
    from #results2
;

  select dt
        ,max(case when groupx = 1 then rq_ else 0 end) as group1_rq
        ,max(case when groupx = 2 then rq_ else 0 end) as group2_rq
        ,max(case when groupx = 1 then cow else 0 end) as group1_returns
        ,max(case when groupx = 2 then cow else 0 end) as group2_returns
    from #results2
group by dt
;

select count() from #results where dt = '2016-02-23' and groupx=1
select * from #results where dt = '2016-02-23' and groupx=1
select count(distinct subscriber_id) from #results where dt = '2016-02-23' and groupx=1

select * from #results where dt = '2016-02-23' and groupx=1;
select * from freesat_returns where dt = '2016-02-23' and groupx = 1;

select dt,count() from freesat_returns  group by dt


      --------------
      -- RoI results
  select subscriber_id
        ,min(dt) as dt
    into #roi_min_dt
    from roi_returns
group by subscriber_id
;

  select dt
    into #roi_dates
    from roi_returns
group by dt
;

  select bas.subscriber_id
        ,dte.dt
        ,sum(case when bas.subscriber_id is null then 0 else 1 end * 1.0) / max(case when dte.dt - mdt.dt > 6 then 7 else dte.dt - mdt.dt + 1 end) as rq
    into #roi_results
    from #roi_dates                 as dte
         inner join roi_returns     as bas on dte.dt between bas.dt and bas.dt + 6
         left  join #roi_min_dt     as mdt on mdt.subscriber_id = bas.subscriber_id
group by bas.subscriber_id
        ,dte.dt
;

  select dt
        ,count() as cow
    into #roi_counts
    from roi_returns
group by dt
order by dt
;

  select res.dt
        ,avg(rq)
        ,min(cow)
    from #roi_results as res
         inner join #roi_counts as cow on res.dt = cow.dt
group by res.dt
;






---
select top 10 * from freesat_returns_import
select top 10 * from freesat_returns




select top 10 * from barb_daily_ind_prog_viewed

select max(local_start_time_of_session)
from barb_daily_ind_prog_viewed

select count(distinct household_number)
,sky_stb_holder_hh
,sky_stb_viewing
from barb_daily_ind_prog_viewed
where date(local_start_time_of_session) >= '2016-01-01'
group by sky_stb_viewing
,sky_stb_holder_hh



select top 10 *
from BARB_PVF_VWREC_PANEL_MEM

select count(distinct household_number)
,viewing_platform
from BARB_PVF_VWREC_PANEL_MEM
where date_of_activity >= '2016-01-01'
group by viewing_platform

select count(distinct household_number)
from barb_daily_ind_prog_viewed
where date(local_start_time_of_session) = '2016-01-01'
;
select count(distinct household_number)
from BARB_PVF_VWREC_PANEL_MEM
where date_of_activity >= '2016-01-01'

select distinct household_number
into #temp
from barb_daily_ind_prog_viewed
where date(local_start_time_of_session) = '2016-01-01'
;
select viewing_platform,count(distinct bas.household_number)
from BARB_PVF_VWREC_PANEL_MEM as bas
left join #temp as tmp on bas.household_number = tmp.household_number
where tmp.household_number is null
and date_of_activity >= '2016-01-01'
group by viewing_platform

select top 10 * from barb_panel_demogr_home_char




select count(distinct subscriber_id) from freesat_returns
select top 10 * from freesat_returns



select maxdt,count() from
(select max(dt) as maxdt,subscriber_id from freesat_returns group by subscriber_id) as sub
group by maxdt

select top 10 * from vespa_analysts.panel_movements_log
select requested_movement_type,count()  from vespa_analysts.panel_movements_log
where requested_movement_type in ('FreeSat panel test' ,'Freesat Test Adds')
group by requested_movement_type


select * from
(select max(dt) as maxdt,subscriber_id from freesat_returns group by subscriber_id) as sub
inner join vespa_subscriber_status as vss on cast(vss.card_subscriber_id as int)= sub.subscriber_id
where maxdt='2016-01-27'

2015-12-14-SKY-SKY-SPMSQ-P005-0003.xml

select top 10 * from barb_viewing_data
select top 10 * from barb_panel_demogr_tv_char
select digital_satellite,count() from barb_panel_demogr_tv_char group by digital_satellite


select distinct(account_number) into #tmp from vespa_subscriber_status where panel_no=1

select tmp.account_number into #a from #tmp as tmp inner join cust_single_account_view as sav on sav.account_number = tmp.account_number where cust_active_dtv = 1
85 are marked as active

select status_start_dt
,status_code
from cust_subs_hist as csh
inner join #a on csh.account_number = #a.account_number
where subscription_sub_type ='DTV Primary Viewing'
and status_end_dt > '2016-02-24'

--all enabled since July, which is when the initial selection was made

select requested_movement_type,count()  from vespa_analysts.panel_movements_log as log
inner join #tmp on #tmp.account_number = log.account_number
where requested_movement_type in ('FreeSat panel test' ,'Freesat Test Adds')
group by requested_movement_type


--all accounts remaining are from the original 'FreeSat panel test' group


select top 10 * from freesat_returns



select top 10 * from BARB_PVF_VWREC_PANEL_MEM

select barb_panel_demogr_tv_char
date_of_activity
household_number
digital_satellite

select digital_satellite
,count()
from barb_panel_demogr_tv_char
group by digital_satellite

select count()
from barb_panel_demogr_tv_char

select * from panbal_run_log
select top 10 * from waterfall_base

  select account_number
        ,case when requested_movement_type when 'FreeSat panel test' then 1
                                           when 'Freesat Test Adds'  then 2 else 0 end
    from vespa_analysts.panel_movements_log as log
         inner join panbal_segment_snapshots as snp as snp on log.account_number = snp.account_number
         inner join panbal_segments_lookup_normalised as lkp on snp.segment_id = lkp.segment_id
   where requested_movement_type in ('FreeSat panel test' ,'Freesat Test Adds')




select * from panbal_metrics


select count()
    from vespa_analysts.panel_movements_log as log



select top 10 * from freesat_returns
select count(distinct subscriber_id)
 from freesat_returns


select min(dt),max(dt)
 from freesat_returns


 select distinct(card_subscriber_id)
    from vespa_analysts.panel_movements_log as log
   where requested_movement_type in ('FreeSat panel test' ,'Freesat Test Adds')

select distinct(subscriber_id) from freesat_returns


      -- Pre RQ
  select account_number
        ,groupx = case requested_movement_type when 'FreeSat panel test' then 1
                                               when 'Freesat Test Adds'  then 2 else 0 end
    into #accs
    from vespa_analysts.panel_movements_log as log
   where requested_movement_type in ('FreeSat panel test' ,'Freesat Test Adds')

;

  select viq.account_number
        ,adjusted_event_start_date_vespa as dt
    into #viq
    from viq_viewing_data_scaling as viq
         inner join #accs as acc on viq.account_number = acc.account_number
   where adjusted_event_start_date_vespa >= '2015-01-01'
group by viq.account_number
        ,adjusted_event_start_date_vespa
;

  select vi1.account_number
        ,vi1.dt
        ,sum(case when vi1.account_number is null then 0 else 1 end * 1.0) / 7 as rq
    into #by_acc
    from #viq as vi1
         inner join #viq as vi2 on vi1.account_number = vi2.account_number
                               and vi2.dt between vi1.dt-6 and vi1.dt
group by vi1.account_number
        ,vi1.dt
;

  select dt
        ,avg(rq) as arq
    from #by_acc
group by dt
;

      -- Pre Count
  select dt
        ,count()
    from #viq
group by dt



