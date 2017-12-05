select * from sk_prod.cust_set_top_box where account_number in ('621991609911')
and active_box_flag = 'Y'
;

select account_number,si_external_identifier,src_system_id from sk_prod.cust_service_instance
where src_system_id in ('13734859506733389284679'
,'13734859506653389284677')
;


select max(event_start_date_time_utc) from sk_prod.vespa_dp_prog_viewed_current;
select max(dt) from vespa_analysts.alt_panel_data;

create table #subs(subid int);

insert into #subs values (13906010);
insert into #subs values (31751536);
insert into #subs values (11333060);
insert into #subs values (7015999);
insert into #subs values (32028696);
insert into #subs values (4884545);
insert into #subs values (30545549);
insert into #subs values (14822911);
insert into #subs values (31821930);
insert into #subs values (26936458);
insert into #subs values (16943482);
insert into #subs values (18379406);
insert into #subs values (21390398);
insert into #subs values (32000863);
insert into #subs values (20740369);
insert into #subs values (30490146);
insert into #subs values (18543911);
insert into #subs values (24418236);
insert into #subs values (24778475);
insert into #subs values (29060085);
insert into #subs values (26260085);
insert into #subs values (26260087);
insert into #subs values (28605332);
insert into #subs values (28605328);
insert into #subs values (30660468);
insert into #subs values (31806117);
insert into #subs values (32135833);
insert into #subs values (28636148);
insert into #subs values (28636150);
insert into #subs values (28401486);
insert into #subs values (24453862);
insert into #subs values (24991472);
insert into #subs values (19256951);
insert into #subs values (18372125);
insert into #subs values (9237177);
insert into #subs values (10476891);
insert into #subs values (32393261);
insert into #subs values (32393260);










-- select subscriber_id
--       ,date(event_start_date_time_utc) as dt
--   into #results
--   from sk_prod.vespa_dp_prog_viewed_current as pvc
--        inner join #subs on pvc.subscriber_id = #subs.subid
-- ;

select subid
      ,max(case when dt='2013-07-20' and data_received=1 then 1 else 0 end) as d07_20
      ,max(case when dt='2013-07-21' and data_received=1 then 1 else 0 end) as d07_21
      ,max(case when dt='2013-07-22' and data_received=1 then 1 else 0 end) as d07_22
      ,max(case when dt='2013-07-23' and data_received=1 then 1 else 0 end) as d07_23
      ,max(case when dt='2013-07-24' and data_received=1 then 1 else 0 end) as d07_24
      ,max(case when dt='2013-07-25' and data_received=1 then 1 else 0 end) as d07_25
      ,max(case when dt='2013-07-26' and data_received=1 then 1 else 0 end) as d07_26
      ,max(case when dt='2013-07-27' and data_received=1 then 1 else 0 end) as d07_27
      ,max(case when dt='2013-07-28' and data_received=1 then 1 else 0 end) as d07_28
      ,max(case when dt='2013-07-29' and data_received=1 then 1 else 0 end) as d07_29
      ,max(case when dt='2013-07-30' and data_received=1 then 1 else 0 end) as d07_30
      ,max(case when dt='2013-07-31' and data_received=1 then 1 else 0 end) as d07_31
      ,max(case when dt='2013-08-01' and data_received=1 then 1 else 0 end) as d08_01
      ,max(case when dt='2013-08-02' and data_received=1 then 1 else 0 end) as d08_02
      ,max(case when dt='2013-08-03' and data_received=1 then 1 else 0 end) as d08_03
      ,max(case when dt='2013-08-04' and data_received=1 then 1 else 0 end) as d08_04
      ,max(case when dt='2013-08-05' and data_received=1 then 1 else 0 end) as d08_05
from #subs left join vespa_analysts.alt_panel_data as apd on #subs.subid = cast(apd.subscriber_id as int)
group by subid



select max(dt) from



select account_number
,max(TSA_OPT_IN)
--from sk_prod.cust_single_account_view
--from vespa_analysts.ConsentIssue_05_Revised_Consent_Info
                   FROM sk_prod.SAM_REGISTRANT
where account_number in (
'290000138450'
,'620008042322'
,'621029102913'
,'621061607746'
,'621341151515'
,'621840928496'
,'621965044475'
,'630130010063'
,'621937166810'
,'621641555647'
,'621461916952'
,'621360907573'
,'621058333462'
,'621991609911'
,'621084391237'
,'621341986431'
,'400019859646'
)
group by account_number

select account_number, CUST_VIEWING_DATA_CAPTURE_ALLOWED
from sk_prod.cust_single_account_view
where account_number in (
'290000138450'
,'620008042322'
,'621029102913'
,'621061607746'
,'621341151515'
,'621840928496'
,'621965044475'
,'630130010063'
,'621937166810'
,'621641555647'
,'621461916952'
,'621360907573'
,'621058333462'
,'621991609911'
,'621084391237'
,'621341986431'
,'400019859646'
)


