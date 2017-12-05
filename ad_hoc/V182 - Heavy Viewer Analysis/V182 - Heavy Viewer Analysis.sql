--drop table v182_duration_by_channel_and_account; drop table v182_duration_by_channel_day; commit;
---V182 Heavy Viewing Code---

----Top Channels of 'Heavy' viewers watching any given channel---
---Create Full Daily Viewing Details for a 0.1% sample (Phase II Onwards)---
CREATE TABLE  v182_duration_by_channel_and_account
    (
            Account_Number                             varchar(20)  not null
            ,channel_name_inc_hd_staggercast                              varchar(30)
            ,total_duration                                   int
)
;

CREATE TABLE  v182_duration_by_channel_day
    (
            Account_Number                             varchar(20)  not null
            ,channel_name_inc_hd_staggercast                              varchar(30)
            ,total_duration                                   int
)
;

CREATE TABLE  v182_account_viewing_days
    ( 
            Account_Number                             varchar(20)  not null
            ,viewing_day                      date
            ,viewing_post_6am                      tinyint
)
;

commit;
CREATE VARIABLE @snapshot_start_dt              datetime;
CREATE VARIABLE @snapshot_end_dt                datetime;
CREATE VARIABLE @viewing_var_sql                varchar(5000);
CREATE VARIABLE @viewing_scanning_day           datetime;
CREATE VARIABLE @playback_snapshot_start_dt            datetime;

SET @viewing_var_sql = '
insert into v182_duration_by_channel_day (
 select
                
                a.Account_Number
               ,channel_name_inc_hd_staggercast
            ,sum(viewing_duration) as total_duration
from vespa_analysts.VESPA_DAILY_AUGS_##^^*^*## as a
left outer join epg_data_phase_2 as b 
on a.programme_trans_sk=b.programme_trans_sk
group by a.Account_Number
,channel_name_inc_hd_staggercast
)

delete from v182_duration_by_channel_and_account where account_number is null or account_number is not null

insert into v182_duration_by_channel_and_account (
 select
                
                Account_Number
               ,channel_name_inc_hd_staggercast
            ,sum(total_duration) as total_duration
from v182_duration_by_channel_day
group by Account_Number
               ,channel_name_inc_hd_staggercast
)

delete  from v182_duration_by_channel_day where account_number is null or account_number is not null

insert into v182_duration_by_channel_day (
 select
                
                Account_Number
               ,channel_name_inc_hd_staggercast
            ,total_duration
from v182_duration_by_channel_and_account
)

insert into v182_account_viewing_days
(select account_number
,min(cast(viewing_starts as date)) as viewing_date
,max(case when dateformat(viewing_starts,''HH'') in (''00'',''01'',''01'',''03'',''04'',''05'') then 0 else 1 end) as viewing_post_6am
from vespa_analysts.VESPA_DAILY_AUGS_##^^*^*##
group by account_number
)
'
;
SET @snapshot_start_dt  = '2013-01-01';
SET @snapshot_end_dt    = '2013-01-30'; 

SET @viewing_scanning_day = @snapshot_start_dt;

while @viewing_scanning_day <= @snapshot_end_dt
begin
    EXECUTE(replace(@viewing_var_sql,'##^^*^*##',dateformat(@viewing_scanning_day, 'yyyymmdd')))
    commit

    set @viewing_scanning_day = dateadd(day, 1, @viewing_scanning_day)
end

commit;

---Add Channel Rank---
--drop table #channel_rank;
--drop table #total_duration_by_account;

select account_number ,channel_name_inc_hd_staggercast,  rank() over (partition by account_number order by total_duration desc) as rank_id 
into #channel_rank
from v182_duration_by_channel_and_account 
;

commit;
create hg index idx1 on #channel_rank(account_number);
create hg index idx2 on #channel_rank(channel_name_inc_hd_staggercast);


--select top 500 * from #channel_rank
select account_number 
,sum(total_duration) as all_channel_duration
into #total_duration_by_account
from v182_duration_by_channel_and_account 
group by account_number
;

commit;
create hg index idx1 on #total_duration_by_account(account_number);
--select top 500 * from #total_duration_by_account

alter table v182_duration_by_channel_and_account add channel_view_rank integer;

update v182_duration_by_channel_and_account
set channel_view_rank=b.rank_id
from v182_duration_by_channel_and_account as a
left outer join #channel_rank as b
on a.account_number = b.account_number and a.channel_name_inc_hd_staggercast=b.channel_name_inc_hd_staggercast
;


alter table v182_duration_by_channel_and_account add all_channel_duration integer;

update v182_duration_by_channel_and_account
set all_channel_duration=b.all_channel_duration
from v182_duration_by_channel_and_account as a
left outer join #total_duration_by_account as b
on a.account_number = b.account_number 
;
commit;

select account_number
,sum(viewing_post_6am) as days_viewing_captured
into v182_account_list
from v182_account_viewing_days
group by account_number
;

commit;
CREATE UNIQUE INDEX idx1 ON v182_account_list(account_number);
---Add weighting of mid-day in period----
CREATE VARIABLE @snapshot_start_dt datetime;
set @snapshot_start_dt='2013-01-15';

--OBTAIN WEIGHTING;
IF object_ID ('reconcile_weights') IS NOT NULL THEN
DROP TABLE  reconcile_weights
END IF;

select          a.account_number
                ,b.weighting as weighting
into            reconcile_weights
from            vespa_analysts.SC2_intervals as a
inner join      vespa_analysts.SC2_weightings as b
on              cast(@snapshot_start_dt as date) = b.scaling_day
and             a.scaling_segment_ID = b.scaling_segment_ID
and             cast(@snapshot_start_dt as date) between a.reporting_starts and a.reporting_ends
;

commit;
CREATE UNIQUE INDEX idx1 ON reconcile_weights(account_number);

alter table v182_account_list add account_weight float;

update v182_account_list 
set account_weight=b.weighting
from v182_account_list as a
left outer join reconcile_weights as b
on a.account_number = b.account_number
;
--select sum(account_weight) from v182_account_list where days_viewing_captured>=20

----Generate List of Channels---
--drop table v182_distinct_channel_list;
select channel_name_inc_hd_staggercast
,rank() over (order by channel_name_inc_hd_staggercast) as rank_id 
into v182_distinct_channel_list
from v182_duration_by_channel_and_account
where channel_name_inc_hd_staggercast is not null
group by channel_name_inc_hd_staggercast
;
--select * from v182_distinct_channel_list;
---Add Account Profile Info (as at start/mid/end?)---
commit;


---Create Loop going through each account to get total viewing details of all channels from 'Top' viewers of each channel--

--Initial Split Top viewers defined as where channel is in their Top 10---

CREATE VARIABLE @channel_num_min              integer;
CREATE VARIABLE @channel_num_max              integer;
CREATE VARIABLE @channel_num_latest              integer;
CREATE VARIABLE @channel_name              varchar(150);

set @channel_num_min  =(select min (rank_id) from v182_distinct_channel_list);
set @channel_num_max  =(select max (rank_id) from v182_distinct_channel_list);
set @channel_num_latest=1;

--set @channel_num_latest=2; set @channel_num_max=2;

set @channel_name = (select channel_name_inc_hd_staggercast from v182_distinct_channel_list where rank_id = @channel_num_latest);
--select @channel_name;
SET @viewing_var_sql = '
--insert into v182_duration_by_channel_day (
 select *
into v182_test
from v182_duration_by_channel_and_account
where channel_name_inc_hd_staggercast=@channel_name and channel_view_rank<=1000
 
--)
'

;


while @channel_num_latest <= @channel_num_max
begin
    EXECUTE(@viewing_var_sql)
    commit

    set @channel_num_latest = @channel_num_latest+1

    set @channel_name = (select channel_name_inc_hd_staggercast from v182_distinct_channel_list where rank_id = @channel_num_latest)
end

commit;


select * from  v182_test;
select @channel_num_latest 
select  @channel_num_max







/*
select days_viewing_captured
,count(*)
from #days_viewing_captured
group by days_viewing_captured
order by days_viewing_captured
*/








----Initial Analysis----

select channel_name_inc_hd_staggercast
,count(*) as accounts
,sum(total_duration) as tot_dur
,sum(days_viewing_captured) as account_viewing_days
,avg(channel_view_rank)
,sum(case when channel_view_rank<=1 then 1 else 0 end) accounts_with_channel_in_top_1
,sum(case when channel_view_rank<=3 then 1 else 0 end) accounts_with_channel_in_top_3
,sum(case when channel_view_rank<=10 then 1 else 0 end) accounts_with_channel_in_top_10
,sum(case when channel_view_rank<=20 then 1 else 0 end) accounts_with_channel_in_top_20
from v182_duration_by_channel_and_account as a
left outer join v182_account_list as b
on a.account_number = b.account_number
where b.days_viewing_captured>=20
--where channel_view_rank=15
group by channel_name_inc_hd_staggercast
order by tot_dur desc
;

select account_number
,max(case when channel_view_rank=1 then channel_name_inc_hd_staggercast else null end) as channel_rank_01
,max(case when channel_view_rank=2 then channel_name_inc_hd_staggercast else null end) as channel_rank_02
,max(case when channel_view_rank=3 then channel_name_inc_hd_staggercast else null end) as channel_rank_03
into #acc_summary
from v182_duration_by_channel_and_account
group by account_number
--
;






select channel_rank_01,channel_rank_02,channel_rank_03
,count(*) as accounts
from #acc_summary
group by channel_rank_01,channel_rank_02,channel_rank_03
order by accounts desc

/*
select top 500 * from v182_duration_by_channel_and_account order by account_number , channel_view_rank;
select top 500 * from v182_duration_by_channel_day;
select * from vespa_analysts.VESPA_DAILY_AUGS_20130101 as a
left outer join epg_data_phase_2 as b 
on a.programme_trans_sk=b.programme_trans_sk
where account_number = '200000881629' and channel_name = 'BBC NEWS'

select count(* ) from v182_duration_by_channel_and_account
select count(distinct account_number ) from v182_duration_by_channel_and_account
commit;
*/
