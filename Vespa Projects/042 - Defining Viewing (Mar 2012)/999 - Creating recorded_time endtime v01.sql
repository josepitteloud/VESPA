

----Take 1 % sample of table to use to create update of end time viewing---
--select * from vespa_analysts.VESPA_all_viewing_records_20120115_1pc;
--drop table vespa_analysts.VESPA_all_viewing_records_20120115_1pc;

select * into vespa_analysts.VESPA_all_viewing_records_20120115_1pc from  vespa_analysts.VESPA_all_viewing_records_20120115 
where right(cast(subscriber_id as varchar(264)),2) ='54'
order by subscriber_id , adjusted_event_start_time , x_adjusted_event_end_time ,tx_start_datetime_utc ;
commit;

alter table  vespa_analysts.VESPA_all_viewing_records_20120115_1pc add row_num integer identity;
commit;

--select top 500 * from vespa_analysts.VESPA_all_viewing_records_20120115_1pc;

alter table  vespa_analysts.VESPA_all_viewing_records_20120115_1pc add next_record_recorded_time_utc datetime;
commit;


create hg index idx1 on vespa_analysts.VESPA_all_viewing_records_20120115_1pc(row_num);
create hg index idx2 on vespa_analysts.VESPA_all_viewing_records_20120115_1pc(subscriber_id);

update vespa_analysts.VESPA_all_viewing_records_20120115_1pc as a 
set next_record_recorded_time_utc = case    when a.subscriber_id<>b.subscriber_id then null
                                            when a.x_viewing_end_time<>b.x_viewing_start_time then null
                                            when a.recorded_time_utc=b.recorded_time_utc then null
                                            when a.recorded_time_utc is not null then b.recorded_time_utc else null end
from vespa_analysts.VESPA_all_viewing_records_20120115_1pc as a
left outer join vespa_analysts.VESPA_all_viewing_records_20120115_1pc as b
on a.row_num = b.row_num-1
;

--Update end of viewing time based on subsequent record

update vespa_analysts.VESPA_all_viewing_records_20120115_1pc
set viewing_record_end_time_local = case when next_record_recorded_time_utc is null then viewing_record_end_time_local
                                          
when dateformat(next_record_recorded_time_utc,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,next_record_recorded_time_utc) 
when dateformat(next_record_recorded_time_utc,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,next_record_recorded_time_utc) 
when dateformat(next_record_recorded_time_utc,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,next_record_recorded_time_utc) 
                    else next_record_recorded_time_utc  end 
;
commit;

create hg index idx3 on vespa_analysts.VESPA_all_viewing_records_20120115_1pc(viewing_record_start_time_local);
create hg index idx4 on vespa_analysts.VESPA_all_viewing_records_20120115_1pc(viewing_record_end_time_local);
--select top 500 * from vespa_analysts.VESPA_all_viewing_records_20120115_1pc;

----Create a second by second for Sky1 for 1pc sample


--drop table vespa_analysts.second_by_second_20120115_sky1;
---Create table to insert into loop---
create table vespa_analysts.second_by_second_20120115_sky1_1pc
(

subscriber_id                       decimal(8)              not null
--,account_number                     varchar(20)             null
,second_viewed                      datetime                not null
,viewed                             smallint                not null
,viewed_live                        smallint                null
,viewed_playback                    smallint                null
,viewed_dual_speed                    smallint                null
,viewed_6x_speed                    smallint                null
,viewed_12x_speed                    smallint                null
,viewed_30x_speed                    smallint                null
);
commit;


---Create second by second log---
create variable @programme_time_start datetime;
create variable @programme_time_end datetime;
create variable @programme_time datetime;

set @programme_time_start = cast('2012-01-15 00:00:00' as datetime);
set @programme_time_end =cast('2012-01-16 00:00:00' as datetime);
set @programme_time = @programme_time_start;


---Start of Loop
WHILE @programme_time <  @programme_time_end LOOP
insert into vespa_analysts.second_by_second_20120115_sky1_1pc
select subscriber_id
--,account_number
,@programme_time as second_viewed
,1 as viewed
,max(case when play_back_speed is null then 1 else 0 end) as viewed_live
,max(case when play_back_speed =2 then 1 else 0 end) as viewed_playback
,max(case when play_back_speed =4 then 1 else 0 end) as viewed_dual_speed
,max(case when play_back_speed =12 then 1 else 0 end) as viewed_6x_speed
,max(case when play_back_speed =24 then 1 else 0 end) as viewed_12x_speed
,max(case when play_back_speed =60 then 1 else 0 end) as viewed_30x_speed

from vespa_analysts.VESPA_all_viewing_records_20120115_1pc
where  cast(viewing_record_start_time_local as datetime)<=@programme_time and cast(viewing_record_end_time_local as datetime)>@programme_time
--and (play_back_speed is null or play_back_speed = 2)
and channel_name_inc_hd = 'Sky 1'
group by subscriber_id
--,account_number 
,second_viewed
,viewed
;

 SET @programme_time =dateadd(second,1,@programme_time);
    COMMIT;

END LOOP;
commit;

--select  @programme_time;


select count(*) from vespa_analysts.second_by_second_20120115_sky1_1pc;

select second_viewed
,sum(viewed_live) 
,sum(viewed_playback)
,sum(viewed_dual_speed)
,sum(viewed_6x_speed)
,sum(viewed_12x_speed)
,sum(viewed_30x_speed)

from vespa_analysts.second_by_second_20120115_sky1_1pc
group by second_viewed
order by second_viewed
;






























