/*

Probabilistic Capping
Capped vs uncapped event end times

*/

-- Have a look at an augmented table
select top 1000 * from vespa_analysts.vespa_daily_augs_20121105

-- Look at proportion of capped/uncapped events in augmented table
select case when capped_event_end_time is null then 0 else 1 end as capped
        ,count(*) as Num_Events
from vespa_analysts.vespa_daily_augs_20121105
group by capped
order by capped
/*
0       20,340,890
1       1,066,149
*/

-- Combine augmented tables for week 5-11 Nov
-- drop table Vespa_Augs_20121105_11
drop table Vespa_Augs_20121105_11
select *
into Vespa_Augs_20121105_11
from (
select * from vespa_analysts.vespa_daily_augs_20121105
union all
select * from vespa_analysts.vespa_daily_augs_20121106
union all
select * from vespa_analysts.vespa_daily_augs_20121107
union all
select * from vespa_analysts.vespa_daily_augs_20121108
union all
select * from vespa_analysts.vespa_daily_augs_20121109
union all
select * from vespa_analysts.vespa_daily_augs_20121110
union all
select * from vespa_analysts.vespa_daily_augs_20121111
) t
-- 159,359,700 Row(s) affected

select top 1000 * from Vespa_Augs_20121105_11 order by subscriber_id,viewing_Starts

select subscriber_id,viewing_starts
from Vespa_Augs_20121105_11
group by subscriber_id,viewing_starts
having count(*)>1
-- 283

-- Create index
create hg index idx1 on Vespa_Augs_20121105_11(cb_row_id)

-- Breakdown by capping type
select capped_flag
        ,count(*)
from  Vespa_Augs_20121105_11
group by capped_flag
order by capped_flag
/*
0       151,947,404
1       2,519,425
2       4,892,871
*/

select count(*) from Vespa_Augs_20121105_11 -- 159,359,700
select count(*) from VEA_sample_5_11Nov_Time_Shifted_Events --  51,657,334
select count(*) from VEA_sample_5_11Nov_Live_Viewing_Events -- 110,614,599

-- Compiling info from vespa_events_all and augmented data
drop table VEA_sample_5_11Nov_Capped_Viewing_Events
select  rank() over ( partition by VEA.subscriber_id
                                    ,VEA.EVENT_START_DATE_TIME_UTC
                                    ,VEA.EVENT_END_DATE_TIME_UTC
                         order by VEA.BROADCAST_START_DATE_TIME_UTC)
        as Programme_Order
        ,VEA.pk_viewing_prog_instance_fact
        ,VEA.subscriber_id
        ,VEA.EVENT_START_DATE_TIME_UTC
        ,VEA.EVENT_END_DATE_TIME_UTC
        ,VEA.Duration
        ,VEA.channel_name
        ,AUG.capped_flag
        ,AUG.capped_event_end_time
        ,datediff(second,VEA.EVENT_START_DATE_TIME_UTC,AUG.capped_event_end_time)as Capped_Duration
into VEA_sample_5_11Nov_Capped_Viewing_Events
from sk_prod.VESPA_EVENTS_ALL VEA
inner join Vespa_Augs_20121105_11 AUG
on VEA.pk_viewing_prog_instance_fact = AUG.cb_row_id
where VEA.panel_id = 12
and VEA.EVENT_START_DATE_TIME_UTC >= '2012-11-05 00:00:00'
and VEA.EVENT_START_DATE_TIME_UTC <= '2012-11-11 23:59:59'
and VEA.type_of_viewing_event <> 'Non viewing event'
and VEA.subscriber_id is not null
-- 159,400,222 row(s) affected

delete from VEA_sample_5_11Nov_Capped_Viewing_Events where programme_order > 1
select count(*) from VEA_sample_5_11Nov_Capped_Viewing_Events
-- 135,350,660

-- add hour/minute fields to speedup grouping data for analysis
alter table VEA_sample_5_11Nov_Capped_Viewing_Events add EVENT_START_HOUR   smallint
alter table VEA_sample_5_11Nov_Capped_Viewing_Events add EVENT_START_MINUTE smallint
alter table VEA_sample_5_11Nov_Capped_Viewing_Events add EVENT_END_HOUR     smallint
alter table VEA_sample_5_11Nov_Capped_Viewing_Events add EVENT_END_MINUTE   smallint

-- update fields
update VEA_sample_5_11Nov_Capped_Viewing_Events
set  EVENT_START_HOUR   = hour(EVENT_START_DATE_TIME_UTC)
    ,EVENT_START_MINUTE = minute(EVENT_START_DATE_TIME_UTC)
    ,EVENT_END_HOUR     = hour(EVENT_END_DATE_TIME_UTC)
    ,EVENT_END_MINUTE   = minute(EVENT_END_DATE_TIME_UTC)

-- add binary flag for capped/uncapped events
alter table VEA_sample_5_11Nov_Capped_Viewing_Events add capped_flag_binary tinyint

-- update flag
update VEA_sample_5_11Nov_Capped_Viewing_Events
set capped_flag_binary = case
                            when capped_flag = 0        then 0
                            when capped_flag in (1,2,3) then 1
                            else NULL
                        end

-- add DOW field to group data
alter table VEA_sample_5_11Nov_Capped_Viewing_Events add EVENT_END_DOW varchar(3)

-- update flag
update VEA_sample_5_11Nov_Capped_Viewing_Events
set EVENT_END_DOW = 
        case
                when datepart(weekday,EVENT_END_DATE_TIME_UTC)=1 then 'Sun'
                when datepart(weekday,EVENT_END_DATE_TIME_UTC)=2 then 'Mon'
                when datepart(weekday,EVENT_END_DATE_TIME_UTC)=3 then 'Tue'
                when datepart(weekday,EVENT_END_DATE_TIME_UTC)=4 then 'Wed'
                when datepart(weekday,EVENT_END_DATE_TIME_UTC)=5 then 'Thu'
                when datepart(weekday,EVENT_END_DATE_TIME_UTC)=6 then 'Fri'
                when datepart(weekday,EVENT_END_DATE_TIME_UTC)=7 then 'Sat'
        end

-- index to speedup
create hg index idx1 on VEA_sample_5_11Nov_Capped_Viewing_Events(capped_flag_binary
                                                                ,channel_name
                                                                ,EVENT_END_DOW
                                                                ,EVENT_END_HOUR
                                                                ,EVENT_END_MINUTE)

-- Compute the distribution of event ending for each channel
drop table VEA_5_11Nov_Event_End_Distribution
select   capped_flag_binary
        ,channel_name
        ,EVENT_END_DOW
        ,EVENT_END_HOUR
        ,EVENT_END_MINUTE
        ,count(*) as Num_Events
        ,sum(Num_Events) OVER (PARTITION BY capped_flag_binary
                                            ,channel_name
                                            ,EVENT_END_DOW) as Total_Num_Events_1Day
        ,sum(Num_Events) OVER (PARTITION BY capped_flag_binary
                                            ,channel_name
                                            ,EVENT_END_DOW
                                            ,EVENT_END_HOUR) as Total_Num_Events_1Hour
        ,Num_Events*1.0/Total_Num_Events_1Day as Percentage_1Day
        ,Num_Events*1.0/Total_Num_Events_1Hour as Percentage_1Hour
into VEA_5_11Nov_Event_End_Distribution
from VEA_sample_5_11Nov_Capped_Viewing_Events
group by capped_flag_binary
        ,channel_name
        ,EVENT_END_DOW
        ,EVENT_END_HOUR
        ,EVENT_END_MINUTE
order by capped_flag_binary
        ,channel_name
        ,EVENT_END_DOW
        ,EVENT_END_HOUR
        ,EVENT_END_MINUTE
-- 5,519,528 row(s) affected

-- Rank channels by num of events
select channel_name
       ,sum(Total_Num_Events_1Day) as Total_Events
from VEA_5_11Nov_Event_End_Distribution
group by channel_name
order by Total_Events desc
-- 543
 
-- Report distribution 
select  capped_flag_binary
        ,channel_name
        ,EVENT_END_DOW
        ,case
            when EVENT_END_HOUR < 10 then '0'+trim(str(EVENT_END_HOUR))
            else trim(str(EVENT_END_HOUR))
        end
        +':'+
        case
            when EVENT_END_MINUTE < 10 then '0'+trim(str(EVENT_END_MINUTE))
            else trim(str(EVENT_END_MINUTE))
        end as EVENT_END
        ,Num_Events
        ,Total_Num_Events_1Day
        ,Percentage_1Day
from VEA_5_11Nov_Event_End_Distribution
where channel_name in (
                        select channel_name
                        from VEA_5_11Nov_Event_End_Distribution
                        group by channel_name
                        having sum(Total_Num_Events_1Day) > 7*24*60*1000000 -- average of 1M events per minute
                        ) 
order by capped_flag_binary
        ,channel_name
        ,EVENT_END_DOW 
        ,EVENT_END
-- 


-- Reshape the table with the distribution of event ending for each channel
drop table VEA_5_11Nov_Event_End_Distribution_Reshaped
select base.channel_name
        ,base.EVENT_END_DOW
        ,base.EVENT_END_HOUR
        ,base.EVENT_END_MINUTE
        ,coalesce(cap.Num_Events,0) as Num_Events_Capped
        ,coalesce(cap.Percentage_1Day,0) as Percentage_1Day_Capped
        ,coalesce(cap.Percentage_1Hour,0) as Percentage_1Hour_Capped
        ,coalesce(uncap.Num_Events,0) as Num_Events_Uncapped
        ,coalesce(uncap.Percentage_1Day,0) as Percentage_1Day_Uncapped
        ,coalesce(uncap.Percentage_1Hour,0) as Percentage_1Hour_Uncapped
into VEA_5_11Nov_Event_End_Distribution_Reshaped
from (
select *
from
(select distinct channel_name from VEA_5_11Nov_Event_End_Distribution) cha
,(select distinct EVENT_END_DOW from VEA_5_11Nov_Event_End_Distribution) dow
,(select distinct EVENT_END_HOUR from VEA_5_11Nov_Event_End_Distribution) hour
,(select distinct EVENT_END_MINUTE from VEA_5_11Nov_Event_End_Distribution) min
) base
left join
(
select channel_name
        ,EVENT_END_DOW
        ,EVENT_END_HOUR
        ,EVENT_END_MINUTE
        ,Num_Events
        ,Percentage_1Day
        ,Percentage_1Hour
from VEA_5_11Nov_Event_End_Distribution
where capped_flag_binary = 0
) uncap
on  uncap.channel_name      = base.channel_name
and uncap.EVENT_END_DOW     = base.EVENT_END_DOW
and uncap.EVENT_END_HOUR    = base.EVENT_END_HOUR
and uncap.EVENT_END_MINUTE  = base.EVENT_END_MINUTE
left join
(
select channel_name
        ,EVENT_END_DOW
        ,EVENT_END_HOUR
        ,EVENT_END_MINUTE
        ,Num_Events
        ,Percentage_1Day
        ,Percentage_1Hour
from VEA_5_11Nov_Event_End_Distribution
where capped_flag_binary = 1
) cap
on  uncap.channel_name      = cap.channel_name
and uncap.EVENT_END_DOW     = cap.EVENT_END_DOW
and uncap.EVENT_END_HOUR    = cap.EVENT_END_HOUR
and uncap.EVENT_END_MINUTE  = cap.EVENT_END_MINUTE
-- 5,473,440 row(s) affected

-- index to speedup
create hg index idx1 on VEA_5_11Nov_Event_End_Distribution_Reshaped(channel_name
                                                                    ,EVENT_END_DOW
                                                                    ,EVENT_END_HOUR
                                                                    ,EVENT_END_MINUTE)

-- Look at the top 100 channels most watched
select top 100 channel_name
from VEA_sample_5_11Nov_Capped_Viewing_Events
group by channel_name
order by sum(Capped_Duration) desc

-- Calculate correlations between probability distributions of
-- capped/uncapped events
drop table VEA_5_11Nov_Event_End_Distribution_Correlations
select channel_name
        ,EVENT_END_DOW
        ,EVENT_END_HOUR
        ,min(Num_Events_Uncapped) as Num_Events_Uncapped_Min
        ,median(Num_Events_Uncapped) as Num_Events_Uncapped_Median
        ,max(Num_Events_Uncapped) as Num_Events_Uncapped_Max
        ,sum(Num_Events_Uncapped) as Num_Events_Uncapped_Total
        ,min(Num_Events_Capped) as Num_Events_Capped_Min
        ,median(Num_Events_Capped) as Num_Events_Capped_Median
        ,max(Num_Events_Capped) as Num_Events_Capped_Max
        ,sum(Num_Events_Capped) as Num_Events_Capped_Total
        ,corr(Num_Events_Uncapped,Num_Events_Capped) as Corr_Num_Events
        ,corr(coalesce(log10(Num_Events_Uncapped),0),coalesce(log10(Num_Events_Capped),0))as Corr_log10_Num_Events
into VEA_5_11Nov_Event_End_Distribution_Correlations
from VEA_5_11Nov_Event_End_Distribution_Reshaped
group by channel_name
        ,EVENT_END_DOW
        ,EVENT_END_HOUR
-- 91,224

-- Look at most correlated distributions 
select *
from VEA_5_11Nov_Event_End_Distribution_Correlations
where Num_Events_Capped_Total >= 60
and Num_Events_Uncapped_Total >= 60
order by Corr_Num_Events desc
-- 15,907

select *
from VEA_5_11Nov_Event_End_Distribution_Correlations
where Num_Events_Capped_Total >= 60
and Num_Events_Uncapped_Total >= 60
order by Corr_log10_Num_Events desc
-- 15,907

select sum(case when Corr_log10_Num_Events > 0.9 then 1 else 0 end) as Num_Slots_r09
        ,sum(case when Corr_log10_Num_Events > 0.8 then 1 else 0 end) as Num_Slots_r08
        ,sum(case when Corr_log10_Num_Events > 0.7 then 1 else 0 end) as Num_Slots_r07
        ,sum(case when Corr_log10_Num_Events > 0.6 then 1 else 0 end) as Num_Slots_r06
        ,sum(case when Corr_log10_Num_Events > 0.5 then 1 else 0 end) as Num_Slots_r05
        ,sum(case when Corr_log10_Num_Events > 0.4 then 1 else 0 end) as Num_Slots_r04
        ,sum(case when Corr_log10_Num_Events > 0.33 then 1 else 0 end) as Num_Slots_r033
from VEA_5_11Nov_Event_End_Distribution_Correlations
where Num_Events_Capped_Total >= 60 
and Num_Events_Uncapped_Total >= 60
-- 5,90,431,1101,2287,4083,5664


-- Look at a highly correlated time slot based on absolute # of events 
select  capped_flag_binary
        ,channel_name
        ,EVENT_END_DOW
        ,case
            when EVENT_END_HOUR < 10 then '0'+trim(str(EVENT_END_HOUR))
            else trim(str(EVENT_END_HOUR))
        end
        +':'+
        case
            when EVENT_END_MINUTE < 10 then '0'+trim(str(EVENT_END_MINUTE))
            else trim(str(EVENT_END_MINUTE))
        end as EVENT_END
        ,Num_Events
        ,Total_Num_Events_1Day
        ,Percentage_1Day
from VEA_5_11Nov_Event_End_Distribution
where channel_name = 'Sky Sports 1 HD'
and EVENT_END_DOW = 'Tue'
and EVENT_END_HOUR = 2

-- Look at a highly correlated time slot based on log10 of # events 
select  capped_flag_binary
        ,channel_name
        ,EVENT_END_DOW
        ,case
            when EVENT_END_HOUR < 10 then '0'+trim(str(EVENT_END_HOUR))
            else trim(str(EVENT_END_HOUR))
        end
        +':'+
        case
            when EVENT_END_MINUTE < 10 then '0'+trim(str(EVENT_END_MINUTE))
            else trim(str(EVENT_END_MINUTE))
        end as EVENT_END
        ,Num_Events
        ,Total_Num_Events_1Day
        ,Percentage_1Day
from VEA_5_11Nov_Event_End_Distribution
where  channel_name = 'Sky Sports 1 HD'
and EVENT_END_DOW = 'Sun'
and EVENT_END_HOUR = 17


-- Look at channels with high num of slots where correlation between
-- capped/uncapped events is high
select channel_name
        ,sum(case when Corr_Num_Events > 0.7 then 1 else 0 end) as Num_Slots_High_Corr
        ,count(*) as Num_Slots
        ,Num_Slots_High_Corr*1.0/Num_Slots as Percentage_Slots_High_Corr
from VEA_5_11Nov_Event_End_Distribution_Correlations
where Num_Events_Capped_Total >= 60 
and Num_Events_Uncapped_Total >= 60
group by channel_name
order by Num_Slots_High_Corr desc

-- Look at DOW
select EVENT_END_DOW
        ,count(*) as Num_Slots_High_Corr
from VEA_5_11Nov_Event_End_Distribution_Correlations
where Num_Events_Capped_Total >= 60 
and Num_Events_Uncapped_Total >= 60
and Corr_Num_Events > 0.7
group by EVENT_END_DOW
order by Num_Slots_High_Corr desc
/*
'Sun',279
'Tue',272
'Wed',266
'Mon',252
'Thu',249
'Fri',238
'Sat',217
*/

-- Look at event end hour
select EVENT_END_HOUR
        ,count(*) as Num_Slots_High_Corr
from VEA_5_11Nov_Event_End_Distribution_Correlations
where Num_Events_Capped_Total >= 60 
and Num_Events_Uncapped_Total >= 60
and Corr_Num_Events > 0.7
group by EVENT_END_HOUR
order by Num_Slots_High_Corr desc
/*
21,242
19,236
20,228
22,192
...
*/

-- Report where high correlation capped/uncapped events fall in DOW/hour
select base.EVENT_END_DOW
        ,base.EVENT_END_HOUR
        ,coalesce(t.Num_Slots_High_Corr,0) as Num_Slots_High_Corr
        ,coalesce(t.Percentage_Slots_High_Corr,0) as Percentage_Slots_High_Corr
from
(
select *
from
(select distinct EVENT_END_DOW from VEA_5_11Nov_Event_End_Distribution_Correlations) dow
,(select distinct EVENT_END_HOUR from VEA_5_11Nov_Event_End_Distribution_Correlations) hour
) base
left join
(
select EVENT_END_DOW
        ,EVENT_END_HOUR
        ,sum(case when Corr_Num_Events > 0.7 then 1 else 0 end) as Num_Slots_High_Corr
        ,count(*) as Num_Slots
        ,Num_Slots_High_Corr*1.0/Num_Slots as Percentage_Slots_High_Corr
from VEA_5_11Nov_Event_End_Distribution_Correlations
where Num_Events_Capped_Total >= 60 
and Num_Events_Uncapped_Total >= 60
group by EVENT_END_DOW
        ,EVENT_END_HOUR
) t
on base.EVENT_END_DOW = t.EVENT_END_DOW
and base.EVENT_END_HOUR = t.EVENT_END_HOUR

-----------------------------------------------------------------------------------
--
-- Apply probabilistic capping to capped events and analyse the impact
--
-- A capped event will be said to be legitimate with probability
--
-- p_leg = MAX(r_capped/uncapped,0) * p_uncapped_ends,
--
-- where r_capped/uncapped id the correlation between capped and uncapped hourly 
-- event ending distributiond, and p_uncapped_ends is the probability of a 
-- legitimate event to end in that particular minute within that hour.
-----------------------------------------------------------------------------------

-- Inspect tables
select top 100 * from VEA_5_11Nov_Event_End_Distribution_Correlations
select top 100 * from VEA_sample_5_11Nov_Capped_Viewing_Events
select top 100 * from VEA_5_11Nov_Event_End_Distribution_Reshaped

-- Add weight variable 
alter table VEA_5_11Nov_Event_End_Distribution_Reshaped add probabilistic_capping_weight real

-- Update weight based on correlation
-- update VEA_5_11Nov_Event_End_Distribution_Reshaped set probabilistic_capping_weight = NULL
update VEA_5_11Nov_Event_End_Distribution_Reshaped
set probabilistic_capping_weight = case 
                                        when corr.corr_log10_num_events > 0
                                        then corr.corr_log10_num_events
                                        else 0
                                    end
from VEA_5_11Nov_Event_End_Distribution_Reshaped dist
inner join VEA_5_11Nov_Event_End_Distribution_Correlations corr
on dist.channel_name = corr.channel_name
and dist.EVENT_END_DOW = corr.EVENT_END_DOW
and dist.EVENT_END_HOUR = corr.EVENT_END_HOUR
-- 5,473,440 row(s) updated

alter table VEA_sample_5_11Nov_Capped_Viewing_Events add roulette real
alter table VEA_sample_5_11Nov_Capped_Viewing_Events add capped_flag_binary_new tinyint
alter table VEA_sample_5_11Nov_Capped_Viewing_Events add capped_flag_binary_new_2 tinyint

update VEA_sample_5_11Nov_Capped_Viewing_Events
set roulette =  RAND(NUMBER(*)*(DATEPART(MS,NOW())+1))
-- 135,350,660 row(s) updated

/*
update VEA_sample_5_11Nov_Capped_Viewing_Events 
set capped_flag_binary_new = NULL, capped_flag_binary_new_2 = NULL
*/
update VEA_sample_5_11Nov_Capped_Viewing_Events
set capped_flag_binary_new = case 
                                when events.capped_flag_binary = 1 
                                    and events.duration < 60*60*4 
                                    and roulette < dist.percentage_1hour_uncapped*dist.probabilistic_capping_weight
                                then 0
                                else events.capped_flag_binary
                             end
,capped_flag_binary_new_2 = case 
                                when events.capped_flag_binary = 1
                                    and events.duration < 60*60*4  
                                    and roulette < dist.percentage_1hour_uncapped
                                then 0
                                else events.capped_flag_binary
                             end
from VEA_sample_5_11Nov_Capped_Viewing_Events events
inner join VEA_5_11Nov_Event_End_Distribution_reshaped dist
on dist.channel_name = events.channel_name
and dist.EVENT_END_DOW = events.EVENT_END_DOW
and dist.EVENT_END_HOUR = events.EVENT_END_HOUR
and dist.EVENT_END_MINUTE = events.EVENT_END_MINUTE
COMMIT

-- Look at the impact of uncapping events
select count(*)
        ,sum(duration)/3600.0
from VEA_sample_5_11Nov_Capped_Viewing_Events events
where capped_flag_binary = 1
-- 4,907,349 / 25,749,182.3

select count(*)
        ,sum(duration)/3600.0
from VEA_sample_5_11Nov_Capped_Viewing_Events events
where capped_flag_binary = 0
-- 130,443,311 / 19,770,307.7

select count(*)
from VEA_sample_5_11Nov_Capped_Viewing_Events events
where capped_flag_binary_new <> capped_flag_binary
-- 42,611
-- 30,221

select count(*)
from VEA_sample_5_11Nov_Capped_Viewing_Events events
where capped_flag_binary_new_2 <> capped_flag_binary
-- 114,628
-- 74,006

-- Breakdown by duration (in slots of 10 mins)
select (duration/600)*10 as Duration
        ,count(*) as Number_Events
from VEA_sample_5_11Nov_Capped_Viewing_Events events
where capped_flag_binary_new_2 <> capped_flag_binary
group by duration
order by duration

-- Distribution of difference between capped and actual end times 
-- for events that have just been uncapped
select (datediff(second,capped_event_end_time,EVENT_END_DATE_TIME_UTC)/(60*10))*10 as Minutes_Uncapped
        ,count(*) as Number_Events
from VEA_sample_5_11Nov_Capped_Viewing_Events
where capped_flag_binary_new_2 <> capped_flag_binary
group by (datediff(second,capped_event_end_time,EVENT_END_DATE_TIME_UTC)/(60*10))*10
order by (datediff(second,capped_event_end_time,EVENT_END_DATE_TIME_UTC)/(60*10))*10

select (duration/600)*10 as Duration
        ,(datediff(second,capped_event_end_time,EVENT_END_DATE_TIME_UTC)/(60*10))*10 as Minutes_Uncapped
        ,count(*) as Number_Events
from VEA_sample_5_11Nov_Capped_Viewing_Events
where capped_flag_binary_new_2 <> capped_flag_binary
group by duration,minutes_uncapped
order by duration,minutes_uncapped

select sum(datediff(second,capped_event_end_time,EVENT_END_DATE_TIME_UTC)/3600.0) as Viewing_Hours_Uncapped
from VEA_sample_5_11Nov_Capped_Viewing_Events
where capped_flag_binary_new_2 <> capped_flag_binary
--  65,244
-- 160,593

----------------------------------------------------------------------------
-- Look at which segmentation variables have influence on high correlation
----------------------------------------------------------------------------

-- Look at most correlated time slots
select top 1 programme_name
from sk_prod.VESPA_EVENTS_ALL
where panel_id = 12
and type_of_viewing_event <> 'Non viewing event'
and subscriber_id is not null
and EVENT_END_DATE_TIME_UTC > '2012-11-06 14:00:00'
and EVENT_END_DATE_TIME_UTC < '2012-11-06 15:00:00'
and channel_name = 'Sky Sports 1 HD'
-- Live Tennis - World Tour Tennis
select top 1 programme_name
from sk_prod.VESPA_EVENTS_ALL
where panel_id = 12
and type_of_viewing_event <> 'Non viewing event'
and subscriber_id is not null
and EVENT_END_DATE_TIME_UTC > '2012-11-11 17:00:00'
and EVENT_END_DATE_TIME_UTC < '2012-11-11 18:00:00'
and channel_name = 'Sky Sports 1 HD'
-- Live Ford Super Sunday


-- get programme info data
select case
                when datepart(weekday,EVENT_END_DATE_TIME_UTC)=1 then 'Sun'
                when datepart(weekday,EVENT_END_DATE_TIME_UTC)=2 then 'Mon'
                when datepart(weekday,EVENT_END_DATE_TIME_UTC)=3 then 'Tue'
                when datepart(weekday,EVENT_END_DATE_TIME_UTC)=4 then 'Wed'
                when datepart(weekday,EVENT_END_DATE_TIME_UTC)=5 then 'Thu'
                when datepart(weekday,EVENT_END_DATE_TIME_UTC)=6 then 'Fri'
                when datepart(weekday,EVENT_END_DATE_TIME_UTC)=7 then 'Sat'
        end as EVENT_END_DOW
        ,hour(EVENT_END_DATE_TIME_UTC) as EVENT_END_HOUR
        ,channel_name 
        ,programme_name
        ,genre_description
        ,sub_genre_description
into VEA_sample_5_11Nov_ProgrammeNames_temp
from sk_prod.VESPA_EVENTS_ALL
where panel_id = 12
and EVENT_START_DATE_TIME_UTC >= '2012-11-05 00:00:00'
and EVENT_START_DATE_TIME_UTC <= '2012-11-11 23:59:59'
and type_of_viewing_event <> 'Non viewing event'
and subscriber_id is not null
and hour(EVENT_END_DATE_TIME_UTC) = hour(BROADCAST_END_DATE_TIME_UTC)
-- 220,941,175 row(s) affected

select case
                when datepart(weekday,BROADCAST_END_DATE_TIME_UTC)=1 then 'Sun'
                when datepart(weekday,BROADCAST_END_DATE_TIME_UTC)=2 then 'Mon'
                when datepart(weekday,BROADCAST_END_DATE_TIME_UTC)=3 then 'Tue'
                when datepart(weekday,BROADCAST_END_DATE_TIME_UTC)=4 then 'Wed'
                when datepart(weekday,BROADCAST_END_DATE_TIME_UTC)=5 then 'Thu'
                when datepart(weekday,BROADCAST_END_DATE_TIME_UTC)=6 then 'Fri'
                when datepart(weekday,BROADCAST_END_DATE_TIME_UTC)=7 then 'Sat'
        end as BROADCAST_END_DOW
        ,hour(BROADCAST_START_DATE_TIME_UTC) as BROADCAST_START_HOUR
        ,hour(dateadd(ss,-1,BROADCAST_END_DATE_TIME_UTC)) as BROADCAST_END_HOUR -- force programmes ending at x-1:59:59 instead of x:00:00
        ,channel_name 
        ,programme_name
        ,genre_description
        ,sub_genre_description
into VEA_sample_5_11Nov_ProgrammeNames
from sk_prod.vespa_programme_schedule
where BROADCAST_START_DATE_TIME_UTC >= '2012-11-05 00:00:00'
and BROADCAST_START_DATE_TIME_UTC <= '2012-11-11 23:59:59'
-- 291,894 row(s) affected

select *
from VEA_sample_5_11Nov_ProgrammeNames
where upper(programme_name) like '% LIVE %'
or upper(programme_name) like 'LIVE %'
or upper(programme_name) like '% LIVE'
-- 4,293

-- Check list of programmes where capped/uncapped events are highly correlated
select distinct CORR.channel_name
        ,CORR.EVENT_END_DOW
        ,CORR.EVENT_END_HOUR
        ,CORR.Corr_log10_Num_Events
        ,PROG.programme_name    
        ,PROG.genre_description
from VEA_5_11Nov_Event_End_Distribution_Correlations CORR
left join VEA_sample_5_11Nov_ProgrammeNames PROG
on CORR.event_end_dow = PROG.broadcast_end_dow
and CORR.event_end_hour >= PROG.broadcast_start_hour
and CORR.event_end_hour <= PROG.broadcast_end_hour
and CORR.channel_name = PROG.channel_name
where CORR.Num_Events_Capped_Total >= 60
and CORR.Num_Events_Uncapped_Total >= 60
and CORR.Corr_log10_Num_Events  > 0.7
order by CORR.Corr_log10_Num_Events desc
-- 763

-- List DOW, hour, and genre that have most correlated slots
select EVENT_END_DOW
        ,EVENT_END_HOUR
        ,Genre_Description
        ,count(*) as Number_Correlated_Slots
from (
select distinct CORR.channel_name
        ,CORR.EVENT_END_DOW
        ,CORR.EVENT_END_HOUR
        ,CORR.Corr_log10_Num_Events
        ,PROG.programme_name    
        ,PROG.genre_description
from VEA_5_11Nov_Event_End_Distribution_Correlations CORR
left join VEA_sample_5_11Nov_ProgrammeNames PROG
on CORR.event_end_dow = PROG.broadcast_end_dow
and CORR.event_end_hour >= PROG.broadcast_start_hour
and CORR.event_end_hour <= PROG.broadcast_end_hour
and CORR.channel_name = PROG.channel_name
where CORR.Num_Events_Capped_Total >= 60
and CORR.Num_Events_Uncapped_Total >= 60
and CORR.Corr_log10_Num_Events  > 0.7
) t
group by EVENT_END_DOW
        ,EVENT_END_HOUR
        ,Genre_Description
order by 4 desc


select  CAP.channel_name
        ,CAP.EVENT_END_DOW
        ,CAP.EVENT_END_HOUR
        ,PROG.programme_name    
        ,PROG.genre_description
from VEA_sample_5_11Nov_Capped_Viewing_Events CAP
left join VEA_sample_5_11Nov_ProgrammeNames PROG
on CAP.event_end_dow = PROG.broadcast_end_dow
and CAP.event_end_hour >= PROG.broadcast_start_hour
and CAP.event_end_hour <= PROG.broadcast_end_hour
and CAP.channel_name = PROG.channel_name
where CAP.capped_flag_binary_new_2 <> CAP.capped_flag_binary
order by 

