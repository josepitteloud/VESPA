/*

 Compile viewing data from 25 Feb-03 Mar 2013 with variables required for C&S Checks from VESPA Events All and From the Daily Augmented Tables
 Author : Patrick Igonor
 Date : 15-03-2013
*/

------------------------------------------------------------------------------------------------------------

--First step is to Combine augmented tables for a week (25 Feb to 03 Mar 2013)


select *
into Vespa_Augs_2013_25Feb_03_Mar
from (
select * from vespa_analysts.vespa_daily_augs_20130225
union all
select * from vespa_analysts.vespa_daily_augs_20130226
union all
select * from vespa_analysts.vespa_daily_augs_20130227
union all
select * from vespa_analysts.vespa_daily_augs_20130228
union all
select * from vespa_analysts.vespa_daily_augs_20130301
union all
select * from vespa_analysts.vespa_daily_augs_20130302
union all
select * from vespa_analysts.vespa_daily_augs_20130303
) P

--119,576,397 Row(s) affected

select top 1000* from Vespa_Augs_2013_25Feb_03_Mar
-- Get viewing events from Production

select rank() over ( partition by subscriber_id
                                 ,EVENT_START_DATE_TIME_UTC
                                 ,EVENT_END_DATE_TIME_UTC
                                 ,type_of_viewing_event
                                 ,Duration
                         order by BROADCAST_START_DATE_TIME_UTC)
        as Program_Order
       ,pk_viewing_prog_instance_fact
       ,subscriber_id
       ,EVENT_START_DATE_TIME_UTC
       ,EVENT_END_DATE_TIME_UTC
       ,BROADCAST_START_DATE_TIME_UTC
       ,capping_end_date_time_utc
       ,Duration
       ,channel_name
       ,account_number
       ,case
                        when type_of_viewing_event = 'Sky+ time-shifted viewing event'
            then case
                                        when dateformat(EVENT_START_DATE_TIME_UTC,'YYYY-MM-DD') = dateformat(BROADCAST_START_DATE_TIME_UTC,'YYYY-MM-DD')
                                        then case
                                                        when cast(datediff(second,BROADCAST_START_DATE_TIME_UTC,EVENT_END_DATE_TIME_UTC)/(60.0*60.0) as int) = 0
                                                        then 1
                                                        else 2
                                                        end
                                        else 3
                                        end
                        else 0
        end as Viewing_Type_Detailed
        ,case
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=1 then 'Sun'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=2 then 'Mon'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=3 then 'Tue'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=4 then 'Wed'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=5 then 'Thu'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=6 then 'Fri'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=7 then 'Sat'
        end as EVENT_START_DOW
         ,hour(EVENT_START_DATE_TIME_UTC) as EVENT_START_HOUR
         ,genre_description
         ,service_key
into VEA_25Feb_03Mar_Viewing_Events_New
from sk_prod.VESPA_DP_PROG_VIEWED_CURRENT
where panel_id = 12
and EVENT_START_DATE_TIME_UTC   >= '2013-02-25 00:00:00'
and EVENT_START_DATE_TIME_UTC   <= '2013-03-03 23:59:59'
and type_of_viewing_event <> 'Non viewing event'
and subscriber_id is not null
and account_number is not null
and Duration > 6
--159,556,824 Row(s) affected
delete from VEA_25Feb_03Mar_Viewing_Events_New where Program_Order > 1
--57,774,680 Row(s) affected

-- Add fields for box subscription and pack group
alter table VEA_25Feb_03Mar_Viewing_Events_New add box_subscription varchar (2)   default 'U'
alter table VEA_25Feb_03Mar_Viewing_Events_New add pack_grp varchar (255) default 'Unknown'

-- Update box_subscription
update VEA_25Feb_03Mar_Viewing_Events_New
set box_subscription =
        case
            when csh.subscription_sub_type = 'DTV Primary Viewing' then 'P'
            when csh.subscription_sub_type = 'DTV Extra Subscription' then 'S'
        end
        from VEA_25Feb_03Mar_Viewing_Events_New as VE
    inner join sk_prod.cust_subs_hist as csh
on VE.account_number = csh.account_number
where csh.SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing','DTV Extra Subscription')
and csh.status_code in ('AC','AB','PC')
and csh.effective_from_dt<='2013-02-25 00:00:00'
and csh.effective_to_dt>'2013-03-03 23:59:59'


-- Update Pack group
update VEA_25Feb_03Mar_Viewing_Events_New
set VE.pack_grp = CM.channel_pack
from VEA_25Feb_03Mar_Viewing_Events_New as VE
inner join vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES as CM
on VE.service_key = CM.service_key


--Building up the indexes to speed up the whole processes -----


create hg index idx12 on VEA_25Feb_03Mar_Viewing_Events_New(pk_viewing_prog_instance_fact);
create hg index idx13 on Vespa_Augs_2013_25Feb_03_Mar(cb_row_id);

-- Compiling info from VEA_25Feb_03Mar_Viewing_Events_New and Vespa_Augs_2013_25Feb_03_Mar
select
         VEA.subscriber_id
        ,VEA.pk_viewing_prog_instance_fact
        ,VEA.EVENT_START_DATE_TIME_UTC
        ,VEA.EVENT_END_DATE_TIME_UTC
        ,VEA.Duration
        ,VEA.channel_name
        ,VEA.Viewing_Type_Detailed
        ,VEA.EVENT_START_DOW
        ,VEA.EVENT_START_HOUR
        ,VEA.genre_description
        ,VEA.box_subscription
        ,VEA.pack_grp
        ,VEA.capping_end_date_time_utc
        ,AUG.Viewing_Starts
        ,AUG.Viewing_Stops
        ,AUG.Viewing_Duration
        ,AUG.Capped_Flag
        ,AUG.Capped_Event_End_Time
into    VEA_25Feb_03Mar_Viewing_Events_New_AUG
from VEA_25Feb_03Mar_Viewing_Events_New VEA
left join Vespa_Augs_2013_25Feb_03_Mar AUG
on VEA.pk_viewing_prog_instance_fact = AUG.cb_row_id

---101,782,144 Row(s) affected


--Checking the number of Events by DOW, Hour and event type.. (Old table)

--Total number of events capped
select count(*)as VTD
from   VEA_25Feb_03Mar_Viewing_Events_New_AUG
where  Capped_Flag=1

--Splitting the total number of events capped by Viewing_Type (Live, Vosadal <1hr, Vosdal 1-24hr and Playback
select Viewing_Type_Detailed, count(*)as VTD
from   VEA_25Feb_03Mar_Viewing_Events_New_AUG
where  Capped_Flag=1
group by Viewing_Type_Detailed
order by Viewing_Type_Detailed

--Splitting the total number of events capped by Events Start DOW
select Event_Start_DOW, count(*)as VTD
from   VEA_25Feb_03Mar_Viewing_Events_New_AUG
where  Capped_Flag=1
group by Event_Start_DOW
order by Event_Start_DOW

--Splitting the total number of events capped by Event Start Hour
select Event_Start_hour, count(*)as VTD
from   VEA_25Feb_03Mar_Viewing_Events_New_AUG
where  Capped_Flag=1
group by Event_Start_hour
order by Event_Start_hour

--Splitting the total number of events capped by Pack Group
select pack_grp, count(*)as VTD
from   VEA_25Feb_03Mar_Viewing_Events_New_AUG
where  Capped_Flag=1
group by pack_grp
order by pack_grp

--Splitting the total number of events capped by Genre
select genre_description, count(*)as VTD
from   VEA_25Feb_03Mar_Viewing_Events_New_AUG
where  Capped_Flag=1
group by genre_description
order by genre_description

--Splitting the total number of events capped by Box Subscritpion
select box_subscription, count(*)as VTD
from   VEA_25Feb_03Mar_Viewing_Events_New_AUG
where  Capped_Flag=1
group by box_subscription
order by box_subscription

------New Table

--Total number of events capped
select count(*)as VTD
from   VEA_25Feb_03Mar_Viewing_Events_New_AUG
where  capping_end_date_time_utc <> EVENT_END_DATE_TIME_UTC

--Splitting the total number of events capped by Viewing_Type (Live, Vosadal <1hr, Vosdal 1-24hr and Playback
select Viewing_Type_Detailed, count(*)as VTD
from   VEA_25Feb_03Mar_Viewing_Events_New_AUG
where  capping_end_date_time_utc <> EVENT_END_DATE_TIME_UTC
group by Viewing_Type_Detailed
order by Viewing_Type_Detailed

--Splitting the total number of events capped by Events Start DOW
select Event_Start_DOW, count(*)as VTD
from   VEA_25Feb_03Mar_Viewing_Events_New_AUG
where  capping_end_date_time_utc <> EVENT_END_DATE_TIME_UTC
group by Event_Start_DOW
order by Event_Start_DOW

--Splitting the total number of events capped by Event Start Hour
select Event_Start_hour, count(*)as VTD
from   VEA_25Feb_03Mar_Viewing_Events_New_AUG
where  capping_end_date_time_utc <> EVENT_END_DATE_TIME_UTC
group by Event_Start_hour
order by Event_Start_hour

--Splitting the total number of events capped by Pack Group
select pack_grp, count(*)as VTD
from   VEA_25Feb_03Mar_Viewing_Events_New_AUG
where  capping_end_date_time_utc <> EVENT_END_DATE_TIME_UTC
group by pack_grp
order by pack_grp

--Splitting the total number of events capped by Genre
select genre_description, count(*)as VTD
from   VEA_25Feb_03Mar_Viewing_Events_New_AUG
where  capping_end_date_time_utc <> EVENT_END_DATE_TIME_UTC
group by genre_description
order by genre_description

--Splitting the total number of events capped by Box Subscritpion
select box_subscription, count(*)as VTD
from   VEA_25Feb_03Mar_Viewing_Events_New_AUG
where  capping_end_date_time_utc <> EVENT_END_DATE_TIME_UTC
group by box_subscription
order by box_subscription


--Total Viewing Time After Capping for the old structure
select sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capped_Event_End_Time,EVENT_END_DATE_TIME_UTC)))*1.0/(60.0*60.0) as Sum_Duration_hour_Old
from VEA_25Feb_03Mar_Viewing_Events_New_AUG


--Total Viewing Time After Capping for the old structure based on Viewing Type
select  Viewing_Type_Detailed
       ,sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capped_Event_End_Time,EVENT_END_DATE_TIME_UTC)))*1.0/(60.0*60.0) as Sum_Duration_hour_Old
from    VEA_25Feb_03Mar_Viewing_Events_New_AUG
group by Viewing_Type_Detailed
order by Viewing_Type_Detailed


--Total Viewing Time After Capping for the old structure based on DOW
select  Event_Start_DOW
       ,sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capped_Event_End_Time,EVENT_END_DATE_TIME_UTC)))*1.0/3600 as Sum_Duration_hour_Old
from    VEA_25Feb_03Mar_Viewing_Events_New_AUG
group by Event_Start_DOW
order by Event_Start_DOW

--Total Viewing Time After Capping for the old structure based on Start Hour
select  Event_Start_hour
       ,sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capped_Event_End_Time,EVENT_END_DATE_TIME_UTC)))*1.0/3600 as Sum_Duration_hour_Old
from    VEA_25Feb_03Mar_Viewing_Events_New_AUG
group by Event_Start_hour
order by Event_Start_hour

--Total Viewing Time After Capping for the old structure based on Pack Group
select  pack_grp
       ,sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capped_Event_End_Time,EVENT_END_DATE_TIME_UTC)))*1.0/3600 as Sum_Duration_hour_Old
from    VEA_25Feb_03Mar_Viewing_Events_New_AUG
group by pack_grp
order by pack_grp

--Total Viewing Time After Capping for the old structure based on Genre Description
select  genre_description
       ,sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capped_Event_End_Time,EVENT_END_DATE_TIME_UTC)))*1.0/3600 as Sum_Duration_hour_Old
from    VEA_25Feb_03Mar_Viewing_Events_New_AUG
group by genre_description
order by genre_description

--Total Viewing Time After Capping for the old structure based on Box Subscription
select  box_subscription
       ,sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capped_Event_End_Time,EVENT_END_DATE_TIME_UTC)))*1.0/3600 as Sum_Duration_hour_Old
from    VEA_25Feb_03Mar_Viewing_Events_New_AUG
group by box_subscription
order by box_subscription


--Looking at the viewing time after capping based on the New Viewing Events


--Total Viewing Time before Capping for the new structure
select sum(datediff(second,EVENT_START_DATE_TIME_UTC,EVENT_END_DATE_TIME_UTC))*1.0/(60.0*60.0) as Sum_Duration_hour_New
from VEA_25Feb_03Mar_Viewing_Events_New_AUG

--Total Viewing Time After Capping for the new structure
select sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capping_end_date_time_utc,EVENT_END_DATE_TIME_UTC)))*1.0/(60.0*60.0) as Sum_Duration_hour_New
from VEA_25Feb_03Mar_Viewing_Events_New_AUG


--Total Viewing Time After Capping for the new structure based on Viewing Type
select  Viewing_Type_Detailed
       ,sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capping_end_date_time_utc,EVENT_END_DATE_TIME_UTC)))*1.0/(60.0*60.0) as Sum_Duration_hour_New
from    VEA_25Feb_03Mar_Viewing_Events_New_AUG
group by Viewing_Type_Detailed
order by Viewing_Type_Detailed


--Total Viewing Time After Capping for the new structure based on DOW
select  Event_Start_DOW
       ,sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capping_end_date_time_utc,EVENT_END_DATE_TIME_UTC)))*1.0/3600 as Sum_Duration_hour_New
from    VEA_25Feb_03Mar_Viewing_Events_New_AUG
group by Event_Start_DOW
order by Event_Start_DOW

--Total Viewing Time After Capping for the new structure based on Start Hour
select  Event_Start_hour
       ,sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capping_end_date_time_utc,EVENT_END_DATE_TIME_UTC)))*1.0/3600 as Sum_Duration_hour_New
from    VEA_25Feb_03Mar_Viewing_Events_New_AUG
group by Event_Start_hour
order by Event_Start_hour

--Total Viewing Time After Capping for the new structure based on Pack Group
select  pack_grp
       ,sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capping_end_date_time_utc,EVENT_END_DATE_TIME_UTC)))*1.0/3600 as Sum_Duration_hour_New
from    VEA_25Feb_03Mar_Viewing_Events_New_AUG
group by pack_grp
order by pack_grp

--Total Viewing Time After Capping for the new structure based on Genre Description
select  genre_description
       ,sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capping_end_date_time_utc,EVENT_END_DATE_TIME_UTC)))*1.0/3600 as Sum_Duration_hour_New
from    VEA_25Feb_03Mar_Viewing_Events_New_AUG
group by genre_description
order by genre_description

--Total Viewing Time After Capping for the new structure based on Box Subscription
select  box_subscription
       ,sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capping_end_date_time_utc,EVENT_END_DATE_TIME_UTC)))*1.0/3600 as Sum_Duration_hour_New
from    VEA_25Feb_03Mar_Viewing_Events_New_AUG
group by box_subscription
order by box_subscription

------------------------------------------------------------------------

 grant all on VEA_25Feb_03Mar_Viewing_Events_New_AUG to bednaszs;
 grant all on VEA_25Feb_03Mar_Viewing_Events_New to bednaszs;
 grant all on Vespa_Augs_2013_25Feb_03_Mar to bednaszs;

 grant all on VEA_25Feb_03Mar_Viewing_Events_New_AUG to limac;
 grant all on VEA_25Feb_03Mar_Viewing_Events_New to limac;
 grant all on Vespa_Augs_2013_25Feb_03_Mar to limac;


 ------------------------------------------------------------------------------

select count(*) as Number_Events
 ,Capped_Flag
 ,case
                        when Capping_end_date_time_utc is null then 0
                        when Capping_end_date_time_utc < EVENT_END_DATE_TIME_UTC then 1
                        when Capping_end_date_time_utc = EVENT_END_DATE_TIME_UTC then 2
                        end as Capping_Flag
from   VEA_25Feb_03Mar_Viewing_Events_New_AUG
group by Capped_Flag,Capping_Flag
order by Capped_Flag,Capping_Flag


----------------------------------------------------------------------------------------


select dk_capping_end_time_dim, count(*) as count
from sk_prod.VESPA_DP_PROG_VIEWED_CURRENT
where panel_id = 12
and EVENT_START_DATE_TIME_UTC   >= '2013-02-25 00:00:00'
and EVENT_START_DATE_TIME_UTC   <= '2013-03-03 23:59:59'
and type_of_viewing_event <> 'Non viewing event'
and subscriber_id is not null
and account_number is not null
and Duration > 6
group by dk_capping_end_time_dim
order by dk_capping_end_time_dim
-----------------------------------------------------------------------------------------
select dk_capping_end_time_dim, count(*) as count
from sk_prod.VESPA_DP_PROG_VIEWED_CURRENT
where panel_id = 12
and EVENT_START_DATE_TIME_UTC   >= '2013-01-28 00:00:00'
and EVENT_START_DATE_TIME_UTC   <= '2013-02-03 23:59:59'
and type_of_viewing_event <> 'Non viewing event'
and subscriber_id is not null
and account_number is not null
and Duration > 6
group by dk_capping_end_time_dim
order by dk_capping_end_time_dim






































