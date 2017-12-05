/*

 Compile viewing data from 21-27 Jan 2013 with variables required for C&S Checks from VESPA Events All and From the Daily Augmented Tables
 Author : Patrick Igonor

*/

------------------------------------------------------------------------------------------------------------

--First step is to Combine augmented tables for week 21-27 Jan 2013


select *
into Vespa_Augs_201301_21_27
from (
select * from vespa_analysts.vespa_daily_augs_20130121
union all
select * from vespa_analysts.vespa_daily_augs_20130122
union all
select * from vespa_analysts.vespa_daily_augs_20130123
union all
select * from vespa_analysts.vespa_daily_augs_20130124
union all
select * from vespa_analysts.vespa_daily_augs_20130125
union all
select * from vespa_analysts.vespa_daily_augs_20130126
union all
select * from vespa_analysts.vespa_daily_augs_20130127
) P

--103,748,743 Row(s) affected


-- Create Indexes
create hg index idx1 on Vespa_Augs_201301_21_27(cb_row_id);


-- Get viewing events from vespa events all

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
into VEA_21_27_Jan_Viewing_Events
from sk_prod.vespa_events_all
where panel_id = 12
and EVENT_START_DATE_TIME_UTC   >= '2013-01-21 00:00:00'
and EVENT_START_DATE_TIME_UTC   <= '2013-01-27 23:59:59'
and type_of_viewing_event <> 'Non viewing event'
and subscriber_id is not null
and account_number is not null
and Duration > 6
--133,968,775 Row(s) affected

-- Remove events that have second and later programmes
delete from VEA_21_27_Jan_Viewing_Events where Program_Order > 1
--45,439,093 Row(s) affected

-- Add fields for box subscription and pack group
alter table VEA_21_27_Jan_Viewing_Events add box_subscription varchar (2)   default 'U'
alter table VEA_21_27_Jan_Viewing_Events add pack_grp         varchar (255) default 'Unknown'

-- Update box_subscription
update VEA_21_27_Jan_Viewing_Events
set box_subscription =
        case
            when csh.subscription_sub_type = 'DTV Primary Viewing' then 'P'
            when csh.subscription_sub_type = 'DTV Extra Subscription' then 'S'
        end
        from VEA_21_27_Jan_Viewing_Events as VE
    inner join sk_prod.cust_subs_hist as csh
on VE.account_number = csh.account_number
where csh.SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing','DTV Extra Subscription')
and csh.status_code in ('AC','AB','PC')
and csh.effective_from_dt<='2013-01-21 00:00:00'
and csh.effective_to_dt>'2013-01-27 23:59:59'
--88,100,987 Row(s) affected


-- Update Pack group
update VEA_21_27_Jan_Viewing_Events
set VE.pack_grp = CM.channel_pack
from VEA_21_27_Jan_Viewing_Events as VE
inner join vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES as CM
on VE.service_key = CM.service_key
--88,517,362 Row(s) affected


--Building up the indexes to speed up the whole processes -----


create hg index idx7_7_N on VEA_21_27_Jan_Viewing_Events(pk_viewing_prog_instance_fact);


-- Compiling info from vespa_events_all and augmented data
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
        ,AUG.Viewing_Starts
        ,AUG.Viewing_Stops
        ,AUG.Viewing_Duration
        ,AUG.Capped_Flag
        ,AUG.Capped_Event_End_Time
into     VEA_21_27_Jan_Viewing_Events_Combined
from VEA_21_27_Jan_Viewing_Events VEA
left join Vespa_Augs_201301_21_27 AUG
on VEA.pk_viewing_prog_instance_fact = AUG.cb_row_id

---88,529,682 Row(s) affected


----------------------------------------

-- Get viewing events from VESPA_DP_PROG_VIEWED_CURRENT

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
        ,VEA.Viewing_Starts
        ,VEA.Viewing_Stops
        ,VEA.Viewing_Duration
        ,VEA.Capped_Flag
        ,VEA.Capped_Event_End_Time
        ,VEN.capped_full_flag
        ,VEN.capped_partial_flag
        ,VEN.dk_capping_metadata_dim
        ,VEN.dk_capping_threshold_dim
        ,VEN.dk_capping_end_datehour_dim
        ,VEN.dk_capping_end_time_dim
        ,VEN.capping_end_date_time_utc
        ,VEN.capping_end_date_time_local
into VEA_21_27_Jan_Viewing_Events_Final
from sk_prod.VESPA_DP_PROG_VIEWED_CURRENT VEN
inner join VEA_21_27_Jan_Viewing_Events_Combined VEA
on VEA.pk_viewing_prog_instance_fact = VEN.pk_viewing_prog_instance_fact

--88,528,392 Row(s) affected
------------------------------------------------------------------------------------------------
--Checking the number of capped and non capped events..

select capped_full_flag,count(*) from VEA_21_27_Jan_Viewing_Events_Final
group by capped_full_flag

select capped_partial_flag, count(*) from VEA_21_27_Jan_Viewing_Events_Final
group by capped_partial_flag

------------------------------------------------------------------------------------------------
--Checking the number of Events by DOW, Hour and event type.. (Old table)
select  Viewing_Type_Detailed
       ,Event_Start_DOW
       ,Event_Start_hour
       ,count(*)as Num_Events_Old
into   Capped_Old
from   VEA_21_27_Jan_Viewing_Events_Final
where  Capped_Event_End_Time is not null
group by Viewing_Type_Detailed
        ,Event_Start_DOW
        ,Event_Start_hour
--525 Row(s) affected

--Total number of events capped
select count(*)as VTD
from   VEA_21_27_Jan_Viewing_Events_Final
where  Capped_Event_End_Time is not null

--Splitting the total number of events capped by Viewing_Type (Live, Vosadal <1hr, Vosdal 1-24hr and Playback
select Viewing_Type_Detailed, count(*)as VTD
from   VEA_21_27_Jan_Viewing_Events_Final
where  Capped_Event_End_Time is not null
group by Viewing_Type_Detailed
order by Viewing_Type_Detailed

--Splitting the total number of events capped by Events Start DOW
select Event_Start_DOW, count(*)as VTD
from   VEA_21_27_Jan_Viewing_Events_Final
where  Capped_Event_End_Time is not null
group by Event_Start_DOW
order by Event_Start_DOW

--Splitting the total number of events capped by Event Start Hour
select Event_Start_hour, count(*)as VTD
from   VEA_21_27_Jan_Viewing_Events_Final
where  Capped_Event_End_Time is not null
group by Event_Start_hour
order by Event_Start_hour

--Splitting the total number of events capped by Pack Group
select pack_grp, count(*)as VTD
from   VEA_21_27_Jan_Viewing_Events_Final
where  Capped_Event_End_Time is not null
group by pack_grp
order by pack_grp

--Splitting the total number of events capped by Genre
select genre_description, count(*)as VTD
from   VEA_21_27_Jan_Viewing_Events_Final
where  Capped_Event_End_Time is not null
group by genre_description
order by genre_description

--Splitting the total number of events capped by Box Subscritpion
select box_subscription, count(*)as VTD
from   VEA_21_27_Jan_Viewing_Events_Final
where  Capped_Event_End_Time is not null
group by box_subscription
order by box_subscription

------------------------------------------------------------------------------------------------------------------------

--Checking the number of Events by DOW, Hour and event type.. (New table)
select  Viewing_Type_Detailed
       ,Event_Start_DOW
       ,Event_Start_hour
       ,count(*)as Num_Events_New
into    Capped_New
from   VEA_21_27_Jan_Viewing_Events_Final
where capping_end_date_time_utc <> EVENT_END_DATE_TIME_UTC
group by Viewing_Type_Detailed
        ,Event_Start_DOW
        ,Event_Start_hour
--574 Row(s) affected

--Total number of events capped
select count(*)as VTD
from   VEA_21_27_Jan_Viewing_Events_Final
where  capping_end_date_time_utc <> EVENT_END_DATE_TIME_UTC

--Splitting the total number of events capped by Viewing_Type (Live, Vosadal <1hr, Vosdal 1-24hr and Playback
select Viewing_Type_Detailed, count(*)as VTD
from   VEA_21_27_Jan_Viewing_Events_Final
where  capping_end_date_time_utc <> EVENT_END_DATE_TIME_UTC
group by Viewing_Type_Detailed
order by Viewing_Type_Detailed

--Splitting the total number of events capped by Events Start DOW
select Event_Start_DOW, count(*)as VTD
from   VEA_21_27_Jan_Viewing_Events_Final
where  capping_end_date_time_utc <> EVENT_END_DATE_TIME_UTC
group by Event_Start_DOW
order by Event_Start_DOW

--Splitting the total number of events capped by Event Start Hour
select Event_Start_hour, count(*)as VTD
from   VEA_21_27_Jan_Viewing_Events_Final
where  capping_end_date_time_utc <> EVENT_END_DATE_TIME_UTC
group by Event_Start_hour
order by Event_Start_hour

--Splitting the total number of events capped by Pack Group
select pack_grp, count(*)as VTD
from   VEA_21_27_Jan_Viewing_Events_Final
where  capping_end_date_time_utc <> EVENT_END_DATE_TIME_UTC
group by pack_grp
order by pack_grp

--Splitting the total number of events capped by Genre
select genre_description, count(*)as VTD
from   VEA_21_27_Jan_Viewing_Events_Final
where  capping_end_date_time_utc <> EVENT_END_DATE_TIME_UTC
group by genre_description
order by genre_description

--Splitting the total number of events capped by Box Subscritpion
select box_subscription, count(*)as VTD
from   VEA_21_27_Jan_Viewing_Events_Final
where  capping_end_date_time_utc <> EVENT_END_DATE_TIME_UTC
group by box_subscription
order by box_subscription

--Comparisons of the above ---(Comparing the capped events from Vespa Events all to the New_Viewing data from CBI)

select  O.Viewing_Type_Detailed
       ,O.Event_Start_DOW
       ,O.Event_Start_hour
       ,O.Num_Events_Old
       ,N.Num_Events_New
       ,(N.Num_Events_New - O.Num_Events_Old)*1.0/(O.Num_Events_Old) as Diff_Events
from    Capped_New N
inner join Capped_Old O
on      O.Viewing_Type_Detailed = N.Viewing_Type_Detailed
and     O.Event_Start_DOW = N.Event_Start_DOW
and     O.Event_Start_hour = N.Event_Start_hour
--525 Row(s) affected
------------------------------------------------------------------------------------------------------------------

--Looking at the viewing time after capping based on the old Viewing Events

select
            Viewing_Type_Detailed
           ,Event_Start_DOW
           ,Event_Start_hour
           ,sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capped_Event_End_Time,EVENT_END_DATE_TIME_UTC)))*1.0/(60.0*60.0) as Total_Viewing_Old
into        Duration_After_Capping_Old
from        VEA_21_27_Jan_Viewing_Events_Final
group by    Viewing_Type_Detailed
           ,Event_Start_DOW
           ,Event_Start_hour
--672 Row(s) affected


--Total Viewing Time After Capping for the old structure
select sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capped_Event_End_Time,EVENT_END_DATE_TIME_UTC)))*1.0/(60.0*60.0) as Sum_Duration_hour_Old
from VEA_21_27_Jan_Viewing_Events_Final


--Total Viewing Time After Capping for the old structure based on Viewing Type
select  Viewing_Type_Detailed
       ,sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capped_Event_End_Time,EVENT_END_DATE_TIME_UTC)))*1.0/3600 as Sum_Duration_hour_Old
from    VEA_21_27_Jan_Viewing_Events_Final
group by Viewing_Type_Detailed
order by Viewing_Type_Detailed


--Total Viewing Time After Capping for the old structure based on DOW
select  Event_Start_DOW
       ,sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capped_Event_End_Time,EVENT_END_DATE_TIME_UTC)))*1.0/3600 as Sum_Duration_hour_Old
from    VEA_21_27_Jan_Viewing_Events_Final
group by Event_Start_DOW
order by Event_Start_DOW

--Total Viewing Time After Capping for the old structure based on Start Hour
select  Event_Start_hour
       ,sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capped_Event_End_Time,EVENT_END_DATE_TIME_UTC)))*1.0/3600 as Sum_Duration_hour_Old
from    VEA_21_27_Jan_Viewing_Events_Final
group by Event_Start_hour
order by Event_Start_hour

--Total Viewing Time After Capping for the old structure based on Pack Group
select  pack_grp
       ,sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capped_Event_End_Time,EVENT_END_DATE_TIME_UTC)))*1.0/3600 as Sum_Duration_hour_Old
from    VEA_21_27_Jan_Viewing_Events_Final
group by pack_grp
order by pack_grp

--Total Viewing Time After Capping for the old structure based on Genre Description
select  genre_description
       ,sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capped_Event_End_Time,EVENT_END_DATE_TIME_UTC)))*1.0/3600 as Sum_Duration_hour_Old
from    VEA_21_27_Jan_Viewing_Events_Final
group by genre_description
order by genre_description

--Total Viewing Time After Capping for the old structure based on Box Subscription
select  box_subscription
       ,sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capped_Event_End_Time,EVENT_END_DATE_TIME_UTC)))*1.0/3600 as Sum_Duration_hour_Old
from    VEA_21_27_Jan_Viewing_Events_Final
group by box_subscription
order by box_subscription


--Looking at the viewing time after capping based on the New Viewing Events

select
            Viewing_Type_Detailed
           ,Event_Start_DOW
           ,Event_Start_hour
           ,sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capping_end_date_time_utc,EVENT_END_DATE_TIME_UTC)))*1.0/(60.0*60.0) as Total_Viewing_New
into        Duration_After_Capping_New
from        VEA_21_27_Jan_Viewing_Events_Final
group by    Viewing_Type_Detailed
           ,Event_Start_DOW
           ,Event_Start_hour

--Total Viewing Time After Capping for the new structure
select sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capping_end_date_time_utc,EVENT_END_DATE_TIME_UTC)))*1.0/(60.0*60.0) as Sum_Duration_hour_New
from VEA_21_27_Jan_Viewing_Events_Final


--Total Viewing Time After Capping for the new structure based on Viewing Type
select  Viewing_Type_Detailed
       ,sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capping_end_date_time_utc,EVENT_END_DATE_TIME_UTC)))*1.0/3600 as Sum_Duration_hour_New
from    VEA_21_27_Jan_Viewing_Events_Final
group by Viewing_Type_Detailed
order by Viewing_Type_Detailed


--Total Viewing Time After Capping for the new structure based on DOW
select  Event_Start_DOW
       ,sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capping_end_date_time_utc,EVENT_END_DATE_TIME_UTC)))*1.0/3600 as Sum_Duration_hour_New
from    VEA_21_27_Jan_Viewing_Events_Final
group by Event_Start_DOW
order by Event_Start_DOW

--Total Viewing Time After Capping for the new structure based on Start Hour
select  Event_Start_hour
       ,sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capping_end_date_time_utc,EVENT_END_DATE_TIME_UTC)))*1.0/3600 as Sum_Duration_hour_New
from    VEA_21_27_Jan_Viewing_Events_Final
group by Event_Start_hour
order by Event_Start_hour

--Total Viewing Time After Capping for the new structure based on Pack Group
select  pack_grp
       ,sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capping_end_date_time_utc,EVENT_END_DATE_TIME_UTC)))*1.0/3600 as Sum_Duration_hour_New
from    VEA_21_27_Jan_Viewing_Events_Final
group by pack_grp
order by pack_grp

--Total Viewing Time After Capping for the new structure based on Genre Description
select  genre_description
       ,sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capping_end_date_time_utc,EVENT_END_DATE_TIME_UTC)))*1.0/3600 as Sum_Duration_hour_New
from    VEA_21_27_Jan_Viewing_Events_Final
group by genre_description
order by genre_description

--Total Viewing Time After Capping for the new structure based on Box Subscription
select  box_subscription
       ,sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capping_end_date_time_utc,EVENT_END_DATE_TIME_UTC)))*1.0/3600 as Sum_Duration_hour_New
from    VEA_21_27_Jan_Viewing_Events_Final
group by box_subscription
order by box_subscription

--Comparisons of the above ---(Comparing the capped viewing time from Vespa Events all to the New_Viewing data from CBI)

select  NC.Viewing_Type_Detailed
       ,NC.Event_Start_DOW
       ,NC.Event_Start_hour
       ,NC.Total_Viewing_New
       ,OC.Total_Viewing_Old
       ,(NC.Total_Viewing_New - OC.Total_Viewing_Old)*1.0/(OC.Total_Viewing_Old) as Diff_dur_hours
from    Duration_After_Capping_New NC
inner join Duration_After_Capping_Old OC
on      NC.Viewing_Type_Detailed = OC.Viewing_Type_Detailed
and     NC.Event_Start_DOW = OC.Event_Start_DOW
and     NC.Event_Start_hour = OC.Event_Start_hour
--672 Row (s) affected


grant all on Vespa_Augs_201301_21_27 to limac;
grant all on VEA_21_27_Jan_Viewing_Events to limac;
grant all on VEA_21_27_Jan_Viewing_Events_Combined to limac;
grant all on VEA_21_27_Jan_Viewing_Events_Final to limac;
grant all on Capped_Old to limac;
grant all on Capped_New to limac;
grant all on Duration_After_Capping_New to limac;
grant all on Duration_After_Capping_Old to limac;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--checks Total Viewing After Capping for New Table
select sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capping_end_date_time_utc,EVENT_END_DATE_TIME_UTC)))*1.0/(60.0*60.0) as Total_Viewing_New
from VEA_21_27_Jan_Viewing_Events_NEWTABLE

--checks Total Viewing After Capping for Old Table
select sum(datediff(second,EVENT_START_DATE_TIME_UTC,coalesce(Capped_Event_End_Time,EVENT_END_DATE_TIME_UTC)))*1.0/(60.0*60.0) as Sum_Duration_hour_Old
from VEA_21_27_Jan_Viewing_Events_Combined


--checks Total Viewing before Capping for Old Table
select sum(datediff(second,EVENT_START_DATE_TIME_UTC,EVENT_END_DATE_TIME_UTC))*1.0/(60.0*60.0) as Sum_Duration_hour_New
from VEA_21_27_Jan_Viewing_Events_NEWTABLE


--checks Total Viewing before Capping for Old Table
select sum(datediff(second,EVENT_START_DATE_TIME_UTC,EVENT_END_DATE_TIME_UTC))*1.0/(60.0*60.0) as Sum_Duration_hour_Old
from VEA_21_27_Jan_Viewing_Events_Combined

--Checks # of Events after Capping for old Table
select count(*)as VTD
from   VEA_21_27_Jan_Viewing_Events_Combined
where  Capped_Event_End_Time is not null

--Checks # of Events after Capping for New Table
select count(*)as VTD
from   VEA_21_27_Jan_Viewing_Events_NEWTABLE
where  capping_end_date_time_utc <> EVENT_END_DATE_TIME_UTC




