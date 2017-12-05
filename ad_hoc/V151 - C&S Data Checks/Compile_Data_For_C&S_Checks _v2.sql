/*

 Compile viewing data from 21-27 Jan 2013 with variables required for C&S Checks from VESPA Events All and From the Daily Augmented Tables
 Author : Patrick Igonor

*/

------------------------------------------------------------------------------------------------------------

--First step is to Combine augmented tables for week 21-27 Jan 2013


select *
into Vespa_Augs_201301_07_13
from (
select * from vespa_analysts.vespa_daily_augs_20130107
union all
select * from vespa_analysts.vespa_daily_augs_20130108
union all
select * from vespa_analysts.vespa_daily_augs_20130109
union all
select * from vespa_analysts.vespa_daily_augs_20130110
union all
select * from vespa_analysts.vespa_daily_augs_20130111
union all
select * from vespa_analysts.vespa_daily_augs_20130112
union all
select * from vespa_analysts.vespa_daily_augs_20130113
) P

--147,160,018 Row(s) affected

-- Create Indexes
create hg index idx1 on Vespa_Augs_201301_07_13(cb_row_id);


-- Breakdown by capping type
select capped_flag
        ,count(*)
from  Vespa_Augs_201301_21_27
group by capped_flag
order by capped_flag
/*
capped_flag     count()
0               98,557,614
1               1,840,237
2               3,350,892
*/



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
into VEA_07_13_Jan_Viewing_Events
from sk_prod.vespa_events_all
where panel_id = 12
and EVENT_START_DATE_TIME_UTC   >= '2013-01-07 00:00:00'
and EVENT_START_DATE_TIME_UTC   <= '2013-01-13 23:59:59'
and type_of_viewing_event <> 'Non viewing event'
and subscriber_id is not null
and account_number is not null
and Duration > 6
--175,817,756 Row(s) affected

-- Remove events that have second and later programmes
delete from VEA_07_13_Jan_Viewing_Events where Program_Order > 1
--50,726,800 Row(s) affected

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
--93,458,304 Row(s) affected


--Building up the indexes to speed up the whole processes -----

create lf index idx1_1 on VEA_21_27_Jan_Viewing_Events(Viewing_Type_Detailed);
create lf index idx2_2 on VEA_21_27_Jan_Viewing_Events(EVENT_START_DOW);
create lf index idx3_3 on VEA_21_27_Jan_Viewing_Events(event_start_hour);
create lf index idx4_4 on VEA_21_27_Jan_Viewing_Events(box_subscription);
create lf index idx5_5 on VEA_21_27_Jan_Viewing_Events(pack_grp);
create lf index idx6_6 on VEA_21_27_Jan_Viewing_Events(genre_description);
create hg index idx7_7 on VEA_21_27_Jan_Viewing_Events(pk_viewing_prog_instance_fact);


-- Compiling info from vespa_events_all and augmented data
select
         VEA.subscriber_id
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
inner join Vespa_Augs_201301_21_27 AUG
on VEA.pk_viewing_prog_instance_fact = AUG.cb_row_id
group by VEA.subscriber_id
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
---86,080,239 Row(s) affected


------------------------------------------------------------------------------------------------------------------------------

-- Get viewing events from VESPA_DP_PROG_VIEWED_CURRENT

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
         ,capped_full_flag
         ,capped_partial_flag
         ,dk_capping_metadata_dim
         ,dk_capping_threshold_dim
         ,dk_capping_end_datehour_dim
         ,dk_capping_end_time_dim
         ,capping_end_date_time_utc
         ,capping_end_date_time_local
into VEA_21_27_Jan_Viewing_Events_DP_PROG_NEW
from sk_prod.VESPA_DP_PROG_VIEWED_CURRENT
where panel_id = 12
and EVENT_START_DATE_TIME_UTC   >= '2013-01-21 00:00:00'
and EVENT_START_DATE_TIME_UTC   <= '2013-01-27 23:59:59'
and type_of_viewing_event <> 'Non viewing event'
and subscriber_id is not null
and Duration > 6
--143,212,682 Row(s) affected

-- Remove events that have second and later programmes
delete from VEA_21_27_Jan_Viewing_Events_DP_PROG_NEW where Program_Order > 1
--49,713,512 Row(s) affected

-- Add fields for box subscription and pack group
alter table VEA_21_27_Jan_Viewing_Events_DP_PROG_NEW add box_subscription varchar (2)   default 'U'
alter table VEA_21_27_Jan_Viewing_Events_DP_PROG_NEW add pack_grp varchar (255) default 'Unknown'

-- Update box_subscription
update VEA_21_27_Jan_Viewing_Events_DP_PROG_NEW
set box_subscription =
        case
            when csh.subscription_sub_type = 'DTV Primary Viewing' then 'P'
            when csh.subscription_sub_type = 'DTV Extra Subscription' then 'S'
        end
        from VEA_21_27_Jan_Viewing_Events_DP_PROG_NEW as VE
    inner join sk_prod.cust_subs_hist as csh
    on VE.account_number = csh.account_number
    where csh.SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing','DTV Extra Subscription')
    and csh.status_code in ('AC','AB','PC')
    and csh.effective_from_dt<='2013-01-21 00:00:00'
    and csh.effective_to_dt>'2013-01-27 23:59:59'
--87,526,038 Row(s) affected


-- Update Pack group
update VEA_21_27_Jan_Viewing_Events_DP_PROG_NEW
set VE.pack_grp = CM.channel_pack
from VEA_21_27_Jan_Viewing_Events_DP_PROG_NEW as VE
inner join vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES as CM
on VE.service_key = CM.service_key
--93,486,026 Row(s) affected



-------------------------------------------------------------------
-- Checking the number of events by the different metrics on both tables (New and Old)

select Viewing_Type_Detailed,count(*)
from VEA_21_27_Jan_Viewing_Events_combined
group by Viewing_Type_Detailed

/*Viewing_Type_Detailed         count()
0                               60,683,039
2                               7,589,290
1                               3,992,173
3                               13,815,733
*/

select Viewing_Type_Detailed,count(*)
from VEA_21_27_Jan_Viewing_Events_DP_PROG_NEW
group by Viewing_Type_Detailed

/*
Viewing_Type_Detailed   count()
0                       64,856,638
1                       4,300,531
2                       8,163,355
3                       16,178,646
*/
----------------------------------------------------
select Event_Start_DOW,count(*)
from VEA_21_27_Jan_Viewing_Events_combined
group by Event_Start_DOW

/*
Event_Start_DOW         count()
Sun                     7,021,770
Mon                     17,974,995
Tue                     16,920,388
Wed                     17,244,073
Thu                     15,923,454
Fri                     9,947,778
Sat                     1,047,777
*/

select Event_Start_DOW,count(*)
from VEA_21_27_Jan_Viewing_Events_DP_PROG_NEW
group by Event_Start_DOW

/*
Event_Start_DOW         count()

Sun                     7,546,293
Mon                     19,299,698
Tue                     18,163,837
Wed                     18,522,115
Thu                     17,097,462
Fri                     11,752,880
Sat                     1,116,885
*/
----------------------------------------------------
select pack_grp,count(*)
from VEA_21_27_Jan_Viewing_Events_combined
group by pack_grp

/*
pack_grp                        count()
Diginets                        6,934,378
Terrestrial non-commercial      14,929,729
Other                           41,479,134
Other non-commercial            1,444,249
Unknown                         12,254
Diginets non-commercial         2,891,941
Terrestrial                     18,388,550
*/

select pack_grp,count(*)
from VEA_21_27_Jan_Viewing_Events_DP_PROG_NEW
group by pack_grp

/*
pack_grp                        count()
Diginets                        7,558,450
Terrestrial non-commercial      16,345,230
Other                           45,077,801
Other non-commercial            1,604,417
Unknown                         13,144
Diginets non-commercial         3,164,667
Terrestrial                     19,735,461
*/
----------------------------------------------------
select box_subscription,count(*)
from VEA_21_27_Jan_Viewing_Events_combined
group by box_subscription
/*
box_subscription        count()
S                       9,103,044
U                       414,539
P                       76,562,652
*/

select box_subscription,count(*)
from VEA_21_27_Jan_Viewing_Events_DP_PROG_NEW
group by box_subscription
/*
box_subscription        count()
U                       5,973,132
S                       9,094,588
P                       78,431,450
*/
-----------------------------------------------------
--Checks--

select * from VEA_21_27_Jan_Viewing_Events
where pk_viewing_prog_instance_fact in
(select pk_viewing_prog_instance_fact from VEA_21_27_Jan_Viewing_Events
 except
 select cb_row_id from Vespa_Augs_201301_21_27)
 --Error: Feature, INTERSECT or EXCEPT, is not supported
select
         VEA.subscriber_id
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
into    Missing_Table
from VEA_21_27_Jan_Viewing_Events VEA
right join Vespa_Augs_201301_21_27 AUG
on VEA.pk_viewing_prog_instance_fact = AUG.cb_row_id
where VEA.pk_viewing_prog_instance_fact is null
group by VEA.subscriber_id
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
--4,703,241 Row(s) affected



-----------------------------------------------------------------------Suspended meanwhile until the match is sorted....
--Number of Events by DOW, hour and event type for the Vespa Events All Table---???

select  Event_Start_DOW
       ,Event_Start_Hour
       ,Viewing_Type_Detailed
       ,count(*)as Num_Events
into   Number_Events
from   VEA_21_27_Jan_Viewing_Events_combined
Group by Event_Start_DOW
        ,Event_Start_Hour
        ,Viewing_Type_Detailed
--672 Row(s) affected

--Number of Events by DOW, hour and event type for the New_Viewing Events Table---

select  Event_Start_DOW
       ,Event_Start_Hour
       ,Viewing_Type_Detailed
       ,count(*)as Num_Events_New
into   Number_Events_New
from   VEA_21_27_Jan_Viewing_Events_DP_PROG_NEW
Group by Event_Start_DOW
        ,Event_Start_Hour
        ,Viewing_Type_Detailed
--672 Row(s) affected
--------------------------------------------------------------------------------------

select pk_viewing_prog_instance_fact
into igonorp.Missing_ID_07_13
from VEA_07_13_Jan_Viewing_Events
where pk_viewing_prog_instance_fact not in (select cb_row_id from Vespa_Augs_201301_07_13)
--2,066,575 Row(s) affected




