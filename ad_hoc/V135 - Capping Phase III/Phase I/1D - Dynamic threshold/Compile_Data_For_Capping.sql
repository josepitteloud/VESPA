/*

1. Compile viewing data for 5-11 Nov with variables required to create capping segments

2. Create a ntile curve for each segment

3. For each ntile curve calculate the first and second derivate

*/

------------------------------------------------------------------------------------------------------------

-- Get viewing events from vespa events all
select rank() over ( partition by subscriber_id
                                 ,EVENT_START_DATE_TIME_UTC
                                 ,EVENT_END_DATE_TIME_UTC
                                 ,type_of_viewing_event
                                 ,Duration
                         order by BROADCAST_START_DATE_TIME_UTC)
        as Program_Order
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
into VEA_5_11Nov_Viewing_Events
from sk_prod.vespa_events_all
where panel_id = 12
and EVENT_START_DATE_TIME_UTC   >= '2012-11-05 00:00:00'
and EVENT_START_DATE_TIME_UTC   <= '2012-11-11 23:59:59'
and type_of_viewing_event <> 'Non viewing event'
and subscriber_id is not null
and Duration > 6


grant select on VEA_5_11Nov_Viewing_Events to limac

-- Remove events that have second and later programmes
delete from VEA_5_11Nov_Viewing_Events where Program_Order > 1


-- Add fields for box subscription and pack group
alter table VEA_5_11Nov_Viewing_Events add box_subscription varchar (2)   default 'U'
alter table VEA_5_11Nov_Viewing_Events add pack_grp         varchar (255) default 'Unknown'

-- Update box_subscription
update VEA_5_11Nov_Viewing_Events
set box_subscription =
        case
            when csh.subscription_sub_type = 'DTV Primary Viewing' then 'P'
            when csh.subscription_sub_type = 'DTV Extra Subscription' then 'S'
        end
        from VEA_5_11Nov_Viewing_Events as VE
    inner join sk_prod.cust_subs_hist as csh
    on VE.account_number = csh.account_number
    where csh.SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing','DTV Extra Subscription')
    and csh.status_code in ('AC','AB','PC')
    and csh.effective_from_dt<='2012-11-05 00:00:00'
    and csh.effective_to_dt>'2012-11-11 23:59:59'



-- Update Pack group
update VEA_5_11Nov_Viewing_Events
set VE.pack_grp = CM.channel_pack
from VEA_5_11Nov_Viewing_Events as VE
inner join vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES as CM
on VE.service_key = CM.service_key
-- 145,183,891 Row(s) affected

-------------------------------------------------------------------
-- Check distributions

select pack_grp,count(*)
from VEA_5_11Nov_Viewing_Events
group by pack_grp
/*
Unknown 594,560
Diginets        12,213,856
Terrestrial     27,036,565
Other   77,022,153
Diginets non-commercial 4,027,075
Terrestrial non-commercial      22,221,849
Other non-commercial    2,662,393
*/

select box_subscription,count(*)
from VEA_5_11Nov_Viewing_Events
group by box_subscription
/*
U       9,286,622
S       13,942,093
P       122,549,736
*/


--Building up the indexes to speed up the whole processes ------------------

create lf index idx1_VD on VEA_5_11Nov_Viewing_Events(Viewing_Type_Detailed);
create lf index idx2_ED on VEA_5_11Nov_Viewing_Events(EVENT_START_DOW);
create lf index idx3_EH on VEA_5_11Nov_Viewing_Events(event_start_hour);
create lf index idx4_BS on VEA_5_11Nov_Viewing_Events(box_subscription);
create lf index idx5_PG on VEA_5_11Nov_Viewing_Events(pack_grp);
create lf index idx6_GD on VEA_5_11Nov_Viewing_Events(genre_description);

-----------------------------------------------------------------------------------------------------------------------------------------
-- 2. Calculate ntile profile for the 3 different segmentations of the viewing data
-----------------------------------------------------------------------------------------------------------------------------------------

--Ntiles partition based on  Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour
select  Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,ntile
        ,Median(Duration) as Median_Duration
        ,Min(Duration) as Min_Duration
        ,Max(Duration) as Max_Duration
into    Segmentation_1_Ntiles
from    (select duration
        ,Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,ntile(200) over (partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour order by duration) as ntile
        from   VEA_5_11Nov_Viewing_Events
        )P
group by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,ntile
order by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,ntile
----134,400 Row(s) affected

------------------------------------------------------------------------------------------------------------
--Ntiles partition based Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,pack_grp,genre_description

select         Viewing_Type_Detailed
                ,EVENT_START_DOW
                ,event_start_hour
                ,pack_grp
                ,genre_description
                ,ntile
                ,Median(Duration) as Median_Duration
                ,Min(Duration) as Min_Duration
                ,Max(Duration) as Max_Duration
into            Segmentation_2_Ntiles
from            (select duration
                ,Viewing_Type_Detailed
                ,EVENT_START_DOW
                ,event_start_hour
                ,pack_grp
                ,genre_description
                ,ntile(200) over (partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,pack_grp,genre_description order by duration) as ntile
                from VEA_5_11Nov_Viewing_Events
                )A
group by        Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,pack_grp,genre_description,ntile
order by        Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,pack_grp,genre_description,ntile
-- 2,982,023 Row(s) affected


--------------------------------------------------------------------------------------------------------------------------------
---Ntiles partition based on Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,box_subscription,pack_grp,genre_description

select           Viewing_Type_Detailed
                ,EVENT_START_DOW
                ,event_start_hour
                ,box_subscription
                ,pack_grp
                ,genre_description
                ,ntile
                ,Median(Duration) as Median_Duration
                ,Min(Duration) as Min_Duration
                ,Max(Duration) as Max_Duration
into            Segmentation_3_Ntiles
from            (select duration
                ,Viewing_Type_Detailed
                ,EVENT_START_DOW
                ,event_start_hour
                ,pack_grp
                ,box_subscription
                ,genre_description
                ,ntile(200) over (partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,box_subscription,pack_grp,genre_description order by duration) as ntile
                from VEA_5_11Nov_Viewing_Events
                )A
group by        Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,box_subscription,pack_grp,genre_description,ntile
order by        Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,box_subscription,pack_grp,genre_description,ntile
-- 6,167,547 row(s) affected

-- Set privileges
grant all on segmentation_1_ntiles to limac;
grant all on segmentation_2_ntiles to limac;
grant all on segmentation_3_ntiles to limac;
---------------------------------------------------------------------
-- 3. Calculate the first/second deriavative of ntile functions
---------------------------------------------------------------------

-- Calculating the Gradient of Segmentation_1_Ntiles
select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,x1 as ntile
        ,y1 as Median_duration
        ,Min_Duration
        ,g1 as First_Gradient
        ,Second_Gradient
into    First_Segmentation_Gradient
from
        (
select  Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,x1
        ,y1
        ,Min_Duration
        ,First_Gradient as g1
        ,max(x1) over( partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour order by x1 rows between 1 preceding and 1 preceding ) as x0
        ,max(g1) over( partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour order by x1 rows between 1 preceding and 1 preceding ) as g0
        ,(g1-g0)*1.0/(x1-x0) as Second_Gradient
        from
        (
SELECT   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,ntile as x1
       ,Median_duration as y1
       ,Min_Duration
       ,max(x1) over( partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour order by x1 rows between 1 preceding and 1 preceding ) as x0
       ,max(y1) over( partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour order by x1 rows between 1 preceding and 1 preceding ) as y0
       ,(y1-y0)*1.0/(x1-x0) as First_Gradient
from Segmentation_1_Ntiles
        ) t1
        ) t2
order by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,ntile
--134,400 Row(s) affected

---Calculating the Gradient of Segmentation_2_Ntiles----
select  Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description
        ,x1 as ntile
        ,y1 as Median_duration
        ,Min_Duration
        ,g1 as First_Gradient
        ,Second_Gradient
into    Second_Segmentation_Gradient
from
        (
select  Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description
        ,x1
        ,y1
        ,Min_Duration
        ,First_Gradient as g1
        ,max(x1) over( partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,pack_grp,genre_description order by x1 rows between 1 preceding and 1 preceding ) as x0
        ,max(g1) over( partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,pack_grp,genre_description order by x1 rows between 1 preceding and 1 preceding ) as g0
        ,(g1-g0)*1.0/(x1-x0) as Second_Gradient
        from
        (
SELECT  Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description
        ,ntile as x1
       ,Median_duration as y1
       ,Min_Duration
       ,max(x1) over( partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,pack_grp,genre_description order by x1 rows between 1 preceding and 1 preceding ) as x0
       ,max(y1) over( partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,pack_grp,genre_description order by x1 rows between 1 preceding and 1 preceding ) as y0
       ,(y1-y0)*1.0/(x1-x0) as First_Gradient
from Segmentation_2_Ntiles
        ) t1
        ) t2
order by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,pack_grp,genre_description,ntile
--2,982,023 Row(s) affected

---Calculating the Gradient of Segmentation_3_Ntiles----
select  Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description
        ,x1 as ntile
        ,y1 as Median_duration
        ,Min_Duration
        ,g1 as First_Gradient
        ,Second_Gradient
into    Third_Segmentation_Gradient
from
        (
select  Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description
        ,x1
        ,y1
        ,Min_Duration
        ,First_Gradient as g1
        ,max(x1) over( partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,box_subscription,pack_grp,genre_description order by x1 rows between 1 preceding and 1 preceding ) as x0
        ,max(g1) over( partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,box_subscription,pack_grp,genre_description order by x1 rows between 1 preceding and 1 preceding ) as g0
        ,(g1-g0)*1.0/(x1-x0) as Second_Gradient
        from
        (
SELECT  Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description
        ,ntile as x1
       ,Median_duration as y1
       ,Min_Duration
       ,max(x1) over( partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,box_subscription,pack_grp,genre_description order by x1 rows between 1 preceding and 1 preceding ) as x0
       ,max(y1) over( partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,box_subscription,pack_grp,genre_description order by x1 rows between 1 preceding and 1 preceding ) as y0
       ,(y1-y0)*1.0/(x1-x0) as First_Gradient
from Segmentation_3_Ntiles
        ) t1
        ) t2
order by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,box_subscription,pack_grp,genre_description,ntile
---6,167,547 Row(s) affected

-- set privileges
grant all on first_segmentation_gradient to limac;
grant all on second_segmentation_gradient to limac;
grant all on third_segmentation_gradient to limac;

---------------------------------------------------------------------------------------------------------------------------------

-- Have a look at the gradients
----------------------Event_Start_DOW = Monday and Viewing_Type_Detailed=0
select * from First_Segmentation_Gradient where Viewing_Type_Detailed=0 and EVENT_START_DOW ='Mon'
select * from Second_Segmentation_Gradient where Viewing_Type_Detailed=0 and EVENT_START_DOW ='Mon'
select * from Third_Segmentation_Gradient where Viewing_Type_Detailed=0 and EVENT_START_DOW ='Mon'

----------------------Event_Start_DOW = Saturday and Viewing_Type_Detailed=0
select * from First_Segmentation_Gradient where Viewing_Type_Detailed=0 and EVENT_START_DOW ='Sat'
select * from Second_Segmentation_Gradient where Viewing_Type_Detailed=0 and EVENT_START_DOW ='Sat'
select * from Third_Segmentation_Gradient where Viewing_Type_Detailed=0 and EVENT_START_DOW ='Sat'

-------------------------------------------------------------
/* Calculating the number of events per segments -----*/

------Segment_1
select Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,count(*) as Number_Events
into Num_Events_Seg_1
from VEA_5_11Nov_Viewing_Events
group by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour

------Segment_2
select Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,pack_grp,genre_description,count(*) as Number_Events
into Num_Events_Seg_2
from VEA_5_11Nov_Viewing_Events
group by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,pack_grp,genre_description

------Segment_3
select Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,box_subscription,pack_grp,genre_description,count(*) as Number_Events
into Num_Events_Seg_3
from VEA_5_11Nov_Viewing_Events
group by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,box_subscription,pack_grp,genre_description;

-------------------------------------------------------
grant all on Num_Events_Seg_1 to limac;
grant all on Num_Events_Seg_2 to limac;
grant all on Num_Events_Seg_3 to limac;
commit;


select * 
from Num_Events_Seg_2
where Viewing_Type_Detailed = 0
and event_start_hour = 8
and EVENT_Start_DOW = 'Mon'
and pack_grp = 'Terrestrial'
and genre_description = 'Children'


select * 
from VEA_5_11Nov_Viewing_Events
where Viewing_Type_Detailed = 0
and event_start_hour = 8
and EVENT_Start_DOW = 'Mon'
and pack_grp = 'Terrestrial'
and genre_description = 'Children'


--Joining the Minimum and Maximumm duration to the Gradient tables
---Segment_1

Alter table Gradient_Threshold_Seg1 add Min_Duration int;
Alter table Gradient_Threshold_Seg1 add Max_Duration int;

Update Gradient_Threshold_Seg1 as FSG
set FSG.Max_Duration=SN1.Max_Duration
from Segmentation_1_Ntiles as SN1
inner join Gradient_Threshold_Seg1 as FSG
on SN1.Viewing_Type_Detailed = FSG.Viewing_Type_Detailed
and SN1.EVENT_START_DOW = FSG.EVENT_START_DOW
and SN1.event_start_hour = FSG.event_start_hour
--672 Row(s) affected


---Segment_2

Alter table Gradient_Threshold_Seg2 add Min_Duration int;
Alter table Gradient_Threshold_Seg2 add Max_Duration int;

Update Gradient_Threshold_Seg2 as SSG
set SSG.Min_Duration=SN2.Min_Duration
from Segmentation_2_Ntiles as SN2
inner join Gradient_Threshold_Seg2 as SSG
on SN2.Viewing_Type_Detailed = SSG.Viewing_Type_Detailed
and SN2.EVENT_START_DOW = SSG.EVENT_START_DOW
and SN2.event_start_hour = SSG.event_start_hour
and coalesce(SN2.pack_grp, 'Unknown') = coalesce(SSG.pack_grp, 'Unknown')
and coalesce(SN2.genre_description, 'Empty') = coalesce(SSG.genre_description, 'Empty')
---22,125 Row(s) affected


---Segment_3

Alter table Gradient_Threshold_Seg3 add Min_Duration int;
Alter table Gradient_Threshold_Seg3 add Max_Duration int;

Update Gradient_Threshold_Seg3 as TSG
set TSG.Min_Duration=SN3.Min_Duration
from Segmentation_3_Ntiles as SN3
inner join Gradient_Threshold_Seg3 as TSG
on SN3.Viewing_Type_Detailed = TSG.Viewing_Type_Detailed
and SN3.EVENT_START_DOW = TSG.EVENT_START_DOW
and SN3.event_start_hour = TSG.event_start_hour
and SN3.box_subscription = TSG.box_subscription
and coalesce(SN3.pack_grp, 'Unknown') = coalesce(TSG.pack_grp, 'Unknown')
and coalesce(SN3.genre_description, 'Empty') = coalesce(TSG.genre_description, 'Empty')
---58,960 Row(s) affected

select top 10* from Gradient_Threshold_Seg1

grant all on VEA_5_11Nov_Viewing_Events to limac


Grant all on First_Segmentation_Gradient to limac;
Grant all on Second_Segmentation_Gradient to limac;
Grant all on Third_Segmentation_Gradient to limac;


------------------------------------------------------------------
---Segment_1

select           SN1.Viewing_Type_Detailed
                ,SN1.EVENT_START_DOW
                ,SN1.event_start_hour
                ,SN1.ntile
                ,SN1.Median_Duration
from Segmentation_1_Ntiles as SN1
inner join Gradient_Threshold_Seg1_v2 as TSG
on SN1.Viewing_Type_Detailed = TSG.Viewing_Type_Detailed
and SN1.EVENT_START_DOW = TSG.EVENT_START_DOW
and SN1.event_start_hour = TSG.event_start_hour
where TSG.Threshold_Grad_v2 is null
and SN1.Viewing_Type_Detailed <>0
and TSG.Viewing_Type_Detailed <>0


---Segment_2

select           SN2.Viewing_Type_Detailed
                ,SN2.EVENT_START_DOW
                ,SN2.event_start_hour
                ,SN2.pack_grp
                ,SN2.genre_description
                ,SN2.ntile
                ,SN2.Median_Duration
from Segmentation_2_Ntiles as SN2
inner join Gradient_Threshold_Seg2_v2 as TSG
on SN2.Viewing_Type_Detailed = TSG.Viewing_Type_Detailed
and SN2.EVENT_START_DOW = TSG.EVENT_START_DOW
and SN2.event_start_hour = TSG.event_start_hour
and coalesce(SN2.pack_grp, 'Unknown') = coalesce(TSG.pack_grp, 'Unknown')
and coalesce(SN2.genre_description, 'Empty') = coalesce(TSG.genre_description, 'Empty')
where TSG.Threshold_Grad_v2 is null
and SN2.Viewing_Type_Detailed = 0
and TSG.Viewing_Type_Detailed = 0

------Segment_3

select           SN3.Viewing_Type_Detailed
                ,SN3.EVENT_START_DOW
                ,SN3.event_start_hour
                ,SN3.box_subscription
                ,SN3.pack_grp
                ,SN3.genre_description
                ,SN3.ntile
                ,SN3.Median_Duration
from Segmentation_3_Ntiles as SN3
inner join Gradient_Threshold_Seg3_v2 as TSG
on SN3.Viewing_Type_Detailed = TSG.Viewing_Type_Detailed
and SN3.EVENT_START_DOW = TSG.EVENT_START_DOW
and SN3.event_start_hour = TSG.event_start_hour
and SN3.box_subscription = TSG.box_subscription
and coalesce(SN3.pack_grp, 'Unknown') = coalesce(TSG.pack_grp, 'Unknown')
and coalesce(SN3.genre_description, 'Empty') = coalesce(TSG.genre_description, 'Empty')
where TSG.Threshold_Grad_v2 is null
and SN3.Viewing_Type_Detailed = 0
and TSG.Viewing_Type_Detailed = 0

