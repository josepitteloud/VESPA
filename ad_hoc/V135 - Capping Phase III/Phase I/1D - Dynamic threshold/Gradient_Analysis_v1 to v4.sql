
---Version 1

/*

1. For each segment calculate the average for absolute values of the second gradient:
second_gradient_avg = sum [ abs(f''(x_i)) ] / n

2. For each segment find the first ntile where the second gradient is greater than the average and
is followed by a negative value that is also greater than the average.

*/

---
-- 1a----Calculating the average of Segmentation_1_Ntiles

select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,avg(abs(Second_Gradient)) as Second_Gradient_Average
from First_Segmentation_Gradient
group by Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour

-- 1b----Calculating the average of Segmentation_2_Ntiles

select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description
        ,avg(abs(Second_Gradient)) as Second_Gradient_Average
from Second_Segmentation_Gradient
group by Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description

-- 1c----Calculating the average of Segmentation_3_Ntiles

select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description
        ,avg(abs(Second_Gradient)) as Second_Gradient_Average
from Third_Segmentation_Gradient
group by Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description

                                        --- First version of Dynamic Capping---


/*For each segment find the first ntile where the second gradient is greater than the average and
is followed by a negative value that is also greater that the average.
*/
----First ntile with the above condition on First_Segmentation Gradient----


select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,min(Thresholds)as Threshold_Grad
into    Gradient_Threshold_Seg1_v1
from
        (
select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,ntile
        ,Second_Gradient
        ,Median_duration
        ,max(Second_Gradient) over( partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour order by ntile rows between 1 following and 1 following) as next_value
        ,case when (Second_Gradient > Second_Gradient_Average) and ((next_value*-1) > Second_Gradient_Average) then ntile else NULL
         end as Thresholds
from
        (
        select   Viewing_Type_Detailed
                ,EVENT_START_DOW
                ,event_start_hour
                ,ntile
                ,Second_Gradient
                ,Median_duration
                ,avg(abs(Second_Gradient)) over(partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour) as Second_Gradient_Average
        from First_Segmentation_Gradient
        )t1
        )t2
group by
         Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
--672 Row(s) affected



--Adding a capping limit of 20 and 120 rule based on the existing rule

Alter table Gradient_Threshold_Seg1_v1 add Capping_Threshold_Grad int;

Update Gradient_Threshold_Seg1_v1  as FSG
set FSG.Capping_Threshold_Grad = case
                                                when FSG.Viewing_Type_Detailed=0 and SN1.Min_Duration is null or SN1.Min_Duration < (20*60) then 20*60
                                                when FSG.Viewing_Type_Detailed=0 and SN1.Min_Duration > (120*60) then (120*60)
                                                else SN1.Min_Duration
                                        end
from Segmentation_1_Ntiles as SN1
inner join Gradient_Threshold_Seg1_v1 as FSG
on SN1.Viewing_Type_Detailed = FSG.Viewing_Type_Detailed
and SN1.EVENT_START_DOW = FSG.EVENT_START_DOW
and SN1.event_start_hour = FSG.event_start_hour
and SN1.ntile = FSG.Threshold_Grad

--507 Row(s) affected

--Add Minimum duration to Gradient_Table
Alter table Gradient_Threshold_Seg1_v1 add Min_Duration int;

Update Gradient_Threshold_Seg1_v1  as FSG
set FSG.Min_Duration = SN1.Min_Duration
from Segmentation_1_Ntiles as SN1
inner join Gradient_Threshold_Seg1_v1 as FSG
on SN1.Viewing_Type_Detailed = FSG.Viewing_Type_Detailed
and SN1.EVENT_START_DOW = FSG.EVENT_START_DOW
and SN1.event_start_hour = FSG.event_start_hour
and SN1.ntile = FSG.Threshold_Grad

--507 Row(s) affected


----First ntile with the above condition on Second_Segmentation Gradient----

select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description
        ,min(Thresholds)as Threshold_Grad
into    Gradient_Threshold_Seg2_v1
from
        (
select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description
        ,ntile
        ,Second_Gradient
        ,Median_duration
        ,max(Second_Gradient) over( partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,pack_grp,genre_description order by ntile rows between 1 following and 1 following) as next_value
        ,case when (Second_Gradient > Second_Gradient_Average) and ((next_value*-1) > Second_Gradient_Average) then ntile else NULL
         end as Thresholds
from
        (
        select   Viewing_Type_Detailed
                ,EVENT_START_DOW
                ,event_start_hour
                ,pack_grp
                ,genre_description
                ,ntile
                ,Second_Gradient
                ,Median_duration
                ,avg(abs(Second_Gradient)) over(partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,pack_grp,genre_description) as Second_Gradient_Average
        from Second_Segmentation_Gradient
        )t1
        )t2
group by
         Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description
---22,125 Row(s) affected


--Adding a capping limit of 20 and 120 rule based on the existing rule

Alter table Gradient_Threshold_Seg2_v1 add Capping_Threshold_Grad  int;

Update Gradient_Threshold_Seg2_v1  as SSG
set SSG.Capping_Threshold_Grad = case
                                                when SSG.Viewing_Type_Detailed=0 and SN2.Min_Duration is null or SN2.Min_Duration < (20*60) then 20*60
                                                when SSG.Viewing_Type_Detailed=0 and SN2.Min_Duration > (120*60) then (120*60)
                                                else SN2.Min_Duration
                                        end
from Segmentation_2_Ntiles as SN2
inner join Gradient_Threshold_Seg2_v1 as SSG
on SN2.Viewing_Type_Detailed = SSG.Viewing_Type_Detailed
and SN2.EVENT_START_DOW = SSG.EVENT_START_DOW
and SN2.event_start_hour = SSG.event_start_hour
and coalesce(SN2.pack_grp, 'Empty') = coalesce(SSG.pack_grp, 'Empty')
and coalesce(SN2.genre_description, 'Unknown') = coalesce(SSG.genre_description, 'Unknown')
and SN2.ntile = SSG.Threshold_Grad
--17,374 Row(s) affected

---Add minimum duration to Gradient table
Alter table Gradient_Threshold_Seg2_v1 add Min_Duration  int;

Update Gradient_Threshold_Seg2_v1  as SSG
set SSG.Min_Duration = SN2.Min_Duration
from Segmentation_2_Ntiles as SN2
inner join Gradient_Threshold_Seg2_v1 as SSG
on SN2.Viewing_Type_Detailed = SSG.Viewing_Type_Detailed
and SN2.EVENT_START_DOW = SSG.EVENT_START_DOW
and SN2.event_start_hour = SSG.event_start_hour
and coalesce(SN2.pack_grp, 'Empty') = coalesce(SSG.pack_grp, 'Empty')
and coalesce(SN2.genre_description, 'Unknown') = coalesce(SSG.genre_description, 'Unknown')
and SN2.ntile = SSG.Threshold_Grad

--17,374 Row(s) affected

----First ntile with the above condition on Third_Segmentation Gradient----

select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description
        ,min(Thresholds)as Threshold_Grad
into    Gradient_Threshold_Seg3_v1
from
        (
select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description
        ,ntile
        ,Second_Gradient
        ,Median_duration
        ,max(Second_Gradient) over( partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,box_subscription,pack_grp,genre_description order by ntile rows between 1 following and 1 following) as next_value
        ,case when (Second_Gradient > Second_Gradient_Average) and ((next_value*-1) > Second_Gradient_Average) then ntile else NULL
         end as Thresholds
from
        (
        select   Viewing_Type_Detailed
                ,EVENT_START_DOW
                ,event_start_hour
                ,box_subscription
                ,pack_grp
                ,genre_description
                ,ntile
                ,Second_Gradient
                ,Median_duration
                ,avg(abs(Second_Gradient)) over(partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,box_subscription,pack_grp,genre_description) as Second_Gradient_Average
        from Third_Segmentation_Gradient
        )t1
        )t2
group by
         Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description
--58,960 Row(s) affected


--Adding a capping limit of 20 and 120 rule based on the existing rule

Alter table Gradient_Threshold_Seg3_v1 add Capping_Threshold_Grad  int;

Update Gradient_Threshold_Seg3_v1  as TSG
set TSG.Capping_Threshold_Grad = case
                                                when TSG.Viewing_Type_Detailed=0 and SN3.Min_Duration is null or SN3.Min_Duration < (20*60) then 20*60
                                                when TSG.Viewing_Type_Detailed=0 and SN3.Min_Duration > (120*60) then (120*60)
                                                else SN3.Min_Duration
                                        end
from Segmentation_3_Ntiles as SN3
inner join Gradient_Threshold_Seg3_v1 as TSG
on SN3.Viewing_Type_Detailed = TSG.Viewing_Type_Detailed
and SN3.EVENT_START_DOW = TSG.EVENT_START_DOW
and SN3.event_start_hour = TSG.event_start_hour
and SN3.box_subscription = TSG.box_subscription
and coalesce(SN3.pack_grp, 'Empty') = Coalesce(TSG.pack_grp, 'Empty')
and coalesce(SN3.genre_description, 'Unknown') = coalesce(TSG.genre_description, 'Unknown')
and SN3.ntile = TSG.Threshold_Grad

--42,128 Row(s) affected

--Add Minimum duration to Gradient table

Alter table Gradient_Threshold_Seg3_v1 add Min_Duration  int;

Update Gradient_Threshold_Seg3_v1  as TSG
set TSG.Min_Duration = SN3.Min_Duration
from Segmentation_3_Ntiles as SN3
inner join Gradient_Threshold_Seg3_v1 as TSG
on SN3.Viewing_Type_Detailed = TSG.Viewing_Type_Detailed
and SN3.EVENT_START_DOW = TSG.EVENT_START_DOW
and SN3.event_start_hour = TSG.event_start_hour
and SN3.box_subscription = TSG.box_subscription
and coalesce(SN3.pack_grp, 'Empty') = Coalesce(TSG.pack_grp, 'Empty')
and coalesce(SN3.genre_description, 'Unknown') = coalesce(TSG.genre_description, 'Unknown')
and SN3.ntile = TSG.Threshold_Grad

--42,128 Row(s) affected

                                --- Second version of Dynamic Capping---


-- 1
/*For each segment find the first ntile where the second gradient is greater than the average and
is followed by a negative value that is also greater that the average.
Only select the ntiles > 160 from the above
*/

select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,min(Thresholds)as Threshold_Grad_v2
into     Gradient_Threshold_Seg1_v2
from
        (
select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,ntile
        ,Second_Gradient
        ,Median_duration
        ,max(Second_Gradient) over( partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour order by ntile rows between 1 following and 1 following) as next_value
        ,case when (Second_Gradient > Second_Gradient_Average) and ((next_value*-1) > Second_Gradient_Average)then
                case when ntile>160 then ntile else NULL
                end
        end as Thresholds
from
        (
        select   Viewing_Type_Detailed
                ,EVENT_START_DOW
                ,event_start_hour
                ,ntile
                ,Second_Gradient
                ,Median_duration
                ,avg(abs(Second_Gradient)) over(partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour) as Second_Gradient_Average
        from First_Segmentation_Gradient
        )t1
        )t2
group by
         Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
--672 Row(s) affected



--Adding a capping limit of 20 and 120 rule based on the existing rule

Alter table Gradient_Threshold_Seg1_v2 add Capping_Threshold_Grad int;

Update Gradient_Threshold_Seg1_v2  as FSG
set FSG.Capping_Threshold_Grad = case
                                                when FSG.Viewing_Type_Detailed=0 and SN1.Min_Duration is null or SN1.Min_Duration < (20*60) then 20*60
                                                when FSG.Viewing_Type_Detailed=0 and SN1.Min_Duration > (120*60) then (120*60)
                                                else SN1.Min_Duration
                                        end
from Segmentation_1_Ntiles as SN1
inner join Gradient_Threshold_Seg1_v2 as FSG
on SN1.Viewing_Type_Detailed = FSG.Viewing_Type_Detailed
and SN1.EVENT_START_DOW = FSG.EVENT_START_DOW
and SN1.event_start_hour = FSG.event_start_hour
and SN1.ntile = FSG.Threshold_Grad_v2

--506 Row(s) affected

--Add Minimum duration to Gradient_Table
Alter table Gradient_Threshold_Seg1_v2 add Min_Duration int;

Update Gradient_Threshold_Seg1_v2  as FSG
set FSG.Min_Duration = SN1.Min_Duration
from Segmentation_1_Ntiles as SN1
inner join Gradient_Threshold_Seg1_v2 as FSG
on SN1.Viewing_Type_Detailed = FSG.Viewing_Type_Detailed
and SN1.EVENT_START_DOW = FSG.EVENT_START_DOW
and SN1.event_start_hour = FSG.event_start_hour
and SN1.ntile = FSG.Threshold_Grad_v2

--506 Row(s) affected

----First ntile with the above condition on Second_Segmentation Gradient----

select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description
        ,min(Thresholds)as Threshold_Grad_v2
into    Gradient_Threshold_Seg2_v2
from
        (
select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description
        ,ntile
        ,Second_Gradient
        ,Median_duration
        ,max(Second_Gradient) over( partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,pack_grp,genre_description order by ntile rows between 1 following and 1 following) as next_value
        ,case when (Second_Gradient > Second_Gradient_Average) and ((next_value*-1) > Second_Gradient_Average)then
                case when ntile>160 then ntile else NULL
                end
         end as Thresholds
from
        (
        select   Viewing_Type_Detailed
                ,EVENT_START_DOW
                ,event_start_hour
                ,pack_grp
                ,genre_description
                ,ntile
                ,Second_Gradient
                ,Median_duration
                ,avg(abs(Second_Gradient)) over(partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,pack_grp,genre_description) as Second_Gradient_Average
        from Second_Segmentation_Gradient
        )t1
        )t2
group by
         Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description
---22,125 Row(s) affected


--Adding a capping limit of 20 and 120 rule based on the existing rule

Alter table Gradient_Threshold_Seg2_v2 add Capping_Threshold_Grad  int;

Update Gradient_Threshold_Seg2_v2  as SSG
set SSG.Capping_Threshold_Grad = case
                                                when SSG.Viewing_Type_Detailed=0 and SN2.Min_Duration is null or SN2.Min_Duration < (20*60) then 20*60
                                                when SSG.Viewing_Type_Detailed=0 and SN2.Min_Duration > (120*60) then (120*60)
                                                else SN2.Min_Duration
                                        end
from Segmentation_2_Ntiles as SN2
inner join Gradient_Threshold_Seg2_v2 as SSG
on SN2.Viewing_Type_Detailed = SSG.Viewing_Type_Detailed
and SN2.EVENT_START_DOW = SSG.EVENT_START_DOW
and SN2.event_start_hour = SSG.event_start_hour
and coalesce(SN2.pack_grp, 'Empty') = coalesce(SSG.pack_grp, 'Empty')
and coalesce(SN2.genre_description, 'Unknown') = coalesce(SSG.genre_description, 'Unknown')
and SN2.ntile = SSG.Threshold_Grad_v2

--11,942 Row(s) affected

---Add minimum duration to Gradient table
Alter table Gradient_Threshold_Seg2_v2 add Min_Duration  int;

Update Gradient_Threshold_Seg2_v2  as SSG
set SSG.Min_Duration = SN2.Min_Duration
from Segmentation_2_Ntiles as SN2
inner join Gradient_Threshold_Seg2_v2 as SSG
on SN2.Viewing_Type_Detailed = SSG.Viewing_Type_Detailed
and SN2.EVENT_START_DOW = SSG.EVENT_START_DOW
and SN2.event_start_hour = SSG.event_start_hour
and coalesce(SN2.pack_grp, 'Empty') = coalesce(SSG.pack_grp, 'Empty')
and coalesce(SN2.genre_description, 'Unknown') = coalesce(SSG.genre_description, 'Unknown')
and SN2.ntile = SSG.Threshold_Grad_v2

--11,942 Row(s) affected


----First ntile with the above condition on Third_Segmentation Gradient----

select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description
        ,min(Thresholds)as Threshold_Grad_v2
into    Gradient_Threshold_Seg3_v2
from
        (
select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description
        ,ntile
        ,Second_Gradient
        ,Median_duration
        ,max(Second_Gradient) over( partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,box_subscription,pack_grp,genre_description order by ntile rows between 1 following and 1 following) as next_value
        ,case when (Second_Gradient > Second_Gradient_Average) and ((next_value*-1) > Second_Gradient_Average)then
                case when ntile>160 then ntile else NULL
                end
         end as Thresholds
from
        (
        select   Viewing_Type_Detailed
                ,EVENT_START_DOW
                ,event_start_hour
                ,box_subscription
                ,pack_grp
                ,genre_description
                ,ntile
                ,Second_Gradient
                ,Median_duration
                ,avg(abs(Second_Gradient)) over(partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,box_subscription,pack_grp,genre_description) as Second_Gradient_Average
        from Third_Segmentation_Gradient
        )t1
        )t2
group by
         Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description

---58,960 Row(s) affected



--Adding a capping limit of 20 and 120 rule based on the existing rule

Alter table Gradient_Threshold_Seg3_v2 add Capping_Threshold_Grad  int;

Update Gradient_Threshold_Seg3_v2  as TSG
set TSG.Capping_Threshold_Grad = case
                                                when TSG.Viewing_Type_Detailed=0 and SN3.Min_Duration is null or SN3.Min_Duration < (20*60) then 20*60
                                                when TSG.Viewing_Type_Detailed=0 and SN3.Min_Duration > (120*60) then (120*60)
                                                else SN3.Min_Duration
                                        end
from Segmentation_3_Ntiles as SN3
inner join Gradient_Threshold_Seg3_v2 as TSG
on SN3.Viewing_Type_Detailed = TSG.Viewing_Type_Detailed
and SN3.EVENT_START_DOW = TSG.EVENT_START_DOW
and SN3.event_start_hour = TSG.event_start_hour
and SN3.box_subscription = TSG.box_subscription
and coalesce(SN3.pack_grp, 'Empty') = Coalesce(TSG.pack_grp, 'Empty')
and coalesce(SN3.genre_description, 'Unknown') = coalesce(TSG.genre_description, 'Unknown')
and SN3.ntile = TSG.Threshold_Grad_v2

--23,054 Row(s) affected

--Add Minimum duration to Gradient table

Alter table Gradient_Threshold_Seg3_v2 add Min_Duration  int;

Update Gradient_Threshold_Seg3_v2  as TSG
set TSG.Min_Duration = SN3.Min_Duration
from Segmentation_3_Ntiles as SN3
inner join Gradient_Threshold_Seg3_v2 as TSG
on SN3.Viewing_Type_Detailed = TSG.Viewing_Type_Detailed
and SN3.EVENT_START_DOW = TSG.EVENT_START_DOW
and SN3.event_start_hour = TSG.event_start_hour
and SN3.box_subscription = TSG.box_subscription
and coalesce(SN3.pack_grp, 'Empty') = Coalesce(TSG.pack_grp, 'Empty')
and coalesce(SN3.genre_description, 'Unknown') = coalesce(TSG.genre_description, 'Unknown')
and SN3.ntile = TSG.Threshold_Grad_v2

--23,054 Row(s) affected

                           --- Third version of Dynamic Capping ---

/*


1. For each segment find the first ntile where the second gradient is greater than the average and
is followed by a negative value that is also greater that the average.

2. Find the miniumum ntile that is greater than 160 and if this is null, then find the maximum ntile that is less than 160
*/

---
----First ntile with the above condition on First_Segmentation Gradient----

select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,coalesce(min(Threshold_1),max(Threshold_2)) as Threshold_Grad_v3
into    Gradient_Threshold_Seg1_v3
from (
select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,case when Thresholds >= 160 then Thresholds end as Threshold_1
        ,case when Thresholds < 160 then Thresholds end as Threshold_2
from
        (
select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,ntile
        ,Second_Gradient
        ,Median_duration
        ,max(Second_Gradient) over( partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour order by ntile rows between 1 following and 1 following) as next_value
        ,case when (Second_Gradient > Second_Gradient_Average) and ((next_value*-1) > Second_Gradient_Average) then ntile else NULL
         end as Thresholds
from
        (
        select   Viewing_Type_Detailed
                ,EVENT_START_DOW
                ,event_start_hour
                ,ntile
                ,Second_Gradient
                ,Median_duration
                ,avg(abs(Second_Gradient)) over(partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour) as Second_Gradient_Average
        from First_Segmentation_Gradient
        )t1
        )t2
                                )t3
group by
         Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour

--672 Row(s) affected


--Adding a capping limit of 20 and 120 rule based on the existing rule

Alter table Gradient_Threshold_Seg1_v3 add Capping_Threshold_Grad int;

Update Gradient_Threshold_Seg1_v3  as FSG
set FSG.Capping_Threshold_Grad = case
                                                when FSG.Viewing_Type_Detailed=0 and SN1.Min_Duration is null or SN1.Min_Duration < (20*60) then 20*60
                                                when FSG.Viewing_Type_Detailed=0 and SN1.Min_Duration > (120*60) then (120*60)
                                                else SN1.Min_Duration
                                        end
from Segmentation_1_Ntiles as SN1
inner join Gradient_Threshold_Seg1_v3 as FSG
on SN1.Viewing_Type_Detailed = FSG.Viewing_Type_Detailed
and SN1.EVENT_START_DOW = FSG.EVENT_START_DOW
and SN1.event_start_hour = FSG.event_start_hour
and SN1.ntile = FSG.Threshold_Grad_v3

--507 Row(s) affected

--Add Minimum duration to Gradient_Table
Alter table Gradient_Threshold_Seg1_v3 add Min_Duration int;

Update Gradient_Threshold_Seg1_v3  as FSG
set FSG.Min_Duration = SN1.Min_Duration
from Segmentation_1_Ntiles as SN1
inner join Gradient_Threshold_Seg1_v3 as FSG
on SN1.Viewing_Type_Detailed = FSG.Viewing_Type_Detailed
and SN1.EVENT_START_DOW = FSG.EVENT_START_DOW
and SN1.event_start_hour = FSG.event_start_hour
and SN1.ntile = FSG.Threshold_Grad_v3

--507 Row(s) affected


----First ntile with the above condition on Second_Segmentation Gradient----

select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description
        ,coalesce(min(Threshold_1),max(Threshold_2)) as Threshold_Grad_v3
into    Gradient_Threshold_Seg2_v3
from (
select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description
        ,case when Thresholds >= 160 then Thresholds end as Threshold_1
        ,case when Thresholds < 160 then Thresholds end as Threshold_2
from
        (
select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description
        ,ntile
        ,Second_Gradient
        ,Median_duration
        ,max(Second_Gradient) over( partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,pack_grp,genre_description order by ntile rows between 1 following and 1 following) as next_value
        ,case when (Second_Gradient > Second_Gradient_Average) and ((next_value*-1) > Second_Gradient_Average) then ntile else NULL
         end as Thresholds
from
        (
        select   Viewing_Type_Detailed
                ,EVENT_START_DOW
                ,event_start_hour
                ,pack_grp
                ,genre_description
                ,ntile
                ,Second_Gradient
                ,Median_duration
                ,avg(abs(Second_Gradient)) over(partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,pack_grp,genre_description) as Second_Gradient_Average
        from Second_Segmentation_Gradient
        )t1
        )t2
                ) t3
group by
         Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description

---22,125 Row(s) affected


--Adding a capping limit of 20 and 120 rule based on the existing rule

Alter table Gradient_Threshold_Seg2_v3 add Capping_Threshold_Grad  int;

Update Gradient_Threshold_Seg2_v3  as SSG
set SSG.Capping_Threshold_Grad = case
                                                when SSG.Viewing_Type_Detailed=0 and SN2.Min_Duration is null or SN2.Min_Duration < (20*60) then 20*60
                                                when SSG.Viewing_Type_Detailed=0 and SN2.Min_Duration > (120*60) then (120*60)
                                                else SN2.Min_Duration
                                        end
from Segmentation_2_Ntiles as SN2
inner join Gradient_Threshold_Seg2_v3 as SSG
on SN2.Viewing_Type_Detailed = SSG.Viewing_Type_Detailed
and SN2.EVENT_START_DOW = SSG.EVENT_START_DOW
and SN2.event_start_hour = SSG.event_start_hour
and coalesce(SN2.pack_grp, 'Empty') = coalesce(SSG.pack_grp, 'Empty')
and coalesce(SN2.genre_description, 'Unknown') = coalesce(SSG.genre_description, 'Unknown')
and SN2.ntile = SSG.Threshold_Grad_v3

--17,374 Row(s) affected

---Add minimum duration to Gradient table
Alter table Gradient_Threshold_Seg2_v3 add Min_Duration  int;

Update Gradient_Threshold_Seg2_v3  as SSG
set SSG.Min_Duration = SN2.Min_Duration
from Segmentation_2_Ntiles as SN2
inner join Gradient_Threshold_Seg2_v3 as SSG
on SN2.Viewing_Type_Detailed = SSG.Viewing_Type_Detailed
and SN2.EVENT_START_DOW = SSG.EVENT_START_DOW
and SN2.event_start_hour = SSG.event_start_hour
and coalesce(SN2.pack_grp, 'Empty') = coalesce(SSG.pack_grp, 'Empty')
and coalesce(SN2.genre_description, 'Unknown') = coalesce(SSG.genre_description, 'Unknown')
and SN2.ntile = SSG.Threshold_Grad_v3

--17,374 Row(s) affected

----First ntile with the above condition on Third_Segmentation Gradient----

select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description
        ,coalesce(min(Threshold_1),max(Threshold_2)) as Threshold_Grad_v3
into    Gradient_Threshold_Seg3_v3
from (
select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description
        ,case when Thresholds >= 160 then Thresholds end as Threshold_1
        ,case when Thresholds < 160 then Thresholds end as Threshold_2
from
        (
select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,box_subscription
        ,genre_description
        ,ntile
        ,Second_Gradient
        ,Median_duration
        ,max(Second_Gradient) over( partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,box_subscription,pack_grp,genre_description order by ntile rows between 1 following and 1 following) as next_value
        ,case when (Second_Gradient > Second_Gradient_Average) and ((next_value*-1) > Second_Gradient_Average) then ntile else NULL
         end as Thresholds
from
        (
        select   Viewing_Type_Detailed
                ,EVENT_START_DOW
                ,event_start_hour
                ,box_subscription
                ,pack_grp
                ,genre_description
                ,ntile
                ,Second_Gradient
                ,Median_duration
                ,avg(abs(Second_Gradient)) over(partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,box_subscription,pack_grp,genre_description) as Second_Gradient_Average
        from Third_Segmentation_Gradient
        )t1
        )t2
                ) t3
group by
         Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description
--58,960 Row(s) affected

--Adding a capping limit of 20 and 120 rule based on the existing rule

Alter table Gradient_Threshold_Seg3_v3 add Capping_Threshold_Grad  int;

Update Gradient_Threshold_Seg3_v3  as TSG
set TSG.Capping_Threshold_Grad = case
                                                when TSG.Viewing_Type_Detailed=0 and SN3.Min_Duration is null or SN3.Min_Duration < (20*60) then 20*60
                                                when TSG.Viewing_Type_Detailed=0 and SN3.Min_Duration > (120*60) then (120*60)
                                                else SN3.Min_Duration
                                        end
from Segmentation_3_Ntiles as SN3
inner join Gradient_Threshold_Seg3_v3 as TSG
on SN3.Viewing_Type_Detailed = TSG.Viewing_Type_Detailed
and SN3.EVENT_START_DOW = TSG.EVENT_START_DOW
and SN3.event_start_hour = TSG.event_start_hour
and SN3.box_subscription = TSG.box_subscription
and coalesce(SN3.pack_grp, 'Empty') = Coalesce(TSG.pack_grp, 'Empty')
and coalesce(SN3.genre_description, 'Unknown') = coalesce(TSG.genre_description, 'Unknown')
and SN3.ntile = TSG.Threshold_Grad_v3

--42,128 Row(s) affected

--Add Minimum duration to Gradient table

Alter table Gradient_Threshold_Seg3_v3 add Min_Duration  int;

Update Gradient_Threshold_Seg3_v3  as TSG
set TSG.Min_Duration = SN3.Min_Duration
from Segmentation_3_Ntiles as SN3
inner join Gradient_Threshold_Seg3_v3 as TSG
on SN3.Viewing_Type_Detailed = TSG.Viewing_Type_Detailed
and SN3.EVENT_START_DOW = TSG.EVENT_START_DOW
and SN3.event_start_hour = TSG.event_start_hour
and SN3.box_subscription = TSG.box_subscription
and coalesce(SN3.pack_grp, 'Empty') = Coalesce(TSG.pack_grp, 'Empty')
and coalesce(SN3.genre_description, 'Unknown') = coalesce(TSG.genre_description, 'Unknown')
and SN3.ntile = TSG.Threshold_Grad_v3

--42,128 Row(s) affected


                                --- Fourth version of Dynamic Capping ---

/*

1. For each segment find the second gradient that is greater than zero and
is followed by a negative value. Then select the minimum of the absolute value between the two. The resulting ntile is our target.

*/

----First ntile with the above condition on First_Segmentation Gradient----


select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,ntile
        ,Second_Gradient
        ,Median_duration
        ,max(Second_Gradient) over( partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour order by ntile rows between 1 following and 1 following) as next_value
         ,case
                when (Second_Gradient > 0) and (next_value < 0)
                then
                        case
                                when abs(Second_Gradient) > abs(next_value)
                                then next_value
                                else Second_Gradient
                                end
                        else null

         end as Change
into Gradient_Threshold_Seg1_v4_PART1
from
        (
        select   Viewing_Type_Detailed
                ,EVENT_START_DOW
                ,event_start_hour
                ,ntile
                ,Second_Gradient
                ,Median_duration
        from First_Segmentation_Gradient
        )t1
---134,400 Row(s) affected

select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,max(abs(Change)) as Max_Abs_Change
        ,max(Change) as Max_Change
        ,case when  Max_Abs_Change = Max_Change then Max_Change else -Max_Abs_Change end as Maximum_Change
into     Gradient_Threshold_Seg1_v4_PART2
from Gradient_Threshold_Seg1_v4_PART1
where Median_duration <7200
group by
         Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
--672 Row(s) affected



--selecting the resulting ntile as threashold from the two tables above---
select   GTS1.Viewing_Type_Detailed
        ,GTS1.EVENT_START_DOW
        ,GTS1.event_start_hour
        ,min(GTS1.ntile)as Threshold_Grad_v4
into    Gradient_Threshold_Seg1_v4
from     Gradient_Threshold_Seg1_v4_PART1 as GTS1
inner join Gradient_Threshold_Seg1_v4_PART2 as NES
on NES.Viewing_Type_Detailed = GTS1.Viewing_Type_Detailed
and NES.EVENT_START_DOW = GTS1.EVENT_START_DOW
and NES.event_start_hour = GTS1.event_start_hour
and abs(NES.Maximum_Change) = abs(GTS1.Change)
group by GTS1.Viewing_Type_Detailed
        ,GTS1.EVENT_START_DOW
        ,GTS1.event_start_hour

--672 Row(s) affected

--Add Minimum duration to Gradient_Table
Alter table Gradient_Threshold_Seg1_v4 add Min_Duration int;

Update Gradient_Threshold_Seg1_v4 as FSG
set FSG.min_duration = SN1.min_duration
from Segmentation_1_Ntiles as SN1
inner join Gradient_Threshold_Seg1_v4 as FSG
on SN1.Viewing_Type_Detailed = FSG.Viewing_Type_Detailed
and SN1.EVENT_START_DOW = FSG.EVENT_START_DOW
and SN1.event_start_hour = FSG.event_start_hour
and SN1.ntile = FSG.Threshold_Grad_v4

--672 Row(s) affected

--Adding a capping limit of 20 and 120 rule based on the existing rule

Alter table Gradient_Threshold_Seg1_v4 add Capping_Threshold_Grad int;

Update Gradient_Threshold_Seg1_v4  as FSG
set FSG.Capping_Threshold_Grad = case
                                                when FSG.Viewing_Type_Detailed=0 and SN1.Min_Duration is null or SN1.Min_Duration < (20*60) then 20*60
                                                when FSG.Viewing_Type_Detailed=0 and SN1.Min_Duration > (120*60) then (120*60)
                                                else SN1.Min_Duration
                                        end
from Segmentation_1_Ntiles as SN1
inner join Gradient_Threshold_Seg1_v4 as FSG
on SN1.Viewing_Type_Detailed = FSG.Viewing_Type_Detailed
and SN1.EVENT_START_DOW = FSG.EVENT_START_DOW
and SN1.event_start_hour = FSG.event_start_hour
and SN1.ntile = FSG.Threshold_Grad_v4

----First ntile with the above condition on Second_Segmentation Gradient----


select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description
        ,ntile
        ,Second_Gradient
        ,Median_duration
        ,max(Second_Gradient) over( partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,pack_grp,genre_description order by ntile rows between 1 following and 1 following) as next_value
         ,case
                when (Second_Gradient > 0) and (next_value < 0)
                then
                        case
                                when abs(Second_Gradient) > abs(next_value)
                                then next_value
                                else Second_Gradient
                                end
                        else null

         end as Change
into Gradient_Threshold_Seg2_v4_PART1
from
        (
        select   Viewing_Type_Detailed
                ,EVENT_START_DOW
                ,event_start_hour
                ,pack_grp
                ,genre_description
                ,ntile
                ,Second_Gradient
                ,Median_duration
        from Second_Segmentation_Gradient
        )t1
---2,982,023 Row(s) affected


select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description
        ,max(abs(Change)) as Max_Abs_Change
        ,max(Change) as Max_Change
        ,case when  Max_Abs_Change = Max_Change then Max_Change else -Max_Abs_Change end as Maximum_Change
into     Gradient_Threshold_Seg2_v4_PART2
from Gradient_Threshold_Seg2_v4_PART1
group by
         Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description
--22,125 Row(s) affected




--selecting the resulting ntile as threashold from the two tables above---
select   GTS2.Viewing_Type_Detailed
        ,GTS2.EVENT_START_DOW
        ,GTS2.event_start_hour
        ,GTS2.pack_grp
        ,GTS2.genre_description
        ,min(GTS2.ntile)as Threshold_Grad_v4
into     Gradient_Threshold_Seg2_v4
from     Gradient_Threshold_Seg2_v4_PART1 as GTS2
inner join Gradient_Threshold_Seg2_v4_PART2 as NES
on NES.Viewing_Type_Detailed = GTS2.Viewing_Type_Detailed
and NES.EVENT_START_DOW = GTS2.EVENT_START_DOW
and NES.event_start_hour = GTS2.event_start_hour
and coalesce(NES.pack_grp, 'Empty') = coalesce(GTS2.pack_grp, 'Empty')
and coalesce(NES.genre_description, 'Unknown') = coalesce(GTS2.genre_description, 'Unknown')
and coalesce(abs(NES.Maximum_Change),1) = coalesce(abs(GTS2.Change),1)
group by GTS2.Viewing_Type_Detailed
        ,GTS2.EVENT_START_DOW
        ,GTS2.event_start_hour
        ,GTS2.pack_grp
        ,GTS2.genre_description
--22,125 Row(s) affected

--Add Minimum duration to Gradient_Table
Alter table Gradient_Threshold_Seg2_v4 add Min_Duration int;

Update Gradient_Threshold_Seg2_v4  as FSG
set FSG.min_duration = SN2.min_duration
from Segmentation_2_Ntiles as SN2
inner join Gradient_Threshold_Seg2_v4 as FSG
on SN2.Viewing_Type_Detailed = FSG.Viewing_Type_Detailed
and SN2.EVENT_START_DOW = FSG.EVENT_START_DOW
and SN2.event_start_hour = FSG.event_start_hour
and coalesce(SN2.pack_grp, 'Empty') = coalesce(FSG.pack_grp, 'Empty')
and coalesce(SN2.genre_description, 'Unknown') = coalesce(FSG.genre_description, 'Unknown')
and SN2.ntile = FSG.Threshold_Grad_v4
--22,125 Row(s) affected

--Adding a capping limit of 20 and 120 rule based on the existing rule

Alter table Gradient_Threshold_Seg2_v4 add Capping_Threshold_Grad int;

Update Gradient_Threshold_Seg2_v4  as FSG
set FSG.Capping_Threshold_Grad = case
                                                when FSG.Viewing_Type_Detailed=0 and SN2.Min_Duration is null or SN2.Min_Duration < (20*60) then 20*60
                                                when FSG.Viewing_Type_Detailed=0 and SN2.Min_Duration > (120*60) then (120*60)
                                                else SN2.Min_Duration
                                        end
from Segmentation_2_Ntiles as SN2
inner join Gradient_Threshold_Seg2_v4 as FSG
on SN2.Viewing_Type_Detailed = FSG.Viewing_Type_Detailed
and SN2.EVENT_START_DOW = FSG.EVENT_START_DOW
and SN2.event_start_hour = FSG.event_start_hour
and coalesce(SN2.pack_grp, 'Empty') = coalesce(FSG.pack_grp, 'Empty')
and coalesce(SN2.genre_description, 'Unknown') = coalesce(FSG.genre_description, 'Unknown')
and SN2.ntile = FSG.Threshold_Grad_v4
--22,125 Row(s) affected

----First ntile with the above condition on Third_Segmentation Gradient----


select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description
        ,ntile
        ,Second_Gradient
        ,Median_duration
        ,max(Second_Gradient) over( partition by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour,box_subscription,pack_grp,genre_description order by ntile rows between 1 following and 1 following) as next_value
         ,case
                when (Second_Gradient > 0) and (next_value < 0)
                then
                        case
                                when abs(Second_Gradient) > abs(next_value)
                                then next_value
                                else Second_Gradient
                                end
                        else null

         end as Change
into Gradient_Threshold_Seg3_v4_PART1
from
        (
        select   Viewing_Type_Detailed
                ,EVENT_START_DOW
                ,event_start_hour
                ,box_subscription
                ,pack_grp
                ,genre_description
                ,ntile
                ,Second_Gradient
                ,Median_duration
        from Third_Segmentation_Gradient
        )t1
--6,167,547 Row(s) affected


select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description
        ,max(abs(Change)) as Max_Abs_Change
        ,max(Change) as Max_Change
        ,case when  Max_Abs_Change = Max_Change then Max_Change else -Max_Abs_Change end as Maximum_Change
into     Gradient_Threshold_Seg3_v4_PART2
from Gradient_Threshold_Seg3_v4_PART1
group by
         Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description
--58,960 Row(s) affected



--selecting the resulting ntile as threashold from the two tables above---
select   GTS3.Viewing_Type_Detailed
        ,GTS3.EVENT_START_DOW
        ,GTS3.event_start_hour
        ,GTS3.box_subscription
        ,GTS3.pack_grp
        ,GTS3.genre_description
        ,min(GTS3.ntile)as Threshold_Grad_v4
into     Gradient_Threshold_Seg3_v4
from     Gradient_Threshold_Seg3_v4_PART1 as GTS3
inner join Gradient_Threshold_Seg3_v4_PART2 as NES
on NES.Viewing_Type_Detailed = GTS3.Viewing_Type_Detailed
and NES.EVENT_START_DOW = GTS3.EVENT_START_DOW
and NES.event_start_hour = GTS3.event_start_hour
and NES.box_subscription = GTS3.box_subscription
and coalesce(NES.pack_grp, 'Empty') = coalesce(GTS3.pack_grp, 'Empty')
and coalesce(NES.genre_description, 'Unknown') = coalesce(GTS3.genre_description, 'Unknown')
and coalesce(abs(NES.Maximum_Change),1) = coalesce(abs(GTS3.Change),1)
group by GTS3.Viewing_Type_Detailed
        ,GTS3.EVENT_START_DOW
        ,GTS3.event_start_hour
        ,GTS3.box_subscription
        ,GTS3.pack_grp
        ,GTS3.genre_description

--58,960 Row(s) affected


--Add Minimum duration to Gradient_Table
Alter table Gradient_Threshold_Seg3_v4 add Min_Duration int;

Update Gradient_Threshold_Seg3_v4  as FSG
set FSG.min_duration = SN3.min_duration
from Segmentation_3_Ntiles as SN3
inner join Gradient_Threshold_Seg3_v4 as FSG
on SN3.Viewing_Type_Detailed = FSG.Viewing_Type_Detailed
and SN3.EVENT_START_DOW = FSG.EVENT_START_DOW
and SN3.event_start_hour = FSG.event_start_hour
and SN3.box_subscription = FSG.box_subscription
and coalesce(SN3.pack_grp, 'Empty') = coalesce(FSG.pack_grp, 'Empty')
and coalesce(SN3.genre_description, 'Unknown') = coalesce(FSG.genre_description, 'Unknown')
and SN3.ntile = FSG.Threshold_Grad_v4

--58,960 Row(s) affected

--Adding a capping limit of 20 and 120 rule based on the existing rule

Alter table Gradient_Threshold_Seg3_v4 add Capping_Threshold_Grad int;

Update Gradient_Threshold_Seg3_v4  as FSG
set FSG.Capping_Threshold_Grad = case
                                                when FSG.Viewing_Type_Detailed=0 and SN3.Min_Duration is null or SN3.Min_Duration < (20*60) then 20*60
                                                when FSG.Viewing_Type_Detailed=0 and SN3.Min_Duration > (120*60) then (120*60)
                                                else SN3.Min_Duration
                                        end
from Segmentation_3_Ntiles as SN3
inner join Gradient_Threshold_Seg3_v4 as FSG
on SN3.Viewing_Type_Detailed = FSG.Viewing_Type_Detailed
and SN3.EVENT_START_DOW = FSG.EVENT_START_DOW
and SN3.event_start_hour = FSG.event_start_hour
and SN3.box_subscription = FSG.box_subscription
and coalesce(SN3.pack_grp, 'Empty') = coalesce(FSG.pack_grp, 'Empty')
and coalesce(SN3.genre_description, 'Unknown') = coalesce(FSG.genre_description, 'Unknown')
and SN3.ntile = FSG.Threshold_Grad_v4

--58,960 Row(s) affected


/* Calculating the number of events per segments -----*/

------Segment_1--------------

select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,count(*)as Num
into     Num_Events_Seg_1
from     VEA_5_11Nov_Viewing_Events
group by Viewing_Type_Detailed,EVENT_START_DOW,event_start_hour


------Segment_2--------------

select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description
        ,count(*)as Num
into     Num_Events_Seg_2
from     VEA_5_11Nov_Viewing_Events
group by Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description


------Segment_3--------------

select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description
        ,count(*)as Num
into     Num_Events_Seg_3
from     VEA_5_11Nov_Viewing_Events
group by Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description;

------------------------------------------------------

