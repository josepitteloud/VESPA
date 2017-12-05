/* Implementing the existing capping rules on the data used for calculating Gradient */

-- Segment 1
select Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,198 as Threshold_Curr
into    Current_Threshold_Seg1
from Segmentation_1_Ntiles
where Viewing_Type_Detailed <> 0
group by Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
---504 Row(s) affected

-- Segment 2
select Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description
        ,min(Threshold) as Threshold_Curr
into    Current_Threshold_Seg2
from (
select Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description
        ,Median_duration
        ,case
            when event_start_hour in (20,21,22) and (median_duration/60) >= ((23-event_start_hour-1)*60+122) then ntile
            when event_start_hour in (23,0,1,2,3) and (median_duration/60) >= 122 then ntile
            else NULL
        end as ntile_meet_criteria
        -- Obtain first ntile where the duration is greater or equal to the value we said above we wanted to be
        ,min(ntile_meet_criteria) over (partition by Viewing_Type_Detailed
                                        ,EVENT_START_DOW
                                        ,event_start_hour
                                        ,pack_grp
                                        ,genre_description)
        as min_ntile
        ,max(ntile) over (partition by Viewing_Type_Detailed
                                        ,EVENT_START_DOW
                                        ,event_start_hour
                                        ,pack_grp
                                        ,genre_description)
        as max_ntile
        ,coalesce(min_ntile,max_ntile-20) as Threshold
from Segmentation_2_Ntiles
where Viewing_Type_Detailed = 0
and event_start_hour in (20,21,22,23,0,1,2,3)
) t
group by Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description
---1,590 Row(s) affected

-- Segment 3

select Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description
        ,min(Threshold) as Threshold_Curr
into    Current_Threshold_Seg3
from
(
select Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description
        ,case
            when event_start_hour between 4 and 14 and (median_duration) >= (243*60) then ntile-2
            when event_start_hour between 15 and 19 then (max_ntile-2)
            else NULL
        end as ntile_meet_criteria
        ,min(ntile_meet_criteria) over (partition by Viewing_Type_Detailed
                                        ,EVENT_START_DOW
                                        ,event_start_hour
                                        ,box_subscription
                                        ,pack_grp
                                        ,genre_description)
        as min_ntile
        ,coalesce(min_ntile,max_ntile-25) as Threshold
from (
select  Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description
        ,Median_duration
        ,ntile
        ,max(ntile) over (partition by Viewing_Type_Detailed
                                        ,EVENT_START_DOW
                                        ,event_start_hour
                                        ,box_subscription
                                        ,pack_grp
                                        ,genre_description)
        as max_ntile
from Segmentation_3_Ntiles
where Viewing_Type_Detailed = 0
and event_start_hour between 4 and 19
) t1
    ) t2
group by Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description
---9,625 Row(s) affected


--Joining the Minimum duration to the Current Threshold Tables
---Segment_1

Alter table Current_Threshold_Seg1 add Min_Duration int;
Alter table Current_Threshold_Seg1 add Capping_Threshold int;

Update Current_Threshold_Seg1  as FSG
set FSG.Min_Duration = SN1.Min_Duration
,FSG.Capping_Threshold = case
                                                when FSG.Viewing_Type_Detailed=0 and SN1.Min_Duration is null or SN1.Min_Duration < (20*60) then 20*60
                                                when FSG.Viewing_Type_Detailed=0 and SN1.Min_Duration > (120*60) then (120*60)
                                                else SN1.Min_Duration
                                        end
from Segmentation_1_Ntiles as SN1
inner join Current_Threshold_Seg1 as FSG
on SN1.Viewing_Type_Detailed = FSG.Viewing_Type_Detailed
and SN1.EVENT_START_DOW = FSG.EVENT_START_DOW
and SN1.event_start_hour = FSG.event_start_hour
and SN1.ntile = FSG.Threshold_Curr

---Segment_2

Alter table Current_Threshold_Seg2 add Min_Duration int;
Alter table Current_Threshold_Seg2 add Capping_Threshold  int;

Update Current_Threshold_Seg2  as SSG
set SSG.Min_Duration=SN2.Min_Duration
,SSG.Capping_Threshold = case
                                                when SSG.Viewing_Type_Detailed=0 and SN2.Min_Duration is null or SN2.Min_Duration < (20*60) then 20*60
                                                when SSG.Viewing_Type_Detailed=0 and SN2.Min_Duration > (120*60) then (120*60)
                                                else SN2.Min_Duration
                                        end
from Segmentation_2_Ntiles as SN2
inner join Current_Threshold_Seg2 as SSG
on SN2.Viewing_Type_Detailed = SSG.Viewing_Type_Detailed
and SN2.EVENT_START_DOW = SSG.EVENT_START_DOW
and SN2.event_start_hour = SSG.event_start_hour
and coalesce(SN2.pack_grp, 'empty') = coalesce(SSG.pack_grp, 'empty')
and coalesce(SN2.genre_description,'Unknown') = coalesce(SSG.genre_description,'Unknown')
and SN2.ntile = SSG.Threshold_Curr


---Segment_3


Alter table Current_Threshold_Seg3 add Min_Duration int;
Alter table Current_Threshold_Seg3 add Capping_Threshold  int;

Update Current_Threshold_Seg3  as TSG
set TSG.Min_Duration=SN3.Min_Duration
,TSG.Capping_Threshold = case
                                                when TSG.Viewing_Type_Detailed=0 and SN3.Min_Duration is null or SN3.Min_Duration < (20*60) then 20*60
                                                when TSG.Viewing_Type_Detailed=0 and SN3.Min_Duration > (120*60) then (120*60)
                                                else SN3.Min_Duration
                                        end
from Segmentation_3_Ntiles as SN3
inner join Current_Threshold_Seg3 as TSG
on SN3.Viewing_Type_Detailed = TSG.Viewing_Type_Detailed
and SN3.EVENT_START_DOW = TSG.EVENT_START_DOW
and SN3.event_start_hour = TSG.event_start_hour
and SN3.box_subscription = TSG.box_subscription
and coalesce(SN3.pack_grp, 'empty') = coalesce(TSG.pack_grp, 'empty')
and coalesce(SN3.genre_description,'Unknown') = coalesce(TSG.genre_description,'Unknown')
and SN3.ntile = TSG.Threshold_Curr

---Checking the difference between the existing capping rules and the new ones

--Version 1

---Segment_1 --

select   GTS1.Viewing_Type_Detailed
        ,GTS1.EVENT_START_DOW
        ,GTS1.event_start_hour
        ,GTS1.Threshold_Grad
        ,CTS1.Threshold_Curr
        ,GTS1.Capping_Threshold_Grad
        ,CTS1.Capping_Threshold
        ,NES.Number_Events
        ,GTS1.Threshold_Grad-CTS1.Threshold_Curr as Difference_Ntiles_Threshold
        ,GTS1.Capping_Threshold_Grad-CTS1.Capping_Threshold as Difference_Capping_Duration
into     Seg1_Comparison_v1
from     Gradient_Threshold_Seg1_v1 as GTS1
inner join Current_Threshold_Seg1 as CTS1
on GTS1.Viewing_Type_Detailed = CTS1.Viewing_Type_Detailed
and GTS1.EVENT_START_DOW = CTS1.EVENT_START_DOW
and GTS1.event_start_hour = CTS1.event_start_hour
inner join Num_Events_Seg_1 as NES
on NES.Viewing_Type_Detailed = CTS1.Viewing_Type_Detailed
and NES.EVENT_START_DOW = CTS1.EVENT_START_DOW
and NES.event_start_hour = CTS1.event_start_hour

---504 Row(s) affected

---Segment_2
select   GTS2.Viewing_Type_Detailed
        ,GTS2.EVENT_START_DOW
        ,GTS2.event_start_hour
        ,GTS2.pack_grp
        ,GTS2.genre_description
        ,GTS2.Threshold_Grad
        ,CTS2.Threshold_Curr
        ,GTS2.Capping_Threshold_Grad
        ,CTS2.Capping_Threshold
        ,NES.Number_Events
        ,GTS2.Threshold_Grad-CTS2.Threshold_Curr as Difference_Ntiles_Threshold
        ,GTS2.Capping_Threshold_Grad-CTS2.Capping_Threshold as Difference_Capping_Duration
into     Seg2_Comparison_v1
from     Gradient_Threshold_Seg2_v1 as GTS2
inner join Current_Threshold_Seg2 as CTS2
on GTS2.Viewing_Type_Detailed = CTS2.Viewing_Type_Detailed
and GTS2.EVENT_START_DOW = CTS2.EVENT_START_DOW
and GTS2.event_start_hour = CTS2.event_start_hour
and coalesce(GTS2.pack_grp,'Empty')= coalesce(CTS2.pack_grp,'Empty')
and coalesce(GTS2.genre_description, 'Unknown')= coalesce(CTS2.genre_description, 'Unknown')
inner join Num_Events_seg_2 as NES
on NES.Viewing_Type_Detailed = CTS2.Viewing_Type_Detailed
and NES.EVENT_START_DOW = CTS2.EVENT_START_DOW
and NES.event_start_hour = CTS2.event_start_hour
and coalesce(NES.pack_grp, 'Empty')= coalesce(CTS2.pack_grp, 'Empty')
and coalesce(NES.genre_description, 'Unknown') = coalesce(CTS2.genre_description, 'Unknown')

---1,590 Row(s) affected



---Segment_3

select   GTS3.Viewing_Type_Detailed
        ,GTS3.EVENT_START_DOW
        ,GTS3.event_start_hour
        ,GTS3.box_subscription
        ,GTS3.pack_grp
        ,GTS3.genre_description
        ,GTS3.Threshold_Grad
        ,CTS3.Threshold_Curr
        ,GTS3.Capping_Threshold_Grad
        ,CTS3.Capping_Threshold
        ,NES.Number_Events
        ,GTS3.Threshold_Grad-CTS3.Threshold_Curr as Difference_Ntiles_Threshold
        ,GTS3.Capping_Threshold_Grad-CTS3.Capping_Threshold as Difference_Capping_Duration
into     Seg3_Comparison_v1
from     Gradient_Threshold_Seg3_v1 as GTS3
inner join Current_Threshold_Seg3 as CTS3
on GTS3.Viewing_Type_Detailed = CTS3.Viewing_Type_Detailed
and GTS3.EVENT_START_DOW = CTS3.EVENT_START_DOW
and GTS3.event_start_hour = CTS3.event_start_hour
and GTS3.box_subscription = CTS3.box_subscription
and coalesce(GTS3.pack_grp,'Empty')= coalesce(CTS3.pack_grp,'Empty')
and coalesce(GTS3.genre_description, 'Unknown')= coalesce(CTS3.genre_description, 'Unknown')
inner join Num_Events_seg_3 as NES
on NES.Viewing_Type_Detailed = CTS3.Viewing_Type_Detailed
and NES.EVENT_START_DOW = CTS3.EVENT_START_DOW
and NES.event_start_hour = CTS3.event_start_hour
and NES.box_subscription = CTS3.box_subscription
and coalesce(NES.pack_grp, 'Empty')= coalesce(CTS3.pack_grp, 'Empty')
and coalesce(NES.genre_description, 'Unknown') = coalesce(CTS3.genre_description, 'Unknown')

--9,625 Row(s) affected

--Version 2

--Segment_1

select   GTS1.Viewing_Type_Detailed
        ,GTS1.EVENT_START_DOW
        ,GTS1.event_start_hour
        ,GTS1.Threshold_Grad_v2
        ,CTS1.Threshold_Curr
        ,GTS1.Capping_Threshold_Grad
        ,CTS1.Capping_Threshold
        ,NES.Number_Events
        ,GTS1.Threshold_Grad_v2-CTS1.Threshold_Curr as Difference_Ntiles_Threshold
        ,GTS1.Capping_Threshold_Grad-CTS1.Capping_Threshold as Difference_Capping_Duration
into     Seg1_Comparison_v2
from     Gradient_Threshold_Seg1_v2 as GTS1
inner join Current_Threshold_Seg1 as CTS1
on GTS1.Viewing_Type_Detailed = CTS1.Viewing_Type_Detailed
and GTS1.EVENT_START_DOW = CTS1.EVENT_START_DOW
and GTS1.event_start_hour = CTS1.event_start_hour
inner join Num_Events_Seg_1 as NES
on NES.Viewing_Type_Detailed = CTS1.Viewing_Type_Detailed
and NES.EVENT_START_DOW = CTS1.EVENT_START_DOW
and NES.event_start_hour = CTS1.event_start_hour

--504 Row(s) affected
---Segment_2

select   GTS2.Viewing_Type_Detailed
        ,GTS2.EVENT_START_DOW
        ,GTS2.event_start_hour
        ,GTS2.pack_grp
        ,GTS2.genre_description
        ,GTS2.Threshold_Grad_v2
        ,CTS2.Threshold_Curr
        ,GTS2.Capping_Threshold_Grad
        ,CTS2.Capping_Threshold
        ,NES.Number_Events
        ,GTS2.Threshold_Grad_v2-CTS2.Threshold_Curr as Difference_Ntiles_Threshold
        ,GTS2.Capping_Threshold_Grad-CTS2.Capping_Threshold as Difference_Capping_Duration
into     Seg2_Comparison_v2
from     Gradient_Threshold_Seg2_v2 as GTS2
inner join Current_Threshold_Seg2 as CTS2
on GTS2.Viewing_Type_Detailed = CTS2.Viewing_Type_Detailed
and GTS2.EVENT_START_DOW = CTS2.EVENT_START_DOW
and GTS2.event_start_hour = CTS2.event_start_hour
and coalesce(GTS2.pack_grp,'Empty')= coalesce(CTS2.pack_grp,'Empty')
and coalesce(GTS2.genre_description, 'Unknown')= coalesce(CTS2.genre_description, 'Unknown')
inner join Num_Events_seg_2 as NES
on NES.Viewing_Type_Detailed = CTS2.Viewing_Type_Detailed
and NES.EVENT_START_DOW = CTS2.EVENT_START_DOW
and NES.event_start_hour = CTS2.event_start_hour
and coalesce(NES.pack_grp, 'Empty')= coalesce(CTS2.pack_grp, 'Empty')
and coalesce(NES.genre_description, 'Unknown') = coalesce(CTS2.genre_description, 'Unknown')

---1,590 Row(s) affected

---Segment_3

select   GTS3.Viewing_Type_Detailed
        ,GTS3.EVENT_START_DOW
        ,GTS3.event_start_hour
        ,GTS3.box_subscription
        ,GTS3.pack_grp
        ,GTS3.genre_description
        ,GTS3.Threshold_Grad_v2
        ,CTS3.Threshold_Curr
        ,GTS3.Capping_Threshold_Grad
        ,CTS3.Capping_Threshold
        ,NES.Number_Events
        ,GTS3.Threshold_Grad_v2-CTS3.Threshold_Curr as Difference_Ntiles_Threshold
        ,GTS3.Capping_Threshold_Grad-CTS3.Capping_Threshold as Difference_Capping_Duration
into     Seg3_Comparison_v2
from     Gradient_Threshold_Seg3_v2 as GTS3
inner join Current_Threshold_Seg3 as CTS3
on GTS3.Viewing_Type_Detailed = CTS3.Viewing_Type_Detailed
and GTS3.EVENT_START_DOW = CTS3.EVENT_START_DOW
and GTS3.event_start_hour = CTS3.event_start_hour
and GTS3.box_subscription = CTS3.box_subscription
and coalesce(GTS3.pack_grp,'Empty')= coalesce(CTS3.pack_grp,'Empty')
and coalesce(GTS3.genre_description, 'Unknown')= coalesce(CTS3.genre_description, 'Unknown')
inner join Num_Events_seg_3 as NES
on NES.Viewing_Type_Detailed = CTS3.Viewing_Type_Detailed
and NES.EVENT_START_DOW = CTS3.EVENT_START_DOW
and NES.event_start_hour = CTS3.event_start_hour
and NES.box_subscription = CTS3.box_subscription
and coalesce(NES.pack_grp, 'Empty')= coalesce(CTS3.pack_grp, 'Empty')
and coalesce(NES.genre_description, 'Unknown') = coalesce(CTS3.genre_description, 'Unknown')

---9,625 Row(s) affected

--Version 3

---Segment_1

select   GTS1.Viewing_Type_Detailed
        ,GTS1.EVENT_START_DOW
        ,GTS1.event_start_hour
        ,GTS1.Threshold_Grad_v3
        ,CTS1.Threshold_Curr
        ,GTS1.Capping_Threshold_Grad
        ,CTS1.Capping_Threshold
        ,NES.Number_Events
        ,GTS1.Threshold_Grad_v3-CTS1.Threshold_Curr as Difference_Ntiles_Threshold
        ,GTS1.Capping_Threshold_Grad-CTS1.Capping_Threshold as Difference_Capping_Duration
into     Seg1_Comparison_v3
from     Gradient_Threshold_Seg1_v3 as GTS1
inner join Current_Threshold_Seg1 as CTS1
on GTS1.Viewing_Type_Detailed = CTS1.Viewing_Type_Detailed
and GTS1.EVENT_START_DOW = CTS1.EVENT_START_DOW
and GTS1.event_start_hour = CTS1.event_start_hour
inner join Num_Events_Seg_1 as NES
on NES.Viewing_Type_Detailed = CTS1.Viewing_Type_Detailed
and NES.EVENT_START_DOW = CTS1.EVENT_START_DOW
and NES.event_start_hour = CTS1.event_start_hour

---504 Row(s) affected

---Segment_2
select   GTS2.Viewing_Type_Detailed
        ,GTS2.EVENT_START_DOW
        ,GTS2.event_start_hour
        ,GTS2.pack_grp
        ,GTS2.genre_description
        ,GTS2.Threshold_Grad_v3
        ,CTS2.Threshold_Curr
        ,GTS2.Capping_Threshold_Grad
        ,CTS2.Capping_Threshold
        ,NES.Number_Events
        ,GTS2.Threshold_Grad_v3-CTS2.Threshold_Curr as Difference_Ntiles_Threshold
        ,GTS2.Capping_Threshold_Grad-CTS2.Capping_Threshold as Difference_Capping_Duration
into    Seg2_Comparison_v3
from    Gradient_Threshold_Seg2_v3 as GTS2
inner join Current_Threshold_Seg2 as CTS2
on GTS2.Viewing_Type_Detailed = CTS2.Viewing_Type_Detailed
and GTS2.EVENT_START_DOW = CTS2.EVENT_START_DOW
and GTS2.event_start_hour = CTS2.event_start_hour
and coalesce(GTS2.pack_grp,'Empty')= coalesce(CTS2.pack_grp,'Empty')
and coalesce(GTS2.genre_description, 'Unknown')= coalesce(CTS2.genre_description, 'Unknown')
inner join Num_Events_seg_2 as NES
on NES.Viewing_Type_Detailed = CTS2.Viewing_Type_Detailed
and NES.EVENT_START_DOW = CTS2.EVENT_START_DOW
and NES.event_start_hour = CTS2.event_start_hour
and coalesce(NES.pack_grp, 'Empty')= coalesce(CTS2.pack_grp, 'Empty')
and coalesce(NES.genre_description, 'Unknown') = coalesce(CTS2.genre_description, 'Unknown')

---1,590 Row(s) affected



---Segment_3

select   GTS3.Viewing_Type_Detailed
        ,GTS3.EVENT_START_DOW
        ,GTS3.event_start_hour
        ,GTS3.box_subscription
        ,GTS3.pack_grp
        ,GTS3.genre_description
        ,GTS3.Threshold_Grad_v3
        ,CTS3.Threshold_Curr
        ,GTS3.Capping_Threshold_Grad
        ,CTS3.Capping_Threshold
        ,NES.Number_Events
        ,GTS3.Threshold_Grad_v3-CTS3.Threshold_Curr as Difference_Ntiles_Threshold
        ,GTS3.Capping_Threshold_Grad-CTS3.Capping_Threshold as Difference_Capping_Duration
into     Seg3_Comparison_v3
from     Gradient_Threshold_Seg3_v3 as GTS3
inner join Current_Threshold_Seg3 as CTS3
on GTS3.Viewing_Type_Detailed = CTS3.Viewing_Type_Detailed
and GTS3.EVENT_START_DOW = CTS3.EVENT_START_DOW
and GTS3.event_start_hour = CTS3.event_start_hour
and GTS3.box_subscription = CTS3.box_subscription
and coalesce(GTS3.pack_grp,'Empty')= coalesce(CTS3.pack_grp,'Empty')
and coalesce(GTS3.genre_description, 'Unknown')= coalesce(CTS3.genre_description, 'Unknown')
inner join Num_Events_seg_3 as NES
on NES.Viewing_Type_Detailed = CTS3.Viewing_Type_Detailed
and NES.EVENT_START_DOW = CTS3.EVENT_START_DOW
and NES.event_start_hour = CTS3.event_start_hour
and NES.box_subscription = CTS3.box_subscription
and coalesce(NES.pack_grp, 'Empty')= coalesce(CTS3.pack_grp, 'Empty')
and coalesce(NES.genre_description, 'Unknown') = coalesce(CTS3.genre_description, 'Unknown')

--9,625 Row(s) affected

--Version 4

---Segment_1

select   GTS1.Viewing_Type_Detailed
        ,GTS1.EVENT_START_DOW
        ,GTS1.event_start_hour
        ,GTS1.Threshold_Grad_v4
        ,CTS1.Threshold_Curr
        ,GTS1.Capping_Threshold_Grad
        ,CTS1.Capping_Threshold
        ,NES.Number_Events
        ,GTS1.Threshold_Grad_v4-CTS1.Threshold_Curr as Difference_Ntiles_Threshold
        ,GTS1.Capping_Threshold_Grad-CTS1.Capping_Threshold as Difference_Capping_Duration
into    Seg1_Comparison_v4
from     Gradient_Threshold_Seg1_v4 as GTS1
inner join Current_Threshold_Seg1 as CTS1
on GTS1.Viewing_Type_Detailed = CTS1.Viewing_Type_Detailed
and GTS1.EVENT_START_DOW = CTS1.EVENT_START_DOW
and GTS1.event_start_hour = CTS1.event_start_hour
inner join Num_Events_Seg_1 as NES
on NES.Viewing_Type_Detailed = CTS1.Viewing_Type_Detailed
and NES.EVENT_START_DOW = CTS1.EVENT_START_DOW
and NES.event_start_hour = CTS1.event_start_hour

---504 Row(s) affected

---Segment_2
select   GTS2.Viewing_Type_Detailed
        ,GTS2.EVENT_START_DOW
        ,GTS2.event_start_hour
        ,GTS2.pack_grp
        ,GTS2.genre_description
        ,GTS2.Threshold_Grad_v4
        ,CTS2.Threshold_Curr
        ,GTS2.Capping_Threshold_Grad
        ,CTS2.Capping_Threshold
        ,NES.Number_Events
        ,GTS2.Threshold_Grad_v4-CTS2.Threshold_Curr as Difference_Ntiles_Threshold
        ,GTS2.Capping_Threshold_Grad-CTS2.Capping_Threshold as Difference_Capping_Duration
into     Seg2_Comparison_v4
from     Gradient_Threshold_Seg2_v4 as GTS2
inner join Current_Threshold_Seg2 as CTS2
on GTS2.Viewing_Type_Detailed = CTS2.Viewing_Type_Detailed
and GTS2.EVENT_START_DOW = CTS2.EVENT_START_DOW
and GTS2.event_start_hour = CTS2.event_start_hour
and coalesce(GTS2.pack_grp,'Empty')= coalesce(CTS2.pack_grp,'Empty')
and coalesce(GTS2.genre_description, 'Unknown')= coalesce(CTS2.genre_description, 'Unknown')
inner join Num_Events_seg_2 as NES
on NES.Viewing_Type_Detailed = CTS2.Viewing_Type_Detailed
and NES.EVENT_START_DOW = CTS2.EVENT_START_DOW
and NES.event_start_hour = CTS2.event_start_hour
and coalesce(NES.pack_grp, 'Empty')= coalesce(CTS2.pack_grp, 'Empty')
and coalesce(NES.genre_description, 'Unknown') = coalesce(CTS2.genre_description, 'Unknown')


---1,590 Row(s) affected

---Segment_3

select   GTS3.Viewing_Type_Detailed
        ,GTS3.EVENT_START_DOW
        ,GTS3.event_start_hour
        ,GTS3.box_subscription
        ,GTS3.pack_grp
        ,GTS3.genre_description
        ,GTS3.Threshold_Grad_v4
        ,CTS3.Threshold_Curr
        ,GTS3.Capping_Threshold_Grad
        ,CTS3.Capping_Threshold
        ,NES.Number_Events
        ,GTS3.Threshold_Grad_v4-CTS3.Threshold_Curr as Difference_Ntiles_Threshold
        ,GTS3.Capping_Threshold_Grad-CTS3.Capping_Threshold as Difference_Capping_Duration
into     Seg3_Comparison_v4
from     Gradient_Threshold_Seg3_v4 as GTS3
inner join Current_Threshold_Seg3 as CTS3
on GTS3.Viewing_Type_Detailed = CTS3.Viewing_Type_Detailed
and GTS3.EVENT_START_DOW = CTS3.EVENT_START_DOW
and GTS3.event_start_hour = CTS3.event_start_hour
and GTS3.box_subscription = CTS3.box_subscription
and coalesce(GTS3.pack_grp,'Empty')= coalesce(CTS3.pack_grp,'Empty')
and coalesce(GTS3.genre_description, 'Unknown')= coalesce(CTS3.genre_description, 'Unknown')
inner join Num_Events_seg_3 as NES
on NES.Viewing_Type_Detailed = CTS3.Viewing_Type_Detailed
and NES.EVENT_START_DOW = CTS3.EVENT_START_DOW
and NES.event_start_hour = CTS3.event_start_hour
and NES.box_subscription = CTS3.box_subscription
and coalesce(NES.pack_grp, 'Empty')= coalesce(CTS3.pack_grp, 'Empty')
and coalesce(NES.genre_description, 'Unknown') = coalesce(CTS3.genre_description, 'Unknown')
--9,625 Row(s) affected 

