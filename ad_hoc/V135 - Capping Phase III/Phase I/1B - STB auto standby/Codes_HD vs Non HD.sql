/* Capping Phase III
Checking the profile of the Hd vs Non HD to see if there are differences in behaviours--
Period : 2012 - 11 - 07

V01. Look at the profile of HD Box Physical
V02. Look at the model, Manufacturer and description and evaluate their profile

Author: Patrick Igonor
Date Created : 30-11-2012

*/

-----------------------------------------------------------------------
-- V01. Look at the profile of HD Box Physical based on a week's data
-----------------------------------------------------------------------

select
        median(duration),
        HD_box_physical,
        ntiles_Grp
from
        (select duration, SBV.HD_box_physical,
        ntile(200) over (partition by HD_box_physical order by duration) as ntiles_Grp
from sk_prod.vespa_events_all as VEA inner join Vespa_analysts.vespa_single_box_view as SBV on VEA.subscriber_id = SBV.subscriber_id
where date(event_start_date_time_utc) between '2012-08-27' and '2012-09-03'
      ) Patrick
group by  HD_box_physical ,ntiles_Grp
order by HD_box_physical ,ntiles_Grp


------------------------------------------------------------------------------------
-- V02. Look at the model, Manufacturer and description and evaluate their profile
------------------------------------------------------------------------------------


/*---- Running the ntiles based on Manufacturer, Model and description----*/

-- Step 1: Get data from both tables

select duration
        ,hour(event_start_date_time_utc)as Event_Start_Hour
        ,x_manufacturer+'|'+x_description+'|'+x_model_number as Manuf_Descr_Model
into VEA_STB_1week
from sk_prod.vespa_events_all as VEA
inner join limac.LOCAL_Cust_Set_Top_Box as STB
on VEA.account_number = STB.account_number
where event_start_date_time_utc between '2012-08-27 00:00:00' and '2012-09-03 23:59:59'

alter table VEA_STB_1week add Event_Start_Hour_Intervals varchar(10)

----Step 2: Building intervals within event start hour-----

Update VEA_STB_1week
set Event_Start_Hour_Intervals =
        case
                when Event_Start_Hour between 4  and 14 then 'HR_4_14'
                when Event_Start_Hour between 15 and 19 then 'HR_15_19'
                when Event_Start_Hour between 20 and 21 then 'HR_20_21'
                when Event_Start_Hour in (22,23,1,0,2,3)  then 'HR_22_3'
        end


-- Step 3: build indexes to speed up the next query

create hg index idx2 on VEA_STB_1week (Manuf_Descr_Model,Event_Start_Hour_Intervals)

-- Step 4: Calculate ntiles-----------

select median(duration)as Median_Duration
        ,Manuf_Descr_Model
        ,Event_Start_Hour_Intervals
        ,ntiles_Grp
into Median_Dur_Int
from    (select duration
        ,Manuf_Descr_Model
        ,Event_Start_Hour_Intervals
        ,ntile(200) over (partition by Manuf_Descr_Model,Event_Start_Hour_Intervals order by duration) as ntiles_Grp
        from VEA_STB_1week
        ) Patrick
group by  Manuf_Descr_Model,Event_Start_Hour_Intervals, ntiles_Grp
order by  Manuf_Descr_Model,Event_Start_Hour_Intervals, ntiles_Grp








