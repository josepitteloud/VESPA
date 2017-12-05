/*
Author: Patrick Igonor
Lead: Claudio Lima
Proportions of Viewing Data based on Genre, Time of the Day and DOW (Weekdays & Weekends)
*/
--Pulling out the capping instances and flags------
select *
into Vespa_Augs_201304_08_14
from (
select * from vespa_analysts.vespa_daily_augs_20130408
union all
select * from vespa_analysts.vespa_daily_augs_20130409
union all
select * from vespa_analysts.vespa_daily_augs_20130410
union all
select * from vespa_analysts.vespa_daily_augs_20130411
union all
select * from vespa_analysts.vespa_daily_augs_20130412
union all
select * from vespa_analysts.vespa_daily_augs_20130413
union all
select * from vespa_analysts.vespa_daily_augs_20130414
) t
-- 140,409,523 row(s) affected

--Table of interest
select top 100 * from HH_Data_Return_8_14_Apr

--Pulling out the capping metrics from the new data structure table
--drop table VESPA_DP_PROG_VIEWED_201304_08_14
select rank() over ( partition by subscriber_id
                                 ,EVENT_START_DATE_TIME_UTC
                                 ,EVENT_END_DATE_TIME_UTC
                                 ,type_of_viewing_event
                                 ,Duration
                         order by BROADCAST_START_DATE_TIME_UTC)
        as Program_Order
       ,account_number
       ,subscriber_id
       ,pk_viewing_prog_instance_fact
       ,event_start_date_time_utc
       ,event_end_date_time_utc
       ,instance_start_date_time_utc
       ,instance_end_date_time_utc
       ,Duration
       ,channel_name
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
                when datepart(weekday,EVENT_START_DATE_TIME_UTC) in (1,7) then 'Weekend' else 'Weekdays'
        end as EVENT_START_DOW
         ,hour(EVENT_START_DATE_TIME_UTC) as EVENT_START_HOUR
         ,spot_standard_daypart_uk
         ,coalesce(genre_description,'Undefined')as genre_description
         ,capping_end_date_time_utc
         ,capping_end_date_time_local
         ,capped_full_flag
         ,capped_partial_flag
into VESPA_DP_PROG_VIEWED_201304_08_14
from sk_prod.VESPA_DP_PROG_VIEWED_201304
where panel_id = 12
and EVENT_START_DATE_TIME_UTC   >= '2013-04-08 00:00:00'
and EVENT_START_DATE_TIME_UTC   <= '2013-04-14 23:59:59'
and type_of_viewing_event <> 'Non viewing event'
and subscriber_id is not null
and account_number in (select account_number from limac.HH_Data_Return_8_14_Apr
                        where reported_week > 0)
--and Duration > 6
-- 176,365,156 row(s) affected

--Building up the indexes to speed up the whole process -----
create hg index idx1 on VESPA_DP_PROG_VIEWED_201304_08_14(pk_viewing_prog_instance_fact);
create hg index idx2 on Vespa_Augs_201304_08_14(Cb_Row_id);


-- Compiling info from new vespa_events table and augmented data
select   VEA.account_number
        ,VEA.instance_start_date_time_utc
        ,VEA.instance_end_date_time_utc
        ,VEA.Duration
        ,VEA.Viewing_Type_Detailed
        ,VEA.EVENT_START_DOW
        ,VEA.genre_description
        ,spot_standard_daypart_uk
        ,AUG.Viewing_Starts
        ,AUG.Viewing_Stops
        ,AUG.Viewing_Duration
        ,AUG.Capped_Flag
        ,AUG.Capped_Event_End_Time
into VEA_Capping_April_08_14
from VESPA_DP_PROG_VIEWED_201304_08_14 VEA
left join Vespa_Augs_201304_08_14 AUG
on VEA.pk_viewing_prog_instance_fact = AUG.cb_row_id
-- 176,365,156 row(s) affected

-- How many events where not found in AUG
select count(*) from VEA_Capping_April_08_14 where viewing_starts is null -- 39,024,215

-- Build more indexes for new match
create hg index idx3 on VESPA_DP_PROG_VIEWED_201304_08_14(subscriber_id,instance_start_date_time_utc);
create hg index idx4 on Vespa_Augs_201304_08_14(subscriber_id,viewing_starts);

--Matching based on subscriber_id and Intance_start_time
-- Compiling info from new vespa_events table and augmented data
select   VEA.account_number
        ,VEA.instance_start_date_time_utc
        ,VEA.instance_end_date_time_utc
        ,VEA.Duration
        ,VEA.Viewing_Type_Detailed
        ,VEA.EVENT_START_DOW
        ,VEA.genre_description
        ,spot_standard_daypart_uk
        ,AUG.Viewing_Starts
        ,AUG.Viewing_Stops
        ,AUG.Viewing_Duration
        ,AUG.Capped_Flag
        ,AUG.Capped_Event_End_Time
into VEA_Capping_April_08_14_v2
from VESPA_DP_PROG_VIEWED_201304_08_14 VEA
left join Vespa_Augs_201304_08_14 AUG
on VEA.subscriber_id = AUG.subscriber_id
and VEA.instance_start_date_time_utc = AUG.Viewing_Starts
-- 176,365,725 row(s) affected

-- How many events where not found in AUG
select count(*) from VEA_Capping_April_08_14_v2 where viewing_starts is null -- 38,606,184


----------Calculating the viewing total across 3 different metrics

select  account_number
       ,sum(Entertainment_Viewing) as Total_Entertainment_Viewing
       ,sum(Specialist_Viewing) as Total_Specialist_Viewing
       ,sum(News_Documentaries_Viewing) as Total_News_Documentaries_Viewing
       ,sum(Movies_Viewing) as Total_Movies_Viewing
       ,sum(Music_Radio_Viewing) as Total_Music_Radio_Viewing
       ,sum(Children_Viewing) as Total_Children_Viewing
       ,sum(Sports_Viewing) as Total_Sports_Viewing
       ,sum(Undefined_Viewing) as Total_Undefined_Viewing
       ,Total_Entertainment_Viewing+Total_Specialist_Viewing+Total_News_Documentaries_Viewing+Total_Movies_Viewing+Total_Music_Radio_Viewing+Total_Children_Viewing+Total_Sports_Viewing+Total_Undefined_Viewing as Total_Viewing_Genre_Description
       ,sum(Early_Peak_Viewing) as Total_Early_Peak_Viewing
       ,sum(Night_Time_Viewing) as Total_Night_Time_Viewing
       ,sum(Morning_Viewing) as Total_Morning_Viewing
       ,sum(Pre_Peak_Viewing) as Total_Pre_Peak_Viewing
       ,sum(Post_Peak_Viewing) as Total_Post_Peak_Viewing
       ,sum(Late_Peak_Viewing) as Total_Late_Peak_Viewing
       ,Total_Early_Peak_Viewing+Total_Night_Time_Viewing+Total_Morning_Viewing+Total_Pre_Peak_Viewing+Total_Post_Peak_Viewing+Total_Late_Peak_Viewing as Total_Viewing_Spot_Standard_daypart_uk
       ,sum(Weekend_Viewing) as Total_Weekend_Viewing
       ,sum(Weekdays_Viewing) as Total_Weekdays_Viewing
       ,Total_Weekend_Viewing+Total_Weekdays_Viewing as Total_Viewing_EVENT_START_DOW
into    Viewing_Aggregated_Data
from (
select  account_number
       ,case when genre_description ='Entertainment' then datediff(second,Viewing_Starts,Viewing_Stops)else 0 end as Entertainment_Viewing
       ,case when genre_description ='Specialist' then datediff(second,Viewing_Starts,Viewing_Stops)else 0 end as Specialist_Viewing
       ,case when genre_description ='News & Documentaries' then datediff(second,Viewing_Starts,Viewing_Stops)else 0 end as News_Documentaries_Viewing
       ,case when genre_description ='Movies' then datediff(second,Viewing_Starts,Viewing_Stops)else 0 end as Movies_Viewing
       ,case when genre_description ='Music & Radio' then datediff(second,Viewing_Starts,Viewing_Stops)else 0 end as Music_Radio_Viewing
       ,case when genre_description ='Children' then datediff(second,Viewing_Starts,Viewing_Stops)else 0 end as Children_Viewing
       ,case when genre_description ='Sports' then datediff(second,Viewing_Starts,Viewing_Stops)else 0 end as Sports_Viewing
       ,case when genre_description ='Undefined' then datediff(second,Viewing_Starts,Viewing_Stops)else 0 end as Undefined_Viewing
       ,case when spot_standard_daypart_uk = 'Early Peak' then datediff(second,Viewing_Starts,Viewing_Stops)else 0 end as Early_Peak_Viewing
       ,case when spot_standard_daypart_uk = 'Night Time' then datediff(second,Viewing_Starts,Viewing_Stops)else 0 end as Night_Time_Viewing
       ,case when spot_standard_daypart_uk = 'Morning' then datediff(second,Viewing_Starts,Viewing_Stops)else 0 end as Morning_Viewing
       ,case when spot_standard_daypart_uk = 'Pre-Peak' then datediff(second,Viewing_Starts,Viewing_Stops)else 0 end as Pre_Peak_Viewing
       ,case when spot_standard_daypart_uk = 'Post Peak' then datediff(second,Viewing_Starts,Viewing_Stops)else 0 end as Post_Peak_Viewing
       ,case when spot_standard_daypart_uk = 'Late Peak' then datediff(second,Viewing_Starts,Viewing_Stops)else 0 end as Late_Peak_Viewing
       ,case when EVENT_START_DOW = 'Weekend' then datediff(second,Viewing_Starts,Viewing_Stops)else 0 end as Weekend_Viewing
       ,case when EVENT_START_DOW = 'Weekdays' then datediff(second,Viewing_Starts,Viewing_Stops)else 0 end as Weekdays_Viewing
       from VEA_Capping_April_08_14
        where viewing_starts is not null -- where there is capping information
   )P
   group by account_number
-- 423,453 row(s) affected

select top 100 * from Viewing_Aggregated_Data

--Calculating the proportions and aggregating across account numbers
select  account_number
       ,Total_Entertainment_Viewing*1.0/Total_Viewing_Genre_Description as Entertainment_Prop
       ,Total_Specialist_Viewing*1.0/Total_Viewing_Genre_Description as Specialist_Prop
       ,Total_News_Documentaries_Viewing*1.0/Total_Viewing_Genre_Description as News_Documentaries_Prop
       ,Total_Movies_Viewing*1.0/Total_Viewing_Genre_Description as Movies_Prop
       ,Total_Music_Radio_Viewing*1.0/Total_Viewing_Genre_Description as Music_radio_Prop
       ,Total_Children_Viewing*1.0/Total_Viewing_Genre_Description as Children_Prop
       ,Total_Sports_Viewing*1.0/Total_Viewing_Genre_Description as Sports_Prop
       ,Total_Undefined_Viewing*1.0/Total_Viewing_Genre_Description as Undefined_Prop
       ,Total_Early_Peak_Viewing*1.0/Total_Viewing_Spot_Standard_daypart_uk as Early_Peak_Prop
       ,Total_Night_Time_Viewing*1.0/Total_Viewing_Spot_Standard_daypart_uk as Night_Time_Prop
       ,Total_Morning_Viewing*1.0/Total_Viewing_Spot_Standard_daypart_uk as Morning_Prop
       ,Total_Pre_Peak_Viewing*1.0/Total_Viewing_Spot_Standard_daypart_uk as Pre_Peak_Prop
       ,Total_Post_Peak_Viewing*1.0/Total_Viewing_Spot_Standard_daypart_uk as Post_Peak_Prop
       ,Total_Late_Peak_Viewing*1.0/Total_Viewing_Spot_Standard_daypart_uk as Late_Peak_Prop
       ,Total_Weekend_Viewing*1.0/Total_Viewing_EVENT_START_DOW as Weekend_Prop
       ,Total_Weekdays_Viewing*1.0/Total_Viewing_EVENT_START_DOW as Weekdays_Prop
       ,Total_Viewing_EVENT_START_DOW
into    Viewing_Aggregated_Data_Proportion
from limac.Viewing_Aggregated_Data
where Total_Viewing_Genre_Description != 0
and Total_Viewing_Spot_Standard_daypart_uk != 0
and Total_Viewing_EVENT_START_DOW !=0
-- 423,453 row(s) affected

--Granting Priviledges
grant all on Vespa_Augs_201304_08_14 to igonorp;
grant all on VESPA_DP_PROG_VIEWED_201304_08_14 to igonorp;
grant all on VEA_Capping_April_08_14 to igonorp;
grant all on Viewing_Aggregated_Data to igonorp;
grant all on Viewing_Aggregated_Data_Proportion to igonorp;
grant all on Viewing_Proportions to igonorp;
grant all on Viewing_proportion_Reporting_Qual to igonorp;

--Profiling the Viewing data based on Genre_Description, Day_parts and DOW (Weekend and weekdays) ---
select
         case
            when HHP.Reported_Week = 0 then 'No return'
            when HHP.Reported_Week between 1 and 3 then 'Low return'
            when HHP.Reported_Week between 4 and 7 then 'Acceptable return'
        end as Reporting_Quality
       ,VE.genre_description
       ,VE.spot_standard_daypart_uk
       ,VE.EVENT_START_DOW
       ,sum(datediff(second,viewing_starts,viewing_stops))/3600.0 as Sum_Viewing -- in hours
from limac.VEA_Capping_April_08_14 VE
inner join limac.HH_Data_Return_8_14_Apr HHP
on HHP.account_number = VE.account_number
where VE.viewing_starts is not null
group by
        Reporting_Quality
       ,VE.genre_description
       ,VE.spot_standard_daypart_uk
       ,VE.EVENT_START_DOW
order by Sum_Viewing desc

-------Going into granular details...Looking at the viewing proportion in detail

select          account_number
                ,case
                when Entertainment_Prop <= 0.1 then 0.1
                when Entertainment_Prop <= 0.2 then 0.2
                when Entertainment_Prop <= 0.3 then 0.3
                when Entertainment_Prop <= 0.4 then 0.4
                when Entertainment_Prop <= 0.5 then 0.5
                when Entertainment_Prop <= 0.6 then 0.6
                when Entertainment_Prop <= 0.7 then 0.7
                when Entertainment_Prop <= 0.8 then 0.8
                when Entertainment_Prop <= 0.9 then 0.9
                when Entertainment_Prop <= 1.0 then 1.0
        end as Entertainment_Proportion
                ,case
                when Specialist_Prop <= 0.1 then 0.1
                when Specialist_Prop <= 0.2 then 0.2
                when Specialist_Prop <= 0.3 then 0.3
                when Specialist_Prop <= 0.4 then 0.4
                when Specialist_Prop <= 0.5 then 0.5
                when Specialist_Prop <= 0.6 then 0.6
                when Specialist_Prop <= 0.7 then 0.7
                when Specialist_Prop <= 0.8 then 0.8
                when Specialist_Prop <= 0.9 then 0.9
                when Specialist_Prop <= 1.0 then 1.0
        end as Specialist_Proportion
                ,case
                when News_Documentaries_Prop <= 0.1 then 0.1
                when News_Documentaries_Prop <= 0.2 then 0.2
                when News_Documentaries_Prop <= 0.3 then 0.3
                when News_Documentaries_Prop <= 0.4 then 0.4
                when News_Documentaries_Prop <= 0.5 then 0.5
                when News_Documentaries_Prop <= 0.6 then 0.6
                when News_Documentaries_Prop <= 0.7 then 0.7
                when News_Documentaries_Prop <= 0.8 then 0.8
                when News_Documentaries_Prop <= 0.9 then 0.9
                when News_Documentaries_Prop <= 1.0 then 1.0
        end as  News_Documentaries_Proportion
                ,case
                when Movies_Prop <= 0.1 then 0.1
                when Movies_Prop <= 0.2 then 0.2
                when Movies_Prop <= 0.3 then 0.3
                when Movies_Prop <= 0.4 then 0.4
                when Movies_Prop <= 0.5 then 0.5
                when Movies_Prop <= 0.6 then 0.6
                when Movies_Prop <= 0.7 then 0.7
                when Movies_Prop <= 0.8 then 0.8
                when Movies_Prop <= 0.9 then 0.9
                when Movies_Prop <= 1.0 then 1.0
        end as Movies_Proportion
                ,case
                when Music_radio_Prop <= 0.1 then 0.1
                when Music_radio_Prop <= 0.2 then 0.2
                when Music_radio_Prop <= 0.3 then 0.3
                when Music_radio_Prop <= 0.4 then 0.4
                when Music_radio_Prop <= 0.5 then 0.5
                when Music_radio_Prop <= 0.6 then 0.6
                when Music_radio_Prop <= 0.7 then 0.7
                when Music_radio_Prop <= 0.8 then 0.8
                when Music_radio_Prop <= 0.9 then 0.9
                when Music_radio_Prop <= 1.0 then 1.0
        end as Music_radio_Proportion
                ,case
                when Children_Prop <= 0.1 then 0.1
                when Children_Prop <= 0.2 then 0.2
                when Children_Prop <= 0.3 then 0.3
                when Children_Prop <= 0.4 then 0.4
                when Children_Prop <= 0.5 then 0.5
                when Children_Prop <= 0.6 then 0.6
                when Children_Prop <= 0.7 then 0.7
                when Children_Prop <= 0.8 then 0.8
                when Children_Prop <= 0.9 then 0.9
                when Children_Prop <= 1.0 then 1.0
        end as Children_Proportion
                ,case
                when Sports_Prop <= 0.1 then 0.1
                when Sports_Prop <= 0.2 then 0.2
                when Sports_Prop <= 0.3 then 0.3
                when Sports_Prop <= 0.4 then 0.4
                when Sports_Prop <= 0.5 then 0.5
                when Sports_Prop <= 0.6 then 0.6
                when Sports_Prop <= 0.7 then 0.7
                when Sports_Prop <= 0.8 then 0.8
                when Sports_Prop <= 0.9 then 0.9
                when Sports_Prop <= 1.0 then 1.0
        end as Sports_Proportion
                ,case
                when Undefined_Prop <= 0.1 then 0.1
                when Undefined_Prop <= 0.2 then 0.2
                when Undefined_Prop <= 0.3 then 0.3
                when Undefined_Prop <= 0.4 then 0.4
                when Undefined_Prop <= 0.5 then 0.5
                when Undefined_Prop <= 0.6 then 0.6
                when Undefined_Prop <= 0.7 then 0.7
                when Undefined_Prop <= 0.8 then 0.8
                when Undefined_Prop <= 0.9 then 0.9
                when Undefined_Prop <= 1.0 then 1.0
        end as Undefined_Proportion
                ,case
                when Early_Peak_Prop <= 0.1 then 0.1
                when Early_Peak_Prop <= 0.2 then 0.2
                when Early_Peak_Prop <= 0.3 then 0.3
                when Early_Peak_Prop <= 0.4 then 0.4
                when Early_Peak_Prop <= 0.5 then 0.5
                when Early_Peak_Prop <= 0.6 then 0.6
                when Early_Peak_Prop <= 0.7 then 0.7
                when Early_Peak_Prop <= 0.8 then 0.8
                when Early_Peak_Prop <= 0.9 then 0.9
                when Early_Peak_Prop <= 1.0 then 1.0
        end as Early_Peak_Proportion
                ,case
                when Night_Time_Prop <= 0.1 then 0.1
                when Night_Time_Prop <= 0.2 then 0.2
                when Night_Time_Prop <= 0.3 then 0.3
                when Night_Time_Prop <= 0.4 then 0.4
                when Night_Time_Prop <= 0.5 then 0.5
                when Night_Time_Prop <= 0.6 then 0.6
                when Night_Time_Prop <= 0.7 then 0.7
                when Night_Time_Prop <= 0.8 then 0.8
                when Night_Time_Prop <= 0.9 then 0.9
                when Night_Time_Prop <= 1.0 then 1.0
        end as Night_Time_Proportion
                ,case
                when Morning_Prop <= 0.1 then 0.1
                when Morning_Prop <= 0.2 then 0.2
                when Morning_Prop <= 0.3 then 0.3
                when Morning_Prop <= 0.4 then 0.4
                when Morning_Prop <= 0.5 then 0.5
                when Morning_Prop <= 0.6 then 0.6
                when Morning_Prop <= 0.7 then 0.7
                when Morning_Prop <= 0.8 then 0.8
                when Morning_Prop <= 0.9 then 0.9
                when Morning_Prop <= 1.0 then 1.0
        end as  Morning_Proportion
                ,case
                when Pre_Peak_Prop <= 0.1 then 0.1
                when Pre_Peak_Prop <= 0.2 then 0.2
                when Pre_Peak_Prop <= 0.3 then 0.3
                when Pre_Peak_Prop <= 0.4 then 0.4
                when Pre_Peak_Prop <= 0.5 then 0.5
                when Pre_Peak_Prop <= 0.6 then 0.6
                when Pre_Peak_Prop <= 0.7 then 0.7
                when Pre_Peak_Prop <= 0.8 then 0.8
                when Pre_Peak_Prop <= 0.9 then 0.9
                when Pre_Peak_Prop <= 1.0 then 1.0
        end as Pre_Peak_Proportion
                ,case
                when Post_Peak_Prop <= 0.1 then 0.1
                when Post_Peak_Prop <= 0.2 then 0.2
                when Post_Peak_Prop <= 0.3 then 0.3
                when Post_Peak_Prop <= 0.4 then 0.4
                when Post_Peak_Prop <= 0.5 then 0.5
                when Post_Peak_Prop <= 0.6 then 0.6
                when Post_Peak_Prop <= 0.7 then 0.7
                when Post_Peak_Prop <= 0.8 then 0.8
                when Post_Peak_Prop <= 0.9 then 0.9
                when Post_Peak_Prop <= 1.0 then 1.0
        end as Post_Peak_Proportion
                ,case
                when Late_Peak_Prop <= 0.1 then 0.1
                when Late_Peak_Prop <= 0.2 then 0.2
                when Late_Peak_Prop <= 0.3 then 0.3
                when Late_Peak_Prop <= 0.4 then 0.4
                when Late_Peak_Prop <= 0.5 then 0.5
                when Late_Peak_Prop <= 0.6 then 0.6
                when Late_Peak_Prop <= 0.7 then 0.7
                when Late_Peak_Prop <= 0.8 then 0.8
                when Late_Peak_Prop <= 0.9 then 0.9
                when Late_Peak_Prop <= 1.0 then 1.0
        end as Late_Peak_Proportion
                ,case
                when Weekend_Prop <= 0.1 then 0.1
                when Weekend_Prop <= 0.2 then 0.2
                when Weekend_Prop <= 0.3 then 0.3
                when Weekend_Prop <= 0.4 then 0.4
                when Weekend_Prop <= 0.5 then 0.5
                when Weekend_Prop <= 0.6 then 0.6
                when Weekend_Prop <= 0.7 then 0.7
                when Weekend_Prop <= 0.8 then 0.8
                when Weekend_Prop <= 0.9 then 0.9
                when Weekend_Prop <= 1.0 then 1.0
        end as Weekend_Proportion
                ,case
                when Weekdays_Prop <= 0.1 then 0.1
                when Weekdays_Prop <= 0.2 then 0.2
                when Weekdays_Prop <= 0.3 then 0.3
                when Weekdays_Prop <= 0.4 then 0.4
                when Weekdays_Prop <= 0.5 then 0.5
                when Weekdays_Prop <= 0.6 then 0.6
                when Weekdays_Prop <= 0.7 then 0.7
                when Weekdays_Prop <= 0.8 then 0.8
                when Weekdays_Prop <= 0.9 then 0.9
                when Weekdays_Prop <= 1.0 then 1.0
        end as Weekdays_Proportion
into  Viewing_Proportions
from limac.Viewing_Aggregated_Data_Proportion
--423,453 Row(s) affected

select   case
         when HHP.Reported_Week = 0 then 'No return'
         when HHP.Reported_Week between 1 and 3 then 'Low return'
         when HHP.Reported_Week between 4 and 7 then 'Acceptable return'
        end as Reporting_Quality
       ,VP.account_number
       ,VP.Entertainment_Proportion
       ,VP.Specialist_Proportion
       ,VP.News_Documentaries_Proportion
       ,VP.Movies_Proportion
       ,VP.Music_radio_Proportion
       ,VP.Children_Proportion
       ,VP.Sports_Proportion
       ,VP.Undefined_Proportion
       ,VP.Early_Peak_Proportion
       ,VP.Night_Time_Proportion
       ,VP.Morning_Proportion
       ,VP.Pre_Peak_Proportion
       ,VP.Post_Peak_Proportion
       ,VP.Late_Peak_Proportion
       ,VP.Weekend_Proportion
       ,VP.Weekdays_Proportion
into Viewing_proportion_Reporting_Qual
from Viewing_Proportions VP
inner join limac.HH_Data_Return_8_14_Apr HHP
on HHP.account_number = VP.account_number
--423,453 Row(s) affected

select distinct (account_number), count(*) from limac.HH_Data_Return_8_14_Apr
group by account_number having count(*) > 1

--Please note that the below could have been done all at once but I wanted this breakdown just to make it simpler & faster too....
--Entertainment_Proportion
select  Reporting_Quality
       ,Entertainment_Proportion
       ,count(*) as Num_HH
from Viewing_proportion_Reporting_Qual
group by  Reporting_Quality
         ,Entertainment_Proportion

--Specialist_Proportion
select  Reporting_Quality
       ,Specialist_Proportion
       ,count(*) as Num_HH
from Viewing_proportion_Reporting_Qual
group by  Reporting_Quality
         ,Specialist_Proportion

--News_Documentaries_Proportion
select  Reporting_Quality
       ,News_Documentaries_Proportion
       ,count(*) as Num_HH
from Viewing_proportion_Reporting_Qual
group by  Reporting_Quality
         ,News_Documentaries_Proportion

--Movies_Proportion
select  Reporting_Quality
       ,Movies_Proportion
       ,count(*) as Num_HH
from Viewing_proportion_Reporting_Qual
group by  Reporting_Quality
         ,Movies_Proportion

--Music_radio_Proportion
select  Reporting_Quality
       ,Music_radio_Proportion
       ,count(*) as Num_HH
from Viewing_proportion_Reporting_Qual
group by  Reporting_Quality
         ,Music_radio_Proportion

--Children_Proportion
select  Reporting_Quality
       ,Children_Proportion
       ,count(*) as Num_HH
from Viewing_proportion_Reporting_Qual
group by  Reporting_Quality
         ,Children_Proportion

--Sports_Proportion
select  Reporting_Quality
       ,Sports_Proportion
       ,count(*) as Num_HH
from Viewing_proportion_Reporting_Qual
group by  Reporting_Quality
         ,Sports_Proportion

--Undefined_Proportion
select  Reporting_Quality
       ,Undefined_Proportion
       ,count(*) as Num_HH
from Viewing_proportion_Reporting_Qual
group by  Reporting_Quality
         ,Undefined_Proportion

--Early_Peak_Proportion
select  Reporting_Quality
       ,Early_Peak_Proportion
       ,count(*) as Num_HH
from Viewing_proportion_Reporting_Qual
group by  Reporting_Quality
         ,Early_Peak_Proportion

--Night_Time_Proportion
select  Reporting_Quality
       ,Night_Time_Proportion
       ,count(*) as Num_HH
from Viewing_proportion_Reporting_Qual
group by  Reporting_Quality
         ,Night_Time_Proportion

--Morning_Proportion
select  Reporting_Quality
       ,Morning_Proportion
       ,count(*) as Num_HH
from Viewing_proportion_Reporting_Qual
group by  Reporting_Quality
         ,Morning_Proportion

--Pre_Peak_Proportion
select  Reporting_Quality
       ,Pre_Peak_Proportion
       ,count(*) as Num_HH
from Viewing_proportion_Reporting_Qual
group by  Reporting_Quality
         ,Pre_Peak_Proportion

--Post_Peak_Proportion
select  Reporting_Quality
       ,Post_Peak_Proportion
       ,count(*) as Num_HH
from Viewing_proportion_Reporting_Qual
group by  Reporting_Quality
         ,Post_Peak_Proportion

--Late_Peak_Proportion
select  Reporting_Quality
       ,Late_Peak_Proportion
       ,count(*) as Num_HH
from Viewing_proportion_Reporting_Qual
group by  Reporting_Quality
         ,Late_Peak_Proportion

--Weekend_Proportion
select  Reporting_Quality
       ,Weekend_Proportion
       ,count(*) as Num_HH
from Viewing_proportion_Reporting_Qual
group by  Reporting_Quality
         ,Weekend_Proportion

--Weekdays_Proportion
select  Reporting_Quality
       ,Weekdays_Proportion
       ,count(*) as Num_HH
from Viewing_proportion_Reporting_Qual
group by  Reporting_Quality
         ,Weekdays_Proportion


--Checks
select count(*)
from limac.Viewing_Aggregated_Data
--where Total_Viewing_Genre_Description <> Total_Viewing_Spot_Standard_daypart_uk
--where Total_Viewing_Genre_Description <> Total_Viewing_EVENT_START_DOW
where Total_Viewing_EVENT_START_DOW <> Total_Viewing_Spot_Standard_daypart_uk

