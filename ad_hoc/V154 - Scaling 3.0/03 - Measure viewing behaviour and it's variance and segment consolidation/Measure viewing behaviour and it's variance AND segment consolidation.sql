/*************************************************************************************

 Compile viewing data from the 8th to the 14th of July 2013
 Analyst : Patrick Igonor
 Lead Analyst : Claudio Lima
 Date : 20-08-2013
**************************************************************************************/
select type_of_viewing_event, count(*) from sk_prod.vespa_dp_prog_viewed_current
group by type_of_viewing_event
--Tables of Interest--
select top 10* from sk_prod.vespa_dp_prog_viewed_current
select top 10* from vespa_analysts.SC2_Segments_lookup_V2_1
select top 10* from vespa_analysts.SC2_Intervals
select top 10* from vespa_analysts.SC2_Weightings
select top 10* from vespa_analysts.SC2_Sky_base_segment_snapshots

--Extracting data from the Viewing Events Table --
select  account_number
       ,genre_description
       ,programme_name
       ,sub_genre_description
       ,synopsis
       ,channel_name
       ,hour(event_start_date_time_utc) as EVENT_START_HOUR
       ,case
                when datepart(weekday,event_start_date_time_utc)=1 then 'Sun'
                when datepart(weekday,event_start_date_time_utc)=2 then 'Mon'
                when datepart(weekday,event_start_date_time_utc)=3 then 'Tue'
                when datepart(weekday,event_start_date_time_utc)=4 then 'Wed'
                when datepart(weekday,event_start_date_time_utc)=5 then 'Thu'
                when datepart(weekday,event_start_date_time_utc)=6 then 'Fri'
                when datepart(weekday,event_start_date_time_utc)=7 then 'Sat'
        end as EVENT_START_DOW
       ,case
                when datepart(weekday,event_start_date_time_utc) in (1,7) then 'Weekend' else 'Weekdays'
        end as EVENT_START_WW
       ,spot_standard_daypart_uk
       ,live_recorded
       ,playback_type
       ,type_of_viewing_event
       ,panel_id
       ,duration
       ,broadcast_start_date_time_utc
       ,broadcast_end_date_time_utc
       ,event_start_date_time_utc
       ,event_end_date_time_utc
       ,instance_start_date_time_utc
       ,instance_end_date_time_utc
       ,capped_full_flag
       ,capped_partial_flag
       ,capping_end_date_time_utc
       ,capping_end_date_time_local
into Viewing_Events_Jul_08_14_2013 ----(Table dropped, we still have it as another name)
from sk_prod.vespa_dp_prog_viewed_201307
where EVENT_START_DATE_TIME_UTC   >= '2013-07-08 00:00:00'
and   EVENT_START_DATE_TIME_UTC   <= '2013-07-14 23:59:59'
and subscriber_id is not null
and account_number is not null
--138,582,746 Row(s) affected

------------------------------Runnin the above on the server
--Getting the account_number and max scaling_segment_ID for the reporting week (8th to 14th of July 2013)

select account_number, max(scaling_segment_ID) as scaling_segment_ID
into Max_Scaling_ID
from vespa_analysts.SC2_Intervals
where (reporting_starts <= '2013-07-08' and reporting_ends >= '2013-07-14')
or (reporting_starts >= '2013-07-08' and reporting_starts <= '2013-07-14')
or (reporting_ends >= '2013-07-08' and reporting_ends <= '2013-07-14')
group by account_number
--453,408 Row(s) affected


--Bringing in the account number and all scaling variables including segment_scaling_ID into a single table (Account_number_Scaling_Variables)

select  AH.account_number
       ,SEG.scaling_segment_ID
       ,SEG.universe
       ,SEG.isba_tv_region
       ,SEG.hhcomposition
       ,SEG.tenure
       ,SEG.package
       ,SEG.boxtype
into Account_number_Scaling_Variables
from Max_Scaling_ID AH
inner join vespa_analysts.SC2_Segments_lookup_V2_1 SEG
on AH.scaling_segment_ID = SEG.scaling_segment_ID
--453,408 Row(s) affected
select top 5* from Account_number_Scaling_Variables

--Recoding the scaling variables into smaller units
select   account_number
        ,scaling_segment_ID
        ,universe
        ,isba_tv_region
        , case when isba_tv_region = 'Border'  then 'NI, Scotland, & Border'
               when isba_tv_region = 'Central Scotland'  then 'NI, Scotland, & Border'
               when isba_tv_region = 'East Of England'  then 'Wales & Midlands'
               when isba_tv_region = 'HTV Wales'  then 'Wales & Midlands'
               when isba_tv_region = 'HTV West'  then  'South England'
               when isba_tv_region = 'London'  then 'London'
               when isba_tv_region = 'Meridian (exc. Chann'  then 'South England'
               when isba_tv_region = 'Midlands'  then 'Wales & Midlands'
               when isba_tv_region = 'North East'  then 'North England'
               when isba_tv_region = 'North Scotland'  then 'NI, Scotland, & Border'
               when isba_tv_region = 'North West'  then 'North England'
               when isba_tv_region = 'Not Defined'  then 'Not Defined'
               when isba_tv_region = 'South West'  then 'South England'
               when isba_tv_region = 'Ulster'  then 'NI, Scotland, & Border'
               when isba_tv_region = 'Yorkshire'  then 'North England'
          end as isba_tv_region_v2
         ,case when hhcomposition = '00' then '00: Families'
               when hhcomposition = '01' then '01: Extended family'
               when hhcomposition = '02' then '02: Extended household'
               when hhcomposition = '03' then '03: Pseudo family'
               when hhcomposition = '04' then '04: Single male'
               when hhcomposition = '05' then '05: Single female'
               when hhcomposition = '06' then '06: Male homesharers'
               when hhcomposition = '07' then '07: Female homesharers'
               when hhcomposition = '08' then '08: Mixed homesharers'
               when hhcomposition = '09' then '09: Abbreviated male families'
               when hhcomposition = '10' then '10: Abbreviated female families'
               when hhcomposition = '11' then '11: Multi-occupancy dwelling'
               when hhcomposition = 'U'  then 'U: Unclassified HHComp'
           end as hhcomposition
         ,case when hhcomposition = '00: Families' then 'A) Families'
               when hhcomposition = '01: Extended family' then 'A) Families'
               when hhcomposition = '02: Extended household' then 'A) Families'
               when hhcomposition = '03: Pseudo family' then 'A) Families'
               when hhcomposition = '04: Single male' then 'B) Singles'
               when hhcomposition = '05: Single female' then 'B) Singles'
               when hhcomposition = '06: Male homesharers' then 'C) Homesharers'
               when hhcomposition = '07: Female homesharers' then 'C) Homesharers'
               when hhcomposition = '08: Mixed homesharers' then 'C) Homesharers'
               when hhcomposition = '09: Abbreviated male families' then   'A) Families'
               when hhcomposition = '10: Abbreviated female families' then   'A) Families'
               when hhcomposition = '11: Multi-occupancy dwelling' then 'C) Homesharers'
               when hhcomposition = 'U: Unclassified HHComp'  then 'D) Unclassified HHComp'
         end as hhcomposition_v2
         ,tenure
         ,case when tenure = 'A) 0-2 Years' then 'A) 0-2 Years'
               when tenure = 'B) 3-10 Years'then 'B) 3-10 Years'
               when tenure = 'C) 10 Years+' then 'C) 10 Years+'
               when tenure = 'D) Unknown' then 'C) 10 Years+'
         end as tenure_v2
         ,boxtype
         ,case when boxtype = 'A) HDx & No_secondary_box' then '2) Skyplus & No_secondary_box'
               when boxtype = 'B) HD & No_secondary_box' then '1) HD & No_secondary_box'
               when boxtype = 'C) Skyplus & No_secondary_box' then '2) Skyplus & No_secondary_box'
               when boxtype = 'D) FDB & No_secondary_box' then '3) FDB & No_secondary_box'
               when boxtype = 'E) HD & HD' then '4) HD & HD'
               when boxtype = 'F) HD & Skyplus' then '5) HD & Less capable'
               when boxtype = 'G) HD & FDB' then '5) HD & Less capable'
               when boxtype = 'H) HDx & HDx' then '6) Skyplus & Skyplus'
               when boxtype = 'I) HDx & Skyplus' then '6) Skyplus & Skyplus'
               when boxtype = 'J) HDx & FDB'  then '7) Skyplus & Less capable'
               when boxtype = 'K) Skyplus & Skyplus' then '6) Skyplus & Skyplus'
               when boxtype = 'L) Skyplus & FDB' then '7) Skyplus & Less capable'
               when boxtype = 'M) FDB & FDB' then '8) FDB & FDB'
         end as boxtype_v2
         ,package
         ,case when package = 'Basic - Ent' then 'Basic'
               when package = 'Basic - Ent Extra' then 'Basic'
               when package = 'Dual Movies' then 'Movies'
               when package = 'Dual Sports' then 'Sports'
               when package = 'Other Premiums' then 'Movies & Sports'
               when package = 'Single Movies' then 'Movies'
               when package = 'Single Sports' then 'Sports'
               when package = 'Top Tier' then 'Movies & Sports'
         end as package_v2
into Account_number_Scaling_Variables_recoded
from Account_number_Scaling_Variables
--453,408 Row(s) affected

--Checks on the above codes
select
       hhcomposition_v2
       ,count(*) as count
from Account_number_Scaling_Variables_recoded
group by
      hhcomposition_v2


--Matching the above table to the Viewing events table based on Account Number (This is for the new scaling variabes i.e the recoded ones)
select  VE.account_number
       ,VE.genre_description
       ,VE.programme_name
       ,VE.sub_genre_description
       ,VE.channel_name
       ,VE.EVENT_START_HOUR
       ,VE.EVENT_START_DOW
       ,VE.EVENT_START_WW
       ,VE.spot_standard_daypart_uk
       ,VE.live_recorded
       ,VE.playback_type
       ,VE.broadcast_start_date_time_utc
       ,VE.broadcast_end_date_time_utc
       ,VE.event_start_date_time_utc
       ,VE.event_end_date_time_utc
       ,VE.instance_start_date_time_utc
       ,VE.instance_end_date_time_utc
       ,VE.capped_full_flag
       ,VE.capped_partial_flag
       ,VE.capping_end_date_time_utc
       ,VE.capping_end_date_time_local
       ,AC.scaling_segment_ID
       ,AC.universe
       ,AC.isba_tv_region
       ,AC.isba_tv_region_v2
       ,AC.hhcomposition
       ,AC.hhcomposition_v2
       ,AC.tenure
       ,AC.tenure_v2
       ,AC.package
       ,AC.package_v2
       ,AC.boxtype
       ,AC.boxtype_v2
into Final_Viewing_Scal_Var_Jul_08_14_2013 ----(Table dropped, we still have it as another name)
from Viewing_Events_Jul_08_14_2013 VE
left join Account_number_Scaling_Variables_recoded AC
on VE.account_number = AC.account_number
--138,582,746 Row(s) affected

 --Recoding the Dayparts above ---
select  account_number
       ,genre_description
       ,programme_name
       ,sub_genre_description
       ,channel_name
       ,EVENT_START_HOUR
       ,EVENT_START_DOW
       ,EVENT_START_WW
       ,case when EVENT_START_HOUR between 6 and 8   then 'Breakfast-time'
             when EVENT_START_HOUR between 9 and 11  then 'Morning'
             when EVENT_START_HOUR between 12 and 14 then 'Lunch'
             when EVENT_START_HOUR between 15 and 17 then 'Early Prime'
             when EVENT_START_HOUR between 18 and 20 then 'Prime'
             when EVENT_START_HOUR between 21 and 23 then 'Night'
             when EVENT_START_HOUR between 0 and 5   then 'Late Night'
       end as Day_Parts
       ,spot_standard_daypart_uk
       ,live_recorded
       ,playback_type
       ,broadcast_start_date_time_utc
       ,broadcast_end_date_time_utc
       ,event_start_date_time_utc
       ,event_end_date_time_utc
       ,instance_start_date_time_utc
       ,instance_end_date_time_utc
       ,capped_full_flag
       ,capped_partial_flag
       ,capping_end_date_time_utc
       ,capping_end_date_time_local
       ,scaling_segment_ID
       ,universe
       ,isba_tv_region
       ,isba_tv_region_v2
       ,hhcomposition
       ,hhcomposition_v2
       ,tenure
       ,tenure_v2
       ,package
       ,package_v2
       ,boxtype
       ,boxtype_v2
       ,case   when boxtype = 'A) HDx & No_secondary_box' then '2) PVR & No_secondary_box'
               when boxtype = 'B) HD & No_secondary_box' then '1) HD & No_secondary_box'
               when boxtype = 'C) Skyplus & No_secondary_box' then '2) PVR & No_secondary_box'
               when boxtype = 'D) FDB & No_secondary_box' then '3) FDB & No_secondary_box'
               when boxtype = 'E) HD & HD' then '4) HD & PVR'
               when boxtype = 'F) HD & Skyplus' then '4) HD & PVR'
               when boxtype = 'G) HD & FDB' then '5) HD & FDB'
               when boxtype = 'H) HDx & HDx' then '6) PVR & PVR'
               when boxtype = 'I) HDx & Skyplus' then '6) PVR & PVR'
               when boxtype = 'J) HDx & FDB'  then '7) PVR & FDB'
               when boxtype = 'K) Skyplus & Skyplus' then '6) PVR & PVR'
               when boxtype = 'L) Skyplus & FDB' then '7) PVR & FDB'
               when boxtype = 'M) FDB & FDB' then '8) FDB & FDB'
         end as boxtype_v3
into Final_Viewing_Scal_Var_Jul_08_14_2013_Dayparts_Recode
from Final_Viewing_Scal_Var_Jul_08_14_2013
--138,582,746 Row(s) affected

--Checks----------------------------------------------------------------------------------------*****************
select Day_Parts, count(*) from Final_Viewing_Scal_Var_Jul_08_14_2013_Dayparts_Recode
group by Day_Parts

select top 10* from Final_Viewing_Scal_Var_Jul_08_14_2013
select top 10* from Metrics_Duration
--Checks
select package, count(*) from  Final_Viewing_Scal_Var_Jul_08_14_2013_Dayparts_Recode
group by package
order by package

select hhcomposition, count(*) from  Final_Viewing_Scal_Var_Jul_08_14_2013_Dayparts_Recode
group by hhcomposition
order by hhcomposition
--universe              count()
 (Null)                 160,493
A) Single box HH        100,352,325
B) Multiple box HH      38,069,928
-----------------------------------------------------------------------------------------------********************
---Calculating the viewing total across 3 different metrics

***************ISBA_TV_REGION************************************

create table Metrics_Duration_Region_v1 (
         Scaling_Variables varchar (40)
        ,Scaling_Sub_Variables varchar(40)
        ,EVENT_START_WW varchar(15), live_recorded varchar(15), Day_Parts varchar(25)
        ,instance_dur bigint
        );
        commit

INSERT INTO Metrics_Duration_Region_v1
select 'isba_tv_region' as Scaling_Variables
      ,isba_tv_region as Scaling_Sub_Variables
      ,EVENT_START_WW
      ,live_recorded
      ,Day_Parts
      ,sum(case
          when capped_full_flag = 1 then 0
          when capped_partial_flag = 1 then datediff(second, instance_start_date_time_utc, coalesce(capping_end_date_time_utc,instance_end_date_time_utc))
            else datediff(second, instance_start_date_time_utc, instance_end_date_time_utc)
      end)
from Final_Viewing_Scal_Var_Jul_08_14_2013_Dayparts_Recode
group by Scaling_Variables,Scaling_Sub_Variables,EVENT_START_WW, live_recorded, Day_Parts
--448 Row(s) affected
;

***************ISBA_TV_REGION_V2************************************
create table Metrics_Duration_Region_v2 (
         Scaling_Variables varchar (40)
        ,Scaling_Sub_Variables varchar(40)
        ,EVENT_START_WW varchar(15), live_recorded varchar(15), Day_Parts varchar(25)
        ,instance_dur bigint
        );
        commit

INSERT INTO Metrics_Duration_Region_v2
select 'isba_tv_region_v2' as Scaling_Variables
      ,isba_tv_region_v2 as Scaling_Sub_Variables
      ,EVENT_START_WW
      ,live_recorded
      ,Day_Parts
      ,sum(case
          when capped_full_flag = 1 then 0
          when capped_partial_flag = 1 then datediff(second, instance_start_date_time_utc, coalesce(capping_end_date_time_utc,instance_end_date_time_utc))
            else datediff(second, instance_start_date_time_utc, instance_end_date_time_utc)
      end)
from Final_Viewing_Scal_Var_Jul_08_14_2013_Dayparts_Recode
group by Scaling_Variables,Scaling_Sub_Variables,EVENT_START_WW, live_recorded, Day_Parts
--196 Row(s) affected 
;
***************HOUSEHOLD COMPOSITION************************************

create table Metrics_Duration_hhcomp_v1 (
         Scaling_Variables varchar (40)
        ,Scaling_Sub_Variables varchar(40)
        ,EVENT_START_WW varchar(15), live_recorded varchar(15), Day_Parts varchar(25)
        ,instance_dur bigint
        );
        commit

INSERT INTO Metrics_Duration_hhcomp_v1
select 'hhcomposition' as Scaling_Variables
      ,hhcomposition as Scaling_Sub_Variables
      ,EVENT_START_WW
      ,live_recorded
      ,Day_Parts
      ,sum(case
          when capped_full_flag = 1 then 0
          when capped_partial_flag = 1 then datediff(second, instance_start_date_time_utc, coalesce(capping_end_date_time_utc,instance_end_date_time_utc))
            else datediff(second, instance_start_date_time_utc, instance_end_date_time_utc)
      end)
from Final_Viewing_Scal_Var_Jul_08_14_2013_Dayparts_Recode
group by Scaling_Variables,Scaling_Sub_Variables,EVENT_START_WW, live_recorded, Day_Parts
--392 Row(s) affected
;
***************HOUSEHOLD COMPOSITION_V2************************************

create table Metrics_Duration_hhcomp_v2 (
         Scaling_Variables varchar (40)
        ,Scaling_Sub_Variables varchar(40)
        ,EVENT_START_WW varchar(15), live_recorded varchar(15), Day_Periods varchar(25)
        ,instance_dur bigint
        );
        commit

INSERT INTO Metrics_Duration_hhcomp_v2
select 'hhcomposition_v2' as Scaling_Variables
      ,hhcomposition_v2 as Scaling_Sub_Variables
      ,EVENT_START_WW
      ,live_recorded
      ,Day_Parts
      ,sum(case
          when capped_full_flag = 1 then 0
          when capped_partial_flag = 1 then datediff(second, instance_start_date_time_utc, coalesce(capping_end_date_time_utc,instance_end_date_time_utc))
            else datediff(second, instance_start_date_time_utc, instance_end_date_time_utc)
      end)
from Final_Viewing_Scal_Var_Jul_08_14_2013_Dayparts_Recode
group by Scaling_Variables,Scaling_Sub_Variables,EVENT_START_WW, live_recorded, Day_Parts
--140 Row(s) affected
;
***************TENURE************************************

create table Metrics_Duration_tenure_v1 (
         Scaling_Variables varchar (40)
        ,Scaling_Sub_Variables varchar(40)
        ,EVENT_START_WW varchar(15), live_recorded varchar(15), Day_Parts varchar(25)
        ,instance_dur bigint
        );
        commit

INSERT INTO Metrics_Duration_tenure_v1
select 'tenure' as Scaling_Variables
      ,tenure as Scaling_Sub_Variables
      ,EVENT_START_WW
      ,live_recorded
      ,Day_Parts
      ,sum(case
          when capped_full_flag = 1 then 0
          when capped_partial_flag = 1 then datediff(second, instance_start_date_time_utc, coalesce(capping_end_date_time_utc,instance_end_date_time_utc))
            else datediff(second, instance_start_date_time_utc, instance_end_date_time_utc)
      end)
from Final_Viewing_Scal_Var_Jul_08_14_2013_Dayparts_Recode
group by Scaling_Variables,Scaling_Sub_Variables,EVENT_START_WW, live_recorded, Day_Parts
--127 Row(s) affected
;
***************TENURE_V2************************************

create table Metrics_Duration_tenure_v2 (
         Scaling_Variables varchar (40)
        ,Scaling_Sub_Variables varchar(40)
        ,EVENT_START_WW varchar(15), live_recorded varchar(15), Day_Parts varchar(25)
        ,instance_dur bigint
        );
        commit

INSERT INTO Metrics_Duration_tenure_v2
select 'tenure_v2' as Scaling_Variables
      ,tenure_v2 as Scaling_Sub_Variables
      ,EVENT_START_WW
      ,live_recorded
      ,Day_Parts
      ,sum(case
          when capped_full_flag = 1 then 0
          when capped_partial_flag = 1 then datediff(second, instance_start_date_time_utc, coalesce(capping_end_date_time_utc,instance_end_date_time_utc))
            else datediff(second, instance_start_date_time_utc, instance_end_date_time_utc)
      end)
from Final_Viewing_Scal_Var_Jul_08_14_2013_Dayparts_Recode
group by Scaling_Variables,Scaling_Sub_Variables,EVENT_START_WW, live_recorded, Day_Parts
--112 Row(s) affected
;
***************BOXTYPE************************************

create table Metrics_Duration_boxtype_v1 (
         Scaling_Variables varchar (40)
        ,Scaling_Sub_Variables varchar(40)
        ,EVENT_START_WW varchar(15), live_recorded varchar(15), Day_Parts varchar(25)
        ,instance_dur bigint
        );
        commit

INSERT INTO Metrics_Duration_boxtype_v1
select 'boxtype' as Scaling_Variables
      ,boxtype as Scaling_Sub_Variables
      ,EVENT_START_WW
      ,live_recorded
      ,Day_Parts
      ,sum(case
          when capped_full_flag = 1 then 0
          when capped_partial_flag = 1 then datediff(second, instance_start_date_time_utc, coalesce(capping_end_date_time_utc,instance_end_date_time_utc))
            else datediff(second, instance_start_date_time_utc, instance_end_date_time_utc)
      end)
from Final_Viewing_Scal_Var_Jul_08_14_2013_Dayparts_Recode
group by Scaling_Variables,Scaling_Sub_Variables,EVENT_START_WW, live_recorded, Day_Parts
--392 Row(s) affected
;
***************BOXTYPE_V2************************************

create table Metrics_Duration_boxtype_v2 (
         Scaling_Variables varchar (40)
        ,Scaling_Sub_Variables varchar(40)
        ,EVENT_START_WW varchar(15), live_recorded varchar(15), Day_Parts varchar(25)
        ,instance_dur bigint
        );
        commit

INSERT INTO Metrics_Duration_boxtype_v2
select 'boxtype_v2' as Scaling_Variables
      ,boxtype_v2 as Scaling_Sub_Variables
      ,EVENT_START_WW
      ,live_recorded
      ,Day_Parts
      ,sum(case
          when capped_full_flag = 1 then 0
          when capped_partial_flag = 1 then datediff(second, instance_start_date_time_utc, coalesce(capping_end_date_time_utc,instance_end_date_time_utc))
            else datediff(second, instance_start_date_time_utc, instance_end_date_time_utc)
      end)
from Final_Viewing_Scal_Var_Jul_08_14_2013_Dayparts_Recode
group by Scaling_Variables,Scaling_Sub_Variables,EVENT_START_WW, live_recorded, Day_Parts
--252 Row(s) affected
;
***************BOXTYPE_V3************************************

create table Metrics_Duration_boxtype_v3 (
         Scaling_Variables varchar (40)
        ,Scaling_Sub_Variables varchar(40)
        ,EVENT_START_WW varchar(15), live_recorded varchar(15), Day_Parts varchar(25)
        ,instance_dur bigint
        );
        commit

INSERT INTO Metrics_Duration_boxtype_v3
select 'boxtype_v3' as Scaling_Variables
      ,boxtype_v3 as Scaling_Sub_Variables
      ,EVENT_START_WW
      ,live_recorded
      ,Day_Parts
      ,sum(case
          when capped_full_flag = 1 then 0
          when capped_partial_flag = 1 then datediff(second, instance_start_date_time_utc, coalesce(capping_end_date_time_utc,instance_end_date_time_utc))
            else datediff(second, instance_start_date_time_utc, instance_end_date_time_utc)
      end)
from Final_Viewing_Scal_Var_Jul_08_14_2013_Dayparts_Recode
group by Scaling_Variables,Scaling_Sub_Variables,EVENT_START_WW, live_recorded, Day_Parts
--252 Row(s) affected
;

***************PACKAGE************************************

create table Metrics_Duration_package_v1 (
         Scaling_Variables varchar (40)
        ,Scaling_Sub_Variables varchar(40)
        ,EVENT_START_WW varchar(15), live_recorded varchar(15), Day_Parts varchar(25)
        ,instance_dur bigint
        );
        commit

INSERT INTO Metrics_Duration_package_v1
select 'package' as Scaling_Variables
      ,package as Scaling_Sub_Variables
      ,EVENT_START_WW
      ,live_recorded
      ,Day_Parts
      ,sum(case
          when capped_full_flag = 1 then 0
          when capped_partial_flag = 1 then datediff(second, instance_start_date_time_utc, coalesce(capping_end_date_time_utc,instance_end_date_time_utc))
            else datediff(second, instance_start_date_time_utc, instance_end_date_time_utc)
      end)
from Final_Viewing_Scal_Var_Jul_08_14_2013_Dayparts_Recode
group by Scaling_Variables,Scaling_Sub_Variables,EVENT_START_WW, live_recorded, Day_Parts
--252 Row(s) affected
;
***************PACKAGE_V2************************************

create table Metrics_Duration_package_v2 (
         Scaling_Variables varchar (40)
        ,Scaling_Sub_Variables varchar(40)
        ,EVENT_START_WW varchar(15), live_recorded varchar(15), Day_Parts varchar(25)
        ,instance_dur bigint
        );
        commit

INSERT INTO Metrics_Duration_package_v2
select 'package_v2' as Scaling_Variables
      ,package_v2 as Scaling_Sub_Variables
      ,EVENT_START_WW
      ,live_recorded
      ,Day_Parts
      ,sum(case
          when capped_full_flag = 1 then 0
          when capped_partial_flag = 1 then datediff(second, instance_start_date_time_utc, coalesce(capping_end_date_time_utc,instance_end_date_time_utc))
            else datediff(second, instance_start_date_time_utc, instance_end_date_time_utc)
      end)
from Final_Viewing_Scal_Var_Jul_08_14_2013_Dayparts_Recode
group by Scaling_Variables,Scaling_Sub_Variables,EVENT_START_WW, live_recorded, Day_Parts
--140 Row(s) affected


--Combining all the above scaling variables into one table
select * into Metrics_Duration from Metrics_Duration_Region_v1
union all
select * from Metrics_Duration_Region_v2
union all
select * from Metrics_Duration_hhcomp_v1
union all
select * from Metrics_Duration_hhcomp_v2
union all
select * from Metrics_Duration_tenure_v1
union all
select * from Metrics_Duration_tenure_v2
union all
select * from Metrics_Duration_boxtype_v1
union all
select * from Metrics_Duration_boxtype_v2
union all
select * from Metrics_Duration_boxtype_v3
union all
select * from Metrics_Duration_package_v1
union all
select * from Metrics_Duration_package_v2
--2,703 Row(s) affected

--Checks
select count(*) from Metrics_Duration
where Scaling_Variables = 'boxtype_v3'

--Getting the total sum @ the Scaling_Variables level
create table Metrics_Duration_Total (
         Scaling_Variables varchar (40)
        ,Scaling_Sub_Variables varchar(40)
        ,instance_duration_sum bigint
        );
        commit

INSERT INTO Metrics_Duration_Total
select Scaling_Variables
      ,Scaling_Sub_Variables
      ,sum(instance_dur)
from Metrics_Duration
group by Scaling_Variables
        ,Scaling_Sub_Variables
--88 Row(s) affected

--Appending the two table above to each other---
select MD.Scaling_Variables
      ,MD.Scaling_Sub_Variables
      ,MD.EVENT_START_WW
      ,MD.live_recorded
      ,MD.Day_Parts
      ,MD.instance_dur
      ,MDT.instance_duration_sum
into Viewing_Duration_Aggregate
from Metrics_Duration as MD
left join Metrics_Duration_Total MDT
on MD.Scaling_Variables = MDT.Scaling_Variables
and MD.Scaling_Sub_Variables = MDT.Scaling_Sub_Variables
--2,703 Row(s) affected


--Calculating Proportions --

select   Scaling_Variables
        ,Scaling_Sub_Variables
        ,EVENT_START_WW
        ,live_recorded
        ,Day_Parts
        ,instance_dur
        ,instance_duration_sum
        ,1.0*instance_dur / instance_duration_sum as Proportions_of_Viewing
into Viewing_Duration_Proportions
from Viewing_Duration_Aggregate
group by Scaling_Variables
        ,Scaling_Sub_Variables
        ,EVENT_START_WW
        ,live_recorded
        ,Day_Parts
        ,instance_dur
        ,instance_duration_sum
--2,451 Row(s) affected

-- Checks to be sure every Scaling_Sub_Variables adds up to 1 (Everything seems alright)

select Scaling_Variables,Scaling_Sub_Variables from Viewing_Duration_Proportions
group by Scaling_Variables,Scaling_Sub_Variables
having sum(Proportions_of_Viewing) > 1.001
or sum(Proportions_of_Viewing) < 0.999

--Granting Priviledges
grant all on Account_number_Scaling_Variables to limac;
grant all on Account_number_Scaling_Variables_recoded to limac;
grant all on Final_Viewing_Scal_Var_Jul_08_14_2013_Dayparts_Recode to limac;
grant all on Metrics_Duration to limac;
grant all on Metrics_Duration_Total to limac;
grant all on Viewing_Duration_Aggregate to limac;
grant all on Viewing_Duration_Proportions to limac;
commit;

--Putting the different new scaling segments into individual tables for cartesian join performance
select distinct isba_tv_region, isba_tv_region_v2
into V154_isba_tv_region_v2
from Final_Viewing_Scal_Var_Jul_08_14_2013_Dayparts_Recode
where isba_tv_region is not null and isba_tv_region_v2 is not null
;
select distinct hhcomposition, hhcomposition_v2
into V154_hhcomposition_v2
from Final_Viewing_Scal_Var_Jul_08_14_2013_Dayparts_Recode
where hhcomposition is not null and hhcomposition_v2 is not null
;
select distinct tenure, tenure_v2
into V154_tenure_v2
from Final_Viewing_Scal_Var_Jul_08_14_2013_Dayparts_Recode
where tenure is not null and tenure_v2 is not null
;
select distinct package, package_v2
into V154_package_v2
from Final_Viewing_Scal_Var_Jul_08_14_2013_Dayparts_Recode
where package is not null and package_v2 is not null
;
select distinct boxtype, boxtype_v3
into V154_boxtype_v3
from Final_Viewing_Scal_Var_Jul_08_14_2013_Dayparts_Recode
where boxtype is not null and boxtype_v3 is not null
select top 10* from V154_boxtype_v3
--Granting Priviledges
grant all on V154_isba_tv_region_v2 to glasera;
grant all on V154_hhcomposition_v2 to glasera;
grant all on V154_tenure_v2 to glasera;
grant all on V154_package_v2 to glasera;
grant all on V154_boxtype_v3 to glasera;
commit;

