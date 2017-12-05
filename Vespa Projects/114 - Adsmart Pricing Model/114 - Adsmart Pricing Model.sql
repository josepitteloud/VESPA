


---Part A - Create a List of relevant Spots---
if object_id('BARB_SPOT_DATA_PROJECT_114') is not null drop table BARB_SPOT_DATA_PROJECT_114;
select * into BARB_SPOT_DATA_PROJECT_114  from Neighbom.LANDMARK_MASTER_SPOT_DATA
where spot_type = 'CS'
;
--select * from BARB_SPOT_DATA_PROJECT_114;
commit;

---Part B - Add on Channel Name Details

alter table BARB_SPOT_DATA_PROJECT_114 add full_name varchar(255);
alter table BARB_SPOT_DATA_PROJECT_114 add vespa_name varchar(255);
alter table BARB_SPOT_DATA_PROJECT_114 add channel_name varchar(255);
alter table BARB_SPOT_DATA_PROJECT_114 add techedge_name varchar(255);
alter table BARB_SPOT_DATA_PROJECT_114 add infosys_name varchar(255);


update BARB_SPOT_DATA_PROJECT_114 
set a.full_name=b.full_name
,a.vespa_name=b.vespa_name
,a.channel_name=b.channel_name
,a.techedge_name=b.techedge_name
,a.infosys_name=b.infosys_name
from BARB_SPOT_DATA_PROJECT_114 as a
left outer join VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES as b
on a.service_key=b.service_key
where a.local_date_of_transmission between b.effective_from and b.effective_to
;
commit; 

---Remeove trailing spaces from Full_Name field to crete a field to match to lookup to be used to match to EPG data
alter table BARB_SPOT_DATA_PROJECT_114 add spot_channel_name varchar(255);
update BARB_SPOT_DATA_PROJECT_114 
set spot_channel_name = trim(full_name)
from BARB_SPOT_DATA_PROJECT_114 
;
commit;
create  hg index idx1 on BARB_SPOT_DATA_PROJECT_114(service_key);
create  hg index idx2 on BARB_SPOT_DATA_PROJECT_114(utc_spot_start_date_time);
create  hg index idx3 on BARB_SPOT_DATA_PROJECT_114(utc_spot_end_date_time);
--select count(*) from BARB_SPOT_DATA_PROJECT_114;
----Link Spot Data to Viewing Data
--grant all on BARB_SPOT_DATA_PROJECT_114 to public;
--------------------------------------------------------------------------------
-- PART C SETUP - Extract Viewing data
--------------------------------------------------------------------------------

/*
PART C   - Extract Viewing data
     C01 - Viewing table for period
     C03 - Clean data
     
*/

CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(3000);
CREATE VARIABLE @scanning_day           datetime;
CREATE VARIABLE @var_num_days           smallint;


-- Date range of programmes to capture
SET @var_prog_period_start  = '2012-08-14';
SET @var_prog_period_end    = '2012-08-28';
-- How many days (after end of broadcast period) to check for timeshifted viewing

--select @var_num_days;
if object_id('VESPA_Programmes_project_114') is not null drop table VESPA_Programmes_project_114;
select
      programme_trans_sk
      ,Channel_Name
      ,Epg_Title
      ,synopsis
      ,Genre_Description
      ,Sub_Genre_Description
      ,Tx_Start_Datetime_UTC
      ,Tx_End_Datetime_UTC
      ,tx_date_utc
       ,service_key
      ,datediff(mi,Tx_Start_Datetime_UTC,Tx_End_Datetime_UTC) as programme_duration
  into VESPA_Programmes_project_114 -- drop table  VESPA_Programmes
  from sk_prod.VESPA_EPG_DIM
 where tx_date_time_utc <= dateadd(day, 1, @var_prog_period_end) -- because @var_prog_period_end is a date and defaults to 00:00:00 when compared to datetimes
-- Add further filters to programmes here if required, eg, lower(channel_name) like '%bbc%'
   ;
--select top 500 * from VESPA_Programmes_project_114 where upper(channel_name) like '%ATLANTIC%';
commit;
create unique hg index idx1 on VESPA_Programmes_project_114(programme_trans_sk);
create  hg index idx2 on VESPA_Programmes_project_114(tx_date_utc);
create  hg index idx3 on VESPA_Programmes_project_114(service_key);
commit;
------ C01 - Viewing table for period
-- C01 - Viewing table for period
commit;

---Add Service key to Don's original table---

select a.*
,b.service_key
,b.channel_name
,b.time_in_seconds_since_recording as x_time_in_seconds_since_recording
into project_114_raw_viewing
from Rombaoad.V98_Tot_mins_cap_raw as a
left outer join sk_prod.vespa_events_viewed_all as b
on a.pk_viewing_prog_instance_fact=b.pk_viewing_prog_instance_fact
;
commit;
create  hg index idx1 on project_114_raw_viewing(service_key);
create  hg index idx2 on project_114_raw_viewing(viewing_starts);
create  hg index idx3 on project_114_raw_viewing(viewing_stops);

----Add a single weighting value rather than a daily weighting value (use 20th aug as mid point and 21st only partial data).

select account_number 
,max(weightings) as overall_project_weighting
into #overall_project_weighting
from project_114_raw_viewing
where cast(viewing_starts as date) ='2012-08-20'
group by account_number
;
commit;
create  hg index idx1 on #overall_project_weighting(account_number);

alter table project_114_raw_viewing add overall_project_weighting real default 0;
update project_114_raw_viewing
set overall_project_weighting=case when b.overall_project_weighting is null then 0 else b.overall_project_weighting end
from project_114_raw_viewing as a
left outer join #overall_project_weighting as b
on a.account_number =b.account_number
;
commit;

---Create Base table of all accounts used for analysis
--drop table project_114_base_Accounts;
select account_number
,overall_project_weighting
into project_114_base_Accounts
from project_114_raw_viewing
where overall_project_weighting>0
group by account_number
,overall_project_weighting
;
commit;
create  hg index idx1 on project_114_base_Accounts(account_number);



---Add on Adsmart attributes----
--drop table #adsmart_attributes;
select account_number
,isba_tv_region 
,case   when ent_extra=1 then '1: Entertainment Extra' else '2: No Entertainment Extra' end as entertainment_extra_status
,case   when sky_sports_1=1 and  sky_sports_2=1 and movies_1=1 and movies_2=1 then '1: All Premiums'
        when sky_sports_1=1 and  sky_sports_2=1 and movies_1=0 and movies_2=0  then '2: Dual Sports'
        when sky_sports_1=0 and  sky_sports_2=0 and movies_1=1 and movies_2=1  then '3: Dual Movies'
        when sky_sports_1+sky_sports_2+movies_1+movies_2>0  then '4: Other Premiums' else '5: No Premiums' end as premium_details
,h_mosaic_uk_2009_group
,H_AFFLUENCE
,demographic
into #adsmart_attributes
from mustaphs.AdSmart_20121016
;
commit;

exec sp_create_tmp_table_idx '#adsmart_attributes', 'account_number';
---Create table of one record per service_instance_id---
SELECT account_number
,service_instance_id
,min(stb.x_pvr_type) as pvr_type
,min(stb.x_box_type) as box_type
,min(stb.x_description) as description_x
,min(stb.x_manufacturer) as manufacturer
,min(stb.x_model_number) as model_number
  INTO #boxes_with_model_info -- drop table #boxes
  FROM  sk_prod.CUST_SET_TOP_BOX AS stb 
where box_replaced_dt = '9999-09-09'
group by account_number
,service_instance_id
 ;

commit;
exec sp_create_tmp_table_idx '#boxes_with_model_info', 'service_instance_id';

---Create src_system_id lookup
--drop table  #subs_details;
select src_system_id
,min(cast(si_external_identifier as integer)) as subscriberid
,max(case when si_service_instance_type in ('Primary DTV') then 1 else 0 end) as primary_box
into #subs_details
from
sk_prod.CUST_SERVICE_INSTANCE as b
where si_service_instance_type in ('Primary DTV','Secondary DTV (extra digiboxes)')
group by src_system_id
;
commit;
exec sp_create_tmp_table_idx '#subs_details', 'src_system_id';

select a.*
,b.subscriberid
,b.primary_box
 ,CASE  WHEN pvr_type ='PVR6'                                THEN 1
             WHEN pvr_type ='PVR5'                                THEN 1
             WHEN pvr_type ='PVR4' AND manufacturer = 'Samsung' THEN 1
             WHEN pvr_type ='PVR4' AND manufacturer = 'Pace'    THEN 1
                                                                    ELSE 0
       END AS Adsmartable
into #box_details_by_subscriber_id
from #boxes_with_model_info as a
left outer join #subs_details as b
on a.service_instance_id=b.src_system_id
;
commit;

commit;
exec sp_create_tmp_table_idx '#box_details_by_subscriber_id', 'subscriberid';
commit;


----Add Back on to Main Table----
alter table project_114_base_Accounts add isba_tv_region varchar(50);
alter table project_114_base_Accounts add entertainment_extra_status varchar(50);
alter table project_114_base_Accounts add premium_details varchar(50);
alter table project_114_base_Accounts add mosaic_group varchar(50);
--alter table project_114_base_Accounts delete mosaic_group ;
alter table project_114_base_Accounts add HH_Affluence varchar(2);

update project_114_base_Accounts
set isba_tv_region=b.isba_tv_region
,entertainment_extra_status=b.entertainment_extra_status
,premium_details=b.premium_details
,mosaic_group=b.demographic
,HH_Affluence = H_AFFLUENCE
from project_114_base_Accounts  as a
left outer join #adsmart_attributes as b
on a.account_number=b.account_number
commit;




alter table project_114_base_Accounts add Adsmartable tinyint;

select account_number
,max(Adsmartable) adsmartable_hh
into #admartable_hh_details
from #box_details_by_subscriber_id
group by account_number
;
commit;
exec sp_create_tmp_table_idx '#admartable_hh_details', 'account_number';
commit;

update project_114_base_Accounts
set adsmartable=b.adsmartable_hh
from project_114_base_Accounts as a
left outer join #admartable_hh_details as b
on a.account_number=b.account_number
commit;
--select count(*) from project_114_base_Accounts;
--Create table for use in Spot analysis only containing viewing for channels where spots being analysed--

select distinct service_key
into #distinct_service_keys
from  BARB_SPOT_DATA_PROJECT_114
;
commit;

commit;
exec sp_create_tmp_table_idx '#distinct_service_keys', 'service_key';

--Create Table of Data for analysis in spots

select a.*
into project_114_raw_viewing_analysis_channels_only
from project_114_raw_viewing as a
left outer join #distinct_service_keys as b
on a.service_key=b.service_key
where b.service_key is not null
;
commit;


--select top 100 * from sk_prod.vespa_events_viewed_all;
--select top 100 * from project_114_raw_viewing;
--select top 100 * from project_114_raw_viewing_test;
--select top 100 * from Rombaoad.V98_Tot_mins_cap_raw;
--select top 100 * from BARB_SPOT_DATA_PROJECT_114;

--select count(*) from Rombaoad.V98_Tot_mins_cap_raw;
--select channel_name , count(*) from project_114_raw_viewing group by channel_name;

--
drop table  project_114_raw_viewing_test; select * into project_114_raw_viewing_test from project_114_raw_viewing where cast(viewing_starts as date)='2012-08-17'; commit;
commit;
create  hg index idx1 on project_114_raw_viewing_test(service_key);
create  hg index idx2 on project_114_raw_viewing_test(viewing_starts);
create  hg index idx3 on project_114_raw_viewing_test(viewing_stops);

drop table  BARB_SPOT_DATA_PROJECT_114_Test; select * into BARB_SPOT_DATA_PROJECT_114_Test from BARB_SPOT_DATA_PROJECT_114 where cast(utc_spot_start_date_time as date)='2012-08-17'; commit;

commit;
create  hg index idx1 on BARB_SPOT_DATA_PROJECT_114_Test(service_key);
create  hg index idx2 on BARB_SPOT_DATA_PROJECT_114_Test(utc_spot_start_date_time);
create  hg index idx3 on BARB_SPOT_DATA_PROJECT_114_Test(utc_spot_end_date_time);
commit;
--select count(*) from BARB_SPOT_DATA_PROJECT_114;
--select top 500 * from  BARB_SPOT_DATA_PROJECT_114_Test;

if object_id('Project_114_viewing_table_dump') is not null drop table Project_114_viewing_table_dump;

select vw.*
    ,spot.utc_spot_start_date_time
    ,spot.utc_spot_end_date_time
    ,spot.utc_break_start_date_time
    ,spot.utc_break_end_date_time
,spot.full_name
,spot.vespa_name
,spot.techedge_name 
,spot.infosys_name 

,spot.spot_position_in_break
,spot.no_spots_in_break 
,spot.spot_duration
-- TO BE ADDED,spot.clearcast_commerical_no
into Project_114_viewing_table_dump
from  project_114_raw_viewing_test  as vw 
inner join BARB_SPOT_DATA_PROJECT_114_Test as spot
        on  vw.service_key=spot.service_key
where  
    (          dateadd(second,x_time_in_seconds_since_recording*-1,viewing_starts) between utc_spot_start_date_time and utc_spot_end_date_time 
        or       dateadd(second,x_time_in_seconds_since_recording*-1,viewing_stops) between utc_spot_start_date_time and utc_spot_end_date_time 
        or       dateadd(second,x_time_in_seconds_since_recording*-1,viewing_starts) < utc_spot_start_date_time and  dateadd(second,x_time_in_seconds_since_recording*-1,viewing_stops)> utc_spot_end_date_time 
    )
;
--select top 500 * from Rombaoad.V98_Tot_mins_cap_raw;
--select x_time_in_seconds_since_recording , count(*) from Project_114_viewing_table_dump group by x_time_in_seconds_since_recording;
--select top 500 * from Project_114_viewing_table_dump;
commit;
--select utc_spot_start_date_time,channel_name , count(*) as records from Project_114_viewing_table_dump group by utc_spot_start_date_time,channel_name order by records desc;

commit;
create  hg index idx1 on Project_114_viewing_table_dump(subscriber_id);
create  hg index idx2 on Project_114_viewing_table_dump(account_number);
create  hg index idx3 on Project_114_viewing_table_dump(service_key);
commit;
---Create Summary of details per spot---


if object_id('Project_114_viewing_table_by_spot') is not null drop table Project_114_viewing_table_by_spot;
select subscriber_id
,account_number
,service_key
,utc_spot_start_date_time
,spot_duration
,spot_position_in_break
,max(weightings) as weighting_val
into Project_114_viewing_table_by_spot
from Project_114_viewing_table_dump
where  x_time_in_seconds_since_recording=0
  and dateadd(second,x_time_in_seconds_since_recording*-1,viewing_starts) <= utc_spot_start_date_time
   and  dateadd(second,x_time_in_seconds_since_recording*-1,viewing_stops)> utc_spot_end_date_time 
group by subscriber_id
,account_number
,service_key
,utc_spot_start_date_time
,spot_duration
,spot_position_in_break
;





---Create Output---
select case when isba_tv_region is null then 'Unknown' else isba_tv_region end as isba
,entertainment_extra_status
,premium_details
,mosaic_group
,HH_Affluence
,spot_duration
,spot_position_in_break
,case when adsmartable is null then 0 else adsmartable end as adsmartable_box
,sum(weighting_val*(spot_duration/30)) as thirty_second_impact_equivalents
,count(distinct subscriber_id) as boxes
,count(distinct account_number) as accounts
into #data_for_output
from Project_114_viewing_table_by_spot
group by isba
,entertainment_extra_status
,premium_details
,mosaic_group
,HH_Affluence
,spot_duration
,spot_position_in_break
,adsmartable_box
;
--select distinct isba_tv_region from  Project_114_viewing_table_by_spot
select * from #data_for_output where thirty_second_impact_equivalents>0;

output to 'C:\Users\barnetd\Documents\Project 114 - Adsmart\adsmart output for pivot.csv' format ascii;

commit;

---Create simple univariate profiles---
--select top 100 * from project_114_base_Accounts;

select case when isba_tv_region is null then 'Unknown' else isba_tv_region end as isba
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by isba
order by isba
;

---Repeat for ISBA Viewing Data---
select case when isba_tv_region is null then 'Unknown' else isba_tv_region end as isba
,sum(spot_duration/30) as unweighted_impacts
,sum(weighting_val*(spot_duration/30)) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_box_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*weighting_val*(spot_duration/30) end) as adsmartable_box_weighted_impacts
from Project_114_viewing_table_by_spot
group by isba
order by isba
;

commit;

--Repeat for Package Base Acounts---
select entertainment_extra_status
,premium_details
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by entertainment_extra_status
,premium_details
order by entertainment_extra_status
,premium_details

---Repeat for ISBA Viewing Data---
select entertainment_extra_status
,premium_details
,sum(spot_duration/30) as unweighted_impacts
,sum(weighting_val*(spot_duration/30)) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_box_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*weighting_val*(spot_duration/30) end) as adsmartable_box_weighted_impacts
from Project_114_viewing_table_by_spot
group by entertainment_extra_status
,premium_details
order by entertainment_extra_status
,premium_details
;


---Repeat for Mosaic Base---
select mosaic_group
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by mosaic_group
order by mosaic_group
;

--Repeat for Mosaic Viewing Data
select mosaic_group
,sum(spot_duration/30) as unweighted_impacts
,sum(weighting_val*(spot_duration/30)) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_box_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*weighting_val*(spot_duration/30) end) as adsmartable_box_weighted_impacts
from Project_114_viewing_table_by_spot
group by mosaic_group
order by mosaic_group
;


---Repeat for Affluence---
select HH_Affluence
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by HH_Affluence
order by HH_Affluence
;

--Repeat for Viewing by Affluence
select HH_Affluence
,sum(spot_duration/30) as unweighted_impacts
,sum(weighting_val*(spot_duration/30)) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_box_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*weighting_val*(spot_duration/30) end) as adsmartable_box_weighted_impacts
from Project_114_viewing_table_by_spot
group by HH_Affluence
order by HH_Affluence
;




/*
--drop table #account_level_summary;
select account_number
,case when isba_tv_region is null then 'Unknown' else isba_tv_region end as isba
,entertainment_extra_status
,premium_details
,mosaic_group
,HH_Affluence
,max(weighting_val) as hh_weighting
,max(case when adsmartable is null then 0 else adsmartable end) as adsmartable_hh
,sum(case when adsmartable=1 then weighting_val*(spot_duration/30) else 0 end) as adsmartable_box_impacts
,sum(case when (adsmartable=0 or adsmartable is null) then weighting_val*(spot_duration/30) else 0 end) as non_adsmartable_box_impacts
into #account_level_summary
from Project_114_viewing_table_by_spot
group by account_number
, isba
,entertainment_extra_status
,premium_details
,mosaic_group
,HH_Affluence
;

select sum(adsmartable_hh) as adsmartable_hh
,sum(hh_weighting) as total hh

*/

--select cast(viewing_starts as date) as daydetail , count(*) from Project_114_viewing_table_dump group by daydetail order by daydetail ;
---Create Universal Capping Value (use 20th August as mid-point of analysis).

select account_number
,max(weighting_val) as weighting
into #midpoint_for_weighting
from Project_114_viewing_table_dump
where cast(viewing_starts as date) ='2012-08-20'
group by account_number
;



--select  * from Rombaoad.V98_Tot_mins_cap_raw where account_number = '210023524098';



------Part II - Add on extra adsmart variables----

alter table Project_114_viewing_table_by_spot 
add  Sky_Reward_L12            INTEGER         default 0 
,add  box_type                  VARCHAR(50)     default 'missing'
,add  bt_fibre_area             varchar(10)
,add  Cable_area                VARCHAR(3)      default 'NA' 
--ID&V Not Availabale
,add  Family_Lifestage          VARCHAR(2)      default 'M' 
,add  exchange_id               varchar(10)
,add  exchange_status           varchar(10)
,add  exchange_unbundled        varchar(10)
,add  Financial_outlook         VARCHAR(50)     default 'missing' 
,add  government_region         varchar(50)
,add  HomeOwner                 VARCHAR(50)     default 'missing'  
,add  h_lifestage               VARCHAR(50)     default 'missing' 
,add  Kids_Aged_LE4             varchar(1)      default 'N'          -- data defined from AXCIOM
,add  Kids_Aged_5to11           varchar(1)      default 'N'          -- data defined from AXCIOM
,add  Kids_Aged_12to17          varchar(1)      default 'N'          -- data defined from AXCIOM
,add  MIRROR_MEN_MIN            VARCHAR(5)                           -- data defined from Experians ConsumerView
,add  MIRROR_WOMEN_MIN          VARCHAR(5)                           -- data defined from Experians ConsumerView
,add  Mirror_has_children       VARCHAR(50)     default 'missing'    -- data defined from Experians ConsumerView
,add  Mirror_ABC1               VARCHAR(1)      default 'M'          -- data defined from Experians ConsumerView
,add  Total_miss_pmt            INTEGER         default 0            -- Number of unbilled payments Last 12 months
,add  Movies_downgrades         INTEGER         default 0            -- Movies downgrades L12
,add  Sports_downgrades         INTEGER         default 0            -- Sports downgrades L12
,add  current_offer             INTEGER         default 0            -- account currently in offer
,add  barb_desc_itv             VARCHAR(50)                          -- BARB ITV Description
,add  Sky_Go_Reg                INTEGER         default 0            -- Sky Go number of downloads 12 months
 ,add  Sky_cust_life             VARCHAR(20)     default 'E) missing' -- based on Sky Tenure
,add  TA_attempts               INTEGER         default 0            -- TA attempts Last 12 months
,add  value_segment             VARCHAR(50)     default 'missing'    -- Current Value Segment
;
commit;

update  Project_114_viewing_table_by_spot 
set  Sky_Reward_L12   =b.Sky_Reward_L12  
,   box_type   =b.box_type          
,   bt_fibre_area =b.bt_fibre_area       
,   Cable_area =b.Cable_area        
--ID&V Not Availabale
,   Family_Lifestage  =b.Family_Lifestage   
,   exchange_id    =b.exchange_id      
,   exchange_status   =b.exchange_status  
,   exchange_unbundled = b.exchange_unbundled  
,   Financial_outlook   =b.Financial_outlook
,   government_region   =b.government_region
,   HomeOwner    =b.HomeOwner    
,   h_lifestage  =b.h_lifestage      
,   Kids_Aged_LE4  =b.Kids_Aged_LE4       
,   Kids_Aged_5to11   =b.Kids_Aged_5to11   
,   Kids_Aged_12to17   =b.Kids_Aged_12to17 
,   MIRROR_MEN_MIN   =b.MIRROR_MEN_MIN  
,   MIRROR_WOMEN_MIN   =b.MIRROR_WOMEN_MIN
,   Mirror_has_children  =b.Mirror_has_children
,   Mirror_ABC1  =b.Mirror_ABC1       
,   Total_miss_pmt =b.Total_miss_pmt    
,   Movies_downgrades  =b.Movies_downgrades
,   Sports_downgrades =b.Sports_downgrades
,   current_offer =b.current_offer  
,   barb_desc_itv  =b.barb_desc_itv
,   Sky_Go_Reg   =b.Sky_Go_Reg   
 ,   Sky_cust_life  =b.Sky_cust_life
,   TA_attempts  =b.TA_attempts  
,   value_segment  =b.value_segment

from    Project_114_viewing_table_by_spot as a
left outer join  mustaphs.AdSmart_20121016 as b
on a.account_number = b.account_number
commit;






/*



select top 100 * from Neighbom.LANDMARK_MASTER_SPOT_DATA;

select local_date_of_transmission , count(*) as records  from Neighbom.LANDMARK_MASTER_SPOT_DATA 
group by local_date_of_transmission 
order by local_date_of_transmission;

commit;

select count(*) 
from Neighbom.LANDMARK_MASTER_SPOT_DATA
where spot_type = 'CS'


commit;
select top 100 * into #test from Rombaoad.V98_Tot_mins_cap_raw;commit; select * from #test

select top 100 *  from sk_prod.VESPA_EPG_DIM where pk_viewing_prog_instance_fact =10405396439;
select top 100 *  from sk_prod.VESPA_EVENTS_VIEWED_ALL where pk_viewing_prog_instance_fact =10405396439;
select count(*) , count (distinct pk_viewing_prog_instance_fact) from sk_prod.VESPA_EVENTS_VIEWED_ALL 


account_number,dk_programme_instance_dim,viewing_starts,viewing_stops,broadcast_date_utc,viewing_duration,programme_instance_duration,genre_description,sub_genre_description,pk_viewing_prog_instance_fact,subscriber_id,cb_key_household,scaling_segment_ID,weightings
'210023524098',46619632,'2012-08-13 12:52:44.000','2012-08-13 12:52:51.000','2012-08-13',7,3600,'Entertainment','Travel',10405396439,990982,2523595902351835136,200302,13.280093


into project_

select top 100 * from  mustaphs.AdSmart_20121016;


vespa_analysts.SC2_weightings




SELECT base.account_number
--      ,x_pvr_type
--      ,x_manufacturer
      ,CASE  WHEN x_pvr_type ='PVR6'                                THEN 1
             WHEN x_pvr_type ='PVR5'                                THEN 1
             WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Samsung' THEN 1
             WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Pace'    THEN 1
--             WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Thomson' THEN 1
                                                                    ELSE 0
       END AS Adsmartable
      ,SUM(Adsmartable) AS T_AdSm_box
INTO #SetTop
FROM   sk_prod.CUST_SET_TOP_BOX  AS SetTop
        inner join AdSmart as Base
         on SetTop.account_number = Base.account_number
         where box_replaced_dt = '9999-09-09'
         GROUP BY base.account_number
                ,x_pvr_type
                ,x_manufacturer
                ,box_replaced_dt;

select top 100 * from sk_prod.CUST_SET_TOP_BOX;

commit;

SELECT account_number
,service_instance_id
,x_pvr_type 
,x_manufacturer
,max(dw_created_dt) as max_dw_created_dt
  INTO #boxes -- drop table #boxes
  FROM sk_prod.CUST_SET_TOP_BOX  
where box_replaced_dt = '9999-09-09'

 --(box_installed_dt <= cast('2012-01-15'  as date) 
 --  AND box_replaced_dt   > cast('2012-01-15'  as date)) or box_installed_dt is null
group by account_number
,service_instance_id
,x_pvr_type 
,x_manufacturer
 ;
select count(distinct account_number) from sk_prod.CUST_SET_TOP_BOX  
--select top 500 * from Rombaoad.V98_Tot_mins_cap_raw;
select * from sk_prod.vespa_epg_dim where programme_trans_sk=10405396439
*/
commit;



grant select on Project_114_viewing_table_dump to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh
, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg,vespa_analysts;
commit;


grant select on BARB_SPOT_DATA_PROJECT_114  to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh
, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg,vespa_analysts;
commit;

grant select on BARB_SPOT_DATA_PROJECT_114_TEST  to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh
, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg,vespa_analysts;
commit;

grant select on BARB_SPOT_DATA_PROJECT_114  to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh
, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg,vespa_analysts;
commit;

grant select on project_114_raw_viewing  to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh
, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg,vespa_analysts;
commit;


grant select on project_114_base_Accounts  to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh
, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg,vespa_analysts;
commit;

grant select on project_114_raw_viewing_analysis_channels_only  to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh
, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg,vespa_analysts;
commit;
 




