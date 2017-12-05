/*------------------------------------------------------------------------------
        Project: V115 - Adsmart Pricing Model
        Version: 2
        Created: 20121101
        Lead: Sarah Moore
        Analyst: Dan Barnett
        SK Prod: 4
*/------------------------------------------------------------------------------
/*
        Purpose
        -------
        Create a template process for Spot analysis (in This case for Barclays Cash ISA)

        SECTIONS
        --------
        PART A   - Create List of relevant Spots
        PART B   - Add on Channel details (according to Service Key Lookup)
        PART C   - Extract Viewing data for relevant period
	Part D 	 - Create Table of all accounts to add demographics and weighting details
	Part E 	 - Loop Creating all viewed spots in period
	Part F	 - Add on remaining variables to Accounts Table
	Part G	- Create simple univariate profiles---
        Tables
        -------
      
*/

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

---Add a unique identifier to the spot data

alter table BARB_SPOT_DATA_PROJECT_114 add spot_column integer identity;
--select top 100 * from BARB_SPOT_DATA_PROJECT_114 ;


--select count(*) from BARB_SPOT_DATA_PROJECT_114;
----Link Spot Data to Viewing Data
--grant all on BARB_SPOT_DATA_PROJECT_114 to public;


	---PART C   - Extract Viewing data for relevant period

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
--select count (distinct account_number) from project_114_raw_viewing
---Part D 	 - Create Table of all accounts to add demographics and weighting details

---Create Base table of all accounts used for analysis

if object_id('project_114_base_Accounts') is not null drop table project_114_base_Accounts;
select a.account_number
,b.weighting as overall_project_weighting
into project_114_base_Accounts
from  vespa_analysts.SC2_intervals as a
inner join vespa_analysts.SC2_weightings as b
on  cast('2012-08-20' as date) = b.scaling_day
and a.scaling_segment_ID = b.scaling_segment_ID
and cast('2012-08-20' as date) between a.reporting_starts and a.reporting_ends
;
commit;


create  hg index idx1 on project_114_base_Accounts(account_number);
--select sum(overall_project_weighting) from  project_114_base_Accounts;

select * into adsmart from mustaphs.AdSmart_20121107;commit;

---Add on Adsmart attributes----
--drop table #adsmart_attributes;
select account_number
,isba_tv_region 
--,case   when ent_extra=1 then '1: Entertainment Extra' else '2: No Entertainment Extra' end as entertainment_extra_status
,case   when sky_sports_1=1 and  sky_sports_2=1 and movies_1=1 and movies_2=1 then '1: All Premiums'
        when sky_sports_1=1 and  sky_sports_2=1 and movies_1=0 and movies_2=0  then '2: Dual Sports'
        when sky_sports_1=0 and  sky_sports_2=0 and movies_1=1 and movies_2=1  then '3: Dual Movies'
        when sky_sports_1+sky_sports_2+movies_1+movies_2>0  then '4: Other Premiums' else '5: No Premiums' end as premium_details
,sky_sports_1
,sky_sports_2
,movies_1
,movies_2
,h_mosaic_uk_2009_group
,H_AFFLUENCE
,demographic
,T_AdSm_box
into #adsmart_attributes
from AdSmart
;
commit;
--select top 100 *  from AdSmart;
exec sp_create_tmp_table_idx '#adsmart_attributes', 'account_number';

--Create Package Details for actual date of analysis (20th Aug 2012)


SELECT csh.account_number
      ,csh.cb_key_household
      ,csh.first_activation_dt
      ,CASE WHEN  cel.mixes = 0                     THEN 'A) 0 Mixes'
            WHEN  cel.mixes = 1
             AND (style_culture = 1 OR variety = 1) THEN 'B) 1 Mix - Variety or Style&Culture'
            WHEN  cel.mixes = 1                     THEN 'C) 1 Mix - Other'
            WHEN  cel.mixes = 2
             AND  style_culture = 1
             AND  variety = 1                       THEN 'D) 2 Mixes - Variety and Style&Culture'
            WHEN  cel.mixes = 2
             AND (style_culture = 0 OR variety = 0) THEN 'E) 2 Mixes - Other Combination'
            WHEN  cel.mixes = 3                     THEN 'F) 3 Mixes'
            WHEN  cel.mixes = 4                     THEN 'G) 4 Mixes'
            WHEN  cel.mixes = 5                     THEN 'H) 5 Mixes'
            WHEN  cel.mixes = 6                     THEN 'I) 6 Mixes'
            ELSE                                         'J) Unknown'
        END as mix_type
       ,CAST(NULL AS VARCHAR(20)) AS new_package
  INTO #mixes
  FROM sk_prod.cust_subs_hist as csh
       INNER JOIN sk_prod.cust_entitlement_lookup as cel
               ON csh.current_short_description = cel.short_description
 WHERE csh.subscription_sub_type ='DTV Primary Viewing'
   AND csh.subscription_type = 'DTV PACKAGE'
   AND csh.status_code in ('AC','AB','PC')
   AND csh.effective_from_dt <= '2012-08-20'
   AND csh.effective_to_dt   >  '2012-08-20'
   AND csh.effective_from_dt != csh.effective_to_dt
;

UPDATE #mixes
   Set new_package = CASE WHEN mix_type IN ( 'A) 0 Mixes'
                                            ,'B) 1 Mix - Variety or Style&Culture'
                                            ,'D) 2 Mixes - Variety and Style&Culture')
                          THEN 'Entertainment'

                          WHEN mix_type IN ( 'C) 1 Mix - Other'
                                            ,'E) 2 Mixes - Other Combination'
                                            ,'F) 3 Mixes'
                                            ,'G) 4 Mixes'
                                            ,'H) 5 Mixes'
                                            ,'I) 6 Mixes')
                          THEN  'Entertainment Extra'
                          ELSE  'Unknown'
                     END;

commit;

exec sp_create_tmp_table_idx '#mixes', 'account_number';

----Add Back on to Main Table----
alter table project_114_base_Accounts add isba_tv_region varchar(50);
alter table project_114_base_Accounts add entertainment_extra_status varchar(50);
alter table project_114_base_Accounts add premium_details varchar(50);
alter table project_114_base_Accounts add mosaic_group varchar(50);
--alter table project_114_base_Accounts delete mosaic_group ;
alter table project_114_base_Accounts add HH_Affluence varchar(2);
alter table project_114_base_Accounts add Adsmartable tinyint;

alter table project_114_base_Accounts add sky_sports_1 tinyint;
alter table project_114_base_Accounts add sky_sports_2 tinyint;
alter table project_114_base_Accounts add movies_1 tinyint;
alter table project_114_base_Accounts add movies_2 tinyint;

update project_114_base_Accounts
set isba_tv_region=b.isba_tv_region
--,entertainment_extra_status=b.entertainment_extra_status
,premium_details=b.premium_details
,mosaic_group=b.demographic
,HH_Affluence = H_AFFLUENCE
,Adsmartable=case when b.T_AdSm_box>0 then 1 else 0 end
,sky_sports_1=b.sky_sports_1
,sky_sports_2=b.sky_sports_2
,movies_1=b.movies_1
,movies_2=b.movies_2
from project_114_base_Accounts  as a
left outer join #adsmart_attributes as b
on a.account_number=b.account_number;
commit;

update project_114_base_Accounts
set premium_details=case when premium_details is null then '5: No Premiums   ' else premium_details end
from project_114_base_Accounts;
commit;


update project_114_base_Accounts
set entertainment_extra_status=case when b.new_package = 'Entertainment Extra' then '1: Entertainment Extra' else '2: No Entertainment Extra' end
from project_114_base_Accounts  as a
left outer join #mixes as b
on a.account_number=b.account_number;




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
--select top 500 * from project_114_raw_viewing_analysis_channels_only;
--select broadcast_date_utc , count(*) as records from project_114_raw_viewing_analysis_channels_only group by broadcast_date_utc order by broadcast_date_utc;
--select top 100 * from sk_prod.vespa_events_viewed_all;
--select top 100 * from project_114_raw_viewing;
--select top 100 * from project_114_raw_viewing_test;
--select top 100 * from Rombaoad.V98_Tot_mins_cap_raw;
--select top 100 * from BARB_SPOT_DATA_PROJECT_114;

--select count(*) from Rombaoad.V98_Tot_mins_cap_raw;
--select channel_name , count(*) from project_114_raw_viewing group by channel_name;

--
create  hg index idx1 on project_114_raw_viewing_analysis_channels_only(service_key);
create  hg index idx2 on project_114_raw_viewing_analysis_channels_only(viewing_starts);
create  hg index idx3 on project_114_raw_viewing_analysis_channels_only(viewing_stops);


--select count(*) from BARB_SPOT_DATA_PROJECT_114;
--select top 500 * from  BARB_SPOT_DATA_PROJECT_114_Test;
--select x_time_in_seconds_since_recording , count(*) as records from project_114_raw_viewing_analysis_channels_only group by x_time_in_seconds_since_recording

---Part E 	 - Loop Creating all viewed spots in period

if object_id('Project_114_viewing_table') is not null drop table Project_114_viewing_table;

--
create table Project_114_viewing_table
(spot_column integer
,account_number varchar(20)
,subscriber_id numeric (8,0)
);

commit;

----Create table of current days viewing

create variable @viewing_start_time datetime;
create variable @viewing_end_time datetime;
commit;
set @viewing_start_time = cast ('2012-08-14 00:00:00' as datetime);
set @viewing_end_time = cast ('2012-08-14 23:59:59' as datetime);

---Loop by Minute---
    
WHILE @viewing_start_time < '2012-08-29 00:00:00' LOOP
select *
into project114_single_day_viewing
from project_114_raw_viewing_analysis_channels_only
where viewing_starts between @viewing_start_time and @viewing_end_time
;

---Create table of spots for that day (+ few extra hours for viewing that overlaps days--
select * into project114_single_day_spots
 from BARB_SPOT_DATA_PROJECT_114
where utc_spot_end_date_time  between @viewing_start_time and dateadd(hour,4,@viewing_end_time);

insert into Project_114_viewing_table

select spot.spot_column
,vw.account_number
,vw.subscriber_id
from  project114_single_day_viewing as vw 
inner join project114_single_day_spots as spot
        on  vw.service_key=spot.service_key
where  
 (     viewing_starts <= utc_spot_start_date_time and  viewing_stops> utc_spot_end_date_time 
    )
;



drop table project114_single_day_viewing;
drop table project114_single_day_spots;

set @viewing_start_time= dateadd(day,1,@viewing_start_time);
set @viewing_end_time= dateadd(day,1,@viewing_end_time)
;

    COMMIT;

    END LOOP;

--select * from Project_114_viewing_table;
--select * from project114_single_day_spots;



commit;
--select utc_spot_start_date_time,channel_name , count(*) as records from Project_114_viewing_table_dump group by utc_spot_start_date_time,channel_name order by records desc;

commit;
create  hg index idx1 on Project_114_viewing_table(subscriber_id);
create  hg index idx2 on Project_114_viewing_table(account_number);
create  hg index idx3 on Project_114_viewing_table(service_key);
commit;


grant select on Project_114_viewing_table to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh
, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg,vespa_analysts,neighbom;
commit;


grant select on BARB_SPOT_DATA_PROJECT_114  to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh
, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg,vespa_analysts,neighbom;
commit;

grant select on project_114_raw_viewing  to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh
, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg,vespa_analysts,neighbom;
commit;


grant select on project_114_base_Accounts  to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh
, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg,vespa_analysts,neighbom;
commit;

grant select on project_114_raw_viewing_analysis_channels_only  to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh
, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg,vespa_analysts,neighbom;
commit;
---Create Summary of details per spot---
--Add back on spot details to Project_114_viewing_table (e.g., Position in Break/Duration etc.,)

alter table Project_114_viewing_table add service_key int;
alter table Project_114_viewing_table add utc_spot_start_date_time datetime;
alter table Project_114_viewing_table add utc_spot_end_date_time datetime;
alter table Project_114_viewing_table add spot_duration int;
alter table Project_114_viewing_table add spot_channel_name varchar(255);
alter table Project_114_viewing_table add spot_position_in_break int;

update Project_114_viewing_table
set a.service_key=b.service_key
,a.utc_spot_start_date_time=b.utc_spot_start_date_time
,a.utc_spot_end_date_time=b.utc_spot_end_date_time
,a.spot_duration=b.spot_duration
,a.spot_channel_name=b.spot_channel_name
,a.spot_position_in_break=b.spot_position_in_break
from Project_114_viewing_table as a
left outer join BARB_SPOT_DATA_PROJECT_114 as b
on a.spot_column=b.spot_column
;
commit;


---Part F	 - Add on remaining variables to Accounts Table

----Add on other variables for analysis---

alter table project_114_base_Accounts
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
--,add  h_lifestage               VARCHAR(50)     default 'Unclassified' 
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
----Extra Added 20121112--
alter table project_114_base_Accounts
add sky_id varchar(10) default 'Unknown'
,add id_v integer 
,add bb varchar(10) default 'Unknown'
,add talk varchar(10) default 'Unknown'
,add HD varchar(10) default 'Unknown'
,add Anytime_plus varchar(10) default 'Unknown'
,add Pending_cancel varchar(10) default 'Unknown'
,add ESPN varchar(10) default 'Unknown'
,add Multiroom varchar(10) default 'Unknown'
;
commit;

update  project_114_base_Accounts
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
,   HomeOwner    =CASE WHEN b.tenure in ('1','2') THEN  'No'
                                  WHEN b.tenure =  ('0')     THEN  'Yes'
                                  ELSE null
                                  END  
--,   h_lifestage  =b.h_lifestage      
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

---New Variables to Include 
,id_v=model_score
, sky_id                 =    CASE WHEN b.sky_id = 0 THEN  'No'
                                  WHEN b.sky_id = 1 THEN  'Yes'
                                  ELSE 'Unknown'
                                  END
, bb= case when bb_type is null then 'No' when bb_type='NA' then 'No' else 'Yes' end
,talk= case when talk_product is null then 'No' when talk_product='NA' then 'No' else 'Yes' end 
, HD=case when b.HDTV=1 then 'Yes' when  b.HDTV=0 then 'No' else  'Unknown' end

, Pending_cancel=case when b.Pending_cancel =1 then 'Yes' when  b.Pending_cancel=0 then 'No' else  'Unknown' end
, ESPN =case when b.ESPN_Subscribers=1 then 'Yes' when  b.ESPN_Subscribers=0 then 'No' else  'Unknown' end
, Multiroom =case when b.multiroom=1 then 'Yes' when  b.multiroom=0 then 'No' else  'Unknown' end
, Anytime_plus =case when b.Anytime_plus=1 then 'Yes' when  b.Anytime_plus=0 then 'No' else  'Unknown' end

from    project_114_base_Accounts as a
left outer join  AdSmart as b
on a.account_number = b.account_number
commit;

---Add on variables not picked up in original code

Alter table project_114_base_Accounts Add  HOUSEHOLD_COMPOSITION varchar(35)  default 'UNCLASSIFIED';
--alter table project_114_base_Accounts delete  h_lifestage     ;
alter table project_114_base_Accounts add  h_lifestage               VARCHAR(50)     default 'g) Unclassified' ;
alter table project_114_base_Accounts add  cust_tenure               VARCHAR(50)     default 'Unknown' ;
alter table project_114_base_Accounts add  h_lifestage_full               VARCHAR(50)     default 'Unclassified' ;
alter table project_114_base_Accounts add  current_package               VARCHAR(50)     default 'Unknown' ;
alter table project_114_base_Accounts add  disney               integer     default 0 ;
alter table project_114_base_Accounts add  skyplus               integer     default 0 ;



--select distinct HOUSEHOLD_COMPOSITION from project_114_base_Accounts;
update project_114_base_Accounts
set HOUSEHOLD_COMPOSITION=b.HOUSEHOLD_COMPOSITION
,   h_lifestage =         CASE WHEN Lifestage = '00' and Head_of_HH_age_band = '16 to 24'  THEN 'a) Very young adults (Age 16-24)' --Very young family
                                  WHEN Lifestage = '01' and Head_of_HH_age_band = '16 to 24'  THEN 'a) Very young adults (Age 16-24)' --Very young single
                                  WHEN Lifestage = '02' and Head_of_HH_age_band = '16 to 24'  THEN 'a) Very young adults (Age 16-24)' --Very young homesharers
                                  WHEN Lifestage = '03' and Head_of_HH_age_band = '25 to 35'  THEN 'b) Young adults (25-35)'      --Young family
                                  WHEN Lifestage = '04' and Head_of_HH_age_band = '25 to 35'  THEN 'b) Young adults (25-35)'      --Young single
                                  WHEN Lifestage = '05' and Head_of_HH_age_band = '25 to 35'  THEN 'b) Young adults (25-35)'      --Young homesharers
                                  WHEN Lifestage = '06' and Head_of_HH_age_band = '36 to 45'  THEN 'c) Mature adults (36-45)'     --Mature family
                                  WHEN Lifestage = '07' and Head_of_HH_age_band = '36 to 45'  THEN 'c) Mature adults (36-45)'     --Mature singles
                                  WHEN Lifestage = '08' and Head_of_HH_age_band = '36 to 45'  THEN 'c) Mature adults (36-45)'     --Mature homesharers
                                  WHEN Lifestage = '09' and Head_of_HH_age_band = '46 to 55'  THEN 'd) Middle-aged adults (46-55)' --Older family
                                  WHEN Lifestage = '10' and Head_of_HH_age_band = '46 to 55'  THEN 'd) Middle-aged adults (46-55)' --Older single
                                  WHEN Lifestage = '11' and Head_of_HH_age_band = '46 to 55'  THEN 'd) Middle-aged adults (46-55)' --Older homesharers
                                  WHEN Lifestage = '09' and Head_of_HH_age_band = '56 to 65'  THEN 'e) Older adults (56-65)'       --Older family
                                  WHEN Lifestage = '10' and Head_of_HH_age_band = '56 to 65'  THEN 'e) Older adults (56-65)'       --Older single
                                  WHEN Lifestage = '11' and Head_of_HH_age_band = '56 to 65'  THEN 'e) Older adults (56-65)'       --Older homesharers
                                  WHEN Lifestage = '12' and Head_of_HH_age_band = '66 Plus'   THEN 'f) Elderly adults(65+)'        --Elderly family
                                  WHEN Lifestage = '13' and Head_of_HH_age_band = '66 Plus'   THEN 'f) Elderly adults(65+)'        --Elderly single
                                  WHEN Lifestage = '14' and Head_of_HH_age_band = '66 Plus'   THEN 'f) Elderly adults(65+)'        --Elderly homesharers
                                  WHEN Lifestage = 'U'                                        THEN 'g) Unclassified'
                                  ELSE 'g) Unclassified'
                                  END
,cust_tenure=b.cust_tenure
,h_lifestage_full      =          CASE Lifestage                 WHEN '00'  THEN 'Very young family'
                                                            WHEN '01'  THEN 'Very young single'
                                                            WHEN '02'  THEN 'Very young homesharers'
                                                            WHEN '03'  THEN 'Young family'
                                                            WHEN '04'  THEN 'Young single'
                                                            WHEN '05'  THEN 'Young homesharers'
                                                            WHEN '06'  THEN 'Mature family'
                                                            WHEN '07'  THEN 'Mature singles'
                                                            WHEN '08'  THEN 'Mature homesharers'
                                                            WHEN '09'  THEN 'Older family'
                                                            WHEN '10'  THEN 'Older single'
                                                            WHEN '11'  THEN 'Older homesharers'
                                                            WHEN '12'  THEN 'Elderly family'
                                                            WHEN '13'  THEN 'Elderly single'
                                                            WHEN '14'  THEN 'Elderly homesharers'
                                                            WHEN 'U'   THEN 'Unclassified'
                                                            ELSE            'Unclassified'
                                                            END
,current_package= b.current_package
,disney=b.disney
,skyplus=b.skyplus
from    project_114_base_Accounts as a
left outer join  AdSmart as b
on a.account_number = b.account_number
commit;
--select distinct cust_tenure from AdSmart;

----Upadte 20121112 Create Tenure details as of 20th Aug 2012 fro Sky Customer Life----

--drop table #life;
select distinct a.account_number
        ,case when datediff(day,acct_first_account_activation_dt,cast('2012-08-20' as date)) <=   91 then 'A) Welcome'
              when datediff(day,acct_first_account_activation_dt,cast('2012-08-20' as date)) <=  300 then 'B) Mid'
              when datediff(day,acct_first_account_activation_dt,cast('2012-08-20' as date)) <=  420 then 'C) End'
              when datediff(day,acct_first_account_activation_dt,cast('2012-08-20' as date)) >   420 then 'D) 15+'
              else                                                                    'E) missing'
              end as Sky_cust_life
    ,max(acct_first_account_activation_dt)
        ,rank() over(PARTITION BY a.account_number ORDER BY acct_first_account_activation_dt desc) AS rank_id
         INTO #life
    from project_114_base_Accounts AS A LEFT JOIN sk_prod.cust_single_account_view as SAV
                 ON A.account_number = SAV.Account_number
--    where cust_active_dtv = 1  ---Do for all accounts not just active as some from 20th August may no longer be active.
    group by a.account_number, Sky_cust_life,acct_first_account_activation_dt;
commit;

DELETE FROM  #life where rank_id >1;
commit;

--select expression, count(*) from #life group by expression order by expression;


--update file with Sky_cust_life data
UPDATE  project_114_base_Accounts
SET    Sky_cust_life             = SCL.Sky_cust_life
      FROM  project_114_base_Accounts  AS Base
         INNER JOIN #life AS SCL
         ON base.account_number = SCL.account_number;
commit;

alter table project_114_base_Accounts add region varchar(70);

UPDATE  project_114_base_Accounts
SET    region             = SCL.metropolitan_area_and_itv_region
      FROM  project_114_base_Accounts  AS Base
         INNER JOIN adsmart AS SCL
         ON base.account_number = SCL.account_number;
commit;



---Part G	- Create simple univariate profiles---


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
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*(spot_duration/30) end) as adsmartable_hh_weighted_impacts
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
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
;
---Repeat for Package Viewing Data---
select entertainment_extra_status
,premium_details
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*(spot_duration/30)) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*(spot_duration/30) end) as adsmartable_hh_weighted_impacts
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by entertainment_extra_status
,premium_details
order by entertainment_extra_status
,premium_details
;


---Repeat for Mosaic Base---
select case when mosaic_group is null then 'p )Unclassified' when mosaic_group ='missing' then 'p )Unclassified' else mosaic_group end as mosaic
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by mosaic
order by mosaic
;

--Repeat for Mosaic Viewing Data
select case when mosaic_group is null then 'p )Unclassified' when mosaic_group ='missing' then 'p )Unclassified' else mosaic_group end as mosaic
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by mosaic
order by mosaic
;

---Repeat for Affluence---
select case when HH_Affluence is null then 'H) Unknown' when hh_affluence='M' then 'H) Unknown' when hh_affluence='U' then 'H) Unknown'
      WHEN HH_Affluence IN ('00','01','02')       THEN 'A) Very Low'
                                                WHEN HH_Affluence IN ('03','04', '05')      THEN 'B) Low'
                                                WHEN HH_Affluence IN ('06','07','08')       THEN 'C) Mid Low'
                                                WHEN HH_Affluence IN ('09','10','11')       THEN 'D) Mid'
                                                WHEN HH_Affluence IN ('12','13','14')       THEN 'E) Mid High'
                                                WHEN HH_Affluence IN ('15','16','17')       THEN 'F) High'
                                                WHEN HH_Affluence IN ('18','19')            THEN 'G) Very High' else 'H) Unknown' end as affluence_band


,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by affluence_band
order by affluence_band
;

--Repeat for Viewing by Affluence
select  case when HH_Affluence is null then 'H) Unknown' when hh_affluence='M' then 'H) Unknown' when hh_affluence='U' then 'H) Unknown'
      WHEN HH_Affluence IN ('00','01','02')       THEN 'A) Very Low'
                                                WHEN HH_Affluence IN ('03','04', '05')      THEN 'B) Low'
                                                WHEN HH_Affluence IN ('06','07','08')       THEN 'C) Mid Low'
                                                WHEN HH_Affluence IN ('09','10','11')       THEN 'D) Mid'
                                                WHEN HH_Affluence IN ('12','13','14')       THEN 'E) Mid High'
                                                WHEN HH_Affluence IN ('15','16','17')       THEN 'F) High'
                                                WHEN HH_Affluence IN ('18','19')            THEN 'G) Very High' else 'H) Unknown' end as affluence_band
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by affluence_band
order by affluence_band
;
commit;

--select count(*) from Project_114_viewing_table;




---Repeat Base Accounts for Sky_Reward_L12---
select case when Sky_Reward_L12 is null then 'No' when Sky_Reward_L12 >0 then 'Yes' else 'No' end as sky_reward_L12M
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by Sky_Reward_L12M
order by Sky_Reward_L12M
;

--Repeat for Viewing by Sky_Reward_L12
select   case when Sky_Reward_L12 is null then 'No' when Sky_Reward_L12 >0 then 'Yes' else 'No' end as sky_reward_L12M
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by sky_reward_L12M
order by Sky_Reward_L12M
;
commit;


---Repeat Base Accounts for box_type---
select case when box_type is null then 'Unknown' else box_type end as box_type_for_hh
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by box_type_for_hh   
order by box_type_for_hh   
;

--Repeat for Viewing by box_type
select    case when box_type is null then 'Unknown' else box_type end as box_type_for_hh   
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by box_type_for_hh
order by box_type_for_hh
;
commit;

------------------



---Repeat Base Accounts for bt_fibre_area ---
select   case when bt_fibre_area is null then 'Unknown' else bt_fibre_area end as bt_fibre_area_type    
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by bt_fibre_area_type    
order by bt_fibre_area_type    
;

--Repeat for Viewing by bt_fibre_area
select    case when bt_fibre_area is null then 'Unknown' else bt_fibre_area end as bt_fibre_area_type  
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by bt_fibre_area_type
order by bt_fibre_area_type
;
commit;


------------------
---Repeat Base Accounts for Cable_area ---
select  case when Cable_area is null then 'Y' when cable_area='N/A' then 'Y' else cable_area end as cable_area_type
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by cable_area_type    
order by cable_area_type    
;

--Repeat for Viewing by Cable_area
select   case when Cable_area is null then 'Y' when cable_area='N/A' then 'Y' else cable_area end as cable_area_type
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by cable_area_type
order by cable_area_type
;
commit;

--select count(*) from  Project_114_viewing_table;



------------------
---Repeat Base Accounts for exchange_id ---
select  case when exchange_id     is null then 'Unknown' else exchange_id     end as exchange_id_name
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by exchange_id_name    
order by exchange_id_name    
;

--Repeat for Viewing by exchange_id
select   case when exchange_id     is null then 'Unknown' else exchange_id     end as exchange_id_name
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by exchange_id_name
order by exchange_id_name
;
commit;




------------------
---Repeat Base Accounts for exchange_status    ---
select case when  exchange_status   is null then 'UNKNOWN' else exchange_status end as exchange_status_type
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by exchange_status_type       
order by exchange_status_type       
;

--Repeat for Viewing by exchange_status
select   case when  exchange_status   is null then 'UNKNOWN' else exchange_status end as exchange_status_type
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
group by exchange_status_type
order by exchange_status_type
;
commit;


---Repeat Base Accounts for exchange_status    ---
select  case when  exchange_unbundled   is null then 'UNKNOWN' else exchange_unbundled end as exchange_unbundled_type 
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by exchange_unbundled_type       
order by exchange_unbundled_type       
;

--Repeat for Viewing by exchange_status
select   case when  exchange_unbundled   is null then 'UNKNOWN' else exchange_unbundled end as exchange_unbundled_type 
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
group by exchange_unbundled_type
order by exchange_unbundled_type
;
commit;



---Repeat Base Accounts for Financial_outlook    ---
select case when Financial_outlook  is null then 'U Unallocated' when financial_outlook ='missing' then 'U Unallocated' else financial_outlook end as financial_outlook_group
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by financial_outlook_group           
order by financial_outlook_group           
;

--Repeat for Viewing by Financial_outlook
select  case when Financial_outlook  is null then 'U Unallocated' when financial_outlook ='missing' then 'U Unallocated' else financial_outlook end as financial_outlook_group
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by financial_outlook_group
order by financial_outlook_group
;
commit;


---Repeat Base Accounts for government_region    ---
select case when government_region   is null then 'Unknown' else government_region   end as government_region_group
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by government_region_group          
order by government_region_group        
;

--Repeat for Viewing by government_region
select   case when government_region   is null then 'Unknown' else government_region   end as government_region_group
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
group by government_region_group
order by government_region_group
;
commit;




---Repeat Base Accounts for HomeOwner    ---
select case when HomeOwner is null then 'No' else HomeOwner end as HomeOwner_group
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by HomeOwner_group           
order by HomeOwner_group         
;

--Repeat for Viewing by HomeOwner
select case when HomeOwner is null then 'No' else HomeOwner end as HomeOwner_group
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by HomeOwner_group
order by HomeOwner_group
;
commit;






---Repeat Base Accounts for household_composition    ---
select case when household_composition
 is null then 'UNCLASSIFIED' else household_composition end as household_composition_group
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by household_composition_group             
order by household_composition_group           
;

--Repeat for Viewing by household_composition
select case when household_composition
 is null then 'UNCLASSIFIED' else household_composition end as household_composition_group
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by household_composition_group
order by household_composition_group
;
commit;





---Repeat Base Accounts for h_lifestage    ---
select case when h_lifestage  is null then 'g) Unclassified' when h_lifestage = 'missing' then 'g) Unclassified' else h_lifestage end as h_lifestage_group
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by h_lifestage_group             
order by h_lifestage_group           
;

--Repeat for Viewing by h_lifestage (Lifestage Bands)
select case when h_lifestage  is null then 'g) Unclassified' when h_lifestage = 'missing' then 'g) Unclassified' else h_lifestage end as h_lifestage_group
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by h_lifestage_group
order by h_lifestage_group
;
commit;


---Repeat Base Accounts for h_lifestage    ---
select case when h_lifestage_full  is null then 'Unclassified' else h_lifestage_full end as h_lifestage_full_group
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by h_lifestage_full_group             
order by h_lifestage_full_group           
;

--Repeat for Viewing by h_lifestage_full
select case when h_lifestage_full  is null then 'Unknown' else h_lifestage_full end as h_lifestage_full_group
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by h_lifestage_full_group
order by h_lifestage_full_group
;
commit;



---Repeat Base Accounts for Kids_Aged_LE4    ---
select case when Kids_Aged_LE4  is null then 'N' when Kids_Aged_LE4  ='M' then 'N' else Kids_Aged_LE4   end as Kids_Aged_LE4_group
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by Kids_Aged_LE4_group         
order by Kids_Aged_LE4_group          
;

--Repeat for Viewing by Kids_Aged_LE4
select case when Kids_Aged_LE4  is null then 'N' when Kids_Aged_LE4  ='M' then 'N' else Kids_Aged_LE4   end as Kids_Aged_LE4_group
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by Kids_Aged_LE4_group 
order by Kids_Aged_LE4_group 
;
commit;




---Repeat Base Accounts for Kids_Aged_5to11   ---
select case when Kids_Aged_5to11     is null then 'N' when Kids_Aged_5to11     ='M' then 'N' else Kids_Aged_5to11     end as Kids_Aged_5to11_group
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by Kids_Aged_5to11_group        
order by Kids_Aged_5to11_group         
;

--Repeat for Viewing by Kids_Aged_5to11
select case when Kids_Aged_5to11     is null then 'N' when Kids_Aged_5to11     ='M' then 'N' else Kids_Aged_5to11     end as Kids_Aged_5to11_group
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by Kids_Aged_5to11_group        
order by Kids_Aged_5to11_group         
;
commit;




---Repeat Base Accounts for Kids_Aged_12to17     ---
select case when Kids_Aged_12to17       is null then 'N' when Kids_Aged_12to17        ='M' then 'N' else Kids_Aged_12to17       end as Kids_Aged_12to17_group
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by Kids_Aged_12to17_group        
order by Kids_Aged_12to17_group            
;

--Repeat for Viewing by Kids_Aged_12to17  
select  case when Kids_Aged_12to17       is null then 'N' when Kids_Aged_12to17        ='M' then 'N' else Kids_Aged_12to17       end as Kids_Aged_12to17_group
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by Kids_Aged_12to17_group        
order by Kids_Aged_12to17_group            
;
commit;




---Repeat Base Accounts for MIRROR_MEN_MIN    ---
select case when MIRROR_MEN_MIN is null then 'No Mirror' else MIRROR_MEN_MIN end as MIRROR_MEN_MIN_GROUP
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by MIRROR_MEN_MIN_GROUP       
order by MIRROR_MEN_MIN_GROUP           
;

--Repeat for Viewing by MIRROR_MEN_MIN
select   case when MIRROR_MEN_MIN is null then 'No Mirror' else MIRROR_MEN_MIN end as MIRROR_MEN_MIN_GROUP
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by MIRROR_MEN_MIN_GROUP      
order by MIRROR_MEN_MIN_GROUP         
;
commit;


---Repeat Base Accounts for MIRROR_WOMEN_MIN    ---
select case when MIRROR_WOMEN_MIN is null then 'No Mirror' else MIRROR_WOMEN_MIN end as MIRROR_WOMEN_MIN_GROUP
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by MIRROR_WOMEN_MIN_GROUP       
order by MIRROR_WOMEN_MIN_GROUP           
;

--Repeat for Viewing by MIRROR_WOMEN_MIN
select   case when MIRROR_WOMEN_MIN is null then 'No Mirror' else MIRROR_WOMEN_MIN end as MIRROR_WOMEN_MIN_GROUP
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by MIRROR_WOMEN_MIN_GROUP      
order by MIRROR_WOMEN_MIN_GROUP         
;
commit;




---Repeat Base Accounts for Mirror_has_children    ---
select case when Mirror_has_children   is null then 'Y' when Mirror_has_children='M' then 'Y' when Mirror_has_children='missing' then 'Y'  else Mirror_has_children   end as Mirror_has_children_GROUP
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by Mirror_has_children_GROUP       
order by Mirror_has_children_GROUP           
;

--Repeat for Viewing by Mirror_has_children
select   case when Mirror_has_children   is null then 'Y' when Mirror_has_children='M' then 'Y' when Mirror_has_children='missing' then 'Y'  else Mirror_has_children   end as Mirror_has_children_GROUP
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by Mirror_has_children_GROUP 
order by Mirror_has_children_GROUP    
;
commit;




---Repeat Base Accounts for Mirror_ABC1      ---
select case when Mirror_ABC1     is null then 'Y'  else Mirror_ABC1  end as Mirror_ABC1_Group ,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by Mirror_ABC1_Group         
order by Mirror_ABC1_Group              
;

--Repeat for Viewing by Mirror_ABC1
select  case when Mirror_ABC1     is null then 'Y'  else Mirror_ABC1  end as Mirror_ABC1_Group 
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by Mirror_ABC1_Group         
order by Mirror_ABC1_Group              
;

commit;



---Repeat Base Accounts for Total_miss_pmt    ---
select case when Total_miss_pmt  is null then 'Unknown' else Total_miss_pmt end as Total_miss_pmt_group
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by Total_miss_pmt_group        
order by Total_miss_pmt_group            
;

--Repeat for Viewing by Total_miss_pmt
select  case when Total_miss_pmt  is null then 'No' when Total_miss_pmt>0 then 'Yes'  else 'No'  end as Total_miss_pmt_group
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by Total_miss_pmt_group       
order by Total_miss_pmt_group           
;

commit;



---Repeat Base Accounts for Movies_downgrades    ---
select case when Movies_downgrades    is null then 0 else Movies_downgrades    end as Movies_downgrades_group
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by Movies_downgrades_group        
order by Movies_downgrades_group            
;
--Repeat for Viewing by Movies_downgrades
select  case when Movies_downgrades    is null then 0 else Movies_downgrades    end as Movies_downgrades_group
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
into dbarnett.viewing_001
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by Movies_downgrades_group       
order by Movies_downgrades_group           
;




---Repeat Base Accounts for sports_downgrades    ---
select case when sports_downgrades    is null then 0 else sports_downgrades    end as sports_downgrades_group
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by sports_downgrades_group        
order by sports_downgrades_group            
;
--Repeat for Viewing by sports_downgrades
select  case when sports_downgrades    is null then 0 else sports_downgrades    end as sports_downgrades_group
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
into dbarnett.viewing_002
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by sports_downgrades_group       
order by sports_downgrades_group           
;




---Repeat Base Accounts for current_offer    ---
select case when current_offer     is null then 0 else current_offer     end as current_offer_group
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by current_offer_group        
order by current_offer_group            
;
--Repeat for Viewing by current_offer
select   case when current_offer     is null then 0 else current_offer     end as current_offer_group
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
into dbarnett.viewing_003
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by current_offer_group       
order by current_offer_group           
;




---Repeat Base Accounts for region (formerly barb_desc_itv_group)    ---
select case when region  is null then 'Unknown' else region     end as region_Group
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by region_Group        
order by region_Group            
;
--Repeat for Viewing by region
select     case when region       is null then 'Unknown' else region     end as region_Group
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
into dbarnett.viewing_004
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by region_Group       
order by region_Group           
;
commit;

---Repeat Base Accounts for Sky_Go_Reg    ---
select case when Sky_Go_Reg is null then 'b) No'  when sky_go_reg=1 then 'a) Yes'  when sky_go_reg=0 then 'b) No' else  'b) No' end as sky_go_reg_group
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by sky_go_reg_group           
order by sky_go_reg_group               
;

--Repeat for Viewing by Sky_Go_Reg
select    case when Sky_Go_Reg is null then 'b) No'  when sky_go_reg=1 then 'a) Yes'  when sky_go_reg=0 then 'b) No' else  'b) No' end as sky_go_reg_group
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
into dbarnett.viewing_005
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by sky_go_reg_group       
order by sky_go_reg_group           
;



---Repeat Base Accounts for cust_tenure    ---
select case when cust_tenure is null then 'Unknown'   else cust_tenure end as cust_tenure_group
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by cust_tenure_group           
order by cust_tenure_group               
;

--Repeat for Viewing by cust_tenure
select    case when cust_tenure is null then 'Unknown'   else cust_tenure end as cust_tenure_group
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
into dbarnett.viewing_006
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
group by cust_tenure_group           
order by cust_tenure_group               
;


commit;

---Repeat Base Accounts for Sky_cust_life    ---
select  case when Sky_cust_life is null then 'D) 15+'  else  Sky_cust_life end as Sky_cust_life_group
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by Sky_cust_life_group           
order by Sky_cust_life_group               
;

--Repeat for Viewing by Sky_cust_life
select    case when Sky_cust_life is null then 'D) 15+'  else  Sky_cust_life end as Sky_cust_life_group
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
into dbarnett.viewing_007
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by Sky_cust_life_group           
order by Sky_cust_life_group               
;





---Repeat Base Accounts for TA_attempts      ---
select case when TA_attempts is null then 0 else ta_attempts end as ta_attempts_group 
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by ta_attempts_group           
order by ta_attempts_group                 
;

--Repeat for Viewing by TA_attempts
select  case when TA_attempts is null then 'No' when TA_attempts>0 then 'Yes' else 'No' end as ta_attempts_group 
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
into dbarnett.viewing_008
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by ta_attempts_group           
order by ta_attempts_group               
;



---Repeat Base Accounts for value_segment      ---
select case when value_segment is null then 'Unknown' when value_segment='missing' then 'Unknown' else value_segment end as value_segment_group
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by value_segment_group           
order by value_segment_group                 
;

--Repeat for Viewing by value_segment
select case when value_segment is null then 'Unknown' when value_segment='missing' then 'Unknown' else value_segment end as value_segment_group
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
into dbarnett.viewing_009
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by value_segment_group           
order by value_segment_group               
;


commit;


---Repeat Base Accounts for sky_id      ---
select  case when sky_id= 'Unknown' then 'No' else sky_id end as sky_id_group
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by sky_id_group           
order by sky_id_group                 
;

--Repeat for Viewing by sky_id
select  case when sky_id= 'Unknown' then 'No' else sky_id end as sky_id_group
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
into dbarnett.viewing_010
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by sky_id_group           
order by sky_id_group               
;
commit;
---Repeat Base Accounts for BB      ---
select BB
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by BB           
order by BB                 
;

--Repeat for Viewing by BB
select BB
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
into dbarnett.viewing_011
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by BB           
order by BB               
;

---Repeat Base Accounts for Talk      ---
select talk
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by talk           
order by talk                 
;

--Repeat for Viewing by talk
select talk
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
into dbarnett.viewing_012
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by talk           
order by talk               
;

---Repeat Base Accounts for HD      ---
select case when HD ='Unknown' then 'No' else HD end as has_hd
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by has_hd           
order by has_hd                 
;

--Repeat for Viewing by HD
select case when HD ='Unknown' then 'No' else HD end as has_hd
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
into dbarnett.viewing_013
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by has_hd           
order by has_hd               
;


---Repeat Base Accounts for Anytime_plus      ---
select case when Anytime_plus='Unknown' then 'No' else Anytime_plus end as anytime_plus_group
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by anytime_plus_group           
order by anytime_plus_group                 
;

--Repeat for Viewing by Anytime_plus
select case when Anytime_plus='Unknown' then 'No' else Anytime_plus end as anytime_plus_group
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
into dbarnett.viewing_014
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by anytime_plus_group           
order by anytime_plus_group               
;



---Repeat Base Accounts for Pending_cancel      ---
select case when Pending_cancel ='Unknown' then 'No' else Pending_cancel end as PC_group
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by PC_group           
order by PC_group                 
;

--Repeat for Viewing by Pending_cancel
select case when Pending_cancel ='Unknown' then 'No' else Pending_cancel end as PC_group
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
into dbarnett.viewing_015
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by PC_group           
order by PC_group               
;




---Repeat Base Accounts for espn      ---
select case when espn ='Unknown' then 'No' else espn end as espn_group
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by espn_group           
order by espn_group                 
;

--Repeat for Viewing by espn_group
select case when espn ='Unknown' then 'No' else espn end as espn_group
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
into dbarnett.viewing_016
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by espn_group           
order by espn_group               
;



---Repeat Base Accounts for multiroom      ---
select case when multiroom ='Unknown' then 'No' else multiroom end as multiroom_group
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by multiroom_group           
order by multiroom_group                 
;

--Repeat for Viewing by espn_group
select case when multiroom ='Unknown' then 'No' else multiroom end as multiroom_group
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
into dbarnett.viewing_017
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by multiroom_group           
order by multiroom_group               
;



---Repeat Base Accounts for entertainment_extra      ---
select case when entertainment_extra_status='1: Entertainment Extra' then 'Yes' else 'No' end as has_entertainment_extra
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by has_entertainment_extra           
order by has_entertainment_extra                 
;

--Repeat for Viewing by entertainment_extra_status
select case when entertainment_extra_status='1: Entertainment Extra' then 'Yes' else 'No' end as has_entertainment_extra
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
into dbarnett.viewing_018
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by has_entertainment_extra           
order by has_entertainment_extra               
;


---Repeat Base Accounts for sky_sports_1      ---
select case when sky_sports_1 =1 then 'Yes' else 'No' end as has_sky_sports_1
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by has_sky_sports_1           
order by has_sky_sports_1                 
;

--Repeat for Viewing by sky_sports_1
select  case when sky_sports_1 =1 then 'Yes' else 'No' end as has_sky_sports_1
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
into dbarnett.viewing_019
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by  has_sky_sports_1           
order by   has_sky_sports_1             
;


---Repeat Base Accounts for sky_sports_2      ---
select case when sky_sports_2 =1 then 'Yes' else 'No' end as has_sky_sports_2
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by has_sky_sports_2           
order by has_sky_sports_2                 
;

--Repeat for Viewing by sky_sports_2
select  case when sky_sports_2 =1 then 'Yes' else 'No' end as has_sky_sports_2
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
into dbarnett.viewing_020
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by  has_sky_sports_2           
order by   has_sky_sports_2             
;


---Repeat Base Accounts for sky_sports_3      ---
select case when  sky_sports_1 =1 and sky_sports_2 =1 then 'Yes' else 'No' end as has_sky_sports_3
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by has_sky_sports_3           
order by has_sky_sports_3                 
;

--Repeat for Viewing by sky_sports_3
select  case when  sky_sports_1 =1 and sky_sports_2 =1 then 'Yes' else 'No' end as has_sky_sports_3
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
into dbarnett.viewing_021
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by  has_sky_sports_3           
order by   has_sky_sports_3             
;


---Repeat Base Accounts for movies_1      ---
select case when movies_1 =1 then 'Yes' else 'No' end as has_movies_1
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by has_movies_1           
order by has_movies_1                 
;

--Repeat for Viewing by movies_1
select  case when movies_1 =1 then 'Yes' else 'No' end as has_movies_1
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
into dbarnett.viewing_022
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by  has_movies_1           
order by   has_movies_1             
;



---Repeat Base Accounts for movies_2      ---
select case when movies_2 =1 then 'Yes' else 'No' end as has_movies_2
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by has_movies_2           
order by has_movies_2                 
;

--Repeat for Viewing by movies_2
select  case when movies_2 =1 then 'Yes' else 'No' end as has_movies_2
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
into dbarnett.viewing_023
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by  has_movies_2           
order by   has_movies_2             
;



---Repeat Base Accounts for movies_3      ---
select case when  movies_1 =1 and movies_2 =1 then 'Yes' else 'No' end as has_movies_premiere
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by has_movies_premiere           
order by has_movies_premiere                 
;

--Repeat for Viewing by movies_premiere
select  case when  movies_1 =1 and movies_2 =1 then 'Yes' else 'No' end as has_movies_premiere
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
into dbarnett.viewing_024
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by  has_movies_premiere           
order by   has_movies_premiere             
;

---Repeat Base Accounts for disney      ---
select case when disney=1 then 'Yes' else 'No' end as has_disney
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by has_disney           
order by has_disney                 
;

--Repeat for Viewing by movies_premiere
select  case when disney=1 then 'Yes' else 'No' end as has_disney
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts
into dbarnett.viewing_025
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by  has_disney           
order by   has_disney             
;


---Repeat Base Accounts for skyplus      ---
select case when skyplus=1 then 'Yes' else 'No' end as has_skyplus
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when adsmartable=1 then 1 else 0 end) as adsmartable_unweighted_accounts
,sum(case when adsmartable=1 then overall_project_weighting else 0 end) as adsmartable_weighted_accounts
from project_114_base_Accounts
group by has_skyplus           
order by has_skyplus                 
;
commit;
--Repeat for Viewing by movies_premiere
--drop table dbarnett.viewing_026;
select  case when skyplus=1 then 'Yes' else 'No' end as has_skyplus
,sum(spot_duration/30) as unweighted_impacts
,sum(overall_project_weighting*cast(spot_duration as real)/30) as weighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*spot_duration/30 end) as adsmartable_hh_unweighted_impacts
,sum(case when adsmartable is null then 0 else adsmartable*overall_project_weighting*cast(spot_duration as real)/30 end) as adsmartable_hh_weighted_impacts

,sum(cast(spot_duration as real)/30) as unweighted_impacts_test
,sum(case when adsmartable is null then 0 else adsmartable*cast(spot_duration as real)/30 end) as adsmartable_hh_unweighted_impacts_test

into dbarnett.viewing_026
from Project_114_viewing_table as a 
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
group by  has_skyplus           
order by   has_skyplus             
;

commit;

select * from dbarnett.viewing_026





----PART II-----
---Create Output---
--drop table project_114_all_viewing_demographic_data;
select a.spot_duration
,a.spot_position_in_break
,a.service_key
,a.spot_channel_name
,overall_project_weighting*cast(spot_duration as real)/30 as weighted_impacts
,case when adsmartable is null then 0 else adsmartable end as adsmartable_hh
--CBI--,case when isba_tv_region is null then 'Unknown' else isba_tv_region end as isba
--Replaced by Yes/No on individual parts of DTV Package--,entertainment_extra_status
--Replaced by Yes/No on individual parts of DTV Package--,premium_details
,case when mosaic_group is null then 'p )Unclassified' when mosaic_group ='missing' then 'p )Unclassified' else mosaic_group end as demographic_and_lifestyle
,case when HH_Affluence is null then 'H) Unknown' when hh_affluence='M' then 'H) Unknown' when hh_affluence='U' then 'H) Unknown'
      WHEN HH_Affluence IN ('00','01','02')       THEN 'A) Very Low'
                                                WHEN HH_Affluence IN ('03','04', '05')      THEN 'B) Low'
                                                WHEN HH_Affluence IN ('06','07','08')       THEN 'C) Mid Low'
                                                WHEN HH_Affluence IN ('09','10','11')       THEN 'D) Mid'
                                                WHEN HH_Affluence IN ('12','13','14')       THEN 'E) Mid High'
                                                WHEN HH_Affluence IN ('15','16','17')       THEN 'F) High'
                                                WHEN HH_Affluence IN ('18','19')            THEN 'G) Very High' else 'H) Unknown' end as affluence_band
,case when Sky_Reward_L12 is null then 'No' when Sky_Reward_L12>0 then 'Yes' else 'No' end as sky_reward_L12M
--CBI--,case when box_type is null then 'Unknown' else box_type end as box_type_for_hh
--CBI--, case when bt_fibre_area is null then 'Unknown' else bt_fibre_area end as bt_fibre_area_type    
, case when Cable_area is null then 'Y' when cable_area='N/A' then 'Y' else cable_area end as cable_area_type
--CBI--, case when exchange_id     is null then 'Unknown' else exchange_id     end as exchange_id_name
--CBI--,case when  exchange_status   is null then 'UNKNOWN' else exchange_status end as exchange_status_type
--CBI--,case when  exchange_unbundled   is null then 'UNKNOWN' else exchange_unbundled end as exchange_unbundled_type 
,case when Financial_outlook  is null then 'U Unallocated' when financial_outlook ='missing' then 'U Unallocated' else financial_outlook end as financial_outlook_group
--CBI--,case when government_region   is null then 'Unknown' else government_region   end as government_region_group
,case when HomeOwner is null then 'No' else HomeOwner end as HomeOwner_group
--,case when household_composition is null then 'UNCLASSIFIED' else household_composition end as household_composition_group

, case when h_lifestage  is null then 'g) Unclassified' when h_lifestage = 'missing' then 'g) Unclassified' else h_lifestage end as h_lifestage_group
,case when h_lifestage_full  is null then 'Unknown' else h_lifestage_full end as lifestage_bands

,case when Kids_Aged_LE4  is null then 'N' when Kids_Aged_LE4  ='M' then 'N' else Kids_Aged_LE4   end as Kids_Aged_LE4_group
,case when Kids_Aged_5to11     is null then 'N' when Kids_Aged_5to11     ='M' then 'N' else Kids_Aged_5to11     end as Kids_Aged_5to11_group
, case when Kids_Aged_12to17       is null then 'N' when Kids_Aged_12to17        ='M' then 'N' else Kids_Aged_12to17       end as Kids_Aged_12to17_group
,case when MIRROR_MEN_MIN is null then 'No Mirror' else MIRROR_MEN_MIN end as MIRROR_MEN_MIN_GROUP
, case when MIRROR_WOMEN_MIN is null then 'No Mirror' else MIRROR_WOMEN_MIN end as MIRROR_WOMEN_MIN_GROUP
,case when Mirror_has_children   is null then 'Y' when Mirror_has_children='M' then 'Y' when Mirror_has_children='missing' then 'Y'  else Mirror_has_children   end as Mirror_has_children_GROUP
,case when Mirror_ABC1     is null then 'Y'  else Mirror_ABC1  end as Mirror_ABC1_Group 
,case when Total_miss_pmt  is null then 'No' when  Total_miss_pmt>0 then 'Yes' else 'No'  end as previous_missed_payments
,case when Movies_downgrades  is null then 'No' when  Movies_downgrades>0 then 'Yes' else 'No'  end as Movies_downgrades_group
,case when sports_downgrades  is null then 'No' when  sports_downgrades>0 then 'Yes' else 'No'  end as sports_downgrades_group
,case when current_offer  is null then 'No' when  current_offer>0 then 'Yes' else 'No'  end as current_offer_group
, case when barb_desc_itv       is null then 'Unknown' else barb_desc_itv     end as region
,case when Sky_Go_Reg is null then 'b) No'  when sky_go_reg=1 then 'a) Yes'  when sky_go_reg=0 then 'b) No' else  'b) No' end as sky_go_reg_group
--CBI--, case when cust_tenure is null then 'Unknown'   else cust_tenure end as cust_tenure_group
,case when Sky_cust_life is null then 'E) missing'  else  Sky_cust_life end as Sky_cust_life_group
,case when TA_attempts  is null then 'No' when  TA_attempts>0 then 'Yes' else 'No'  end as TA_attempts_group
,case when value_segment is null then 'Unknown' when value_segment='missing' then 'Unknown' else value_segment end as value_segment_group
, case when sky_id= 'Unknown' then 'No' else sky_id end as sky_id_group
,BB 
,talk
,case when HD ='Unknown' then 'No' else HD end as has_hd
,Anytime_plus
,case when Pending_cancel ='Unknown' then 'No' else Pending_cancel end as Pending_Cancel_Account
,case when espn ='Unknown' then 'No' else espn end as has_epsn
,case when multiroom ='Unknown' then 'No' else multiroom end as has_multiroom
,case when entertainment_extra_status='1: Entertainment Extra' then 'Yes' else 'No' end as has_entertainment_extra
,case when sky_sports_1 =1 then 'Yes' else 'No' end as has_sky_sports_1
,case when sky_sports_2 =1 then 'Yes' else 'No' end as has_sky_sports_2
,case when  sky_sports_1 =1 and sky_sports_2 =1 then 'Yes' else 'No' end as has_sky_sports_3
,case when movies_1 =1 then 'Yes' else 'No' end as has_movies_1
,case when movies_2 =1 then 'Yes' else 'No' end as has_movies_2
,case when  movies_1 =1 and movies_2 =1 then 'Yes' else 'No' end as has_movies_premiere
,case when disney=1 then 'Yes' else 'No' end as has_disney
,case when skyplus=1 then 'Yes' else 'No' end as has_skyplus

into project_114_all_viewing_demographic_data
from Project_114_viewing_table as a
left outer join project_114_base_Accounts as b
on a.account_number = b.account_number
where b.account_number is not null
--and right(b.account_number,3) in ('367')
;
grant all on project_114_all_viewing_demographic_data to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh
, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg,vespa_analysts,neighbom;
commit;

--------Create Table for Pivot (Including Spot Details)---------
select spot_duration
,spot_position_in_break
,service_key
,spot_channel_name
,adsmartable_hh
,demographic_and_lifestyle
,affluence_band
,sky_reward_L12M
,cable_area_type
,financial_outlook_group

,HomeOwner_group
,h_lifestage_group
,lifestage_bands
,Kids_Aged_LE4_group
,Kids_Aged_5to11_group
,Kids_Aged_12to17_group
,MIRROR_MEN_MIN_GROUP
,MIRROR_WOMEN_MIN_GROUP
,Mirror_has_children_GROUP
,Mirror_ABC1_Group 
,previous_missed_payments
,Movies_downgrades_group
,sports_downgrades_group
,current_offer_group
,region
,sky_go_reg_group

,Sky_cust_life_group
,TA_attempts_group
,value_segment_group
,sky_id_group
,BB 
,talk
,has_hd
,Anytime_plus
,Pending_Cancel_Account
,has_epsn
,has_multiroom
,has_entertainment_extra
,has_sky_sports_1
,has_sky_sports_2
,has_sky_sports_3
,has_movies_1
,has_movies_2
,has_movies_premiere
,has_disney
,has_skyplus
,sum(weighted_impacts) as total_weighted_impacts
,count(*) as records
into project_114_all_viewing_pivot_inc_channel_info
from project_114_all_viewing_demographic_data
group by spot_duration
,spot_position_in_break
,service_key
,spot_channel_name
,adsmartable_hh
,demographic_and_lifestyle
,affluence_band
,sky_reward_L12M
,cable_area_type
,financial_outlook_group

,HomeOwner_group
,h_lifestage_group
,lifestage_bands
,Kids_Aged_LE4_group
,Kids_Aged_5to11_group
,Kids_Aged_12to17_group
,MIRROR_MEN_MIN_GROUP
,MIRROR_WOMEN_MIN_GROUP
,Mirror_has_children_GROUP
,Mirror_ABC1_Group 
,previous_missed_payments
,Movies_downgrades_group
,sports_downgrades_group
,current_offer_group
,region
,sky_go_reg_group

,Sky_cust_life_group
,TA_attempts_group
,value_segment_group
,sky_id_group
,BB 
,talk
,has_hd
,Anytime_plus
,Pending_Cancel_Account
,has_epsn
,has_multiroom
,has_entertainment_extra
,has_sky_sports_1
,has_sky_sports_2
,has_sky_sports_3
,has_movies_1
,has_movies_2
,has_movies_premiere
,has_disney
,has_skyplus
;

commit;


--------Create Table for Pivot (Excluding Spot Details)---------
select adsmartable_hh
,demographic_and_lifestyle
,affluence_band
,sky_reward_L12M
,cable_area_type
,financial_outlook_group

,HomeOwner_group
,h_lifestage_group
,lifestage_bands
,Kids_Aged_LE4_group
,Kids_Aged_5to11_group
,Kids_Aged_12to17_group
,MIRROR_MEN_MIN_GROUP
,MIRROR_WOMEN_MIN_GROUP
,Mirror_has_children_GROUP
,Mirror_ABC1_Group 
,previous_missed_payments
,Movies_downgrades_group
,sports_downgrades_group
,current_offer_group
,region
,sky_go_reg_group

,Sky_cust_life_group
,TA_attempts_group
,value_segment_group
,sky_id_group
,BB 
,talk
,has_hd
,Anytime_plus
,Pending_Cancel_Account
,has_epsn
,has_multiroom
,has_entertainment_extra
,has_sky_sports_1
,has_sky_sports_2
,has_sky_sports_3
,has_movies_1
,has_movies_2
,has_movies_premiere
,has_disney
,has_skyplus
,sum(weighted_impacts) as total_weighted_impacts
,count(*) as records
into project_114_all_viewing_pivot_exc_channel_info
from project_114_all_viewing_demographic_data
group by adsmartable_hh
,demographic_and_lifestyle
,affluence_band
,sky_reward_L12M
,cable_area_type
,financial_outlook_group

,HomeOwner_group
,h_lifestage_group
,lifestage_bands
,Kids_Aged_LE4_group
,Kids_Aged_5to11_group
,Kids_Aged_12to17_group
,MIRROR_MEN_MIN_GROUP
,MIRROR_WOMEN_MIN_GROUP
,Mirror_has_children_GROUP
,Mirror_ABC1_Group 
,previous_missed_payments
,Movies_downgrades_group
,sports_downgrades_group
,current_offer_group
,region
,sky_go_reg_group

,Sky_cust_life_group
,TA_attempts_group
,value_segment_group
,sky_id_group
,BB 
,talk
,has_hd
,Anytime_plus
,Pending_Cancel_Account
,has_epsn
,has_multiroom
,has_entertainment_extra
,has_sky_sports_1
,has_sky_sports_2
,has_sky_sports_3
,has_movies_1
,has_movies_2
,has_movies_premiere
,has_disney
,has_skyplus
;

commit;


grant all on project_114_all_viewing_pivot_inc_channel_info to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh
, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg,vespa_analysts,neighbom;
commit;


grant all on project_114_all_viewing_pivot_exc_channel_info to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh
, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg,vespa_analysts,neighbom;
commit;

select * from project_114_all_viewing_pivot_exc_channel_info;
--output to 'G:\RTCI\Sky Projects\Vespa\DBarnett Filestore\project_114_all_viewing_pivot_exc_channel_info.csv' format ascii;

output to 'C:\Users\barnetd\Documents\Project 114 - Adsmart\project_114_all_viewing_pivot_exc_channel_info.csv' format ascii;

--select count(*) from Project_114_viewing_table;

select b.value_segment ,sum(overall_project_weighting) from project_114_base_Accounts as a
left outer join  adsmart as b
on a.account_number=b.account_number
group by b.value_segment


select b.value_segment ,sum(overall_project_weighting),count(*) as accounts, sum(case when a.account_number is not null then 1 else 0 end) as vespa_accounts from adsmart as b
left outer join  project_114_base_Accounts as a
on a.account_number=b.account_number
group by b.value_segment

commit;


--select top 500 * from sk_prod.vespa_epg_dim;



