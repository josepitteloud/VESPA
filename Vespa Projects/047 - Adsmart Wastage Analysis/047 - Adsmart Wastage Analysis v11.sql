

---Project 047 Ad SMart Tradeable Wastage

--http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=47&Source=http%3A%2F%2Frtci%2FLists%2FRTCI%2520IC%2520dev%2FProjectInceptionView%2Easpx

/*  From Sharepoint
To understand the viability of various AdSmart and linear trading approaches, we wish understand how much inventory could be created 
through just trading linear ‘wastage’.  In order to identify the reachable wastage from the BARB traded audience we have had to define a set of
 ‘Mirror Segments’, which define the reachable audiences for each BARB traded audience. 
The reason we cannot reach all of the wastage is that AdSmart cannot identify which householder is watching at any one point in time, 
therefore whilst there might be wastage in an advert bought against housewives with children if someone other than the housewife is 
viewing the television there is no way for us to identify and then serve a different advert to this person. However we could in this 
situation still deliver alternative advertising to any household with no children as nobody in that household (with the exception of visitors) 
would fall into the BARB traded demographic. The brief is still being refined by Rory Skrebowski. 
*/

----
/*
Analysis looks to build a universe of a Households in UK with detailes of whether the households has 1 or more people in 
each of the tradeable demographics e.g., Male 16-54

This is then matched to the spot traded data (and spot viewing data from Project 042 - vespa_analysts.vespa_spot_data_By_channel )to calculate what proportion of households that viewed the advert are not on the traded demographic

As data on sk_prod.ilu does not exactly match the traded demographics the following work arounds have been created
ILU Has Adults as 18+ BARB Traded Demographics are 16+
ILU_HHSocioEcon groups together C1 and C2 And has retired seperately where on BARB C1 and C2 are seperated and retired HH 
are grouped within the A/B/C1/C2/D/E classes

---Update

A bespoke appoximation to ABC1 has now been built using various parts of ILU data


*/

---Part 01 - Create Household level information on whether household has anyone within each of the traded demographics

--drop table #household_summary_details;

---With Revised ABC1 Definitions

select cb_key_household
,min(ILU_HHSocioEcon) as socio_demographic_level
,max(ILU_HHAfflu) as household_affluence_level
,max(case when ILU_Agef in (1,2,3) then 1 else 0 end) as adult_18_34
,max(case when ILU_Agef in (1,2,3,4,5) then 1 else 0 end) as adult_18_44
,max(case when ILU_Agef in (1,2,3,4,5,6,7) then 1 else 0 end) as adult_18_54
,max(case when ILU_Agef in (2,3,4,5) then 1 else 0 end) as adult_25_44
,max(case when ILU_HHSocioEcon in  (1)  then 1 
          when ILU_HHSocioEcon in  (2) and ILU_OccY3 in ('2','5','7')  then 1 
          when ILU_HHSocioEcon in  (2) and ILU_OccY3 in ('6') and ILU_HHAfflu>=9 then 1 
          when ILU_HHSocioEcon in  (5) and  ILU_HHAfflu>=8   then 1
          else 0 end) as adult_ABC1
,max(case when ILU_iKid0004+ILU_iKid0507+ILU_iKid0810+ILU_iKid1116>0 then 1 else 0 end) as hh_with_children
,max(case when ILU_Gender in (0,1) then 1 else 0 end) as adult_male   --Includes Unknown gender (approx 4% of people)
,max(case when ILU_Agef in (1,2,3) and ILU_Gender in (0,1) then 1 else 0 end) as adult_male_18_34


,max(case when ILU_Gender not in (0,1) then 0
          when ILU_HHSocioEcon in  (1)  then 1 
          when ILU_HHSocioEcon in  (2) and ILU_OccY3 in ('2','5','7')  then 1 
          when ILU_HHSocioEcon in  (2) and ILU_OccY3 in ('6') and ILU_HHAfflu>=9 then 1 
          when ILU_HHSocioEcon in  (5) and  ILU_HHAfflu>=8   then 1
          else 0 end) as adult_male_ABC1



,max(case when ILU_Agef in (1,2,3) and ILU_Gender in (2) then 1 else 0 end) as adult_female_18_34
,max(case when ILU_Agef in (1,2,3,4,5) and ILU_Gender in (2) then 1 else 0 end) as adult_female_18_44
,max(case when ILU_Agef in (1,2,3,4,5,6,7) and ILU_Gender in (2) then 1 else 0 end) as adult_female_18_54

,max(case when ILU_Gender not in (2) then 0
          when ILU_HHSocioEcon in  (1)  then 1 
          when ILU_HHSocioEcon in  (2) and ILU_OccY3 in ('2','5','7')  then 1 
          when ILU_HHSocioEcon in  (2) and ILU_OccY3 in ('6') and ILU_HHAfflu>=9 then 1 
          when ILU_HHSocioEcon in  (5) and  ILU_HHAfflu>=8   then 1
          else 0 end) as adult_female_ABC1

,max(case when ILU_Gender in (2) then 1 else 0 end) as adult_female   
,max(case when ILU_OccY3 in ('1') then 1 else 0 end) as occ_caftsman_in_hh   
,max(case when ILU_OccY3 in ('2') then 1 else 0 end) as occ_education_in_hh   
,max(case when ILU_OccY3 in ('3') then 1 else 0 end) as occ_housewife_in_hh   
,max(case when ILU_OccY3 in ('4') then 1 else 0 end) as occ_manual_in_hh   
,max(case when ILU_OccY3 in ('5') then 1 else 0 end) as occ_middle_management_in_hh   
,max(case when ILU_OccY3 in ('6') then 1 else 0 end) as occ_office_clerical_in_hh   
,max(case when ILU_OccY3 in ('7') then 1 else 0 end) as occ_professional_senior_in_hh   
,max(case when ILU_OccY3 in ('8') then 1 else 0 end) as occ_retired_in_hh   
,max(case when ILU_OccY3 in ('9','0','U') then 1 else 0 end) as occ_other_in_hh   

,max(case when ILU_Correspondent  in ('P1') then ILU_Agef else null end) as head_hh_agef
into #household_summary_details
from sk_prod.ILU
where ILU_Correspondent  in ('P1','P2','OR')  and cb_address_status = '1' and cb_address_dps is not null  
group by cb_key_household
;

commit;
create hg index idx1 on  #household_summary_details(cb_key_household);
---Vespa Boxes returning data on 15th Jan 2012


select distinct cb_key_household 
into #vespa_hh
from  vespa_analysts.VESPA_all_viewing_records_20120115
where cast(Adjusted_Event_Start_Time as date) = '2012-01-15'
;
commit;
create hg index idx1 on #vespa_hh(cb_key_household);
commit;
--select count(*) from #vespa_hh;
-----
select account_number
,cb_key_household
into #vespa_data_hh
from  vespa_analysts.VESPA_all_viewing_records_20120115
where cast(Adjusted_Event_Start_Time as date) = '2012-01-15'
group by account_number
,cb_key_household
;

--select count(*) , count(distinct cb_key_household) from #vespa_data_hh;

--select count(distinct account_number) , count(distinct cb_key_household) , count(distinct subscriber_id) from vespa_analysts.VESPA_all_viewing_records_20120115;

---Create Summary table of all accounts that returned data on 15th Jan along with their weighting value

--drop table vespa_analysts.jan_15_vespa_scaling_lookup;
select a.account_number
,a.cb_key_household
,b.scaling_segment_id
,c.weighting
,d.adult_18_34
,d.adult_18_44
,d.adult_18_54
,d.adult_25_44
,d.adult_ABC1
,d.hh_with_children
,d.adult_male
,d.adult_male_18_34
,d.adult_male_ABC1
,d.adult_female_18_34
,d.adult_female_18_44
,d.adult_female_18_54
,d.adult_female_ABC1
,d.adult_female
,d.head_hh_agef
into vespa_analysts.jan_15_vespa_scaling_lookup
from  #vespa_data_hh as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
left outer join vespa_analysts.scaling_weightings as c
on b.scaling_segment_id=c.scaling_segment_id
left outer join #household_summary_details as d
on a.cb_key_household  = d.cb_key_household

where cast ('2012-01-15' as date)  between b.reporting_starts and b.reporting_ends
and c.scaling_day = '2012-01-15'
;
commit;
create hg index idx1 on  vespa_analysts.jan_15_vespa_scaling_lookup(account_number);
commit;


----Create Summary Stats by spot---

--select top 100 * from vespa_analysts.vespa_spot_data_By_channel ;
--drop table vespa_analysts.vespa_spots_with_trading_metric_counts;
select station_code 
,channel_name_inc_hd
,corrected_spot_transmission_start_datetime
,corrected_spot_transmission_end_datetime
,spot_duration
,sum(case when seconds_of_spot_viewed_live >0 then b.weighting else 0 end) as households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_18_34*b.weighting else 0 end) as adult_18_34_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_18_44*b.weighting else 0 end) as adult_18_44_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_18_54*b.weighting else 0 end) as adult_18_54_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_25_44*b.weighting else 0 end) as adult_25_44_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_ABC1*b.weighting else 0 end) as adult_ABC1_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then hh_with_children*b.weighting else 0 end) as hh_with_children_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_male*b.weighting else 0 end) as adult_male_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_male_18_34*b.weighting else 0 end) as adult_male_18_34_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_male_ABC1*b.weighting else 0 end) as adult_male_ABC1_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_female_18_34*b.weighting else 0 end) as adult_female_18_34_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_female_18_44*b.weighting else 0 end) as adult_female_18_44_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_female_18_54*b.weighting else 0 end) as adult_female_18_54_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_female_ABC1*b.weighting else 0 end) as adult_female_ABC1_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_female*b.weighting else 0 end) as adult_female_households_viewing
into vespa_analysts.vespa_spots_with_trading_metric_counts
from vespa_analysts.vespa_spot_data_By_channel as a
left outer join vespa_analysts.jan_15_vespa_scaling_lookup as b
on a.account_number =b.account_number
group by station_code 
,channel_name_inc_hd
,corrected_spot_transmission_start_datetime
,corrected_spot_transmission_end_datetime
,spot_duration
;
commit;

/*
select * from vespa_analysts.vespa_spots_with_trading_metric_counts order by station_code 
,channel_name_inc_hd
,corrected_spot_transmission_start_datetime
,corrected_spot_transmission_end_datetime
,spot_duration
;

output to 'C:\Users\barnetd\Documents\trading metrics.xls' format excel;
*/

---Part 02 - Load in Spot Traded demographic information (Supplied by Chris Thomas)---

create table vespa_analysts.project047_spots_and_traded_demographics
(
    channel varchar(32)
    , partner varchar(16)
    , country varchar(10)
    , timeshift varchar(1)
    , break_date varchar(8)
    ,tx_time varchar(6)
    ,demo_code varchar(6)
    ,demograph varchar(32)
    ,product varchar(64)
    ,length integer
    ,nominal_price decimal(10,2)
    ,actual_impacts decimal(10,2)
);

commit;
input into vespa_analysts.project047_spots_and_traded_demographics from 'C:\Users\barnetd\Documents\Project 047 - Adsmart Wastage Analysis\spots and demo.csv' format ascii;
commit;


alter table vespa_analysts.project047_spots_and_traded_demographics add raw_corrected_spot_time varchar(6);
alter table vespa_analysts.project047_spots_and_traded_demographics add corrected_spot_transmission_date date;

update vespa_analysts.project047_spots_and_traded_demographics
set raw_corrected_spot_time= case 
      when left (tx_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (tx_time,2) as integer)-24 ||right (tx_time,4) 
      else tx_time end 
,corrected_spot_transmission_date  = case when left (tx_time,2) in ('24','25','26','27','28','29') then cast(break_date as date)+1
else cast(break_date as date) end
from vespa_analysts.project047_spots_and_traded_demographics
;

alter table  vespa_analysts.project047_spots_and_traded_demographics add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.project047_spots_and_traded_demographics
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project047_spots_and_traded_demographics
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project047_spots_and_traded_demographics
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;


--drop table vespa_analysts.project047_spots_viewing_channel_lookup;
create table vespa_analysts.project047_spots_viewing_channel_lookup
(    spot_channel varchar(64)
    , channel_name_inc_hd varchar(64)
);

commit;
input into vespa_analysts.project047_spots_viewing_channel_lookup
from 'C:\Users\barnetd\Documents\Project 047 - Adsmart Wastage Analysis\spot to viewing channel name lookup.csv' format ascii;
commit;

--select top 100 * from vespa_analysts.project047_spots_and_traded_demographics;
--select country , count(*) from vespa_analysts.project047_spots_and_traded_demographics group by country;

--delete from vespa_analysts.project047_spots_and_traded_demographics where country is null;

alter table  vespa_analysts.project047_spots_and_traded_demographics add channel_name_inc_hd varchar(64);

update vespa_analysts.project047_spots_and_traded_demographics
set channel_name_inc_hd = b.channel_name_inc_hd
from vespa_analysts.project047_spots_and_traded_demographics as a
left outer join vespa_analysts.project047_spots_viewing_channel_lookup as b
on a.channel = b.spot_channel
;
commit;



---Update viewed spot data with traded demographic info---
--drop table vespa_analysts_project_047_spot_and_demo_audience_figures;
select a.*
    , b.partner 
    ,b.demo_code
    ,b.demograph
    ,b.product 
   
    ,b.nominal_price 
    ,b.actual_impacts
, case when demograph = 'ADULTS' then households_viewing 
       when demograph = 'ADULTS 16-34' then adult_18_34_households_viewing
       when demograph = 'ADULTS 16-44' then adult_18_44_households_viewing
       when demograph = 'ADULTS 16-54' then adult_18_54_households_viewing
       when demograph = 'ADULTS 25-44' then adult_25_44_households_viewing
       when demograph = 'ADULTS ABC1' then adult_ABC1_households_viewing
       when demograph = 'CHILDREN' then hh_with_children_households_viewing
       when demograph = 'HOUSEWIVES' then households_viewing
       when demograph = 'HOUSEWIVES 16-44' then adult_18_44_households_viewing
       when demograph = 'HOUSEWIVES 16-54' then adult_18_54_households_viewing

       when demograph = 'HOUSEWIVES ABC1' then adult_ABC1_households_viewing
       when demograph = 'HOUSEWIVES WITH CHILDREN' then hh_with_children_households_viewing

       when demograph = 'MEN' then adult_male_households_viewing
       when demograph = 'MEN 16-34' then adult_male_18_34_households_viewing
       when demograph = 'MEN ABC1' then adult_male_ABC1_households_viewing

       when demograph = 'WOMEN' then adult_female_households_viewing
       when demograph = 'WOMEN 16-34' then adult_female_18_34_households_viewing
       when demograph = 'WOMEN 16-44' then adult_female_18_44_households_viewing
       when demograph = 'WOMEN 16-54' then adult_female_18_54_households_viewing
       when demograph = 'WOMEN ABC1' then adult_female_ABC1_households_viewing

else 0 end as target_demographic_households_viewing
into vespa_analysts_project_047_spot_and_demo_audience_figures
from vespa_analysts.vespa_spots_with_trading_metric_counts as a
left outer join vespa_analysts.project047_spots_and_traded_demographics as b
on a.channel_name_inc_hd = b.channel_name_inc_hd and a.corrected_spot_transmission_start_datetime=b.corrected_spot_transmission_start_datetime
--where a.channel_name_inc_hd = 'Sky Sports 1'
where demograph is not null
;
commit;
--select * from vespa_analysts.project047_spots_and_traded_demographics where demograph = 'CHILDREN';
--select count(*) from vespa_analysts_project_047_spot_and_demo_audience_figures;
--select top 100 * from vespa_analysts.vespa_spots_with_trading_metric_counts;
--select top 100 * from vespa_analysts_project_047_spot_and_demo_audience_figures;




---Output spreadsheet of all spots for all Sky channels with Target and Mirror HH volumes---
select * from vespa_analysts_project_047_spot_and_demo_audience_figures;

output to 'C:\Users\barnetd\Documents\Project 047 - Adsmart Wastage Analysis\Project  047 - demographic mirror details of spots.xls' format excel;



---Update 20120403 Add in if box is adsmartable or not----

select account_number
,cb_key_household
,subscriber_id
into vespa_analysts.vespa_data_subscribers_20120115
from  vespa_analysts.VESPA_all_viewing_records_20120115
where cast(Adjusted_Event_Start_Time as date) = '2012-01-15'
group by account_number
,cb_key_household
,subscriber_id;
--select count(*) from vespa_analysts.vespa_data_subscribers_20120115;

Alter table vespa_analysts.vespa_data_subscribers_20120115
add pvr tinyint default 0,
add box_type varchar(2) default 'SD',
add primary_box bit default 0,
add package varchar(30) default 'Basic';
commit;


--Add on box details – most recent dw_created_dt for a box (where a box hasn’t been replaced at that date)  taken from cust_set_top_box.  
--This removes instances where more than one box potentially live for a subscriber_id at a time (due to null box installed and replaced dates).

SELECT account_number
,service_instance_id
,max(dw_created_dt) as max_dw_created_dt
  INTO #boxes -- drop table #boxes
  FROM sk_prod.CUST_SET_TOP_BOX  
 WHERE (box_installed_dt <= cast('2012-01-15'  as date) 
   AND box_replaced_dt   > cast('2012-01-15'  as date)) or box_installed_dt is null
group by account_number
,service_instance_id
 ;

--select count(*) from vespa_analysts.aug_22_base_details;
commit;


commit;
exec sp_create_tmp_table_idx '#boxes', 'account_number';
exec sp_create_tmp_table_idx '#boxes', 'service_instance_id';
exec sp_create_tmp_table_idx '#boxes', 'max_dw_created_dt';
--select account_number , count(di

---Create table of one record per service_instance_id---
SELECT acc.account_number
,acc.service_instance_id
,min(stb.x_pvr_type) as pvr_type
,min(stb.x_box_type) as box_type
,min(stb.x_description) as description_x
,min(stb.x_manufacturer) as manufacturer
,min(stb.x_model_number) as model_number
  INTO #boxes_with_model_info -- drop table #boxes
  FROM #boxes  AS acc left outer join sk_prod.CUST_SET_TOP_BOX AS stb 
        ON acc.account_number = stb.account_number
 and acc.max_dw_created_dt=stb.dw_created_dt
group by acc.account_number
,acc.service_instance_id
 ;

commit;
exec sp_create_tmp_table_idx '#boxes_with_model_info', 'service_instance_id';


---Add on Service instance ID
-- B01 - Create Base

--creating a date variable to use throughout the code (Date used in for end of day prior to target date (e.g., status at end of 28th Nov)
  create variable @target_date date;
     set @target_date = '20120115';


if object_id('sky_base_2012_01_15') is not null drop table vespa_analysts.sky_base_2012_01_15;
CREATE TABLE vespa_analysts.sky_base_2012_01_15 ( -- drop table govt_region_base
         account_number                  varchar(30)   NOT NULL
         ,cb_key_household                 bigint      
         ,current_short_description      varchar(70)
         ,postcode                        varchar(10)  default 'Unknown'
        ,service_instance_id            varchar(50) 
        ,SUBSCRIPTION_SUB_TYPE          varchar(50) 
);


--drop  index   vespa_analysts.sky_base_2012_01_15.idx1;

create hg index idx1 on vespa_analysts.sky_base_2012_01_15(account_number);
create hg index idx2 on vespa_analysts.sky_base_2012_01_15(cb_key_household);

grant all on vespa_analysts.sky_base_2012_01_15               to public;


select account_number
        , cb_key_household
        , csh.current_short_description
        ,service_instance_id
        ,SUBSCRIPTION_SUB_TYPE
        , rank() over (partition by account_number ,SUBSCRIPTION_SUB_TYPE,service_instance_id order by effective_from_dt, cb_row_id) as rank
into #sky_accounts -- drop table #sky_accounts
from sk_prod.cust_subs_hist as csh
where SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing','DTV Extra Subscription') --the DTV + Multiroom sub Type
   and status_code in ('AC','PC','AB')               --Active Status Codes (Including Active Block)
   and effective_from_dt <= @target_date             --Start on or before date
   and effective_to_dt > @target_date                --ends after date
   and effective_from_dt<>effective_to_dt            --ignore all but the last thing each customer did in a day

and account_number is not null;
commit;


delete from #sky_accounts where rank>1;
commit;

create hg index idx1 on #sky_accounts(service_instance_id);

create  hg index idx2 on #sky_accounts(cb_key_household);

insert into vespa_analysts.sky_base_2012_01_15 (account_number, cb_key_household, current_short_description,service_instance_id,SUBSCRIPTION_SUB_TYPE)
select account_number, cb_key_household, current_short_description,service_instance_id,SUBSCRIPTION_SUB_TYPE
from #sky_accounts
;
commit;


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

commit;
exec sp_create_tmp_table_idx '#subs_details', 'src_system_id';

commit;


--alter table vespa_analysts.F1_analysis_20111104 delete subscription_type;
alter table vespa_analysts.sky_base_2012_01_15 add subscriber_id bigint;
alter table vespa_analysts.sky_base_2012_01_15 add primary_box tinyint;

update vespa_analysts.sky_base_2012_01_15
set subscriber_id=b.subscriberid
,primary_box =b.primary_box
from vespa_analysts.sky_base_2012_01_15 as a
left outer join #subs_details as b
on a.service_instance_id=b.src_system_id
;
commit;
--exec gen_create_table 'vespa_analysts.sky_base_2012_01_15';
--alter table vespa_analysts.vespa_data_subscribers_20120115 delete service_instance_id;
alter table vespa_analysts.vespa_data_subscribers_20120115 add service_instance_id varchar(50) ;
--alter table vespa_analysts.vespa_data_subscribers_20120115 add primary_box tinyint;

update vespa_analysts.vespa_data_subscribers_20120115
set service_instance_id = b.service_instance_id
,primary_box =case when b.primary_box is null then 0 else b.primary_box end
from vespa_analysts.vespa_data_subscribers_20120115 as a
left outer join vespa_analysts.sky_base_2012_01_15 as b
on a.subscriber_id = b.subscriber_id
;

commit;

--select top 500 * from vespa_analysts.vespa_data_subscribers_20120115;

/*
alter table vespa_analysts.vespa_data_subscribers_20120115 add x_pvr_type  varchar(50);
alter table vespa_analysts.vespa_data_subscribers_20120115 add x_box_type  varchar(20);
alter table vespa_analysts.vespa_data_subscribers_20120115 add x_description  varchar(100);
alter table vespa_analysts.vespa_data_subscribers_20120115 add x_manufacturer  varchar(50);
alter table vespa_analysts.vespa_data_subscribers_20120115 add x_model_number  varchar(50);
*/

update  vespa_analysts.vespa_data_subscribers_20120115
set x_pvr_type=b.pvr_type
,x_box_type=b.box_type

,x_description=b.description_x
,x_manufacturer=b.manufacturer
,x_model_number=b.model_number
from vespa_analysts.vespa_data_subscribers_20120115 as a
left outer join #boxes_with_model_info as b
on a.service_instance_id=b.service_instance_id
;
commit;

--e


--select top 100 * from vespa_analysts.vespa_data_subscribers_20120115;


update vespa_analysts.vespa_data_subscribers_20120115
set pvr =case when x_pvr_type like '%PVR%' then 1 else 0 end
,box_type =case when x_box_type like '%HD%' then 'HD' else 'SD' end
from vespa_analysts.vespa_data_subscribers_20120115
;

commit;


--select top 100 * from  vespa_analysts.vespa_data_subscribers_20120115 ;

--select x_manufacturer , count(*) from vespa_analysts.vespa_data_subscribers_20120115 group by x_manufacturer;









---Repeat Summary by Spot info but add in TV Region/Affluence/Lifestage for use in profiling---

---

---


--drop table #adsmartable_box;
--
select subscriber_id
,max(case when x_pvr_type in ('PVR5','PVR6','PVR7') 
            OR ( x_pvr_type='PVR4' AND  x_manufacturer in ('Pace','Samsung','Thomson')) 
          then 1 else 0 end) as adsmartable
into #adsmartable_box
from vespa_analysts.vespa_data_subscribers_20120115 
group by subscriber_id
;

commit;
create hg index idx1 on  #adsmartable_box(subscriber_id);

--select head_hh_agef , count(*) from vespa_analysts.jan_15_vespa_scaling_lookup group by head_hh_agef;

--drop table vespa_analysts.vespa_spots_with_trading_metric_counts_with_hh_attributes;
select station_code 
,channel_name_inc_hd
,corrected_spot_transmission_start_datetime
,corrected_spot_transmission_end_datetime
,spot_duration
,c.isba_tv_region
,c.affluence
,b.head_hh_agef
,d.adsmartable
,sum(case when seconds_of_spot_viewed_live >0 then b.weighting else 0 end) as households_viewing
,sum(case when seconds_of_spot_viewed_live >0 and (c.lifestage = 'Unknown' or c.lifestage is null or b.head_hh_agef is null) then b.weighting else 0 end) as unknown_demographic_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_18_34*b.weighting else 0 end) as adult_18_34_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_18_44*b.weighting else 0 end) as adult_18_44_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_18_54*b.weighting else 0 end) as adult_18_54_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_25_44*b.weighting else 0 end) as adult_25_44_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_ABC1*b.weighting else 0 end) as adult_ABC1_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then hh_with_children*b.weighting else 0 end) as hh_with_children_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_male*b.weighting else 0 end) as adult_male_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_male_18_34*b.weighting else 0 end) as adult_male_18_34_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_male_ABC1*b.weighting else 0 end) as adult_male_ABC1_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_female_18_34*b.weighting else 0 end) as adult_female_18_34_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_female_18_44*b.weighting else 0 end) as adult_female_18_44_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_female_18_54*b.weighting else 0 end) as adult_female_18_54_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_female_ABC1*b.weighting else 0 end) as adult_female_ABC1_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_female*b.weighting else 0 end) as adult_female_households_viewing
into vespa_analysts.vespa_spots_with_trading_metric_counts_with_hh_attributes
from vespa_analysts.vespa_spot_data_By_channel as a
left outer join vespa_analysts.jan_15_vespa_scaling_lookup as b
on a.account_number =b.account_number
left outer join vespa_analysts.scaling_segments_lookup as c
on a.scaling_segment_id=c.scaling_segment_id
left outer join #adsmartable_box as d
on a.subscriber_id=d.subscriber_id

group by station_code 
,channel_name_inc_hd
,corrected_spot_transmission_start_datetime
,corrected_spot_transmission_end_datetime
,spot_duration
,c.isba_tv_region
,c.affluence
,b.head_hh_agef
,d.adsmartable
;



commit;
--select distinct lifestage from vespa_analysts.scaling_segments_lookup;
--select top 100 * from vespa_analysts.jan_15_vespa_scaling_lookup;
--select top 100 * from vespa_analysts.vespa_spots_with_trading_metric_counts_with_hh_attributes;
--drop table vespa_analysts_project_047_spot_and_demo_audience_figures_with_hh_attributes;
select a.channel_name_inc_hd
,a.spot_duration
,a.isba_tv_region
,a.affluence
,a.head_hh_agef
,a.adsmartable
    , b.partner 
    ,b.demo_code
    ,b.demograph
    ,b.product 
   
    ,b.nominal_price 
    ,b.actual_impacts
,sum(households_viewing) as total_hh_viewing
,sum(case when demograph = 'ADULTS' then households_viewing 
       when demograph = 'HOUSEWIVES' then households_viewing
       when unknown_demographic_households_viewing>0 then 0  
       when demograph = 'ADULTS 16-34' then adult_18_34_households_viewing
       when demograph = 'ADULTS 16-44' then adult_18_44_households_viewing
       when demograph = 'ADULTS 16-54' then adult_18_54_households_viewing
       when demograph = 'ADULTS 25-44' then adult_25_44_households_viewing
       when demograph = 'ADULTS ABC1' then adult_ABC1_households_viewing
       when demograph = 'CHILDREN' then hh_with_children_households_viewing

       when demograph = 'HOUSEWIVES 16-44' then adult_18_44_households_viewing
       when demograph = 'HOUSEWIVES 16-54' then adult_18_54_households_viewing

       when demograph = 'HOUSEWIVES ABC1' then adult_ABC1_households_viewing
       when demograph = 'HOUSEWIVES WITH CHILDREN' then hh_with_children_households_viewing

       when demograph = 'MEN' then adult_male_households_viewing
       when demograph = 'MEN 16-34' then adult_male_18_34_households_viewing
       when demograph = 'MEN ABC1' then adult_male_ABC1_households_viewing

       when demograph = 'WOMEN' then adult_female_households_viewing
       when demograph = 'WOMEN 16-34' then adult_female_18_34_households_viewing
       when demograph = 'WOMEN 16-44' then adult_female_18_44_households_viewing
       when demograph = 'WOMEN 16-54' then adult_female_18_54_households_viewing
       when demograph = 'WOMEN ABC1' then adult_female_ABC1_households_viewing

else 0 end) as target_demographic_households_viewing
,sum(case when demograph in ('ADULTS','HOUSEWIVES') then 0 else unknown_demographic_households_viewing end) as unknown_demographic
into vespa_analysts_project_047_spot_and_demo_audience_figures_with_hh_attributes
from vespa_analysts.vespa_spots_with_trading_metric_counts_with_hh_attributes as a
left outer join vespa_analysts.project047_spots_and_traded_demographics as b
on a.channel_name_inc_hd = b.channel_name_inc_hd and a.corrected_spot_transmission_start_datetime=b.corrected_spot_transmission_start_datetime
--where a.channel_name_inc_hd = 'Sky Sports 1'
where demograph is not null
group by a.channel_name_inc_hd
,a.spot_duration
,a.isba_tv_region
,a.affluence
,a.adsmartable
,a.head_hh_agef
    , b.partner 
    ,b.demo_code
    ,b.demograph
    ,b.product 
   
    ,b.nominal_price 
    ,b.actual_impacts
;

--select  lifestage , count(*) as records from vespa_analysts.vespa_spots_with_trading_metric_counts_with_hh_attributes group by lifestage order by records desc;
--select top 100 * from vespa_analysts.project047_spots_and_traded_demographics;
--select top 100 * from vespa_analysts.vespa_spots_with_trading_metric_counts_with_hh_attributes;
commit;

--Due to size of output, Spot length is excluded.  Also only 20/30 second duration spots included (as requested in brief)

--drop table #attribute_output_for_spot_demo_audience;
select demograph
,channel_name_inc_hd
--,spot_duration
,isba_tv_region
,adsmartable
,case when affluence = 'Very High' then '01: Very High'
      when affluence = 'High' then '02: High'
      when affluence = 'Mid High' then '03: Mid High'
      when affluence = 'Mid' then '04: Mid'
      when affluence = 'Mid Low' then '05: Mid Low'
      when affluence = 'Low' then '06: Low'
      when affluence = 'Very Low' then '07: Very Low'
      when affluence = 'Unknown' then '08: Unknown' else '08: Unknown' end as affluence_group

,case when head_hh_agef   in (1,2,3) then '01: 18-34'

when head_hh_agef  in (4,5,6,7) then '02: 35-54'

when head_hh_agef  >7 then '03: 55+' else '04: Unknown' end as age_group

, sum(total_hh_viewing) as total_households_viewing_spot
, sum(target_demographic_households_viewing) as target_demographic_households_viewing_spot
, sum(total_hh_viewing-target_demographic_households_viewing-unknown_demographic) as mirror_segment_viewing_spot
, sum(unknown_demographic) as unknown_segment_viewing_spot
into #attribute_output_for_spot_demo_audience
from vespa_analysts_project_047_spot_and_demo_audience_figures_with_hh_attributes
where affluence is not null and spot_duration in (20,30) and adsmartable is not null
group by demograph
,channel_name_inc_hd
,adsmartable
--,spot_duration
,isba_tv_region
,affluence_group
,age_group 
;

commit;

--select min (total_households_viewing_spot) from #attribute_output_for_spot_demo_audience;
--select * from #attribute_output_for_spot_demo_audience where total_households_viewing_spot = 0;
--select * from #attribute_output_for_spot_demo_audience where mirror_segment_viewing_spot <0;

--select distinct lifestage from #attribute_output_for_spot_demo_audience order by lifestage;
--select sum(total_households_viewing_spot), sum(target_demographic_households_viewing_spot),sum(mirror_segment_viewing_spot), sum(unknown_segment_viewing_spot) from #attribute_output_for_spot_demo_audience;

--select count(*) from #attribute_output_for_spot_demo_audience
select * from #attribute_output_for_spot_demo_audience;
output to 'C:\Users\barnetd\Documents\Project 047 - Adsmart Wastage Analysis\attribute for spot demographic v07 (with adsmartable split).csv' format ascii;

---This CSV output gets pasted in to the 'attribute for spot demographic' worksheet of Project 047 - Pivot for Target and Mirror Profiles v2 (with revised ABC1 definitions).xls

---Part 03 Reach and Frequency---

--select top 100 * from  vespa_analysts_project_047_spot_and_demo_audience_figures_with_hh_attributes;

---Add Spot metrics on to data that has viewing summary by subscriber and spot---
alter table vespa_analysts.vespa_spot_data_By_channel add households_viewing decimal(12,4);
alter table vespa_analysts.vespa_spot_data_By_channel add adult_18_34_households_viewing decimal(12,4);
alter table vespa_analysts.vespa_spot_data_By_channel add adult_18_44_households_viewing decimal(12,4);
alter table vespa_analysts.vespa_spot_data_By_channel add adult_18_54_households_viewing decimal(12,4);
alter table vespa_analysts.vespa_spot_data_By_channel add adult_25_44_households_viewing decimal(12,4);
alter table vespa_analysts.vespa_spot_data_By_channel add adult_ABC1_households_viewing decimal(12,4);
alter table vespa_analysts.vespa_spot_data_By_channel add hh_with_children_households_viewing decimal(12,4);
alter table vespa_analysts.vespa_spot_data_By_channel add adult_male_households_viewing decimal(12,4);
alter table vespa_analysts.vespa_spot_data_By_channel add adult_male_18_34_households_viewing decimal(12,4);
alter table vespa_analysts.vespa_spot_data_By_channel add adult_male_ABC1_households_viewing decimal(12,4);
alter table vespa_analysts.vespa_spot_data_By_channel add adult_female_18_34_households_viewing decimal(12,4);
alter table vespa_analysts.vespa_spot_data_By_channel add adult_female_18_44_households_viewing decimal(12,4);
alter table vespa_analysts.vespa_spot_data_By_channel add adult_female_18_54_households_viewing decimal(12,4);
alter table vespa_analysts.vespa_spot_data_By_channel add adult_female_ABC1_households_viewing decimal(12,4);
alter table vespa_analysts.vespa_spot_data_By_channel add adult_female_households_viewing decimal(12,4);

update vespa_analysts.vespa_spot_data_By_channel
set households_viewing= case when seconds_of_spot_viewed_live >0 then b.weighting else 0 end
,adult_18_34_households_viewing= case when seconds_of_spot_viewed_live >0 then adult_18_34*b.weighting else 0 end 
,adult_18_44_households_viewing= case when seconds_of_spot_viewed_live >0 then adult_18_44*b.weighting else 0 end 
,adult_18_54_households_viewing= case when seconds_of_spot_viewed_live >0 then adult_18_54*b.weighting else 0 end 
,adult_25_44_households_viewing= case when seconds_of_spot_viewed_live >0 then adult_25_44*b.weighting else 0 end 
,adult_ABC1_households_viewing= case when seconds_of_spot_viewed_live >0 then adult_ABC1*b.weighting else 0 end 
,hh_with_children_households_viewing= case when seconds_of_spot_viewed_live >0 then hh_with_children*b.weighting else 0 end 
,adult_male_households_viewing= case when seconds_of_spot_viewed_live >0 then adult_male*b.weighting else 0 end 
,adult_male_18_34_households_viewing= case when seconds_of_spot_viewed_live >0 then adult_male_18_34*b.weighting else 0 end 
,adult_male_ABC1_households_viewing= case when seconds_of_spot_viewed_live >0 then adult_male_ABC1*b.weighting else 0 end 
,adult_female_18_34_households_viewing= case when seconds_of_spot_viewed_live >0 then adult_female_18_34*b.weighting else 0 end 
,adult_female_18_44_households_viewing= case when seconds_of_spot_viewed_live >0 then adult_female_18_44*b.weighting else 0 end 
,adult_female_18_54_households_viewing= case when seconds_of_spot_viewed_live >0 then adult_female_18_54*b.weighting else 0 end 
,adult_female_ABC1_households_viewing= case when seconds_of_spot_viewed_live >0 then adult_female_ABC1*b.weighting else 0 end 
,adult_female_households_viewing= case when seconds_of_spot_viewed_live >0 then adult_female*b.weighting else 0 end 
from vespa_analysts.vespa_spot_data_By_channel as a
left outer join vespa_analysts.jan_15_vespa_scaling_lookup as b
on a.account_number =b.account_number
;
commit;

----Add on Target Demograph---


alter table vespa_analysts.vespa_spot_data_By_channel add demograph varchar(32);

update vespa_analysts.vespa_spot_data_By_channel
set demograph=b.demograph
from vespa_analysts.vespa_spot_data_By_channel as a 
left outer join vespa_analysts.project047_spots_and_traded_demographics as b
on a.channel_name_inc_hd = b.channel_name_inc_hd and a.corrected_spot_transmission_start_datetime=b.corrected_spot_transmission_start_datetime
;
commit;

alter table vespa_analysts.vespa_spot_data_By_channel add target_households_viewing decimal(12,4);
alter table vespa_analysts.vespa_spot_data_By_channel add mirror_households_viewing decimal(12,4);

update vespa_analysts.vespa_spot_data_By_channel
set target_households_viewing=case when demograph = 'ADULTS' then households_viewing 
       when demograph = 'ADULTS 16-34' then adult_18_34_households_viewing
       when demograph = 'ADULTS 16-44' then adult_18_44_households_viewing
       when demograph = 'ADULTS 16-54' then adult_18_54_households_viewing
       when demograph = 'ADULTS 25-44' then adult_25_44_households_viewing
       when demograph = 'ADULTS ABC1' then adult_ABC1_households_viewing
       when demograph = 'CHILDREN' then hh_with_children_households_viewing
       when demograph = 'HOUSEWIVES' then households_viewing
       when demograph = 'HOUSEWIVES 16-44' then adult_18_44_households_viewing
       when demograph = 'HOUSEWIVES 16-54' then adult_18_54_households_viewing

       when demograph = 'HOUSEWIVES ABC1' then adult_ABC1_households_viewing
       when demograph = 'HOUSEWIVES WITH CHILDREN' then hh_with_children_households_viewing

       when demograph = 'MEN' then adult_male_households_viewing
       when demograph = 'MEN 16-34' then adult_male_18_34_households_viewing
       when demograph = 'MEN ABC1' then adult_male_ABC1_households_viewing

       when demograph = 'WOMEN' then adult_female_households_viewing
       when demograph = 'WOMEN 16-34' then adult_female_18_34_households_viewing
       when demograph = 'WOMEN 16-44' then adult_female_18_44_households_viewing
       when demograph = 'WOMEN 16-54' then adult_female_18_54_households_viewing
       when demograph = 'WOMEN ABC1' then adult_female_ABC1_households_viewing

else 0 end

from vespa_analysts.vespa_spot_data_By_channel as a 
;
commit;


update vespa_analysts.vespa_spot_data_By_channel
set mirror_households_viewing=households_viewing-target_households_viewing
from vespa_analysts.vespa_spot_data_By_channel
;


---Add on Profile Attributes 
alter table vespa_analysts.vespa_spot_data_By_channel add isba_tv_region varchar(32);
alter table vespa_analysts.vespa_spot_data_By_channel add affluence varchar(32);
alter table vespa_analysts.vespa_spot_data_By_channel add lifestage varchar(32);

update vespa_analysts.vespa_spot_data_By_channel
set isba_tv_region=c.isba_tv_region
,affluence=c.affluence
,lifestage=c.lifestage
from vespa_analysts.vespa_spot_data_By_channel as a
left outer join vespa_analysts.scaling_segments_lookup as c
on a.scaling_segment_id=c.scaling_segment_id
;


alter table vespa_analysts.jan_15_vespa_scaling_lookup add isba_tv_region varchar(32);
alter table vespa_analysts.jan_15_vespa_scaling_lookup add affluence varchar(32);
alter table vespa_analysts.jan_15_vespa_scaling_lookup add lifestage varchar(32); 

update vespa_analysts.jan_15_vespa_scaling_lookup
set isba_tv_region=c.isba_tv_region
,affluence=c.affluence
,lifestage=c.lifestage
from vespa_analysts.jan_15_vespa_scaling_lookup as a
left outer join vespa_analysts.scaling_segments_lookup as c
on a.scaling_segment_id=c.scaling_segment_id
;
commit;

--select count(*) , sum(adsmartable) from #adsmartable_hh

--select x_pvr_type , count(*) from vespa_analysts.vespa_data_subscribers_20120115  group by x_pvr_type;

--select x_pvr_type, x_manufacturer,count(*) , sum(case when x_pvr_type='PVR5' OR (( x_pvr_type='PVR4' AND ( x_manufacturer='Pace' OR x_manufacturer='Samsung'))) then 1 else 0 end) as adsmartable from vespa_analysts.vespa_data_subscribers_20120115 group by x_pvr_type, x_manufacturer;


--select x_pvr_type , x_box_type, count(*) as boxes , sum(primary_box) as primary_boxes from vespa_analysts.vespa_data_subscribers_20120115 group by  x_pvr_type , x_box_type;
--select x_pvr_type , x_box_type, count(*) as boxes , sum(primary_box) as primary_boxes from vespa_analysts.vespa_data_subscribers_20120115 group by  x_pvr_type , x_box_type;

--select x_pvr_type, count(*) as boxes , sum(primary_box) as primary_boxes , sum(case when x_pvr_type='PVR5' OR (( x_pvr_type='PVR4' AND ( x_manufacturer='Pace' OR x_manufacturer='Samsung'))) then 1 else 0 end) as adsmartable from vespa_analysts.vespa_data_subscribers_20120115 group by  x_pvr_type  order by x_pvr_type;

--commit;


--select sum(weighting)  from vespa_analysts.jan_15_vespa_scaling_lookup where adult_18_34 is null



--drop table #account_summary_details;
select a.account_number
,a.isba_tv_region
,case when a.affluence = 'Very High' then '01: Very High'
      when a.affluence = 'High' then '02: High'
      when a.affluence = 'Mid High' then '03: Mid High'
      when a.affluence = 'Mid' then '04: Mid'
      when a.affluence = 'Mid Low' then '05: Mid Low'
      when a.affluence = 'Low' then '06: Low'
      when a.affluence = 'Very Low' then '07: Very Low'
      when a.affluence = 'Unknown' then '08: Unknown' else '08: Unknown' end as affluence_group
, case when a.lifestage  in (
'18-24 ,Left home'
,'25-34 ,Child 0-4'
,'25-34 ,Child 8-16'
,'25-34 ,Child5-7'
,'25-34 ,Couple (no kids)'
,'25-34 ,Single (no kids)') then '01: 18-34'

when a.lifestage  in (
'35-44 ,Couple (no kids)'
,'35-44 ,Single (no kids)'
,'35-54 ,Child 0-4'
,'35-54 ,Child 11-16'
,'35-54 ,Child 5-10'
,'35-54 ,Grown up children at home'
,'45-54 ,Couple (no kids)'
,'45-54 ,Single (no kids)'
) then '02: 35-54'

when a.lifestage  in (
'55-64 ,Not retired - couple'
,'55-64 ,Not retired - single'
,'55-64 ,Retired'
) then '03: 55-64'

when a.lifestage  in (

'65-74 ,Not retired'
,'65-74 ,Retired couple'
,'65-74 ,Retired single'
,'75+   ,Couple'
,'75+   ,Single'

) then '04: 65+'
when a.lifestage  in ('Unknown'
) then '05: Unknown' else '05: Unknown' end as age_group
,max(a.weighting) as households
,sum(case when b.mirror_households_viewing is null then 0 when mirror_households_viewing>0 then 1 else 0 end) as frequency 
into #account_summary_details
from vespa_analysts.jan_15_vespa_scaling_lookup as a
left outer join vespa_analysts.vespa_spot_data_By_channel as b
on a.account_number = b.account_number
group by a.account_number
,a.isba_tv_region
,affluence_group
,age_group
;

--select top 100 * from vespa_analysts.vespa_spot_data_By_channel;
--select top 100 * from vespa_analysts.jan_15_vespa_scaling_lookup;

select frequency
,isba_tv_region
,affluence_group
,age_group
,count(*)
,sum(households)
from #account_summary_details
group by frequency,isba_tv_region
,affluence_group
,age_group
order by frequency,isba_tv_region
,affluence_group
,age_group
;

commit;




commit;
--select mirror_households_viewing , count(*) from vespa_analysts.vespa_spot_data_By_channel group by mirror_households_viewing
--select top 100 *  from vespa_analysts.vespa_spot_data_By_channel;
--select top 100 *  from vespa_analysts.scaling_segments_lookup;

 
