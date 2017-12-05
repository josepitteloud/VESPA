

---Project 047 Ad SMart Tradeable Wastage

--http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=47&Source=http%3A%2F%2Frtci%2FLists%2FRTCI%2520IC%2520dev%2FProjectInceptionView%2Easpx

/*  From Sharepoint
To understand the viability of various AdSmart and linear trading approaches, we wish understand how much inventory could be created 
through just trading linear �wastage�.  In order to identify the reachable wastage from the BARB traded audience we have had to define a set of
 �Mirror Segments�, which define the reachable audiences for each BARB traded audience. 
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

This is then matched to the spot traded data to calculate what proportion of households that viewed the advert are not on the traded demographic

As data on sk_prod.ilu does not exactly match the traded demographics the following work arounds have been created
ILU Has Adults as 18+ BARB Traded Demographics are 16+
ILU_HHSocioEcon groups together C1 and C2 And has retired seperately where on BARB C1 and C2 are seperated and retired HH 
are grouped within the A/B/C1/C2/D/E classes
*/

---Part 01 - Create Household level information on whether household has anyone within each of the traded demographics

--drop table #household_summary_details;

select cb_key_household
,min(ILU_HHSocioEcon) as socio_demographic_level
,max(case when ILU_Agef in (1,2,3) then 1 else 0 end) as adult_18_34
,max(case when ILU_Agef in (1,2,3,4,5) then 1 else 0 end) as adult_18_44
,max(case when ILU_Agef in (1,2,3,4,5,6,7) then 1 else 0 end) as adult_18_54
,max(case when ILU_Agef in (2,3,4,5) then 1 else 0 end) as adult_25_44
,max(case when ILU_HHSocioEcon in  (1,2) then 1 else 0 end) as adult_ABC1C2
,max(case when ILU_iKid0004+ILU_iKid0507+ILU_iKid0810+ILU_iKid1116>0 then 1 else 0 end) as hh_with_children
,max(case when ILU_Gender in (0,1) then 1 else 0 end) as adult_male   --Includes Unknown gender (approx 4% of people)
,max(case when ILU_Agef in (1,2,3) and ILU_Gender in (0,1) then 1 else 0 end) as adult_male_18_34
,max(case when ILU_HHSocioEcon in  (1,2) and ILU_Gender in (0,1) then 1 else 0 end) as adult_male_ABC1C2
,max(case when ILU_Agef in (1,2,3) and ILU_Gender in (2) then 1 else 0 end) as adult_female_18_34
,max(case when ILU_Agef in (1,2,3,4,5) and ILU_Gender in (2) then 1 else 0 end) as adult_female_18_44
,max(case when ILU_Agef in (1,2,3,4,5,6,7) and ILU_Gender in (2) then 1 else 0 end) as adult_female_18_54
,max(case when ILU_HHSocioEcon in  (1,2) and ILU_Gender in (2) then 1 else 0 end) as adult_female_ABC1C2
,max(case when ILU_Gender in (2) then 1 else 0 end) as adult_female   
into #household_summary_details
from sk_prod.ILU
where ILU_Correspondent  in ('P1','P2','OR')  and cb_address_status = '1' and cb_address_dps is not null  
group by cb_key_household
;

--select adult_18_34 ,adult_male, adult_male_18_34 , adult_female_18_34 , count(*) from #household_summary_details group by  adult_18_34 ,adult_male, adult_male_18_34 , adult_female_18_34  order by  adult_18_34 ,adult_male, adult_male_18_34 , adult_female_18_34 ;
--select adult_male , adult_female , count(*) from #household_summary_details group by adult_male , adult_female order by adult_male , adult_female;

select count(*) as households
,sum(adult_18_34)
,sum(adult_18_44)
,sum(adult_18_54)
,sum(adult_25_44)
,sum(adult_ABC1C2)
,sum(hh_with_children)
,sum(adult_male)
,sum(adult_male_18_34)
,sum(adult_male_ABC1C2)
,sum(adult_female_18_34)
,sum(adult_female_18_44)
,sum(adult_female_18_54)
,sum(adult_female_ABC1C2)
,sum(adult_female)
from  #household_summary_details
;  
commit;
----Currently Active Sky Households----


select distinct cb_key_household
into #sky_active_hh
from sk_prod.cust_single_account_view
where cb_address_status = '1' and cb_address_dps is not null and acct_status_code in ('AC','AB','PC')
;

commit;
create hg index idx1 on  #household_summary_details(cb_key_household);
create hg index idx1 on #sky_active_hh(cb_key_household);
commit;

select count(*) as households
,sum(adult_18_34)
,sum(adult_18_44)
,sum(adult_18_54)
,sum(adult_25_44)
,sum(adult_ABC1C2)
,sum(hh_with_children)
,sum(adult_male)
,sum(adult_male_18_34)
,sum(adult_male_ABC1C2)
,sum(adult_female_18_34)
,sum(adult_female_18_44)
,sum(adult_female_18_54)
,sum(adult_female_ABC1C2)
,sum(adult_female)
from  #household_summary_details as a
left outer join #sky_active_hh as b
on a.cb_key_household=b.cb_key_household
where b.cb_key_household is not null
;  


commit;

---Vespa Boxes returning data on 15th Jan 2012


select distinct cb_key_household 
into #vespa_hh
from  vespa_analysts.VESPA_all_viewing_records_20120115
where cast(Adjusted_Event_Start_Time as date) = '2012-01-15'
;
commit;
create hg index idx1 on #vespa_hh(cb_key_household);
commit;

select count(*) as households
,sum(adult_18_34)
,sum(adult_18_44)
,sum(adult_18_54)
,sum(adult_25_44)
,sum(adult_ABC1C2)
,sum(hh_with_children)
,sum(adult_male)
,sum(adult_male_18_34)
,sum(adult_male_ABC1C2)
,sum(adult_female_18_34)
,sum(adult_female_18_44)
,sum(adult_female_18_54)
,sum(adult_female_ABC1C2)
,sum(adult_female)
from  #household_summary_details as a
left outer join #vespa_hh as b
on a.cb_key_household=b.cb_key_household
where b.cb_key_household is not null
;
commit;

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

--drop table vespa_analysts.jan_15_vespa_scaling_lookup;
select a.account_number
,a.cb_key_household
,b.scaling_segment_id
,c.weighting
,d.adult_18_34
,d.adult_18_44
,d.adult_18_54
,d.adult_25_44
,d.adult_ABC1C2
,d.hh_with_children
,d.adult_male
,d.adult_male_18_34
,d.adult_male_ABC1C2
,d.adult_female_18_34
,d.adult_female_18_44
,d.adult_female_18_54
,d.adult_female_ABC1C2
,d.adult_female
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

select count(*) as households
,sum(adult_18_34)
,sum(adult_18_44)
,sum(adult_18_54)
,sum(adult_25_44)
,sum(adult_ABC1C2)
,sum(hh_with_children)
,sum(adult_male)
,sum(adult_male_18_34)
,sum(adult_male_ABC1C2)
,sum(adult_female_18_34)
,sum(adult_female_18_44)
,sum(adult_female_18_54)
,sum(adult_female_ABC1C2)
,sum(adult_female)

,sum(weighting) 
,sum(adult_18_34*weighting)
,sum(adult_18_44*weighting)
,sum(adult_18_54*weighting)
,sum(adult_25_44*weighting)
,sum(adult_ABC1C2*weighting)
,sum(hh_with_children*weighting)
,sum(adult_male*weighting)
,sum(adult_male_18_34*weighting)
,sum(adult_male_ABC1C2*weighting)
,sum(adult_female_18_34*weighting)
,sum(adult_female_18_44*weighting)
,sum(adult_female_18_54*weighting)
,sum(adult_female_ABC1C2*weighting)
,sum(adult_female*weighting)

from  #household_summary_details as a
left outer join vespa_analysts.jan_15_vespa_scaling_lookup as b
on a.cb_key_household=b.cb_key_household
where b.cb_key_household is not null
;


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
,sum(case when seconds_of_spot_viewed_live >0 then adult_ABC1C2*b.weighting else 0 end) as adult_ABC1C2_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then hh_with_children*b.weighting else 0 end) as hh_with_children_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_male*b.weighting else 0 end) as adult_male_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_male_18_34*b.weighting else 0 end) as adult_male_18_34_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_male_ABC1C2*b.weighting else 0 end) as adult_male_ABC1C2_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_female_18_34*b.weighting else 0 end) as adult_female_18_34_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_female_18_44*b.weighting else 0 end) as adult_female_18_44_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_female_18_54*b.weighting else 0 end) as adult_female_18_54_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_female_ABC1C2*b.weighting else 0 end) as adult_female_ABC1C2_households_viewing
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


/*
---Add on Pick TV which is not include in lookup----

update vespa_analysts.project047_spots_and_traded_demographics
set channel_name_inc_hd = 'Pick TV'
where channel = 'Pick TV'
;
*/


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
       when demograph = 'ADULTS ABC1' then adult_ABC1C2_households_viewing
       when demograph = 'CHILDREN' then hh_with_children_households_viewing
       when demograph = 'HOUSEWIVES' then households_viewing
       when demograph = 'HOUSEWIVES 16-44' then adult_18_44_households_viewing
       when demograph = 'HOUSEWIVES 16-54' then adult_18_54_households_viewing

       when demograph = 'HOUSEWIVES ABC1' then adult_ABC1C2_households_viewing
       when demograph = 'HOUSEWIVES WITH CHILDREN' then hh_with_children_households_viewing

       when demograph = 'MEN' then adult_male_households_viewing
       when demograph = 'MEN 16-34' then adult_male_18_34_households_viewing
       when demograph = 'MEN ABC1' then adult_male_ABC1C2_households_viewing

       when demograph = 'WOMEN' then adult_female_households_viewing
       when demograph = 'WOMEN 16-34' then adult_female_18_34_households_viewing
       when demograph = 'WOMEN 16-44' then adult_female_18_44_households_viewing
       when demograph = 'WOMEN 16-54' then adult_female_18_54_households_viewing
       when demograph = 'WOMEN ABC1' then adult_female_ABC1C2_households_viewing

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


select * from vespa_analysts_project_047_spot_and_demo_audience_figures;

output to 'C:\Users\barnetd\Documents\Project 047 - Adsmart Wastage Analysis\demographic mirror details of spots.xls' format excel;

---Add on TV Region, Age And Affluence of HH----

---Repeat Summary by Spot info but add in TV Region/Affluence/Lifestage---
--drop table vespa_analysts.vespa_spots_with_trading_metric_counts_with_hh_attributes;
select station_code 
,channel_name_inc_hd
,corrected_spot_transmission_start_datetime
,corrected_spot_transmission_end_datetime
,spot_duration
,c.isba_tv_region
,c.affluence
,c.lifestage
,sum(case when seconds_of_spot_viewed_live >0 then b.weighting else 0 end) as households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_18_34*b.weighting else 0 end) as adult_18_34_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_18_44*b.weighting else 0 end) as adult_18_44_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_18_54*b.weighting else 0 end) as adult_18_54_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_25_44*b.weighting else 0 end) as adult_25_44_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_ABC1C2*b.weighting else 0 end) as adult_ABC1C2_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then hh_with_children*b.weighting else 0 end) as hh_with_children_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_male*b.weighting else 0 end) as adult_male_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_male_18_34*b.weighting else 0 end) as adult_male_18_34_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_male_ABC1C2*b.weighting else 0 end) as adult_male_ABC1C2_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_female_18_34*b.weighting else 0 end) as adult_female_18_34_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_female_18_44*b.weighting else 0 end) as adult_female_18_44_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_female_18_54*b.weighting else 0 end) as adult_female_18_54_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_female_ABC1C2*b.weighting else 0 end) as adult_female_ABC1C2_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_female*b.weighting else 0 end) as adult_female_households_viewing
into vespa_analysts.vespa_spots_with_trading_metric_counts_with_hh_attributes
from vespa_analysts.vespa_spot_data_By_channel as a
left outer join vespa_analysts.jan_15_vespa_scaling_lookup as b
on a.account_number =b.account_number
left outer join vespa_analysts.scaling_segments_lookup as c
on a.scaling_segment_id=c.scaling_segment_id
group by station_code 
,channel_name_inc_hd
,corrected_spot_transmission_start_datetime
,corrected_spot_transmission_end_datetime
,spot_duration
,c.isba_tv_region
,c.affluence
,c.lifestage
;
commit;

select a.channel_name_inc_hd
,a.spot_duration
,a.isba_tv_region
,a.affluence
,a.lifestage
    , b.partner 
    ,b.demo_code
    ,b.demograph
    ,b.product 
   
    ,b.nominal_price 
    ,b.actual_impacts
,sum(households_viewing) as total_hh_viewing
,sum( case when demograph = 'ADULTS' then households_viewing 
       when demograph = 'ADULTS 16-34' then adult_18_34_households_viewing
       when demograph = 'ADULTS 16-44' then adult_18_44_households_viewing
       when demograph = 'ADULTS 16-54' then adult_18_54_households_viewing
       when demograph = 'ADULTS 25-44' then adult_25_44_households_viewing
       when demograph = 'ADULTS ABC1' then adult_ABC1C2_households_viewing
       when demograph = 'CHILDREN' then hh_with_children_households_viewing
       when demograph = 'HOUSEWIVES' then households_viewing
       when demograph = 'HOUSEWIVES 16-44' then adult_18_44_households_viewing
       when demograph = 'HOUSEWIVES 16-54' then adult_18_54_households_viewing

       when demograph = 'HOUSEWIVES ABC1' then adult_ABC1C2_households_viewing
       when demograph = 'HOUSEWIVES WITH CHILDREN' then hh_with_children_households_viewing

       when demograph = 'MEN' then adult_male_households_viewing
       when demograph = 'MEN 16-34' then adult_male_18_34_households_viewing
       when demograph = 'MEN ABC1' then adult_male_ABC1C2_households_viewing

       when demograph = 'WOMEN' then adult_female_households_viewing
       when demograph = 'WOMEN 16-34' then adult_female_18_34_households_viewing
       when demograph = 'WOMEN 16-44' then adult_female_18_44_households_viewing
       when demograph = 'WOMEN 16-54' then adult_female_18_54_households_viewing
       when demograph = 'WOMEN ABC1' then adult_female_ABC1C2_households_viewing

else 0 end) as target_demographic_households_viewing

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
,a.lifestage
    , b.partner 
    ,b.demo_code
    ,b.demograph
    ,b.product 
   
    ,b.nominal_price 
    ,b.actual_impacts
;

--select top 500 * from vespa_analysts_project_047_spot_and_demo_audience_figures_with_hh_attributes;

--select top 100 * from vespa_analysts.scaling_segments_lookup;

--select count(*) from vespa_analysts.vespa_spots_with_trading_metric_counts_with_hh_attributes;

--select channel_name_inc_hd , sum(nominal_price)as price from vespa_analysts_project_047_spot_and_demo_audience_figures group by channel_name_inc_hd order by price;

--select distinct demograph from vespa_analysts.project047_spots_and_traded_demographics order by demograph;

commit;

--drop table #attribute_output_for_spot_demo_audience;
select demograph
,channel_name_inc_hd
--,spot_duration
,isba_tv_region
,case when affluence = 'Very High' then '01: Very High'
      when affluence = 'High' then '02: High'
      when affluence = 'Mid High' then '03: Mid High'
      when affluence = 'Mid' then '04: Mid'
      when affluence = 'Mid Low' then '05: Mid Low'
      when affluence = 'Low' then '06: Low'
      when affluence = 'Very Low' then '07: Very Low'
      when affluence = 'Unknown' then '08: Unknown' else '08: Unknown' end as affluence_group
, case when lifestage  in (
'18-24 ,Left home'
,'25-34 ,Child 0-4'
,'25-34 ,Child 8-16'
,'25-34 ,Child5-7'
,'25-34 ,Couple (no kids)'
,'25-34 ,Single (no kids)') then '01: 18-34'

when lifestage  in (
'35-44 ,Couple (no kids)'
,'35-44 ,Single (no kids)'
,'35-54 ,Child 0-4'
,'35-54 ,Child 11-16'
,'35-54 ,Child 5-10'
,'35-54 ,Grown up children at home'
,'45-54 ,Couple (no kids)'
,'45-54 ,Single (no kids)'
) then '02: 35-54'

when lifestage  in (
'55-64 ,Not retired - couple'
,'55-64 ,Not retired - single'
,'55-64 ,Retired'
) then '03: 55-64'

when lifestage  in (

'65-74 ,Not retired'
,'65-74 ,Retired couple'
,'65-74 ,Retired single'
,'75+   ,Couple'
,'75+   ,Single'

) then '04: 65+'
when lifestage  in ('Unknown'
) then '05: Unknown' else '05: Unknown' end as age_group

, sum(total_hh_viewing) as total_households_viewing_spot
, sum(target_demographic_households_viewing) as target_demographic_households_viewing_spot
, sum(total_hh_viewing-target_demographic_households_viewing) as mirror_segment_viewing_spot
into #attribute_output_for_spot_demo_audience
from vespa_analysts_project_047_spot_and_demo_audience_figures_with_hh_attributes
where affluence is not null and spot_duration in (20,30)
group by demograph
,channel_name_inc_hd
--,spot_duration
,isba_tv_region
,affluence_group
,age_group 
;

commit;

--select distinct lifestage from #attribute_output_for_spot_demo_audience order by lifestage;


--select count(*) from #attribute_output_for_spot_demo_audience
select * from #attribute_output_for_spot_demo_audience;
output to 'C:\Users\barnetd\Documents\Project 047 - Adsmart Wastage Analysis\attribute for spot demographic.csv' format ascii;





/*
select * from 
vespa_analysts.project047_spots_and_traded_demographics 
where channel_name_inc_hd = 'Sky Sports 1'
order by corrected_spot_transmission_start_datetime
*/


--select * from vespa_analysts.project047_spots_viewing_channel_lookup

--select top 500 * from vespa_analysts.project047_spots_and_traded_demographics where channel_name_inc_hd='Pick TV';
--select top 500 * from vespa_analysts.project047_spots_viewing_channel_lookup;

--select distinct channel_name_inc_hd from vespa_analysts.VESPA_all_viewing_records_20120115 order by channel_name_inc_hd;
--select distinct channel_name_inc_hd from vespa_analysts.vespa_spots_with_trading_metric_counts order by channel_name_inc_hd;
--select distinct channel from vespa_analysts.project047_spots_and_traded_demographics order by channel
--select top 100 * from vespa_analysts.vespa_spot_data_By_channel;

select * from vespa_analysts.project047_spots_viewing_channel_lookup;
