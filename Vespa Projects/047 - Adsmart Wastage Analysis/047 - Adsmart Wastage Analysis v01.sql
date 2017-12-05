

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

This is then matched to the spot traded data to calculate what proportion of households that viewed the advert are not on the traded demographic

As data on sk_prod.ilu does not exactly match the traded demographics the following work arounds have been created
ILU Has Adults as 18+ BARB Traded Demographics are 16+
ILU_HHSocioEcon groups together C1 and C2 And has retired seperately where on BARB C1 and C2 are seperated and retired HH 
are grouped within the A/B/C1/C2/D/E classes


---Part 01 - Create Household level information on whether household has anyone within each of the traded demographics

--drop table #household_summary_details;

select cb_key_household
,min(ILU_HHSocioEcon) as socio_demographic_level
,max(case when ILU_Agef in (1,2,3) then 1 else 0 end) as adult_18_34
,max(case when ILU_Agef in (1,2,3,4,5) then 1 else 0 end) as adult_18_44
,max(case when ILU_Agef in (1,2,3,4,5,6,7) then 1 else 0 end) as adult_18_54
,max(case when ILU_Agef in (2,3,4,5,6,7) then 1 else 0 end) as adult_25_54
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


--select count(*) from #household_summary_details;

select count(*) as households
,sum(adult_18_34)
,sum(adult_18_44)
,sum(adult_18_54)
,sum(adult_25_54)
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
,sum(adult_25_54)
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
,sum(adult_25_54)
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
,d.adult_25_54
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
,sum(adult_25_54)
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
,sum(adult_25_54*weighting)
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

select station_code 
,channel_name_inc_hd
,corrected_spot_transmission_start_datetime
,corrected_spot_transmission_end_datetime
,spot_duration
,sum(case when seconds_of_spot_viewed_live >0 then b.weighting else 0 end) as households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_18_34*b.weighting else 0 end) as adult_18_34_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_18_44*b.weighting else 0 end) as adult_18_44_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_18_54*b.weighting else 0 end) as adult_18_54_households_viewing
,sum(case when seconds_of_spot_viewed_live >0 then adult_25_54*b.weighting else 0 end) as adult_25_54_households_viewing
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
    ,actual_impacts integer
);

commit;
input into vespa_analysts.project047_spots_and_traded_demographics from 'C:\Users\barnetd\Documents\Project 047 - Adsmart Wastage Analysis\spots and demo.csv' format ascii;
commit;

