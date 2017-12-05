

---Project 047 Case study of Individual Spots for Sky 1 and other channels---
--drop table #viewing_segment_summary_sky_1;

select  channel_name_inc_hd
,corrected_spot_transmission_start_datetime
,corrected_spot_transmission_end_datetime
,spot_duration
--,adsmartable_box
,demograph

,adsmartable_box
,isba_tv_region
,case when household_affluence_level in (16,17) then '01: Very High'
      when household_affluence_level in (13,14,15)  then '02: High'
      when household_affluence_level in (11,12)  then '03: Mid High'
      when household_affluence_level in (9,10)  then '04: Mid'
      when household_affluence_level in (7,8)  then '05: Mid Low'
      when household_affluence_level in (5,6)  then '06: Low'
      when household_affluence_level in (1,2,3,4)  then '07: Very Low'
      when household_affluence_level is null then '08: Unknown' else '08: Unknown' end as affluence_group
,case when head_hh_agef   in (1,2,3) then '01: 18-34'

when head_hh_agef  in (4,5,6,7) then '02: 35-54'

when head_hh_agef  >7 then '03: 55+' else '04: Unknown' end as age_group

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
into #viewing_segment_summary_sky_1
from vespa_analysts.vespa_spots_with_trading_metric_counts_with_hh_attributes_02_16_jan
where demograph is not null and channel_name_inc_hd = 'Sky 1' and cast(corrected_spot_transmission_start_datetime as date) ='2012-01-15'
group by channel_name_inc_hd
,corrected_spot_transmission_start_datetime
,corrected_spot_transmission_end_datetime
,spot_duration
,adsmartable_box
,demograph
,adsmartable_box
,affluence_group
,age_group
,isba_tv_region
order by channel_name_inc_hd
,corrected_spot_transmission_start_datetime
,corrected_spot_transmission_end_datetime
,spot_duration
,adsmartable_box
,demograph
,adsmartable_box
,affluence_group
,age_group
,isba_tv_region
;

select channel_name_inc_hd
,corrected_spot_transmission_start_datetime
,corrected_spot_transmission_end_datetime
,spot_duration
,demograph
, sum(total_hh_viewing) as total_households_viewing_spot
, sum(target_demographic_households_viewing) as target_demographic_households_viewing_spot
, sum(unknown_demographic) as unknown_segment_viewing_spot
, sum(total_hh_viewing-target_demographic_households_viewing-unknown_demographic) as mirror_segment_viewing_spot

, sum(case when adsmartable_box = 1 then total_hh_viewing else 0 end ) as total_households_viewing_spot_adsmartable_box
, sum(case when adsmartable_box = 1 then target_demographic_households_viewing else 0 end) as target_demographic_households_viewing_spot_adsmartable_box
, sum(case when adsmartable_box = 1 then unknown_demographic  else 0 end) as unknown_segment_viewing_spot_adsmartable_box
, sum(case when adsmartable_box = 1 then total_hh_viewing-target_demographic_households_viewing-unknown_demographic else 0 end) as mirror_segment_viewing_spot_adsmartable_box


from #viewing_segment_summary_sky_1
where adsmartable_box is not null
group by channel_name_inc_hd
,corrected_spot_transmission_start_datetime
,corrected_spot_transmission_end_datetime
,spot_duration
,demograph
order by channel_name_inc_hd
,corrected_spot_transmission_start_datetime
,corrected_spot_transmission_end_datetime
,spot_duration
,demograph
;

---Repeat but with Affluence Profile ---

select channel_name_inc_hd
,corrected_spot_transmission_start_datetime
,corrected_spot_transmission_end_datetime
,spot_duration
,demograph
, sum(total_hh_viewing) as total_households_viewing_spot
, sum(target_demographic_households_viewing) as target_demographic_households_viewing_spot
, sum(unknown_demographic) as unknown_segment_viewing_spot
, sum(total_hh_viewing-target_demographic_households_viewing-unknown_demographic) as mirror_segment_viewing_spot

, sum(case when  affluence_group ='01: Very High'  then total_hh_viewing-target_demographic_households_viewing-unknown_demographic else 0 end) as mirror_segment_viewing_spot_very_high_affluence
, sum(case when  affluence_group = '02: High'  then total_hh_viewing-target_demographic_households_viewing-unknown_demographic else 0 end) as mirror_segment_viewing_spot_high_affluence
, sum(case when  affluence_group = '03: Mid High' then total_hh_viewing-target_demographic_households_viewing-unknown_demographic else 0 end) as mirror_segment_viewing_spot_mid_high_affluence
, sum(case when  affluence_group = '04: Mid'  then total_hh_viewing-target_demographic_households_viewing-unknown_demographic else 0 end) as mirror_segment_viewing_spot_mid_affluence
, sum(case when  affluence_group = '05: Mid Low'  then total_hh_viewing-target_demographic_households_viewing-unknown_demographic else 0 end) as mirror_segment_viewing_spot_mid_low_affluence
, sum(case when  affluence_group = '06: Low'  then total_hh_viewing-target_demographic_households_viewing-unknown_demographic else 0 end) as mirror_segment_viewing_spot_low_affluence
, sum(case when  affluence_group = '07: Very Low'  then total_hh_viewing-target_demographic_households_viewing-unknown_demographic else 0 end) as mirror_segment_viewing_spot_very_low_affluence
, sum(case when  affluence_group  = '08: Unknown' then total_hh_viewing-target_demographic_households_viewing-unknown_demographic else 0 end) as mirror_segment_viewing_spot_unknown_affluence

from #viewing_segment_summary_sky_1
where adsmartable_box is not null
group by channel_name_inc_hd
,corrected_spot_transmission_start_datetime
,corrected_spot_transmission_end_datetime
,spot_duration
,demograph
order by channel_name_inc_hd
,corrected_spot_transmission_start_datetime
,corrected_spot_transmission_end_datetime
,spot_duration
,demograph
;


select tx_start_datetime_utc,epg_title from sk_prod.vespa_epg_dim where tx_date ='20120115'
and channel_name ='Sky1' and bss_name = 'Sky 1 Digital'
order by tx_date_time_utc


