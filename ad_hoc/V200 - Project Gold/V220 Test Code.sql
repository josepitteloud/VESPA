
--select top 100 * from v220_zero_mix_active_uk_accounts;
--drop table dbarnett.v220_pivot_activity_data;
select non_premium_pay_engagement
,non_premium_pay_duration
,case when round((seconds_viewed_Sky_Sports_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)=0 then 'a) No Sports Viewed'
when round((seconds_viewed_Sky_Sports_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<60 then 'b) Under 60 min per Week'
when round((seconds_viewed_Sky_Sports_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<180 then 'c) 60-179 min per Week'
when round((seconds_viewed_Sky_Sports_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<360 then 'd) 180-359 min per Week'
else 'e) 6+ Hours per week' end 

 as minutes_sports_per_week
,case when tv_package_group<>'e) Other' then tv_package_group when 

 a.entertainment_extra_flag=1 then 'e) Entertainment Extra with No Premiums' 
when a.entertainment_extra_flag=0
then 'f) Entertainment with No Premiums' else 'g) Other' end as package_type
,tenure
,isba_tv_region
,CASE hh_composition      when   '00' then 	'a) Family'
when '01'	then 'a) Family'
when '02'	then 'a) Family'
when '03'	then 'a) Family'
when '04'	then 'b) Single'
when '05'	then 'b) Single'
when '06'	then 'c) Homesharer'
when '07'	then 'c) Homesharer'
when '08'	then 'c) Homesharer'
when '09'	then 'a) Family'
when '10'	then 'a) Family'
when '11'	then 'c) Homesharer'
when 'U' 	then 'd) Unclassified'
else 'd) Unclassified' end as household_composition
,case when date_of_last_downgrade>='2013-03-12' then 1 else 0 end as downgrade_in_last_06M      
,case when all_downgrades>=5 then 'a) 5+ downgrades ever'
      when all_downgrades>=2 then 'b) 2-4 downgrades ever'
      when all_downgrades>0 then 'c) 1 downgrade ever' else 'd) Never Downgraded' end as downgrade_ever

,case when sports_downgrades>=5 then 'a) 5+ sports downgrades ever'
      when sports_downgrades>=2 then 'b) 2-4 sports downgrades ever'
      when sports_downgrades>0 then 'c) 1 sports downgrades ever' else 'd) Never Downgraded Sports' end as downgrade_ever_sports_channels

,case when all_upgrades>=5 then 'a) 5+ upgrades ever'
      when all_upgrades>=2 then 'b) 2-4 upgrades ever'
      when all_upgrades>0 then 'c) 1 upgrade ever' else 'd) Never upgraded' end as upgrade_ever

,case when sports_upgrades>=5 then 'a) 5+ sports upgrades ever'
      when sports_upgrades>=2 then 'b) 2-4 sports upgrades ever'
      when sports_upgrades>0 then 'c) 1 sports upgrade ever' else 'd) Never upgraded Sports' end as upgrade_ever_sports_channels
,case when cable_area='Y' then 1 else 0 end as cable_area_hh
,value_segment
,affluence_septile
,box_type_group
,case when bb_type in ('1) Unlimited (New)','2) Unlimited (Old)','3) Everyday','4) Everyday Lite','5) Connect') then 1 else 0 end as has_bb
,case when talk_product is not null then 1 else 0 end as has_talk
,case when has_bb=1 and has_talk =1 then 'a) TV, BB and Talk'
      when has_bb=1 and has_talk =0 then 'b) TV and BB'
      when has_bb=0 and has_talk =1 then 'c) TV and Talk' else 'd) TV Only' end as tv_bb_talk
,case   when last_12m_bill_paid<200 then 'a) Under £200'
        when last_12m_bill_paid<300 then 'b) £200-£299'
        when last_12m_bill_paid<400 then 'c) £300-£399'
        when last_12m_bill_paid<500 then 'd) £400-£499'
        when last_12m_bill_paid<600 then 'e) £500-£599'
        when last_12m_bill_paid<700 then 'f) £600-£699'
        when last_12m_bill_paid<800 then 'g) £700-£799' else 'h) £800+' end as last_12mths_bill_amt
,sum(a.weight_value) as weighted_accounts
into dbarnett.v220_pivot_activity_data
from v200_zero_mix_full_account_list as a
left outer join  v220_zero_mix_active_uk_accounts as b
on a.account_number = b.account_number
where a.vespa_zero_mix_panel_account=1 
group by non_premium_pay_engagement
,non_premium_pay_duration
,minutes_sports_per_week
,package_type
,tenure
,isba_tv_region
,household_composition
,downgrade_in_last_06M      
,downgrade_ever
,downgrade_ever_sports_channels
,upgrade_ever
,upgrade_ever_sports_channels
,cable_area_hh
,value_segment
,affluence_septile
,box_type_group
,has_bb
,has_talk
,tv_bb_talk
,last_12mths_bill_amt
;

commit;

--select household_composition ,count(*) from dbarnett.v220_pivot_activity_data group by household_composition


grant all on dbarnett.v220_pivot_activity_data to public;
commit;
