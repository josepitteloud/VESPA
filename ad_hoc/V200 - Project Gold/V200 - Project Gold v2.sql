

---Part A - All UK Households
----Get List of All Households----

--drop table #uk_households;
select cb_key_household
,min(cb_key_family) as family_key
,cb_address_postcode
,cb_address_postcode_area
--,cb_address_postcode_district
,cb_address_postcode_sector
into #uk_households
from sk_prod.experian_consumerview
where cb_address_dps is not null and cb_address_status = '1'
group by cb_key_household
,cb_address_postcode
,cb_address_postcode_area
--,cb_address_postcode_district
,cb_address_postcode_sector
;

commit;
create hg index idx1 on #uk_households(cb_key_household);
commit;

---PART B - All Account Movements from all accounts

--drop table #all_accounts_with_sports_prem_details_added;
select csh.account_number
,effective_from_dt
,effective_to_dt
,cel.prem_sports as sports_premiums_new
,cel_old.prem_sports as sports_premiums_old
,ent_cat_prod_changed
,status_code
into            #all_accounts_with_sports_prem_details_added
FROM            sk_prod.cust_subs_hist csh
LEFT OUTER JOIN sk_prod.cust_entitlement_lookup as cel on csh.current_short_description = cel.short_description
LEFT OUTER JOIN sk_prod.cust_entitlement_lookup as cel_old on csh.previous_short_description = cel_old.short_description
WHERE          csh.subscription_sub_type ='DTV Primary Viewing'
                and csh.subscription_type = 'DTV PACKAGE'
                and status_code in ('AC','AB','PC')
--and ent_cat_prod_changed='Y'
;

commit;
create  hg index idx1 on #all_accounts_with_sports_prem_details_added (account_number);

commit;
--drop table #account_summary;
select account_number
,sum(case when sports_premiums_new >0  and effective_to_dt = '9999-09-09' then datediff(day,effective_from_dt,cast('2013-08-29' as date))
            when sports_premiums_new >0  then datediff(day,effective_from_dt,effective_to_dt)
 else 0 end) as days_with_sport
,sum(case when sports_premiums_new >0 then 0  
            when effective_to_dt = '9999-09-09'  then datediff(day,effective_from_dt,cast('2013-08-29' as date))            
else datediff(day,effective_from_dt,effective_to_dt) end) as days_without_sport
,sum( case when ent_cat_prod_changed='Y' and sports_premiums_new<sports_premiums_old then 1 else 0 end) as sports_downgrades
,sum( case when ent_cat_prod_changed='Y' and sports_premiums_new>sports_premiums_old then 1 else 0 end) as sports_upgrades
into #account_summary
from #all_accounts_with_sports_prem_details_added
group by account_number
;


commit;
create hg index idx1 on #account_summary(account_number);
commit;


--drop table #all_accounts; drop table #one_record_per_hh_key; drop table #uk_view;
select csh.account_number
,cel.prem_sports as sports_premiums_new
,cel.prem_movies as movies_premiums_new
,status_code
,case when status_code in ('AC','AB','PC') then 1 else 0 end as active_account
,effective_from_dt as latest_account_change_date
,cb_key_household
,rank() over  (partition by cb_key_household order by active_account desc,effective_from_dt desc , account_number desc) as rank_household
into            #all_accounts

FROM            sk_prod.cust_subs_hist csh
LEFT OUTER JOIN sk_prod.cust_entitlement_lookup as cel on csh.current_short_description = cel.short_description
WHERE          csh.subscription_sub_type ='DTV Primary Viewing'
                and csh.subscription_type = 'DTV PACKAGE'
--                and status_code in ('AC','AB','PC')
                and effective_to_dt = '9999-09-09'
--and ent_cat_prod_changed='Y'
;

commit;
create hg index idx1 on #all_accounts(account_number);
commit;

---Create Summary of 1 record per Household Key--
select cb_key_household
,max(case when rank_household=1 then account_number else null end) latest_sky_household_account_number
,max(case when rank_household=1 and status_code in ('AC','AB','PC') then 1 else 0 end) active_sky_household 
,max(case when rank_household=1 and status_code in ('PO') then 1 else 0 end) cuscan_sky_household 
,max(case when rank_household=1 and status_code in ('SC') then 1 else 0 end) syscan_sky_household 
,sum(case when status_code in ('AC','AB','PC') then 1 else 0 end) active_sky_accounts_at_hh 
,sum(case when status_code in ('PO') then 1 else 0 end) cuscan_sky_accounts_at_hh 
,sum(case when status_code in ('SC') then 1 else 0 end) syscan_sky_accounts_at_hh 
into #one_record_per_hh_key
from  #all_accounts
--where rank_household=1
group by cb_key_household
;

/*
select syscan_sky_accounts_at_hh
,syscan_sky_household
,count(*)
from #one_record_per_hh_key
group by syscan_sky_accounts_at_hh
,syscan_sky_household
order by syscan_sky_accounts_at_hh
,syscan_sky_household
*/

commit;
create hg index idx1 on #one_record_per_hh_key(cb_key_household);
commit;

---Add on Current Product/Package Holdings---


--Create Summary View
--drop table #uk_view;
select a.cb_key_household
,case when b.active_sky_household =1 then 1 else 0 end as currently_active_hh

,case when b.active_sky_accounts_at_hh =1 then 1 else 0 end as total_active_sky_accounts_at_hh

,case when b.cuscan_sky_household =1 then 1 else 0 end as cuscan_accounts_at_hh
,case when b.syscan_sky_household =1 then 1 else 0 end as syscan_accounts_at_hh
,case when b.cuscan_sky_accounts_at_hh =1 then b.cuscan_sky_accounts_at_hh else 0 end as total_cuscan_accounts_at_hh
,case when b.syscan_sky_accounts_at_hh >0 then b.syscan_sky_accounts_at_hh else 0 end as total_syscan_accounts_at_hh
,latest_sky_household_account_number
,c.days_with_sport
,c.days_without_sport
,c.sports_downgrades
,c.sports_upgrades
,case when d.acct_analogue_account_number<>'?' then 1 else 0 end as analogue_customer
,d.PROD_LATEST_ENTITLEMENT_PREM_SPORTS
,d.PROD_LATEST_ENTITLEMENT_PREM_MOVIES
,d.PROD_LATEST_ENTITLEMENT_NO_OF_PAY_UNITS
into #uk_view
from #uk_households as a
left outer join #one_record_per_hh_key as b
on a.cb_key_household=b.cb_key_household
left outer join #account_summary as c
on b.latest_sky_household_account_number = c.account_number
left outer join sk_prod.cust_single_account_view as d
on b.latest_sky_household_account_number = d.account_number
;
commit;
--select top 100 * from sk_prod.ilu


/*
select total_syscan_accounts_at_hh
,syscan_accounts_at_hh
,count(*)
from #uk_view
group by total_syscan_accounts_at_hh
,syscan_accounts_at_hh
order by total_syscan_accounts_at_hh
,syscan_accounts_at_hh
*/

/*
select cuscan_accounts_at_hh
,syscan_accounts_at_hh
,count(*)
from #uk_view
group by cuscan_accounts_at_hh
,syscan_accounts_at_hh
order by cuscan_accounts_at_hh
,syscan_accounts_at_hh
*/

select top 500 * from #uk_view;
commit;

select count(*) as UK_Household , sum(currently_active_hh) as active_uk_hh
, sum(case when currently_active_hh=1 and analogue_customer=1 then 1 else 0 end ) as active_uk_hh_analogue_customer

, sum(case when currently_active_hh=1 and total_syscan_accounts_at_hh>0 then 1 else 0 end) as active_now_syscan_hh_ever
, sum(case when currently_active_hh=1 and total_cuscan_accounts_at_hh>0 then 1 else 0 end) as active_now_cuscan_hh_ever

, sum(case when currently_active_hh=0 and syscan_accounts_at_hh>0 then 1 else 0 end) as inactive_hh_previous_account_syscan
, sum(case when currently_active_hh=0 and cuscan_accounts_at_hh>0 then 1 else 0 end) as inactive_hh_previous_account_cuscan
from #uk_view;
commit;

---Lapsed Household Analysis--










commit;

/*


select top 5000 * from #account_summary

select  * from #account_summary where account_number='620041578563' 
select  * from #all_accounts_with_sports_prem_details_added where account_number='620041578563' 

select sum(sports_upgrades) from #account_summary

---Calcualte days with/without sport



select * from           sk_prod.cust_subs_hist where account_number = '620041578563' 
and subscription_sub_type ='DTV Primary Viewing'
                and subscription_type = 'DTV PACKAGE'



select csh.account_number
,effective_from_dt
,effective_to_dt
,cel.prem_sports as sports_premiums_new
,cel_old.prem_sports as sports_premiums_old
,ent_cat_prod_changed
,current_short_description
,previous_short_description
,csh.*
FROM            sk_prod.cust_subs_hist csh
LEFT OUTER JOIN sk_prod.cust_entitlement_lookup as cel on csh.current_short_description = cel.short_description
LEFT OUTER JOIN sk_prod.cust_entitlement_lookup as cel_old on csh.previous_short_description = cel_old.short_description
WHERE         csh.account_number = '620041578563' and csh.subscription_sub_type ='DTV Primary Viewing'
                and csh.subscription_type = 'DTV PACKAGE'
                and status_code in ('AC')
--and ent_cat_prod_changed='Y'
order by effective_from_dt
;

select PROD_LATEST_ENTITLEMENT_PREM_SPORTS
,PROD_LATEST_ENTITLEMENT_SPORTS_1
,PROD_LATEST_ENTITLEMENT_SPORTS_2
 from           sk_prod.cust_single_account_view where account_number = '620041578563' 




commit;
create hg index idx1 on #account_summary(account_number);
commit;


commit;
create hg index idx1 on #sky_household_info(cb_key_household);
commit;


commit;
create hg index idx1 on #sky_household_info(cb_key_household);
commit;
,case when acct_analogue_account_number<>'?' then 1 else 0 end as analogue_customer

*/