

---Part A - All UK Households
----Get List of All Households----

--drop table #uk_households;
select cb_key_household
,min(cb_key_family) as family_key
,cb_address_postcode
,cb_address_postcode_area
,cb_address_postcode_district
,cb_address_postcode_sector
into #uk_households
from sk_prod.experian_consumerview
where cb_address_dps is not null and cb_address_status = '1'
group by cb_key_household
,cb_address_postcode
,cb_address_postcode_area
,cb_address_postcode_district
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
,max(case when  effective_to_dt = '9999-09-09' then 
into #account_summary
from #all_accounts_with_sports_prem_details_added
group by account_number
;


commit;
create hg index idx1 on #account_summary(account_number);
commit;


--drop table #all_currently_active_accounts;
select csh.account_number
,cel.prem_sports as sports_premiums_new
,cel.prem_movies as movies_premiums_new
,status_code
,case when status_code in ('AC','AB','PC') then 1 else 0 end as active_account
,effective_from_dt as latest_account_change_date
,rank() over  (partition by cb_key_household desc order by active_account,effective_from_dt) as rank_pack
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





--Create Summary of UK Households from Sky Tables--
select cb_key_household
,max(case when acct_status_code in ('AC','AB','PC') then 1 else 0 end) active_sky_household
,max(case when acct_status_code in ('AC','AB','PC') then account_number else null end) active_sky_household_ac_number

,sum(case when acct_status_code in ('PO') then 1 else 0 end) cuscan_sky_household
,sum(case when acct_status_code in ('SC') then 1 else 0 end) syscan_sky_household
into #sky_household_info
from sk_prod.cust_single_account_view
where cb_address_dps is not null and cb_address_status = '1'
group by cb_key_household
;
commit;
create hg index idx1 on #sky_household_info(cb_key_household);
commit;


--Create Summary View

select a.cb_key_household
,case when b.active_sky_household =1 then 1 else 0 end as currently_active_hh

,case when b.cuscan_sky_household =1 then 1 else 0 end as cuscan_accounts_at_hh
,case when b.syscan_sky_household =1 then 1 else 0 end as syscan_accounts_at_hh

,c.days_with_sport
,c.days_without_sport
,c.sports_downgrades
,c.sports_upgrades
into #uk_view
from #uk_households as a
left outer join #sky_household_info as b
on a.cb_key_household=b.cb_key_household
left outer join #account_summary as c
on b.active_sky_household_ac_number = c.account_number
;

select count(*) as UK_Household , sum(currently_active_hh) as active_uk_hh
, sum(case when currently_active_hh=0 and syscan_accounts_at_hh>0 then 1 else 0 end) as syscan_hh
, sum(case when currently_active_hh=0 and syscan_accounts_at_hh=0 and cuscan_accounts_at_hh>0 then 1 else 0 end) as cuscan_hh
from #uk_view


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

select * from           sk_prod.cust_single_account_view where account_number = '620041578563' 




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