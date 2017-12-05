


---Ability to View-----

select          account_number
                ,count(distinct activity_dt) as distinct_days_used
into            #skygo_usage
from            SK_PROD.SKY_PLAYER_USAGE_DETAIL
where           activity_dt >=  cast(@analysis_date as date)-182
and             activity_dt <=@analysis_date
group by        account_number
order by        account_number
;

select * from SK_PROD.SKY_PLAYER_USAGE_DETAIL
where account_number='620041578563' 
order by activity_dt




select * from SK_PROD.SKY_PLAYER_REGISTRANT
where account_number='620041578563' 
commit;

select site_name
,site_url
,count(*)
from SK_PROD.SKY_PLAYER_USAGE_DETAIL
where account_number is not null
and video_sk <> -1
and x_usage_type = 'Live Viewing'
and activity_dt>='2014-01-01'
--and left(site_name,2) not in ('SM')
group by site_name
,site_url
;

commit;

subscription_sub_type,subscription_type,count()
'3DTV','A-LA-CARTE',594826
'3DTV','DTV PACKAGE',1
'3DTV','ENHANCED',6
'?','DTV PACKAGE',39
'?','ENHANCED',3
'Broadband Accessory','ENHANCED',1
'Broadband DSL Line','BROADBAND',4701404
'Broadband DSL Line','ENHANCED',2
'CLOUDWIFI','MCAFEE',4489579
'DTV Artsworld','A-LA-CARTE',1
'DTV Chelsea TV','A-LA-CARTE',30893
'DTV Current TV','A-LA-CARTE',12
'DTV Disney Channel','A-LA-CARTE',1791
'DTV Disney Channel','DTV PACKAGE',1
'DTV Extra Subscription','DTV PACKAGE',2962341
'DTV Extra Subscription','ENHANCED',7
'DTV FilmFour','A-LA-CARTE',1385
'DTV FilmFour','DTV PACKAGE',13
'DTV HD','DTV PACKAGE',4
'DTV HD','ENHANCED',4948842
'DTV MUTV','A-LA-CARTE',72763
'DTV MUTV','DTV PACKAGE',4
'DTV Music Choice Extra','A-LA-CARTE',33
'DTV PV Premium','DTV PACKAGE',3
'DTV Primary Viewing','DTV PACKAGE',10143986
'DTV Primary Viewing','ENHANCED',2
'DTV Season Ticket','A-LA-CARTE',65999
'DTV Sky Sports Xtra','A-LA-CARTE',217
'DTV Sky+','A-LA-CARTE',1
'DTV Sky+','DTV PACKAGE',6
'DTV Sky+','ENHANCED',11072852
'DTV_HD_EXTN','A-LA-CARTE',9580
'ESPN','A-LA-CARTE',2018
'ESPN','DTV PACKAGE',1
'ESPN','ENHANCED',3
'FREESAT','DTV PACKAGE',9344
'FREESAT','ENHANCED',30905
'HD Pack','DTV PACKAGE',4
'HD Pack','ENHANCED',228892
'MCAFEE','MCAFEE',189806
'MGM','A-LA-CARTE',64
'PDL subscriptions','ENHANCED',5056623
'PDL wireless connection product','ENHANCED',27
'PayAsYouGoOut','A-LA-CARTE',106
'Pub_Channel','A-LA-CARTE',478
'SKY TALK LINE RENTAL','A-LA-CARTE',16
'SKY TALK LR FEATURE','BROADBAND',2
'SKY TALK SELECT','SKY TALK',12456
'SKY TALK USAGE','SKY TALK',2
'SKYINSIDER','A-LA-CARTE',21156
'SOLUS Broadband Surcharge','MCAFEE',233611
'SPPP','?',1134
'STANDALONESURCHARGE','MCAFEE',664559
'SW_Sample','A-LA-CARTE',56
'Sky Go Extra','A-LA-CARTE',456432
'Sky Go Extra','ENHANCED',2








create variable @analysis_date date;
set @analysis_date='2013-10-31';


select account_number
,count(*) as active_stbs
into #active_stb_by_account
        from sk_prod.cust_subs_hist
WHERE 
             effective_from_dt <= @analysis_date
and             effective_to_dt > @analysis_date
and status_code in ('AC','AB','PC')
and  subscription_sub_type in ('DTV Primary Viewing','DTV Extra Subscription')
 and subscription_type='DTV PACKAGE'  

group by account_number;

select active_stbs
,count(*)
from #active_stb_by_account
group by active_stbs
order by active_stbs
;
commit;


----Number of Active STB as at 21st oct 2013---



---Number of People in HH
--drop table  #experian_hh_summary_people;
select          cb_key_household
                ,max(case when h_number_of_children_in_household_2011='U' then 0 else cast(h_number_of_children_in_household_2011 as integer) end )    as num_children_in_hh
                ,max(h_number_of_adults+0)                      as num_adults_in_hh
into            #experian_hh_summary_people
FROM            sk_prod.experian_consumerview
where           cb_address_status = '1' 
and             cb_address_dps IS NOT NULL 
and             cb_address_organisation IS NULL
group by        cb_key_household
;
commit;

exec sp_create_tmp_table_idx '#experian_hh_summary_people', 'cb_key_household';
commit;


alter table dbarnett.v250_Account_profiling add number_children_in_hh integer;
alter table dbarnett.v250_Account_profiling add number_adults_in_hh integer;
alter table dbarnett.v250_Account_profiling add number_people_in_hh integer;

update dbarnett.v250_Account_profiling
set active_stb_subs=case when b.active_stbs is null then 0 else b.active_stbs  end
from dbarnett.v250_Account_profiling as a
left outer join  #active_stb_by_account as b
on a.account_number = b.account_number
;

commit;



select num_children_in_hh,num_adults_in_hh
,count(*) as households
from #experian_hh_summary_people
group by num_children_in_hh,num_adults_in_hh
order by num_children_in_hh,num_adults_in_hh





select (num_children_in_hh+0)+(num_adults_in_hh +0) as num_people_in_hh
,count(*) as households
from #experian_hh_summary_people
group by num_people_in_hh
order by num_people_in_hh
;
commit;

select *  FROM            sk_prod.experian_consumerview
where cb_address_postcode='HP23 5PS' and cb_address_buildingno='6'


select broadcast_channel
,count(*) as records

from SK_PROD.SKY_PLAYER_USAGE_DETAIL as a
where account_number is not null
and video_sk <> -1
and x_usage_type = 'Live Viewing'
and left(a.site_name,2) not in ('SM')
--and broadcast_channel in ('StreamSkySports1','StreamSkySports2', 'StreamSkySports3','StreamSkySports4' )
group by broadcast_channel
order by records desc
;

commit;

---Sky Sports Login Details---

select a.account_number
,a.broadcast_channel
,b.LOGIN_DT
,FIRST_LOGIN_DT
,LAST_LOGIN_DT

 from sk_prod.SKY_PLAYER_USAGE_DETAIL as a
left outer join sk_prod.SKY_PLAYER_LOGIN_DETAIL as b
on a.SAMPROFILEID=b.SAMPROFILEID
left outer join sk_prod.SKY_PLAYER_REGISTRANT as c
on b.SAMPROFILEID=c.SAM_PROFILE_ID
where a.account_number = '620041578563'
and activity_dt>='2014-01-01'
order by activity_dt


select * from sk_prod.SKY_PLAYER_LOGIN_DETAIL as b
left outer join sk_prod.SKY_PLAYER_REGISTRANT as c
on b.SAMPROFILEID=c.SAM_PROFILE_ID
where account_number = '620041578563'
and LOGIN_DT>='2014-01-01'
order by LOGIN_DT


commit;


select a.*
from sk_prod.SKY_PLAYER_USAGE_DETAIL as a
where a.account_number = '620041578563'
and activity_dt>='2014-01-01'
order by activity_dt, cb_row_id



---Sky Go Login by Time---

---First Login Date by time
--drop table #login_summary_by_day;
select account_number
,cast (LOGIN_DT as date) as login_date
,min(LOGIN_DT) as login_time
,count(*) as logins
into #login_summary_by_day
from sk_prod.SKY_PLAYER_LOGIN_DETAIL as a
left outer join sk_prod.SKY_PLAYER_REGISTRANT as b
on a.SAMPROFILEID=b.SAM_PROFILE_ID
where left(a.site_name,2) not in ('SM') and cast (LOGIN_DT as date)>='2014-01-01'
and b.account_number = '620041578563'
group by account_number
,login_date
order by login_date
;

select * from #login_summary_by_day
commit;

---Apply all streams content to first login time of day










select logins
,count(*)
from #login_summary
group by logins
order by logins

select * from #login_summary where logins= 152505

select *  from sk_prod.SKY_PLAYER_LOGIN_DETAIL as a
left outer join sk_prod.SKY_PLAYER_REGISTRANT as b
on a.SAMPROFILEID=b.SAM_PROFILE_ID
 where cast(login_dt as date) ='2014-01-19' and account_number is null



---Stream Info - First Stream by Day
select cluster_number
,count(*) from skoczej.v250_cluster_numbers 
group by cluster_number
order by cluster_number

commit;


select * from sk_prod.CONSUMERVIEW_POSTCODE  where cb_address_postcode = 'HP23 5PS'

select activity_dt ,count(*)
from SK_PROD.SKY_PLAYER_USAGE_DETAIL group by activity_dt
order by activity_dt

commit;

commit;
grant all on dbarnett.v250_rights_broadcast_overall to public;
grant all on dbarnett.v250_rights_broadcast_by_live_status  to public; commit;


select account_number
,max(case when subscription_sub_type='DTV Chelsea TV' and status_code in ('AC','AB','PC') and effective_to_dt = '9999-09-09' then 1 else 0 end) as active_chelsea_TV_current
,max(case when subscription_sub_type='DTV Chelsea TV' and status_code in ('AC','AB','PC') then 1 else 0 end) as active_chelsea_TV_ever
,max(case when subscription_sub_type='DTV MUTV' and status_code in ('AC','AB','PC') and effective_to_dt = '9999-09-09' then 1 else 0 end) as active_MUTV_current
,max(case when subscription_sub_type='DTV MUTV' and status_code in ('AC','AB','PC') then 1 else 0 end) as active_MUTV_ever
into #chelsea_mutv
from sk_prod.cust_subs_hist where subscription_sub_type in ('DTV Chelsea TV','DTV MUTV')
group by account_number
;

select  sum(active_chelsea_TV_current)
,sum(active_chelsea_TV_ever)
,sum(active_MUTV_current)
,sum(active_MUTV_ever)
from #chelsea_mutv
