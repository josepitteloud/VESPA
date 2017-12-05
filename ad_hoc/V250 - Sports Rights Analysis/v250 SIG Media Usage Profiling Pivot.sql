/*-----------------------------------------------------------------------------------------------------------------
        Project:V250 - Sports right Analysis Profiling
        SIG Media Usage Profiling Pivot
        
        Analyst: Dan Barnett
        SK Prod: 5

        Create table with household details around media usage for targeting purposes

*/------------------------------------------------------------------------------------------------------------------


---Take some of the variables from the main profiling dataset---

select account_number
,account_weight
,BB_type
,hdtv
,multiroom
,skyplus
,subscription_3d
,DTV_Package
,social_grade
,talk
,line_rental
,pc_clientele_01_me_and_my_pint   
,pc_clientele_02_big_night_out   
,pc_clientele_03_business_and_pleasure   
,pc_clientele_04_family_fun   
,pc_clientele_05_daytime_local   
,pc_clientele_06_pub_play   
,pc_clientele_07_evening_local   
,pc_clientele_08_out_for_dinner   
,pc_clientele_09_student_drinks   
,pc_clientele_10_out_on_the_town   
,pc_clientele_11_leisurely_lunch   
,pc_clientele_12_weekend_lunch   
,pc_clientele_13_catch_up   
,pc_clientele_14_sociable_suburbs   
,mosaic_group
,True_Touch_Type
,num_bedrooms
,residence_type
,household_ownership_type
,affluence_septile
,hh_income_band
,cb_key_household
into dbarnett.v250_experian_model_profiling
from dbarnett.v250_Account_profiling
;

commit;

---Create Lookup Table to match Consumerview to Person Percentile data---
select pc_mosaic_uk_type,p_pixel_v2
,cb_key_household
,cb_key_individual
into #consumerview
FROM            sk_prod.experian_consumerview
where           cb_address_status = '1' 
and             cb_address_dps IS NOT NULL 
and             cb_address_organisation IS NULL
;



select ppixel2011
,mosaic_uk_2009_type
,read_the_daily_mail_the_scottish_daily_mail_regularly_percentile
,read_the_daily_telegraph_regularly_percentile
,read_the_express_regularly_percentile
,read_the_financial_times_regularly_percentile
,read_the_guardian_regularly_percentile
,read_the_independent_regularly_percentile
,read_the_mirror_daily_record_regularly_percentile
,read_the_sun_regularly_percentile
,read_the_the_times_regularly_percentile
,read_trade_and_professional_magazines_percentile
,read_women_s_interests_magazines_percentile
,regularly_visit_bbc_website_percentile
,regularly_visit_facebook_percentile
,regularly_visit_linkedin_percentile
,regularly_visit_msn_percentile
,regularly_visit_myspace_percentile
,regularly_visit_spotify_percentile
,regularly_visit_the_guardian_website_percentile
,regularly_visit_the_telegraph_website_percentile
,regularly_visit_twitter_percentile
,regularly_visit_youtube_percentile
into #person_propensity
from sk_prod.PERSON_PROPENSITIES_GRID_NEW
;

select pc_mosaic_uk_type,p_pixel_v2
,cb_key_household
,min(b.read_the_daily_mail_the_scottish_daily_mail_regularly_percentile) as hh_read_the_the_times_regularly_percentile
,min(b.read_the_daily_telegraph_regularly_percentile) as hh_read_the_daily_telegraph_regularly_percentile
,min(b.read_the_express_regularly_percentile) as hh_read_the_express_regularly_percentile
,min(b.read_the_financial_times_regularly_percentile) as hh_read_the_financial_times_regularly_percentile
,min(b.read_the_guardian_regularly_percentile) as hh_read_the_guardian_regularly_percentile
,min(b.read_the_independent_regularly_percentile) as hh_read_the_independent_regularly_percentile
,min(b.read_the_mirror_daily_record_regularly_percentile) as hh_read_the_mirror_daily_record_regularly_percentile
,min(b.read_the_sun_regularly_percentile) as hh_read_the_sun_regularly_percentile
,min(b.read_trade_and_professional_magazines_percentile) as hh_read_trade_and_professional_magazines_percentile
,min(b.read_women_s_interests_magazines_percentile) as hh_read_women_s_interests_magazines_percentile
,min(b.regularly_visit_bbc_website_percentile) as hh_regularly_visit_bbc_website_percentile
,min(b.regularly_visit_facebook_percentile) as hh_regularly_visit_facebook_percentile
,min(b.regularly_visit_linkedin_percentile) as hh_regularly_visit_linkedin_percentile
,min(b.regularly_visit_msn_percentile) as hh_regularly_visit_msn_percentile
,min(b.regularly_visit_myspace_percentile) as hh_regularly_visit_myspace_percentile

,min(b.regularly_visit_spotify_percentile) as hh_regularly_visit_spotify_percentile
,min(b.regularly_visit_the_guardian_website_percentile) as hh_regularly_visit_the_guardian_website_percentile
,min(b.regularly_visit_the_telegraph_website_percentile) as hh_regularly_visit_the_telegraph_website_percentile
,min(b.regularly_visit_twitter_percentile) as hh_regularly_visit_twitter_percentile

,min(b.regularly_visit_youtube_percentile) as hh_regularly_visit_youtube_percentile

into #join_data
from  #consumerview as a
left outer join #person_propensity as b
on a.p_pixel_v2=b.ppixel2011
and a.pc_mosaic_uk_type=b.mosaic_uk_2009_type
group by pc_mosaic_uk_type,p_pixel_v2
,cb_key_household
;

----Match Percentile Data to Account Table-----
alter table dbarnett.v250_experian_model_profiling add hh_read_the_the_times_regularly_percentile varchar(2);
alter table dbarnett.v250_experian_model_profiling add hh_read_the_daily_telegraph_regularly_percentile varchar(2);
alter table dbarnett.v250_experian_model_profiling add hh_read_the_express_regularly_percentile varchar(2);
alter table dbarnett.v250_experian_model_profiling add hh_read_the_financial_times_regularly_percentile varchar(2);
alter table dbarnett.v250_experian_model_profiling add hh_read_the_guardian_regularly_percentile varchar(2);
alter table dbarnett.v250_experian_model_profiling add hh_read_the_independent_regularly_percentile varchar(2);
alter table dbarnett.v250_experian_model_profiling add hh_read_the_mirror_daily_record_regularly_percentile varchar(2);

alter table dbarnett.v250_experian_model_profiling add hh_read_the_sun_regularly_percentile varchar(2);
alter table dbarnett.v250_experian_model_profiling add hh_read_trade_and_professional_magazines_percentile varchar(2);
alter table dbarnett.v250_experian_model_profiling add hh_read_women_s_interests_magazines_percentile varchar(2);
alter table dbarnett.v250_experian_model_profiling add hh_regularly_visit_bbc_website_percentile varchar(2);
alter table dbarnett.v250_experian_model_profiling add hh_regularly_visit_facebook_percentile varchar(2);
alter table dbarnett.v250_experian_model_profiling add hh_regularly_visit_linkedin_percentile varchar(2);
alter table dbarnett.v250_experian_model_profiling add hh_regularly_visit_msn_percentile varchar(2);

alter table dbarnett.v250_experian_model_profiling add hh_regularly_visit_myspace_percentile varchar(2);
alter table dbarnett.v250_experian_model_profiling add hh_regularly_visit_spotify_percentile varchar(2);
alter table dbarnett.v250_experian_model_profiling add hh_regularly_visit_the_guardian_website_percentile varchar(2);
alter table dbarnett.v250_experian_model_profiling add hh_regularly_visit_the_telegraph_website_percentile varchar(2);
alter table dbarnett.v250_experian_model_profiling add hh_regularly_visit_twitter_percentile varchar(2);
alter table dbarnett.v250_experian_model_profiling add hh_regularly_visit_youtube_percentile varchar(2);


update dbarnett.v250_experian_model_profiling
set hh_read_the_the_times_regularly_percentile = case when b.hh_read_the_the_times_regularly_percentile  is null then 'U' else b.hh_read_the_the_times_regularly_percentile  end
,hh_read_the_daily_telegraph_regularly_percentile = case when b.hh_read_the_daily_telegraph_regularly_percentile  is null then 'U' else b.hh_read_the_daily_telegraph_regularly_percentile  end
,hh_read_the_express_regularly_percentile = case when b.hh_read_the_express_regularly_percentile  is null then 'U' else b.hh_read_the_express_regularly_percentile  end
,hh_read_the_financial_times_regularly_percentile = case when b.hh_read_the_financial_times_regularly_percentile  is null then 'U' else b.hh_read_the_financial_times_regularly_percentile  end
,hh_read_the_guardian_regularly_percentile = case when b.hh_read_the_guardian_regularly_percentile  is null then 'U' else b.hh_read_the_guardian_regularly_percentile  end
,hh_read_the_independent_regularly_percentile = case when b.hh_read_the_independent_regularly_percentile  is null then 'U' else b.hh_read_the_independent_regularly_percentile  end
,hh_read_the_mirror_daily_record_regularly_percentile = case when b.hh_read_the_mirror_daily_record_regularly_percentile  is null then 'U' else b.hh_read_the_mirror_daily_record_regularly_percentile  end
,hh_read_the_sun_regularly_percentile = case when b.hh_read_the_sun_regularly_percentile  is null then 'U' else b.hh_read_the_sun_regularly_percentile  end
,hh_read_trade_and_professional_magazines_percentile = case when b.hh_read_trade_and_professional_magazines_percentile  is null then 'U' else b.hh_read_trade_and_professional_magazines_percentile  end
,hh_read_women_s_interests_magazines_percentile = case when b.hh_read_women_s_interests_magazines_percentile  is null then 'U' else b.hh_read_women_s_interests_magazines_percentile  end
,hh_regularly_visit_bbc_website_percentile = case when b.hh_regularly_visit_bbc_website_percentile  is null then 'U' else b.hh_regularly_visit_bbc_website_percentile  end
,hh_regularly_visit_facebook_percentile = case when b.hh_regularly_visit_facebook_percentile  is null then 'U' else b.hh_regularly_visit_facebook_percentile  end
,hh_regularly_visit_linkedin_percentile = case when b.hh_regularly_visit_linkedin_percentile  is null then 'U' else b.hh_regularly_visit_linkedin_percentile  end
,hh_regularly_visit_msn_percentile = case when b.hh_regularly_visit_msn_percentile  is null then 'U' else b.hh_regularly_visit_msn_percentile  end
,hh_regularly_visit_myspace_percentile = case when b.hh_regularly_visit_myspace_percentile  is null then 'U' else b.hh_regularly_visit_myspace_percentile  end
,hh_regularly_visit_spotify_percentile = case when b.hh_regularly_visit_spotify_percentile  is null then 'U' else b.hh_regularly_visit_spotify_percentile  end
,hh_regularly_visit_the_guardian_website_percentile = case when b.hh_regularly_visit_the_guardian_website_percentile  is null then 'U' else b.hh_regularly_visit_the_guardian_website_percentile  end
,hh_regularly_visit_the_telegraph_website_percentile = case when b.hh_regularly_visit_the_telegraph_website_percentile  is null then 'U' else b.hh_regularly_visit_the_telegraph_website_percentile  end
,hh_regularly_visit_twitter_percentile = case when b.hh_regularly_visit_twitter_percentile  is null then 'U' else b.hh_regularly_visit_twitter_percentile  end
,hh_regularly_visit_youtube_percentile = case when b.hh_regularly_visit_youtube_percentile  is null then 'U' else b.hh_regularly_visit_youtube_percentile  end
from dbarnett.v250_experian_model_profiling as a
left outer join #join_data as b
on a.cb_key_household=b.cb_key_household
;

commit;

--select hh_regularly_visit_youtube_percentile , sum(account_weight) from dbarnett.v250_experian_model_profiling group by hh_regularly_visit_youtube_percentile order by hh_regularly_visit_youtube_percentile
----Remove Account no and HH Key from Final Pivot (in final output code)---


---Make Pivot table public
grant all on dbarnett.v250_experian_model_profiling to public;
commit;

---Add back on Cluster name

alter table dbarnett.v250_experian_model_profiling add cluster_name varchar(100);
update dbarnett.v250_experian_model_profiling
set cluster_name=b.cluster_name
from dbarnett.v250_experian_model_profiling as a
left outer join dbarnett.v250_Account_profiling as b
on a.account_Number=b.account_number
;
commit;


----Add Deciles to Profile----

alter table dbarnett.v250_experian_model_profiling add hh_read_the_the_times_regularly_decile integer;
alter table dbarnett.v250_experian_model_profiling add hh_read_the_daily_telegraph_regularly_decile integer;
alter table dbarnett.v250_experian_model_profiling add hh_read_the_express_regularly_decile integer;
alter table dbarnett.v250_experian_model_profiling add hh_read_the_financial_times_regularly_decile integer;
alter table dbarnett.v250_experian_model_profiling add hh_read_the_guardian_regularly_decile integer;
alter table dbarnett.v250_experian_model_profiling add hh_read_the_independent_regularly_decile integer;
alter table dbarnett.v250_experian_model_profiling add hh_read_the_mirror_daily_record_regularly_decile integer;

alter table dbarnett.v250_experian_model_profiling add hh_read_the_sun_regularly_decile integer;
alter table dbarnett.v250_experian_model_profiling add hh_read_trade_and_professional_magazines_decile integer;
alter table dbarnett.v250_experian_model_profiling add hh_read_women_s_interests_magazines_decile integer;
alter table dbarnett.v250_experian_model_profiling add hh_regularly_visit_bbc_website_decile integer;
alter table dbarnett.v250_experian_model_profiling add hh_regularly_visit_facebook_decile integer;
alter table dbarnett.v250_experian_model_profiling add hh_regularly_visit_linkedin_decile integer;
alter table dbarnett.v250_experian_model_profiling add hh_regularly_visit_msn_decile integer;

alter table dbarnett.v250_experian_model_profiling add hh_regularly_visit_myspace_decile integer;
alter table dbarnett.v250_experian_model_profiling add hh_regularly_visit_spotify_decile integer;
alter table dbarnett.v250_experian_model_profiling add hh_regularly_visit_the_guardian_website_decile integer;
alter table dbarnett.v250_experian_model_profiling add hh_regularly_visit_the_telegraph_website_decile integer;
alter table dbarnett.v250_experian_model_profiling add hh_regularly_visit_twitter_decile integer;
alter table dbarnett.v250_experian_model_profiling add hh_regularly_visit_youtube_decile integer;

update dbarnett.v250_experian_model_profiling
set hh_read_the_the_times_regularly_decile = case when hh_read_the_the_times_regularly_percentile  ='U' then null  else ((cast( hh_read_the_the_times_regularly_percentile  as integer)/10)-10)*-1 end
,hh_read_the_daily_telegraph_regularly_decile = case when hh_read_the_daily_telegraph_regularly_percentile  ='U' then null  else ((cast( hh_read_the_daily_telegraph_regularly_percentile  as integer)/10)-10)*-1 end
,hh_read_the_express_regularly_decile = case when hh_read_the_express_regularly_percentile  ='U' then null  else ((cast( hh_read_the_express_regularly_percentile  as integer)/10)-10)*-1 end
,hh_read_the_financial_times_regularly_decile = case when hh_read_the_financial_times_regularly_percentile  ='U' then null  else ((cast( hh_read_the_financial_times_regularly_percentile  as integer)/10)-10)*-1 end
,hh_read_the_guardian_regularly_decile = case when hh_read_the_guardian_regularly_percentile  ='U' then null  else ((cast( hh_read_the_guardian_regularly_percentile  as integer)/10)-10)*-1 end
,hh_read_the_independent_regularly_decile = case when hh_read_the_independent_regularly_percentile  ='U' then null  else ((cast( hh_read_the_independent_regularly_percentile  as integer)/10)-10)*-1 end
,hh_read_the_mirror_daily_record_regularly_decile = case when hh_read_the_mirror_daily_record_regularly_percentile  ='U' then null  else ((cast( hh_read_the_mirror_daily_record_regularly_percentile  as integer)/10)-10)*-1 end
,hh_read_the_sun_regularly_decile = case when hh_read_the_sun_regularly_percentile  ='U' then null  else ((cast( hh_read_the_sun_regularly_percentile  as integer)/10)-10)*-1 end
,hh_read_trade_and_professional_magazines_decile = case when hh_read_trade_and_professional_magazines_percentile  ='U' then null  else ((cast( hh_read_trade_and_professional_magazines_percentile  as integer)/10)-10)*-1 end
,hh_read_women_s_interests_magazines_decile = case when hh_read_women_s_interests_magazines_percentile  ='U' then null  else ((cast( hh_read_women_s_interests_magazines_percentile  as integer)/10)-10)*-1 end
,hh_regularly_visit_bbc_website_decile = case when hh_regularly_visit_bbc_website_percentile  ='U' then null  else ((cast( hh_regularly_visit_bbc_website_percentile  as integer)/10)-10)*-1 end
,hh_regularly_visit_facebook_decile = case when hh_regularly_visit_facebook_percentile  ='U' then null  else ((cast( hh_regularly_visit_facebook_percentile  as integer)/10)-10)*-1 end
,hh_regularly_visit_linkedin_decile = case when hh_regularly_visit_linkedin_percentile  ='U' then null  else ((cast( hh_regularly_visit_linkedin_percentile  as integer)/10)-10)*-1 end
,hh_regularly_visit_msn_decile = case when hh_regularly_visit_msn_percentile  ='U' then null  else ((cast( hh_regularly_visit_msn_percentile  as integer)/10)-10)*-1 end
,hh_regularly_visit_myspace_decile = case when hh_regularly_visit_myspace_percentile  ='U' then null  else ((cast( hh_regularly_visit_myspace_percentile  as integer)/10)-10)*-1 end
,hh_regularly_visit_spotify_decile = case when hh_regularly_visit_spotify_percentile  ='U' then null  else ((cast( hh_regularly_visit_spotify_percentile  as integer)/10)-10)*-1 end
,hh_regularly_visit_the_guardian_website_decile = case when hh_regularly_visit_the_guardian_website_percentile  ='U' then null  else ((cast( hh_regularly_visit_the_guardian_website_percentile  as integer)/10)-10)*-1 end
,hh_regularly_visit_the_telegraph_website_decile = case when hh_regularly_visit_the_telegraph_website_percentile  ='U' then null  else ((cast( hh_regularly_visit_the_telegraph_website_percentile  as integer)/10)-10)*-1 end
,hh_regularly_visit_twitter_decile = case when hh_regularly_visit_twitter_percentile  ='U' then null  else ((cast( hh_regularly_visit_twitter_percentile  as integer)/10)-10)*-1 end
,hh_regularly_visit_youtube_decile = case when hh_regularly_visit_youtube_percentile  ='U' then null  else ((cast( hh_regularly_visit_youtube_percentile  as integer)/10)-10)*-1 end
from dbarnett.v250_experian_model_profiling as a
;
commit;

select top 10 * from dbarnett.v250_experian_model_profiling;


/*

select hh_read_the_the_times_regularly_percentile
,((cast(hh_read_the_the_times_regularly_percentile as integer)/10)-10)*-1 as reworked_decile
,count(*)
into #decile_test
from dbarnett.v250_experian_model_profiling
where hh_read_the_the_times_regularly_percentile<>'U'
group by hh_read_the_the_times_regularly_percentile
,reworked_decile

select * from #decile_test order by hh_read_the_the_times_regularly_percentile
*/

/*

select * from dbarnett.v250_experian_model_profiling;

commit;

select count(*) from sk_prod.HOUSEHOLD_PROPENSITIES_GRID_NEW
select top 100 * from sk_prod.EXPERIAN_LIFESTYLE

select ppixel2011
,mosaic_uk_2009_type
,read_the_guardian_regularly_percentile
,count(*) as records from sk_prod.PERSON_PROPENSITIES_GRID_NEW
group by ppixel2011
,mosaic_uk_2009_type
,read_the_guardian_regularly_percentile
order by ppixel2011
,mosaic_uk_2009_type
,read_the_guardian_regularly_percentile 


select mosaic_uk_2009_type
,read_the_guardian_regularly_percentile
,count(*) as records from sk_prod.PERSON_PROPENSITIES_GRID_NEW
group by mosaic_uk_2009_type
,read_the_guardian_regularly_percentile
order by mosaic_uk_2009_type
,read_the_guardian_regularly_percentile 





select ppixel2011
,mosaic_uk_2009_type, *  from sk_prod.experian_consumerview
where cb_address_postcode='HP23 5PS' and cb_address_buildingno='6'




select mosaic_uk_2009_type
,count(*) as records from sk_prod.PERSON_PROPENSITIES_GRID_NEW
group by mosaic_uk_2009_type
order by records desc


select ppixel2011
from sk_prod.PERSON_PROPENSITIES_GRID_NEW
where ppixel2011 in ('34349','34350') and mosaic_uk_2009_type='30'

select max(ppixel2011)
from sk_prod.PERSON_PROPENSITIES_GRID_NEW

------------------


--Get Match details from consumerview

select pc_mosaic_uk_type,p_pixel_v2
,cb_key_household
,cb_key_individual
into #consumerview
FROM            sk_prod.experian_consumerview
where           cb_address_status = '1' 
and             cb_address_dps IS NOT NULL 
and             cb_address_organisation IS NULL
;

select pc_mosaic_uk_type,p_pixel_v2
,cb_key_household
,max(b.read_the_the_times_regularly_percentile)
into #join_data
from  #consumerview as a
left outer join #person_propensity as b
on a.p_pixel_v2=b.ppixel2011
and a.pc_mosaic_uk_type=b.mosaic_uk_2009_type
group by pc_mosaic_uk_type,p_pixel_v2
,cb_key_household
;

commit;

select expression , count(*) from #join_data group by expression order by expression
/*

select read_the_the_times_regularly_percentile
,count(*)
from #join_data
group by read_the_the_times_regularly_percentile
order by read_the_the_times_regularly_percentile

commit;

select read_the_the_times_regularly_percentile
,count(*)
from sk_prod.PERSON_PROPENSITIES_GRID_NEW
group by read_the_the_times_regularly_percentile
order by read_the_the_times_regularly_percentile


select read_the_sun_regularly_percentile
,count(*)
from sk_prod.PERSON_PROPENSITIES_GRID_NEW
group by read_the_sun_regularly_percentile
order by read_the_sun_regularly_percentile



select read_the_financial_times_regularly_percentile
,count(*)
from sk_prod.PERSON_PROPENSITIES_GRID_NEW
group by read_the_financial_times_regularly_percentile
order by read_the_financial_times_regularly_percentile



select regularly_visit_spotify_percentile
,count(*)
from sk_prod.PERSON_PROPENSITIES_GRID_NEW
group by regularly_visit_spotify_percentile
order by regularly_visit_spotify_percentile




select p_extended_pixel,count(*)
from sk_prod.experian_consumerview
group by p_extended_pixel
order by p_extended_pixel



select pc_mosaic_uk_type,p_pixel_v2

from sk_prod.experian_consumerview
where cb_address_postcode='HP23 5PS' and cb_address_buildingno='6'

pc_mosaic_uk_type,p_pixel_v2
'03','11405'
'03','11406'

select *

from sk_prod.PERSON_PROPENSITIES_GRID_NEW
where 
mosaic_uk_2009_type
='03' and
 p_pixel_v2='11405'

select regularly_visit_spotify_percentile

from sk_prod.PERSON_PROPENSITIES_GRID_NEW
where 
mosaic_uk_2009_type='03' and
 ppixel2011
='11405'


alter table dbarnett.v250_experian_model_profiling delete hh_read_the_the_times_regularly_decile ;
alter table dbarnett.v250_experian_model_profiling delete hh_read_the_daily_telegraph_regularly_decile ;
alter table dbarnett.v250_experian_model_profiling delete hh_read_the_express_regularly_decile ;
alter table dbarnett.v250_experian_model_profiling delete hh_read_the_financial_times_regularly_decile ;
alter table dbarnett.v250_experian_model_profiling delete hh_read_the_guardian_regularly_decile ;
alter table dbarnett.v250_experian_model_profiling delete hh_read_the_independent_regularly_decile ;
alter table dbarnett.v250_experian_model_profiling delete hh_read_the_mirror_daily_record_regularly_decile ;

alter table dbarnett.v250_experian_model_profiling delete hh_read_the_sun_regularly_decile ;
alter table dbarnett.v250_experian_model_profiling delete hh_read_trade_and_professional_magazines_decile ;
alter table dbarnett.v250_experian_model_profiling delete hh_read_women_s_interests_magazines_decile ;
alter table dbarnett.v250_experian_model_profiling delete hh_regularly_visit_bbc_website_decile ;
alter table dbarnett.v250_experian_model_profiling delete hh_regularly_visit_facebook_decile ;
alter table dbarnett.v250_experian_model_profiling delete hh_regularly_visit_linkedin_decile ;
alter table dbarnett.v250_experian_model_profiling delete hh_regularly_visit_msn_decile ;

alter table dbarnett.v250_experian_model_profiling delete hh_regularly_visit_myspace_decile ;
alter table dbarnett.v250_experian_model_profiling delete hh_regularly_visit_spotify_decile ;
alter table dbarnett.v250_experian_model_profiling delete hh_regularly_visit_the_guardian_website_decile ;
alter table dbarnett.v250_experian_model_profiling delete hh_regularly_visit_the_telegraph_website_decile ;
alter table dbarnett.v250_experian_model_profiling delete hh_regularly_visit_twitter_decile ;
alter table dbarnett.v250_experian_model_profiling delete hh_regularly_visit_youtube_decile ;


*/





--select hh_income_band , count(*) from dbarnett.v250_Account_profiling group by hh_income_band order by hh_income_band