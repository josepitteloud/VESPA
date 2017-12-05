/*-----------------------------------------------------------------------------------------------------------------
        Project:V250 - Sports right Analysis Profiling
        Part - Part 13 Account Weight Production
        
        Analyst: Dan Barnett
        SK Prod: 5
        Produce account weights based on accounts with sufficient data returned

*/------------------------------------------------------------------------------------------------------------------
--Create Date Variables--
create variable @analysis_date date;



set @analysis_date='2013-10-31';



--drop table v250_single_profiling_view;
---Create Initial Table of all Standard UK and ROI Accounts---
select account_number
into dbarnett.v250_single_profiling_view
from sk_prod.cust_single_account_view as a
where acct_type='Standard' and account_number <>'?' and pty_country_code is not null
;

/*
--select * from dbarnett.v250_single_profiling_view;
alter table v250_single_profiling_view delete True_Touch_Type;
alter table v250_single_profiling_view add True_Touch_Type integer;
*/
commit;
alter table v250_single_profiling_view add(    

--SAV--                            
                            pty_country_code                varchar(10)    
                            ,cb_key_household               bigint
                            ,tenure                         varchar(20)
                            ,current_status_code            varchar(2)
                            ,isba_tv_region                 varchar(20)
                            ,cb_address_postcode       varchar(8) 
                            ,cb_address_postcode_area       varchar(4)
                            ,cb_address_postcode_district   varchar(4) 
                            ,number_of_sports_premiums      integer default 0
                            ,number_of_movies_premiums      integer default 0
                            ,current_short_description  varchar(40)
--SAV and Entitlement Lookup
                            ,mix_type                       varchar(40)

--Mix Type
                            ,entertainment_extra_flag       tinyint default 0

--Experian Consumerview
                            ,hh_composition                 varchar(2)
                            ,hh_affluence                   varchar(2)
                            ,head_hh_age                    varchar(1)
                            ,num_children_in_hh             varchar(1) 
                            ,child_age_0_4                  varchar(1) 
                            ,child_age_5_11                 varchar(1) 
                            ,child_age_12_17                varchar(1) 
                            ,num_bedrooms                   varchar(1) 
                            ,residence_type                 varchar(1) 
                            ,household_ownership_type       varchar(1) 
                            ,affluence_septile              varchar(1) 
                            ,mosaic_group                   varchar(1) 
                            ,True_Touch_Type                integer 
                            ,child_hh_00_to_04              varchar(1) 
                            ,child_hh_05_to_11              varchar(1)
                            ,child_hh_12_to_17              varchar(1)
                            ,financial_stress               varchar(1)
 
--SK_PROD.SKY_PLAYER_USAGE_DETAIL
                            ,sky_go_reg_distinct_days_used_L06M  integer
                            ,sky_go_reg_distinct_days_used_L12M  integer

--Cust Subs Hist     
                            ,BB_type                        varchar(20)
                            ,hdtv                           smallint     
                            ,multiroom                      smallint     
                            ,skyplus                        smallint   
                            ,subscription_3d                smallint 

--Value Segment Table
                            ,value_segment                  varchar(20) 


                            ,CQM                            varchar(20) 
--Update via sk_prod.CACI_SOCIAL_CLASS
                            ,social_grade                   varchar(20) 


                            ,cable_area                     integer default 0
---Fibre Area Still to Add
                            ,fibre_area                     integer default 0

--Cust_Set_Top_Box
                            ,adsmartable_hh                 integer default 0


                            ,Mirror_has_children            varchar(1)        
                            ,Mirror_Men                     varchar(5)
                            ,Mirror_Women                   varchar(5)


                            ,vespa_panel                    integer default 0
                            ,box_type                       varchar(20)
---Upgrade/Downgrade/Package Changes/Churn Events/Time on Sports etc.,

);

---Update from Single Account View---
commit;
CREATE HG INDEX idx1 ON v250_single_profiling_view(account_number);

commit;

update v250_single_profiling_view
set pty_country_code=case when b.pty_country_code is null then 'UNK' else b.pty_country_code end
,cb_key_household=b.cb_key_household
--,current_status_code=b.acct_status_code
,tenure=case when (datediff(day,acct_first_account_activation_dt,@analysis_date)) <=  365 then 'A) 0-12 Months'
                when (datediff(day,acct_first_account_activation_dt,@analysis_date)) <=  730 then 'B) 1-2 Years'
                when (datediff(day,acct_first_account_activation_dt,@analysis_date)) <= 1095 then 'C) 2-3 Years'
                when (datediff(day,acct_first_account_activation_dt,@analysis_date)) <= 1825 then 'D) 3-5 Years'
                when (datediff(day,acct_first_account_activation_dt,@analysis_date)) <= 3650 then 'E) 5-10 Years'
                else                                                                          'F) 10 Years+' end
,isba_tv_region=case when b.isba_tv_region is null then 'UNK*' else b.isba_tv_region end
--,number_of_sports_premiums=b.PROD_LATEST_ENTITLEMENT_PREM_SPORTS
--,number_of_movies_premiums=b.PROD_LATEST_ENTITLEMENT_PREM_MOVIES

,cb_address_postcode_area=b.cb_address_postcode_area
,cb_address_postcode_district=b.cb_address_postcode_district
,cb_address_postcode = b.cb_address_postcode

from v250_single_profiling_view as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number = b.account_number
;
commit;
CREATE HG INDEX idx2 ON v250_single_profiling_view(cb_key_household);
commit;

---Update to get Package details as at 1st Nov 2013---

select csh.account_number
,effective_from_dt
,effective_to_dt
,cel.prem_sports as sports_premiums_new
,cel.prem_movies as movies_premiums_new
,ent_cat_prod_changed
,status_code
,cel.contribution_gbp as new_contribution
,current_short_description
,previous_short_description
into            #all_accounts_dtv_details
FROM            sk_prod.cust_subs_hist csh
LEFT OUTER JOIN sk_prod.cust_entitlement_lookup as cel on csh.current_short_description = cel.short_description
WHERE          csh.subscription_sub_type ='DTV Primary Viewing'
                and csh.subscription_type = 'DTV PACKAGE'
--                and status_code in ('AC','AB','PC')
and effective_from_dt <=@analysis_date and effective_to_dt>@analysis_date
;
commit;

commit;
CREATE HG INDEX idx1 ON  #all_accounts_dtv_details(account_number);
commit;

--update v250_single_profiling_view  set current_status_code ='UN'
update v250_single_profiling_view 
set
number_of_sports_premiums=b.sports_premiums_new
,number_of_movies_premiums=b.movies_premiums_new
,current_short_description=b.current_short_description
,current_status_code=b.status_code
from v250_single_profiling_view  as a
left outer join #all_accounts_dtv_details as b
on a.account_number = b.account_number
;
commit;


--Update from Single Account View and Entitlement Lookup---
update v250_single_profiling_view
set mix_type=CASE WHEN  cel.mixes = 0                     THEN 'A) 0 Mixes'
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
        END 
from v250_single_profiling_view as a
left outer join sk_prod.cust_entitlement_lookup as cel
on a.current_short_description = cel.short_description
;

--Update from Mix Type
update v250_single_profiling_view
set entertainment_extra_flag=CASE WHEN mix_type IN ('A) 0 Mixes'
                                            ,'B) 1 Mix - Variety or Style&Culture'
                                            ,'D) 2 Mixes - Variety and Style&Culture')
                          THEN 0

                          WHEN mix_type IN ( 'C) 1 Mix - Other'
                                            ,'E) 2 Mixes - Other Combination'
                                            ,'F) 3 Mixes'
                                            ,'G) 4 Mixes'
                                            ,'H) 5 Mixes'
                                            ,'I) 6 Mixes')
                          THEN  1
                          ELSE  0 end
from v250_single_profiling_view
;



--Update from Experian Consumerview--
--drop table #experian_hh_summary;
select          cb_key_household
                ,max(h_household_composition)                   as hh_composition
                ,max(h_affluence_v2)                            as hh_affluence
                ,max(h_age_coarse)                              as head_hh_age
                ,max(h_number_of_children_in_household_2011)    as num_children_in_hh
                ,max(h_number_of_bedrooms)                      as num_bedrooms
                ,max(h_residence_type_v2)                       as residence_type
                ,max(h_tenure_v2) as household_ownership_type
                ,max(filler_char15) as affluence_septile
                ,max(h_mosaic_uk_group) as mosaic_group
                ,max(p_true_touch_type) as True_Touch_Type
                ,max(h_presence_of_child_aged_0_4_2011) as child_hh_00_to_04
                ,max(h_presence_of_child_aged_5_11_2011) as child_hh_05_to_11
                ,max(h_presence_of_child_aged_12_17_2011) as child_hh_12_to_17
                ,max(p_financial_stress) as financial_stress
                ,min(CASE WHEN convert(integer, h_number_of_children_in_household_2011) > 0 THEN 'Y'
                              WHEN convert(integer, h_number_of_children_in_household_2011) = 0 THEN 'N'
                         ELSE 'M' END) as Mirror_has_children
into            #experian_hh_summary
FROM            sk_prod.experian_consumerview
where           cb_address_status = '1' 
and             cb_address_dps IS NOT NULL 
and             cb_address_organisation IS NULL
group by        cb_key_household
;
commit;

exec sp_create_tmp_table_idx '#experian_hh_summary', 'cb_key_household';
commit;
--select top 500 * from #experian_hh_summary;

update v250_single_profiling_view
set              hh_composition= b.hh_composition
                ,hh_affluence=b.hh_affluence
                ,head_hh_age=b.head_hh_age
                ,num_children_in_hh=b.num_children_in_hh
                ,num_bedrooms=b.num_bedrooms
                ,residence_type=b.residence_type
                ,household_ownership_type=b.household_ownership_type
                ,affluence_septile=b.affluence_septile


                ,mosaic_group=b.mosaic_group
                ,True_Touch_Type=b.True_Touch_Type
                ,child_hh_00_to_04=b.child_hh_00_to_04
                ,child_hh_05_to_11=b.child_hh_05_to_11
                ,child_hh_12_to_17 =b.child_hh_12_to_17
                ,financial_stress=b.financial_stress
                ,Mirror_has_children=b.Mirror_has_children
from v250_single_profiling_view as a
left outer join #experian_hh_summary as b
on a.cb_key_household=b.cb_key_household
;

/*
---Add in Mirror Segments---
SELECT cb_key_household
        ,(case when p_gender = '0' then 'Male'
               when p_gender = '1' then 'Female'
               else null
          end) as person_gender
        ,(case when person_gender = 'Male'      then 1 else 0 end) as Male
        ,(case when person_gender = 'Female'    then 1 else 0 end) as Female
        ,(case when p_actual_age >= 16 and p_actual_age < 25 then '16 to 24'
               when p_actual_age >= 25 and p_actual_age < 35 then '25 to 34'
               when p_actual_age >= 35 and p_actual_age < 45 then '35 to 44'
               when p_actual_age >= 45 and p_actual_age < 55 then '45 to 54'
               when p_actual_age >= 55                       then '55 Plus'
               else null
          end) as age_band
        ,(case when age_band = '16 to 24' then 1 else 0 end) as age16to24
        ,(case when age_band = '25 to 34' then 1 else 0 end) as age25to34
        ,(case when age_band = '35 to 44' then 1 else 0 end) as age35to44
        ,(case when age_band = '45 to 54' then 1 else 0 end) as age45to54
        ,(case when age_band = '55 Plus'  then 1 else 0 end) as age55plus

        ,(case when male = 1 and age16to24 = 1 then 1 else 0 end) as male_age16to24
        ,(case when male = 1 and age25to34 = 1 then 1 else 0 end) as male_age25to34
        ,(case when male = 1 and age35to44 = 1 then 1 else 0 end) as male_age35to44
        ,(case when male = 1 and age45to54 = 1 then 1 else 0 end) as male_age45to54
        ,(case when male = 1 and age55plus = 1 then 1 else 0 end) as male_age55plus

        ,(case when female = 1 and age16to24 = 1 then 1 else 0 end) as female_age16to24
        ,(case when female = 1 and age25to34 = 1 then 1 else 0 end) as female_age25to34
        ,(case when female = 1 and age35to44 = 1 then 1 else 0 end) as female_age35to44
        ,(case when female = 1 and age45to54 = 1 then 1 else 0 end) as female_age45to54
        ,(case when female = 1 and age55plus = 1 then 1 else 0 end) as female_age55plus

INTO #ageband
FROM sk_prod.EXPERIAN_CONSUMERVIEW
where  cb_address_status = '1' 
and             cb_address_dps IS NOT NULL 
and             cb_address_organisation IS NULL;

commit;

exec sp_create_tmp_table_idx '#ageband', 'cb_key_household';
commit;


--select top 100 * from ageband

--drop table mirror_men_and_women;

select cb_key_household
        ,count(cb_key_household)        as number_in_HH
        ,sum(Male)                      as number_of_male
        ,sum(Female)                    as number_of_female

        ,sum(male_age16to24)            as num_of_male_age16to24
        ,sum(male_age25to34)            as num_of_male_age25to34
        ,sum(male_age35to44)            as num_of_male_age35to44
        ,sum(male_age45to54)            as num_of_male_age45to54
        ,sum(male_age55plus)            as num_of_male_age55plus

        ,sum(female_age16to24)          as num_of_female_age16to24
        ,sum(female_age25to34)          as num_of_female_age25to34
        ,sum(female_age35to44)          as num_of_female_age35to44
        ,sum(female_age45to54)          as num_of_female_age45to54
        ,sum(female_age55plus)          as num_of_female_age55plus
into #mirror_men_and_women
from #ageband
group by cb_key_household
order by cb_key_household;
commit;

exec sp_create_tmp_table_idx '#mirror_men_and_women', 'cb_key_household';
commit;
--select top 100 * from mirror_men_and_women
--drop table mirror_men_and_women2

select cb_key_household
       --mirror men
        ,(case when number_of_male > 0 and (number_of_male = num_of_male_age16to24) then 1 else 0 end) as MI
        ,(case when number_of_male > 0 and (num_of_male_age16to24 > 0 or num_of_male_age25to34 > 0) then 1 else 0 end) as MII
        ,(case when number_of_male > 0 and (num_of_male_age16to24 > 0 or num_of_male_age25to34 > 0 or num_of_male_age35to44 > 0) then 1 else 0 end) as MIII
        ,(case when number_of_male > 0 and (num_of_male_age16to24 > 0 or num_of_male_age25to34 > 0 or num_of_male_age35to44 > 0 or num_of_male_age45to54 > 0) then 1 else 0 end) as MIV
        ,(case when number_of_male > 0 and (number_of_male = num_of_male_age55plus) then 1 else 0 end) as MV
        ,(case when number_of_male = 0 then 1 else 0 end) as MVI

        --mirror women
        ,(case when number_of_female > 0 and (number_of_female = num_of_female_age16to24) then 1 else 0 end) as WI
        ,(case when number_of_female > 0 and (num_of_female_age16to24 > 0 or num_of_female_age25to34 > 0) then 1 else 0 end) as WII
        ,(case when number_of_female > 0 and (num_of_female_age16to24 > 0 or num_of_female_age25to34 > 0 or num_of_female_age35to44 > 0) then 1 else 0 end) as WIII
        ,(case when number_of_female > 0 and (num_of_female_age16to24 > 0 or num_of_female_age25to34 > 0 or num_of_female_age35to44 > 0 or num_of_female_age45to54 > 0) then 1 else 0 end) as WIV
        ,(case when number_of_female > 0 and (number_of_female = num_of_female_age55plus) then 1 else 0 end) as WV
        ,(case when number_of_female = 0 then 1 else 0 end) as WVI
into #mirror_men_and_women2
from #mirror_men_and_women;
commit;

exec sp_create_tmp_table_idx '#mirror_men_and_women2', 'cb_key_household';
commit;
--select top 100 * from mirror_men_and_women2

--Group into mirror men and mirror women segmentation
--drop table mirror_men_and_women_seg
select *
        ,(case when MI = 1      then 'MI'
              when MII = 1      then 'MII'
              when MIII = 1     then 'MIII'
              when MIV = 1      then 'MIV'
              when MV = 1       then 'MV'
              when MVI = 1      then 'MVI'
              else 'MVII'
           end) as Mirror_Men
        ,(case when WI = 1      then 'WI'
              when WII = 1      then 'WII'
              when WIII = 1     then 'WIII'
              when WIV = 1      then 'WIV'
              when WV = 1       then 'WV'
              when WVI = 1      then 'WVI'
              else 'WVII'
           end) as Mirror_Women
into #mirror_men_and_women_seg
from #mirror_men_and_women2;

commit;

exec sp_create_tmp_table_idx '#mirror_men_and_women_seg', 'cb_key_household';
commit;

update v250_single_profiling_view
set Mirror_Women=b.Mirror_Women
,Mirror_Men=b.Mirror_Men
from v250_single_profiling_view as a
left outer join #mirror_men_and_women_seg as b
on a.cb_key_household=b.cb_key_household
;
commit;


--Update from sk_prod.CACI_SOCIAL_CLASS--
select cb_key_household
        ,min(lukcat_fr_de_nrs) AS social_grade
        
into #social_grade
from sk_prod.CACI_SOCIAL_CLASS
where           cb_address_status = '1' 
and             cb_address_dps IS NOT NULL 
and             cb_address_organisation IS NULL
group by cb_key_household;
commit;

exec sp_create_tmp_table_idx '#social_grade', 'cb_key_household';
commit;

update v250_single_profiling_view
set social_grade=b.social_grade
from v250_single_profiling_view as a
left outer join #social_grade as b
on a.cb_key_household=b.cb_key_household
;
commit;
*/

--Update from Cust Subs Hist--


--BB_TYPE;
Select          distinct account_number
                ,CASE WHEN current_product_sk in (43494,43543) THEN '1) Fibre'
                WHEN current_product_sk in (42128,43373,43587) THEN '2) Unlimited'
                WHEN current_product_sk=42129 THEN '3) Everyday'
                WHEN current_product_sk in (42130,43586) THEN '4) Everyday Lite'
                WHEN current_product_sk in (42131,43584,43585) THEN '5) Connect' else '6) NA'
                END AS bb_type
                ,rank() over(PARTITION BY account_number ORDER BY effective_to_dt desc) AS rank_id
INTO            #bb
FROM            sk_prod.cust_subs_hist
WHERE           subscription_sub_type = 'Broadband DSL Line'
and             effective_from_dt <= @analysis_date
and             effective_to_dt > @analysis_date
and             effective_from_dt != effective_to_dt
and             (status_code IN ('AC','AB') 
                OR (status_code='PC' and prev_status_code NOT IN ('?','RQ','AP','UB','BE','PA') )
                OR (status_code='CF' AND prev_status_code='PC')
                OR (status_code='AP' AND sale_type='SNS Bulk Migration'))
GROUP BY        account_number
                ,bb_type
                ,effective_to_dt
;
DELETE FROM #bb where rank_id >1;
commit;
commit;
exec sp_create_tmp_table_idx '#bb', 'account_number';
commit;
--select bb_type , count(*) from #bb group by bb_type;
select          distinct account_number, BB_type
                ,rank() over(PARTITION BY account_number ORDER BY BB_type desc) AS rank_id
into            #bbb
from            #bb;
commit;

DELETE FROM #bbb where rank_id >1;
commit;
commit;
exec sp_create_tmp_table_idx '#bbb', 'account_number';
commit;
Update          v250_single_profiling_view
set             a.bb_type = case when b.bb_type is null then '6) NA' 
                                 when b.bb_type='NA' then '6) NA'
                            else b.bb_type end
from            v250_single_profiling_view as a
left join       #bbb as b 
on              a.account_number = b.account_number;
commit;
;
--select bb_type,count(*) from v250_single_profiling_view group by bb_type order by bb_type;
--drop table #bb;
--drop table #bbb;

--MULTI-ROOM, SKY+, HDTV & 3DTV;
SELECT          account_number
                ,MAX(CASE WHEN subscription_sub_type ='DTV Extra Subscription'                      THEN 1 ELSE 0 END) AS multiroom
                ,MAX(CASE WHEN subscription_sub_type ='DTV HD'                                      THEN 1 ELSE 0 END) AS hdtv
                ,MAX(CASE WHEN subscription_sub_type ='DTV Sky+'                                    THEN 1 ELSE 0 END) AS skyplus
                ,max(case when subscription_type = 'A-LA-CARTE' and subscription_sub_type = '3DTV'  THEN 1 ELSE 0 END) AS subscription_3d
INTO            #box_prods
FROM            sk_prod.cust_subs_hist
WHERE           subscription_sub_type  IN ('DTV Extra Subscription','DTV HD','DTV Sky+','3DTV')
AND             effective_from_dt <> effective_to_dt
AND             effective_from_dt <= @analysis_date
AND             effective_to_dt    >  @analysis_date
AND             status_code in  ('AC','AB','PC')
GROUP BY        account_number;


commit;
exec sp_create_tmp_table_idx '#box_prods', 'account_number';
commit;

Update          v250_single_profiling_view
set             a.multiroom = case when b.multiroom =1 then b.multiroom else 0 end
                ,a.hdtv = case when b.hdtv =1 then b.hdtv else 0 end
                ,a.skyplus = case when b.skyplus =1 then b.skyplus else 0 end
                ,a.subscription_3d = case when b.subscription_3d =1 then b.subscription_3d else 0 end
from            v250_single_profiling_view as a
left join       #box_prods as b 
on              a.account_number = b.account_number;
commit;




/*
--SK_PROD.SKY_PLAYER_USAGE_DETAIL Sky Go Update--
select          account_number
                ,count(distinct cb_data_date) as distinct_days_used
into            #skygo_usage
from            SK_PROD.SKY_PLAYER_USAGE_DETAIL
where           cb_data_date >= cast(@analysis_date as date)-182
and             cb_data_date <=@analysis_date
group by        account_number
order by        account_number
;
commit;
exec sp_create_tmp_table_idx '#skygo_usage', 'account_number';
commit;

Update          v250_single_profiling_view
set             sky_go_reg_distinct_days_used_L06M = case when b.distinct_days_used is null then 0 else distinct_days_used end
from            v250_single_profiling_view as a
left join       #skygo_usage as b 
on              a.account_number = b.account_number;
commit;

----Repeat for last 12M---
select          account_number
                ,count(distinct cb_data_date) as distinct_days_used
into            #skygo_usage_l12M
from            SK_PROD.SKY_PLAYER_USAGE_DETAIL
where           cb_data_date >= cast(@analysis_date as date)-365
and             cb_data_date <=@analysis_date
group by        account_number
order by        account_number
;
commit;
exec sp_create_tmp_table_idx '#skygo_usage_l12M', 'account_number';
commit;

Update          v250_single_profiling_view
set             sky_go_reg_distinct_days_used_L12M = case when b.distinct_days_used is null then 0 else distinct_days_used end
from            v250_single_profiling_view as a
left join       #skygo_usage_l12M as b 
on              a.account_number = b.account_number;
commit;
*/


/*
---Adsmartable HH---
SELECT account_number
--      ,x_pvr_type
--      ,x_manufacturer
      ,CASE  WHEN x_pvr_type ='PVR6'                                THEN 1
             WHEN x_pvr_type ='PVR5'                                THEN 1
             WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Samsung' THEN 1
             WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Pace'    THEN 1
--             WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Thomson' THEN 1
                                                                    ELSE 0
       END AS Adsmartable
into #set_top_ads
FROM   sk_prod.CUST_SET_TOP_BOX  AS SetTop
        
--where box_replaced_dt = '9999-09-09';
WHERE box_installed_dt <= @analysis_date AND box_replaced_dt > @analysis_date; --not replaced

commit;


select account_number,
      SUM(Adsmartable) AS T_AdSm_box,
      MAX(Adsmartable) AS HH_HAS_ADSMART_STB
INTO #SetTop2
FROM #set_top_ads
GROUP BY account_number;
commit;


--      create index on #SetTop2
CREATE   HG INDEX idx10 ON #SetTop2(account_number);

UPDATE v250_single_profiling_view
SET adsmartable_hh = st.HH_HAS_ADSMART_STB
FROM v250_single_profiling_view  AS Base
  left outer join  #SetTop2 AS ST
        ON base.account_number = ST.account_number;
commit;

-- delete temp file
drop table #SetTop2;
drop table #set_top_ads;
commit;


--CQM---

--select top 100 * from sk_prod.ID_V_universe_all;


UPDATE v250_single_profiling_view
SET CQM = Case when model_score  <=6  then    'a) 1-6 low risk'
                             when model_score  <=12 then    'b) 7-12'
                             when model_score  <=18 then    'c) 13-18'
                             when model_score  <=24 then    'd) 19-24'
                             when model_score  <=30 then    'e) 25-30'
                             when model_score  <=36 then    'f) 31-36 high risk'
                             else                        'g) Unknown' end
FROM v250_single_profiling_view  AS a
  left outer join  sk_prod.ID_V_universe_all as b
        ON a.cb_key_household = b.cb_key_household;
commit;
*/

----









/*
select cb_address_postcode ,cb_address_postcode_area,cb_address_postcode_sector,cb_address_postcode_district,pty_country_code
 from sk_prod.cust_single_account_view where account_number='620041578563' 

select top 100 * from sk_prod.CACI_SOCIAL_CLASS where cb_address_postcode = 'HP23 5PS' and cb_address_buildingno = '6'
select *  from        sk_prod.experian_consumerview where cb_address_postcode = 'HP23 5PS' and cb_address_buildingno = '6';



select top 100 cb_key_household,
p_true_touch_type
from sk_prod.experian_consumerview
where p_true_touch_type is not null


select top 100 cb_key_household, 
case financial_stress 
when '0' then 'Very low'
when '1' then 'Low'
when '2' then 'Medium'
when '3' then 'High'
when '4' then 'Very high'
when 'U' then 'Unclassified'
else null end 'Person Financial Stress'
from sk_prod.experian_consumerview
where financial_stress is not null


Update          v250_single_profiling_view
set             bb_type = case when bb_type is null then '6) NA' 
                                 when bb_type='NA' then '6) NA'
                            else bb_type end
from            v250_single_profiling_view as a

*/



-----Box Type------------
-------------------------------------------------  02 - set top boxes
-- Boxtype & Universe

-- Boxtype is defined as the top two boxtypes held by a household ranked in the following order
-- 1) HD, 2) HDx, 3) Skyplus, 4) FDB

-- Capture all active boxes for this week
SELECT    csh.service_instance_id
          ,csh.account_number
          ,subscription_sub_type
          ,rank() over (PARTITION BY csh.service_instance_id ORDER BY csh.account_number, csh.cb_row_id desc) AS rank
  INTO accounts -- drop table accounts
  FROM sk_prod.cust_subs_hist as csh
        INNER JOIN AdSmart AS ss
        ON csh.account_number = ss.account_number
 WHERE  csh.subscription_sub_type IN ('DTV Primary Viewing','DTV Extra Subscription')     --the DTV sub Type
   AND csh.status_code IN ('AC','AB','PC')                  --Active Status Codes
   AND csh.effective_from_dt <= @analysis_date
   AND csh.effective_to_dt > @analysis_date
   AND csh.effective_from_dt <> effective_to_dt;
commit;

-- De-dupe active boxes
DELETE FROM accounts WHERE rank>1;
commit;

CREATE HG INDEX idx14 ON accounts(service_instance_id);
commit;

-- Identify HD boxes
SELECT  stb.service_instance_id
       ,SUM(CASE WHEN current_product_description LIKE '%HD%'     THEN 1  ELSE 0 END) AS HD
       ,SUM(CASE WHEN current_product_description LIKE '%HD%1TB%'
                   or current_product_description LIKE '%HD%2TB%' THEN 1  ELSE 0 END) AS HD1TB -- combine 1 and 2 TB
INTO hda -- drop table hda
FROM sk_prod.CUST_SET_TOP_BOX AS stb
        INNER JOIN accounts AS acc
        ON stb.service_instance_id = acc.service_instance_id
WHERE box_installed_dt <= @analysis_date
        AND box_replaced_dt   > @analysis_date
        AND current_product_description like '%HD%'
GROUP BY stb.service_instance_id;
commit;

CREATE HG INDEX idx14 ON hda(service_instance_id);
commit;



--drop table scaling_box_level_viewing;
--commit;

SELECT  --acc.service_instance_id,
       acc.account_number
       ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Extra Subscription' THEN 1 ELSE 0  END) AS MR
       ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Sky+'               THEN 1 ELSE 0  END) AS SP
       ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV HD'                 THEN 1 ELSE 0  END) AS HD
       ,MAX(CASE  WHEN hda.HD = 1                                         THEN 1 ELSE 0  END) AS HDstb
       ,MAX(CASE  WHEN hda.HD1TB = 1                                      THEN 1 ELSE 0  END) AS HD1TBstb
INTO scaling_box_level_viewing
FROM sk_prod.cust_subs_hist AS csh
        INNER JOIN accounts AS acc
        ON csh.service_instance_id = acc.service_instance_id --< Limits to your universe
                LEFT OUTER JOIN sk_prod.cust_entitlement_lookup cel
                ON csh.current_short_description = cel.short_description
                        LEFT OUTER JOIN hda
                        ON csh.service_instance_id = hda.service_instance_id --< Links to the HD Set Top Boxes
 WHERE csh.effective_FROM_dt <= @analysis_date
   AND csh.effective_to_dt    > @analysis_date
   AND csh.status_code IN  ('AC','AB','PC')
   AND csh.SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing','DTV Sky+', 'DTV Extra Subscription','DTV HD' )
   AND csh.effective_FROM_dt <> csh.effective_to_dt
GROUP BY acc.service_instance_id ,acc.account_number;
commit;

drop table accounts; commit;
drop table hda; commit;


-- Identify boxtype of each box and whether it is a primary or a secondary box
SELECT  tgt.account_number
       ,SUM(CASE WHEN MR=1 THEN 1 ELSE 0 END) AS mr_boxes
       ,MAX(CASE WHEN MR=0 AND ((tgt.HD =1 AND HD1TBstb = 1) OR (tgt.HD =1 AND HDstb = 1))         THEN 4 -- HD ( inclusive of HD1TB)
                 WHEN MR=0 AND ((tgt.SP =1 AND tgt.HD1TBstb = 1) OR (tgt.SP =1 AND tgt.HDstb = 1)) THEN 3 -- HDx ( inclusive of HD1TB)
                 WHEN MR=0 AND tgt.SP =1                                                           THEN 2 -- Skyplus
                 ELSE                                                                              1 END) AS pb -- FDB
       ,MAX(CASE WHEN MR=1 AND ((tgt.HD =1 AND HD1TBstb = 1) OR (tgt.HD =1 AND HDstb = 1))         THEN 4 -- HD ( inclusive of HD1TB)
                 WHEN MR=1 AND ((tgt.SP =1 AND tgt.HD1TBstb = 1) OR (tgt.SP =1 AND tgt.HDstb = 1)) THEN 3 -- HDx ( inclusive of HD1TB)
                 WHEN MR=1 AND tgt.SP =1                                                           THEN 2 -- Skyplus
                 ELSE                                                                              1 END) AS sb -- FDB
        ,convert(varchar(20), null) as universe
        ,convert(varchar(30), null) as boxtype
  INTO boxtype_ac -- drop table boxtype_ac
  FROM scaling_box_level_viewing AS tgt
GROUP BY tgt.account_number;
commit;

-- Build the combined flags
update boxtype_ac
set universe = CASE WHEN mr_boxes = 0 THEN 'Single box HH'
                         WHEN mr_boxes = 1 THEN 'Dual box HH'
                         ELSE 'Multiple box HH' END
    ,boxtype  =
        CASE WHEN       mr_boxes = 0 AND  pb =  3 AND sb =  1   THEN  'HDx & No_secondary_box'
             WHEN       mr_boxes = 0 AND  pb =  4 AND sb =  1   THEN  'HD & No_secondary_box'
             WHEN       mr_boxes = 0 AND  pb =  2 AND sb =  1   THEN  'Skyplus & No_secondary_box'
             WHEN       mr_boxes = 0 AND  pb =  1 AND sb =  1   THEN  'FDB & No_secondary_box'
             WHEN       mr_boxes > 0 AND  pb =  4 AND sb =  4   THEN  'HD & HD' -- If a hh has HD  then all boxes have HD (therefore no HD and HDx)
             WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  3) OR (pb =  3 AND sb =  4)  THEN  'HD & HD'
             WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  2) OR (pb =  2 AND sb =  4)  THEN  'HD & Skyplus'
             WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  1) OR (pb =  1 AND sb =  4)  THEN  'HD & FDB'
             WHEN       mr_boxes > 0 AND  pb =  3 AND sb =  3                            THEN  'HDx & HDx'
             WHEN       mr_boxes > 0 AND (pb =  3 AND sb =  2) OR (pb =  2 AND sb =  3)  THEN  'HDx & Skyplus'
             WHEN       mr_boxes > 0 AND (pb =  3 AND sb =  1) OR (pb =  1 AND sb =  3)  THEN  'HDx & FDB'
             WHEN       mr_boxes > 0 AND  pb =  2 AND sb =  2                            THEN  'Skyplus & Skyplus'
             WHEN       mr_boxes > 0 AND (pb =  2 AND sb =  1) OR (pb =  1 AND sb =  2)  THEN  'Skyplus & FDB'
                        ELSE   'FDB & FDB' END
;
commit;


--Add Box Types in HH back to main table---

--alter table v183_all_active_accounts add boxtype varchar(50);

update v250_single_profiling_view
set box_type=b.boxtype
from v250_single_profiling_view as a
left outer join boxtype_ac as b
on a.account_number = b.account_number
;

commit;



--select * from v250_single_profiling_view;
--select distinct value_seg_date from sk_prod.value_segments_five_yrs order by value_seg_date

update v250_single_profiling_view
set value_segment=b.value_segment
from v250_single_profiling_view as a
left outer join sk_prod.value_segments_five_yrs as b
 ON a.account_number = b.account_number
where value_seg_date='2013-10-30'
;
commit;






-----Update Value Segment and Box Type for End Oct


---Add on Weights for accounts----

--drop table #account_details_for_weighting;drop table #weighting_values; drop table #account_weight;
select b.account_number
,case when bb_type  in ('NA','6) NA') 
--and talk_product is null 
then 'a) TV Only' else 'b) TV with BB and or Talk' end as product_holding_group

,case when b.value_segment = 'UNSTABLE' then 'UNSTABLE' else 'STABLE' end as value_segment_group
,case when b.affluence_septile in ('4','5','6') then '01) High Affluence' else '02) Non-High Affluence' end as affluence_group
,case when b.tenure in ('A) 0-12 Months','B) 1-2 Years','C) 2-3 Years') then 'A-C) 0-36 Months' else tenure end as tenure_group 

,case          when b.box_type in ('FDB & FDB','HD & FDB','HD & HD','HD & Skyplus','HDx & FDB','HDx & HDx','HDx & Skyplus','Skyplus & FDB','Skyplus & Skyplus') then 'a) Multiple Boxes'  
                        when b.box_type in ('HD & No_secondary_box','HDx & No_secondary_box','Skyplus & No_secondary_box') then 'b) Single Non-FDB Box' 
                        when b.box_type in ('FDB & No_secondary_box') then 'c) FDB Box' else 'b) Single Non-FDB Box'  end as box_type_group ---Set non-Matches to Most popular group 

,case when number_of_sports_premiums=2 and number_of_movies_premiums=2 then 'a) Top Tier' 
                        when number_of_sports_premiums=2 and number_of_movies_premiums=0 then 'b) Dual Sports' 
                        when number_of_sports_premiums=0 and number_of_movies_premiums=2 then 'c) Dual Movies' 
                        when number_of_sports_premiums+ number_of_movies_premiums>0 then 'd) Other Premiums' else 'e) Other' end as tv_package_group

,case when c.total_days_with_viewing>=280 then 1 else 0 end as vespa_viewing_account

into #account_details_for_weighting
from v250_single_profiling_view as b
left outer join dbarnett.v250_annualised_activity_table_for_workshop as c
on b.account_number=c.account_number
where current_status_code in ('AC','AB','PC') and pty_country_code='GBR'
;
--select top 100 * from dbarnett.v250_annualised_activity_table_for_workshop;
select product_holding_group
,value_segment_group
,affluence_group
,tenure_group 

,box_type_group
,tv_package_group
,count(*) as accounts
,sum(vespa_viewing_account) as vespa_account
into #weighting_values
from #account_details_for_weighting
group by product_holding_group
,value_segment_group
,affluence_group
,tenure_group 

,box_type_group
,tv_package_group
;

commit;

select account_number
,accounts/cast(vespa_account as real) as weight
into #account_weight
from #account_details_for_weighting as a
left outer join #weighting_values as b
on  a.product_holding_group=b.product_holding_group
and a.tv_package_group=b.tv_package_group
and a.box_type_group=b.box_type_group
and a.value_segment_group=b.value_segment_group
and a.tenure_group=b.tenure_group
and a.affluence_group=b.affluence_group 
where vespa_viewing_account=1 
;
commit;
--select sum(weight) from #account_weight;
--alter table v223_unbundling_viewing_summary_by_account add account_weight real;

alter table dbarnett.v250_annualised_activity_table_for_workshop delete account_weight;
alter table dbarnett.v250_annualised_activity_table_for_workshop add account_weight real;

--update dbarnett.v250_annualised_activity_table_for_workshop set account_weight=0;
update dbarnett.v250_annualised_activity_table_for_workshop
set account_weight= case when b.weight is null then 0 else b.weight end
from dbarnett.v250_annualised_activity_table_for_workshop as a
left outer join #account_weight as b
on a.account_number = b.account_number
;
commit;
--select count(*) from dbarnett.v250_annualised_activity_table_for_workshop
--select sum (account_weight) ,count(*) from dbarnett.v250_annualised_activity_table_for_workshop where account_weight>0 ;
delete from dbarnett.v250_annualised_activity_table_for_workshop where account_weight = 0; commit;
--select top 100 * from dbarnett.v250_annualised_activity_table_for_workshop;
---
grant all on dbarnett.v250_annualised_activity_table_for_workshop to public;

--select sum(account_weight) from v223_unbundling_viewing_summary_by_account 

--select count(*) from v220_zero_mix_active_uk_accounts;
commit;