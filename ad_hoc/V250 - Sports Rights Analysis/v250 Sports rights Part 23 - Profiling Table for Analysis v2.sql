/*-----------------------------------------------------------------------------------------------------------------
        Project:V250 - Sports right Analysis Profiling
        Part - Part 23 Profiling details for accounts
        
        Analyst: Dan Barnett
        SK Prod: 5
        

*/------------------------------------------------------------------------------------------------------------------

--Profiling metrics taken as end end of the analysis period

--Create Date Variables--
create variable @analysis_date date;
set @analysis_date='2013-10-31';

--drop table dbarnett.v250_Account_profiling;
---Create Initial Table of all Standard UK and ROI Accounts---
select account_number
into dbarnett.v250_Account_profiling
from sk_prod.cust_single_account_view as a
where acct_type='Standard' and account_number <>'?' and pty_country_code is not null
;

/*
alter table dbarnett.v250_Account_profiling delete True_Touch_Type;
alter table dbarnett.v250_Account_profiling add True_Touch_Type integer;
*/
commit;
alter table dbarnett.v250_Account_profiling add(    

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

---Upgrade/Downgrade/Package Changes/Churn Events/Time on Sports etc.,

);

---Update from Single Account View---
commit;
CREATE HG INDEX idx1 ON dbarnett.v250_Account_profiling(account_number);

commit;

update dbarnett.v250_Account_profiling
set pty_country_code=case when b.pty_country_code is null then 'UNK' else b.pty_country_code end
,cb_key_household=b.cb_key_household
,current_status_code=b.acct_status_code
,tenure=case when (datediff(day,acct_first_account_activation_dt,@analysis_date)) <=  365 then 'A) 0-12 Months'
                when (datediff(day,acct_first_account_activation_dt,@analysis_date)) <=  730 then 'B) 1-2 Years'
                when (datediff(day,acct_first_account_activation_dt,@analysis_date)) <= 1095 then 'C) 2-3 Years'
                when (datediff(day,acct_first_account_activation_dt,@analysis_date)) <= 1825 then 'D) 3-5 Years'
                when (datediff(day,acct_first_account_activation_dt,@analysis_date)) <= 3650 then 'E) 5-10 Years'
                else                                                                          'F) 10 Years+' end
,isba_tv_region=case when b.isba_tv_region is null then 'UNK*' else b.isba_tv_region end
,number_of_sports_premiums=b.PROD_LATEST_ENTITLEMENT_PREM_SPORTS
,number_of_movies_premiums=b.PROD_LATEST_ENTITLEMENT_PREM_MOVIES

,cb_address_postcode_area=b.cb_address_postcode_area
,cb_address_postcode_district=b.cb_address_postcode_district
,cb_address_postcode = b.cb_address_postcode

from dbarnett.v250_Account_profiling as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number = b.account_number
;
commit;
CREATE HG INDEX idx2 ON dbarnett.v250_Account_profiling(cb_key_household);
commit;
--Update from Single Account View and Entitlement Lookup---
SELECT ar.account_number
        --,effective_from_dt
        ,cel.prem_sports
        ,cel.prem_movies
        ,ent_cat_prod_start_dt
        ,cel.Variety
        ,cel.Knowledge
        ,cel.Kids
        ,cel.Style_Culture
        ,cel.Music
        ,cel.News_Events
,csh.current_short_description
        ,rank() over(partition by ar.account_number ORDER BY csh.effective_from_dt, csh.cb_row_id desc) as rank
INTO #dtv
--        tempdb..viq_current_pkg_tmp
FROM dbarnett.v250_Account_profiling ar
        left join sk_prod.cust_subs_hist as csh
            on csh.account_number = ar.account_number
        inner join sk_prod.cust_entitlement_lookup as cel
            on csh.current_short_description = cel.short_description
WHERE csh.subscription_sub_type ='DTV Primary Viewing'
       AND csh.subscription_type = 'DTV PACKAGE'
and             effective_from_dt <= @analysis_date
and             effective_to_dt > @analysis_date
       AND csh.effective_from_dt != csh.effective_to_dt;
commit;


DELETE FROM #dtv WHERE rank > 1;
commit;
---Add on Sports and Movies Premiums---
alter table dbarnett.v250_Account_profiling add sports_premiums tinyint;
alter table dbarnett.v250_Account_profiling add movies_premiums tinyint;
alter table dbarnett.v250_Account_profiling add DTV_Package varchar(30);



update dbarnett.v250_Account_profiling
set mix_type=CASE WHEN  cel.mixes = 0                     THEN 'A) 0 Mixes'
            WHEN  cel.mixes = 1
             AND (cel.style_culture = 1 OR cel.variety = 1) THEN 'B) 1 Mix - Variety or Style&Culture'
            WHEN  cel.mixes = 1                     THEN 'C) 1 Mix - Other'
            WHEN  cel.mixes = 2
             AND  cel.style_culture = 1
             AND  cel.variety = 1                       THEN 'D) 2 Mixes - Variety and Style&Culture'
            WHEN  cel.mixes = 2
             AND (cel.style_culture = 0 OR cel.variety = 0) THEN 'E) 2 Mixes - Other Combination'
            WHEN  cel.mixes = 3                     THEN 'F) 3 Mixes'
            WHEN  cel.mixes = 4                     THEN 'G) 4 Mixes'
            WHEN  cel.mixes = 5                     THEN 'H) 5 Mixes'
            WHEN  cel.mixes = 6                     THEN 'I) 6 Mixes'
            ELSE                                         'J) Unknown'
        END 
,sports_premiums=case when b.prem_sports is null then 0 else b.prem_sports end
,movies_premiums=case when b.prem_movies is null then 0 else b.prem_movies end
,DTV_Package=case when b.prem_sports =2 and  b.prem_movies=2 then 'a) All Premiums'
                  when b.prem_sports =2 and  b.prem_movies=0 then 'b) Dual Sports'
                  when b.prem_sports =0 and  b.prem_movies=2 then 'c) Dual Movies'
                  when b.prem_sports + b.prem_movies>0 then 'd) Other Premiums' else 'e) No premiums' end
from dbarnett.v250_Account_profiling as a
inner join #dtv as b
on a.account_number = b.account_number
  inner join sk_prod.cust_entitlement_lookup as cel
            on b.current_short_description = cel.short_description

;
--select * from sk_prod.cust_entitlement_lookup;
--Update from Mix Type
update dbarnett.v250_Account_profiling
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
from dbarnett.v250_Account_profiling
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

update dbarnett.v250_Account_profiling
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
from dbarnett.v250_Account_profiling as a
left outer join #experian_hh_summary as b
on a.cb_key_household=b.cb_key_household
;

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

update dbarnett.v250_Account_profiling
set Mirror_Women=b.Mirror_Women
,Mirror_Men=b.Mirror_Men
from dbarnett.v250_Account_profiling as a
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

update dbarnett.v250_Account_profiling
set social_grade=b.social_grade
from dbarnett.v250_Account_profiling as a
left outer join #social_grade as b
on a.cb_key_household=b.cb_key_household
;
commit;


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
Update          dbarnett.v250_Account_profiling
set             a.bb_type = case when b.bb_type is null then '6) NA' 
                                 when b.bb_type='NA' then '6) NA'
                            else b.bb_type end
from            dbarnett.v250_Account_profiling as a
left join       #bbb as b 
on              a.account_number = b.account_number;
commit;
;

---Add in a BB_Fibre Flag--
--
alter table dbarnett.v250_Account_profiling add bb_fibre tinyint;
update dbarnett.v250_Account_profiling
set bb_fibre=case when bb_type='1) Fibre' then 1 else 0 end
from dbarnett.v250_Account_profiling
;
commit;

--select bb_type,count(*) from dbarnett.v250_Account_profiling group by bb_type order by bb_type;
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

Update          dbarnett.v250_Account_profiling
set             a.multiroom = case when b.multiroom =1 then b.multiroom else 0 end
                ,a.hdtv = case when b.hdtv =1 then b.hdtv else 0 end
                ,a.skyplus = case when b.skyplus =1 then b.skyplus else 0 end
                ,a.subscription_3d = case when b.subscription_3d =1 then b.subscription_3d else 0 end
from            dbarnett.v250_Account_profiling as a
left join       #box_prods as b 
on              a.account_number = b.account_number;
commit;




--drop table #skygo_usage_l12M; drop table #skygo_usage;
--SK_PROD.SKY_PLAYER_USAGE_DETAIL Sky Go Update--
select          account_number
                ,count(distinct activity_dt) as distinct_days_used
into            #skygo_usage
from            SK_PROD.SKY_PLAYER_USAGE_DETAIL
where           activity_dt >=  cast(@analysis_date as date)-182
and             activity_dt <=@analysis_date
group by        account_number
order by        account_number
;
commit;
exec sp_create_tmp_table_idx '#skygo_usage', 'account_number';
commit;
--select top 500 * from #skygo_usage
Update          dbarnett.v250_Account_profiling
set             sky_go_reg_distinct_days_used_L06M = case when b.distinct_days_used is null then 0 else distinct_days_used end
from            dbarnett.v250_Account_profiling as a
left join       #skygo_usage as b 
on              a.account_number = b.account_number;
commit;

----Repeat for last 12M---
select          account_number
                ,count(distinct activity_dt) as distinct_days_used
into            #skygo_usage_l12M
from            SK_PROD.SKY_PLAYER_USAGE_DETAIL
where           activity_dt >= cast(@analysis_date as date)-365
and             activity_dt <=@analysis_date
group by        account_number
order by        account_number
;
commit;
exec sp_create_tmp_table_idx '#skygo_usage_l12M', 'account_number';
commit;

Update          dbarnett.v250_Account_profiling
set             sky_go_reg_distinct_days_used_L12M = case when b.distinct_days_used is null then 0 else distinct_days_used end
from            dbarnett.v250_Account_profiling as a
left join       #skygo_usage_l12M as b 
on              a.account_number = b.account_number;
commit;

--select sum( sky_go_reg_distinct_days_used_L12M ), sum(sky_go_reg_distinct_days_used_L06M) from  dbarnett.v250_Account_profiling 


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

UPDATE dbarnett.v250_Account_profiling
SET adsmartable_hh = st.HH_HAS_ADSMART_STB
FROM dbarnett.v250_Account_profiling  AS Base
  left outer join  #SetTop2 AS ST
        ON base.account_number = ST.account_number;
commit;

-- delete temp file
drop table #SetTop2;
drop table #set_top_ads;
commit;


--CQM---

--select top 100 * from sk_prod.ID_V_universe_all;


UPDATE dbarnett.v250_Account_profiling
SET CQM = Case when model_score  <=6  then    'a) 1-6 low risk'
                             when model_score  <=12 then    'b) 7-12'
                             when model_score  <=18 then    'c) 13-18'
                             when model_score  <=24 then    'd) 19-24'
                             when model_score  <=30 then    'e) 25-30'
                             when model_score  <=36 then    'f) 31-36 high risk'
                             else                        'g) Unknown' end
FROM dbarnett.v250_Account_profiling  AS a
  left outer join  sk_prod.ID_V_universe_all as b
        ON a.cb_key_household = b.cb_key_household;
commit;


----
grant all on dbarnett.v250_Account_profiling to public;

---Add Account Weight----
alter table dbarnett.v250_Account_profiling add account_weight real;

update dbarnett.v250_Account_profiling
set account_weight=b.account_weight
from dbarnett.v250_Account_profiling as a 
left outer join dbarnett.v250_master_account_list as b
on a.account_number=b.account_number
;

commit;

delete from dbarnett.v250_Account_profiling where account_weight is null;
delete from dbarnett.v250_Account_profiling where account_weight =0;
commit;

--select count(*) from dbarnett.v250_Account_profiling;

---Add in talk and Line Rental
SELECT csh.account_number
                ,MAX(CASE  WHEN csh.subscription_sub_type = 'SKY TALK SELECT'
                                AND(csh.status_code = 'A'
                                OR (csh.status_code = 'FBP' AND prev_status_code in ('PC','A'))
                                OR (csh.status_code = 'RI'  AND prev_status_code in ('FBP','A'))
                                OR (csh.status_code = 'PC'  AND prev_status_code = 'A')        )
                           THEN 1 ELSE 0 END)   AS talk
           ,MAX(CASE       WHEN csh.subscription_sub_type ='SKY TALK LINE RENTAL'
                                AND csh.status_code in ('A','CRQ','PAX')
                           THEN 1 ELSE 0 END) AS wlr
into #talk_prods
FROM            sk_prod.cust_subs_hist AS csh
        LEFT OUTER JOIN sk_prod.cust_entitlement_lookup cel
                on csh.current_short_description = cel.short_description
        inner join dbarnett.v250_Account_profiling u
                on csh.account_number = u.account_number
WHERE                        effective_from_dt <= @analysis_date
and             effective_to_dt > @analysis_date
and             effective_from_dt != effective_to_dt
GROUP BY        csh.account_number;
commit;

commit;
CREATE HG INDEX idx1 ON #talk_prods(account_number);

commit;
alter table dbarnett.v250_Account_profiling add talk tinyint;
alter table dbarnett.v250_Account_profiling add line_rental tinyint;

update dbarnett.v250_Account_profiling 
set talk=case when b.talk=1 then 1 else 0 end
,line_rental= case when b.wlr=1 then 1 else 0 end
from dbarnett.v250_Account_profiling as a
left outer join #talk_prods as b
on a.account_number = b.account_number
;
commit;


---Add in Sky Go Extra
SELECT csh.account_number

           ,MAX(CASE WHEN  csh.subscription_type='A-LA-CARTE'  and   csh.subscription_sub_type ='Sky Go Extra'
                                AND csh.status_code in ('AC','AB','PC')
                           THEN 1 ELSE 0 END) AS sky_go_extra
into #sky_go_extra
FROM            sk_prod.cust_subs_hist AS csh
        LEFT OUTER JOIN sk_prod.cust_entitlement_lookup cel
                on csh.current_short_description = cel.short_description
        inner join dbarnett.v250_Account_profiling u
                on csh.account_number = u.account_number
WHERE                        effective_from_dt <= @analysis_date
and             effective_to_dt > @analysis_date
and             effective_from_dt != effective_to_dt
GROUP BY        csh.account_number;
commit;

commit;
CREATE HG INDEX idx1 ON #sky_go_extra(account_number);

commit;
alter table dbarnett.v250_Account_profiling add sky_go_extra tinyint;

update dbarnett.v250_Account_profiling 
set sky_go_extra=case when b.sky_go_extra=1 then 1 else 0 end
from dbarnett.v250_Account_profiling as a
left outer join #sky_go_extra as b
on a.account_number = b.account_number
;
commit;
---On Demand usage
select          account_number
                ,sum(download_count) as on_demand_downloads_L12M          
into            #anytime_plus_dl
from            sk_prod.cust_anytime_plus_downloads
where           cast(last_modified_dt as date) >= @analysis_date-365
and             cast(last_modified_dt as date) <= @analysis_date    
group by account_number  
;
commit;
CREATE HG INDEX idx1 ON #anytime_plus_dl(account_number);

commit;
alter table dbarnett.v250_Account_profiling add on_demand_downloads_L12M   integer;

update dbarnett.v250_Account_profiling 
set on_demand_downloads_L12M  =case when b.on_demand_downloads_L12M>0 then b.on_demand_downloads_L12M else 0 end
from dbarnett.v250_Account_profiling as a
left outer join #anytime_plus_dl as b
on a.account_number = b.account_number
;
commit;


select          account_number
                ,count(*) as sky_store_PPV_L12M          
into            #sky_store_ppv
from            sk_prod.cust_product_charges_ppv
where           cast(last_modified_dt as date) >= @analysis_date-365
and             cast(last_modified_dt as date) <= @analysis_date    
and ppv_cancelled_dt ='9999-09-09'
group by account_number  
;

commit;
CREATE HG INDEX idx1 ON #sky_store_ppv(account_number);

commit;
alter table dbarnett.v250_Account_profiling add sky_store_PPV_L12M    integer;

update dbarnett.v250_Account_profiling 
set sky_store_PPV_L12M  =case when b.sky_store_PPV_L12M >0 then b.sky_store_PPV_L12M  else 0 end
from dbarnett.v250_Account_profiling as a
left outer join #sky_store_ppv as b
on a.account_number = b.account_number
;
commit;


SELECT  base.account_number
,count(*) as offers
INTO     #offers
FROM     sk_prod.cust_product_offers AS CPO  inner join dbarnett.v250_Account_profiling  AS Base
                    ON CPO.account_number = base.account_number
WHERE    offer_id                NOT IN (SELECT offer_id
                                         FROM citeam.sk2010_offers_to_exclude)
        and cast(offer_start_dt as date) >= @analysis_date-365
and             cast(offer_start_dt as date) <= @analysis_date  
        AND offer_amount          < 0
        AND offer_dim_description   NOT IN ('PPV 1 Administration Charge','PPV EURO1 Administration Charge')
        AND UPPER (offer_dim_description) NOT LIKE '%VIP%'
        AND UPPER (offer_dim_description) NOT LIKE '%STAFF%'
        AND UPPER (offer_dim_description) NOT LIKE 'PRICE PROTECTION%'
and offer_dim_description in ( '#1.50 Off DTV For 6 Months - Existing UK (Auto Trans)'
,'#1.50 Off DTV With HD For 6 Months - Existing UK (Auto Trans)'
,'#10 off DTV for 1 month with #10 Upfront Payment'
,'#10 Off Of Top Tier For One Month - Existing customers UK'
,'#12.50 Off Entertainment Extra for 6 months with 4 Premiums'
,'#15 Off DTV for 2 Months With #30 Upfront Payment - UK'
,'#2.50 Off DTV With 1+ Premiums For 12 Months - Existing UK (Auto Trans)'
,'#2.50 Off DTV With 1+ Premiums For 6 Months - Existing UK (Auto Trans)'
,'#5 Off Of Dual Movies For One Month - Existing customers UK'
,'#5 Off Of Dual Sports For One Month - Existing customers UK'
,'1 Month Free DTV - UK'
,'10 GBP off Sky TV for 6 months with Broadband Unlimited'
,'10 Percent off Top Tier For 9 Months - Existing UK Customers(Auto-Trans)'
,'10% of DTV for 12 Months - UK Customers'
,'10% Off DTV For 12 Months - Existing UK Customers (Auto Trans)'
,'10% off DTV for 12 Months with PVR STB - Existing Customer UK'
,'10% off DTV for 3 Months - Existing Customer(Auto-Trans)'
,'10% off DTV for 6 Months  - UK Customers'
,'10% Off DTV For 6 Months (2+ Year Tenure) - Existing UK Customers'
,'10% off DTV For 6 months with 2+ Premiums and 1+ of  tenure - Existing Customer (Auto Trans) Recontract'
,'10% off DTV Subscription for 12 Months Existing Customer recontract Letter  (Auto Transfer)'
,'10% off DTV Subscription for 12 Months Existing PVR Customer recontract Letter  (Auto Transfer)'
,'10.75 GBP Off DTV for 12 Months (Auto Trans) - Existing UK Customers'
,'11.50GBP off Entertainment Extra, or Plus for 12 Months - Existing UK Customers'
,'12 Month Free Entertainment Extra+ - UK Customers'
,'12 Months 50% Off DTV - Existing Customers (Auto Transfer)'
,'12 Months 50% off DTV (Auto Transfer) Existing Customer'
,'12 Months 50% Off DTV for Existing Customers (Auto-Tnf)'
,'12 Months 50% Off DTV for New Customers with HD Pack (Auto-TNF) - Online'
,'12 Months 50% Off DTV forExisting Customers With Sky+ Subs (Auto-Tnf) UK'
,'12 Months 5GBP off Entertainment Extra+ - Existing UK Customers'
,'12 Months 75% Off DTV - Existing UK Customers'
,'12 Months Free Dual Movies When Upgrading to Top Tier - Existing UK (Auto Trans)'
,'12 Months Half Price DTV - Exception Offer (Auto Trans)'
,'12 Months Half Price DTV for FDB (Auto Trans) - Existing UK Customer'
,'12 Months Half Price DTV Package (Recontract) - Existing UK Customers'
,'12 Months Half Price DTV with HD Pack'
,'12 Months Half Price Dual Movies (Auto Trans)'
,'12 Months Half Price Dual Movies With Basic Packs - Existing UK (Auto Trans)'
,'12 Months Half Price Dual Movies With Dual Sports - Existing UK Customers'
,'12 Months Half Price Dual Movies With Dual Sports Existing UK (Auto Trans)'
,'12 Months Half Price Top Tier - Existing Customer UK (Online) - Auto Trans'
,'12 Months Half Price Top Tier - Existing Customer UK (Sep 12)'
,'12GBP off Dual Sports for 12 Months - UK Customers'
,'13.25GBP off DTV for 12 Months with BB Unlimited, Sky Talk, Ent Extra, 4 Premiums and HD - Existing UK Customers'
,'15 Percent off DTV for 6 Months - Existing UK Customers'
,'15.75GBP off DTV for 12 Months with Ents Extra, 4 Premiums and HD - Existing UK Customers'
,'18 Months 3GBP Off Dual Sports (1+ year tenure) - Existing UK Customer'
,'2 Months Free Dual Movies with 4 Premiums (8GBP Off) (Auto Trans) - New UK Customers'
,'2 Months Free Dual Movies with 4 Premiums (8GBP Off) (Auto Trans)- UK Customers'
,'2 Months Free Movies With  Basic Pack Customers UK'
,'2 Months Free Top Tier (29GBP) - Existing UK Customers'
,'2.50 GBP Off Entertainment Extra+ for 6 Months non HD Customers'
,'20% of DTV for 12 Months with 2 Years+ Tenure - UK Customers'
,'20% off DTV for 12 Months(Recontract) - Existing UK Customer'
,'20% off DTV for 3 Months (2+ Tenure) - Existing UK Customers'
,'20% off DTV For 6 months With 2+ Premiums - Customers With 5+ tenure Recontract (Auto-Trans)'
,'20% off DTV For 6 months with 2+ Premiums and 3+ tenure - Existing Customer UK (Auto Trans) Recontract'
,'20% off DTV For 6 months With 2+ tenure'
,'20% Off DTV for 9 Months - Existing UK Sky+ Customers'
,'20% off DTV for 9 Months - UK Customers With 2+ Years Tenure'
,'20% off DTV Subscription for 12 Months Existing Sky+ Subs Customer  (Auto Transfer)'
,'20% Off Viewing Subscritpion for 6 Months'
,'24 Months 4GBP Off Dual Sports (1+ year tenure) - Existing UK Customer'
,'24 Months 7GBP off Movies And Sports (1+ year tenure) - Existing UK Customers'
,'25 Percent Off DTV for 9 Months - Existing PVR UK Customers'
,'25% off DTV for 12 Months - Existing UK Customer'
,'25% off DTV for 12 Months - Existing UK Customer (Auto Trans)'
,'25% off DTV for 2 Months - Existing Customer'
,'25% off DTV for 3 Months - Existing Customer'
,'25% off DTV for 6 Months - Existing UK Customer (Auto Transfer)'
,'25% Off DTV for 6 months for 1+ premium and a PVR- Existing Customers'
,'25% Off DTV for 6 months for Basic pack and a PVR- Existing Customers'
,'25% off DTV Subscription for 12 Months Existing Sky+ Subs Customer  (Auto Transfer)'
,'25% Off DTV Subscription for 6 Months - Existing Customers'
,'25% Off Dual Movies For 3 Months (4 GBP) - Exisiting UK Customer'
,'25% Off Dual Movies For 3 Months With 1+ Year Tenure -  Existing Customer UK (Auto-trans)'
,'25% Off Dual Movies For 3 Months With top Tier And 1+ Year Tenure -  Existing Customer UK (Auto-trans)'
,'25% Off Dual Movies For 6 Months With 3+ Years Tenure (4 GBP) - Existing UK Customer (Recontract)'
,'25% Off Dual Sports For 3 Months - Exisiting UK Customer'
,'25% Off Dual Sports For 3 Months With 1+ Year Tenure -  Existing Customer UK (Auto-trans)'
,'25% Off Dual Sports For 3 Months With 1+ Years Tenure (5.25 GBP) - Exisiting UK Customer (Recontract)'
,'25% Off Dual Sports For 3 Months With Top Tier And 1+ Year Tenure -  Existing Customer UK (Auto-trans)'
,'25% Off Dual Sports For 6 Months With 3+ Years Tenure - Exisiting UK Customer (Recontract)'
,'3 Months Free ESPN with Dual Sports - Existing Customers UK (Online)'
,'3 Months Free ESPN With Dual Sports - Existing UK Customers'
,'3 Months Free ESPN With Dual Sports - UK Customers'
,'3 Months Free Sky Movies Only (16 GBP) - Existing UK Customer (Auto Transfer)'
,'3 Months free Sports for dual Movies Customers - (Auto Trans) (Retail Support)'
,'3 Months Half Price Broadband Unlimited - Existing UK Customers'
,'3 Months Half Price DTV Subscription - Existing UK Customer'
,'3 Months Half Price Dual Sports - Existing UK Customers (Auto Trans)'
,'3 Months Half Price ESPN for Existing Customers With Dual Sports- UK'
,'3 Months Half Price ESPN for With Dual Sports - UK (Online)'
,'3 Months Half Price Movies - UK Customer (Auto Trans)'
,'3 Months Half Price Movies (8GBP off) - Existing UK Customers (Auto Trans - Online)'
,'3 Months Half Price Movies (8GBP off) - Existing UK Customers (Auto Trans)'
,'3 Months Half Price Movies On Top Tier (4 GBP) - Existing UK Customer (Auto Transfer)'
,'3 Months Half Price Movies Only (8 GBP) - Existing UK Customer (Auto Transfer)'
,'3 Months Half Price Sports - Existing UK Customers (Auto Trans)'
,'3 Months Half Price Sports (10.50GBP off) - UK Customers'
,'3 Months Half Price Sports and Movies (#14.50) off - UK Customer'
,'3 Months Half Price Top Tier - Existing UK Customer (Auto Transfer)'
,'30% off DTV for 12 Months(Recontract) - Existing UK Customer'
,'4 GBP off Movies for 3 Months - Existing UK Customers'
,'4 Months Dual Sports for 5GBP - Existing UK Customers'
,'4 Months Free DTV - Existing UK Customers'
,'5 GBP Off DTV For 1 Month With upfront payment - UK MDU Only'
,'5 GBP Sky Movies for 8 Months with Sports (3 GBP) - Existing UK Customers'
,'5 GBP Sky Movies for 8 Months without Sports (11 GBP) - Existing UK Customers'
,'5.25GBP Off DTV for 12 months - UK Customer'
,'50% off 4 Premiums for 6 months - UK Customer (Auto Trans)'
,'50% off Chelsea TV for 3 Months'
,'50% off Chelsea TV for 3 Months - Existing Customer'
,'50% off Chelsea TV for 3 Months - Existing Customer (Online)'
,'50% Off DTV Subscription - Auto Transfer OS Offer'
,'50% off DTV Subscription for 12 months - SORT Offer (Auto Trans)'
,'50% off DTV Subscription for 6 months - SORT Offer (Auto Trans)'
,'50% off DTV Subscription for 6 Months Existing Sky+ Subs Customer  (Auto Transfer)'
,'50% Off DTV With Entertainment Extra for 6 Months Excluding Top Tier - UK Trans'
,'50% off Dual Movies Subscription for 6 months - SORT Offer (Auto Trans)'
,'50% off Dual Sports Subscription for 6 months - SORT Offer (Auto Trans)'
,'50% off Entertainment with No Premiums for 3 months - Existing PVR UK 1+ Tenure (Auto Trans)'
,'50% off MUTV for 3 Months - Existing Customer'
,'50% off MUTV for 3 Months - Existing Customer (Online)'
,'5GBP off DTV with 4 premiums - Existing UK Customers'
,'5GBP off DTV with 4 premiums for one Month - Existing UK Customers (Online Auto)'
,'5GBP Off Entertainment Extra for 12 Months When Taking HD - UK Existing Customers'
,'5GBP Sports for 2 Months (16GBP Off) (Auto Trans) - Existing UK Customers'
,'5GBP Sports for 3 Months (17GBP Off) (Auto Trans) - Existing UK Customers'
,'6 Month 2.50GBP off Entertainment Extra+ - Existing UK Customers'
,'6 Month 2.50GBP off Entertainment Extra+ - UK Customers'
,'6 Months 1/2 price DTV Subscription (Auto Transfer)'
,'6 Months 14 GBP off Entertainment Extra+ with 4 Premiums - UK Customers'
,'6 Months 14GBP off Entertainment Extra with Sports and Movies - UK Customers'
,'6 Months 2.50GBP off Entertainment Extra+ with Premiums - Existing UK Customers'
,'6 Months 25% DTV for FDB Customers - Existing UK Customers'
,'6 Months 50% off Entertainment Extra+ (Auto Trans) (Retail Support) - UK Customers'
,'6 Months 50% off Entertainment with Dual Sports or Dual Movies (Auto Trans) (Retail Support) - UK Customers'
,'6 Months Free DTV - Existing customer UK (Auto Trans)'
,'6 Months Free DTV Subscription - Auto Transfer - Existing customer UK(recontract)'
,'6 Months Free Dual Movies When Upgrading to Top Tier Existing UK'
,'6 Months Free MUTV Subscription - Existing Customers'
,'6 Months Free Sky Movies Only (16 GBP) - Existing UK Customer (Auto Transfer)'
,'6 Months Half Price DTV with Sky+ Subscription - Existing Customer'
,'6 Months Half Price Dual Movies (8 GBP) - UK Customer (Auto-Trans)'
,'6 Months Half Price Dual Movies With 5+ Years Tenure (8 GBP) - Existing UK Customer (Recontract)'
,'6 Months Half Price Dual Sports - Exisiting UK Customer'
,'6 Months Half Price Dual Sports With 5+ Years Tenure - Exisiting UK Customer (Recontract)'
,'6 Months Half price Entertainment Extra For 6 Months  (Auto-Trans)'
,'6 Months Half Price Entertainment Extra with HD - UK Customer'
,'6 Months Half Price ESPN for exisitng ESPN subscribers - Existing UK Customers'
,'6 Months Half Price Movies (8 GBP) - Existing UK Customer (Auto Transfer)'
,'6 Months Half Price Movies On Top Tier (4 GBP) - Existing UK Customer (Auto Transfer)'
,'6 Months Half Price Movies When Ordering Sports (4 GBP off) - Existing UK Customers (Auto-Trans)'
,'6 Months Half Price Sky Go Extra - Existing Customers'
,'6 Months Half Price Sports When Ordering Movies - Existing UK Customers (Auto Trans)'
,'6 Months Half Price Sports When Upgrading To 4 Premiums from Basic (10.50 GBP off) - Existing UK Customers (Auto-Trans)'
,'6 Months Half Price Top Tier for Basic Customers'
,'6.50 GBP Off DTV for 6 Months - UK Customer'
,'6.50GBP off Entertainment Extra for 12 Months - Existing UK Customers (Recontract)'
,'7 GBP Off DTV for 3 Months when upgrading to 4 Premiums from Basic  - Existing UK Customers'
,'75% off DTV for 12 Months When Ordering HD Pack - Existing UK Customers'
,'75% off DTV for 12 Months with Sky+ Subs When No Box or Install Offers Applied - Existing UK Customers'
,'75% off DTV for 9 months - Existing PVR UK (Auto Trans)'
,'7GBP Off 4 Premiums for 6 Months - Existing UK Customer'
,'7GBP off Top Tier for 3 Months - UK Customers 1+ Year Tenure'
,'7GBP off Top Tier for 6 Months - UK Customers 1+ Year Tenure'
,'8 Months Half Price Movies (8GBP) - UK customers with 10+ Years Tenure'
,'9 GBP off 4 Premiuims For 3 Months - UK Customers With 1+ Year Tenure'
,'9 GBP off 4 Premiuims For 6 Months - UK Customers With 5+ Year Tenure'
,'9 GBP off 4 Premiums for 6 Months (AutoTrans) - Existing UK Customers'
,'9 Months 20% off DTV for Existing FDB Owners - UK Existing'
,'9 Months 25% off DTV Subscription - Existing UK Customer'
,'9 Months DTV Half Price, Sky+ Customers - Existing UK Customers'
,'9 Months Free Sky Movies Only (16 GBP) - Existing UK Customer (Auto Transfer)'
,'9GBP off Top Tier for 10 Months - UK Customers with 5+ Years Tenure'
,'DTV 20% off for 6 Months with Sky+ Subscription - Existing UK customers'
,'DTV 20% off for 8 Months - Existing UK Customers'
,'DTV 25% off for 12 Months - UK Customer'
,'DTV 25% off for 12 Months with FDB - Exisitng Customers UK'
,'DTV 35% off for 12 Months With Sky+ - Existing UK Customer'
,'DTV 50% off for 2 Months - Existing UK Customers'
,'DTV 50% Off For 9 Months - UK Customers'
,'DTV 6.50GBP Off For 12 Months - Existing UK Customers'
,'DTV 75% off for 12 Months With Sky+ Subscription - Reinstating UK Customers'
,'DTV Free for 2 Months - Existing UK Customers'
,'DTV Half Price for 12 Months With FDB - Reinstating UK Customers'
,'DTV Half Price for 12 Months With Sky+ Subscription - Reinstating UK Customers'
,'DTV Subscription 10% off for 12 Months'
,'DTV Subscription 10% off for 12 Months For FDB Customers - UK (Auto Trans)'
,'DTV Subscription 20% off for 12 Months For FDB Customers - UK (Auto Trans)'
,'DTV Subscription 20% off for 6 Months with 2 or more Premiums and HD Mix 1+ Tenure (Auto Trans) UK'
,'DTV Subscription 20% off for 6 Months with 2 or more Premiums and MS 1+ Tenure(Auto Transfer) UK'
,'DTV Subscription Free for 3 Months - Existing UK (trans)'
,'Dual Movies for #1 on Top Tier for 3 Months (Existing UK)'
,'Dual Movies for #5 for 2 Months for basic Pack - Existing UK Customer (Online)'
,'Dual Movies for 1GBP for 6 Months (7GBP Discount) - Existing UK Customers (Auto Trans)'
,'Dual Movies Free for 1 Month for Basic Mix Customers - Existing UK (Online)'
,'Dual Movies Free for 2 Months When Upgrading to Top Tier - Existing UK Customers'
,'Dual Movies Half Price for 3 Months - 3 Year Tenure or Less UK Customers'
,'Dual Movies Half Price for 6 Months for Basic Mix  -Existing UK Customers'
,'Dual Movies Half Price For 6 Months When Ordering Broadband Unlimited -  Existing Customer UK (Auto-trans)'
,'Dual Movies Half Price For 6 Months When Ordering HD Pack -  Existing Customer UK (Auto-trans)'
,'Dual Movies Half Price For 6 Months When Ordering Line Rental -  Existing Customer UK (Auto-trans)'
,'Dual Movies Half Price For 6 Months When Ordering MR -  Existing Customer UK (Auto-trans)'
,'Dual Sports 5GBP off for 24 Months - UK Customer 1+ Years Tenure'
,'Dual Sports and Movies Half Price for 12 Months - Existing UK Customer'
,'Dual Sports Half Price for 3 Months - 3 Year Tenure or Less UK Customers'
,'Dual Sports Half Price For 6 Months When Ordering Broadband Unlimited -  Existing Customer UK  (Auto-trans)'
,'Dual Sports Half Price For 6 Months When Ordering HD Pack-  Existing Customer UK (Auto-trans)'
,'Dual Sports Half Price For 6 Months When Ordering Line Rental -  Existing Customer UK (Auto-trans)'
,'Dual Sports Half Price For 6 Months When Ordering MR -  Existing Customer UK (Auto-trans)'
,'Dual Sports Or Top Tier, 11GBP off for 4 Months - UK Customer with 3+ Years Tenure'
,'Entertainment Extra 6.50GBP off for 12 Months - Existing UK Customers (Recontract)'
,'Entertainment Extra Pack Half Price for 6 months - UK Customers'
,'Entertainment Extra+ 2.50 off for 3 Months - Existing UK Customer'
,'Entertainment Extra+ 2.50GBP off for 6 Months - Existing UK Customers'
,'Entertainment Extra+ 2.50GBP off for 9 Months - Existing UK Customers'
,'Entertainment Extra+ and Sky Movies at 23.75GBP for 12 Months - UK Customers'
,'Entertainment Extra+ Half Price for 12 Months - UK Customer'
,'Entertainment Pack at 5GBP for 3 Months (16.50GBP Off) - Existing UK Customers'
,'Entertainment Pack for #5 for 3 Months with PVR- Existing Customer UK'
,'Free Chelsea TV for 2 Months'
,'Free DTV Package - Outsource'
,'Free Sky Sports Only (21GBP) for 2 Months - Existing UK Customers'
,'Half Price DTV for 3 Months - UK Customers (Auto-Trans)'
,'Half Price Entertainment Package for 6 Months - UK Customers'
,'Half Price ESPN for 3 Months - Existing UK Customers'
,'Half Price HD Mix for 12 Months - New Customer - (Online Offer)'
,'Half Price Premiums On Top Tier For 6 Months When Ordering Broadband Unlimited -  Existing Customer UK (Auto-trans)'
,'Half Price Premiums On Top Tier For 6 Months When Ordering HD Pack -  Existing Customer UK (Auto-trans)'
,'Half Price Premiums On Top Tier For 6 Months When Ordering Line Rental -  Existing Customer UK (Auto-trans)'
,'Half Price Premiums On Top Tier For 6 Months When Ordering MR -  Existing Customer UK (Auto-trans)'
,'Half Price Sky Movies for 12 Months (8 GBP) - UK Customers'
,'MUTV Free for 2 Months'
,'MUTV Free for 3 Months'
,'Single Sport packages 13 GBP Off For 12 Months - Existing Customer UK'
,'Sky Movies 10GBP for 12 months - Existing UK Customer'
,'Sky Movies 5GBP for 3 Months with Broadband and line rental - Existing UK Customers'
,'Sky Movies 5GBP for 3 Months with Top Tier - Existing UK Customer'
,'Sky Movies 5GBP for 3 Months without Top Tier - Existing UK Customer'
,'Sky Movies at 9.99GBP for 12 Months - Existing UK Customers (Auto)'
,'Sky Movies at 9.99GBP for 12 Months - UK Customers'
,'Sky Movies Free for 3 Months - UK Customers'
,'Sky Movies Half Price for 6 Months - UK Customers With 3+ Years Tenure'
,'Sky Movies Half Price on Top Tier - UK Customers With 3+ Years Tenure'
,'Sky Movies, 8GBP off for 4 Months - UK Custmer with 3+ Years Tenure'
,'Sky Sports 13GBP for 24 Months - Existing UK Customers'
,'Sky Sports 16GBP for 18 Months - Existing UK Customers'
,'Sky Sports 1GBP for 6 Months with Top Tier - Existing UK Customers'
,'Sky Sports 20GBP off for 1 Months - Existing UK Customer'
,'Sky Sports 20GBP off for 1 Months - Existing UK Customer(Auto)'
,'Sky Sports and Movies at 23 GBP for 6 Months upgrading from Basic - Existing UK Customers'
,'Sky Sports Half Price - UK Customers With 3+ Years Tenure'
,'Sky Sports Half Price for 12 Months - Existing Uk Customers'
,'Sky Sports Half Price for 3 Months With Top Tier - Existing UK Customers'
,'Sky Sports Half Price on Top Tier - UK Customers With 3+ Years Tenure'
,'Sky World Free for 12 Months - Competition Winner'
,'Sports and Movies for 20GBP for 10 months - Existing UK Customers'
,'Sports Half Price for 6 Months - UK Customer 3+ years Tenure'
,'Top Tier 10 GBP for 3 Months - Existing UK Customer'
,'Top Tier 19GBP off for 3 Months - Existing UK Customers'
,'Top Tier 22GBP for 3 Months - 1 Year+ Tenure UK Customer'
,'Top Tier at 10GBP for 10 Months - Existing UK Customers'
,'Top Tier at 10GBP for 6 Months - Existing UK Customers'
,'Top Tier at 10GBP for 6 Months(20 GBP off) - Existing UK Customers(Auto)'
,'Top Tier free for 12 Months - UK Customers'
,'Top Tier, 8GBP off for 4 Months - UK Customer with 1+ Years Tenure'
,'Upgrade from Base Pack Only To Dual Movies at 5 GBP for 3 Months - Existing UK Customers'
,'Upgrade from Base Pack Only To Dual Movies Free for 2 Months - Existing UK Customers'
,'Upgrade from Base Pack Only To Dual Movies Half Price for 12 Months - Existing UK Customer (Auto)'
,'Upgrade from Base Pack Only To Dual Movies Half Price for 3 Months - Existing UK Customers'
,'Upgrade from Base Pack Only To Dual Movies Half Price for 6 Months - Existing UK Customers'
,'Upgrade from Base Pack Only To Dual Sports Half Price for 3 Months - Existing UK Customers (Auto)'
,'Upgrade from Base Pack Only to Top Tier Half Price for 3 Months - UK Existing Customers (Auto)'
,'Upgrade from Base Pack to Top Tier 30 GBP off for 1 Month - Existing UK Customer (Auto)'
,'Upgrade from Basic to Dual Sports Half Price for 6 Months - Existing UK Customers'
,'Upgrade From Basic to Top Tier and Get Top Tier Half Price for 6 Months - UK Existing Online (Sep 12)'
,'Upgrade from Basic to Top Tier, 10 GBP for 3 Months - Existing UK Customer (Auto)'
,'Upgrade from Dual Movies to Top Tier 1 GBP for 3 Months - Existing UK Customers (Auto)'
,'Upgrade from Dual Movies to Top Tier 1 GBP for 6 Months - Existing UK Customers (Auto)'
,'Upgrade from Dual Sports to Top Tier 1 GBP for 3 Months - Existing UK Customers (Auto)'
,'Upgrade From Entertainment Extra to Entertainment Extra+ Free for 12 Months - Existing UK Customers'
,'Upgrade From Entertainment Extra to Entertainment Extra+ Half Price for 12 Months - Existing UK Customers'
,'Upgrade From Entertainment Extra to Entertainment Extra+ Half Price for 12 Months- Existing UK Customers'
,'Upgrade From Entertainment Extra to Entertainment Extra+ Half Price for 12 Months With Premiums - Existing UK Customers (Auto)'
,'Upgrade From Entertainment Extra to Entertainment Extra+ Half Price for 3 Months - Existing UK Customers'
,'Upgrade From Entertainment to Entertainment Extra+ Free for 12 Months - Existing UK Customers'
,'Upgrade From Entertainment to Entertainment Extra+ Half Price for 12 Months - Existing UK Customers'
,'Upgrade From Entertainment to Entertainment Extra+ Half Price for 3 Months - Existing UK Customers'
,'Upgrade from Mix & 0 Premiums to Mix & Dual Sports Half Price for 3 Months - UK'
,'Upgrade from Mix & 0 Premiums to Mix & Dual Sports Half Price for 6 Months - UK (Online)'
,'Upgrade from Mix & Dual Sports to Mix & 4 Premiums & get Dual Movies Half Price for 3 Months - UK (Online Sep 11)'
,'Upgrade to 4 Premiums Half Price for 3 Months - Existing UK'
,'Upgrade to Dual Movies Half Price for 3 Months - Existing UK'
,'Upgrade to Dual Sports Half Price for 3 Months - Existing UK'
,'Upgrade to Top Tier from any Basic Mix Half Price for 6 Months (UK)'
)
and offer_end_dt>offer_start_dt
GROUP BY base.account_number
;
commit;


commit;
CREATE HG INDEX idx1 ON #offers(account_number);

commit;
alter table dbarnett.v250_Account_profiling add dtv_software_offers   integer;

update dbarnett.v250_Account_profiling 
set dtv_software_offers =case when b.offers is not null then b.offers  else 0 end
from dbarnett.v250_Account_profiling as a
left outer join #offers as b
on a.account_number = b.account_number
;
commit;

--Last 12M Invoice---

select a.account_number
,sum(b.total_paid_amt*-1) as total_bill_amt_paid
into #last_12M_paid_amt 
from dbarnett.v250_Account_profiling  as a
left outer join sk_prod.cust_bills  as b
on a.account_number = b.account_number
where payment_due_dt between cast(@analysis_date as date)-365
and            @analysis_date
group by a.account_number
;
commit;
CREATE HG INDEX idx1 ON #last_12M_paid_amt (account_number);

commit;
alter table dbarnett.v250_Account_profiling add invoice_paid_amt_L12M   real;
alter table dbarnett.v250_Account_profiling add invoice_paid_amt_L12M_Scaled   real;

update dbarnett.v250_Account_profiling 
set invoice_paid_amt_L12M =case when b.total_bill_amt_paid is not null then b.total_bill_amt_paid  else 0 end
,invoice_paid_amt_L12M_Scaled =case when b.total_bill_amt_paid is not null then b.total_bill_amt_paid*account_weight  else 0 end
from dbarnett.v250_Account_profiling as a
left outer join #last_12M_paid_amt  as b
on a.account_number = b.account_number
;
commit;


select cb_address_postcode
, pc_clientele_01_me_and_my_pint
, pc_clientele_02_big_night_out
, pc_clientele_03_business_and_pleasure
, pc_clientele_04_family_fun
, pc_clientele_05_daytime_local
, pc_clientele_06_pub_play
, pc_clientele_07_evening_local
, pc_clientele_08_out_for_dinner
, pc_clientele_09_student_drinks
, pc_clientele_10_out_on_the_town
, pc_clientele_11_leisurely_lunch
, pc_clientele_12_weekend_lunch
, pc_clientele_13_catch_up
, pc_clientele_14_sociable_suburbs
into #clientele
FROM sk_prod.CONSUMERVIEW_POSTCODE
;


commit;
CREATE HG INDEX idx1 ON #clientele (cb_address_postcode);

commit;
alter table dbarnett.v250_Account_profiling add pc_clientele_01_me_and_my_pint   varchar(4);
alter table dbarnett.v250_Account_profiling add pc_clientele_02_big_night_out   varchar(4);
alter table dbarnett.v250_Account_profiling add pc_clientele_03_business_and_pleasure   varchar(4);
alter table dbarnett.v250_Account_profiling add pc_clientele_04_family_fun   varchar(4);
alter table dbarnett.v250_Account_profiling add pc_clientele_05_daytime_local   varchar(4);
alter table dbarnett.v250_Account_profiling add pc_clientele_06_pub_play   varchar(4);
alter table dbarnett.v250_Account_profiling add pc_clientele_07_evening_local   varchar(4);
alter table dbarnett.v250_Account_profiling add pc_clientele_08_out_for_dinner   varchar(4);
alter table dbarnett.v250_Account_profiling add pc_clientele_09_student_drinks   varchar(4);
alter table dbarnett.v250_Account_profiling add pc_clientele_10_out_on_the_town   varchar(4);
alter table dbarnett.v250_Account_profiling add pc_clientele_11_leisurely_lunch   varchar(4);
alter table dbarnett.v250_Account_profiling add pc_clientele_12_weekend_lunch   varchar(4);
alter table dbarnett.v250_Account_profiling add pc_clientele_13_catch_up   varchar(4);
alter table dbarnett.v250_Account_profiling add pc_clientele_14_sociable_suburbs   varchar(4);

update dbarnett.v250_Account_profiling 
set pc_clientele_01_me_and_my_pint=case when c.pc_clientele_01_me_and_my_pint is null then '000' else c.pc_clientele_01_me_and_my_pint end
,pc_clientele_02_big_night_out=case when c.pc_clientele_02_big_night_out is null then '000' else c.pc_clientele_02_big_night_out end
,pc_clientele_03_business_and_pleasure =case when c.pc_clientele_03_business_and_pleasure  is null then '000' else c.pc_clientele_03_business_and_pleasure  end
,pc_clientele_04_family_fun=case when c.pc_clientele_04_family_fun is null then '000' else c.pc_clientele_04_family_fun end
,pc_clientele_05_daytime_local=case when c.pc_clientele_05_daytime_local is null then '000' else c.pc_clientele_05_daytime_local end
,pc_clientele_06_pub_play=case when c.pc_clientele_06_pub_play is null then '000' else c.pc_clientele_06_pub_play end
,pc_clientele_07_evening_local=case when c.pc_clientele_07_evening_local is null then '000' else c.pc_clientele_07_evening_local end
,pc_clientele_08_out_for_dinner  =case when c.pc_clientele_08_out_for_dinner   is null then '000' else c.pc_clientele_08_out_for_dinner   end
,pc_clientele_09_student_drinks =case when c.pc_clientele_09_student_drinks  is null then '000' else c.pc_clientele_09_student_drinks end
,pc_clientele_10_out_on_the_town=case when c.pc_clientele_10_out_on_the_town is null then '000' else c.pc_clientele_10_out_on_the_town end
,pc_clientele_11_leisurely_lunch=case when c.pc_clientele_11_leisurely_lunch is null then '000' else c.pc_clientele_11_leisurely_lunch end
,pc_clientele_12_weekend_lunch=case when c.pc_clientele_12_weekend_lunch is null then '000' else c.pc_clientele_12_weekend_lunch end
,pc_clientele_13_catch_up =case when c.pc_clientele_13_catch_up  is null then '000' else c.pc_clientele_13_catch_up  end
,pc_clientele_14_sociable_suburbs=case when c.pc_clientele_14_sociable_suburbs is null then '000' else c.pc_clientele_14_sociable_suburbs end

from dbarnett.v250_Account_profiling as a
left outer join sk_prod.cust_single_account_view  as b
on a.account_number = b.account_number
left outer join #clientele  as c
on b.cb_address_postcode = c.cb_address_postcode
;
commit;


Select           account_number
                ,1 as ever_had_bb
                ,min(effective_from_dt) as first_bb_date
INTO            #bb_summary
FROM            sk_prod.cust_subs_hist
WHERE           subscription_sub_type = 'Broadband DSL Line'
and             effective_from_dt <= @analysis_date
and             effective_from_dt != effective_to_dt
and             (status_code IN ('AC','AB') 
                OR (status_code='PC' and prev_status_code NOT IN ('?','RQ','AP','UB','BE','PA') )
                OR (status_code='CF' AND prev_status_code='PC')
                OR (status_code='AP' AND sale_type='SNS Bulk Migration'))
GROUP BY        account_number
;
--select first_bb_date ,

commit;
CREATE HG INDEX idx1 ON #bb_summary (account_number);

commit;
alter table dbarnett.v250_Account_profiling add BB_History   varchar(30);
alter table dbarnett.v250_Account_profiling add BB_Tenure   varchar(30);

update dbarnett.v250_Account_profiling
set BB_History =case when  ever_had_bb=1 and bb_type ='6) NA' then '2) Had BB'
                    when  ever_had_bb=1 and bb_type <>'6) NA' then '1) Has BB' else '3) Never Had BB' end
,BB_Tenure=case when ever_had_bb is null then 'G) No BB' when (datediff(day,first_bb_date,@analysis_date)) <=  365 then 'A) 0-12 Months'
                when (datediff(day,first_bb_date,@analysis_date)) <=  730 then 'B) 1-2 Years'
                when (datediff(day,first_bb_date,@analysis_date)) <= 1095 then 'C) 2-3 Years'
                when (datediff(day,first_bb_date,@analysis_date)) <= 1825 then 'D) 3-5 Years'
                when (datediff(day,first_bb_date,@analysis_date)) <= 3650 then 'E) 5-10 Years'
                else                                                                          'F) 10 Years+' end
from dbarnett.v250_Account_profiling as a
left outer join #bb_summary as b
on a.account_number =b.account_number
;
commit;


---------------------------------------------------------------------------------
--  Cable Availibility
---------------------------------------------------------------------------------
SELECT a.account_number
      ,CASE  WHEN cable_postcode ='N' THEN 'N'
             WHEN cable_postcode ='n' THEN 'N'
             WHEN cable_postcode ='Y' THEN 'Y'
             WHEN cable_postcode ='y' THEN 'Y'
                                      ELSE 'N/A'
       END AS Cable_area
into #cable
  FROM dbarnett.v250_Account_profiling as a
left outer join sk_prod.cust_single_account_view  as b
on a.account_number = b.account_number
       LEFT OUTER JOIN sk_prod.broadband_postcode_exchange  AS bb
       ON replace(b.cb_address_postcode,' ','') = replace(bb.cb_address_postcode,' ','');
commit;


--      create index
CREATE   HG INDEX idx06 ON #cable(account_number);

--      update CABLE_AVAILABLE
UPDATE dbarnett.v250_Account_profiling
SET  Cable_area = case when cab.Cable_area ='Y' then 1 else 0 end
FROM dbarnett.v250_Account_profiling  AS Base
  INNER JOIN #cable AS cab
        ON base.account_number = cab.account_number;
commit;


SELECT   base.account_number
       ,1 AS ESPN_Subscribers
INTO #ESPN
  FROM sk_prod.cust_subs_hist AS ESPN
        inner join dbarnett.v250_Account_profiling AS Base
         ON ESPN.account_number = Base.account_number
 WHERE subscription_type ='A-LA-CARTE'               --A La Carte Stack
   AND subscription_sub_type = 'ESPN'                --ESPN Subscriptions
   AND status_code in ('AC','AB','PC')               --Active Status Codes
group by base.account_number
,ESPN_Subscribers
 
;

commit;
CREATE HG INDEX idx1 ON #ESPN (account_number);

commit;
alter table dbarnett.v250_Account_profiling add prev_espn_sub
   tinyint;

update dbarnett.v250_Account_profiling
set prev_espn_sub
 =case when  b.ESPN_Subscribers is null then 0 else 1 end

from dbarnett.v250_Account_profiling as a
left outer join #ESPN as b
on a.account_number =b.account_number
;



SELECT   base.account_number
       ,1 AS ESPN_Subscribers
INTO #ESPN_L12M
  FROM sk_prod.cust_subs_hist AS ESPN
        inner join dbarnett.v250_Account_profiling AS Base
         ON ESPN.account_number = Base.account_number
 WHERE subscription_type ='A-LA-CARTE'               --A La Carte Stack
   AND subscription_sub_type = 'ESPN'                --ESPN Subscriptions
   AND status_code in ('AC','AB','PC')               --Active Status Codes
and (effective_from_dt between @analysis_date-365 and @analysis_date
or effective_from_dt<@analysis_date-365 and effective_to_dt >=@analysis_date)
group by base.account_number
,ESPN_Subscribers
 
;

commit;
CREATE HG INDEX idx1 ON #ESPN_L12M (account_number);

commit;
alter table dbarnett.v250_Account_profiling add prev_espn_sub_L12M
   tinyint;

update dbarnett.v250_Account_profiling
set prev_espn_sub_L12M
 =case when  b.ESPN_Subscribers is null then 0 else 1 end

from dbarnett.v250_Account_profiling as a
left outer join #ESPN_L12M as b
on a.account_number =b.account_number
;

--select sum(prev_espn_sub),sum(prev_espn_sub_L12M) from dbarnett.v250_Account_profiling


--drop table #package_changes;
SELECT    csh.Account_number
         ,ncel.prem_movies + ncel.prem_sports AS current_premiums
         ,ocel.prem_movies + ocel.prem_sports AS old_premiums
         ,ncel.prem_movies                    AS current_movies
         ,ocel.prem_movies                    AS old_movies
         ,                   ncel.prem_sports AS current_sports
         ,                   ocel.prem_sports AS old_sports
         ,rank() over(PARTITION BY base.account_number ORDER BY effective_to_dt desc) AS rank_id
         ,effective_to_dt
         ,effective_from_dt
                    INTO #package_changes
    FROM sk_prod.cust_subs_hist AS csh
         inner join sk_prod.cust_entitlement_lookup AS ncel
                    ON csh.current_short_description = ncel.short_description
         inner join sk_prod.cust_entitlement_lookup AS ocel
                    ON csh.previous_short_description = ocel.short_description
         inner join dbarnett.v250_Account_profiling AS Base
                    ON csh.account_number = base.account_number
WHERE csh.effective_to_dt > csh.effective_from_dt
    AND csh.effective_from_dt <= @analysis_date  -- Date range
    AND subscription_sub_type='DTV Primary Viewing'
    AND status_code IN ('AC','PC','AB')   -- Active records
  
    AND csh.ent_cat_prod_changed = 'Y'    -- The package has changed - VERY IMPORTANT
    AND csh.prev_ent_cat_product_id<>'?'  -- This is not an Aquisition
;
commit;

select account_number
,sum(case when current_sports <old_sports and effective_from_dt between @analysis_date-182 and @analysis_date then 1 else 0 end) as sports_downgrades_L06M
,sum(case when current_sports <old_sports and effective_from_dt between @analysis_date-365 and @analysis_date then 1 else 0 end) as sports_downgrades_L12M
,sum(case when current_sports <old_sports   then 1 else 0 end) as sports_downgrades_Ever

,sum(case when current_movies <old_movies and effective_from_dt between @analysis_date-182 and @analysis_date then 1 else 0 end) as movies_downgrades_L06M
,sum(case when current_movies <old_movies and effective_from_dt between @analysis_date-365 and @analysis_date then 1 else 0 end) as movies_downgrades_L12M
,sum(case when current_movies <old_movies   then 1 else 0 end) as movies_downgrades_Ever

,sum(case when current_sports >old_sports and effective_from_dt between @analysis_date-182 and @analysis_date then 1 else 0 end) as sports_upgrades_L06M
,sum(case when current_sports >old_sports and effective_from_dt between @analysis_date-365 and @analysis_date then 1 else 0 end) as sports_upgrades_L12M
,sum(case when current_sports >old_sports   then 1 else 0 end) as sports_upgrades_Ever

,sum(case when current_movies >old_movies and effective_from_dt between @analysis_date-182 and @analysis_date then 1 else 0 end) as movies_upgrades_L06M
,sum(case when current_movies >old_movies and effective_from_dt between @analysis_date-365 and @analysis_date then 1 else 0 end) as movies_upgrades_L12M
,sum(case when current_movies >old_movies   then 1 else 0 end) as movies_upgrades_Ever
into #package_changes_by_account
from #package_changes
group by account_number
;


commit;
CREATE HG INDEX idx1 ON #package_changes_by_account (account_number);

commit;
alter table dbarnett.v250_Account_profiling add sports_downgrades_L06M integer;

alter table dbarnett.v250_Account_profiling add sports_downgrades_Ever integer;

alter table dbarnett.v250_Account_profiling add Movies_downgrades_L06M integer;
alter table dbarnett.v250_Account_profiling add Movies_downgrades_Ever integer;


alter table dbarnett.v250_Account_profiling add sports_upgrades_L06M integer;
alter table dbarnett.v250_Account_profiling add sports_upgrades_Ever integer;

alter table dbarnett.v250_Account_profiling add Movies_upgrades_L06M integer;
alter table dbarnett.v250_Account_profiling add Movies_upgrades_Ever integer;

alter table dbarnett.v250_Account_profiling add sports_downgrades_L12M integer;
alter table dbarnett.v250_Account_profiling add Movies_downgrades_L12M integer;
alter table dbarnett.v250_Account_profiling add sports_upgrades_L12M integer;
alter table dbarnett.v250_Account_profiling add Movies_upgrades_L12M integer;

update dbarnett.v250_Account_profiling
set sports_downgrades_L06M =case when  b.sports_downgrades_L06M is null then 0 else b.sports_downgrades_L06M end
, sports_downgrades_L12M =case when  b.sports_downgrades_L12M is null then 0 else b.sports_downgrades_L12M end
,sports_downgrades_Ever =case when  b.sports_downgrades_Ever is null then 0 else b.sports_downgrades_Ever end
,Movies_downgrades_L06M =case when  b.Movies_downgrades_L06M is null then 0 else b.Movies_downgrades_L06M end
,Movies_downgrades_L12M =case when  b.Movies_downgrades_L12M is null then 0 else b.Movies_downgrades_L12M end
,Movies_downgrades_Ever =case when  b.Movies_downgrades_Ever is null then 0 else b.Movies_downgrades_Ever end

,sports_upgrades_L06M =case when  b.sports_upgrades_L06M is null then 0 else b.sports_upgrades_L06M end
,sports_upgrades_L12M =case when  b.sports_upgrades_L12M is null then 0 else b.sports_upgrades_L12M end
,sports_upgrades_Ever =case when  b.sports_upgrades_Ever is null then 0 else b.sports_upgrades_Ever end
,Movies_upgrades_L06M =case when  b.Movies_upgrades_L06M is null then 0 else b.Movies_upgrades_L06M end
,Movies_upgrades_L12M =case when  b.Movies_upgrades_L12M is null then 0 else b.Movies_upgrades_L12M end
,Movies_upgrades_Ever =case when  b.Movies_upgrades_Ever is null then 0 else b.Movies_upgrades_Ever end

from dbarnett.v250_Account_profiling as a
left outer join #package_changes_by_account as b
on a.account_number =b.account_number
;
commit;
select a.account_number
,max(x_skyfibre_enabled) as sky_fibre_area
into #bb_fibre_area
  FROM dbarnett.v250_Account_profiling as a
left outer join sk_prod.cust_single_account_view  as b
on a.account_number = b.account_number
       LEFT OUTER JOIN sk_prod.BT_FIBRE_POSTCODE   AS bb
       ON replace(b.cb_address_postcode,' ','') = replace(bb.cb_address_postcode,' ','')
group by a.account_number;
commit;
--select sky_fibre_area, count(*) from #bb_fibre_area group by sky_fibre_area
commit;
CREATE HG INDEX idx1 ON #bb_fibre_area (account_number);

commit;
--alter table dbarnett.v250_Account_profiling delete bb_fibre_area ;
alter table dbarnett.v250_Account_profiling add bb_fibre_area tinyint;

update dbarnett.v250_Account_profiling
set bb_fibre_area =case when  upper(sky_fibre_area)='Y' then 1 else 0 end

from dbarnett.v250_Account_profiling as a
left outer join #bb_fibre_area as b
on a.account_number =b.account_number
;
commit;

alter table dbarnett.v250_Account_profiling add bb_network_status varchar(12);
update dbarnett.v250_Account_profiling
set bb_network_status=exchange_status
from dbarnett.v250_Account_profiling as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number = b.account_number
;
commit;

select  account_number
       ,count(*) as churn_events
  into #all_churn_records
  from sk_prod.cust_subs_hist as csh
 where subscription_sub_type ='DTV Primary Viewing'     --DTV stack
   and status_code in ('PO','SC')                       --CUSCAN and SYSCAN status codes
   and prev_status_code in ('AC','AB','PC')             --Previously ACTIVE
   and status_code_changed = 'Y'
   and effective_from_dt != effective_to_dt
and effective_from_dt<=@analysis_date
group by account_number
;
commit;

alter table dbarnett.v250_Account_profiling add churn_events_ever integer;
update dbarnett.v250_Account_profiling
set churn_events_ever=case when b.churn_events is null then 0 else b.churn_events end 
from dbarnett.v250_Account_profiling as a
left outer join #all_churn_records as b
on a.account_number = b.account_number
;
commit;



alter table dbarnett.v250_Account_profiling add value_segment varchar(20) default 'Unknown';
update dbarnett.v250_Account_profiling
set value_segment=case when b.value_segment is null then 'Unknown' else b.value_segment end
from dbarnett.v250_Account_profiling as a
left outer join sk_prod.VALUE_SEGMENTS_FIVE_YRS as b
on a.account_number = b.account_number
where b.value_seg_date='2013-10-30'
;

commit;
update dbarnett.v250_Account_profiling
set value_segment=case when value_segment is null then 'Unknown' else value_segment end
from dbarnett.v250_Account_profiling as a
;

commit;
--alter table dbarnett.v250_Account_profiling delete multiscreen ;
alter table dbarnett.v250_Account_profiling add multiscreen varchar(50);
update dbarnett.v250_Account_profiling
set multiscreen=case    when sky_go_extra=1 and multiroom=1 then '1) Sky Go Multiscreen'
                        when sky_go_extra=1  then '2) Sky Go Extra - No Multiroom' else '3) No Sky Go Extra' end
from dbarnett.v250_Account_profiling as a
;

commit;

--------------------------------------Viewing Summary Stats------------------------------------------------------

---Sport Summaries----
---Total Sport Viewing (Mins and Progs) by channel - aggregated from main viewing data
--dbarnett.v250_all_sports_programmes_viewed_deduped 
--select * from v250_channel_to_service_key_lookup_deduped ;

--select distinct channel_name from v250_channel_to_service_key_lookup_deduped order by channel_name ;

---Summary by Sub Genre----
--add in if programme viewed (10/15 minute definition)
--drop table dbarnett.v250_summary_sport_viewing_by_account_for_profiling; drop table dbarnett.v250_all_sports_by_sub_genre; commit;

alter table dbarnett.v250_all_sports_programmes_viewed_deduped add programme_viewed tinyint;

update dbarnett.v250_all_sports_programmes_viewed_deduped
set programme_viewed=case when a.sub_genre_description in 

('Athletics'
,'American Football'
,'Baseball'
,'Basketball'
,'Boxing'
,'Fishing'
,'Ice Hockey'
,'Motor Sport'
,'Wrestling') and viewing_duration_total>=600 then 1 
when a.sub_genre_description not in 

('Athletics'
,'American Football'
,'Baseball'
,'Basketball'
,'Boxing'
,'Fishing'
,'Ice Hockey'
,'Motor Sport'
,'Wrestling') and viewing_duration_total>=900 then 1  
else 0 end
from dbarnett.v250_all_sports_programmes_viewed_deduped as a
;
commit;

select a.account_number
,sum(case when b.channel_name='Sky Sports 1' then viewing_duration_total else 0 end) as seconds_viewed_sky_sports_1
,sum(case when b.channel_name='Sky Sports 1' then programme_viewed else 0 end) as programmes_viewed_sky_sports_1

,sum(case when b.channel_name='Sky Sports 2' then viewing_duration_total else 0 end) as seconds_viewed_sky_sports_2
,sum(case when b.channel_name='Sky Sports 2' then programme_viewed else 0 end) as programmes_viewed_sky_sports_2

,sum(case when b.channel_name='Sky Sports 3' then viewing_duration_total else 0 end) as seconds_viewed_sky_sports_3
,sum(case when b.channel_name='Sky Sports 3' then programme_viewed else 0 end) as programmes_viewed_sky_sports_3

,sum(case when b.channel_name='Sky Sports 4' then viewing_duration_total else 0 end) as seconds_viewed_sky_sports_4
,sum(case when b.channel_name='Sky Sports 4' then programme_viewed else 0 end) as programmes_viewed_sky_sports_4

,sum(case when b.channel_name='Sky Sports F1' then viewing_duration_total else 0 end) as seconds_viewed_sky_sports_F1
,sum(case when b.channel_name='Sky Sports F1' then programme_viewed else 0 end) as programmes_viewed_sky_sports_F1

,sum(case when left(b.channel_name,3)='Sky' and 
b.channel_name not in ('Sky Sports 1','Sky Sports 2','Sky Sports 3','Sky Sports 4', 'Sky Sports F1') 
then viewing_duration_total else 0 end) as seconds_viewed_sport_other_sky
,sum(case  when left(b.channel_name,3)='Sky' and 
b.channel_name not in ('Sky Sports 1','Sky Sports 2','Sky Sports 3','Sky Sports 4', 'Sky Sports F1') 
then programme_viewed else 0 end) as programmes_viewed_sport_other_sky

,sum(case when left(b.channel_name,3)='BBC' 
then viewing_duration_total else 0 end) as seconds_viewed_sport_BBC
,sum(case  when left(b.channel_name,3)='BBC'
then programme_viewed else 0 end) as programmes_viewed_sport_BBC

,sum(case when left(b.channel_name,3)='ITV' 
then viewing_duration_total else 0 end) as seconds_viewed_sport_ITV
,sum(case  when left(b.channel_name,3)='ITV'
then programme_viewed else 0 end) as programmes_viewed_sport_ITV

,sum(case when left(b.channel_name,9)='Channel 4' 
then viewing_duration_total else 0 end) as seconds_viewed_sport_CH4
,sum(case  when left(b.channel_name,9)='Channel 4'
then programme_viewed else 0 end) as programmes_viewed_sport_CH4

,sum(case when left(b.channel_name,9)='Channel 5' 
then viewing_duration_total else 0 end) as seconds_viewed_sport_CH5
,sum(case  when left(b.channel_name,9)='Channel 5'
then programme_viewed else 0 end) as programmes_viewed_sport_CH5


,sum(case when left(b.channel_name,4)='ESPN' 
then viewing_duration_total else 0 end) as seconds_viewed_sport_ESPN
,sum(case  when left(b.channel_name,4)='ESPN'
then programme_viewed else 0 end) as programmes_viewed_sport_ESPN


,sum(case when left(b.channel_name,8)='BT Sport' 
then viewing_duration_total else 0 end) as seconds_viewed_sport_BT_Sport
,sum(case  when left(b.channel_name,8)='BT Sport'
then programme_viewed else 0 end) as programmes_viewed_sport_BT_Sport

,sum(case when left(b.channel_name,9)='Eurosport' 
then viewing_duration_total else 0 end) as seconds_viewed_sport_Eurosport
,sum(case  when left(b.channel_name,9)='Europort'
then programme_viewed else 0 end) as programmes_viewed_sport_Eurosport



,sum( viewing_duration_total) as seconds_viewed_sport
,count(*) as programmes_viewed_sport

into dbarnett.v250_summary_sport_viewing_by_account_for_profiling
from dbarnett.v250_all_sports_programmes_viewed_deduped  as a
left outer join v250_channel_to_service_key_lookup_deduped as b 
on a.service_key = b.service_key
left outer join dbarnett.v250_master_account_list_with_weight as c
on a.account_number =c.account_number
where c.account_number is not null
group by a.account_number
;
commit;


select a.account_number
,sum(case when sub_genre_description='American Football' then viewing_duration_total else 0 end) as seconds_viewed_American_Football
,sum(case when sub_genre_description='Athletics' then viewing_duration_total else 0 end) as seconds_viewed_Athletics
,sum(case when sub_genre_description='Baseball' then viewing_duration_total else 0 end) as seconds_viewed_Baseball
,sum(case when sub_genre_description='Basketball' then viewing_duration_total else 0 end) as seconds_viewed_Basketball
,sum(case when sub_genre_description='Boxing' then viewing_duration_total else 0 end) as seconds_viewed_Boxing
,sum(case when sub_genre_description='Cricket' then viewing_duration_total else 0 end) as seconds_viewed_Cricket

,sum(case when sub_genre_description='Darts' then viewing_duration_total else 0 end) as seconds_viewed_Darts
,sum(case when sub_genre_description='Equestrian' then viewing_duration_total else 0 end) as seconds_viewed_Equestrian
,sum(case when sub_genre_description='Extreme' then viewing_duration_total else 0 end) as seconds_viewed_Extreme
,sum(case when sub_genre_description='Fishing' then viewing_duration_total else 0 end) as seconds_viewed_Fishing
,sum(case when sub_genre_description='Football' then viewing_duration_total else 0 end) as seconds_viewed_Football
,sum(case when sub_genre_description='Golf' then viewing_duration_total else 0 end) as seconds_viewed_Golf
,sum(case when sub_genre_description='Ice Hockey' then viewing_duration_total else 0 end) as seconds_viewed_Ice_Hockey

,sum(case when sub_genre_description='Motor Sport' then viewing_duration_total else 0 end) as seconds_viewed_Motor_Sport
,sum(case when sub_genre_description='Racing' then viewing_duration_total else 0 end) as seconds_viewed_Racing
,sum(case when sub_genre_description='Rugby' then viewing_duration_total else 0 end) as seconds_viewed_Rugby
,sum(case when sub_genre_description='Snooker/Pool' then viewing_duration_total else 0 end) as seconds_viewed_Snooker_Pool
,sum(case when sub_genre_description='Tennis' then viewing_duration_total else 0 end) as seconds_viewed_Tennis
,sum(case when sub_genre_description='Watersports' then viewing_duration_total else 0 end) as seconds_viewed_Watersports
,sum(case when sub_genre_description='Wintersports' then viewing_duration_total else 0 end) as seconds_viewed_Wintersports
,sum(case when sub_genre_description='Wrestling' then viewing_duration_total else 0 end) as seconds_viewed_Wrestling

,sum(case when sub_genre_description='American Football' then programme_viewed else 0 end) as programmes_viewed_American_Football
,sum(case when sub_genre_description='Athletics' then programme_viewed else 0 end) as programmes_viewed_Athletics
,sum(case when sub_genre_description='Baseball' then programme_viewed else 0 end) as programmes_viewed_Baseball
,sum(case when sub_genre_description='Basketball' then programme_viewed else 0 end) as programmes_viewed_Basketball
,sum(case when sub_genre_description='Boxing' then programme_viewed else 0 end) as programmes_viewed_Boxing
,sum(case when sub_genre_description='Cricket' then programme_viewed else 0 end) as programmes_viewed_Cricket

,sum(case when sub_genre_description='Darts' then programme_viewed else 0 end) as programmes_viewed_Darts
,sum(case when sub_genre_description='Equestrian' then programme_viewed else 0 end) as programmes_viewed_Equestrian
,sum(case when sub_genre_description='Extreme' then programme_viewed else 0 end) as programmes_viewed_Extreme
,sum(case when sub_genre_description='Fishing' then programme_viewed else 0 end) as programmes_viewed_Fishing
,sum(case when sub_genre_description='Football' then programme_viewed else 0 end) as programmes_viewed_Football
,sum(case when sub_genre_description='Golf' then programme_viewed else 0 end) as programmes_viewed_Golf
,sum(case when sub_genre_description='Ice Hockey' then programme_viewed else 0 end) as programmes_viewed_Ice_Hockey

,sum(case when sub_genre_description='Motor Sport' then programme_viewed else 0 end) as programmes_viewed_Motor_Sport
,sum(case when sub_genre_description='Racing' then programme_viewed else 0 end) as programmes_viewed_Racing
,sum(case when sub_genre_description='Rugby' then programme_viewed else 0 end) as programmes_viewed_Rugby
,sum(case when sub_genre_description='Snooker/Pool' then programme_viewed else 0 end) as programmes_viewed_Snooker_Pool
,sum(case when sub_genre_description='Tennis' then programme_viewed else 0 end) as programmes_viewed_Tennis
,sum(case when sub_genre_description='Watersports' then programme_viewed else 0 end) as programmes_viewed_Watersports
,sum(case when sub_genre_description='Wintersports' then programme_viewed else 0 end) as programmes_viewed_Wintersports
,sum(case when sub_genre_description='Wrestling' then programme_viewed else 0 end) as programmes_viewed_Wrestling

into dbarnett.v250_all_sports_by_sub_genre
from dbarnett.v250_all_sports_programmes_viewed_deduped  as a
left outer join dbarnett.v250_master_account_list_with_weight as c
on a.account_number =c.account_number
where c.account_number is not null
group by a.account_number
;
commit;

---Add Viewing Metrics on to Table---
alter table dbarnett.v250_Account_profiling add annualised_minutes_total_tv_viewed real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_sports_tv_viewed real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_pay_tv_viewed real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_pay_sports_tv_viewed real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_sky_sports_tv_viewed real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_sky_movies_tv_viewed real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_pay_ents_tv_viewed real;

alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Sky_Sports_1 real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Sky_Sports_2 real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Sky_Sports_3 real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Sky_Sports_4 real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Sky_Sports_F1 real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Total_Sky_Sports real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Other_Sky_Channels real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_BBC real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_ITV real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_C4 real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_C5 real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_ESPN real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_BT_Sport real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Eurosport real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Other real;

alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_American_Football real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Athletics real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Baseball real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Basketball real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Boxing real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Cricket real;

alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Darts real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Equestrian real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Extreme real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Fishing real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Football real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Golf real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Ice_Hockey real;

alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Motor_Sport real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Racing real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Rugby real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Snooker_Pool real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Tennis real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Watersports real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Wintersports real;
alter table dbarnett.v250_Account_profiling add annualised_minutes_viewed_Wrestling real;

alter table dbarnett.v250_Account_profiling add Distinct_sport_subgenres_viewed integer;
alter table dbarnett.v250_Account_profiling add Distinct_sport_rights_viewed integer;

alter table dbarnett.v250_Account_profiling add annualised_programmes_sports_tv_viewed real;

alter table dbarnett.v250_Account_profiling add annualised_programmes_pay_sports_tv_viewed real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_sky_sports_tv_viewed real;



alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Sky_Sports_1 real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Sky_Sports_2 real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Sky_Sports_3 real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Sky_Sports_4 real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Sky_Sports_F1 real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Total_Sky_Sports real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Other_Sky_Channels real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_BBC real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_ITV real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_C4 real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_C5 real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_ESPN real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_BT_Sport real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Eurosport real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Other real;

alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_American_Football real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Athletics real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Baseball real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Basketball real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Boxing real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Cricket real;

alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Darts real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Equestrian real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Extreme real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Fishing real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Football real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Golf real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Ice_Hockey real;

alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Motor_Sport real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Racing real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Rugby real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Snooker_Pool real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Tennis real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Watersports real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Wintersports real;
alter table dbarnett.v250_Account_profiling add annualised_programmes_viewed_Wrestling real;
alter table dbarnett.v250_Account_profiling add total_days_with_viewing integer;

update dbarnett.v250_Account_profiling
set total_days_with_viewing=b.total_days_with_viewing
from dbarnett.v250_Account_profiling as a
left outer join dbarnett.v250_master_account_list as b
on a.account_number =b.account_number
;
--select count(*) from dbarnett.v250_master_account_list
update dbarnett.v250_Account_profiling
set annualised_minutes_viewed_Sky_Sports_1=cast(seconds_viewed_Sky_Sports_1 as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_Sky_Sports_2=cast(seconds_viewed_Sky_Sports_2 as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_Sky_Sports_3=cast(seconds_viewed_Sky_Sports_3 as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_Sky_Sports_4=cast(seconds_viewed_Sky_Sports_4 as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_Sky_Sports_F1=cast(seconds_viewed_Sky_Sports_F1 as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_Total_Sky_Sports=cast(seconds_viewed_Sky_Sports_1+seconds_viewed_Sky_Sports_2+seconds_viewed_Sky_Sports_3+seconds_viewed_Sky_Sports_4+seconds_viewed_Sky_Sports_F1 as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_Other_Sky_Channels=cast(seconds_viewed_sport_other_sky as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_BBC=cast(seconds_viewed_sport_BBC as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_ITV=cast(seconds_viewed_sport_ITV as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_C4=cast(seconds_viewed_sport_CH4 as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_C5=cast(seconds_viewed_sport_CH5 as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_ESPN=cast(seconds_viewed_sport_ESPN as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_BT_Sport=cast(seconds_viewed_sport_BT_Sport as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_Eurosport=cast(seconds_viewed_sport_Eurosport as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_Other=cast(seconds_viewed_sport-
seconds_viewed_sport_Eurosport-seconds_viewed_sport_BT_Sport-seconds_viewed_sport_ESPN-seconds_viewed_sport_CH5
-seconds_viewed_sport_CH4-seconds_viewed_sport_ITV-seconds_viewed_sport_BBC-seconds_viewed_sport_other_sky-
seconds_viewed_Sky_Sports_1-seconds_viewed_Sky_Sports_2-seconds_viewed_Sky_Sports_3-seconds_viewed_Sky_Sports_4-seconds_viewed_Sky_Sports_F1 as real)
/60*(365/cast(total_days_with_viewing as real))

,annualised_minutes_viewed_American_Football=cast(seconds_viewed_American_Football as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_Athletics=cast(seconds_viewed_Athletics as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_Baseball=cast(seconds_viewed_Baseball as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_Basketball=cast(seconds_viewed_Basketball as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_Boxing=cast(seconds_viewed_Boxing as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_Cricket=cast(seconds_viewed_Cricket as real)/60*(365/cast(total_days_with_viewing as real))

,annualised_minutes_viewed_Darts=cast(seconds_viewed_Darts as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_Equestrian=cast(seconds_viewed_Equestrian as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_Extreme=cast(seconds_viewed_Extreme as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_Fishing=cast(seconds_viewed_Fishing as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_Football=cast(seconds_viewed_Football as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_Golf=cast(seconds_viewed_Golf as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_Ice_Hockey=cast(seconds_viewed_Ice_Hockey as real)/60*(365/cast(total_days_with_viewing as real))

,annualised_minutes_viewed_Motor_Sport=cast(seconds_viewed_Motor_Sport as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_Racing=cast(seconds_viewed_Racing as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_Rugby=cast(seconds_viewed_Rugby as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_Snooker_Pool=cast(seconds_viewed_Snooker_Pool as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_Tennis=cast(seconds_viewed_Tennis as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_Watersports=cast(seconds_viewed_Watersports as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_Wintersports=cast(seconds_viewed_Wintersports as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_viewed_Wrestling=cast(seconds_viewed_Wrestling as real)/60*(365/cast(total_days_with_viewing as real))




,annualised_programmes_viewed_Sky_Sports_1=cast(programmes_viewed_Sky_Sports_1 as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_Sky_Sports_2=cast(programmes_viewed_Sky_Sports_2 as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_Sky_Sports_3=cast(programmes_viewed_Sky_Sports_3 as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_Sky_Sports_4=cast(programmes_viewed_Sky_Sports_4 as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_Sky_Sports_F1=cast(programmes_viewed_Sky_Sports_F1 as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_Total_Sky_Sports=cast(programmes_viewed_Sky_Sports_1+programmes_viewed_Sky_Sports_2+programmes_viewed_Sky_Sports_3+programmes_viewed_Sky_Sports_4+programmes_viewed_Sky_Sports_F1 as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_Other_Sky_Channels=cast(programmes_viewed_sport_other_sky as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_BBC=cast(programmes_viewed_sport_BBC as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_ITV=cast(programmes_viewed_sport_ITV as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_C4=cast(programmes_viewed_sport_CH4 as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_C5=cast(programmes_viewed_sport_CH5 as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_ESPN=cast(programmes_viewed_sport_ESPN as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_BT_Sport=cast(programmes_viewed_sport_BT_Sport as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_Eurosport=cast(programmes_viewed_sport_Eurosport as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_Other=cast(programmes_viewed_sport-
programmes_viewed_sport_Eurosport-programmes_viewed_sport_BT_Sport-programmes_viewed_sport_ESPN-programmes_viewed_sport_CH5
-programmes_viewed_sport_CH4-programmes_viewed_sport_ITV-programmes_viewed_sport_BBC-programmes_viewed_sport_other_sky-
programmes_viewed_Sky_Sports_1-programmes_viewed_Sky_Sports_2-programmes_viewed_Sky_Sports_3-programmes_viewed_Sky_Sports_4-programmes_viewed_Sky_Sports_F1 as real)*(365/cast(total_days_with_viewing as real))

,annualised_programmes_viewed_American_Football=cast(programmes_viewed_American_Football as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_Athletics=cast(programmes_viewed_Athletics as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_Baseball=cast(programmes_viewed_Baseball as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_Basketball=cast(programmes_viewed_Basketball as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_Boxing=cast(programmes_viewed_Boxing as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_Cricket=cast(programmes_viewed_Cricket as real)*(365/cast(total_days_with_viewing as real))

,annualised_programmes_viewed_Darts=cast(programmes_viewed_Darts as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_Equestrian=cast(programmes_viewed_Equestrian as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_Extreme=cast(programmes_viewed_Extreme as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_Fishing=cast(programmes_viewed_Fishing as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_Football=cast(programmes_viewed_Football as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_Golf=cast(programmes_viewed_Golf as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_Ice_Hockey=cast(programmes_viewed_Ice_Hockey as real)*(365/cast(total_days_with_viewing as real))

,annualised_programmes_viewed_Motor_Sport=cast(programmes_viewed_Motor_Sport as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_Racing=cast(programmes_viewed_Racing as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_Rugby=cast(programmes_viewed_Rugby as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_Snooker_Pool=cast(programmes_viewed_Snooker_Pool as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_Tennis=cast(programmes_viewed_Tennis as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_Watersports=cast(programmes_viewed_Watersports as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_Wintersports=cast(programmes_viewed_Wintersports as real)*(365/cast(total_days_with_viewing as real))
,annualised_programmes_viewed_Wrestling=cast(programmes_viewed_Wrestling as real)*(365/cast(total_days_with_viewing as real))


from dbarnett.v250_Account_profiling as a
left outer join dbarnett.v250_summary_sport_viewing_by_account_for_profiling as b
on a.account_number =b.account_number
left outer join dbarnett.v250_all_sports_by_sub_genre as c
on a.account_number =c.account_number

;
commit;

--select top 100 * from dbarnett.v250_master_account_list
update dbarnett.v250_Account_profiling
set annualised_minutes_total_tv_viewed=cast(total_viewing_duration_all as real)/60*(365/cast(a.total_days_with_viewing as real))
,annualised_minutes_sports_tv_viewed=cast(total_viewing_duration_sports as real)/60*(365/cast(a.total_days_with_viewing as real))
from dbarnett.v250_Account_profiling as a
left outer join dbarnett.v250_master_account_list as b
on a.account_number = b.account_number
;
commit;

--grant all on dbarnett.v250_Account_profiling to public;
alter table v250_channel_to_service_key_lookup_deduped add pay integer;
alter table v250_channel_to_service_key_lookup_deduped add ent_channel integer;
--select top 100 *  from vespa_analysts.CHANNEL_MAP_prod_SERVICE_KEY_ATTRIBUTES
--drop table #pay_flag;
select service_key
,max(case when upper(Pay_free_indicator)='PAY' then 1 else 0 end) as pay
,max(case when channel_genre in ('Sports','Movies') then 0 else 1 end) as ent_channel
into #pay_flag
from vespa_analysts.CHANNEL_MAP_prod_SERVICE_KEY_ATTRIBUTES
group by service_key
;


update v250_channel_to_service_key_lookup_deduped
set pay=case when b.pay is null then 0 else b.pay end
,ent_channel=case when b.ent_channel is null then 0 else b.ent_channel end
from v250_channel_to_service_key_lookup_deduped as a
left outer join #pay_flag as b
on a.service_key=b.service_key
;
commit;

alter table v250_channel_to_service_key_lookup_deduped add grouped_channel varchar(40);

update v250_channel_to_service_key_lookup_deduped
set grouped_channel=case when b.channel_name_inc_hd_staggercast_channel_families is null then a.channel_name else b.channel_name_inc_hd_staggercast_channel_families end
from v250_channel_to_service_key_lookup_deduped as a
left outer join v200_channel_lookup_with_channel_family as b
on a.channel_name=b.channel_name
;
commit;


update v250_channel_to_service_key_lookup_deduped
set grouped_channel=case when grouped_channel in ('Sky Disney','Sky Greats','Sky Movies','Sky SciFi/Horror') then 'Sky Movies Channels'  else grouped_channel end
from v250_channel_to_service_key_lookup_deduped as a
;
commit;

---
--drop table #acc_summary;
Select account_number
,sum(case when pay=1 then viewing_duration else 0 end) as seconds_viewed_pay
,sum(case when pay=1 and grouped_channel='Sky Movies Channels' then viewing_duration else 0 end) as seconds_viewed_pay_movies
,sum(case when pay=1 and ent_channel=0 and grouped_channel<>'Sky Movies Channels' then viewing_duration else 0 end) as seconds_viewed_pay_sports
,sum(case when pay=1 and ent_channel=1 then viewing_duration else 0 end) as seconds_viewed_pay_ent
into #acc_summary
from dbarnett.v250_viewing_by_account_and_channel as a
left outer join v250_channel_to_service_key_lookup_deduped as b
on a.service_key=b.service_key
group by account_number
;
commit;
--alter table dbarnett.v250_Account_profiling add annualised_minutes_pay_tv_viewed real;
--alter table dbarnett.v250_Account_profiling add annualised_minutes_pay_sports_tv_viewed real;
--alter table dbarnett.v250_Account_profiling add annualised_minutes_sky_movies_tv_viewed real;
--alter table dbarnett.v250_Account_profiling add annualised_minutes_pay_ents_tv_viewed real;

update dbarnett.v250_Account_profiling
set annualised_minutes_pay_tv_viewed=cast(seconds_viewed_pay as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_pay_sports_tv_viewed=cast(seconds_viewed_pay_sports as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_sky_movies_tv_viewed=cast(seconds_viewed_pay_movies as real)/60*(365/cast(total_days_with_viewing as real))
,annualised_minutes_pay_ents_tv_viewed=cast(seconds_viewed_pay_ent as real)/60*(365/cast(total_days_with_viewing as real))
from dbarnett.v250_Account_profiling as a
left outer join #acc_summary as b
on a.account_number=b.account_number
;
commit;

--select top 500 * from dbarnett.v250_Account_profiling;

alter table dbarnett.v250_Account_profiling delete child_age_0_4;
alter table dbarnett.v250_Account_profiling delete child_age_5_11;
alter table dbarnett.v250_Account_profiling delete child_age_12_17;
alter table dbarnett.v250_Account_profiling delete vespa_panel;
alter table dbarnett.v250_Account_profiling delete annualised_minutes_sky_sports_tv_viewed;
alter table dbarnett.v250_Account_profiling delete annualised_programmes_sports_tv_viewed;
alter table dbarnett.v250_Account_profiling delete annualised_programmes_pay_sports_tv_viewed;
alter table dbarnett.v250_Account_profiling delete annualised_programmes_sky_sports_tv_viewed;
commit;

---Distinct rights Viewed---
select account_number
,count(distinct analysis_right) as distinct_rights
into #distinct_rights
from dbarnett.v250_sports_rights_viewed_by_right_overall
where analysis_right not in ('UEFA Champions League -  Sky Sports','Premier League Football - Sky Sports','F1 - BBC','F1 - Sky Sports','ECB Cricket Sky Sports' )
group by account_number
; 
--Distinct Sub Genres
select account_number
,count(distinct sub_genre_description) as distinct_sub_genres
into #distinct_sub_genres
from dbarnett.v250_all_sports_programmes_viewed_deduped
group by account_number
; 

update dbarnett.v250_Account_profiling
set Distinct_sport_rights_viewed=case when b.distinct_rights is null then 0 else b.distinct_rights end 
from dbarnett.v250_Account_profiling as a
left outer join #distinct_rights as b
on a.account_number=b.account_number
;
commit;


update dbarnett.v250_Account_profiling
set Distinct_sport_subgenres_viewed=case when b.distinct_sub_genres is null then 0 else b.distinct_sub_genres end 
from dbarnett.v250_Account_profiling as a
left outer join #distinct_sub_genres as b
on a.account_number=b.account_number
;
commit;

alter table dbarnett.v250_Account_profiling add bt_sport_viewer tinyint;
update dbarnett.v250_Account_profiling
set bt_sport_viewer=case when annualised_minutes_viewed_bt_sport>0 then 1 else 0 end
from dbarnett.v250_Account_profiling
;
commit;

-----Add In Survey Flag---
--Survey Data loaded in using \Git\Vespa\ad_hoc\V250 - Sports Rights Analysis\V250 - Load Survey Data (winscp).sql
--dbarnett.v250_sports_rights_survey_responses_winscp
----Add Response Flag on to Profiling Table--
alter table dbarnett.v250_Account_profiling add survey_responder tinyint;
update dbarnett.v250_Account_profiling
set survey_responder=case when b.ID_name is not null then 1 else 0 end
from dbarnett.v250_Account_profiling as a
left outer join dbarnett.v250_sports_rights_survey_responses_winscp as b
on a.account_number = b.ID_name
;

commit;

alter table dbarnett.v250_Account_profiling add survey_interest_watching_sports tinyint;
update dbarnett.v250_Account_profiling
set survey_interest_watching_sports=case when q20_c in ('6','7','8','9','10') then 1 else 0 end
from dbarnett.v250_Account_profiling as a
left outer join dbarnett.v250_sports_rights_survey_responses_winscp as b
on a.account_number = b.ID_name
;

commit;

alter table dbarnett.v250_Account_profiling add asked_follow_up_sports_rights_questions tinyint;

update dbarnett.v250_Account_profiling
set asked_follow_up_sports_rights_questions=case when q20_c ='' then 0 when q20_c in ('1','2','3','4','5') and q21 in ('Nobody in the household watches sports','Someone else is the main sports viewer in the household') then 0 else 1 end
from dbarnett.v250_Account_profiling as a
left outer join dbarnett.v250_sports_rights_survey_responses_winscp as b
on a.account_number = b.ID_name
;
commit;


---Add on Total Numebr of Sports programmes viewed
--select distinct analysis_right from dbarnett.v250_sports_rights_viewed_by_right_overall order by analysis_right
select account_number
,sum(total_programmes_viewed_over_threshold) as total_sports_programmes_viewed_over_threshold
into #total_sports_programmes_viewed
from dbarnett.v250_sports_rights_viewed_by_right_overall
where analysis_right not in ('ECB Cricket Sky Sports'
,'England Football Internationals - ITV'
,'F1 - BBC'
,'F1 - Sky Sports'
,'Premier League Football - Sky Sports'
,'UEFA Champions League -  Sky Sports')
group by account_number
;
--select total_sports_programmes_viewed_over_threshold , count(*) from #total_sports_programmes_viewed group by total_sports_programmes_viewed_over_threshold order by total_sports_programmes_viewed_over_threshold
--select round(annualised_total_sports_programmes_viewed_over_threshold,0) as vals , count(*) from dbarnett.v250_Account_profiling group by vals order by vals

alter table dbarnett.v250_Account_profiling add annualised_total_sports_programmes_viewed_over_threshold real;
update dbarnett.v250_Account_profiling
set annualised_total_sports_programmes_viewed_over_threshold=total_sports_programmes_viewed_over_threshold*(365/cast(total_days_with_viewing as real))
from dbarnett.v250_Account_profiling as a
left outer join #total_sports_programmes_viewed as b
on a.account_number = b.account_number
;

commit;


---Account Activity
--Details Split Nov 2012 - Oct 2013 and Nov 2013 - Feb 2014 Inclusive---

-----Churn Events---

--drop table #all_churn_records_during_and_post_analysis_period;
select  account_number
        ,sum(case when status_code in ('PO') and effective_from_dt between '2012-11-01' and '2013-10-31' then 1 else 0 end) as cuscan_events_201211_to_201310
        ,sum(case when status_code in ('PO') and effective_from_dt between '2013-11-01' and '2014-02-28' then 1 else 0 end) as cuscan_events_201311_to_201402
        ,sum(case when status_code in ('SC') and effective_from_dt between '2012-11-01' and '2013-10-31' then 1 else 0 end) as syscan_events_201211_to_201310
        ,sum(case when status_code in ('SC') and effective_from_dt between '2013-11-01' and '2014-02-28' then 1 else 0 end) as syscan_events_201311_to_201402
  into #all_churn_records_during_and_post_analysis_period
  from sk_prod.cust_subs_hist as csh
 where subscription_sub_type ='DTV Primary Viewing'     --DTV stack
   and status_code in ('PO','SC')                       --CUSCAN and SYSCAN status codes
   and prev_status_code in ('AC','AB','PC')             --Previously ACTIVE
   and status_code_changed = 'Y'
   and effective_from_dt != effective_to_dt
and effective_from_dt >= '2012-11-01'
group by account_number
;


commit;
CREATE HG INDEX idx1 ON #all_churn_records_during_and_post_analysis_period(account_number);

alter table dbarnett.v250_Account_profiling add churn_events_201211_to_201310 integer;
alter table dbarnett.v250_Account_profiling add cuscan_events_201211_to_201310 integer;
alter table dbarnett.v250_Account_profiling add syscan_events_201211_to_201310 integer;
alter table dbarnett.v250_Account_profiling add churn_flag_201211_to_201310 integer;
alter table dbarnett.v250_Account_profiling add churn_events_201311_to_201402 integer;
alter table dbarnett.v250_Account_profiling add cuscan_events_201311_to_201402 integer;
alter table dbarnett.v250_Account_profiling add syscan_events_201311_to_201402 integer;
alter table dbarnett.v250_Account_profiling add churn_flag_201311_to_201402 integer;

update dbarnett.v250_Account_profiling
set churn_events_201211_to_201310=case when b.cuscan_events_201211_to_201310 is null then 0 else b.cuscan_events_201211_to_201310+ b.syscan_events_201211_to_201310  end
, cuscan_events_201211_to_201310=case when b.cuscan_events_201211_to_201310 is null then 0 else b.cuscan_events_201211_to_201310 end
, syscan_events_201211_to_201310=case when  b.syscan_events_201211_to_201310  is null then 0 else b.syscan_events_201211_to_201310  end
, churn_flag_201211_to_201310=case when b.cuscan_events_201211_to_201310 is null then 0  when b.cuscan_events_201211_to_201310+ b.syscan_events_201211_to_201310>0 then 1 else 0   end

,churn_events_201311_to_201402=case when b.cuscan_events_201311_to_201402 is null then 0 else b.cuscan_events_201311_to_201402+ b.syscan_events_201311_to_201402  end
, cuscan_events_201311_to_201402=case when b.cuscan_events_201311_to_201402 is null then 0 else b.cuscan_events_201311_to_201402 end
, syscan_events_201311_to_201402=case when  b.syscan_events_201311_to_201402  is null then 0 else b.syscan_events_201311_to_201402  end
, churn_flag_201311_to_201402=case when b.cuscan_events_201311_to_201402 is null then 0  when b.cuscan_events_201311_to_201402+ b.syscan_events_201311_to_201402>0 then 1 else 0 end
from dbarnett.v250_Account_profiling as a
left outer join #all_churn_records_during_and_post_analysis_period as b
on a.account_number = b.account_number
;

commit;

----Turnaround Activity---
SELECT      cca.account_number
            ,(CASE WHEN cca.Wh_Attempt_Outcome_Description_1 IN ( 'Turnaround Saved'
                                                                 ,'Legacy Save'
                                                                 ,'Home Move Saved'
                                                                 ,'Home Move Accept Saved')
            then 'b)TA_SAVED' else 'a)TA_FAILED' end) as ta_outcome   
,case when  attempt_date between '2012-11-01' and '2013-10-31'  then 'a) analysis period'
when  attempt_date between '2013-11-01' and '2014-02-28'    then 'b) post analysis period' else 'c) other' end as event_period
INTO        #V250_TA
FROM        sk_prod.cust_change_attempt AS cca
inner join  sk_prod.cust_subscriptions AS subs
ON          cca.subscription_id = subs.subscription_id
WHERE       cca.change_attempt_type                  = 'CANCELLATION ATTEMPT'
AND         subs.ph_subs_subscription_sub_type       = 'DTV Primary Viewing'
AND         cca.attempt_date                           >= '2012-11-01'
AND         cca.attempt_date                           <= '2014-02-28' 
AND         cca.created_by_id  NOT IN ('dpsbtprd', 'batchuser')
AND         cca.Wh_Attempt_Outcome_Description_1 in 
            ('Turnaround Saved','Legacy Save','Home Move Saved','Home Move Accept Saved','Turnaround Not Saved','Legacy Fail','Home Move Not Saved')
;

---Summary by Outcome---
--drop table #ta_account_summary;
select account_number
,sum(case when event_period = 'a) analysis period' then 1 else 0 end) as ta_events_201211_to_201310 
,sum(case when event_period = 'a) analysis period' and ta_outcome ='b)TA_SAVED' then 1 else 0 end) as ta_saved_events_201211_to_201310 
,sum(case when event_period = 'a) analysis period' and ta_outcome ='a)TA_FAILED' then 1 else 0 end) as ta_failed_events_201211_to_201310 


,sum(case when event_period = 'b) post analysis period' then 1 else 0 end) as ta_events_201311_to_201402 
,sum(case when event_period = 'b) post analysis period' and ta_outcome ='b)TA_SAVED' then 1 else 0 end) as ta_saved_events_201311_to_201402 
,sum(case when event_period = 'b) post analysis period' and ta_outcome ='a)TA_FAILED' then 1 else 0 end) as ta_failed_events_201311_to_201402 

into #ta_account_summary
from #V250_TA
group by account_number
;
commit;
alter table dbarnett.v250_Account_profiling add ta_events_201211_to_201310 integer;
alter table dbarnett.v250_Account_profiling add ta_saved_events_201211_to_201310 integer;
alter table dbarnett.v250_Account_profiling add ta_failed_events_201211_to_201310 integer;
alter table dbarnett.v250_Account_profiling add ta_flag_201211_to_201310 integer;
alter table dbarnett.v250_Account_profiling add ta_events_201311_to_201402 integer;
alter table dbarnett.v250_Account_profiling add ta_saved_events_201311_to_201402 integer;
alter table dbarnett.v250_Account_profiling add ta_failed_events_201311_to_201402 integer;
alter table dbarnett.v250_Account_profiling add ta_flag_201311_to_201402 integer;

update dbarnett.v250_Account_profiling
set ta_events_201211_to_201310=case when b.ta_saved_events_201211_to_201310 is null then 0 else b.ta_saved_events_201211_to_201310+ b.ta_failed_events_201211_to_201310  end
, ta_saved_events_201211_to_201310=case when b.ta_saved_events_201211_to_201310 is null then 0 else b.ta_saved_events_201211_to_201310 end
, ta_failed_events_201211_to_201310=case when  b.ta_failed_events_201211_to_201310  is null then 0 else b.ta_failed_events_201211_to_201310  end
, ta_flag_201211_to_201310=case when b.ta_saved_events_201211_to_201310 is null then 0  when b.ta_saved_events_201211_to_201310+ b.ta_failed_events_201211_to_201310>0 then 1 else 0   end

,ta_events_201311_to_201402=case when b.ta_saved_events_201311_to_201402 is null then 0 else b.ta_saved_events_201311_to_201402+ b.ta_failed_events_201311_to_201402  end
, ta_saved_events_201311_to_201402=case when b.ta_saved_events_201311_to_201402 is null then 0 else b.ta_saved_events_201311_to_201402 end
, ta_failed_events_201311_to_201402=case when  b.ta_failed_events_201311_to_201402  is null then 0 else b.ta_failed_events_201311_to_201402  end
, ta_flag_201311_to_201402=case when b.ta_saved_events_201311_to_201402 is null then 0  when b.ta_saved_events_201311_to_201402+ b.ta_failed_events_201311_to_201402>0 then 1 else 0 end
from dbarnett.v250_Account_profiling as a
left outer join #ta_account_summary as b
on a.account_number = b.account_number
;
--select  b.ta_saved_events_201211_to_201310 from #ta_account_summary as b ta_saved_events_201211_to_20310 
commit;

---Active Block Events--

--drop table  #V250_active_block_activity;
select          account_number
,sum(case when effective_from_dt  between '2012-11-01' and '2013-10-31' and status_code in ('AB') then 1 else 0 end) as active_block_events_201211_to_201310
,sum(case when effective_from_dt  between '2013-11-01' and '2014-02-28'  and status_code in ('AB') then 1 else 0 end) as active_block_events_201311_to_201402

,sum(case when effective_from_dt  between '2012-11-01' and '2013-10-31' and status_code in ('AC') then 1 else 0 end) as active_block_reactivated_events_201211_to_201310
,sum(case when effective_from_dt  between '2013-11-01' and '2014-02-28'  and status_code in ('AC') then 1 else 0 end) as active_block_reactivated_events_201311_to_201402

into            #V250_active_block_activity
from            sk_prod.cust_subs_hist as csh
where           subscription_sub_type ='DTV Primary Viewing'     
and            ( status_code in ('AB') or (prev_status_code in ('AB') and status_code='AC'))                      
and             status_code_changed = 'Y' 
and             effective_from_dt >= '2012-11-01'
and             effective_from_dt <= '2014-02-28'     
group by        account_number
;

commit;
alter table dbarnett.v250_Account_profiling add active_block_events_201211_to_201310 integer;
alter table dbarnett.v250_Account_profiling add active_block_reactivated_events_201211_to_201310 integer;
alter table dbarnett.v250_Account_profiling add active_block_flag_201211_to_201310 integer;
alter table dbarnett.v250_Account_profiling add active_block_events_201311_to_201402 integer;
alter table dbarnett.v250_Account_profiling add active_block_reactivated_events_201311_to_201402 integer;
alter table dbarnett.v250_Account_profiling add active_block_flag_201311_to_201402 integer;

update dbarnett.v250_Account_profiling
set active_block_events_201211_to_201310=case when b.active_block_events_201211_to_201310 is null then 0 else b.active_block_events_201211_to_201310  end
, active_block_reactivated_events_201211_to_201310=case when b.active_block_reactivated_events_201211_to_201310 is null then 0 else b.active_block_reactivated_events_201211_to_201310 end
, active_block_flag_201211_to_201310=case when b.active_block_reactivated_events_201211_to_201310 is null then 0  when b.active_block_events_201211_to_201310>0 then 1 else 0   end

,active_block_events_201311_to_201402=case when b.active_block_events_201311_to_201402 is null then 0 else b.active_block_events_201311_to_201402  end
, active_block_reactivated_events_201311_to_201402=case when b.active_block_reactivated_events_201311_to_201402 is null then 0 else b.active_block_reactivated_events_201311_to_201402 end
, active_block_flag_201311_to_201402=case when b.active_block_reactivated_events_201311_to_201402 is null then 0  when  b.active_block_events_201311_to_201402>0 then 1 else 0 end
from dbarnett.v250_Account_profiling as a
left outer join #V250_active_block_activity as b
on a.account_number = b.account_number
;
--select  b.active_block_reactivated_events_201211_to_201310 from #ta_account_summary as b ta_saved_events_201211_to_20310 
commit;

---Add Downgrade attempts
--drop table  #V250_PAT;
SELECT      cca.account_number 
,sum(case when  attempt_date between '2012-11-01' and '2013-10-31'  then 1 else 0 end) as downgrade_attempts_201211_to_201310 
,sum(case when  attempt_date between '2012-11-01' and '2013-10-31' and Wh_Attempt_Outcome_Description_1 in ('PAT Save') then 1 else 0 end) as downgrade_saved_201211_to_201310 
,sum(case when  attempt_date between '2012-11-01' and '2013-10-31' and Wh_Attempt_Outcome_Description_1 in ('PAT No Save','PAT Partial Save') then 1 else 0 end) as downgrade_failed_201211_to_201310 

,sum(case when  attempt_date between '2013-11-01' and '2014-02-28'  then 1 else 0 end) as downgrade_attempts_201311_to_201402
,sum(case when  attempt_date between '2013-11-01' and '2014-02-28' and Wh_Attempt_Outcome_Description_1 in ('PAT Save')  then 1 else 0 end) as downgrade_saved_201311_to_201402
,sum(case when  attempt_date between '2013-11-01' and '2014-02-28'  and Wh_Attempt_Outcome_Description_1 in ('PAT No Save','PAT Partial Save')  then 1 else 0 end) as downgrade_failed_201311_to_201402 
INTO        #V250_PAT
FROM        sk_prod.cust_change_attempt AS cca
inner join  sk_prod.cust_subscriptions AS subs
ON          cca.subscription_id = subs.subscription_id
WHERE       cca.change_attempt_type                  = 'DOWNGRADE ATTEMPT'
AND         subs.ph_subs_subscription_sub_type       = 'DTV Primary Viewing'
AND         cca.attempt_date                           >= '2012-11-01'
AND         cca.attempt_date                           <= '2014-02-28' 
and Wh_Attempt_Outcome_Description_1 IN ( 
                                                                 'PAT No Save'
                                                                 ,'PAT Save'
                                                                 ,'PAT Partial Save')
group by cca.account_number 
;

commit;

alter table dbarnett.v250_Account_profiling add downgrade_attempts_201211_to_201310 integer;
alter table dbarnett.v250_Account_profiling add downgrade_saved_201211_to_201310 integer;
alter table dbarnett.v250_Account_profiling add downgrade_failed_201211_to_201310 integer;
alter table dbarnett.v250_Account_profiling add downgrade_attempts_flag_201211_to_201310 integer;
alter table dbarnett.v250_Account_profiling add downgrade_attempts_201311_to_201402 integer;
alter table dbarnett.v250_Account_profiling add downgrade_saved_201311_to_201402 integer;
alter table dbarnett.v250_Account_profiling add downgrade_failed_201311_to_201402 integer;
alter table dbarnett.v250_Account_profiling add downgrade_attempts_flag_201311_to_201402 integer;

update dbarnett.v250_Account_profiling
set downgrade_attempts_201211_to_201310=case when b.downgrade_attempts_201211_to_201310 is null then 0 else b.downgrade_attempts_201211_to_201310  end
, downgrade_saved_201211_to_201310=case when b.downgrade_saved_201211_to_201310 is null then 0 else b.downgrade_saved_201211_to_201310 end
, downgrade_failed_201211_to_201310=case when b.downgrade_failed_201211_to_201310 is null then 0  when b.downgrade_failed_201211_to_201310>0 then 1 else 0   end
,downgrade_attempts_flag_201211_to_201310=case when b.downgrade_failed_201211_to_201310 >0 then 1  else 0  end
,downgrade_attempts_201311_to_201402=case when b.downgrade_attempts_201311_to_201402 is null then 0 else b.downgrade_attempts_201311_to_201402  end
, downgrade_saved_201311_to_201402=case when b.downgrade_saved_201311_to_201402 is null then 0 else b.downgrade_saved_201311_to_201402 end
, downgrade_failed_201311_to_201402=case when b.downgrade_failed_201311_to_201402 is null then 0  when  b.downgrade_failed_201311_to_201402>0 then 1 else 0 end
,downgrade_attempts_flag_201311_to_201402=case when b.downgrade_failed_201311_to_201402 >0 then 1  else 0  end
from dbarnett.v250_Account_profiling as a
left outer join  #V250_PAT as b
on a.account_number = b.account_number
;
commit;
--select churn_flag_201311_to_201402 , count(*) , sum(account_weight) from dbarnett.v250_Account_profiling group by churn_flag_201311_to_201402 order by churn_flag_201311_to_201402

---Broadband Cancellations---


select account_number
,max(case when status_code in ('CN','PA','PO') and effective_from_dt  between '2012-11-01' and '2013-10-31' then 1 else 0 end) as broadband_cancel_201211_to_201310
,max(case when status_code in ('CN','PA','PO') and effective_from_dt  between '2013-11-01' and '2014-02-28' then 1 else 0 end) as broadband_cancel_201311_to_201402
into #bb_churn
FROM            sk_prod.cust_subs_hist
WHERE           subscription_sub_type = 'Broadband DSL Line'
and             effective_from_dt  between '2013-11-01' and '2014-02-28' and status_code in ('CN','PA','PO')
group by account_number
;

alter table dbarnett.v250_Account_profiling add broadband_cancel_201211_to_201310 integer;
alter table dbarnett.v250_Account_profiling add broadband_cancel_201311_to_201402 integer;

update dbarnett.v250_Account_profiling
set broadband_cancel_201211_to_201310=case when b.broadband_cancel_201211_to_201310 is null then 0 else b.broadband_cancel_201211_to_201310  end
, broadband_cancel_201311_to_201402=case when b.broadband_cancel_201311_to_201402 is null then 0 else b.broadband_cancel_201311_to_201402 end
from dbarnett.v250_Account_profiling as a
left outer join  #bb_churn as b
on a.account_number = b.account_number
;

commit;
----Sports Rights by Genre

--drop table #pv_summary;
--drop table dbarnett.v250_rank_information;
select
      a.account_number
,     a.account_weight                                                                                                                                              
,     1.00* (           annualised_AMOTH_programmes_viewed_over_threshold_A                           
                  +     annualised_AMCH4_programmes_viewed_over_threshold_A                           
                  +     annualised_AMESPN_programmes_viewed_over_threshold_A                          
                  +     annualised_AMSS_programmes_viewed_over_threshold_A                            
                  +     annualised_NFLCH4_programmes_viewed_over_threshold_L                          
                  +     annualised_NFLSS_programmes_viewed_over_threshold_L                           
                  +     annualised_NFLCH4_programmes_viewed_over_threshold_N                          
                  +     annualised_NFLSS_programmes_viewed_over_threshold_N                           
                  +     annualised_AFBBC_programmes_viewed_over_threshold_A                           
                  +     annualised_AFBTS_programmes_viewed_over_threshold_A                           
                  +     annualised_NFLBBC_programmes_viewed_over_threshold_L                          
                  +     annualised_NFLBBC_programmes_viewed_over_threshold_N                          
                  +     annualised_AMEUR_programmes_viewed_over_threshold_A   )     /      1839  as    American_Football
,     1.00* (           annualised_ATHOTH_programmes_viewed_over_threshold_A                          
                  +     annualised_ATHBBC_programmes_viewed_over_threshold_A                          
                  +     annualised_ATHBTS_programmes_viewed_over_threshold_A                          
                  +     annualised_ATHCH4_programmes_viewed_over_threshold_A                          
                  +     annualised_ATHCH5_programmes_viewed_over_threshold_A                          
                  +     annualised_ATHESPN_programmes_viewed_over_threshold_A                         
                  +     annualised_ATHEUR_programmes_viewed_over_threshold_A                          
                  +     annualised_ATHSS_programmes_viewed_over_threshold_A                           
                  +     annualised_WACEUR_programmes_viewed_over_threshold_L                          
                  +     annualised_WACEUR_programmes_viewed_over_threshold_N  )     /      2137  as    Athletics
,     1.00* (           annualised_BASEOTH_programmes_viewed_over_threshold_A                         
                  +     annualised_BASEBTS_programmes_viewed_over_threshold_A                         
                  +     annualised_BASEESPN_programmes_viewed_over_threshold_A                              
                  +     annualised_BASESS_programmes_viewed_over_threshold_A                          
                  +     annualised_BASEEUR_programmes_viewed_over_threshold_A )     /      1400  as    Baseball
,     1.00* (           annualised_BASKOTH_programmes_viewed_over_threshold_A                         
                  +     annualised_BASKBTS_programmes_viewed_over_threshold_A                         
                  +     annualised_BASKESPN_programmes_viewed_over_threshold_A                              
                  +     annualised_BASKEUR_programmes_viewed_over_threshold_A                         
                  +     annualised_BASKSS_programmes_viewed_over_threshold_A                          
                  +     annualised_NBASS_programmes_viewed_over_threshold_L                           
                  +     annualised_NBASS_programmes_viewed_over_threshold_N   )     /      2609  as    Basketball
,     1.00* (           annualised_BOXOTH_programmes_viewed_over_threshold_A                          
                  +     annualised_BOXBTS_programmes_viewed_over_threshold_A                          
                  +     annualised_BOXCH4_programmes_viewed_over_threshold_A                          
                  +     annualised_BOXESPN_programmes_viewed_over_threshold_A                         
                  +     annualised_BOXEUR_programmes_viewed_over_threshold_A                          
                  +     annualised_BOXSS_programmes_viewed_over_threshold_A                           
                  +     annualised_BOXMSS_programmes_viewed_over_threshold_L                          
                  +     annualised_BOXMSS_programmes_viewed_over_threshold_N                          
                  +     annualised_BOXBBC_programmes_viewed_over_threshold_A                          
                  +     annualised_BOXOCH5_programmes_viewed_over_threshold_A                         
                  +     annualised_BOXITV1_programmes_viewed_over_threshold_A                         
                  +     annualised_BOXITV4_programmes_viewed_over_threshold_A                         
                  +     annualised_BOXS12_programmes_viewed_over_threshold_A                          
                  +     annualised_BOXCH5_programmes_viewed_over_threshold_L  )     /      8625  as    Boxing
,     1.00* (           annualised_CRIBTS_programmes_viewed_over_threshold_A                          
                  +     annualised_CRICH5_programmes_viewed_over_threshold_A                          
                  +     annualised_CRIESPN_programmes_viewed_over_threshold_A                         
                  +     annualised_CRIEUR_programmes_viewed_over_threshold_A                          
                  +     annualised_CRIITV4_programmes_viewed_over_threshold_A                         
                  +     annualised_CRIOTH_programmes_viewed_over_threshold_A                          
                  +     annualised_CRISS_programmes_viewed_over_threshold_A                           
                  +     annualised_AHCSS_programmes_viewed_over_threshold_L                           
                  +     annualised_ICCSS_programmes_viewed_over_threshold_L                           
                  +     annualised_ECBNSS_programmes_viewed_over_threshold_L                          
                  +     annualised_ECBTSS_programmes_viewed_over_threshold_L                          
                  +     annualised_IHCSS_programmes_viewed_over_threshold_L                           
                  +     annualised_IPLITV_programmes_viewed_over_threshold_L                          
                  +     annualised_SACSS_programmes_viewed_over_threshold_L                           
                  +     annualised_WICSS_programmes_viewed_over_threshold_L                           
                  +     annualised_WICCSS_programmes_viewed_over_threshold_L                          
                  +     annualised_AHCSS_programmes_viewed_over_threshold_N                           
                  +     annualised_ICCSS_programmes_viewed_over_threshold_N                           
                  +     annualised_ECBHCH5_programmes_viewed_over_threshold_N                         
                  +     annualised_ECBNSS_programmes_viewed_over_threshold_N                          
                  +     annualised_ECBTSS_programmes_viewed_over_threshold_N                          
                  +     annualised_IHCSS_programmes_viewed_over_threshold_N                           
                  +     annualised_IPLITV_programmes_viewed_over_threshold_N                          
                  +     annualised_SACSS_programmes_viewed_over_threshold_N                           
                  +     annualised_WICCSS_programmes_viewed_over_threshold_N  )     /      4958  as    Cricket
,     1.00* (           annualised_CTCITV_programmes_viewed_over_threshold_L                          
                  +     annualised_TDFEUR_programmes_viewed_over_threshold_L                          
                  +     annualised_TDFITV_programmes_viewed_over_threshold_L                          
                  +     annualised_CLVITV_programmes_viewed_over_threshold_N                          
                  +     annualised_CUCISS_programmes_viewed_over_threshold_N                          
                  +     annualised_CTBEUR_programmes_viewed_over_threshold_N                          
                  +     annualised_CTCITV_programmes_viewed_over_threshold_N                          
                  +     annualised_TDFEUR_programmes_viewed_over_threshold_N                          
                  +     annualised_TDFITV_programmes_viewed_over_threshold_N  )     /     306      as    Cycling
,     1.00* (           annualised_DRTBBC_programmes_viewed_over_threshold_A                          
                  +     annualised_DARTESPN_programmes_viewed_over_threshold_A                              
                  +     annualised_DARTEUR_programmes_viewed_over_threshold_A                         
                  +     annualised_DARTITV4_programmes_viewed_over_threshold_A                              
                  +     annualised_DARTOTH_programmes_viewed_over_threshold_A                         
                  +     annualised_DARTSS_programmes_viewed_over_threshold_A                          
                  +     annualised_PLDSS_programmes_viewed_over_threshold_L                           
                  +     annualised_WDCSS_programmes_viewed_over_threshold_L                           
                  +     annualised_PLDSS_programmes_viewed_over_threshold_N                           
                  +     annualised_WDCSS_programmes_viewed_over_threshold_N                           
                  +     annualised_DRTCHA_programmes_viewed_over_threshold_A  )     /     576      as    Darts
,     1.00* (           annualised_EQUOTH_programmes_viewed_over_threshold_A                          
                  +     annualised_EQUBBC_programmes_viewed_over_threshold_A                          
                  +     annualised_EQUBTS_programmes_viewed_over_threshold_A                          
                  +     annualised_EQUEUR_programmes_viewed_over_threshold_A                          
                  +     annualised_EQUSS_programmes_viewed_over_threshold_A                           
                  +     annualised_EQUCH4_programmes_viewed_over_threshold_A                          
                  +     annualised_EQUESPN_programmes_viewed_over_threshold_A )     /      1757  as    Equestrian
,     1.00* (           annualised_EXTOTH_programmes_viewed_over_threshold_A                          
                  +     annualised_EXTBTS_programmes_viewed_over_threshold_A                          
                  +     annualised_EXTCH4_programmes_viewed_over_threshold_A                          
                  +     annualised_EXTESPN_programmes_viewed_over_threshold_A                         
                  +     annualised_EXTEUR_programmes_viewed_over_threshold_A                          
                  +     annualised_EXTITV4_programmes_viewed_over_threshold_A                         
                  +     annualised_EXTSS_programmes_viewed_over_threshold_A                           
                  +     annualised_EXTCHA_programmes_viewed_over_threshold_A  )     /      13256 as    Extreme
,     1.00* (           annualised_FSHOTH_programmes_viewed_over_threshold_A                          
                  +     annualised_FISHSS_programmes_viewed_over_threshold_A  )     /      5783  as    Fishing
,     1.00* (           annualised_FOOTBBC_programmes_viewed_over_threshold_A                         
                  +     annualised_FOOTBTS_programmes_viewed_over_threshold_A                         
                  +     annualised_FOOTCH4_programmes_viewed_over_threshold_A                         
                  +     annualised_FOOTESPN_programmes_viewed_over_threshold_A                              
                  +     annualised_FOOTEUR_programmes_viewed_over_threshold_A                         
                  +     annualised_FOOTITV1_programmes_viewed_over_threshold_A                              
                  +     annualised_FOOTITV4_programmes_viewed_over_threshold_A                              
                  +     annualised_FOOTOTH_programmes_viewed_over_threshold_A                         
                  +     annualised_FOOTS12_programmes_viewed_over_threshold_A                         
                  +     annualised_FOOTSS_programmes_viewed_over_threshold_A                          
                  +     annualised_AFCEUR_programmes_viewed_over_threshold_L                          
                  +     annualised_AFCITV_programmes_viewed_over_threshold_L                          
                  +     annualised_AUFBTS_programmes_viewed_over_threshold_L                          
                  +     annualised_BFTBTS_programmes_viewed_over_threshold_L                          
                  +     annualised_BUNBTS_programmes_viewed_over_threshold_L                          
                  +     annualised_BUNESPN_programmes_viewed_over_threshold_L                         
                  +     annualised_CHLITV_programmes_viewed_over_threshold_L                          
                  +     annualised_CONFBTS_programmes_viewed_over_threshold_L                         
                  +     annualised_ELBTSP_programmes_viewed_over_threshold_L                          
                  +     annualised_ELESPN_programmes_viewed_over_threshold_L                          
                  +     annualised_ELITV_programmes_viewed_over_threshold_L                           
                  +     annualised_FACESPN_programmes_viewed_over_threshold_L                         
                  +     annualised_FACITV_programmes_viewed_over_threshold_L                          
                  +     annualised_FLCCSS_programmes_viewed_over_threshold_L                          
                  +     annualised_FLOTSS_programmes_viewed_over_threshold_L                          
                  +     annualised_L1BTS_programmes_viewed_over_threshold_L                           
                  +     annualised_L1ESPN_programmes_viewed_over_threshold_L                          
                  +     annualised_PLBTS_programmes_viewed_over_threshold_L                           
                  +     annualised_PLESPN_programmes_viewed_over_threshold_L                          
                  +     annualised_PLMNFSS_programmes_viewed_over_threshold_L                         
                  +     annualised_PLOLSS_programmes_viewed_over_threshold_L                          
                  +     annualised_PLSLSS_programmes_viewed_over_threshold_L                          
                  +     annualised_PLSNSS_programmes_viewed_over_threshold_L                          
                  +     annualised_PLS4SS_programmes_viewed_over_threshold_L                          
                  +     annualised_PLSULSS_programmes_viewed_over_threshold_L                         
                  +     annualised_SFASS_programmes_viewed_over_threshold_L                           
                  +     annualised_SABTS_programmes_viewed_over_threshold_L                           
                  +     annualised_SAESPN_programmes_viewed_over_threshold_L                          
                  +     annualised_SFLESPN_programmes_viewed_over_threshold_L                         
                          
                  +     annualised_SPFSS_programmes_viewed_over_threshold_L                           
                  +     annualised_SPFLBTS_programmes_viewed_over_threshold_L                         
                  +     annualised_SPLESPN_programmes_viewed_over_threshold_L                         
                  +     annualised_SPLSS_programmes_viewed_over_threshold_L                           
                  +     annualised_CLOSS_programmes_viewed_over_threshold_L                           
                  +     annualised_CLTSS_programmes_viewed_over_threshold_L                           
                  +     annualised_CLWSS_programmes_viewed_over_threshold_L                           
                  +     annualised_USFBTS_programmes_viewed_over_threshold_L                          
                  +     annualised_AFCEUR_programmes_viewed_over_threshold_N                          
                  +     annualised_AUFBTS_programmes_viewed_over_threshold_N                          
                  +     annualised_BUNBTS_programmes_viewed_over_threshold_N                          
                  +     annualised_BUNESPN_programmes_viewed_over_threshold_N                         
                  +     annualised_FLCCSS_programmes_viewed_over_threshold_N                          
                  +     annualised_FLOTSS_programmes_viewed_over_threshold_N                          
                  +     annualised_L1BTS_programmes_viewed_over_threshold_N                           
                  +     annualised_L1ESPN_programmes_viewed_over_threshold_N                          
                  +     annualised_MOTDBBC_programmes_viewed_over_threshold_N                         
                  +     annualised_NIFSS_programmes_viewed_over_threshold_N                           
                  +     annualised_PLMCSS_programmes_viewed_over_threshold_N                          
                  +     annualised_PLNLSS_programmes_viewed_over_threshold_N                          
                  +     annualised_ROISS_programmes_viewed_over_threshold_N                           
                  +     annualised_SFASS_programmes_viewed_over_threshold_N                           
                  +     annualised_SABTS_programmes_viewed_over_threshold_N                           
                  +     annualised_SAESPN_programmes_viewed_over_threshold_N                          
                  +     annualised_SPFSS_programmes_viewed_over_threshold_N                           
                  +     annualised_SPLSS_programmes_viewed_over_threshold_N                           
                  +     annualised_FLSBBC_programmes_viewed_over_threshold_N                          
                  +     annualised_CLNSS_programmes_viewed_over_threshold_N                           
                  +     annualised_USFBTS_programmes_viewed_over_threshold_N                          
                  +     annualised_WIFSS_programmes_viewed_over_threshold_N                           
                  +     annualised_CMSITV_programmes_viewed_over_threshold_L                          
                  +     annualised_CONCBBC_programmes_viewed_over_threshold_L                         
                  +     annualised_EFRITV_programmes_viewed_over_threshold_L                          
                  +     annualised_EWQAITV_programmes_viewed_over_threshold_L                         
                  +     annualised_EWQHITV_programmes_viewed_over_threshold_L                         
                  +     annualised_IFESPN_programmes_viewed_over_threshold_L                          
                  +     annualised_IFBTS_programmes_viewed_over_threshold_L                           
                  +     annualised_NIFSS_programmes_viewed_over_threshold_L                           
                  +     annualised_ROISS_programmes_viewed_over_threshold_L                           
                  +     annualised_SP5SS_programmes_viewed_over_threshold_L                           
                  +     annualised_WCQESPN_programmes_viewed_over_threshold_L                         
                  +     annualised_WIFSS_programmes_viewed_over_threshold_L                           
                  +     annualised_WCLBBBC_programmes_viewed_over_threshold_L                         
                  +     annualised_WCQBTS_programmes_viewed_over_threshold_L  )     /      57755 as    Football

,     1.00* (           annualised_GOLFBBC_programmes_viewed_over_threshold_A                         
                  +     annualised_GOLFESPN_programmes_viewed_over_threshold_A                              
                  +     annualised_GOLFEUR_programmes_viewed_over_threshold_A                         
                  +     annualised_GOLFOTH_programmes_viewed_over_threshold_A                         
                  +     annualised_GOLFSS_programmes_viewed_over_threshold_A                          
                  +     annualised_ATGSS_programmes_viewed_over_threshold_L                           
                  +     annualised_ETGSS_programmes_viewed_over_threshold_L                           
                  +     annualised_PGASS_programmes_viewed_over_threshold_L                           
                  +     annualised_USOGSS_programmes_viewed_over_threshold_L                          
                  +     annualised_ATGSS_programmes_viewed_over_threshold_N                           
                  +     annualised_ETGSS_programmes_viewed_over_threshold_N                           
                  +     annualised_PGASS_programmes_viewed_over_threshold_N                           
                  +     annualised_SOLSS_programmes_viewed_over_threshold_N                           
                  +     annualised_USOGSS_programmes_viewed_over_threshold_N                          
                  +     annualised_BOGSS_programmes_viewed_over_threshold_L                           
                  +     annualised_SOLSS_programmes_viewed_over_threshold_L                           
                  +     annualised_MGBBC_programmes_viewed_over_threshold_L                           
                  +     annualised_USMGSS_programmes_viewed_over_threshold_L                          
                  +     annualised_USPGASS_programmes_viewed_over_threshold_L                         
                  +     annualised_BOGSS_programmes_viewed_over_threshold_N                           
                  +     annualised_MGBBC_programmes_viewed_over_threshold_N                           
                  +     annualised_USMGSS_programmes_viewed_over_threshold_N  )     /      4712  as    Golf
,     1.00* (           annualised_IHOTH_programmes_viewed_over_threshold_A                           
                  +     annualised_IHESPN_programmes_viewed_over_threshold_A                          
                  +     annualised_IHSS_programmes_viewed_over_threshold_A                            
                  +     annualised_IHEUR_programmes_viewed_over_threshold_A   )     /      2037  as    Ice_Hockey
,     1.00* (           annualised_MROSS_programmes_viewed_over_threshold_N                           
                  +     annualised_MROSS_programmes_viewed_over_threshold_L   )     /     124      as    Mixed
,     1.00* (           annualised_MSPBBC_programmes_viewed_over_threshold_A                          
                  +     annualised_MSPBTS_programmes_viewed_over_threshold_A                          
                  +     annualised_MSPCH4_programmes_viewed_over_threshold_A                          
                  +     annualised_MSPCH5_programmes_viewed_over_threshold_A                          
                  +     annualised_MSPESPN_programmes_viewed_over_threshold_A                         
                  +     annualised_MSPEUR_programmes_viewed_over_threshold_A                          
                  +     annualised_MOTSITV1_programmes_viewed_over_threshold_A                              
                  +     annualised_MSPITV4_programmes_viewed_over_threshold_A                         
                  +     annualised_MSPOTH_programmes_viewed_over_threshold_A                          
                  +     annualised_MSPS12_programmes_viewed_over_threshold_A                          
                  +     annualised_MSPSS_programmes_viewed_over_threshold_A                           
                  +     annualised_F1PBBC_programmes_viewed_over_threshold_L                          
                  +     annualised_F1QBBC_programmes_viewed_over_threshold_L                          
                  +     annualised_F1RBBC_programmes_viewed_over_threshold_L                          
                  +     annualised_F1PSS_programmes_viewed_over_threshold_L                           
                  +     annualised_F1QSS_programmes_viewed_over_threshold_L                           
                  +     annualised_F1RSS_programmes_viewed_over_threshold_L                           
                  +     annualised_MGPBBC_programmes_viewed_over_threshold_L                          
                  +     annualised_F1NBBC_programmes_viewed_over_threshold_N                          
                  +     annualised_F1NSS_programmes_viewed_over_threshold_N   )     /      26550 as    Motor_Sport
,     1.00* (           annualised_RACCH4_programmes_viewed_over_threshold_A                          
                  +     annualised_RACESPN_programmes_viewed_over_threshold_A                         
                  +     annualised_RACEUR_programmes_viewed_over_threshold_A                          
                  +     annualised_RACOTH_programmes_viewed_over_threshold_A                          
                  +     annualised_RACSS_programmes_viewed_over_threshold_A                           
                  +     annualised_CHELCH4_programmes_viewed_over_threshold_L                         
                  +     annualised_DERCH4_programmes_viewed_over_threshold_L                          
                  +     annualised_GDNCH4_programmes_viewed_over_threshold_L                          
                  +     annualised_OAKCH4_programmes_viewed_over_threshold_L                          
                  +     annualised_RASCH4_programmes_viewed_over_threshold_L                          
                  +     annualised_CHELCH4_programmes_viewed_over_threshold_N                         
                  +     annualised_GDNCH4_programmes_viewed_over_threshold_N                          
                  +     annualised_RASCH4_programmes_viewed_over_threshold_N  )     /      14655 as    Racing
,     1.00* (           annualised_RUGBBC_programmes_viewed_over_threshold_A                          
                  +     annualised_RUGBTS_programmes_viewed_over_threshold_A                          
                  +     annualised_RUGESPN_programmes_viewed_over_threshold_A                         
                  +     annualised_RUGITV1_programmes_viewed_over_threshold_A                         
                  +     annualised_RUGITV4_programmes_viewed_over_threshold_A                         
                  +     annualised_RUGOTH_programmes_viewed_over_threshold_A                          
                  +     annualised_RUGSS_programmes_viewed_over_threshold_A                           
                  +     annualised_AVPSS_programmes_viewed_over_threshold_L                           
                  +     annualised_BILSS_programmes_viewed_over_threshold_L                           
                  +     annualised_ENRSS_programmes_viewed_over_threshold_L                           
                  +     annualised_HECSS_programmes_viewed_over_threshold_L                           
                  +     annualised_IRBSS_programmes_viewed_over_threshold_L                           
                  +     annualised_PRUSS_programmes_viewed_over_threshold_L                           
                  +     annualised_RLGSS_programmes_viewed_over_threshold_L                           
                  +     annualised_SARUSS_programmes_viewed_over_threshold_L                          
                  +     annualised_BILSS_programmes_viewed_over_threshold_N                           
                  +     annualised_ENRSS_programmes_viewed_over_threshold_N                           
                  +     annualised_HECSS_programmes_viewed_over_threshold_N                           
                  +     annualised_IRBSS_programmes_viewed_over_threshold_N                           
                  +     annualised_PRUSS_programmes_viewed_over_threshold_N                           
                  +     annualised_SARUSS_programmes_viewed_over_threshold_N                          
                  +     annualised_ORUGESPN_programmes_viewed_over_threshold_L                              
                  +     annualised_RIEBBC_programmes_viewed_over_threshold_L                          
                  +     annualised_RIIBBC_programmes_viewed_over_threshold_L                          
                  +     annualised_RISBBC_programmes_viewed_over_threshold_L                          
                  +     annualised_RIWBBC_programmes_viewed_over_threshold_L                          
                  +     annualised_RLCCBBC_programmes_viewed_over_threshold_L                         
                  +     annualised_RLWCBBC_programmes_viewed_over_threshold_L
+                  +     annualised_SNRBBC_programmes_viewed_over_threshold_L )     /      8062  as    Rugby
,     1.00* (           annualised_OTHSNP_programmes_viewed_over_threshold_A                          
                  +     annualised_SNPBBC_programmes_viewed_over_threshold_A                          
                  +     annualised_SNPESPN_programmes_viewed_over_threshold_A                         
                  +     annualised_SNPEUR_programmes_viewed_over_threshold_A                          
                  +     annualised_SNPSS_programmes_viewed_over_threshold_A                           
                  +     annualised_WSCBBC_programmes_viewed_over_threshold_L                          
                  +     annualised_MRPSS_programmes_viewed_over_threshold_N                           
                  +     annualised_MRSSS_programmes_viewed_over_threshold_N                           
                  +     annualised_WSCBBC_programmes_viewed_over_threshold_N                          
                  +     annualised_SNPITV1_programmes_viewed_over_threshold_A                         
                  +     annualised_SNPITV4_programmes_viewed_over_threshold_A                         
                  +     annualised_MRPSS_programmes_viewed_over_threshold_L                           
                  +     annualised_MRSSS_programmes_viewed_over_threshold_L   )     /      1296  as    Snooker_Pool
,     1.00* (           annualised_TENBBC_programmes_viewed_over_threshold_A                          
                  +     annualised_TENBTS_programmes_viewed_over_threshold_A                          
                  +     annualised_TENESPN_programmes_viewed_over_threshold_A                         
                  +     annualised_TENEUR_programmes_viewed_over_threshold_A                          
                  +     annualised_TENITV4_programmes_viewed_over_threshold_A                         
                  +     annualised_OTHTEN_programmes_viewed_over_threshold_A                          
                  +     annualised_TENSS_programmes_viewed_over_threshold_A                           
                  +     annualised_ATPSS_programmes_viewed_over_threshold_L                           
                  +     annualised_AOTEUR_programmes_viewed_over_threshold_L                          
                  +     annualised_FOTEUR_programmes_viewed_over_threshold_L                          
                  +     annualised_FOTITV_programmes_viewed_over_threshold_L                          
                  +     annualised_USOTSS_programmes_viewed_over_threshold_L                          
                  +     annualised_USOTEUR_programmes_viewed_over_threshold_L                         
                  +     annualised_WIMBBC_programmes_viewed_over_threshold_L                          
                  +     annualised_ATPSS_programmes_viewed_over_threshold_N                           
                  +     annualised_AOTEUR_programmes_viewed_over_threshold_N                          
                  +     annualised_FOTEUR_programmes_viewed_over_threshold_N                          
                  +     annualised_FOTITV_programmes_viewed_over_threshold_N                          
                  +     annualised_USOTSS_programmes_viewed_over_threshold_N                          
                  +     annualised_USOTEUR_programmes_viewed_over_threshold_N                         
                  +     annualised_WIMBBC_programmes_viewed_over_threshold_N                          
                  +     annualised_AOTBBC_programmes_viewed_over_threshold_L  )     /      2956  as    Tennis
,     1.00* (           annualised_UNKBBC_programmes_viewed_over_threshold_A                          
                  +     annualised_UNKBTS_programmes_viewed_over_threshold_A                          
                  +     annualised_UNKCH4_programmes_viewed_over_threshold_A                          
                  +     annualised_UNKCH5_programmes_viewed_over_threshold_A                          
                  +     annualised_UNKESPN_programmes_viewed_over_threshold_A                         
                  +     annualised_UNKEUR_programmes_viewed_over_threshold_A                          
                  +     annualised_UNKITV1_programmes_viewed_over_threshold_A                         
                  +     annualised_UNKITV4_programmes_viewed_over_threshold_A                         
                  +     annualised_OTHUNK_programmes_viewed_over_threshold_A                          
                  +     annualised_UNKS12_programmes_viewed_over_threshold_A                          
                  +     annualised_UNKSS_programmes_viewed_over_threshold_A                           
                  +     annualised_UNKCHA_programmes_viewed_over_threshold_A  )     /      34044 as    U
,     1.00* (           annualised_OTHWAT_programmes_viewed_over_threshold_A                          
                  +     annualised_WATBBC_programmes_viewed_over_threshold_A                          
                  +     annualised_WATCH4_programmes_viewed_over_threshold_A                          
                  +     annualised_WATESPN_programmes_viewed_over_threshold_A                         
                  +     annualised_WATEUR_programmes_viewed_over_threshold_A                          
                  +     annualised_WATSS_programmes_viewed_over_threshold_A                           
                  +     annualised_AMCBBC_programmes_viewed_over_threshold_N                          
                  +     annualised_BTRBBC_programmes_viewed_over_threshold_L  )     /      2644  as    Watersports
,     1.00* (           annualised_OTHWIN_programmes_viewed_over_threshold_A                          
                  +     annualised_WINBBC_programmes_viewed_over_threshold_A                          
                  +     annualised_WINBTS_programmes_viewed_over_threshold_A                          
                  +     annualised_WINESPN_programmes_viewed_over_threshold_A                         
                  +     annualised_WINEUR_programmes_viewed_over_threshold_A                          
                  +     annualised_WINSS_programmes_viewed_over_threshold_A                           
                  +     annualised_WINCH4_programmes_viewed_over_threshold_A  )     /      2404  as    Wintersports
,     1.00* (           annualised_OTHWRE_programmes_viewed_over_threshold_A                          
                  +     annualised_WRES12_programmes_viewed_over_threshold_A                          
                  +     annualised_WRESS_programmes_viewed_over_threshold_A                           
                  +     annualised_WWESS_programmes_viewed_over_threshold_L                           
                  +     annualised_TNACHA_programmes_viewed_over_threshold_N                          
                  +     annualised_WWES12_programmes_viewed_over_threshold_N                          
                  +     annualised_WWESS_programmes_viewed_over_threshold_N                           
                  +     annualised_WRECH5_programmes_viewed_over_threshold_A                          
                  +     annualised_WREESPN_programmes_viewed_over_threshold_A                         
                  +     annualised_WRECHA_programmes_viewed_over_threshold_A  )     /      3594  as    Wrestling

----Add Ranks---
,RANK() OVER ( PARTITION BY NULL ORDER BY American_Football desc) as rank_American_Football
,RANK() OVER ( PARTITION BY NULL ORDER BY Athletics desc) as rank_Athletics
,RANK() OVER ( PARTITION BY NULL ORDER BY Baseball desc) as rank_Baseball
,RANK() OVER ( PARTITION BY NULL ORDER BY Basketball desc) as rank_Basketball
,RANK() OVER ( PARTITION BY NULL ORDER BY Boxing desc) as rank_Boxing
,RANK() OVER ( PARTITION BY NULL ORDER BY Cricket desc) as rank_Cricket
,RANK() OVER ( PARTITION BY NULL ORDER BY Cycling desc) as rank_Cycling
,RANK() OVER ( PARTITION BY NULL ORDER BY Darts desc) as rank_Darts
,RANK() OVER ( PARTITION BY NULL ORDER BY Equestrian desc) as rank_Equestrian
,RANK() OVER ( PARTITION BY NULL ORDER BY Extreme desc) as rank_Extreme
,RANK() OVER ( PARTITION BY NULL ORDER BY Fishing desc) as rank_Fishing
,RANK() OVER ( PARTITION BY NULL ORDER BY Football desc) as rank_Football
,RANK() OVER ( PARTITION BY NULL ORDER BY Golf desc) as rank_Golf
,RANK() OVER ( PARTITION BY NULL ORDER BY Ice_Hockey desc) as rank_Ice_Hockey
,RANK() OVER ( PARTITION BY NULL ORDER BY Mixed desc) as rank_Mixed
,RANK() OVER ( PARTITION BY NULL ORDER BY Motor_Sport desc) as rank_Motor_Sport
,RANK() OVER ( PARTITION BY NULL ORDER BY Racing desc) as rank_Racing
,RANK() OVER ( PARTITION BY NULL ORDER BY Rugby desc) as rank_Rugby
,RANK() OVER ( PARTITION BY NULL ORDER BY Snooker_Pool desc) as rank_Snooker_Pool
,RANK() OVER ( PARTITION BY NULL ORDER BY Tennis desc) as rank_Tennis
,RANK() OVER ( PARTITION BY NULL ORDER BY U desc) as rank_U
,RANK() OVER ( PARTITION BY NULL ORDER BY Watersports desc) as rank_Watersports
,RANK() OVER ( PARTITION BY NULL ORDER BY Wintersports desc) as rank_Wintersports
,RANK() OVER ( PARTITION BY NULL ORDER BY Wrestling desc) as rank_Wrestling



into dbarnett.v250_rank_information
from dbarnett.v250_Account_profiling as a
left outer join        dbarnett.v250_annualised_activity_table_analysis_metrics_PV as b
on a.account_number = b.account_number
;

commit;

grant all on dbarnett.v250_rank_information to public;
commit;

---Add on the deciles---

--
create variable @total_accounts integer;
set @total_accounts = (select count(*) from dbarnett.v250_rank_information);
--select @total_accounts;
commit;

alter table dbarnett.v250_rank_information add American_Football_decile integer;
alter table dbarnett.v250_rank_information add Athletics_decile integer;
alter table dbarnett.v250_rank_information add Baseball_decile integer;
alter table dbarnett.v250_rank_information add Basketball_decile integer;
alter table dbarnett.v250_rank_information add Boxing_decile integer;
alter table dbarnett.v250_rank_information add Cricket_decile integer;
alter table dbarnett.v250_rank_information add Cycling_decile integer;
alter table dbarnett.v250_rank_information add Darts_decile integer;
alter table dbarnett.v250_rank_information add Equestrian_decile integer;
alter table dbarnett.v250_rank_information add Extreme_decile integer;
alter table dbarnett.v250_rank_information add Fishing_decile integer;
alter table dbarnett.v250_rank_information add Football_decile integer;
alter table dbarnett.v250_rank_information add Golf_decile integer;
alter table dbarnett.v250_rank_information add Ice_Hockey_decile integer;
alter table dbarnett.v250_rank_information add Mixed_decile integer;
alter table dbarnett.v250_rank_information add Motor_Sport_decile integer;
alter table dbarnett.v250_rank_information add Racing_decile integer;
alter table dbarnett.v250_rank_information add Rugby_decile integer;
alter table dbarnett.v250_rank_information add Snooker_Pool_decile integer;
alter table dbarnett.v250_rank_information add Tennis_decile integer;
alter table dbarnett.v250_rank_information add U_decile integer;
alter table dbarnett.v250_rank_information add Watersports_decile integer;
alter table dbarnett.v250_rank_information add Wintersports_decile integer;
alter table dbarnett.v250_rank_information add Wrestling_decile integer;

commit;

update dbarnett.v250_rank_information
set American_Football_decile=case when American_Football = 0 then 10 else floor (rank_American_Football/(@total_accounts/10 ))+1 end
,Athletics_decile=case when Athletics = 0 then 10 else floor (rank_Athletics/(@total_accounts/10 ))+1 end
,Baseball_decile=case when Baseball = 0 then 10 else floor (rank_Baseball/(@total_accounts/10 ))+1 end
,Basketball_decile=case when Basketball = 0 then 10 else floor (rank_Basketball/(@total_accounts/10 ))+1 end
,Boxing_decile=case when Boxing = 0 then 10 else floor (rank_Boxing/(@total_accounts/10 ))+1 end
,Cricket_decile=case when Cricket = 0 then 10 else floor (rank_Cricket/(@total_accounts/10 ))+1 end
,Cycling_decile=case when Cycling = 0 then 10 else floor (rank_Cycling/(@total_accounts/10 ))+1 end
,Darts_decile=case when Darts = 0 then 10 else floor (rank_Darts/(@total_accounts/10 ))+1 end
,Equestrian_decile=case when Equestrian = 0 then 10 else floor (rank_Equestrian/(@total_accounts/10 ))+1 end
,Extreme_decile=case when Extreme = 0 then 10 else floor (rank_Extreme/(@total_accounts/10 ))+1 end
,Fishing_decile=case when Fishing = 0 then 10 else floor (rank_Fishing/(@total_accounts/10 ))+1 end
,Football_decile=case when Football = 0 then 10 else floor (rank_Football/(@total_accounts/10 ))+1 end
,Golf_decile=case when Golf = 0 then 10 else floor (rank_Golf/(@total_accounts/10 ))+1 end
,Ice_Hockey_decile=case when Ice_Hockey = 0 then 10 else floor (rank_Ice_Hockey/(@total_accounts/10 ))+1 end
,Mixed_decile=case when Mixed = 0 then 10 else floor (rank_Mixed/(@total_accounts/10 ))+1 end
,Motor_Sport_decile=case when Motor_Sport = 0 then 10 else floor (rank_Motor_Sport/(@total_accounts/10 ))+1 end
,Racing_decile=case when Racing = 0 then 10 else floor (rank_Racing/(@total_accounts/10 ))+1 end
,Rugby_decile=case when Rugby = 0 then 10 else floor (rank_Rugby/(@total_accounts/10 ))+1 end
,Snooker_Pool_decile=case when Snooker_Pool = 0 then 10 else floor (rank_Snooker_Pool/(@total_accounts/10 ))+1 end
,Tennis_decile=case when Tennis = 0 then 10 else floor (rank_Tennis/(@total_accounts/10 ))+1 end
,U_decile=case when U = 0 then 10 else floor (rank_U/(@total_accounts/10 ))+1 end
,Watersports_decile=case when Watersports = 0 then 10 else floor (rank_Watersports/(@total_accounts/10 ))+1 end
,Wintersports_decile=case when Wintersports = 0 then 10 else floor (rank_Wintersports/(@total_accounts/10 ))+1 end
,Wrestling_decile=case when Wrestling = 0 then 10 else floor (rank_Wrestling/(@total_accounts/10 ))+1 end
from dbarnett.v250_rank_information
;
commit;


---Create Count of distinct device types used by Account---
--drop table dbarnett.v250_sky_go_site_name_lookup;
create table dbarnett.v250_sky_go_site_name_lookup
(site_name varchar(20)
,site_name_type varchar (20)
,service_type varchar (20)
);
INSERT INTO dbarnett.v250_sky_go_site_name_lookup
            (site_name
             , site_name_type
             ,service_type)

select 'MOBI', 'IOS (Apple)','Sky Go'
union select 'GOIO', 'IOS (Apple)','Sky Go'
union select 'GOXB', 'Xbox','Sky Go'
union select 'XBOX', 'Xbox','Sky Go'
union select 'GOPC', 'PC/Laptop','Sky Go'
union select 'ANDR', 'Android','Sky Go'
union select 'GOAN', 'Android','Sky Go'

union select 'SMRK', 'Roku','Now TV'
union select 'SMPS', 'Sony Playstation','Now TV'
union select 'SMXB', 'Xbox','Now TV'
union select 'SMLG', 'LG Smart TV','Now TV'
union select 'SMYV', 'YouView','Now TV'
union select 'SMPC', 'PC/Laptop','Now TV'
union select 'SMIO', 'IOS (Apple)','Now TV'
union select 'SMAN', 'Android','Now TV'


commit;



--select * from dbarnett.v250_sky_go_site_name_lookup
--Sky Go Devices Used

select account_number
,count(distinct b.site_name_type) as device_types_used
into #device_types_used_by_account
from SK_PROD.SKY_PLAYER_USAGE_DETAIL as a
left outer join dbarnett.v250_sky_go_site_name_lookup as b
on a.site_name = b.site_name
where account_number is not null
and video_sk <> -1
and x_usage_type = 'Live Viewing'
and activity_dt>='2012-01-01' 
and left(a.site_name,2) not in ('SM')
group by account_number
;
commit;

alter table dbarnett.v250_Account_profiling add sky_go_devices_used integer;

update dbarnett.v250_Account_profiling
set sky_go_devices_used=case when b.device_types_used is null then 0 else b.device_types_used  end
from dbarnett.v250_Account_profiling as a
left outer join  #device_types_used_by_account as b
on a.account_number = b.account_number
;

commit;


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

--alter table dbarnett.v250_Account_profiling delete active_stb_subs;
alter table dbarnett.v250_Account_profiling add active_stb_subs integer;

update dbarnett.v250_Account_profiling
set active_stb_subs=case when b.active_stbs is null then 0 else b.active_stbs  end
from dbarnett.v250_Account_profiling as a
left outer join  #active_stb_by_account as b
on a.account_number = b.account_number
;

commit;


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
set number_children_in_hh=case when b.num_children_in_hh is null then null else b.num_children_in_hh  end
,number_adults_in_hh=case when b.num_adults_in_hh is null then null else b.num_adults_in_hh  end
,number_people_in_hh=case when b.num_children_in_hh is null then null else b.num_adults_in_hh+b.num_children_in_hh  end
from dbarnett.v250_Account_profiling as a
left outer join  #experian_hh_summary_people as b
on a.cb_key_household = b.cb_key_household
;

commit;

---Sky Go Sports Usage---
--drop table #sports_stream_days;

select account_number
,min(activity_dt) as first_sports_stream_date
,count(distinct activity_dt) as sports_stream_days
into #sports_stream_days
from SK_PROD.SKY_PLAYER_USAGE_DETAIL as a
left outer join dbarnett.v250_sky_go_site_name_lookup as b
on a.site_name = b.site_name
where account_number is not null
and video_sk <> -1
and x_usage_type = 'Live Viewing'
and activity_dt<=@analysis_date
and left(a.site_name,2) not in ('SM')
and broadcast_channel in ('StreamSkySports1','StreamSkySports2', 'StreamSkySports3','StreamSkySports4','StreamSkySportsFormula1','StreamSkySportsXtra' )

group by account_number
;
commit;


alter table dbarnett.v250_Account_profiling add first_sports_stream_date date;
alter table dbarnett.v250_Account_profiling add sports_stream_days integer;

update dbarnett.v250_Account_profiling
set first_sports_stream_date=b.first_sports_stream_date
,sports_stream_days=case when b.sports_stream_days is null then 0 else b.sports_stream_days  end
from dbarnett.v250_Account_profiling as a
left outer join  #sports_stream_days as b
on a.account_number = b.account_number
;
commit;

---Repeat for General Sky Go Activity---
select account_number
,min(activity_dt) as first_sky_go_date
,count(distinct activity_dt) as sky_go_days
into #sky_go_days
from SK_PROD.SKY_PLAYER_USAGE_DETAIL as a
left outer join dbarnett.v250_sky_go_site_name_lookup as b
on a.site_name = b.site_name
where account_number is not null
and video_sk <> -1
and activity_dt<=@analysis_date
and left(a.site_name,2) not in ('SM')

group by account_number
;
commit;


alter table dbarnett.v250_Account_profiling add first_sky_go_date date;
alter table dbarnett.v250_Account_profiling add sky_go_days integer;

update dbarnett.v250_Account_profiling
set first_sky_go_date=b.first_sky_go_date
,sky_go_days=case when b.sky_go_days is null then 0 else b.sky_go_days  end
from dbarnett.v250_Account_profiling as a
left outer join  #sky_go_days as b
on a.account_number = b.account_number
;
commit;

----repeat for Now TV---

select account_number
,min(activity_dt) as first_now_tv_date
,count(distinct activity_dt) as now_tv_days
into #now_tv_days
from SK_PROD.SKY_PLAYER_USAGE_DETAIL as a
left outer join dbarnett.v250_sky_go_site_name_lookup as b
on a.site_name = b.site_name
where account_number is not null
and video_sk <> -1
and activity_dt<=@analysis_date
and left(a.site_name,2)  in ('SM')

group by account_number
;
commit;


alter table dbarnett.v250_Account_profiling add first_now_tv_date date;
alter table dbarnett.v250_Account_profiling add now_tv_days integer;

update dbarnett.v250_Account_profiling
set first_now_tv_date=b.first_now_tv_date
,now_tv_days=case when b.now_tv_days is null then 0 else b.now_tv_days  end
from dbarnett.v250_Account_profiling as a
left outer join  #now_tv_days as b
on a.account_number = b.account_number
;
commit;

---Add in % Activity Rate ----

--

alter table dbarnett.v250_Account_profiling add sky_go_activity_rate real;
alter table dbarnett.v250_Account_profiling add sky_go_sports_activity_rate real;
alter table dbarnett.v250_Account_profiling add now_tv_activity_rate real;

update dbarnett.v250_Account_profiling
set sky_go_activity_rate=case when first_sky_go_date is null then 0 
when first_sky_go_date=@analysis_date then 1 else sky_go_days/cast(datediff(day,first_sky_go_date,@analysis_date) as real) end
,sky_go_sports_activity_rate=case when first_sports_stream_date is null then 0 when first_sports_stream_date=@analysis_date then 1 else sports_stream_days/cast(datediff(day,first_sports_stream_date,@analysis_date)as real) end
,now_tv_activity_rate=case when first_now_tv_date is null then 0 when first_now_tv_date=@analysis_date then 1 else now_tv_days/cast(datediff(day,first_now_tv_date,@analysis_date)as real) end
from dbarnett.v250_Account_profiling as a
;
commit;

--select now_tv_days , count(*) from dbarnett.v250_Account_profiling group by now_tv_days order by now_tv_days
--select first_sports_stream_date , count(*) from dbarnett.v250_Account_profiling group by first_sports_stream_date order by first_sports_stream_date

---Add on SIG segments---

create table dbarnett.v250_Final_SIG_List
(cluster_value integer
,cluster_name varchar (80)
);
INSERT INTO dbarnett.v250_Final_SIG_List
            (cluster_value
             , cluster_name
             )

select 1,'SIG 01 - International Rugby Fans'
union select 2,'SIG 02 - Flower of Scotland'
union select 3,'SIG 03 - Sports Traditionalists'
union select 4,'SIG 04 - Football Heartland'
union select 5,'SIG 05 - Cricket Enthusiasts'
union select 6,'SIG 06 - Motor Sport Fans'
union select 7,'SIG 07 - Football Fanatics (Single Provider)'
union select 8,'SIG 08 - F1 Super Fans'
union select 9,'SIG 09 - Super Sports Fans'
union select 10,'SIG 10 - Sports Disengaged'
union select 11,'SIG 11 - Cricket Fanatics'
union select 12,'SIG 12 - Football Fanatics (Multi Provider)'
union select 13,'SIG 13 - Fast Card and Football'
union select 14,'SIG 14 - Tennis Fans'
union select 15,'SIG 15 - Club Rugby Fans'
union select 16,'SIG 16 - FTA Football Fans'
union select 17,'SIG 17 - Volatile Football Fans'
union select 18,'SIG 18 - Football and Little Else'
union select 19,'SIG 19 - Big Name Brands'
union select 20,'SIG 20 - Cricket Fans'

;
commit;

grant all on dbarnett.v250_Final_SIG_List to public ; commit;

--select * from dbarnett.v250_Final_SIG_List ;


alter table dbarnett.v250_Account_profiling add cluster_name varchar (80);

update dbarnett.v250_Account_profiling
set cluster_name=c.cluster_name
from dbarnett.v250_Account_profiling as a

left outer join skoczej.v250_cluster_numbers as b
on a.account_number = b.account_number
left outer join dbarnett.v250_Final_SIG_List as c
on b.cluster_number = c.cluster_value
;
commit;

--select cluster_name, count(*) from dbarnett.v250_Account_profiling group by cluster_name order by cluster_name

---Add in People/Screen Ratio---

alter table dbarnett.v250_Account_profiling add people_screen_ratio real;

update dbarnett.v250_Account_profiling
set people_screen_ratio=number_people_in_hh/cast((sky_go_devices_used+active_stb_subs) as real)

from dbarnett.v250_Account_profiling 
;
commit;
--alter table dbarnett.v250_Account_profiling delete people_screen_ratio_grouped;
alter table dbarnett.v250_Account_profiling add people_screen_ratio_grouped varchar(40);

update dbarnett.v250_Account_profiling
set people_screen_ratio_grouped=case when people_screen_ratio <1 then 'a) Under 1 person per screen'
                                     when people_screen_ratio =1 then 'b) 1 person per screen'
                                     when people_screen_ratio <=2 then 'c) >1 and <=2 people per screen'
                                     when people_screen_ratio <=3 then 'd) >2 and <=3 people per screen'
                                     when people_screen_ratio <=4 then 'e) >3 and <=4 people per screen'
                                     when people_screen_ratio <=5 then 'f) >4 and <=5 people per screen'
                                     when people_screen_ratio >5 then 'g) >5 people per screen'
else 'h) Unknown' end

from dbarnett.v250_Account_profiling 
;
commit;

--select  people_screen_ratio_grouped, count(*) from  dbarnett.v250_Account_profiling group by people_screen_ratio_grouped order by people_screen_ratio_grouped


----Streaming Genre


select b.account_number
,cast (LOGIN_DT as date) as login_date
,min(LOGIN_DT) as login_time
,count(*) as logins
into #login_summary_by_day
from sk_prod.SKY_PLAYER_LOGIN_DETAIL as a
left outer join sk_prod.SKY_PLAYER_REGISTRANT as b
on a.SAMPROFILEID=b.SAM_PROFILE_ID
left outer join dbarnett.v250_Account_profiling  as c
on b.account_number=c.account_number

where c.account_number is not null and left(a.site_name,2) not in ('SM')
group by b.account_number
,login_date
order by login_date
;
commit;
commit;
CREATE HG INDEX idx1 ON #login_summary_by_day(account_number);

commit;


select a.account_number
,a.broadcast_channel
,b.login_time
into dbarnett.v250_stream_with_time_added
from SK_PROD.SKY_PLAYER_USAGE_DETAIL as a
left outer join #login_summary_by_day as b
on a.account_number=b.account_number and a.activity_dt = b.login_date
where b.account_number is not null
and video_sk <> -1
and x_usage_type = 'Live Viewing'
and activity_dt<=@analysis_date
and left(a.site_name,2) not in ('SM')
and broadcast_channel in ('StreamSkySports1','StreamSkySports2', 'StreamSkySports3','StreamSkySports4','StreamSkySportsFormula1','StreamSkySportsXtra' )
group by a.account_number
,a.broadcast_channel
,b.login_time
;
commit;
grant all on dbarnett.v250_stream_with_time_added to public;

commit;

--select top 100 * from dbarnett.v250_stream_with_time_added order by account_number , login_time,broadcast_channel;

---Add in Service Key to Match to Broadcast Channel

--select * from v250_channel_to_service_key_lookup_deduped where left (channel_name, 10)= 'Sky Sports' order by channel_name

select channel_name
 , service_key
 , case when channel_name ='Sky Sports F1' then 'StreamSkySportsFormula1'
when channel_name ='Sky Sports 1' then 'StreamSkySports1'
when channel_name ='Sky Sports 3' then 'StreamSkySports3'
when channel_name ='Sky Sports 4' then 'StreamSkySports4'
when channel_name ='Sky Sports 2' then 'StreamSkySports2'
else null end as broadcast_channel
into #service_key_lookup
from v250_channel_to_service_key_lookup_deduped
where service_key in (4002,4081,4022,4026,3835)
;
commit;

select a.*
,c.sub_genre_description
,c.programme_instance_name
into dbarnett.v250_stream_with_time_and_programme_added
from dbarnett.v250_stream_with_time_added as a
left outer join #service_key_lookup as b
on a.broadcast_channel=b.broadcast_channel
left outer join sk_prod.Vespa_programme_schedule as c
on b.service_key=c.service_key
where login_time between broadcast_start_date_time_local and broadcast_end_date_time_local
;
commit;

alter table dbarnett.v250_stream_with_time_and_programme_added add login_date date;

update dbarnett.v250_stream_with_time_and_programme_added
set login_date=cast(login_time as date) 
from dbarnett.v250_stream_with_time_and_programme_added
;
commit;
--select * from dbarnett.v250_stream_with_time_and_programme_added;
select account_number
,sub_genre_description
,count(distinct login_date) as days_viewed
,count(*) as streams
into #summary_by_sub_genre
from dbarnett.v250_stream_with_time_and_programme_added
group by account_number
,sub_genre_description
;
--select * from #summary_by_sub_genre
--drop table #summary_by_sub_genre_and_account;
select account_number
,max(case when sub_genre_description = 'Football' then days_viewed else 0 end) as football_days_viewed
,max(case when sub_genre_description = 'Football' then streams else 0 end) as football_streams

,max(case when sub_genre_description = 'Cricket' then days_viewed else 0 end) as Cricket_days_viewed
,max(case when sub_genre_description = 'Cricket' then streams else 0 end) as Cricket_streams

,max(case when sub_genre_description = 'Golf' then days_viewed else 0 end) as Golf_days_viewed
,max(case when sub_genre_description = 'Golf' then streams else 0 end) as Golf_streams

,max(case when sub_genre_description = 'Rugby' then days_viewed else 0 end) as Rugby_days_viewed
,max(case when sub_genre_description = 'Rugby' then streams else 0 end) as Rugby_streams

,max(case when sub_genre_description = 'Motor Sport' then days_viewed else 0 end) as Motor_Sports_days_viewed
,max(case when sub_genre_description = 'Motor Sport' then streams else 0 end) as Motor_Sports_streams
into #summary_by_sub_genre_and_account
from #summary_by_sub_genre
group by account_number
;

--select distinct sub_genre_description from #summary_by_sub_genre

alter table dbarnett.v250_Account_profiling add football_days_viewed integer;
alter table dbarnett.v250_Account_profiling add football_streams integer;
alter table dbarnett.v250_Account_profiling add Cricket_days_viewed integer;
alter table dbarnett.v250_Account_profiling add Cricket_streams integer;
alter table dbarnett.v250_Account_profiling add Golf_days_viewed integer;
alter table dbarnett.v250_Account_profiling add Golf_streams integer;
alter table dbarnett.v250_Account_profiling add Rugby_days_viewed integer;
alter table dbarnett.v250_Account_profiling add Rugby_streams integer;
alter table dbarnett.v250_Account_profiling add Motor_Sports_days_viewed integer;
alter table dbarnett.v250_Account_profiling add Motor_Sports_streams integer;

update dbarnett.v250_Account_profiling
set football_days_viewed=case when b.football_days_viewed is null then 0 else b.football_days_viewed end
,football_streams=case when b.football_streams is null then 0 else b.football_streams end
,Cricket_days_viewed=case when b.Cricket_days_viewed is null then 0 else b.Cricket_days_viewed end
,Cricket_streams=case when b.Cricket_streams is null then 0 else b.Cricket_streams end
,Golf_days_viewed=case when b.Golf_days_viewed is null then 0 else b.Golf_days_viewed end
,Golf_streams=case when b.Golf_streams is null then 0 else b.Golf_streams end
,Rugby_days_viewed=case when b.Rugby_days_viewed is null then 0 else b.Rugby_days_viewed end
,Rugby_streams=case when b.Rugby_streams is null then 0 else b.Rugby_streams end
,Motor_Sports_days_viewed=case when b.Motor_Sports_days_viewed is null then 0 else b.Motor_Sports_days_viewed end
,Motor_Sports_streams=case when b.Motor_Sports_streams is null then 0 else b.Motor_Sports_streams end
from dbarnett.v250_Account_profiling as a
left outer join #summary_by_sub_genre_and_account as b
on a.account_number = b.account_number
;
commit;
--select Motor_Sports_days_viewed , count(*) from dbarnett.v250_Account_profiling group by Motor_Sports_days_viewed order by Motor_Sports_days_viewed
--select programme_instance_name , count(*) as records from dbarnett.v250_stream_with_time_and_programme_added group by programme_instance_name order by records desc
--select sub_genre_description , count(*) as records from dbarnett.v250_stream_with_time_and_programme_added group by sub_genre_description order by records desc

--select distinct channel_name from sk_prod.Vespa_programme_schedule where upper(channel_name)  like '%XTRA%' order by channel_name

--select top 100 programme_instance_name from sk_prod.Vespa_programme_schedule;
select account_number
,max(case when subscription_sub_type='DTV Chelsea TV' and status_code in ('AC','AB','PC') and effective_from_dt<= @analysis_date and effective_to_dt >@analysis_date  then 1 else 0 end) as active_chelsea_TV_current
,max(case when subscription_sub_type='DTV Chelsea TV' and status_code in ('AC','AB','PC') and effective_from_dt<= @analysis_date then 1 else 0 end) as active_chelsea_TV_ever
,max(case when subscription_sub_type='DTV MUTV' and status_code in ('AC','AB','PC') and effective_from_dt<= @analysis_date and effective_to_dt >@analysis_date  then 1 else 0 end) as active_MUTV_current
,max(case when subscription_sub_type='DTV MUTV' and status_code in ('AC','AB','PC') and effective_from_dt<= @analysis_date then 1 else 0 end) as active_MUTV_ever
into #chelsea_mutv
from sk_prod.cust_subs_hist where subscription_sub_type in ('DTV Chelsea TV','DTV MUTV')
group by account_number
;
alter table dbarnett.v250_Account_profiling add active_chelsea_TV_current tinyint;
alter table dbarnett.v250_Account_profiling add active_chelsea_TV_ever tinyint;
alter table dbarnett.v250_Account_profiling add active_MUTV_current tinyint;
alter table dbarnett.v250_Account_profiling add active_MUTV_ever tinyint;

update dbarnett.v250_Account_profiling
set active_chelsea_TV_current=case when b.active_chelsea_TV_current is null then 0 else b.active_chelsea_TV_current end
,active_chelsea_TV_ever=case when b.active_chelsea_TV_ever is null then 0 else b.active_chelsea_TV_ever end
,active_MUTV_current=case when b.active_MUTV_current is null then 0 else b.active_MUTV_current end
,active_MUTV_ever=case when b.active_MUTV_ever is null then 0 else b.active_MUTV_ever end

from dbarnett.v250_Account_profiling as a
left outer join #chelsea_mutv as b
on a.account_number = b.account_number
;
commit;

----Create Output file for analysis---

--select * from dbarnett.v250_rank_information;
--drop table dbarnett.v250_viewing_and_churn_pivot;
select a.account_number
,a.account_weight
,tenure
,affluence_septile
,financial_stress
,sky_go_reg_distinct_days_used_L06M
,sky_go_reg_distinct_days_used_L12M
,BB_type
,hdtv
,multiroom
,skyplus
,subscription_3d
,value_segment
,social_grade
,cable_area
,talk
,line_rental
,on_demand_downloads_L12M
,sky_store_PPV_L12M
,sports_premiums
,movies_premiums
,DTV_Package
,dtv_software_offers
,sports_downgrades_L06M
,sports_downgrades_Ever
,Movies_downgrades_L06M
,Movies_downgrades_Ever
,sports_upgrades_L06M
,sports_upgrades_Ever
,Movies_upgrades_L06M
,Movies_upgrades_Ever
,sports_downgrades_L12M
,Movies_downgrades_L12M
,sports_upgrades_L12M
,Movies_upgrades_L12M
,annualised_minutes_total_tv_viewed
,annualised_minutes_sports_tv_viewed
,annualised_minutes_pay_tv_viewed
,annualised_minutes_pay_sports_tv_viewed
,annualised_minutes_sky_sports_tv_viewed
,annualised_minutes_sky_movies_tv_viewed
,annualised_minutes_pay_ents_tv_viewed
,annualised_programmes_sports_tv_viewed
,annualised_programmes_pay_sports_tv_viewed
,annualised_programmes_sky_sports_tv_viewed
,bt_sport_viewer
,prev_espn_sub_L12M
,churn_events_201211_to_201310
,cuscan_events_201211_to_201310
,syscan_events_201211_to_201310
,churn_flag_201211_to_201310
,churn_events_201311_to_201402
,cuscan_events_201311_to_201402
,syscan_events_201311_to_201402
,churn_flag_201311_to_201402
,ta_events_201211_to_201310
,ta_saved_events_201211_to_201310
,ta_failed_events_201211_to_201310
,ta_flag_201211_to_201310
,ta_events_201311_to_201402
,ta_saved_events_201311_to_201402
,ta_failed_events_201311_to_201402
,ta_flag_201311_to_201402
,active_block_events_201211_to_201310
,active_block_reactivated_events_201211_to_201310
,active_block_flag_201211_to_201310
,active_block_events_201311_to_201402
,active_block_reactivated_events_201311_to_201402
,active_block_flag_201311_to_201402
,downgrade_attempts_201211_to_201310
,downgrade_saved_201211_to_201310
,downgrade_failed_201211_to_201310
,downgrade_attempts_flag_201211_to_201310
,downgrade_attempts_201311_to_201402
,downgrade_saved_201311_to_201402
,downgrade_failed_201311_to_201402
,downgrade_attempts_flag_201311_to_201402
,broadband_cancel_201211_to_201310
,broadband_cancel_201311_to_201402

----Extra variables for v2

,sky_go_devices_used
,active_stb_subs
,number_children_in_hh
,number_adults_in_hh
,number_people_in_hh

,cluster_name
,sports_stream_days
,sky_go_days
,now_tv_days
,sky_go_activity_rate
,sky_go_sports_activity_rate
,now_tv_activity_rate
,people_screen_ratio
,people_screen_ratio_grouped
,adsmartable_hh
,active_chelsea_TV_current
,active_chelsea_TV_ever
,active_MUTV_current
,active_MUTV_ever

,football_days_viewed
,football_streams
,Cricket_days_viewed
,Cricket_streams
,Golf_days_viewed
,Golf_streams
,Rugby_days_viewed
,Rugby_streams
,Motor_Sports_days_viewed
,Motor_Sports_streams
,case when number_of_sports_premiums > 0 and sports_downgrades_L12M = 0 and sports_upgrades_L12M = 0 then 'S'
when sports_downgrades_L12M > 0 or sports_upgrades_L12M > 0 then 'U'
when number_of_sports_premiums = 0 and sports_downgrades_L12M = 0 and sports_upgrades_L12M = 0 then 'N'
else 'U'
end as sky_sports


---end of extra variables
,American_Football_decile
,Athletics_decile
,Baseball_decile
,Basketball_decile
,Boxing_decile
,Cricket_decile
,Cycling_decile
,Darts_decile
,Equestrian_decile
,Extreme_decile
,Fishing_decile
,Football_decile
,Golf_decile
,Ice_Hockey_decile
,Mixed_decile
,Motor_Sport_decile
,Racing_decile
,Rugby_decile
,Snooker_Pool_decile
,Tennis_decile
,U_decile
,Watersports_decile
,Wintersports_decile
,Wrestling_decile

into dbarnett.v250_viewing_and_churn_pivot
from dbarnett.v250_Account_profiling as a
left outer join dbarnett.v250_rank_information as b
on a.account_number = b.account_number
;
commit;

grant all on dbarnett.v250_viewing_and_churn_pivot to public;
commit;

/*
select Wrestling_decile
,count(*)
from dbarnett.v250_rank_information
group by Wrestling_decile
order by Wrestling_decile
*/

--select * from dbarnett.v250_Account_profiling;

---Get max rank for each sport as an @var then divide by 9 to get 10 grops
--select max(rank_football) from #pv_summary;

/*
select account_number , football, rank_football from #pv_summary where football is not null order by rank_football desc

select count(*) from #pv_summary

commit;

select floor (rank_football/(31000)) as decile , sum(churn_flag_201311_to_201402) , count(*) 
from dbarnett.v250_Account_profiling as a
left outer join  #pv_summary as b
on a.account_number = b.account_number
group by decile
order by decile

*/



--select sum(survey_interest_watching_sports) from dbarnett.v250_Account_profiling




--select survey_responder , count(*) , sum(account_weight) as accounts from dbarnett.v250_Account_profiling group by survey_responder order by survey_responder



--select distinct grouped_channel from v250_channel_to_service_key_lookup_deduped order by grouped_channel
--select count(*) from #summary_by_account_for_profiling;

/*
select multiscreen
,count(*)
from dbarnett.v250_Account_profiling 
group by multiscreen
order by multiscreen


select sky_go_extra
,count(*)
from dbarnett.v250_Account_profiling 
group by sky_go_extra
order by sky_go_extra


annualised_programmes_viewed_eurosport

select first_bb_date
,count(*)
from #bb_summary
group by first_bb_date
order by first_bb_date


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


Update          dbarnett.v250_Account_profiling
set             bb_type = case when bb_type is null then '6) NA' 
                                 when bb_type='NA' then '6) NA'
                            else bb_type end
from            dbarnett.v250_Account_profiling as a

select subscription_type
,subscription_sub_type
from sk_prod.cust_subs_hist
where effective_to_dt ='9999-09-09'
group by subscription_type
,subscription_sub_type
order by subscription_type
,subscription_sub_type
;
subscription_type,subscription_sub_type
'SKY TALK','SKY TALK LINE RENTAL'

select 
                ,count(distinct cb_data_date) as distinct_days_used
into            #skygo_usage
from            SK_PROD.SKY_PLAYER_USAGE_DETAIL


select * from             SK_PROD.SKY_PLAYER_USAGE_DETAIL where account_number = '620041578563' order by activity_dt
commit;
grant all on dbarnett.v250_rights_broadcast_overall to public;
grant all on dbarnett.v250_rights_broadcast_by_live_status  to public; commit;

select * into dbarnett.v250_rights_broadcast_overall_copy from dbarnett.v250_rights_broadcast_overall; commit;


select sky_go_days
,first_sky_go_date
,datediff(day,first_sky_go_date,@analysis_date) as days_since_start
,sky_go_days/cast(datediff(day,first_sky_go_date,@analysis_date)as real)
from dbarnett.v250_Account_profiling as a

*/


