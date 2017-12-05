/*-----------------------------------------------------------------------------------------------------------------
        Project:V250 - Sports right Analysis Profiling
        Version: 1
        Created: 2013-10-10
        
        Analyst: Dan Barnett
        SK Prod: 5

        Collate all profiling variables in to a single table from sources such as Adsmart table and other projects

*/------------------------------------------------------------------------------------------------------------------

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

*/
