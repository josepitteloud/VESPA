
--drop table dbarnett.Project_114_ADSMART_ACCOUNT_ATTRIBUTES;

SELECT

metropolitan_area_and_itv_region as region
,           CASE WHEN base.Total_miss_pmt = 0  THEN  0
                                  WHEN base.Total_miss_pmt >= 1 THEN  1
                                  ELSE 0
                                  END as Prev_miss_pmt

,          CASE WHEN base.Sports_downgrades = 0  THEN  0
                                  WHEN base.Sports_downgrades >= 1 THEN  1
                                  ELSE 0
                                  END as Sports_downgrade

,         CASE WHEN base.Movies_downgrades = 0  THEN 0
                                  WHEN base.Movies_downgrades >= 1 THEN  1
                                  ELSE 0
                                  END as Movies_downgrade

,                 CASE WHEN base.Sky_Go_Reg = 0 THEN  0
                                  WHEN base.Sky_Go_Reg = 1 THEN  1
                                  ELSE 0
                                  END as Sky_Go_Reg

,              CASE WHEN Lifestage = '00' and Head_of_HH_age_band = '16 to 24'  THEN 'Very young adults (Age 16-24)' --Very young family
                                  WHEN Lifestage = '01' and Head_of_HH_age_band = '16 to 24'  THEN 'Very young adults (Age 16-24)' --Very young single
                                  WHEN Lifestage = '02' and Head_of_HH_age_band = '16 to 24'  THEN 'Very young adults (Age 16-24)' --Very young homesharers
                                  WHEN Lifestage = '03' and Head_of_HH_age_band = '25 to 35'  THEN 'Young adults (25-35)'      --Young family
                                  WHEN Lifestage = '04' and Head_of_HH_age_band = '25 to 35'  THEN 'Young adults (25-35)'      --Young single
                                  WHEN Lifestage = '05' and Head_of_HH_age_band = '25 to 35'  THEN 'Young adults (25-35)'      --Young homesharers
                                  WHEN Lifestage = '06' and Head_of_HH_age_band = '36 to 45'  THEN 'Mature adults (36-45)'     --Mature family
                                  WHEN Lifestage = '07' and Head_of_HH_age_band = '36 to 45'  THEN 'Mature adults (36-45)'     --Mature singles
                                  WHEN Lifestage = '08' and Head_of_HH_age_band = '36 to 45'  THEN 'Mature adults (36-45)'     --Mature homesharers
                                  WHEN Lifestage = '09' and Head_of_HH_age_band = '46 to 55'  THEN 'Middle-aged adults (46-55)' --Older family
                                  WHEN Lifestage = '10' and Head_of_HH_age_band = '46 to 55'  THEN 'Middle-aged adults (46-55)' --Older single
                                  WHEN Lifestage = '11' and Head_of_HH_age_band = '46 to 55'  THEN 'Middle-aged adults (46-55)' --Older homesharers
                                  WHEN Lifestage = '09' and Head_of_HH_age_band = '56 to 65'  THEN 'Older adults (56-65)'       --Older family
                                  WHEN Lifestage = '10' and Head_of_HH_age_band = '56 to 65'  THEN 'Older adults (56-65)'       --Older single
                                  WHEN Lifestage = '11' and Head_of_HH_age_band = '56 to 65'  THEN 'Older adults (56-65)'       --Older homesharers
                                  WHEN Lifestage = '12' and Head_of_HH_age_band = '66 Plus'   THEN 'Elderly adults(65+)'        --Elderly family
                                  WHEN Lifestage = '13' and Head_of_HH_age_band = '66 Plus'   THEN 'Elderly adults(65+)'        --Elderly single
                                  WHEN Lifestage = '14' and Head_of_HH_age_band = '66 Plus'   THEN 'Elderly adults(65+)'        --Elderly homesharers
                                  WHEN Lifestage = 'U'                                        THEN 'Unclassified'
                                  ELSE 'Unclassified'
                                  END as h_lifestage
,value_segment

,                    CASE WHEN base.sky_id = 0 THEN  0
                                  WHEN base.sky_id = 1 THEN  1
                                  ELSE 0
                                  END as sky_id
,case when BB_type is null then 0 else 1 end as bb
,case when talk_product is null then 0 else 1 end as talk
,case when HDTV=1 then 1 else 0 end as hd
,case when Anytime_plus =1 then 1 else 0 end as anytimeplus
,case when Pending_cancel =1 then 1 else 0 end as in_pending_cancel
,case when ent_extra =1 then 1 else 0 end as entertainment_extra
,case when ESPN_Subscribers =1 then 1 else 0 end as espn
,case when multiroom =1 then 1 else 0 end as MR

,case when skyplus =1 then 1 else 0 end as sky_plus
,case when Cable_area is null then 1 when cable_area='N/A' then 1 when cable_area='Y' then 1  else 0 end as in_cable_area


,case when h_mosaic_uk_2009_group is null then 'p )Unclassified' when h_mosaic_uk_2009_group ='missing' then 'p )Unclassified' else h_mosaic_uk_2009_group end as demographic_and_lifestyle
,case when H_AFFLUENCE is null then 'H) Unknown' when H_AFFLUENCE='M' then 'H) Unknown' when H_AFFLUENCE='U' then 'H) Unknown'
      WHEN H_AFFLUENCE IN ('00','01','02')       THEN 'A) Very Low'
                                                WHEN H_AFFLUENCE IN ('03','04', '05')      THEN 'B) Low'
                                                WHEN H_AFFLUENCE IN ('06','07','08')       THEN 'C) Mid Low'
                                                WHEN H_AFFLUENCE IN ('09','10','11')       THEN 'D) Mid'
                                                WHEN H_AFFLUENCE IN ('12','13','14')       THEN 'E) Mid High'
                                                WHEN H_AFFLUENCE IN ('15','16','17')       THEN 'F) High'
                                                WHEN H_AFFLUENCE IN ('18','19')            THEN 'G) Very High' else 'H) Unknown' end as affluence_band
,case when Sky_Reward_L12 is null then 'No' when Sky_Reward_L12>0 then 'Yes' else 'No' end as sky_reward_L12M
,case when Financial_outlook  is null then 'U Unallocated' when financial_outlook ='missing' then 'U Unallocated' else financial_outlook end as financial_outlook_group
,case when HomeOwner is null then 'No' else HomeOwner end as HomeOwner_group

--, case when h_lifestage  is null then 'g) Unclassified' when h_lifestage = 'missing' then 'g) Unclassified' else h_lifestage end as h_lifestage_group

,CASE Lifestage                 WHEN '00'  THEN 'Very young family'
                                                            WHEN '01'  THEN 'Very young single'
                                                            WHEN '02'  THEN 'Very young homesharers'
                                                            WHEN '03'  THEN 'Young family'
                                                            WHEN '04'  THEN 'Young single'
                                                            WHEN '05'  THEN 'Young homesharers'
                                                            WHEN '06'  THEN 'Mature family'
                                                            WHEN '07'  THEN 'Mature singles'
                                                            WHEN '08'  THEN 'Mature homesharers'
                                                            WHEN '09'  THEN 'Older family'
                                                            WHEN '10'  THEN 'Older single'
                                                            WHEN '11'  THEN 'Older homesharers'
                                                            WHEN '12'  THEN 'Elderly family'
                                                            WHEN '13'  THEN 'Elderly single'
                                                            WHEN '14'  THEN 'Elderly homesharers'
                                                            WHEN 'U'   THEN 'Unclassified'
                                                            ELSE            'Unclassified'
                                                            END

            as h_lifestage_full 

,case when Kids_Aged_LE4  is null then 'N' when Kids_Aged_LE4  ='M' then 'N' else Kids_Aged_LE4   end as Kids_Aged_LE4_group
,case when Kids_Aged_5to11     is null then 'N' when Kids_Aged_5to11     ='M' then 'N' else Kids_Aged_5to11     end as Kids_Aged_5to11_group
, case when Kids_Aged_12to17       is null then 'N' when Kids_Aged_12to17        ='M' then 'N' else Kids_Aged_12to17       end as Kids_Aged_12to17_group
,case when MIRROR_MEN_MIN is null then 'No Mirror' else MIRROR_MEN_MIN end as MIRROR_MEN_MIN_GROUP
, case when MIRROR_WOMEN_MIN is null then 'No Mirror' else MIRROR_WOMEN_MIN end as MIRROR_WOMEN_MIN_GROUP
,case when Mirror_has_children   is null then 'Y' when Mirror_has_children='M' then 'Y' when Mirror_has_children='missing' then 'Y'  else Mirror_has_children   end as Mirror_has_children_GROUP
,case when Mirror_ABC1     is null then 'Y'  else Mirror_ABC1  end as Mirror_ABC1_Group 
,case when Total_miss_pmt  is null then 'No' when  Total_miss_pmt>0 then 'Yes' else 'No'  end as previous_missed_payments
,case when Movies_downgrades  is null then 'No' when  Movies_downgrades>0 then 'Yes' else 'No'  end as Movies_downgrades_group
,case when sports_downgrades  is null then 'No' when  sports_downgrades>0 then 'Yes' else 'No'  end as sports_downgrades_group
,case when current_offer  is null then 'No' when  current_offer>0 then 'Yes' else 'No'  end as current_offer_group
--, case when barb_desc_itv       is null then 'Unknown' else barb_desc_itv     end as region
,case when Sky_Go_Reg is null then 'b) No'  when sky_go_reg=1 then 'a) Yes'  when sky_go_reg=0 then 'b) No' else  'b) No' end as sky_go_reg_group
,case when Sky_cust_life is null then 'E) missing'  else  Sky_cust_life end as Sky_cust_life_group
,case when TA_attempts  is null then 'No' when  TA_attempts>0 then 'Yes' else 'No'  end as TA_attempts_group
,case when value_segment is null then 'Unknown' when value_segment='missing' then 'Unknown' else value_segment end as value_segment_group

,case when sky_sports_1 =1 then 'Yes' else 'No' end as has_sky_sports_1
,case when sky_sports_2 =1 then 'Yes' else 'No' end as has_sky_sports_2
,case when  sky_sports_1 =1 and sky_sports_2 =1 then 'Yes' else 'No' end as has_sky_sports_3
,case when movies_1 =1 then 'Yes' else 'No' end as has_movies_1
,case when movies_2 =1 then 'Yes' else 'No' end as has_movies_2
,case when  movies_1 =1 and movies_2 =1 then 'Yes' else 'No' end as has_movies_premiere
,case when disney=1 then 'Yes' else 'No' end as has_disney

,case when T_AdSm_box>0 then 1 else 0 end as adsmartable_hh
,1 as total_households

INTO dbarnett.Project_114_ADSMART_ACCOUNT_ATTRIBUTES
FROM mustaphs.ADSMART as Base
where country<>'Ireland (Eire)';
commit;

grant all on Project_114_ADSMART_ACCOUNT_ATTRIBUTES to public;

drop table Project_114_ADSMART_ACCOUNT_ATTRIBUTES_GROUPED;

 select case when region is null then 'Unknown' else region end as  account_region
,         Prev_miss_pmt

,         Sports_downgrade

,          Movies_downgrade

,          Sky_Go_Reg

,value_segment

,            sky_id
,bb
,talk
,hd
,anytimeplus
,in_pending_cancel
,entertainment_extra
,espn
,MR

,sky_plus
,in_cable_area


,demographic_and_lifestyle
,affluence_band
,sky_reward_L12M
,financial_outlook_group
,HomeOwner_group

,h_lifestage
,h_lifestage_full as lifestage_bands

,Kids_Aged_LE4_group
,Kids_Aged_5to11_group
,Kids_Aged_12to17_group
,MIRROR_MEN_MIN_GROUP
,MIRROR_WOMEN_MIN_GROUP
,Mirror_has_children_GROUP
,Mirror_ABC1_Group 
,previous_missed_payments
,Movies_downgrades_group
,sports_downgrades_group
,current_offer_group

,sky_go_reg_group
,Sky_cust_life_group
,TA_attempts_group
,value_segment_group

,has_sky_sports_1
,has_sky_sports_2
,has_sky_sports_3
,has_movies_1
,has_movies_2
,has_movies_premiere
,has_disney

,adsmartable_hh
,sum(total_households) as number_of_hh

INTO dbarnett.Project_114_ADSMART_ACCOUNT_ATTRIBUTES_GROUPED
FROM dbarnett.Project_114_ADSMART_ACCOUNT_ATTRIBUTES
group by  account_region
,         Prev_miss_pmt

,         Sports_downgrade

,          Movies_downgrade

,          Sky_Go_Reg


,value_segment

,            sky_id
,bb
,talk
,hd
,anytimeplus
,in_pending_cancel
,entertainment_extra
,espn
,MR

,sky_plus
,in_cable_area


,demographic_and_lifestyle
,affluence_band
,sky_reward_L12M
,financial_outlook_group
,HomeOwner_group


,lifestage_bands

,Kids_Aged_LE4_group
,Kids_Aged_5to11_group
,Kids_Aged_12to17_group
,MIRROR_MEN_MIN_GROUP
,MIRROR_WOMEN_MIN_GROUP
,Mirror_has_children_GROUP
,Mirror_ABC1_Group 
,previous_missed_payments
,Movies_downgrades_group
,sports_downgrades_group
,current_offer_group
, h_lifestage
,sky_go_reg_group
,Sky_cust_life_group
,TA_attempts_group
,value_segment_group

,has_sky_sports_1
,has_sky_sports_2
,has_sky_sports_3
,has_movies_1
,has_movies_2
,has_movies_premiere
,has_disney

,adsmartable_hh;
commit;

grant all on Project_114_ADSMART_ACCOUNT_ATTRIBUTES_GROUPED to public;
commit;

select count(*) from Project_114_ADSMART_ACCOUNT_ATTRIBUTES_GROUPED;















--select top 500 * from Project_114_ADSMART_ACCOUNT_ATTRIBUTES_GROUPED

select account_region , sum(number_of_hh) from Project_114_ADSMART_ACCOUNT_ATTRIBUTES_GROUPED group by account_region
select value_segment , sum(number_of_hh) from Project_114_ADSMART_ACCOUNT_ATTRIBUTES_GROUPED group by value_segment


 select 
--case when region is null then 'Unknown' else region end as  account_region
--,
Prev_miss_pmt
,Sports_downgrade

,Movies_downgrade

,Sky_Go_Reg
, h_lifestage
,value_segment_group
,sky_id
,bb
,talk
,hd
,anytimeplus
,in_pending_cancel
,entertainment_extra
,espn
,MR


,sports_premiums
,movies_premiums
,sky_plus
,in_cable_area

,adsmartable_hh
,sum(total_households) as number_of_hh

INTO dbarnett.Project_114_ADSMART_ACCOUNT_ATTRIBUTES_GROUPED_EXC_REGION
FROM dbarnett.Project_114_ADSMART_ACCOUNT_ATTRIBUTES
group by 
-- account_region
--,
Prev_miss_pmt
,Sports_downgrade

,Movies_downgrade

,Sky_Go_Reg
, h_lifestage
,value_segment_group
,sky_id
,bb
,talk
,hd
,anytimeplus
,in_pending_cancel
,entertainment_extra
,espn
,MR
,sports_premiums
,movies_premiums
,sky_plus
,in_cable_area
,adsmartable_hh;
commit;

grant all on Project_114_ADSMART_ACCOUNT_ATTRIBUTES_GROUPED_EXC_REGION to public;

select count(*) from Project_114_ADSMART_ACCOUNT_ATTRIBUTES_GROUPED

commit;

--select top 100 * from  mustaphs.ADSMART
--drop table ADSMART_ACCOUNT_ATTRIBUTES;commit;

--select Cable_area   , count(*) from  mustaphs.ADSMART group by Cable_area  
--select pty_country_code , count(*) from  mustaphs.ADSMART group by pty_country_code
--select T_AdSm_box , count(*) as records from dbarnett.ADSMART_ACCOUNT_ATTRIBUTES group by t_adsm_box order by t_adsm_box;


--select top 100 * from rangep.SBO_dtv_base;
--select vespa_seg , count(*) from rangep.SBO_dtv_base group by vespa_seg order by vespa_seg;
