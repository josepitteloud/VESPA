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
/*
,case   when sky_sports_1=1 and  sky_sports_2=1 and movies_1=1 and movies_2=1 then '1: All Premiums'
        when sky_sports_1=1 and  sky_sports_2=1 and movies_1=0 and movies_2=0  then '2: Dual Sports'
        when sky_sports_1=0 and  sky_sports_2=0 and movies_1=1 and movies_2=1  then '3: Dual Movies'
        when sky_sports_1+sky_sports_2+movies_1+movies_2>0  then '4: Other Premiums' else '5: No Premiums' end as premium_details
*/ 


,case   when sky_sports_1+sky_sports_2>0 then 1 else 0 end as sports_premiums
,case   when movies_1+movies_2>0 then 1 else 0 end as movies_premiums
,case when skyplus =1 then 1 else 0 end as sky_plus
,case when Cable_area is null then 1 when cable_area='N/A' then 1 when cable_area='Y' then 1  else 0 end as in_cable_area

,case when T_AdSm_box>0 then 1 else 0 end as adsmartable_hh
,1 as total_households

INTO dbarnett.Project_114_ADSMART_ACCOUNT_ATTRIBUTES
FROM mustaphs.ADSMART as Base
where country<>'Ireland (Eire)';
commit;

grant all on Project_114_ADSMART_ACCOUNT_ATTRIBUTES to public;

--drop table Project_114_ADSMART_ACCOUNT_ATTRIBUTES_GROUPED;

 select case when region is null then 'Unknown' else region end as  account_region
,Prev_miss_pmt
,Sports_downgrade

,Movies_downgrade

,Sky_Go_Reg
, h_lifestage
,case when value_segment is null then 'Unknown' else value_segment end as account_value_segment 
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

INTO dbarnett.Project_114_ADSMART_ACCOUNT_ATTRIBUTES_GROUPED
FROM dbarnett.Project_114_ADSMART_ACCOUNT_ATTRIBUTES
group by  account_region
,Prev_miss_pmt
,Sports_downgrade

,Movies_downgrade

,Sky_Go_Reg
, h_lifestage
,account_value_segment
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

grant all on Project_114_ADSMART_ACCOUNT_ATTRIBUTES_GROUPED to public;

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
,case when value_segment is null then 'Unknown' else value_segment end as account_value_segment 
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
,account_value_segment
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
