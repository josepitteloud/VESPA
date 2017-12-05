----THIS CODE IN CITeam, CITeam ------
----RUN THIS CODE IN THE CITeam SCHEMA

/*
                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES


        SKY ADSMART CUSTOMER BASE FILE CREATION
        --------------------------------
        Author  : Jitesh Patel
        Date    : 12th June 2012


--select top 100 * from mustaphs.AdSmart;
--select value_segment, count(*) from AdSmart GROUP BY value_segment;


SECTIONS
----------------
converts the analysis file into the stageing file
*/


--drop table CITeam.ADSMART_IT;
SELECT
 4 AS Record_type
,account_number
,3 AS Version_number            --2 for update and 3 for full refresh
,cb_key_household
,cb_key_DB_Person
,cb_key_individual
,model_score
,viewing_panel_id = base.panel_id_vespa
,ACCT_CUST_ACCOUNT_ID
,MIRROR_MEN             =    CASE WHEN base.MIRROR_MEN_MIN = 'MI'   THEN 'MI'
                                  WHEN base.MIRROR_MEN_MIN = 'MII'  THEN 'MII'
                                  WHEN base.MIRROR_MEN_MIN = 'MIII' THEN 'MIII'
                                  WHEN base.MIRROR_MEN_MIN = 'MIV'  THEN 'MIV'
                                  WHEN base.MIRROR_MEN_MIN = 'MV'   THEN 'MV'
                                  WHEN base.MIRROR_MEN_MIN = 'MVI'  THEN 'MVI'
                                  WHEN base.MIRROR_MEN_MIN = 'MVII' THEN 'MVII'
                                  ELSE null
                                  END
,MIRROR_WOMEN           =    CASE WHEN base.MIRROR_WOMEN_MIN = 'WI'   THEN 'WI'
                                  WHEN base.MIRROR_WOMEN_MIN = 'WII'  THEN 'WII'
                                  WHEN base.MIRROR_WOMEN_MIN = 'WIII' THEN 'WIII'
                                  WHEN base.MIRROR_WOMEN_MIN = 'WIV'  THEN 'WIV'
                                  WHEN base.MIRROR_WOMEN_MIN = 'WV'   THEN 'WV'
                                  WHEN base.MIRROR_WOMEN_MIN = 'WVI'  THEN 'WVI'
                                  WHEN base.MIRROR_WOMEN_MIN = 'WVII' THEN 'WVII'
                                  ELSE null
                                  END
/*
,Mirror_has_children    =    CASE WHEN base.Family_Lifestage NOT IN ('02','03','06','07','10') THEN  'No'
                                  WHEN base.Family_Lifestage IN     ('02','03','06','07','10') THEN  'Yes'
                                  ELSE null
                                  END
*/
,Mirror_has_children    =    CASE WHEN base.Mirror_has_children = 'N' THEN  'No'
                                  WHEN base.Mirror_has_children = 'Y' THEN  'Yes'
                                  ELSE null
                                  END
/*
,Mirror_ABC1            =    CASE WHEN base.h_mosaic_uk_2009_type NOT IN ('01','02','03','04','05','06','07','08','09','10','11'
                                                                         ,'15','20','22','29','30','31','33','61','62','63','65','66') THEN  'No'
                                  WHEN base.h_mosaic_uk_2009_type     IN ('01','02','03','04','05','06','07','08','09','10','11'
                                                                         ,'15','20','22','29','30','31','33','61','62','63','65','66') THEN  'Yes'
                                  ELSE null
                                  END
*/

,Mirror_ABC1            =    CASE WHEN base.social_grade IN ('A','B','C1')     THEN  'Yes'
                                  WHEN base.social_grade IN ('C2','D','E')     THEN  'No'
                                  ELSE null
                                  END

,Kids_Age_LE4           =    CASE WHEN base.Kids_Aged_LE4 = 'N' THEN  'No'              --this may need to change
                                  WHEN base.Kids_Aged_LE4 = 'Y' THEN  'Yes'
                                  ELSE null
                                  END
,Kids_Age_4to9          =    CASE WHEN base.Kids_Aged_5to11 = 'N' THEN  'No'            --this may need to change
                                  WHEN base.Kids_Aged_5to11 = 'Y' THEN  'Yes'
                                  ELSE null
                                  END
,Kids_Age_10to15        =    CASE WHEN base.Kids_Aged_12to17 = 'N' THEN  'No'           --this may need to change
                                  WHEN base.Kids_Aged_12to17 = 'Y' THEN  'Yes'
                                  ELSE null
                                  END
,Demographic            =    CASE h_mosaic_uk_2009_group    WHEN    'A' THEN    'Alpha Territory'
                                                            WHEN    'B' THEN    'Professional Rewards'
                                                            WHEN    'C' THEN    'Rural Solitude'
                                                            WHEN    'D' THEN    'Small Town Diversity'
                                                            WHEN    'E' THEN    'Active Retirement'
                                                            WHEN    'F' THEN    'Suburban Mindsets'
                                                            WHEN    'G' THEN    'Careers and Kids'
                                                            WHEN    'H' THEN    'New Homemakers'
                                                            WHEN    'I' THEN    'Ex-Council Community'
                                                            WHEN    'J' THEN    'Claimant Cultures'
                                                            WHEN    'K' THEN    'Upper Floor Living'
                                                            WHEN    'L' THEN    'Elderly Needs'
                                                            WHEN    'M' THEN    'Industrial Heritage'
                                                            WHEN    'N' THEN    'Terraced Melting Pot'
                                                            WHEN    'O' THEN    'Liberal Opinions'
                                                            WHEN    'U' THEN    'Unclassified'
                                                            ELSE                null
                                                            END

,Financial_outlook     =     CASE h_fss_v3_group            WHEN    'A' THEN    'Bright Futures'
                                                            WHEN    'B' THEN    'Single Endeavours'
                                                            WHEN    'C' THEN    'Young Essentials'
                                                            WHEN    'D' THEN    'Growing Rewards'
                                                            WHEN    'E' THEN    'Family Interest'
                                                            WHEN    'F' THEN    'Accumulated Wealth'
                                                            WHEN    'G' THEN    'Consolidating Assets'
                                                            WHEN    'H' THEN    'Balancing Budgets'
                                                            WHEN    'I' THEN    'Stretched Finances'
                                                            WHEN    'J' THEN    'Established Reserves'
                                                            WHEN    'K' THEN    'Seasoned Economy'
                                                            WHEN    'L' THEN    'Platinum Pensions'
                                                            WHEN    'M' THEN    'Sunset Security'
                                                            WHEN    'N' THEN    'Traditional Thrift'
                                                            WHEN    'U' THEN    'Unclassified'
                                                            ELSE                null
                                                            END

,H_AFFLUENCE           =     CASE WHEN base.affluence_group = 'A) Very Low'     THEN 'Very Low'
                                  WHEN base.affluence_group = 'B) Low'          THEN 'Low'
                                  WHEN base.affluence_group = 'C) Mid Low'      THEN 'Mid Low'
                                  WHEN base.affluence_group = 'D) Mid'          THEN 'Mid'
                                  WHEN base.affluence_group = 'E) Mid High'     THEN 'Mid High'
                                  WHEN base.affluence_group = 'F) High'         THEN 'High'
                                  WHEN base.affluence_group = 'G) Very High'    THEN 'Very High'
                                  ELSE null
                                  END

,HomeOwner             =     CASE WHEN base.tenure in ('1','2') THEN  'No'
                                  WHEN base.tenure =  ('0')     THEN  'Yes'
                                  ELSE null
                                  END
/*
,h_lifestage      =          CASE Lifestage                 WHEN '00'  THEN 'Very young family'
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
                                                            ELSE            null
                                                            END
*/

,h_lifestage      =          CASE WHEN Lifestage = '00' and Head_of_HH_age_band = '16 to 24'  THEN 'Very young adults (Age 16-24)' --Very young family
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
                                  ELSE null
                                  END

,region = metropolitan_area_and_itv_region

,Cable_area             =    CASE WHEN base.Cable_area = 'N' THEN  'No'
                                  WHEN base.Cable_area = 'Y' THEN  'Yes'
                                  ELSE null
                                  END
,Sky_Go_Reg             =    CASE WHEN base.Sky_Go_Reg = 1 THEN  'Yes'
                                  ELSE 'No'
                                  END
,sky_id                 =    CASE WHEN base.sky_id = 1 THEN  'Yes'
                                  ELSE 'No'
                                  END
,value_segment

,sky_rewards            =    CASE WHEN base.SKY_T_Rewards >= 1 THEN  'Yes'
                                  ELSE 'No'
                                  END
,Turnaround_events      =    CASE WHEN base.TA_attempts >= 1 THEN  'Yes'
                                  ELSE 'No'
                                  END
,Prev_miss_pmt          =    CASE WHEN base.Total_miss_pmt >= 1 THEN  'Yes'
                                  ELSE 'No'
                                  END
,Sports_downgrade       =    CASE WHEN base.Sports_downgrades >= 1 THEN  'Yes'
                                  ELSE 'No'
                                  END

,Movies_downgrade       =    CASE WHEN base.Movies_downgrades >= 1 THEN  'Yes'
                                  ELSE 'No'
                                  END
,Current_offer          =    CASE WHEN base.current_offer >= 1 THEN  'Yes'
                                  ELSE 'No'
                                  END
,Sky_cust_life          =    CASE WHEN base.Sky_cust_life = 'A) Welcome' THEN  'Welcome'
                                  WHEN base.Sky_cust_life = 'B) Mid'     THEN  'Mid'
                                  WHEN base.Sky_cust_life = 'C) End'     THEN  'End'
                                  WHEN base.Sky_cust_life = 'D) 15+'     THEN  '15+'
                                  WHEN base.Sky_cust_life = 'E) missing' THEN  null
                                  ELSE null
                                  END

,government_region
,bt_fibre_area
,exchange_id
,exchange_status
,exchange_unbundled
,household_composition
,isba_tv_region
,current_package
,Box_Type
,tenure     = cust_tenure
INTO CITeam.ADSMART_IT
FROM mustaphs.ADSMART as Base;

--------------------------------------------------------------------------------

--select top 100 * from AdSmart_IT;
--select top 100 * from mustaphs.AdSmart;


GRANT SELECT ON CITeam.ADSMART_IT TO PUBLIC;


select top 100 *
from CITeam.ADSMART_IT
where cb_key_individual is not null;


select top 100 * from CITeam.ADSMART_IT;


--------------------------------------------------------------------------------
--QA the final table
--------------------------------------------------------------------------------

select tenure, count(*)
from CITeam.AdSmart_IT
group by tenure

--------------------------------------------------------------------------------

select SKY_T_Rewards, count(*)
from mustaphs.AdSmart
group by SKY_T_Rewards

--------------------------------------------------------------------------------








