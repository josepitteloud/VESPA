/*  Title       : Adsmart Table/View Build Process 
    Created by  : Tafline James-William
    Date        : 26 November 2012
    Description : This is a sql to build the ADSMART Table FROM the CUST_SINGLE_ACCOUNT view AND other tables.
                : This table potentially replaces the CITEAM.ADSMART_IT table

    Modified by : Tafline James-William
    Changes     : Changed to set the following fields null in the Adsmart Table - exchange_id, exchange_status, exchange_unbundled AND model_score
                : Changed the filter that builds the adsmart table - to fix issues with the existing duplication issue
                :  DE - Changed TEMP_PREV_MISS_PMT table build selection to cater for 14 day period WHERE unbilled is a valid status not actually a missed bill
                :  SM - Commented out field sky_rewards AND added new field used_sky_store as part of CN 1631 CR 08 : 23/01/2014
                :  SM - Commented out field sky_rewards AND added new field used_sky_store as part of CN 1631 CR 08 : 23/01/2014
                :  SM - Commented derivation logic AND update for field sky_rewards AND added new rule for used_sky_store : 23/01/2014
                :  SM - Updated hardcoded version number FROM 3 to 4 : 23/01/2014
                :  SM - Added new field Engagement matrix score - defaulted to HIGH : 23/01/2014
                :  SM - Added new field on demand in last 6 mnths - defaulted to No : 23/01/2014
                :  SM - Added new field had espn on 1st apr 2013- defaulted to No : 23/01/2014
                :  SM - Added new mapping rules for had espn AND sky line fields : 23/01/2014
                :  SM - Changed BT_FIBRE_AREA field to be caleld as FIBRE_AVIALBALE : 07/02/2014
                :  SM - Updated population of field on_demand_in_last_6_months as per CCN 1683 : 24/03/2014
		:  MJ - Updated derivation of field had_espn_on_1st_april_2013 for INC0072125 : 04/06/2014
		:  MJ - Added field cb_address_postcode_area field for CCN1789 : 16/09/2014
		:  MJ - Added 26 new fields for Adsmart L3 Fixed Attributes Drop 1 : 08/10/2014
		:  MJ - Adsmart L3 Fixed Attributes Drop 1 Defect fix 26493 
		:  MJ - Adsmart L3 Fixed Attributes Drop 1 Defect fix 26739 : 11/11/2014


*/
MESSAGE 'Process to build the ADSMART Table & View' type status to client
go
MESSAGE 'Drop table ADSMART AND view if it already exists' type status to client
IF EXISTS(SELECT tname FROM syscatalog 
            WHERE creator='' 
              AND UPPER(tname)='ADSMART' 
              AND UPPER(tabletype)='TABLE')
    BEGIN
        DROP TABLE ADSMART
        IF EXISTS(SELECT tname FROM syscatalog 
            WHERE creator= user_name()  
              AND upper(tname)='ADSMART' 
              AND upper(tabletype)='VIEW')
          BEGIN
             DROP VIEW  ADSMART
          END
        ELSE
          BEGIN
            MESSAGE 'WARN: Table ADSMART exists. View  ADSMART does not exists' type status to client
          END
    END
go

MESSAGE 'Create Table ADSMART' type status to client
CREATE TABLE ADSMART
(
    record_type             integer         NULL DEFAULT NULL,
    account_number          varchar(20)     NULL DEFAULT NULL,
    version_number          integer         NULL DEFAULT NULL,
    cb_key_household        bigint          NULL DEFAULT NULL,
    cb_key_db_person        bigint          NULL DEFAULT NULL,
    cb_key_individual       bigint          NULL DEFAULT NULL,
    model_score             integer         NULL DEFAULT NULL,
    viewing_panel_id        integer         NULL DEFAULT NULL,
    src_system_id           varchar(50)     NULL DEFAULT NULL,
    mirror_men              varchar(5)      NULL DEFAULT NULL,
    mirror_women            varchar(5)      NULL DEFAULT NULL,
    mirror_has_children     varchar(50)     NULL DEFAULT 'Unknown',
    mirror_abc1             varchar(3)      NULL DEFAULT NULL,
    child_0_to_4            varchar(7)      NULL DEFAULT 'Unknown',
    child_5_to_11           varchar(7)      NULL DEFAULT 'Unknown',
    child_12_to_17          varchar(7)      NULL DEFAULT 'Unknown',
    demographic             varchar(50)     NULL DEFAULT 'Unknown',
    financial_outlook       varchar(50)     NULL DEFAULT 'Unknown',
    h_affluence             varchar(25)     NULL DEFAULT NULL,
    homeowner               varchar(50)     NULL DEFAULT NULL,
    h_lifestage             varchar(50)     NULL DEFAULT 'Unknown',
    region                  varchar(70)     NULL DEFAULT NULL,
    cable_area              varchar(7)      NULL DEFAULT 'Unknown',
    sky_go_reg              varchar(3)      NULL DEFAULT 'No',
    sky_id                  varchar(3)      NULL DEFAULT NULL,
    value_segment           varchar(50)     NULL DEFAULT 'Unknown',
    --sky_rewards             varchar(3)      NULL DEFAULT NULL,
    -- Commented sky_rewards as per CCN1631 -CR 008	
    used_sky_store             varchar(3)      NULL DEFAULT 'No',
    turnaround_events       varchar(3)      NULL DEFAULT NULL,
    prev_miss_pmt           varchar(3)      NULL DEFAULT NULL,
    sports_downgrade        varchar(3)      NULL DEFAULT 'No',
    movies_downgrade        varchar(3)      NULL DEFAULT 'No',
    current_offer           varchar(3)      NULL DEFAULT 'No',
    sky_cust_life           varchar(20)     NULL DEFAULT 'Mid',
    government_region       varchar(50)     NULL DEFAULT 'Unknown',
    --bt_fibre_area           varchar(10)     NULL DEFAULT NULL,
    -- Commented sky_rewards as per CCN1631 -CR 008
    exchange_id             varchar(10)     NULL DEFAULT NULL,
    exchange_status         varchar(10)     NULL DEFAULT NULL,
    exchange_unbundled      varchar(10)     NULL DEFAULT NULL,
    household_composition   varchar(35)     NULL DEFAULT 'Unknown',
    isba_tv_region          varchar(40)     NULL DEFAULT NULL,
    current_package         varchar(50)     NULL DEFAULT 'Unknown',
    box_type                varchar(30)     NULL DEFAULT 'Unknown',
    tenure                  varchar(20)     NULL DEFAULT NULL,
    social_class            varchar(15)     NULL DEFAULT 'Unknown',
    engagement_matrix_score  varchar(4)     NULL DEFAULT 'High',
    sky_phone_line	     varchar(3)     NULL DEFAULT 'No',
    on_demand_in_last_6_months	varchar(3)     NULL DEFAULT 'No',
    had_espn_on_1st_april_2013	varchar(3)     NULL DEFAULT 'No',
    cb_address_postcode		varchar(8) NULL DEFAULT NULL, -- Q1 Sports change
    onnet_bb_area 		varchar(3)     NULL DEFAULT 'No', -- Q1 Sports change
    cb_address_postcode_area 	varchar(7) NULL DEFAULT 'Unknown', -- CCN1789
-- Adsmart L3 Drop 1 Start
	broadband_status 	varchar(100) NULL DEFAULT 'Never Had BB',
	tenure_split 		varchar(100) NULL DEFAULT 'Unknown',
	sky_go_extra		varchar(100) NULL DEFAULT 'Never had Sky Go Extra',
	primary_box_type	varchar(100) NULL DEFAULT 'Unknown',
	hd_status		VARCHAR(100) NULL DEFAULT 'Never had HD',
	mr_status		VARCHAR(100) NULL DEFAULT 'Never had MR',
	talk_status		VARCHAR(100) NULL DEFAULT 'Never had Talk',
	movies_status		VARCHAR(100) NULL DEFAULT 'Never had Movies',
	sports_status		VARCHAR(100) NULL DEFAULT 'Never had Sports',
	newspaper_readership    VARCHAR(100) NULL DEFAULT 'Unknown',
	line_rental_status      VARCHAR(100) NULL DEFAULT 'Never had Line Rental',
	onnet_fibre             VARCHAR(100) NULL DEFAULT 'Unknown',
	marketing_opt_out       VARCHAR(3)  NULL DEFAULT '000',
	recent_customer_issue   VARCHAR(100)  NULL DEFAULT 'Hasn''t had Issue',
	multiple_customer_issues VARCHAR(100) NULL DEFAULT 'Unknown',
	age_group               VARCHAR(100) NULL DEFAULT 'Unknown',
	mobile_contract         VARCHAR(100) NULL DEFAULT 'Unknown',
	mobile_avg_monthly_bill VARCHAR(100) NULL DEFAULT 'Unknown',
	type_of_shopper         VARCHAR(100) NULL DEFAULT 'Unknown',
	public_sector_mosaic    VARCHAR(100) NULL DEFAULT 'Unknown',
	number_of_cars          VARCHAR(100) NULL DEFAULT 'Unknown',
	senior_decision_maker   VARCHAR(100) NULL DEFAULT 'Unknown',
	pet_ownership           VARCHAR(100) NULL DEFAULT 'Unknown',
	breakdown_renwal_month  VARCHAR(100) NULL DEFAULT 'Unknown',
	catch_up                VARCHAR(100) NULL DEFAULT 'Unknown',
	box_set                 VARCHAR(100) NULL DEFAULT 'Unknown',
-- Adsmart L3 Drop 1 End
-- Adsmart Tactical Soln START
        tactical_fa1            varchar(20) NULL DEFAULT NULL,
        tactical_fa2            varchar(20) NULL DEFAULT NULL,
        tactical_fa3            varchar(20) NULL DEFAULT NULL,
        tactical_fa4            varchar(20) NULL DEFAULT NULL,
        tactical_fa5            varchar(20) NULL DEFAULT NULL,
        tactical_fa6            varchar(20) NULL DEFAULT NULL,
        tactical_fa7            varchar(20) NULL DEFAULT NULL,
        tactical_fa8            varchar(20) NULL DEFAULT NULL,
        tactical_fa9            varchar(20) NULL DEFAULT NULL,
        tactical_fa10           varchar(20) NULL DEFAULT NULL,
-- Adsmart Tactical Soln END
-- CCN1841 END
        tactical_fa11		varchar(20) NULL DEFAULT NULL,
        tactical_fa12		varchar(20) NULL DEFAULT NULL,
        tactical_fa13		varchar(20) NULL DEFAULT NULL,
        tactical_fa14		varchar(20) NULL DEFAULT NULL,
        tactical_fa15		varchar(20) NULL DEFAULT NULL,
        tactical_fa16		varchar(20) NULL DEFAULT NULL,
        tactical_fa17		varchar(20) NULL DEFAULT NULL,
        tactical_fa18		varchar(20) NULL DEFAULT NULL,
        tactical_fa19		varchar(20) NULL DEFAULT NULL,
        tactical_fa20		varchar(20) NULL DEFAULT NULL,
        tactical_fa21		varchar(20) NULL DEFAULT NULL,
        tactical_fa22		varchar(20) NULL DEFAULT NULL,
        tactical_fa23		varchar(20) NULL DEFAULT NULL,
        tactical_fa24		varchar(20) NULL DEFAULT NULL,
        tactical_fa25		varchar(20) NULL DEFAULT NULL,
        tactical_fa26		varchar(20) NULL DEFAULT NULL,
        tactical_fa27		varchar(20) NULL DEFAULT NULL,
        tactical_fa28		varchar(20) NULL DEFAULT NULL,
        tactical_fa29		varchar(20) NULL DEFAULT NULL,
        tactical_fa30		varchar(20) NULL DEFAULT NULL,
-- CCN1841 END
-- CCN1808 START
        sports_ppv_customers    varchar(20) NULL DEFAULT 'Neither',
        activated_sky_sports_5  varchar(3)  NULL DEFAULT 'No',
-- CCN1808 END
-- Adsmart Drop 2 Start
        simple_segmentation		varchar(100)	NULL DEFAULT 'Unknown',
	intention_to_purchase_movies	varchar(10)	NULL DEFAULT 'Unknown',
	intention_to_purchase_sports	varchar(10)	NULL DEFAULT 'Unknown',
	viewing_of_sky_go		varchar(100)	NULL DEFAULT 'Unknown',
	early_adopter			varchar(100)	NULL DEFAULT 'Unknown',
	mosaic_2014_groups		varchar(100)	NULL DEFAULT 'Unknown',
	mosaic_2014_types		varchar(100)	NULL DEFAULT 'Unknown',
	ab_testing			integer		NULL DEFAULT  NULL,
	sky_generated_home_mover	varchar(100)	NULL DEFAULT 'Unknown',
	second_mortgage			varchar(100)	NULL DEFAULT 'Unknown',
-- Adsmart Drop 2 End
-- CCN1857 Start
    rental_usage_over_last_12_months varchar (15)  NULL DEFAULT 'Unknown',
-- CCN1857 END
-- Adsmart Drop 3 Internal Attributes Start	
	on_offer VARCHAR(50) NULL DEFAULT 'Unknown',
	fibre_available           varchar(10)     NULL DEFAULT 'No',
	legacy_sport VARCHAR(3) NULL DEFAULT 'No',
    household_composition_men VARCHAR(23) NULL DEFAULT 'Unknown',
    household_composition_women VARCHAR(25) NULL DEFAULT 'Unknown',
-- Adsmart Drop 3 Internal Attributes end
-- Adsmart Drop 3 External Attributes Start	
   expectant_mum	varchar(7) NULL DEFAULT 'Unknown',
   age_of_youngest_baby_in_household	VARCHAR(12) NULL DEFAULT 'Unknown',
   pregnant_and_number_of_children_in_household	VARCHAR(11) NULL DEFAULT 'Unknown',
   homemover	VARCHAR(23) NULL NULL DEFAULT 'Unknown',
   home_insurance_renewal_month	varchar(9) NULL DEFAULT 'Unknown',
   house_type	VARCHAR(18) NULL DEFAULT 'Unknown',
   south_facing_garden	VARCHAR(7) NULL DEFAULT 'Unknown',
   car_insurance_renewal_month	VARCHAR(9) NULL DEFAULT 'Unknown',
   age_of_car_purchase	VARCHAR(16) NULL DEFAULT 'Unknown',
   vehicle_type_in_household	varchar(21) NULL DEFAULT 'Unknown',
   number_of_cars_in_household	varchar(7) NULL DEFAULT 'Unknown',
   make_of_car	varchar(19) NULL DEFAULT 'Unknown',
   mobile_phone_network	VARCHAR(23) NULL DEFAULT 'Unknown',
   affluence_band VARCHAR(9) NULL DEFAULT 'Unknown',
   type_or_number_of_overseas_holidays  VARCHAR(15) NULL DEFAULT 'Unknown',
-- Adsmart Drop 3 External Attributes end
-- Quarterly Release - 1 Start
   stb_pre_registration VARCHAR(20) NULL DEFAULT 'Unknown',
   mobile_device_os VARCHAR(16) NULL DEFAULT 'Unknown',
   asia_pack VARCHAR(41) NULL DEFAULT 'Unknown',
   broadband_ip VARCHAR(12) NULL DEFAULT 'No IP Data',
   sky_store_rentals_usage_recency VARCHAR(31) NULL DEFAULT 'Unknown',
   barb_tv_regions VARCHAR(15) NULL DEFAULT 'Unknown',
   buy_and_keep_usage_over_last_12_months VARCHAR(38) NULL DEFAULT 'Unknown',
   buy_and_keep_usage_recency VARCHAR(26) NULL DEFAULT 'Unknown'
-- Quarterly Release - 1 End
)
go

MESSAGE 'Create Index for Table ADSMART - Start' type status to client
CREATE INDEX ACCOUNT_NUMBER_HG ON ADSMART(account_number)
go
CREATE INDEX CB_KEY_DB_PERSON_HG ON ADSMART(cb_key_db_person)
go
CREATE INDEX CB_KEY_HOUSEHOLD_HG ON ADSMART(cb_key_household)
go
CREATE INDEX CB_KEY_INDIVIDUAL_HG ON ADSMART(cb_key_individual)
go
MESSAGE 'Create Index for Table ADSMART - Complete' type status to client
go

/****************************************************************************************
 *                                                                                      *
 *                          POPULATE ADSMART TABLE                                      *
 *                                                                                      *
 ***************************************************************************************/
MESSAGE 'Populate Table ADSMART FROM the CUST_SINGLE_ACCOUNT_VIEW - Start' type status to client
go
INSERT INTO ADSMART
 ( 
   record_type   
 , account_number      
 , version_number 
 , cb_key_household    
 , cb_key_db_person    
 , cb_key_individual   
 , model_score         
 , viewing_panel_id    
 , src_system_id
 , mirror_men          
 , mirror_women        
 , mirror_has_children 
 , mirror_abc1         
 , child_0_to_4
 , child_5_to_11
 , child_12_to_17
 , demographic         
 , financial_outlook   
 , h_affluence         
, homeowner           
 , h_lifestage         
 , region              
 , cable_area          
 , sky_go_reg          
 , sky_id              
 , value_segment       
 --, sky_rewards         
 , used_sky_store         
 , turnaround_events   
 , prev_miss_pmt       
 , sports_downgrade    
 , movies_downgrade    
 , current_offer       
 , sky_cust_life       
 , government_region
 --, bt_fibre_area       
 , fibre_available      
 , exchange_id         
 , exchange_status     
 , exchange_unbundled  
 , household_composition
 , isba_tv_region      
 , current_package     
 , box_type            
 , tenure          
 , social_class
 , cb_address_postcode -- Q1 Sports change
 , cb_address_postcode_area -- CCN1789
-- Adsmart L3 Drop 1 START
 , broadband_status
 , tenure_split
 , sky_go_extra
 , primary_box_type
 , HD_status   
 , MR_status   
 , talk_status
 , movies_status
 , sports_status
 , Newspaper_Readership    
 , Line_rental_status      
 , onnet_fibre             
 , marketing_opt_out       
 , recent_customer_issue   
 , multiple_customer_issues
 , age_group               
 , mobile_contract         
 , mobile_avg_monthly_bill 
 , Type_of_Shopper         
 , Public_Sector_Mosaic    
 , number_of_cars          
 , senior_decision_maker   
 , pet_ownership           
 , breakdown_renwal_month  
 , catch_up                
 , box_set                  
 , affluence_band
-- Adsmart L3 Drop 1 END
)    
 SELECT 
   4 as record_type             
 , sav.account_number          
 , 4 as version_number -- updated version number FROM 3 to 4           
 , sav.cb_key_household        
 , sav.cb_key_db_person        
 , sav.cb_key_individual       
 , NULL as model_score             
 , NULL as viewing_panel_id        
 , sav.src_system_id    
 , sav.mirror_men              
 , sav.mirror_women            
 , coalesce(sav.mirror_has_children,'Unknown')
 , sav.mirror_abc1
-- CCN1738 Start
 , case
        when sav.child_0_to_4 = '0' then 'No'
        when sav.child_0_to_4 = '1' then 'Yes'
        else 'Unknown'
   end
 , case
        when sav.child_5_to_11 = '0' then 'No'
        when sav.child_5_to_11 = '1' then 'Yes'
        else 'Unknown'
   end
 , case
        when sav.child_12_to_17 = '0' then 'No'
        when sav.child_12_to_17 = '1' then 'Yes'
        else 'Unknown'
  end
-- CCN1738 end
 , case 
       when sav.demographic = 'Unclassified' then 'Unknown'
	   when sav.demographic is null then 'Unknown'
	   else sav.demographic
	end    
 , case when sav.financial_outlook is null then 'Unknown'
        when sav.financial_outlook = 'Not Defined' then 'Unknown'
	    when sav.financial_outlook = 'Unclassified' then 'Unknown'
		when sav.financial_outlook = 'Unallocated' then 'Unknown'
	else sav.financial_outlook 
   end financial_outlook
 , sav.h_affluence             
 , sav.homeowner               
 , coalesce(sav.h_lifestage,'Unknown')             
 , sav.region                  
 , coalesce(sav.cable_area,'Unknown')              
 , 'No' as sky_go_reg              
 , sav.sky_id                  
 , 'Unknown' as value_segment           
 --, 'No' as sky_rewards             
 , 'No' as used_sky_store --updated as part of Incident INCINC0063120            
 , sav.turnaround_events       
 , sav.prev_miss_pmt           
 , coalesce(sav.sports_downgrade,'No')        
 , coalesce(sav.movies_downgrade,'No')        
 , coalesce(sav.current_offer,'No')         
 , coalesce(sav.sky_cust_life,'Mid')           
 , coalesce(sav.government_region, 'Unknown')
 , sav.bt_fibre_area           
 --, sav.prod_broadband_exchange_id as exchange_id             
 --, sav.prod_broadband_network_type as exchange_status         
 --, sav.prod_broadband_unbundled as exchange_unbundled      
 , NULL as exchange_id             
 , NULL as exchange_status         
 , NULL as exchange_unbundled      
 , sav.household_composition   
 , sav.adsmart_isba_tv_region as isba_tv_region          
 , coalesce(sav.current_package,'Unknown')
 , coalesce(sav.box_type,'Unknown')                
 , sav.tenure                  
 , coalesce(sav.social_class, 'Unknown')
 , sav.cb_address_postcode  -- Q1 Sports change
 , case 
	when sav.cb_address_postcode_district in ('BT1','BT2','BT3','BT4','BT5','BT6','BT7','BT8','BT9','BT10','BT11','BT12','BT13','BT14','BT15','BT16','BT17','BT18','BT19','BT20','BT21','BT22','BT23','BT24','BT25','BT26','BT27') 
	then 'BT1'
	when sav.cb_address_postcode_district in ('BT28','BT29','BT30','BT31','BT32','BT33','BT34','BT35','BT36','BT37','BT38','BT39','BT40','BT41','BT42','BT43','BT44','BT45','BT46','BT47','BT48','BT49','BT50','BT51','BT52','BT53','BT54','BT55','BT56','BT57')
	then 'BT2'
	when sav.cb_address_postcode_district is null then 'Unknown'
	else sav.cb_address_postcode_area 
   end cb_address_postcode_area -- CCN1789

-- Adsmart L3 Drop 1 direct SAV mapping fields START
 , CASE WHEN sav.prod_active_broadband_package_desc IS NULL AND broadband_latest_agreement_end_dt IS NULL THEN 'Never Had BB'
        WHEN sav.prod_active_broadband_package_desc IS NULL AND DATEDIFF(dd,broadband_latest_agreement_end_dt,TODAY()) <=365  THEN 'No BB, downgraded in last 0 - 12 months'
                            WHEN sav.prod_active_broadband_package_desc IS NULL AND DATEDIFF(dd,broadband_latest_agreement_end_dt,TODAY()) BETWEEN 366 AND 730            THEN 'No BB, downgraded in last 12- 24 mths'
                            WHEN sav.prod_active_broadband_package_desc IS NULL AND DATEDIFF(dd,broadband_latest_agreement_end_dt,TODAY()) >730                           THEN 'No BB, downgraded 24 months+'
                            WHEN sav.prod_active_broadband_package_desc = 'Broadband Connect'                                                                             THEN 'Has BB  Product Connect'
                            WHEN sav.prod_active_broadband_package_desc = 'Sky Broadband Unlimited Pro' OR sav.prod_active_broadband_package_desc = 'Sky Broadband Unlimited' THEN 'Has BB Product Unlimited'
                            WHEN sav.prod_active_broadband_package_desc = 'Sky Broadband Lite'                                                                            THEN 'Has BB  Product Lite'
                            WHEN sav.prod_active_broadband_package_desc = 'Sky Broadband Unlimited Fibre'                                                                 THEN 'Has BB  Product Fibre'
                            WHEN sav.prod_active_broadband_package_desc = 'Sky Fibre Unlimited Pro'                                                                       THEN 'Has BB Product Fibre Pro'
                            WHEN sav.prod_active_broadband_package_desc = 'Sky Broadband Everyday'                                                                        THEN 'Has BB Product Everyday'
                            ELSE 'Never had BB'
   END broadband_status
 , CASE WHEN CAST(sav.acct_tenure_total_months AS INTEGER) < 12               THEN 'Less than 1 year'
     WHEN CAST(sav.acct_tenure_total_months AS INTEGER) BETWEEN 12 AND 23  THEN '1-2 yrs'
     WHEN CAST(sav.acct_tenure_total_months AS INTEGER) BETWEEN 24 AND 35  THEN '2-3 yrs'
     WHEN CAST(sav.acct_tenure_total_months AS INTEGER) BETWEEN 36 AND 59  THEN '3-5 yrs'
     WHEN CAST(sav.acct_tenure_total_months AS INTEGER) BETWEEN 60 AND 119 THEN '5-10 yrs'
     WHEN CAST(sav.acct_tenure_total_months AS INTEGER) >= 120             THEN '10+ yrs'
     ELSE 'Unknown'
 END tenure_split
 , CASE WHEN prod_latest_sky_go_extra_status_code IN ('AC','AB','PC') THEN 'Has Sky Go Extra'
        WHEN prod_first_sky_go_extra_activation_dt IS NULL THEN 'Never had Sky Go Extra'
	ELSE 'Never Had Sky Go Extra'
 END sky_go_extra
 , 'Unknown' as primary_box_type 
 , CASE 
	WHEN sav.prod_count_of_active_hd_subs  > 0                                        THEN 'Has HD'
     	WHEN DATEDIFF(dd,sav.acct_latest_hd_cancellation_dt,TODAY()) <=90                 THEN 'No HD, downgraded in last 3 mth'
     	WHEN DATEDIFF(dd,sav.acct_latest_hd_cancellation_dt,TODAY()) BETWEEN 91 AND 365   THEN 'No HD, downgraded in last 4 - 12 months'
     	WHEN DATEDIFF(dd,sav.acct_latest_hd_cancellation_dt,TODAY()) > 365                THEN 'No HD, hasn''t downgraded in last 12mths, had HD previously'
	WHEN sav.prod_active_hd = 0 AND sav.acct_latest_hd_cancellation_dt IS NULL            THEN 'Never had HD'
     	ELSE 'Never had HD'
  END hd_status
 , CASE 
	WHEN sav.prod_active_multiroom = 1 THEN 'Has MR'
     	WHEN sav.prod_active_multiroom = 0 AND sav.prod_latest_multiroom_cancellation_dt IS NOT NULL THEN 'No MR AND never had previously'
     	WHEN sav.prod_active_multiroom = 0 AND sav.prod_latest_multiroom_cancellation_dt IS NULL     THEN 'Never had MR'
     	ELSE 'Never had MR'
   END mr_status
  , CASE WHEN sav.prod_active_skytalk = 1 AND sav.prod_active_sky_talk_package IN ('SKTTARF03','STSU','STCO','STCOOF12','STCOOF03','STCOOF06','STE40ST')     THEN 'Has Talk Unlimited'
                       WHEN sav.prod_active_skytalk = 1 AND sav.prod_active_sky_talk_package NOT IN ('SKTTARF03','STSU','STCO','STCOOF12','STCOOF03','STCOOF06','STE40ST') THEN 'Has Talk'
                       WHEN sav.prod_active_skytalk = 0 AND DATEDIFF(dd,sav.prod_latest_skytalk_cancellation_dt,TODAY()) <= 365                                            THEN 'No Talk AND downgraded in last 0-12 months'
                       WHEN sav.prod_active_skytalk = 0 AND DATEDIFF(dd,sav.prod_latest_skytalk_cancellation_dt,TODAY()) BETWEEN 366 AND 730                               THEN 'No Talk AND downgraded 12-24mths ago'
                       WHEN sav.prod_active_skytalk = 0 AND DATEDIFF(dd,sav.prod_latest_skytalk_cancellation_dt,TODAY()) > 730                                             THEN 'No Talk AND hasn''t downgraded in last 24 mths +, had Talk previously'
                WHEN sav.prod_active_skytalk = 0 AND sav.prod_latest_skytalk_cancellation_dt IS NULL   THEN 'Never had Talk'
     	ELSE 'Never had Talk'
  END talk_status
 , 'Never had Movies' as movies_status
 , 'Never had Sports' as sports_status
 , 'Unknown' as Newspaper_Readership    
 , 'Never had Line Rental' Line_rental_status      
 , 'Unknown' as onnet_fibre             
 , '000' as marketing_opt_out
 , 'Hasn''t had Issue' as recent_customer_issue   
 , 'Unknown' multiple_customer_issues
 , 'Unknown' as age_group               
 , 'Unknown' as mobile_contract         
 , 'Unknown' as mobile_avg_monthly_bill 
 , 'Unknown' as Type_of_Shopper         
 , 'Unknown' as Public_Sector_Mosaic    
 , 'Unknown' as number_of_cars          
 , 'Unknown' as senior_decision_maker   
 , 'Unknown' as pet_ownership           
 , 'Unknown' as breakdown_renwal_month  
 , 'Unknown' as catch_up                
 , 'Unknown' as box_set                 
 , sav.h_affluence
-- Adsmart L3 Drop 1 direct SAV mapping fields END
 FROM  CUST_SINGLE_ACCOUNT_VIEW sav
WHERE sav.account_number <> '99999999999999'
    AND sav.account_number not like '%.%'
    AND sav.cust_active_dtv = 1
    AND sav.cust_primary_service_instance_id is not null
    AND sav.cb_key_household > 0             
    AND sav.cb_key_household IS NOT NULL             
    AND sav.account_number IS NOT NULL               
    AND sav.fin_currency_code = 'GBP'
/* Commented below for future reference */    
--INNER JOIN
--     CUST_SUBS_HIST subs
-- on sav.sys_account_number = subs.account_number 
--    AND subs.status_code IN ('AC','AB','PC')
--    AND subs.subscription_sub_type IN ('DTV Primary Viewing')
--    AND subs.effective_from_dt <= now()
--    AND subs.effective_to_dt > now() 
--    AND subs.EFFECTIVE_FROM_DT IS NOT NULL            
  go                                                                                                                                                                        
MESSAGE 'Populate Table ADSMART FROM the CUST_SINGLE_ACCOUNT_VIEW - Complete' type status to client
go    

/****************************************************************************************
 *                                                                                      *
 *                          UPDATE ADSMART TABLE                                        *
 *                                                                                      *
 ***************************************************************************************/
                                                                                                                     
/************************************
 *                                  *
 *         SKY_GO_REG               *
 *                                  *
 ************************************/
MESSAGE 'Populate field SKY_GO_REG - START' type status to client
go     
IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_SKYGO_USAGE'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_SKYGO_USAGE already exists - Drop AND recreate' type status to client
    drop table  TEMP_SKYGO_USAGE
  END
MESSAGE 'Create Table TEMP_SKYGO_USAGE' type status to client
SELECT sky.account_number,
       1 AS sky_go_reg
INTO  TEMP_SKYGO_USAGE
FROM  SKY_PLAYER_USAGE_DETAIL AS sky
INNER JOIN ADSMART as base
    ON sky.account_number = base.account_number
WHERE sky.cb_data_date >= dateadd(month, -12, now())
  AND sky.cb_data_date < now()
GROUP BY sky.account_number
go
-- Create Index
CREATE  HG INDEX idx04 ON  TEMP_SKYGO_USAGE(account_number)
go

MESSAGE 'Update field SKY_GO_REG to ADSMART Table' type status to client
go
-- Update ADSMART Table
UPDATE ADSMART a
    SET Sky_Go_Reg = case when sky_go.sky_go_reg = 1 then 'Yes' else 'No' end
    FROM  TEMP_SKYGO_USAGE AS sky_go
    WHERE a.account_number = sky_go.account_number                                                                                    
go
MESSAGE 'Drop Table TEMP_SKYGO_USAGE' type status to client
go
drop table  TEMP_SKYGO_USAGE 
go
MESSAGE 'Populate field SKY_GO_REG - COMPLETE' type status to client
go
/************************************
 *                                  *
 *         VALUE SEGMENTS           *
 *                                  *
 ************************************/
MESSAGE 'Populate field VALUE_SEGMENT - START' type status to client
go
UPDATE ADSMART a
    SET value_segment = CASE WHEN tgt.value_seg = 'missing' THEN 'Unknown' ELSE tgt.value_seg END
    FROM   VALUE_SEGMENTS_DATA AS tgt
    WHERE a.account_number = tgt.account_number
MESSAGE 'Populate field VALUE_SEGMENT - COMPLETE' type status to client
go
/* Commented out section for sky_rewards
************************************
 *                                  *
 *         SKY REWARDS              *
 *                                  *
 ************************************         
--MESSAGE 'Populate field sky_rewards - START' type status to client
--go 
--MESSAGE 'Get sky_rewards_l12 FROM table  SKY_REWARDS_COMPETITIONS' type status to client
--go
--IF EXISTS( SELECT tname FROM syscatalog
  --          WHERE creator= user_name() 
    --          AND UPPER(tname)='TEMP_SKY_REWARDS_L12'
      --        AND UPPER(tabletype)='TABLE')
  --BEGIN
    --MESSAGE 'WARN: Temp Table TEMP_SKY_REWARDS_L12 already exists - Drop AND recreate' type status to client
    --drop table  TEMP_SKY_REWARDS_L12
  --END
--MESSAGE 'Create Table TEMP_SKY_REWARDS_L12' type status to client 
--go
--SELECT base.account_number
  --    ,count(*) as sky_reward_l12
--INTO  TEMP_SKY_REWARDS_L12
  --FROM  SKY_REWARDS_COMPETITIONS as sky
    --INNER JOIN ADSMART as base
    --ON sky.account_number = base.account_number
  --WHERE date_entered >=  dateadd(month, -12, now())
  --GROUP BY base.account_number
--go
-- Create Index
--CREATE HG INDEX idx01 ON  TEMP_SKY_REWARDS_L12(account_number)
--go

--MESSAGE 'Get sky_events_l12 FROM table  SKY_REWARDS_EVENTS' type status to client
--go 
--IF EXISTS( SELECT tname FROM syscatalog
  --          WHERE creator= user_name() 
    --          AND UPPER(tname)='TEMP_SKY_EVENTS_L12'
      --        AND UPPER(tabletype)='TABLE')
  --BEGIN
    --MESSAGE 'WARN: Temp Table TEMP_SKY_EVENTS_L12 already exists - Drop AND recreate' type status to client
    --drop table  TEMP_SKY_EVENTS_L12
  --END
--MESSAGE 'Create Table TEMP_SKY_EVENTS_L12' type status to client 
--go
--SELECT base.account_number
  --    ,count(*) as sky_events_l12
--INTO  TEMP_SKY_EVENTS_L12
  --FROM  SKY_REWARDS_EVENTS as sky
    --INNER JOIN ADSMART as Base
     --ON sky.account_number = base.account_number
  --WHERE date_registered >= dateadd(month, -12, now())
  --GROUP BY base.account_number
--go
-- Create Index
--CREATE HG INDEX idx02 ON  TEMP_SKY_EVENTS_L12(account_number)
--go

--MESSAGE 'Build temp table TEMP_SKYREWARDS' type status to client
--go
--IF EXISTS( SELECT tname FROM syscatalog
 --           WHERE creator= user_name() 
   --           AND UPPER(tname)='TEMP_SKYREWARDS'
     --         AND UPPER(tabletype)='TABLE')
  --BEGIN
    --MESSAGE 'WARN: Temp Table TEMP_SKYREWARDS already exists - Drop AND recreate' type status to client
    --drop table  TEMP_SKYREWARDS
 -- END
--MESSAGE 'Create Table TEMP_SKYREWARDS' type status to client 
--go
--SELECT coalesce(reward.account_number, event.account_number) as account_number
  --    ,coalesce(Sky_Events_L12,0) + coalesce(Sky_Reward_L12,0) as skyrewards
--INTO  TEMP_SKYREWARDS
--FROM  TEMP_SKY_EVENTS_L12 event
 --  FULL OUTER JOIN
   --   TEMP_SKY_REWARDS_L12 reward
    --ON reward.account_number = event.account_number  
--go
-- Create Index
--CREATE HG INDEX idx03 ON  TEMP_SKYREWARDS(account_number)
--go
--MESSAGE 'Update field SKY_REWARDS to ADSMART Table' type status to client
--go      
--UPDATE ADSMART a
  --  SET sky_rewards = case when skyrewards >= 1 then 'Yes' else 'No' end
    --FROM   TEMP_SKYREWARDS AS sky
    --WHERE a.account_number = sky.account_number
--go
--MESSAGE 'Drop Rewards Temp tables' type status to client
--go
--drop table  TEMP_SKY_REWARDS_L12 
--go
--drop table  TEMP_SKY_EVENTS_L12 
--go
--drop table  TEMP_SKYREWARDS 
--go
--MESSAGE 'Populate field SKY_REWARDS - COMPLETE' type status to client
--go 

*/

/************************************
 *                                  *
 *         USED SKY STORE           *
 *                                  *
 ************************************/         
--------------------------------------------------------------------
-- Populate sky_used_store FROM CUST_PPV_SUMMARY 

MESSAGE 'Update field USED_SKY_STORE in ADSMART Table' type status to client
GO

UPDATE ADSMART a
	SET used_sky_store = case 
		WHEN  cps.first_ssr_dth_purchase_date is not NULL THEN 'Yes'
		ELSE 'No'
		END
FROM  CUST_PPV_SUMMARY cps
WHERE a.account_number = cps.account_number
GO
MESSAGE 'Populate field USED_SKY_STORE - COMPLETE' type status to client
GO
---------------------------------------------------------------------------


/************************************
 *                                  *
 *         ON DEMAND LAST 6 MONTHS  *
 *                                  *
 ************************************/
--------------------------------------------------------------------
-- Populate on_demand_last_6_months FROM CUST_EST_ACCOUNT_LVL_AGGREGATIONS

MESSAGE 'Update field ON_DEMAND_IN_LAST_6_MONTHS in ADSMART Table' type status to client
GO

UPDATE ADSMART a
        SET on_demand_in_last_6_months = case
                WHEN  cala.on_demand_latest_conn_dt >= dateadd(MONTH,-6,now()) 
			AND cala.on_demand_latest_conn_dt > dateadd(DAY,14,sav.prod_dtv_activation_dt) THEN 'Yes'
                ELSE 'No'
                END
FROM  CUST_EST_AGGREGATIONS cala,
 CUST_SINGLE_ACCOUNT_VIEW sav
WHERE a.account_number = cala.account_number AND
cala.account_number = sav.account_number
GO
MESSAGE 'Populate field ON_DEMAND_IN_LAST_6_MONTHS - COMPLETE' type status to client
GO
---------------------------------------------------------------------------


/************************************
 *                                  *
 *         PREV_MISS_PMT            *
 *                                  *
 ************************************/


MESSAGE 'Populate field PREV_MISS_PMT - START' type status to client
go
IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_PREV_MISS_PMT'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_PREV_MISS_PMT already exists - Drop AND recreate' type status to client
    drop table  TEMP_PREV_MISS_PMT
  END

MESSAGE 'Create Table TEMP_PREV_MISS_PMT' type status to client


DECLARE @date_minus__12  date
DECLARE @date_minus_14d  date

SET @date_minus__12  = dateadd(month, -12, now())
SET @date_minus_14d = dateadd(day, -14, now())

SELECT account_number,
       1 AS miss,
       SUM(miss) AS Total_missed

INTO  TEMP_PREV_MISS_PMT
FROM  cust_bills
WHERE payment_due_dt between @date_minus__12 AND @date_minus_14d
        AND Status = 'Unbilled'
GROUP BY account_number
go
----------------
-- Create Index
CREATE  HG INDEX idx04 ON  TEMP_PREV_MISS_PMT(account_number)
go


MESSAGE 'Update field PREV_MISS_PMT to ADSMART Table' type status to client
go

UPDATE ADSMART base
SET
      base.Prev_miss_pmt =  'Yes'
FROM   TEMP_PREV_MISS_PMT AS pmt
        WHERE base.account_number = pmt.account_number
go
UPDATE ADSMART base
SET
      base.Prev_miss_pmt =  'No' WHERE base.Prev_miss_pmt <> 'Yes'




----------------



go
MESSAGE 'Drop Table TEMP_PREV_MISS_PMT' type status to client
go
drop table  TEMP_PREV_MISS_PMT
go
MESSAGE 'Populate field PREV_MISS_PMT - COMPLETE' type status to client
go

/* TL[8th Oct 2013] - added back the earlier commented MODEL SCORE derivation [line to 464 to 500], this was earlier defaulted to NULL as explained below:*/
/* TJW - Removed the population of model_score AND set it to null as per changes in the requirement */
/************************************
 *                                  *
 *         MODEL SCORE              *
 *                                  *
 ************************************/
MESSAGE 'Populate field MODEL_SCORE - START' type status to client
go 
IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_MODELSCORE'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MODELSCORE already exists - Drop AND recreate' type status to client
    drop table  TEMP_MODELSCORE
  END
MESSAGE 'Create Table TEMP_MODELSCORE' type status to client
go
SELECT  distinct base.cb_key_household
    ,model.model_score
INTO  TEMP_MODELSCORE
FROM  ID_V_Universe_all AS model
  INNER JOIN ADSMART AS base
    ON base.cb_key_household = model.cb_key_household AND model.cb_key_household > 0
    go
-- Create Index
CREATE HG INDEX idx04 ON  TEMP_MODELSCORE(cb_key_household)
go
MESSAGE 'Update field model_score to ADSMART Table' type status to client
go  
Update ADSMART a
    SET  a.model_score = sm.model_score
    FROM  TEMP_MODELSCORE AS sm
    WHERE a.cb_key_household = sm.cb_key_household
go
MESSAGE 'Drop Table TEMP_MODELSCORE' type status to client
go
drop table  TEMP_MODELSCORE 
go
MESSAGE 'Populate field MODEL_SCORE - COMPLETE' type status to client
go 


/************************************
 *                                  *
 *         PANEL ID                 *
 *                                  *
 ************************************/   
MESSAGE 'Populate field VIEWING_PANEL_ID - START' type status to client
go  
Update ADSMART a
    SET  a.viewing_panel_id = ves.panel_id_vespa
    FROM vespa_analysts.Vespa_Single_Box_View ves
    WHERE a.account_number = ves.account_number
--    AND ves.panel_id_vespa = 12
go
MESSAGE 'Populate field VIEWING_PANEL_ID - COMPLETE' type status to client
go

/************************************
 *                                  *
 *         SKY_PHONE_LINE           *
 *                                  *
 ************************************/
MESSAGE 'Populate field SKY_PHONE_LINE - START' type status to client
GO
UPDATE ADSMART A
SET sky_phone_line = CASE
        WHEN UPPER(CSAV.prod_latest_skytalk_wlr_status_code) = 'A'      THEN 'Yes'
        WHEN UPPER(CSAV.prod_latest_skytalk_wlr_status_code) = 'R'      THEN 'Yes'
        WHEN UPPER(CSAV.prod_latest_skytalk_wlr_status_code) = 'CRQ'    THEN 'Yes'
        WHEN UPPER(CSAV.prod_latest_skytalk_wlr_status_code) = 'BCRQ'   THEN 'Yes'
        ELSE 'No'
        END
FROM   CUST_SINGLE_ACCOUNT_VIEW CSAV
WHERE A.account_number = CSAV.account_number
GO

MESSAGE 'Populate field SKY_PHONE_LINE - COMPLETE' type status to client
GO

-- Q1 Sports change starts

MESSAGE 'POPULATE FIELD FOR Q1 SPORTS STARTS - ONNET_BB_AREA' TYPE STATUS TO CLIENT
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR=''
              AND UPPER(TNAME)='TEMP_CB_POSTCODE'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE TEMP_CB_POSTCODE ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE TEMP_CB_POSTCODE
    END
MESSAGE 'CREATE TABLE TEMP_CB_POSTCODE' TYPE STATUS TO CLIENT
GO

SELECT BB.CB_ADDRESS_POSTCODE --DISTINCT(CB_POSTCODE)
INTO TEMP_CB_POSTCODE
FROM  BB_POSTCODE_TO_EXCHANGE AS BB
INNER JOIN  EASYNET_ROLLOUT_DATA AS EN
     ON EN.EXCHANGE_ID = BB.EXCHANGE_ID
     WHERE EN.EXCHANGE_STATUS = 'ONNET'
GO

-- Create Index

CREATE HG INDEX idx05 ON TEMP_CB_POSTCODE(CB_ADDRESS_POSTCODE)
GO

MESSAGE 'UPDATE FIELD ONNET_BB_AREA TO ADSMART TABLE - START' TYPE STATUS TO CLIENT
GO

UPDATE ADSMART A
SET  ONNET_BB_AREA = 'Yes'
WHERE A.CB_ADDRESS_POSTCODE IN (SELECT DISTINCT(TEMP.CB_ADDRESS_POSTCODE) FROM TEMP_CB_POSTCODE TEMP)
GO

MESSAGE 'UPDATE FIELD ONNET_BB_AREA TO ADSMART TABLE- COMPLETE' TYPE STATUS TO CLIENT
GO

MESSAGE 'DROP TABLE TEMP_CB_POSTCODE' TYPE STATUS TO CLIENT
GO

DROP TABLE TEMP_CB_POSTCODE
GO

MESSAGE 'POPULATE FIELD FOR Q1 SPORTS - ONNET_BB_AREA - COMPLETE' TYPE STATUS TO CLIENT
GO

-- Q1 Sports change ends

/****************************************
 *                                      *
 *         HAD_ESPN_ON_1ST_APRIL_2013   *
 *                                      *
 ****************************************/

MESSAGE 'Populate field HAD_ESPN_ON_1ST_APRIL_2013 - START' type status to client
GO
UPDATE ADSMART A
SET had_espn_on_1st_april_2013 = 'No'
GO

UPDATE ADSMART A
SET had_espn_on_1st_april_2013 = 'Yes'
FROM  CUST_SUBS_HIST CBH
WHERE A.account_number = CBH.account_number
AND UPPER(CBH.subscription_sub_type) = 'ESPN'
GO

MESSAGE 'Populate field HAD_ESPN_ON_1ST_APRIL_2013 - COMPLETE' type status to client
GO

-- Adsmart L3 Drop 1 Other Attributes Start

/************************************
 *                                  *
 *         Movies Status            *
 *                                  *
 ************************************/



MESSAGE 'Populate field MOVIES_STATUS - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_MOVIES'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MOVIES  already exists - Drop AND recreate' type status to client
    drop table  TEMP_MOVIES
  END
MESSAGE 'CREATE TABLE TEMP_MOVIES' type status to client
GO

SELECT  csh.Account_number
        ,csh.effective_from_dt AS start_date
        ,csh.effective_to_dt AS end_date
        ,CASE WHEN ncel.prem_movies IS NULL THEN 0 ELSE ncel.prem_movies END AS current_movies_premiums
         ,rank() over (PARTITION BY csh.account_number ORDER BY end_date DESC, start_date DESC, csh.status_start_dt DESC, csh.cb_row_id DESC) AS sorting_rank
INTO  TEMP_MOVIES
FROM  cust_subs_hist AS csh
         inner join  cust_entitlement_lookup AS ncel
                    ON csh.current_short_description = ncel.short_description
WHERE csh.effective_to_dt > csh.effective_from_dt
AND subscription_sub_type = 'DTV Primary Viewing'
AND status_code IN ('AC','PC','AB')   -- Active records
AND csh.currency_code = 'EUR' -- Exclude Republic of Ireland
AND csh.account_number IS NOT NULL
GO

-- Create Index
CREATE INDEX indx_MOVIES ON  TEMP_MOVIES(account_number)
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_MOVIES_PREMIUMS'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MOVIES_PREMIUMS already exists - Drop AND recreate' type status to client
    drop table  TEMP_MOVIES_PREMIUMS
  END
MESSAGE 'CREATE TABLE TEMP_MOVIES_PREMIUMS' type status to client
GO

--WORKOUT IF PREMIUM EVER CHANGED
SELECT Account_number
       ,MAX(current_movies_premiums) AS HIGHEST
       ,MIN(current_movies_premiums) AS LOWEST
INTO  TEMP_MOVIES_PREMIUMS
FROM  TEMP_MOVIES
GROUP BY Account_number
GO

-- Create Index
CREATE INDEX indx_MOVIES1 ON  TEMP_MOVIES_PREMIUMS(account_number)
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_MOVIES_DG_DATE'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MOVIES_DG_DATE already exists - Drop AND recreate' type status to client
    drop table  TEMP_MOVIES_DG_DATE
  END
MESSAGE 'CREATE TABLE TEMP_MOVIES_DG_DATE' type status to client
GO

--WORK OUT DOWNGRADE DATE
SELECT Account_number
       ,MAX(end_date)AS premium_end_date
INTO  TEMP_MOVIES_DG_DATE
FROM  TEMP_MOVIES
WHERE current_movies_premiums > 0
GROUP BY Account_number
GO

-- Create Index
CREATE INDEX indx_MOVIES2 ON  TEMP_MOVIES_DG_DATE(account_number)
GO

-- Update ADSMART Table
UPDATE ADSMART
SET Movies_Status2 = CASE WHEN HIGHEST = 0 AND LOWEST = 0                  THEN 'Never had Movies'
                         WHEN current_movies_premiums > 0 AND end_date >= TODAY()                       THEN 'Has Movies'
                         WHEN current_movies_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) <= 30  THEN 			'No Movies, downgraded in last 0 - 1 month'
                         WHEN current_movies_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) BETWEEN 31 AND 90 THEN  'No Movies, downgraded in last 2 - 3 month'
						 WHEN current_movies_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) BETWEEN 91 AND 365 THEN 'No Movies, downgraded in last 4 - 12 month'
                         WHEN current_movies_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) > 365              THEN 'No Movies, downgraded 13 months +'
                         ELSE Movies_Status
                    END
FROM ADSMART AS AD
INNER JOIN  TEMP_MOVIES_PREMIUMS AS TMP
ON AD.ACCOUNT_NUMBER = TMP.ACCOUNT_NUMBER
LEFT JOIN  TEMP_MOVIES_DG_DATE AS TMDD
ON AD.ACCOUNT_NUMBER = TMDD.ACCOUNT_NUMBER
LEFT JOIN  TEMP_MOVIES AS TM
ON AD.ACCOUNT_NUMBER = TM.ACCOUNT_NUMBER
WHERE sorting_rank = 1
GO

DROP TABLE  TEMP_MOVIES
DROP TABLE  TEMP_MOVIES_PREMIUMS
DROP TABLE  TEMP_MOVIES_DG_DATE
GO

MESSAGE 'Populate field MOVIES_STATUS - END' type status to client
GO

/************************************
 *                                  *
 *         Sports Status            *
 *                                  *
 ************************************/

MESSAGE 'Populate field SPORTS_STATUS - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_SPORTS'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_SPORTS already exists - Drop AND recreate' type status to client
    drop table  TEMP_SPORTS
  END
MESSAGE 'CREATE TABLE TEMP_SPORTS' type status to client
GO

SELECT  csh.Account_number
        ,csh.effective_from_dt AS start_date
        ,csh.effective_to_dt AS end_date
        ,CASE WHEN ncel.prem_SPORTS IS NULL THEN 0 ELSE ncel.prem_SPORTS END AS current_SPORTS_premiums
         ,rank() over (PARTITION BY csh.account_number ORDER BY end_date DESC, start_date DESC, csh.status_start_dt DESC, csh.cb_row_id DESC) AS sorting_rank
INTO  TEMP_SPORTS
FROM  cust_subs_hist AS csh
         inner join  cust_entitlement_lookup AS ncel
                    ON csh.current_short_description = ncel.short_description
WHERE csh.effective_to_dt > csh.effective_from_dt
AND subscription_sub_type = 'DTV Primary Viewing'
AND status_code IN ('AC','PC','AB')   -- Active records
AND csh.currency_code = 'EUR' -- Exclude Republic of Ireland
AND csh.account_number IS NOT NULL
GO

-- Create Index
CREATE INDEX indx_SPORTS ON  TEMP_SPORTS(account_number)
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_SPORTS_PREMIUMS'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_SPORTS_PREMIUMS already exists - Drop AND recreate' type status to client
    drop table  TEMP_SPORTS_PREMIUMS
  END
MESSAGE 'CREATE TABLE TEMP_SPORTS_PREMIUMS' type status to client
GO

--WORKOUT IF PREMIUM EVER CHANGED
SELECT Account_number
       ,MAX(current_SPORTS_premiums) AS HIGHEST
       ,MIN(current_SPORTS_premiums) AS LOWEST
INTO  TEMP_SPORTS_PREMIUMS
FROM  TEMP_SPORTS
GROUP BY Account_number
GO

-- Create Index
CREATE INDEX indx_SPORTS1 ON  TEMP_SPORTS_PREMIUMS(account_number)
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_SPORTS_DG_DATE'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_SPORTS_DG_DATE already exists - Drop AND recreate' type status to client
    drop table  TEMP_SPORTS_DG_DATE
  END
MESSAGE 'CREATE TABLE TEMP_SPORTS_DG_DATE' type status to client
GO

--WORK OUT DOWNGRADE DATE
SELECT Account_number
       ,MAX(end_date)AS premium_end_date
INTO  TEMP_SPORTS_DG_DATE
FROM  TEMP_SPORTS
WHERE current_SPORTS_premiums > 0
GROUP BY Account_number
GO

-- Create Index
CREATE INDEX indx_SPORTS2 ON  TEMP_SPORTS_DG_DATE(account_number)
GO

-- Update ADSMART Table
UPDATE ADSMART
SET sports_status2 = CASE WHEN HIGHEST = 0 AND LOWEST = 0                                                                               THEN 'Never had Sports'
                         WHEN current_SPORTS_premiums > 0 AND end_date >= TODAY()                                                      THEN 'Has Sports'
                         WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) <= 30              THEN 'No Sports, downgraded in last 0 - 1 month'
                         WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) BETWEEN 31 AND 90  THEN 'No Sports, downgraded in last 2 - 3 month'
						 WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) BETWEEN 91 AND 365 THEN 'No Sports, downgraded in last 4 - 12 month'
                         WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) > 365              THEN 'No Sports, downgraded 13 months +'
                         ELSE SPORTS_Status
                    END
FROM ADSMART AS AD
INNER JOIN  TEMP_SPORTS_PREMIUMS AS TMP
ON AD.ACCOUNT_NUMBER = TMP.ACCOUNT_NUMBER
LEFT JOIN  TEMP_SPORTS_DG_DATE AS TMDD
ON AD.ACCOUNT_NUMBER = TMDD.ACCOUNT_NUMBER
LEFT JOIN  TEMP_SPORTS AS TM
ON AD.ACCOUNT_NUMBER = TM.ACCOUNT_NUMBER
WHERE sorting_rank = 1
GO

DROP TABLE  TEMP_SPORTS
DROP TABLE  TEMP_SPORTS_PREMIUMS
DROP TABLE  TEMP_SPORTS_DG_DATE

MESSAGE 'Populate field SPORTS_STATUS - END' type status to client
GO

/************************************
 *                                  *
 *        Newspaper Readership      *
 *                                  *
 ************************************/


/************************************
 *                                  *
 *        Line Rental Status        *
 *                                  *
 ************************************/

MESSAGE 'Populate field LINE_RENTAL_STATUS - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_LINE_RENTAL'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_LINE_RENTAL  already exists - Drop AND recreate' type status to client
    drop table  TEMP_LINE_RENTAL
  END

MESSAGE 'CREATE TABLE TEMP_LINE_RENTAL' type status to client
GO

SELECT account_number
       ,MAX(effective_to_dt) AS end_date
INTO  TEMP_LINE_RENTAL
FROM  cust_subs_hist
WHERE subscription_sub_type = 'SKY TALK LINE RENTAL'
AND status_code IN ('A','a','R','r','CRQ','crq')
GROUP BY account_number
GO

-- Create Index
CREATE HG INDEX LINE_RENTAL ON  TEMP_LINE_RENTAL (ACCOUNT_NUMBER)
GO

-- Update ADSMART Table
UPDATE ADSMART
SET Line_rental_status = CASE WHEN end_date > TODAY()                                  THEN 'Has Sky Line rental'
                              WHEN DATEDIFF(dd,end_date,TODAY()) <= 365                THEN 'No LR, downgraded in last 0 - 12 months'
                              WHEN DATEDIFF(dd,end_date,TODAY()) BETWEEN 366 AND 730  THEN 'No LR AND hasn''t downgraded in last 24 mths+, had LR previously'
                              WHEN DATEDIFF(dd,end_date,TODAY()) >  730                THEN 'No LR,  downgraded 24 months+'
                              ELSE Line_rental_status
                         END
FROM  ADSMART AS AD
INNER JOIN  TEMP_LINE_RENTAL AS TLR
ON AD.ACCOUNT_NUMBER = TLR.ACCOUNT_NUMBER
GO

DROP TABLE  TEMP_LINE_RENTAL

MESSAGE 'Populate field LINE_RENTAL_STATUS - END' type status to client
GO

/************************************
 *                                  *
 *        Opt Out of Marketing      *
 *                                  *
 ************************************/
MESSAGE 'Populate field marketing_opt_out - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_OPT_OUT'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_OPT_OUT  already exists - Drop AND recreate' type status to client
    drop table  TEMP_OPT_OUT
  END

MESSAGE 'CREATE TABLE TEMP_OPT_OUT' type status to client
GO



SELECT SAV.ACCOUNT_NUMBER
      ,CASE WHEN cust_postal_mail_allowed = 'Y' AND cust_mps_suppression = 0 AND SAV.cb_address_postcode IS NOT NULL  THEN 1
            ELSE 0
       END AS DM_opt_out
      ,CASE WHEN cust_email_allowed = 'Y' AND cust_email_address IS NOT NULL THEN 1
            ELSE 0
       END AS EM_opt_out
      ,CASE WHEN cust_telephone_contact_allowed = 'Y' AND cust_preferred_telephone_number IS NOT NULL THEN 1
            ELSE 0
       END AS TM_opt_out
INTO  TEMP_OPT_OUT
FROM  CUST_SINGLE_ACCOUNT_VIEW AS SAV
INNER JOIN ADSMART AS AD
ON AD.ACCOUNT_NUMBER = SAV.ACCOUNT_NUMBER
GO

CREATE HG INDEX TOO ON  TEMP_OPT_OUT (ACCOUNT_NUMBER)
GO

UPDATE ADSMART
SET marketing_opt_out  = CASE WHEN DM_opt_out = 1 AND EM_opt_out = 1 AND TM_opt_out = 1 THEN '111' --DM, EM & TM
                              WHEN DM_opt_out = 1 AND EM_opt_out = 1 AND TM_opt_out = 0 THEN '110' --DM & EM
                              WHEN DM_opt_out = 1 AND EM_opt_out = 0 AND TM_opt_out = 1 THEN '101' --DM & TM
                              WHEN DM_opt_out = 1 AND EM_opt_out = 0 AND TM_opt_out = 0 THEN '100' --DM
                              WHEN DM_opt_out = 0 AND EM_opt_out = 1 AND TM_opt_out = 1 THEN '011' --EM & TM
                              WHEN DM_opt_out = 0 AND EM_opt_out = 1 AND TM_opt_out = 0 THEN '010' --EM
                              WHEN DM_opt_out = 0 AND EM_opt_out = 0 AND TM_opt_out = 1 THEN '001' --TM
                              WHEN DM_opt_out = 0 AND EM_opt_out = 0 AND TM_opt_out = 0 THEN '000' --NONE
                              ELSE marketing_opt_out
                         END
FROM  ADSMART AS AD
INNER JOIN  TEMP_OPT_OUT AS TOO
ON AD.ACCOUNT_NUMBER = TOO.ACCOUNT_NUMBER
GO

DROP TABLE  TEMP_OPT_OUT
GO

MESSAGE 'Populate field marketing_opt_out - END' type status to client
GO


/************************************
 *                                  *
 *     Recent Customer Issue        *
 *                                  *
 ************************************/
MESSAGE 'Populate field recent_customer_issue - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_CUST_ISSUE1'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_CUST_ISSUE1 already exists - Drop AND recreate' type status to client
    DROP TABLE  TEMP_CUST_ISSUE1
  END

MESSAGE 'CREATE TABLE TEMP_CUST_ISSUE1' type status to client
GO

SELECT ACCOUNT_NUMBER
       ,MAX(opened_date) AS opened_date
INTO  TEMP_CUST_ISSUE1
FROM  CUST_TTM_CASE_HIST
GROUP BY ACCOUNT_NUMBER
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_CUST_ISSUE2'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_CUST_ISSUE2 already exists - Drop AND recreate' type status to client
    DROP TABLE  TEMP_CUST_ISSUE2
  END

MESSAGE 'CREATE TABLE TEMP_CUST_ISSUE2' type status to client

GO

SELECT ACCOUNT_NUMBER
       ,MAX(CREATED_DATE) AS created_date
INTO  TEMP_CUST_ISSUE2
FROM  CUST_TECH_ENQUIRY
GROUP BY ACCOUNT_NUMBER
GO

-- Create Index
CREATE HG INDEX TCI1_ACT ON  TEMP_CUST_ISSUE1 (account_number)
GO

CREATE HG INDEX TCI2_ACT ON  TEMP_CUST_ISSUE2 (account_number)
GO

-- Update ADSMART Table
UPDATE ADSMART
SET recent_customer_issue  = CASE WHEN DATEDIFF(DD,opened_date,TODAY())  <=7 OR DATEDIFF(DD,created_date,TODAY()) <=7 THEN 'Has Issue'
                                  ELSE recent_customer_issue
                             END
FROM  ADSMART AS AD
LEFT JOIN  TEMP_CUST_ISSUE1 AS TCI1
     ON AD.account_number = TCI1.account_number
LEFT JOIN  TEMP_CUST_ISSUE2 AS TCI2
     ON AD.account_number = TCI2.account_number
GO

DROP TABLE  TEMP_CUST_ISSUE1
DROP TABLE  TEMP_CUST_ISSUE2
GO

MESSAGE 'Populate field recent_customer_issue - END' type status to client
GO


/************************************
 *                                  *
 *     Multiple Customer Issues     *
 *                                  *
 ************************************/
MESSAGE 'Populate field multiple_customer_issues- START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_MULTI_CUST_ISSUE1'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MULTI_CUST_ISSUE1 already exists - Drop AND recreate' type status to client
    DROP TABLE  TEMP_CUST_ISSUE1
  END

MESSAGE 'CREATE TABLE TEMP_MULTI_CUST_ISSUE1' type status to client
GO

SELECT ACCOUNT_NUMBER
       ,DATEDIFF(DD,opened_date,TODAY()) AS TIMEGAP
INTO  TEMP_MULTI_CUST_ISSUE1
FROM  CUST_TTM_CASE_HIST
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_MULTI_CUST_ISSUE2'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MULTI_CUST_ISSUE2 already exists - Drop AND recreate' type status to client
    drop table  TEMP_MULTI_CUST_ISSUE2
  END

MESSAGE 'CREATE TABLE TEMP_MULTI_CUST_ISSUE2' type status to client
GO

SELECT ACCOUNT_NUMBER
       ,DATEDIFF(DD,created_date,TODAY()) AS TIMEGAP
INTO  TEMP_MULTI_CUST_ISSUE2
FROM  CUST_TECH_ENQUIRY
GO

-- Create Index
CREATE HG INDEX TCI1_ACT ON  TEMP_MULTI_CUST_ISSUE1 (account_number)
GO

CREATE HG INDEX TCI2_ACT ON  TEMP_MULTI_CUST_ISSUE2 (account_number)
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_MULTI_CUST_ISSUE3'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MULTI_CUST_ISSUE3 already exists - Drop AND recreate' type status to client
    drop table  TEMP_MULTI_CUST_ISSUE3
  END

MESSAGE 'CREATE TABLE TEMP_MULTI_CUST_ISSUE3' type status to client
GO

SELECT ACCOUNT_NUMBER
       ,COUNT(TIMEGAP) AS TG
INTO  TEMP_MULTI_CUST_ISSUE3
FROM  TEMP_MULTI_CUST_ISSUE1
WHERE TIMEGAP <=30
GROUP BY ACCOUNT_NUMBER
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_MULTI_CUST_ISSUE3A'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MULTI_CUST_ISSUE3A already exists - Drop AND recreate' type status to client
    drop table  TEMP_MULTI_CUST_ISSUE3A
  END

MESSAGE 'CREATE TABLE TEMP_MULTI_CUST_ISSUE3A' type status to client
GO

SELECT ACCOUNT_NUMBER
       ,COUNT(TIMEGAP) AS TG
INTO  TEMP_MULTI_CUST_ISSUE3A
FROM  TEMP_MULTI_CUST_ISSUE2
WHERE TIMEGAP <=30
GROUP BY ACCOUNT_NUMBER
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_MULTI_CUST_ISSUE4'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MULTI_CUST_ISSUE4 already exists - Drop AND recreate' type status to client
    drop table  TEMP_MULTI_CUST_ISSUE4
  END

MESSAGE 'CREATE TABLE TEMP_MULTI_CUST_ISSUE4' type status to client
GO

SELECT ACCOUNT_NUMBER
       ,COUNT(TIMEGAP) AS TG
INTO  TEMP_MULTI_CUST_ISSUE4
FROM  TEMP_MULTI_CUST_ISSUE1
WHERE TIMEGAP <=90
GROUP BY ACCOUNT_NUMBER
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_MULTI_CUST_ISSUE4A'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MULTI_CUST_ISSUE4A already exists - Drop AND recreate' type status to client
    drop table  TEMP_MULTI_CUST_ISSUE4A
  END

MESSAGE 'CREATE TABLE TEMP_MULTI_CUST_ISSUE4A' type status to client
GO

SELECT ACCOUNT_NUMBER
       ,COUNT(TIMEGAP) AS TG
INTO  TEMP_MULTI_CUST_ISSUE4A
FROM  TEMP_MULTI_CUST_ISSUE2
WHERE TIMEGAP <=90
GROUP BY ACCOUNT_NUMBER
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_MULTI_CUST_ISSUE5'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MULTI_CUST_ISSUE5 already exists - Drop AND recreate' type status to client
    drop table  TEMP_MULTI_CUST_ISSUE5
  END

MESSAGE 'CREATE TABLE TEMP_MULTI_CUST_ISSUE5' type status to client
GO

SELECT ACCOUNT_NUMBER
       ,COUNT(TIMEGAP) AS TG
INTO  TEMP_MULTI_CUST_ISSUE5
FROM  TEMP_MULTI_CUST_ISSUE1
WHERE TIMEGAP <=365
GROUP BY ACCOUNT_NUMBER
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_MULTI_CUST_ISSUE5A'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MULTI_CUST_ISSUE5A already exists - Drop AND recreate' type status to client
    drop table  TEMP_MULTI_CUST_ISSUE5A
  END

MESSAGE 'CREATE TABLE TEMP_MULTI_CUST_ISSUE5A' type status to client
GO

SELECT ACCOUNT_NUMBER
       ,COUNT(TIMEGAP) AS TG
INTO  TEMP_MULTI_CUST_ISSUE5A
FROM  TEMP_MULTI_CUST_ISSUE2
WHERE TIMEGAP <=365
GROUP BY ACCOUNT_NUMBER
GO

-- Create Index
CREATE HG INDEX TCI3_ACT ON  TEMP_MULTI_CUST_ISSUE3 (account_number)
CREATE HG INDEX TCI3A_ACT ON  TEMP_MULTI_CUST_ISSUE3A (account_number)
CREATE HG INDEX TCI4_ACT ON  TEMP_MULTI_CUST_ISSUE4 (account_number)
CREATE HG INDEX TCI4A_ACT ON  TEMP_MULTI_CUST_ISSUE4A (account_number)
CREATE HG INDEX TCI5_ACT ON  TEMP_MULTI_CUST_ISSUE5 (account_number)
CREATE HG INDEX TCI5A_ACT ON  TEMP_MULTI_CUST_ISSUE5A (account_number)
GO

-- Update ADSMART Table
UPDATE ADSMART
SET multiple_customer_issues = CASE WHEN (TCI3.TG + TCI3A.TG) > 1 THEN 'Has had issue last 1 mths'
                                    WHEN (TCI4.TG + TCI4A.TG) > 1 THEN 'Has had issue last 3 mths'
                                    WHEN (TCI5.TG + TCI5A.TG) > 1 THEN 'Has had issue last 12 mths'
                                    ELSE multiple_customer_issues
                               END
FROM  ADSMART AS AD
LEFT JOIN  TEMP_MULTI_CUST_ISSUE3   AS TCI3
     ON AD.account_number = TCI3.account_number
LEFT JOIN  TEMP_MULTI_CUST_ISSUE3A  AS TCI3A
     ON AD.account_number = TCI3A.account_number
LEFT JOIN  TEMP_MULTI_CUST_ISSUE4   AS TCI4
     ON AD.account_number = TCI4.account_number
LEFT JOIN  TEMP_MULTI_CUST_ISSUE4A  AS TCI4A
     ON AD.account_number = TCI4A.account_number
LEFT JOIN  TEMP_MULTI_CUST_ISSUE5   AS TCI5
     ON AD.account_number = TCI5.account_number
LEFT JOIN  TEMP_MULTI_CUST_ISSUE5A  AS TCI5A
     ON AD.account_number = TCI5A.account_number
GO

DROP TABLE  TEMP_MULTI_CUST_ISSUE1
DROP TABLE  TEMP_MULTI_CUST_ISSUE2
DROP TABLE  TEMP_MULTI_CUST_ISSUE3
DROP TABLE  TEMP_MULTI_CUST_ISSUE3A
DROP TABLE  TEMP_MULTI_CUST_ISSUE4
DROP TABLE  TEMP_MULTI_CUST_ISSUE4A
DROP TABLE  TEMP_MULTI_CUST_ISSUE5
DROP TABLE  TEMP_MULTI_CUST_ISSUE5A
GO

MESSAGE 'Populate field multiple_customer_issues - END' type status to client
GO


/************************************
 *                                  *
 *          Catch Up Viewing        *
 *                                  *
 ************************************/

MESSAGE 'Populate field catch_up - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_CATCH_UP'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_CATCH_UP already exists - Drop AND recreate' type status to client
    drop table  TEMP_CATCH_UP
  END

MESSAGE 'CREATE TABLE TEMP_CATCH_UP' type status to client
GO


SELECT   account_number
         ,MAX(last_modified_dt) AS last_modified_dt
INTO      TEMP_CATCH_UP
FROM      CUST_ANYTIME_PLUS_DOWNLOADS AS CAPD
WHERE    x_content_type_desc = 'PROGRAMME'  --  to exclude trailers
AND      x_actual_downloaded_size_mb > 1   -- to exclude any spurious header/trailer download records
AND      cs_referer LIKE '%Catch Up%'
GROUP BY account_number
GO

-- Create Index
CREATE HG INDEX TCU_ACT ON  TEMP_CATCH_UP (account_number)
GO

-- Update ADSMART Table
UPDATE ADSMART
SET catch_up = CASE WHEN datediff(dd,last_modified_dt,TODAY()) <= 90  THEN 'Downloaded within 0 - 3 months'
                    WHEN datediff(dd,last_modified_dt,TODAY()) <= 180 THEN 'Downloaded within 3 - 6 months'
                    WHEN datediff(dd,last_modified_dt,TODAY()) <= 365 THEN 'Downloaded within  6 - 12 months'
		ELSE 'Unknown'
               END
FROM  ADSMART AS AD
INNER JOIN  TEMP_CATCH_UP AS TCU
ON AD.account_number = TCU.account_number
GO

DROP TABLE  TEMP_CATCH_UP

MESSAGE 'Populate field catch_up - END' type status to client
GO

/************************************
 *                                  *
 *           Box Set Viewing        *
 *                                  *
 ************************************/
MESSAGE 'Populate field box_set - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_BOX_SET'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_BOX_SET already exists - Drop AND recreate' type status to client
    drop table  TEMP_BOX_SET
  END

MESSAGE 'CREATE TABLE TEMP_BOX_SET' type status to client
GO

SELECT   account_number
         ,MAX(last_modified_dt) AS last_modified_dt
INTO      TEMP_BOX_SET
FROM      CUST_ANYTIME_PLUS_DOWNLOADS AS CAPD
WHERE    x_content_type_desc = 'PROGRAMME'  --  to exclude trailers
AND      x_actual_downloaded_size_mb > 1   -- to exclude any spurious header/trailer download records
AND      cs_referer LIKE '%Box Sets%'
GROUP BY account_number
GO

-- Create Index
CREATE HG INDEX TBS_ACT ON  TEMP_BOX_SET (account_number)
GO

-- Update ADSMART Table
UPDATE ADSMART
SET BOX_SET = CASE WHEN datediff(dd,last_modified_dt,TODAY()) <= 90  THEN 'Downloaded within 0 - 3 months'
                   WHEN datediff(dd,last_modified_dt,TODAY()) <= 180 THEN 'Downloaded within 3 - 6 months'
                   WHEN datediff(dd,last_modified_dt,TODAY()) <= 365 THEN 'Downloaded within  6 - 12 months'
		ELSE 'Unknown'
               END
FROM  ADSMART AS AD
INNER JOIN  TEMP_BOX_SET AS TBS
ON AD.account_number = TBS.account_number
GO

DROP TABLE  TEMP_BOX_SET

MESSAGE 'Populate field box_set - END' type status to client
GO


/************************************
 *                                  *
 *           Primary Box Type              *
 *                                  *
 ************************************/

UPDATE ADSMART
SET    PRIMARY_BOX_TYPE = CSHP_T.PrimaryBoxType
FROM   ADSMART AS base
   INNER JOIN (SELECT  stb.account_number
  ,SUBSTR(MIN(CASE
          WHEN (stb.x_model_number LIKE '%W%' OR UPPER(stb.x_description) LIKE '%WI-FI%') THEN '1 890 or 895 Wifi Enabled'
          WHEN stb.x_model_number IN ('DRX 890','DRX 895') AND stb.x_pvr_type IN ('PVR5','PVR6')  THEN '2 890 or 895 Not Wifi Enabled'
          WHEN stb.x_manufacturer IN ('Samsung','Pace') THEN '3 Samsung or Pace Not Wifi Enabled'
          ELSE '9 Unknown' END
                 ),3 ,100) AS PrimaryBoxType
   FROM   cust_set_top_box AS stb
   WHERE          stb.active_box_flag = 'Y'
   AND account_number IS NOT NULL
   AND x_model_number <> 'Unknown'
   GROUP BY  stb.account_number
    ) AS CSHP_T
   ON CSHP_T.account_number = base.account_number
GO


-- Adsmart L3 Drop 1 Other Attributes END
-- Adsmart Tactical Soln. START
-- Adsmart Tactical Soln. END
-- CCN1808 : ADD sports_ppv_customers AND activated_sky_sports_5  START

select      account_number
            ,MAX(CASE WHEN ppv_genre='BOXING'    THEN 1 ELSE 0 END) AS boxing_ppv
            ,MAX(CASE WHEN ppv_genre='WRESTLING' THEN 1 ELSE 0 END) AS wrestling_ppv
into        #PPV
FROM         cust_product_charges_ppv
WHERE       ppv_service='EVENT'
--AND         cast(event_dt as date)>@profile_date -@window_length
AND         cast(event_dt as date)> (TODAY() - 548)
AND         ppv_cancelled_dt = '9999-09-09'
group by    account_number
GO

create hg index idx1 on #PPV(account_number);
GO

MESSAGE '    create CCN1808_TMP_ADSMART table' type status to client
GO
create table CCN1808_TMP_ADSMART(
            account_number                      varchar(20) NULL DEFAULT NULL,
            boxing_ppv                          tinyint     NULL DEFAULT NULL,
            wrestling_ppv                       tinyint     NULL DEFAULT NULL,
            sports_ppv_customers                varchar(20) NULL DEFAULT 'Neither',
            activated_sky_sports_5              varchar(3)  NULL DEFAULT 'No');
GO

MESSAGE '    insert account_number into CCN1808_TMP_ADSMART table' type status to client
GO
insert into CCN1808_TMP_ADSMART(account_number)
select      account_number
FROM        ADSMART
order by    account_number
GO

MESSAGE '    update boxing_ppv, wrestling_ppv in CCN1808_TMP_ADSMART table' type status to client
GO
update      CCN1808_TMP_ADSMART
set         boxing_ppv=case when b.boxing_ppv =1       then 1 else 0 end,
            wrestling_ppv=case when b.wrestling_ppv =1 then 1 else 0 end
FROM        CCN1808_TMP_ADSMART as a
left join   #PPV as b
on          a.account_number=b.account_number
GO

MESSAGE '    update sports_ppv_customers in CCN1808_TMP_ADSMART table' type status to client
GO
update      CCN1808_TMP_ADSMART
set         sports_ppv_customers = case when boxing_ppv=1 AND wrestling_ppv=1 then 'Both'
                                        when boxing_ppv=1 AND wrestling_ppv=0 then 'Boxing Only'
                                        when boxing_ppv=0 AND wrestling_ppv=1 then 'Wrestling Only'
                                                                              else 'Neither'
                                   end
GO

MESSAGE '    populate SS5 temp table' type status to client
GO
SELECT      account_number,
            max(case when subscription_type = 'A-LA-CARTE' AND subscription_sub_type = 'SKYSPORTS5'  THEN 1 ELSE 0 END) AS ss5
INTO        #SS5
FROM         cust_subs_hist
WHERE       subscription_sub_type  IN ('SKYSPORTS5')
AND         effective_from_dt <> effective_to_dt
AND         effective_from_dt <= TODAY()
AND         effective_to_dt   >  TODAY()
GROUP BY    account_number;
GO

create hg index idx1 on #ss5(account_number);
GO

MESSAGE '    update activated_sky_sports_5 in CCN1808_TMP_ADSMART table' type status to client
GO
update      CCN1808_TMP_ADSMART
set         activated_sky_sports_5=case when b.ss5 =1 then 'Yes' else 'No' end
FROM        CCN1808_TMP_ADSMART as a
left join   #SS5 as b
on          a.account_number=b.account_number
GO

MESSAGE '    update main ADSMART table' type status to client
GO
UPDATE ADSMART A
SET    A.sports_ppv_customers   = B.sports_ppv_customers,
       A.activated_sky_sports_5 = B.activated_sky_sports_5
FROM   CCN1808_TMP_ADSMART B
WHERE  A.account_number = B.account_number
GO

DROP TABLE CCN1808_TMP_ADSMART
GO
MESSAGE 'Populate sports_ppv_customers AND activated_sky_sports_5 fields - COMPLETE' type status to client
GO
-- CCN1808 : ADD sports_ppv_customers AND activated_sky_sports_5  END

-- Drop 2 Fixed Attributes Start

/************************************
 *                                  *
 *        SIMPLE_SEGMENTATION       *
 *                                  *
 ************************************/

MESSAGE 'POPULATE SIMPLE_SEGMENTATION FIELDS - STARTS' type status to client
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR=''
              AND UPPER(TNAME)='TEMP_SIMPLE_SEGMENTATION'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE TEMP_SIMPLE_SEGMENTATION ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE TEMP_SIMPLE_SEGMENTATION
    END

MESSAGE 'CREATE TABLE TEMP_SIMPLE_SEGMENTATION' TYPE STATUS TO CLIENT
GO

SELECT a.account_number
		, SEGMENTATION =  CASE 	
									WHEN LOWER(b.segment) LIKE '%support%'		THEN 	'Support'
									WHEN LOWER(b.segment) LIKE '%secure%'		THEN	'Secure'
									WHEN LOWER(b.segment) LIKE '%stimulate%'	THEN	'Stimulate'
									WHEN LOWER(b.segment) LIKE '%stabilise'		THEN	'Stabilise'
													ELSE 'Unknown' END
		, row_number()  OVER (PARTITION BY a.account_number ORDER BY observation_date DESC) AS rank_1
INTO TEMP_SIMPLE_SEGMENTATION
FROM ADSMART as a 						
JOIN  SIMPLE_SEGMENTS_HISTORY as b ON a.account_number = b.account_number

CREATE HG INDEX ISIMSEG ON TEMP_SIMPLE_SEGMENTATION(ACCOUNT_NUMBER)
GO

UPDATE ADSMART 
SET SIMPLE_SEGMENTATION = b.SEGMENTATION
FROM ADSMART AS a 
JOIN TEMP_SIMPLE_SEGMENTATION AS b 
ON a.account_number = b.account_number AND b.rank_1 = 1
GO

DROP TABLE TEMP_SIMPLE_SEGMENTATION
GO

MESSAGE 'POPULATE SIMPLE_SEGMENTATION FIELDS - END' type status to client
GO

/*************************************************
 *                                 		 *
 *        INTENTION_TO_PURCHASE_MOVIES & SPORTS  *
 *                                  	 	 *
 *************************************************/

MESSAGE 'POPULATE INTENTION_TO_PURCHASE_MOVIES & SPORTS - STARTS' type status to client
GO

DECLARE @MOVIES_RUN_DATE DATE 
DECLARE @SPORTS_RUN_DATE DATE 

SET @MOVIES_RUN_DATE = (SELECT  MAX(MODEL_RUN_DATE) 			
				FROM MODELS.MODEL_SCORES
				WHERE UPPER(MODEL_NAME) LIKE '%UPLIFT' AND UPPER(MODEL_NAME) LIKE 'MOVIE%')
SET @SPORTS_RUN_DATE = (SELECT  MAX(MODEL_RUN_DATE) 			
				FROM MODELS.MODEL_SCORES
				WHERE UPPER(MODEL_NAME) LIKE '%UPLIFT' AND UPPER(MODEL_NAME) LIKE 'SPORT%')				
				
				
SELECT	  ACCOUNT_NUMBER 
	, MODEL_NAME
	, MAX(DECILE) DECILE
INTO TEMP_MODELS_INT_TO_PURC
FROM MODELS.MODEL_SCORES
WHERE UPPER(MODEL_NAME) LIKE '%UPLIFT' 
	AND ((UPPER(MODEL_NAME) LIKE 'SPORT%' AND MODEL_RUN_DATE = @SPORTS_RUN_DATE) 
	OR (UPPER(MODEL_NAME) LIKE 'MOVIE%' AND MODEL_RUN_DATE = @MOVIES_RUN_DATE))
GROUP BY ACCOUNT_NUMBER , MODEL_NAME
GO

CREATE HG INDEX IDX1 ON TEMP_MODELS_INT_TO_PURC (ACCOUNT_NUMBER)
GO

UPDATE ADSMART
SET INTENTION_TO_PURCHASE_MOVIES = CASE WHEN TMP.DECILE IN (1,2,3,4,5,6,7,8,9,10) THEN  CAST(TMP.DECILE AS VARCHAR(10))  
					ELSE 'Unknown' END
FROM ADSMART AS BASE
JOIN TEMP_MODELS_INT_TO_PURC AS TMP ON BASE.ACCOUNT_NUMBER = TMP.ACCOUNT_NUMBER AND UPPER(TMP.MODEL_NAME) LIKE '%UPLIFT' AND UPPER(TMP.MODEL_NAME) LIKE 'MOVIE%' 
GO 

UPDATE ADSMART
SET INTENTION_TO_PURCHASE_SPORTS = CASE WHEN TMP.DECILE IN (1,2,3,4,5,6,7,8,9,10) THEN  CAST(TMP.DECILE AS VARCHAR(10))
					ELSE 'Unknown' END
FROM ADSMART AS BASE
JOIN TEMP_MODELS_INT_TO_PURC AS TMP ON BASE.ACCOUNT_NUMBER = TMP.ACCOUNT_NUMBER AND UPPER(TMP.MODEL_NAME) LIKE '%UPLIFT' AND UPPER(TMP.MODEL_NAME) LIKE 'SPORT%' 
GO

DROP TABLE TEMP_MODELS_INT_TO_PURC
GO

MESSAGE 'POPULATE INTENTION_TO_PURCHASE_MOVIES & SPORTS - COMPLETED' type status to client
GO

/*****************************************
 *                                       *
 *        SKY GO USAGE			 *
 *                                       *
 **************************************** --REPLACE BY A PRODUCTIONIZED TABLE -REWRITE THE DEFINITION ACCORDING TO THE DEFINITION */

MESSAGE 'POPULATE SKY GO USAGE - STARTS' type status to client
GO

SELECT   	  ACCOUNT_NUMBER 
		, SKYGO_USAGE_SEGMENT = CASE WHEN SKYGO_LATEST_USAGE_DATE >= DATEADD(MM,-3,GETDATE()) THEN 'Active'  -- ACTIVE USER: HAS USED SKYGO IN THE PAST 3 MONTHS
                                	WHEN SKYGO_LATEST_USAGE_DATE < DATEADD(MM,-3,GETDATE()) THEN 'Lapsed'        -- LAPSED > 1 YR: HAS USED SKYGO BETWEEN THE PAST YEAR AND 3 MONTHS AGO
                                	WHEN SKYGO_LATEST_USAGE_DATE IS NULL THEN 'Registered but never used'
                                        ELSE 'Non registered' END
    , RANK () OVER (PARTITION BY ACCOUNT_NUMBER ORDER BY SKYGO_LATEST_USAGE_DATE DESC, SKYGO_FIRST_STREAM_DATE DESC, CB_ROW_ID DESC) TMP_RANK
INTO TEMP_SKYGO_USAGE
FROM  SKY_OTT_USAGE_SUMMARY_ACCOUNT
GO

DELETE FROM TEMP_SKYGO_USAGE
WHERE TMP_RANK > 1
GO

CREATE HG INDEX SKYGO1 ON TEMP_SKYGO_USAGE(ACCOUNT_NUMBER)
GO

UPDATE ADSMART
SET BASE.VIEWING_OF_SKY_GO = COALESCE(TMP_SKYGO_USG.SKYGO_USAGE_SEGMENT, 'Unknown')
FROM ADSMART AS BASE
JOIN TEMP_SKYGO_USAGE AS TMP_SKYGO_USG ON BASE.ACCOUNT_NUMBER = TMP_SKYGO_USG.ACCOUNT_NUMBER  
GO

DROP TABLE TEMP_SKYGO_USAGE
GO

MESSAGE 'POPULATE SKY GO USAGE - COMPLETED' type status to client
GO

/****************************************
 *                                      *
 *     SKY GENERATED HOME MOVER         *
 *                                      *
 ***************************************/
SELECT
                ACCOUNT_NUMBER
                , CASE
                        WHEN UPPER(HOME_MOVE_STATUS) = 'PRE HOME MOVE'                                                				THEN 'Pre Home Move'
                        WHEN UPPER(HOME_MOVE_STATUS) = 'PENDING'                                       						THEN 'Pending Home Move'
                        WHEN UPPER(HOME_MOVE_STATUS) = 'IN-PROGRESS'                                   						THEN 'In-Progress Home Move'
                        WHEN UPPER(HOME_MOVE_STATUS) = 'POST HOME MOVE' AND DATEDIFF(dd, EFFECTIVE_FROM_DT, GETDATE()) BETWEEN 0 AND 30         THEN 'Post Home Move 0 - 30 Days'
                        WHEN UPPER(HOME_MOVE_STATUS) = 'POST HOME MOVE' AND DATEDIFF(dd, EFFECTIVE_FROM_DT, GETDATE()) BETWEEN 30 AND 60        THEN 'Post Home Move 31 - 60 Days'
                        WHEN UPPER(HOME_MOVE_STATUS) = 'POST HOME MOVE' AND DATEDIFF(dd, EFFECTIVE_FROM_DT, GETDATE()) BETWEEN 60 AND 90        THEN 'Post Home Move 61 - 90 Days'
                        WHEN UPPER(HOME_MOVE_STATUS) = 'NONE'  AND DATEDIFF(dd, EFFECTIVE_FROM_DT, GETDATE()) BETWEEN 0 AND 30         		THEN 'Post Home Move 91 - 120 Days'
                        WHEN UPPER(HOME_MOVE_STATUS) = 'NONE'  AND DATEDIFF(dd, EFFECTIVE_FROM_DT, GETDATE()) > 30	             		THEN 'None'
                        ELSE 'Unknown' END AS HOME_MOVE_STATUS
INTO TMP_MOVERS
FROM (SELECT *, RANK() OVER( PARTITION BY ACCOUNT_NUMBER ORDER BY EFFECTIVE_FROM_DT DESC , DW_LAST_MODIFIED_DT DESC ) AS RANKK
      FROM   CUST_HOME_MOVE_STATUS_HIST ) AS B
WHERE RANKK = 1 AND EFFECTIVE_FROM_DT > DATEADD(DD, -120, GETDATE())
GO

CREATE HG INDEX ID1 ON TMP_MOVERS(ACCOUNT_NUMBER)
GO

UPDATE ADSMART
  SET SKY_GENERATED_HOME_MOVER = COALESCE (HOME_MOVE_STATUS, 'Unknown')
FROM  ADSMART AS A
JOIN TMP_MOVERS AS B ON A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
GO

DROP TABLE TMP_MOVERS
GO

/*************************
 *                       *
 *      A/B TESTING      *
 *                       *
 *************************/

UPDATE ADSMART
	SET AB_TESTING  = ROUND(CAST(RIGHT(CAST (ACCOUNT_NUMBER AS VARCHAR) ,2) AS INT)/5,0)+1 
FROM ADSMART AS BASE 						

-- Drop 2 Fixed Attributes Ends

---------------------------------------------------------------------------------------------------------------

-- Quarterly Release 1 : Update RENTAL_USAGE_OVER_LAST_12_MONTHS / SKY_STORE_RENTALS_USAGE_RECENCY     - Start

---------------------------------------------------------------------------------------------------------------

MESSAGE 'Update field RENTAL_USAGE_OVER_LAST_12_MONTHS in ADSMART Table' type status to client
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR=''
              AND LOWER(TNAME)='temp_rental_usage_over_last_12_months'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE temp_rental_usage_over_last_12_months ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE temp_rental_usage_over_last_12_months
    END
MESSAGE 'CREATE TABLE temp_rental_usage_over_last_12_months' TYPE STATUS TO CLIENT
GO

SELECT b.account_number
	, MAX(ppv_ordered_dt) AS max_dt
	, SUM (CASE WHEN DATEDIFF(dd, ppv_ordered_dt, GETDATE()) <= 365 THEN 1 ELSE 0 END) rentals
INTO temp_rental_usage_over_last_12_months
FROM  CUST_PRODUCT_CHARGES_PPV AS a 
JOIN ADSMART b 
ON a.account_number = b.account_number
WHERE ( ca_product_id LIKE 'PVOD%' OR ca_product_id LIKE 'NAM%' OR ca_product_id LIKE 'VCM%')
AND ppv_cancelled_dt = '9999-09-09'
GROUP BY b.account_number
GO

CREATE HG INDEX id1 ON temp_rental_usage_over_last_12_months (account_number)
CREATE LF INDEX id3 ON temp_rental_usage_over_last_12_months (rentals)
GO

UPDATE ADSMART
SET RENTAL_USAGE_OVER_LAST_12_MONTHS = CASE WHEN  cps.rentals BETWEEN 1 AND 4 	THEN 'Rented 1-4'
											WHEN  cps.rentals BETWEEN 5 AND 7 	THEN 'Rented 5-7'
											WHEN  cps.rentals BETWEEN 8 AND 10	THEN 'Rented 8-10'
											WHEN  cps.rentals BETWEEN 11 AND 18	THEN 'Rented 11-18'
											WHEN  cps.rentals >  	18			THEN 'Rented 18+'
											ELSE 'Unknown'
										END
	, SKY_STORE_RENTALS_USAGE_RECENCY = CASE 	WHEN DATEDIFF(dd, max_dt, GETDATE()) <= 90 				THEN 'Rented 0-3 mths back'
												WHEN DATEDIFF(dd, max_dt, GETDATE()) BETWEEN 91 	AND 180 THEN 'Rented 4-6 mths back'
												WHEN DATEDIFF(dd, max_dt, GETDATE()) BETWEEN 181 	AND 365 THEN 'Rented 7-12 mths back'
												WHEN DATEDIFF(dd, max_dt, GETDATE())  > 365 				THEN 'Rented 12+ mths back'
												ELSE 'Unknown'
										END
FROM ADSMART AS a LEFT JOIN temp_rental_usage_over_last_12_months	AS cps 
ON a.account_number = cps.account_number
GO

DROP TABLE temp_rental_usage_over_last_12_months

-- CCN1857 : Add RENTAL_USAGE_OVER_LAST_12_MONTHS - End

-- Adsmart Drop 3 Internal Attributes

/************************************
 *                                  *
 *         ON OFFER                 *
 *                                  *
 ************************************/
MESSAGE 'POPULATE FIELD FOR ON_OFFER' TYPE STATUS TO CLIENT
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR=''
              AND UPPER(TNAME)='temp_Adsmart_end_of_offer_raw'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE TEMP_SKY_STORE_RENTAL ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE temp_Adsmart_end_of_offer_raw
    END
MESSAGE 'CREATE TABLE temp_Adsmart_end_of_offer_raw' TYPE STATUS TO CLIENT
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR=''
              AND UPPER(TNAME)='temp_Adsmart_end_of_offer_aggregated'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE TEMP_SKY_STORE_RENTAL ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE temp_Adsmart_end_of_offer_aggregated
    END
GO 

SELECT b.account_number
	, CASE WHEN lower (offer_dim_description) LIKE '%sport%' THEN 1 ELSE 0 END AS sport_flag
	, CASE WHEN lower (offer_dim_description) LIKE '%movie%' THEN 1 ELSE 0 END AS movie_flag
	, CASE 	WHEN 	x_subscription_type IN ('SKY TALK','BROADBAND')	THEN 'BBT'
			WHEN 	x_subscription_type LIKE 'ENHANCED' AND
					x_subscription_sub_type LIKE 'Broadband DSL Line' THEN 'BBT'
			WHEN 	x_subscription_type IN ('DTV PACKAGE','A-LA-CARTE','ENHANCED')
				AND sport_flag = 1
				AND movie_flag = 0 THEN 'Sports'
			WHEN 	x_subscription_type IN ('DTV PACKAGE','A-LA-CARTE','ENHANCED')	
				AND sport_flag = 0
				AND movie_flag = 1 THEN 'Movies'
			WHEN 	x_subscription_type IN ('DTV PACKAGE','A-LA-CARTE','ENHANCED')	
				AND sport_flag = 1
				AND movie_flag = 1 THEN 'Top Tier'				
			WHEN 	x_subscription_type IN ('DTV PACKAGE','A-LA-CARTE','ENHANCED')	
				AND sport_flag = 0
				AND movie_flag = 0 THEN 'TV Pack      '	
			ELSE 'Unknown' 	END AS Offer_type
	, CASE WHEN offer_end_dt >= GETDATE() THEN 1 ELSE 0 END	AS live_offer
	, DATE(offer_end_dt) AS offer_end_date
	, ABS(DATEDIFF(dd, offer_end_date, getDATE())) AS days_from_today
	, rank() OVER(PARTITION BY b.account_number ORDER BY live_offer DESC, days_from_today,cb_row_id) 			AS rankk_1
	, rank() OVER(PARTITION BY b.account_number, Offer_type ORDER BY live_offer DESC, days_from_today,cb_row_id) 			AS rankk_2
	, CAST (0 AS bit) 														AS main_offer 
INTO 	temp_Adsmart_end_of_offer_raw
FROM     cust_product_offers AS CPO  
JOIN 	ADSMART AS b 	ON CPO.account_number = b.account_number
WHERE    offer_id                NOT IN (SELECT offer_id
                                         FROM citeam.sk2010_offers_to_exclude)
        --AND offer_end_dt          > getdate() 
        AND offer_amount          < 0
        AND offer_dim_description   NOT IN ('PPV 1 Administration Charge','PPV EURO1 Administration Charge')
        AND UPPER (offer_dim_description) NOT LIKE '%VIP%'
        AND UPPER (offer_dim_description) NOT LIKE '%STAFF%'
        AND UPPER (offer_dim_description) NOT LIKE 'PRICE PROTECTION%'
		AND x_subscription_type NOT IN ('MCAFEE')

DELETE FROM temp_Adsmart_end_of_offer_raw WHERE rankk_2 > 1 				-- To keep the latest offer by each offer type 
GO
CREATE HG INDEX id1 ON temp_Adsmart_end_of_offer_raw(account_number)
GO
-----------		Identifying Accounts with more than one active offer
IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR=''
              AND UPPER(TNAME)='temp_Adsmart_end_of_offer_aggregated'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE temp_Adsmart_end_of_offer_aggregated ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE temp_Adsmart_end_of_offer_aggregated
    END
MESSAGE 'CREATE TABLE temp_Adsmart_end_of_offer_aggregated' TYPE STATUS TO CLIENT
GO

SELECT 
	account_number
	, COUNT(*) offers
	, MAX(live_offer)			AS live_offer_c
	, MIN(CASE 	WHEN offer_end_date >  GETDATE() THEN DATEDIFF(dd, getDATE(), offer_end_date) 	ELSE NULL END) 	AS live_date	
	, MIN(CASE	WHEN offer_end_date <= GETDATE() THEN DATEDIFF(dd,  offer_end_date, getDATE()) 	ELSE NULL END)	AS past_date
INTO temp_Adsmart_end_of_offer_aggregated
FROM temp_Adsmart_end_of_offer_raw
GROUP BY account_number
HAVING offers > 1
GO

CREATE HG INDEX id2 ON temp_Adsmart_end_of_offer_aggregated(account_number)
GO

UPDATE temp_Adsmart_end_of_offer_raw
SET main_offer = CASE WHEN  	b.live_offer_c = a.live_offer 
							AND (CASE WHEN live_offer_c =1 	THEN b.live_date 
															ELSE b.past_date END) = a.days_from_today THEN 1 ELSE 0 END
FROM temp_Adsmart_end_of_offer_raw 			AS a 
JOIN temp_Adsmart_end_of_offer_aggregated 	AS b ON a.account_number = b.account_number

-----------		Deleting offers which end date is not the min date 
DELETE FROM temp_Adsmart_end_of_offer_raw		AS a
WHERE  	main_offer = 0 
GO
-----------		Updating multi offers
UPDATE temp_Adsmart_end_of_offer_raw
SET Offer_type = 'Multi offer'
FROM temp_Adsmart_end_of_offer_raw AS a 
JOIN (SELECT account_number, count(*) hits FROM temp_Adsmart_end_of_offer_raw GROUP BY account_number HAVING hits > 1) AS b ON a.account_number = b.account_number 
-----------		DEleting duplicates
DELETE FROM temp_Adsmart_end_of_offer_raw WHERE rankk_1 > 1 				-- To keep the latest offer by each offer type 
GO

-----------		Updating Adsmart table

UPDATE adsmart
SET ON_OFFER = CASE WHEN b.account_number IS NULL THEN 'Unknown'
ELSE TRIM(offer_type) ||' '||
  CASE 	WHEN days_from_today IS NULL 					THEN 'No info on dates'
	WHEN live_offer = 1 AND days_from_today  > 90			THEN 'Live, ends in over 90 days'
	WHEN live_offer = 1 AND days_from_today  BETWEEN 31 AND 90	THEN 'Live, ends in 31-90 Days'
	WHEN live_offer = 1 AND days_from_today  <= 30 			THEN 'Live, ends in next 30 days'
	WHEN live_offer = 0 AND days_from_today  > 90 			THEN 'Expired, ended over 90 days ago'
	WHEN live_offer = 0 AND days_from_today  BETWEEN 31 AND 90	THEN 'Expired, ended 31-90 Days ago'
	WHEN live_offer = 0 AND days_from_today  <= 30 			THEN 'Expired, ended in last 30 days'
	ELSE 'Unknown' END 
  END 
FROM adsmart as a 
LEFT JOIN temp_Adsmart_end_of_offer_raw as b 
ON a.account_number = b.account_number

DROP TABLE temp_Adsmart_end_of_offer_raw
DROP TABLE temp_Adsmart_end_of_offer_aggregated


/************************************
 *                                  *
 *         LEGACY SPORT            *
 *                                  *
 ************************************/
 
MESSAGE 'POPULATE FIELD FOR LEGACY_SPORT' TYPE STATUS TO CLIENT
GO

UPDATE ADSMART A
SET LEGACY_SPORT = 'Yes'
FROM  CUST_SUBS_HIST CBH
WHERE A.ACCOUNT_NUMBER = CBH.ACCOUNT_NUMBER
AND UPPER(CBH.SUBSCRIPTION_SUB_TYPE) = 'ESPN'
GO

UPDATE ADSMART
SET AFFLUENCE_BAND = 'Unknown'
WHERE AFFLUENCE_BAND is null

UPDATE ADSMART
SET FIBRE_AVAILABLE  = 'No'
WHERE FIBRE_AVAILABLE is null

-----------------------------------------------------------------
----------------- HOMEMOVER ------------------------
-----------------------------------------------------------------



------------------------------------------
-- ASIA PACK
------------------------------------------
MESSAGE 'POPULATE FIELD FOR ASIA_PACK' TYPE STATUS TO CLIENT
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR=''
              AND lower(TNAME)='temp_asia_pack'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE temp_asia_pack ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE temp_asia_pack
    END
	
MESSAGE 'CREATE TABLE temp_asia_pack' TYPE STATUS TO CLIENT
GO

SELECT account_number
      ,cast('Has Asia Pack'  as varchar(41)) AS Asia                         -- 2015-04-30 length was initialized at 13
INTO temp_asia_pack
FROM  CUST_SUBS_HIST 
WHERE subscription_type = 'ENHANCED' 
        AND subscription_sub_type = 'SKYASIA'
   AND status_code in ('AC','AB','PC')                          --Active Status Codes
   AND effective_from_dt <= getdate()                       
   AND effective_to_dt > getdate()
   AND effective_from_dt<>effective_to_dt
GO

-- Create Index
CREATE HG INDEX cwd  ON temp_asia_pack(account_number)

INSERT INTO temp_asia_pack
SELECT DISTINCT account_number
              , 'Previously had Asia Pack' AS Asia
FROM  CUST_SUBS_HIST 
WHERE subscription_type = 'ENHANCED' 
        AND subscription_sub_type = 'SKYASIA'
        AND status_code NOT in ('AC','AB','PC','PA')                            -- **WHY NOT ACTIVE? is it referred to the sub_type or the account?
        AND account_number NOT IN (SELECT account_number FROM temp_asia_pack)
GO

INSERT INTO temp_asia_pack
SELECT DISTINCT account_number
              , 'Has never had Asia Pack' AS Asia
FROM  CUST_SUBS_HIST 
WHERE subscription_type = 'ENHANCED' 
        AND subscription_sub_type = 'SKYASIA'
        AND status_code in ('PA') 
        AND account_number NOT IN (SELECT account_number FROM temp_asia_pack)
GO

UPDATE adsmart
SET ASIA_PACK = COALESCE (CASE WHEN Asia IS NOT NULL  THEN Asia 
                                ELSE 'Has never had Asia Pack'
                          END, 'Unknown')
FROM adsmart as a 
LEFT JOIN temp_asia_pack as b ON a.account_number = b.account_number 
GO

DROP TABLE temp_asia_pack
GO

------------------------------------------------------------------------------------------------------------
--------------------------------------------- BROADBAND_IP -------------------------------------------------
------------------------------------------------------------------------------------------------------------

MESSAGE 'POPULATE FIELD FOR BROADBAND_IP' TYPE STATUS TO CLIENT
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR=''
              AND lower(TNAME)='temp_broadband_ip_1'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE temp_broadband_ip_1 ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE temp_broadband_ip_1
    END

MESSAGE 'CREATE TABLE temp_broadband_ip_1' TYPE STATUS TO CLIENT
GO

SELECT a.account_number
        , CASE WHEN network_code = 'bskyb' THEN 'Sky IP'
               WHEN network_code = 'virgin' THEN 'Virgin IP'
               WHEN network_code = 'bt' THEN 'BT IP'
               WHEN network_code = 'none' then 'No IP Data'
               WHEN network_code = 'talkta' then 'Talk Talk IP'
               ELSE 'Other IP' 
		  END AS network_code_1
        , max(last_modified_dt) latest_date
    INTO temp_broadband_ip_1
FROM  CUST_ANYTIME_PLUS_DOWNLOADS a
JOIN adsmart b ON a.account_number = b.account_number
WHERE network_code IS NOT NULL
GROUP BY a.account_number, network_code_1
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR=''
              AND lower(TNAME)='temp_broadband_ip_2'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE temp_broadband_ip_2 ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE temp_broadband_ip_2
    END

MESSAGE 'CREATE TABLE temp_broadband_ip_2' TYPE STATUS TO CLIENT
GO

SELECT account_number
     , network_code_1
     , latest_date
     , rank() OVER (PARTITION BY account_number ORDER BY latest_date ) AS rankk
INTO temp_broadband_ip_2 
FROM temp_broadband_ip_1
GO

UPDATE adsmart
SET BROADBAND_IP = coalesce(network_code_1, 'No IP Data')              -- Name changed to match Excel definition  Update 2015-02-06
FROM adsmart a
LEFT JOIN (SELECT * FROM temp_broadband_ip_2  WHERE rankk = 1) b ON a.account_number = b.account_number
GO

DROP TABLE temp_broadband_ip_1
DROP TABLE temp_broadband_ip_2
GO



/* ***************************************************************************************
 *                                                                                      *
 *     BUY & KEEP Recency  										            			*
 *                                                                                      *
 ***************************************************************************************/

 MESSAGE 'Update field BUY_AND_KEEP_USAGE_RECENCY in ADSMART Table' type status to client
GO
 
IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR=''
              AND lower(TNAME)='temp_buy_and_keep_usage_recency'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE temp_buy_and_keep_usage_recency ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE temp_buy_and_keep_usage_recency
    END

MESSAGE 'CREATE TABLE temp_buy_and_keep_usage_recency' TYPE STATUS TO CLIENT
GO 
 
SELECT b.account_number
	, MAX(est_latest_purchase_dt) AS max_dt
INTO temp_buy_and_keep_usage_recency
FROM  FACT_EST_CUSTOMER_SNAPSHOT	AS a 
JOIN adsmart	AS b ON a.account_number = b.account_number
WHERE est_latest_purchase_dt IS NOT NULL 
	OR est_first_purchase_dt IS NOT NULL 	
GROUP BY b.account_number
GO

-- Create Index 
CREATE HG INDEX id1 ON temp_buy_and_keep_usage_recency (account_number)
GO

UPDATE adsmart a
SET BUY_AND_KEEP_USAGE_RECENCY = CASE 	WHEN DATEDIFF(dd, max_dt, GETDATE()) <= 90 				THEN 'Bought 0-3 mths back'
										WHEN DATEDIFF(dd, max_dt, GETDATE()) BETWEEN 91 	AND 180 THEN 'Bought 4-6 mths back'
										WHEN DATEDIFF(dd, max_dt, GETDATE()) BETWEEN 181 	AND 365 THEN 'Bought 7-12 mths back'
										WHEN DATEDIFF(dd, max_dt, GETDATE())  > 365 				THEN 'Bought 12+ mths back'
										WHEN max_dt IS NULL 													THEN 'Never Bought'
										ELSE 'Unknown' 
								 END
FROM adsmart a
LEFT JOIN temp_buy_and_keep_usage_recency AS cps 
ON a.account_number = cps.account_number
GO	

DROP TABLE temp_buy_and_keep_usage_recency
GO

/****************************************************************************************
 *                                                                                      *
 *                          CREATE ADSMART VIEW                                         *
 *                                                                                      *
 ***************************************************************************************/ 
MESSAGE 'Create ADSMART View for the new ADSMART Table' type status to client
go 
  create view  ADSMART as
  select * FROM ADSMART
  go
MESSAGE 'View  ADSMART created successfully' type status to client
MESSAGE 'Creating Restricted Views on the ADSMART view' type status to client
go 
call  dba.create_restricted_views_all('ADSMART', user_name() ,1)
GO
MESSAGE 'Build ADSMART Table & View Process - Completed Successfully' type status to client

go
