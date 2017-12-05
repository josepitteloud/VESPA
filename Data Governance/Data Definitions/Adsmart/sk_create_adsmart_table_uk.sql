/*  Title       : Adsmart Table/View Build Process 
    Created by  : Tafline James-William
    Date        : 26 November 2012
    Description : This is a sql to build the ADSMART Table from the CUST_SINGLE_ACCOUNT view and other tables.
                : This table potentially replaces the CITEAM.ADSMART_IT table

    Modified by : Tafline James-William
    Changes     : Changed to set the following fields null in the Adsmart Table - exchange_id, exchange_status, exchange_unbundled and model_score
                : Changed the filter that builds the adsmart table - to fix issues with the existing duplication issue
                :  DE - Changed TEMP_PREV_MISS_PMT table build selection to cater for 14 day period where unbilled is a valid status not actually a missed bill
                :  SM - Commented out field sky_rewards and added new field used_sky_store as part of CN 1631 CR 08 : 23/01/2014
                :  SM - Commented out field sky_rewards and added new field used_sky_store as part of CN 1631 CR 08 : 23/01/2014
                :  SM - Commented derivation logic and update for field sky_rewards and added new rule for used_sky_store : 23/01/2014
                :  SM - Updated hardcoded version number from 3 to 4 : 23/01/2014
                :  SM - Added new field Engagement matrix score - defaulted to HIGH : 23/01/2014
                :  SM - Added new field on demand in last 6 mnths - defaulted to No : 23/01/2014
                :  SM - Added new field had espn on 1st apr 2013- defaulted to No : 23/01/2014
                :  SM - Added new mapping rules for had espn and sky line fields : 23/01/2014
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
MESSAGE 'Drop table ${CBAF_DB_DATA_SCHEMA}.ADSMART and view if it already exists' type status to client
IF EXISTS(SELECT tname FROM syscatalog 
            WHERE creator='${CBAF_DB_DATA_SCHEMA}' 
              AND UPPER(tname)='ADSMART' 
              AND UPPER(tabletype)='TABLE')
    BEGIN
        DROP TABLE ${CBAF_DB_DATA_SCHEMA}.ADSMART
        IF EXISTS(SELECT tname FROM syscatalog 
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}' 
              AND upper(tname)='ADSMART' 
              AND upper(tabletype)='VIEW')
          BEGIN
             DROP VIEW ${CBAF_DB_LIVE_SCHEMA}.ADSMART
          END
        ELSE
          BEGIN
            MESSAGE 'WARN: Table ${CBAF_DB_DATA_SCHEMA}.ADSMART exists. View ${CBAF_DB_LIVE_SCHEMA}.ADSMART does not exists' type status to client
          END
    END
go

MESSAGE 'Create Table ${CBAF_DB_DATA_SCHEMA}.ADSMART' type status to client
CREATE TABLE ${CBAF_DB_DATA_SCHEMA}.ADSMART
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
    homeowner               varchar(50)     NULL DEFAULT 'Unknown',
    h_lifestage             varchar(50)     NULL DEFAULT 'Unknown',
    region                  varchar(70)     NULL DEFAULT 'Unknown',
    cable_area              varchar(7)      NULL DEFAULT 'Unknown',
    sky_go_reg              varchar(3)      NULL DEFAULT 'No',
    sky_id                  varchar(3)      NULL DEFAULT NULL,
    value_segment           varchar(50)     NULL DEFAULT 'Unknown',
    --sky_rewards             varchar(3)      NULL DEFAULT NULL,
    -- Commented sky_rewards as per CCN1631 -CR 008	
    used_sky_store          varchar(7)      NULL DEFAULT 'Unknown',
    turnaround_events       varchar(3)      NULL DEFAULT NULL,
    prev_miss_pmt           varchar(3)      NULL DEFAULT NULL,
    sports_downgrade        varchar(7)      NULL DEFAULT 'Unknown',
    movies_downgrade        varchar(7)      NULL DEFAULT 'Unknown',
    current_offer           varchar(7)      NULL DEFAULT 'Unknown',
    sky_cust_life           varchar(20)     NULL DEFAULT 'Unknown',
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
    engagement_matrix_score varchar(7)      NULL DEFAULT 'Unknown',
    sky_phone_line	    varchar(7)      NULL DEFAULT 'Unknown',
    on_demand_in_last_6_months	varchar(3)     NULL DEFAULT 'No',
    had_espn_on_1st_april_2013	varchar(7)     NULL DEFAULT 'Unknown',
    cb_address_postcode		varchar(8) NULL DEFAULT NULL, -- Q1 Sports change
    onnet_bb_area 		varchar(7)     NULL DEFAULT 'No', -- Q1 Sports change
    cb_address_postcode_area 	varchar(8) NULL DEFAULT 'Unknown', -- CCN1789
-- Adsmart L3 Drop 1 Start
	broadband_status 	varchar(100) NULL DEFAULT 'Unknown',
	tenure_split 		varchar(100) NULL DEFAULT 'Unknown',
	sky_go_extra		varchar(100) NULL DEFAULT 'Never had Sky Go Extra',
	primary_box_type	varchar(100) NULL DEFAULT 'Unknown',
	hd_status		VARCHAR(100) NULL DEFAULT 'Never had HD',
	mr_status		VARCHAR(100) NULL DEFAULT 'Never had MR',
	talk_status		VARCHAR(100) NULL DEFAULT 'Unknown',
	movies_status		VARCHAR(100) NULL DEFAULT 'Unknown',
	sports_status		VARCHAR(100) NULL DEFAULT 'Unknown',
	newspaper_readership    VARCHAR(100) NULL DEFAULT 'Unknown',
	line_rental_status      VARCHAR(100) NULL DEFAULT 'Unknown',
	onnet_fibre             VARCHAR(100) NULL DEFAULT 'Unknown',
	marketing_opt_out       VARCHAR(3)  NULL DEFAULT '000',
	recent_customer_issue   VARCHAR(100)  NULL DEFAULT 'Unknown',
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
        sports_ppv_customers    varchar(20) NULL DEFAULT 'Unknown',
        activated_sky_sports_5  varchar(7)  NULL DEFAULT 'Unknown',
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
   time_since_car_purchase	VARCHAR(16) NULL DEFAULT 'Unknown',
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
   buy_and_keep_usage_recency VARCHAR(26) NULL DEFAULT 'Unknown',
-- Quarterly Release - 1 End
-- Quarterly Release - 2 Start
   movies_on_demand VARCHAR(29) NULL DEFAULT 'Unknown',
   household_campaign_demand VARCHAR(13) NULL DEFAULT 'Percent 0-9',
   technology_engagement_customer_index VARCHAR(23) NULL DEFAULT 'Unknown',
   local_authority VARCHAR(50) NULL DEFAULT 'Unknown',
-- Quarterly Release - 2 End
-- WR2017 DMP related aatributes Start
   FAMILY_LIFESTAGE	VARCHAR(45)	NULL DEFAULT "Unclassified",
   AFFLUENCE_BANDS	VARCHAR(15)	NULL DEFAULT "Unknown",
-- WR2017 DMP related aatributes End
-- Quarterly Release - 3 Start
   sky_sports_status VARCHAR(42) NULL DEFAULT 'Never had Sports',
   sky_movies_status VARCHAR(40) NULL DEFAULT 'Never had Movies', 
   household_viewing_propensity VARCHAR(13) NULL DEFAULT 'Unknown',
-- Quarterly Release - 3 End
-- ROI Attributes - Start
   ROI_MOSAIC VARCHAR(24) NULL DEFAULT 'Unknown',
   ROI_COUNTY VARCHAR(9) NULL DEFAULT 'Unknown',
   Residency VARCHAR(7) NULL DEFAULT 'Unknown',
   ROI_SIMPLE_SEGMENTS VARCHAR(9) NULL DEFAULT 'Unknown',
   ROI_REGION_LEVEL_4 VARCHAR(22) NULL DEFAULT 'Unknown',
   ROI_BROADBAND_STATUS VARCHAR(39) NULL DEFAULT 'Unknown',
   ROI_FIBRE_AVAILABLE VARCHAR(7) NULL DEFAULT 'Unknown',
   ROI_ON_OFF_NET_FIBRE VARCHAR(18) NULL DEFAULT 'Unknown',
   ROI_CABLE_AVAILABLE VARCHAR(7) NULL DEFAULT 'Unknown',
-- ROI Attributes - End
   POC_COUNT VARCHAR(9) NULL DEFAULT 'Unknown',
   SKY_HD_STATUS VARCHAR(60) NULL DEFAULT 'Never had HD',
   BUNDLE_TYPE VARCHAR(8) NULL DEFAULT 'Others',
   YOUNGEST_ADULT_HOUSEHOLD VARCHAR(25) NULL DEFAULT 'Unknown'
 )
go

MESSAGE 'Create Index for Table ${CBAF_DB_DATA_SCHEMA}.ADSMART - Start' type status to client
CREATE INDEX ACCOUNT_NUMBER_HG ON ${CBAF_DB_DATA_SCHEMA}.ADSMART(account_number)
go
CREATE INDEX CB_KEY_DB_PERSON_HG ON ${CBAF_DB_DATA_SCHEMA}.ADSMART(cb_key_db_person)
go
CREATE INDEX CB_KEY_HOUSEHOLD_HG ON ${CBAF_DB_DATA_SCHEMA}.ADSMART(cb_key_household)
go
CREATE INDEX CB_KEY_INDIVIDUAL_HG ON ${CBAF_DB_DATA_SCHEMA}.ADSMART(cb_key_individual)
go
MESSAGE 'Create Index for Table ${CBAF_DB_DATA_SCHEMA}.ADSMART - Complete' type status to client
go

/****************************************************************************************
 *                                                                                      *
 *                          POPULATE ADSMART TABLE                                      *
 *                                                                                      *
 ***************************************************************************************/
MESSAGE 'Populate Table ${CBAF_DB_DATA_SCHEMA}.ADSMART from the CUST_SINGLE_ACCOUNT_VIEW - Start' type status to client
go
INSERT INTO ${CBAF_DB_DATA_SCHEMA}.ADSMART
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
 , tenure          
 , social_class
 , cb_address_postcode -- Q1 Sports change
 , cb_address_postcode_area -- CCN1789
-- Adsmart L3 Drop 1 START
 , broadband_status
 , primary_box_type
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
 , family_lifestage
 , affluence_bands
 , bundle_type
-- Adsmart L3 Drop 1 END
)    
 SELECT 
   4 as record_type             
 , sav.account_number          
 , 4 as version_number -- updated version number from 3 to 4           
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
 , coalesce(sav.homeowner,'No')               
 , CASE 
       WHEN sav.h_lifestage = 'Unclassified'
       THEN 'Unknown'
       WHEN sav.h_lifestage IS NULL
       THEN 'Unknown'             
       ELSE sav.h_lifestage
   END h_lifestage
 , coalesce(sav.region,'Unknown')                  
 , coalesce(sav.cable_area,'Unknown')              
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
 , CASE 
       WHEN sav.household_composition = 'Unclassified' 
       THEN 'Unknown'   
       WHEN sav.household_composition IS NULL
       THEN 'Unknown'
       ELSE sav.household_composition
       END household_composition
 , sav.adsmart_isba_tv_region as isba_tv_region                   
 , sav.tenure                  
 , CASE 
       WHEN sav.social_class = 'Unclassified' 
       THEN 'Unknown'
       WHEN sav.social_class IS NULL
       THEN 'Unknown' 
       ELSE sav.social_class
   END social_class 
 , sav.cb_address_postcode  -- Q1 Sports change
 , CASE 
	WHEN sav.cb_address_postcode_district in ('BT1','BT2','BT3','BT4','BT5','BT6','BT7','BT8','BT9','BT10','BT11','BT12','BT13','BT14','BT15','BT16','BT17','BT18','BT19','BT20','BT21','BT22','BT23','BT24','BT25','BT26','BT27','BT28','BT29','BT36','BT37','BT38','BT39','BT40','BT41') THEN 'BT1'
	WHEN sav.cb_address_postcode_district in ('BT42','BT43','BT44','BT45','BT46','BT47','BT48','BT49','BT50','BT51','BT52','BT53','BT54','BT55','BT56','BT57','BT58','BT59','BT78','BT81','BT82','BT83','BT84','BT85','BT86','BT87','BT88','BT90','BT91') THEN 'BT2'
	WHEN sav.cb_address_postcode_district in ('BT30','BT31','BT32','BT33','BT34','BT35','BT60','BT61','BT62','BT63','BT64','BT65','BT66','BT67','BT68','BT69','BT70','BT71','BT71','BT73','BT74','BT75','BT76','BT77','BT79','BT80','BT92','BT93','BT94') 	THEN 'BT3'
	WHEN sav.cb_address_postcode_district is null then 'Unknown'
	ELSE sav.cb_address_postcode_area 
   END cb_address_postcode_area
-- Adsmart L3 Drop 1 direct SAV mapping fields START
 , CASE WHEN sav.prod_active_broadband_package_desc IS NULL AND broadband_latest_agreement_end_dt IS NULL THEN 'Never had BB'
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
 , 'Unknown' as primary_box_type 
 , CASE WHEN sav.prod_active_skytalk = 1 AND sav.prod_active_sky_talk_package IN ('SKTTARF03','STSU','STCO','STCOOF12','STCOOF03','STCOOF06','STE40ST')     THEN 'Has Talk Unlimited'
                       WHEN sav.prod_active_skytalk = 1 AND sav.prod_active_sky_talk_package NOT IN ('SKTTARF03','STSU','STCO','STCOOF12','STCOOF03','STCOOF06','STE40ST') THEN 'Has Talk'
                       WHEN sav.prod_active_skytalk = 0 AND DATEDIFF(dd,sav.prod_latest_skytalk_cancellation_dt,TODAY()) <= 365                                            THEN 'No Talk and downgraded in last 0-12 months'
                       WHEN sav.prod_active_skytalk = 0 AND DATEDIFF(dd,sav.prod_latest_skytalk_cancellation_dt,TODAY()) BETWEEN 366 AND 730                               THEN 'No Talk and downgraded 12-24mths ago'
                       WHEN sav.prod_active_skytalk = 0 AND DATEDIFF(dd,sav.prod_latest_skytalk_cancellation_dt,TODAY()) > 730                                             THEN 'No Talk and hasn''t downgraded in last 24 mths +, had Talk previously'
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
 , sav.h_lifestage
 , sav.affluence_bands
 , CASE 
       WHEN prod_latest_entitlement_genre in ('Original','Variety','Family') THEN  prod_latest_entitlement_genre
       WHEN prod_latest_entitlement_genre in ('Sky Q Bundle') THEN  'SkyQ'
       ELSE 'Others'
   END bundle_type 
-- Adsmart L3 Drop 1 direct SAV mapping fields END
FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_SINGLE_ACCOUNT_VIEW sav
where sav.account_number <> '99999999999999'
    AND sav.account_number not like '%.%'
    AND sav.cust_active_dtv = 1
    AND sav.cust_primary_service_instance_id is not null
    AND sav.cb_key_household > 0
    AND sav.cb_key_household IS NOT NULL
    AND sav.account_number IS NOT NULL
    AND UPPER(sav.PTY_COUNTRY_CODE) LIKE 'GBR'
/* Commented below for future reference */    
--INNER JOIN
--    ${CBAF_DB_LIVE_SCHEMA}.CUST_SUBS_HIST subs
-- on sav.sys_account_number = subs.account_number 
--    and subs.status_code IN ('AC','AB','PC')
--    and subs.subscription_sub_type IN ('DTV Primary Viewing')
--    AND subs.effective_from_dt <= now()
--    AND subs.effective_to_dt > now() 
--    AND subs.EFFECTIVE_FROM_DT IS NOT NULL            
  go                                                                                                                                                                        
MESSAGE 'Populate Table ${CBAF_DB_DATA_SCHEMA}.ADSMART from the CUST_SINGLE_ACCOUNT_VIEW - Complete' type status to client
go    

/****************************************************************************************
 *                                                                                      *
 *                          UPDATE ADSMART TABLE                                        *
 *                                                                                      *
 ***************************************************************************************/
                                                                                                                     

/************************************
 *                                  *
 *         VALUE SEGMENTS           *
 *                                  *
 ************************************/
MESSAGE 'Populate field VALUE_SEGMENT - START' type status to client
go
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART a
    SET value_segment = CASE WHEN tgt.value_seg = 'missing' THEN 'Unknown' ELSE tgt.value_seg END
    FROM  ${CBAF_DB_LIVE_SCHEMA}.VALUE_SEGMENTS_DATA AS tgt
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
--MESSAGE 'Get sky_rewards_l12 from table ${CBAF_DB_LIVE_SCHEMA}.SKY_REWARDS_COMPETITIONS' type status to client
--go
--IF EXISTS( SELECT tname FROM syscatalog
  --          WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
    --          AND UPPER(tname)='TEMP_SKY_REWARDS_L12'
      --        AND UPPER(tabletype)='TABLE')
  --BEGIN
    --MESSAGE 'WARN: Temp Table TEMP_SKY_REWARDS_L12 already exists - Drop and recreate' type status to client
    --drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_SKY_REWARDS_L12
  --END
--MESSAGE 'Create Table TEMP_SKY_REWARDS_L12' type status to client 
--go
--SELECT base.account_number
  --    ,count(*) as sky_reward_l12
--INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_SKY_REWARDS_L12
  --FROM ${CBAF_DB_LIVE_SCHEMA}.SKY_REWARDS_COMPETITIONS as sky
    --INNER JOIN ${CBAF_DB_DATA_SCHEMA}.ADSMART as base
    --ON sky.account_number = base.account_number
  --WHERE date_entered >=  dateadd(month, -12, now())
  --GROUP BY base.account_number
--go
-- Create Index
--CREATE HG INDEX idx01 ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_SKY_REWARDS_L12(account_number)
--go

--MESSAGE 'Get sky_events_l12 from table ${CBAF_DB_LIVE_SCHEMA}.SKY_REWARDS_EVENTS' type status to client
--go 
--IF EXISTS( SELECT tname FROM syscatalog
  --          WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
    --          AND UPPER(tname)='TEMP_SKY_EVENTS_L12'
      --        AND UPPER(tabletype)='TABLE')
  --BEGIN
    --MESSAGE 'WARN: Temp Table TEMP_SKY_EVENTS_L12 already exists - Drop and recreate' type status to client
    --drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_SKY_EVENTS_L12
  --END
--MESSAGE 'Create Table TEMP_SKY_EVENTS_L12' type status to client 
--go
--SELECT base.account_number
  --    ,count(*) as sky_events_l12
--INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_SKY_EVENTS_L12
  --FROM ${CBAF_DB_LIVE_SCHEMA}.SKY_REWARDS_EVENTS as sky
    --INNER JOIN ${CBAF_DB_DATA_SCHEMA}.ADSMART as Base
     --ON sky.account_number = base.account_number
  --WHERE date_registered >= dateadd(month, -12, now())
  --GROUP BY base.account_number
--go
-- Create Index
--CREATE HG INDEX idx02 ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_SKY_EVENTS_L12(account_number)
--go

--MESSAGE 'Build temp table TEMP_SKYREWARDS' type status to client
--go
--IF EXISTS( SELECT tname FROM syscatalog
 --           WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
   --           AND UPPER(tname)='TEMP_SKYREWARDS'
     --         AND UPPER(tabletype)='TABLE')
  --BEGIN
    --MESSAGE 'WARN: Temp Table TEMP_SKYREWARDS already exists - Drop and recreate' type status to client
    --drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_SKYREWARDS
 -- END
--MESSAGE 'Create Table TEMP_SKYREWARDS' type status to client 
--go
--SELECT coalesce(reward.account_number, event.account_number) as account_number
  --    ,coalesce(Sky_Events_L12,0) + coalesce(Sky_Reward_L12,0) as skyrewards
--INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_SKYREWARDS
--FROM ${CBAF_DB_LIVE_SCHEMA}.TEMP_SKY_EVENTS_L12 event
 --  FULL OUTER JOIN
   --  ${CBAF_DB_LIVE_SCHEMA}.TEMP_SKY_REWARDS_L12 reward
    --ON reward.account_number = event.account_number  
--go
-- Create Index
--CREATE HG INDEX idx03 ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_SKYREWARDS(account_number)
--go
--MESSAGE 'Update field SKY_REWARDS to ADSMART Table' type status to client
--go      
--UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART a
  --  SET sky_rewards = case when skyrewards >= 1 then 'Yes' else 'No' end
    --FROM  ${CBAF_DB_LIVE_SCHEMA}.TEMP_SKYREWARDS AS sky
    --WHERE a.account_number = sky.account_number
--go
--MESSAGE 'Drop Rewards Temp tables' type status to client
--go
--drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_SKY_REWARDS_L12 
--go
--drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_SKY_EVENTS_L12 
--go
--drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_SKYREWARDS 
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
-- Populate sky_used_store from CUST_PPV_SUMMARY 

MESSAGE 'Update field USED_SKY_STORE in ADSMART Table' type status to client
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART a
	SET used_sky_store = case 
		WHEN  cps.first_ssr_dth_purchase_date is not NULL THEN 'Yes'
		ELSE 'No'
		END
FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_PPV_SUMMARY cps
WHERE a.account_number = cps.account_number
GO
MESSAGE 'Populate field USED_SKY_STORE - COMPLETE' type status to client
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
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_PREV_MISS_PMT'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_PREV_MISS_PMT already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_PREV_MISS_PMT
  END

MESSAGE 'Create Table TEMP_PREV_MISS_PMT' type status to client


DECLARE @date_minus__12  date
DECLARE @date_minus_14d  date

SET @date_minus__12  = dateadd(month, -12, now())
SET @date_minus_14d = dateadd(day, -14, now())

SELECT account_number,
       1 AS miss,
       SUM(miss) AS Total_missed

INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_PREV_MISS_PMT
FROM ${CBAF_DB_LIVE_SCHEMA}.cust_bills
WHERE payment_due_dt between @date_minus__12 AND @date_minus_14d
        AND Status = 'Unbilled'
GROUP BY account_number
go
----------------
-- Create Index
CREATE  HG INDEX idx04 ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_PREV_MISS_PMT(account_number)
go


MESSAGE 'Update field PREV_MISS_PMT to ADSMART Table' type status to client
go

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART base
SET
      base.Prev_miss_pmt =  'Yes'
from  ${CBAF_DB_LIVE_SCHEMA}.TEMP_PREV_MISS_PMT AS pmt
        where base.account_number = pmt.account_number
go
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART base
SET
      base.Prev_miss_pmt =  'No' where base.Prev_miss_pmt <> 'Yes'




----------------



go
MESSAGE 'Drop Table TEMP_PREV_MISS_PMT' type status to client
go
drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_PREV_MISS_PMT
go
MESSAGE 'Populate field PREV_MISS_PMT - COMPLETE' type status to client
go

/* TL[8th Oct 2013] - added back the earlier commented MODEL SCORE derivation [line to 464 to 500], this was earlier defaulted to NULL as explained below:*/
/* TJW - Removed the population of model_score and set it to null as per changes in the requirement */
/************************************
 *                                  *
 *         MODEL SCORE              *
 *                                  *
 ************************************/
MESSAGE 'Populate field MODEL_SCORE - START' type status to client
go 
IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_MODELSCORE'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MODELSCORE already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_MODELSCORE
  END
MESSAGE 'Create Table TEMP_MODELSCORE' type status to client
go
SELECT  distinct base.cb_key_household
    ,model.model_score
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_MODELSCORE
FROM ${CBAF_DB_LIVE_SCHEMA}.ID_V_Universe_all AS model
  INNER JOIN ${CBAF_DB_DATA_SCHEMA}.ADSMART AS base
    ON base.cb_key_household = model.cb_key_household
    WHERE base.cb_key_household IS NOT NULL
      AND base.cb_key_household != 0
go
-- Create Index
CREATE HG INDEX idx04 ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_MODELSCORE(cb_key_household)
go
MESSAGE 'Update field model_score to ADSMART Table' type status to client
go  
Update ${CBAF_DB_DATA_SCHEMA}.ADSMART a
    SET  a.model_score = sm.model_score
    FROM ${CBAF_DB_LIVE_SCHEMA}.TEMP_MODELSCORE AS sm
    where a.cb_key_household = sm.cb_key_household
go
MESSAGE 'Drop Table TEMP_MODELSCORE' type status to client
go
drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_MODELSCORE 
go
MESSAGE 'Populate field MODEL_SCORE - COMPLETE' type status to client
go 

/************************************
 *                                  *
 *         SKY_PHONE_LINE           *
 *                                  *
 ************************************/
MESSAGE 'Populate field SKY_PHONE_LINE - START' type status to client
GO
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART A
SET sky_phone_line = CASE
        WHEN UPPER(CSAV.prod_latest_skytalk_wlr_status_code) = 'A'      THEN 'Yes'
        WHEN UPPER(CSAV.prod_latest_skytalk_wlr_status_code) = 'R'      THEN 'Yes'
        WHEN UPPER(CSAV.prod_latest_skytalk_wlr_status_code) = 'CRQ'    THEN 'Yes'
        WHEN UPPER(CSAV.prod_latest_skytalk_wlr_status_code) = 'BCRQ'   THEN 'Yes'
        ELSE 'No'
        END
FROM  ${CBAF_DB_LIVE_SCHEMA}.CUST_SINGLE_ACCOUNT_VIEW CSAV
WHERE A.account_number = CSAV.account_number
GO

MESSAGE 'Populate field SKY_PHONE_LINE - COMPLETE' type status to client
GO

-- Q1 Sports change starts

MESSAGE 'POPULATE FIELD FOR Q1 SPORTS STARTS - ONNET_BB_AREA' TYPE STATUS TO CLIENT
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR='${CBAF_DB_DATA_SCHEMA}'
              AND UPPER(TNAME)='TEMP_CB_POSTCODE'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE TEMP_CB_POSTCODE ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE ${CBAF_DB_DATA_SCHEMA}.TEMP_CB_POSTCODE
    END
MESSAGE 'CREATE TABLE TEMP_CB_POSTCODE' TYPE STATUS TO CLIENT
GO

SELECT BB.CB_ADDRESS_POSTCODE --DISTINCT(CB_POSTCODE)
INTO ${CBAF_DB_DATA_SCHEMA}.TEMP_CB_POSTCODE
FROM ${CBAF_DB_LIVE_SCHEMA}.BB_POSTCODE_TO_EXCHANGE AS BB
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.EASYNET_ROLLOUT_DATA AS EN
     ON EN.EXCHANGE_ID = BB.EXCHANGE_ID
     WHERE EN.EXCHANGE_STATUS = 'ONNET'
GO

-- Create Index

CREATE HG INDEX idx05 ON ${CBAF_DB_DATA_SCHEMA}.TEMP_CB_POSTCODE(CB_ADDRESS_POSTCODE)
GO

MESSAGE 'UPDATE FIELD ONNET_BB_AREA TO ADSMART TABLE - START' TYPE STATUS TO CLIENT
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART A
SET  ONNET_BB_AREA = 'Yes'
WHERE A.CB_ADDRESS_POSTCODE IN (SELECT DISTINCT(TEMP.CB_ADDRESS_POSTCODE) FROM ${CBAF_DB_DATA_SCHEMA}.TEMP_CB_POSTCODE TEMP)
GO

MESSAGE 'UPDATE FIELD ONNET_BB_AREA TO ADSMART TABLE- COMPLETE' TYPE STATUS TO CLIENT
GO

MESSAGE 'DROP TABLE TEMP_CB_POSTCODE' TYPE STATUS TO CLIENT
GO

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.TEMP_CB_POSTCODE
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
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART A
SET had_espn_on_1st_april_2013 = 'No'
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART A
SET had_espn_on_1st_april_2013 = 'Yes'
FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_SUBS_HIST CBH
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
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_MOVIES'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MOVIES  already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_MOVIES
  END
MESSAGE 'CREATE TABLE TEMP_MOVIES' type status to client
GO

SELECT  csh.Account_number
        ,csh.effective_from_dt AS start_date
        ,csh.effective_to_dt AS end_date
        ,CASE WHEN ncel.prem_movies IS NULL THEN 0 ELSE ncel.prem_movies END AS current_movies_premiums
         ,rank() over (PARTITION BY csh.account_number ORDER BY end_date DESC, start_date DESC, csh.status_start_dt DESC, csh.cb_row_id DESC) AS sorting_rank
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_MOVIES
FROM ${CBAF_DB_LIVE_SCHEMA}.cust_subs_hist AS csh
         inner join ${CBAF_DB_LIVE_SCHEMA}.cust_entitlement_lookup AS ncel
                    ON csh.current_short_description = ncel.short_description
WHERE csh.effective_to_dt > csh.effective_from_dt
AND subscription_sub_type = 'DTV Primary Viewing'
AND status_code IN ('AC','PC','AB')   -- Active records
AND csh.account_number IS NOT NULL
GO

-- Create Index
CREATE INDEX indx_MOVIES ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_MOVIES(account_number)
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_MOVIES_PREMIUMS'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MOVIES_PREMIUMS already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_MOVIES_PREMIUMS
  END
MESSAGE 'CREATE TABLE TEMP_MOVIES_PREMIUMS' type status to client
GO

--WORKOUT IF PREMIUM EVER CHANGED
SELECT Account_number
       ,MAX(current_movies_premiums) AS HIGHEST
       ,MIN(current_movies_premiums) AS LOWEST
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_MOVIES_PREMIUMS
FROM ${CBAF_DB_LIVE_SCHEMA}.TEMP_MOVIES
GROUP BY Account_number
GO

-- Create Index
CREATE INDEX indx_MOVIES1 ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_MOVIES_PREMIUMS(account_number)
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_MOVIES_DG_DATE'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MOVIES_DG_DATE already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_MOVIES_DG_DATE
  END
MESSAGE 'CREATE TABLE TEMP_MOVIES_DG_DATE' type status to client
GO

--WORK OUT DOWNGRADE DATE
SELECT Account_number
       ,MAX(end_date)AS premium_end_date
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_MOVIES_DG_DATE
FROM ${CBAF_DB_LIVE_SCHEMA}.TEMP_MOVIES
WHERE current_movies_premiums > 0
GROUP BY Account_number
GO

-- Create Index
CREATE INDEX indx_MOVIES2 ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_MOVIES_DG_DATE(account_number)
GO

-- Update ADSMART Table
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET Movies_Status = CASE WHEN HIGHEST = 0 AND LOWEST = 0                  THEN 'Never had Movies'
                         WHEN current_movies_premiums > 0 AND end_date >= TODAY()                       THEN 'Has Movies'
                         WHEN current_movies_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) <= 90  THEN 'No Movies, downgraded in last 3 mths'
                         WHEN current_movies_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) BETWEEN 91 AND 365 THEN 'No Movies, downgraded in last 4 - 12 months'
                         WHEN current_movies_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) > 365              THEN 'No Movies, hasn''t downgraded in last 12 mths, has Movies previously'
                         ELSE Movies_Status
                    END
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_MOVIES_PREMIUMS AS TMP
ON AD.ACCOUNT_NUMBER = TMP.ACCOUNT_NUMBER
LEFT JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_MOVIES_DG_DATE AS TMDD
ON AD.ACCOUNT_NUMBER = TMDD.ACCOUNT_NUMBER
LEFT JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_MOVIES AS TM
ON AD.ACCOUNT_NUMBER = TM.ACCOUNT_NUMBER
WHERE sorting_rank = 1
GO

DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_MOVIES
DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_MOVIES_PREMIUMS
DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_MOVIES_DG_DATE
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
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_SPORTS'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_SPORTS already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_SPORTS
  END
MESSAGE 'CREATE TABLE TEMP_SPORTS' type status to client
GO

SELECT  csh.Account_number
        ,csh.effective_from_dt AS start_date
        ,csh.effective_to_dt AS end_date
        ,CASE WHEN ncel.prem_SPORTS IS NULL THEN 0 ELSE ncel.prem_SPORTS END AS current_SPORTS_premiums
         ,rank() over (PARTITION BY csh.account_number ORDER BY end_date DESC, start_date DESC, csh.status_start_dt DESC, csh.cb_row_id DESC) AS sorting_rank
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_SPORTS
FROM ${CBAF_DB_LIVE_SCHEMA}.cust_subs_hist AS csh
         inner join ${CBAF_DB_LIVE_SCHEMA}.cust_entitlement_lookup AS ncel
                    ON csh.current_short_description = ncel.short_description
WHERE csh.effective_to_dt > csh.effective_from_dt
AND subscription_sub_type = 'DTV Primary Viewing'
AND status_code IN ('AC','PC','AB')   -- Active records
AND csh.account_number IS NOT NULL
GO

-- Create Index
CREATE INDEX indx_SPORTS ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_SPORTS(account_number)
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_SPORTS_PREMIUMS'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_SPORTS_PREMIUMS already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_SPORTS_PREMIUMS
  END
MESSAGE 'CREATE TABLE TEMP_SPORTS_PREMIUMS' type status to client
GO

--WORKOUT IF PREMIUM EVER CHANGED
SELECT Account_number
       ,MAX(current_SPORTS_premiums) AS HIGHEST
       ,MIN(current_SPORTS_premiums) AS LOWEST
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_SPORTS_PREMIUMS
FROM ${CBAF_DB_LIVE_SCHEMA}.TEMP_SPORTS
GROUP BY Account_number
GO

-- Create Index
CREATE INDEX indx_SPORTS1 ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_SPORTS_PREMIUMS(account_number)
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_SPORTS_DG_DATE'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_SPORTS_DG_DATE already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_SPORTS_DG_DATE
  END
MESSAGE 'CREATE TABLE TEMP_SPORTS_DG_DATE' type status to client
GO

--WORK OUT DOWNGRADE DATE
SELECT Account_number
       ,MAX(end_date)AS premium_end_date
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_SPORTS_DG_DATE
FROM ${CBAF_DB_LIVE_SCHEMA}.TEMP_SPORTS
WHERE current_SPORTS_premiums > 0
GROUP BY Account_number
GO

-- Create Index
CREATE INDEX indx_SPORTS2 ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_SPORTS_DG_DATE(account_number)
GO

-- Update ADSMART Table
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET sports_status = CASE WHEN HIGHEST = 0 AND LOWEST = 0                                                                               THEN 'Never had Sports'
                         WHEN current_SPORTS_premiums > 0 AND end_date >= TODAY()                                                      THEN 'Has Sports'
                         WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) <= 90              THEN 'No Sports, downgraded in last 3 months'
                         WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) BETWEEN 91 AND 365 THEN 'No Sports, downgraded in last 4 - 12 months'
                         WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) > 365              THEN 'No Sports, hasn''t downgraded in last 12 mths, had Sports previously'
                         ELSE SPORTS_Status
                    END
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_SPORTS_PREMIUMS AS TMP
ON AD.ACCOUNT_NUMBER = TMP.ACCOUNT_NUMBER
LEFT JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_SPORTS_DG_DATE AS TMDD
ON AD.ACCOUNT_NUMBER = TMDD.ACCOUNT_NUMBER
LEFT JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_SPORTS AS TM
ON AD.ACCOUNT_NUMBER = TM.ACCOUNT_NUMBER
WHERE sorting_rank = 1
GO

DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_SPORTS
DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_SPORTS_PREMIUMS
DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_SPORTS_DG_DATE

MESSAGE 'Populate field SPORTS_STATUS - END' type status to client
GO

/************************************
 *                                  *
 *        Newspaper Readership      *
 *                                  *
 ************************************/

MESSAGE 'Populate field NEWS_READERSHIP - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_PAPER'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_PAPER already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_PAPER
  END

MESSAGE 'CREATE TABLE TEMP_PAPER' type status to client
GO


SELECT LIFE.cb_key_household
--tabloid
       ,SUM(CASE WHEN s3_003691_data_intr_read_news_express                       ='Y' THEN 1 ELSE 0 END) AS EXPRESS
       ,SUM(CASE WHEN s3_003692_data_intr_read_news_daily_mail                    ='Y' THEN 1 ELSE 0 END) AS DAILY_MAIL
       ,SUM(CASE WHEN s3_003693_data_intr_read_news_mirror                        ='Y' THEN 1 ELSE 0 END) AS MIRROR
       ,SUM(CASE WHEN s3_003695_data_intr_read_news_daily_evening_standard        ='Y' THEN 1 ELSE 0 END) AS EVENING_STANDARD
       ,SUM(CASE WHEN s3_003707_data_intr_read_news_record                        ='Y' THEN 1 ELSE 0 END) AS THE_RECORD
       ,SUM(CASE WHEN s3_003711_data_intr_read_news_daily_star                    ='Y' THEN 1 ELSE 0 END) AS DAILY_STAR
       ,SUM(CASE WHEN s3_003712_data_intr_read_news_sun                           ='Y' THEN 1 ELSE 0 END) AS SUN
       ,SUM(CASE WHEN s3_003741_data_intr_read_news_mail_on_sunday                ='Y' THEN 1 ELSE 0 END) AS MAIL_ON_SUNDAY
       ,SUM(CASE WHEN s3_003742_data_intr_read_news_newspaper_of_the_world        ='Y' THEN 1 ELSE 0 END) AS NOTW
       ,SUM(CASE WHEN s3_003746_data_intr_read_news_sunday_express                ='Y' THEN 1 ELSE 0 END) AS SUNDAY_EXPRESS
       ,SUM(CASE WHEN s3_003748_data_intr_read_news_sunday_sunday_mail            ='Y' THEN 1 ELSE 0 END) AS SUNDAY_MAIL
       ,SUM(CASE WHEN s3_003749_data_intr_read_news_sunday_mirror                 ='Y' THEN 1 ELSE 0 END) AS SUNDAY_MIRROR
       ,SUM(CASE WHEN s3_003750_data_intr_read_news_sunday_post                   ='Y' THEN 1 ELSE 0 END) AS SUNDAY_POST
       ,SUM(CASE WHEN s3_003752_data_intr_read_news_sunday_sport                  ='Y' THEN 1 ELSE 0 END) AS SUNDAY_SPORT
       ,SUM(CASE WHEN s3_003756_data_intr_read_news_the_people                    ='Y' THEN 1 ELSE 0 END) AS THE_PEOPLE
--broadsheet
       ,SUM(CASE WHEN s3_003708_data_intr_read_news_scotsman                      ='Y' THEN 1 ELSE 0 END) AS SCOTSMAN
       ,SUM(CASE WHEN s3_003694_data_intr_read_news_daily_telegraph               ='Y' THEN 1 ELSE 0 END) AS TELEGRAPH
       ,SUM(CASE WHEN s3_003753_data_intr_read_news_sunday_telegraph              ='Y' THEN 1 ELSE 0 END) AS SUNDAY_TELEGRAPH
       ,SUM(CASE WHEN s3_003754_data_intr_read_news_sunday_times                  ='Y' THEN 1 ELSE 0 END) AS SUNDAY_TIMES
       ,SUM(CASE WHEN s3_003697_data_intr_read_news_financial_times               ='Y' THEN 1 ELSE 0 END) AS FT
       ,SUM(CASE WHEN s3_003698_data_intr_read_news_herald                        ='Y' THEN 1 ELSE 0 END) AS HERALD
       ,SUM(CASE WHEN s3_003699_data_intr_read_news_daily_glasgow_herald_scotsman ='Y' THEN 1 ELSE 0 END) AS DGHS
       ,SUM(CASE WHEN s3_003700_data_intr_read_news_guardian                      ='Y' THEN 1 ELSE 0 END) AS GUARDIAN
       ,SUM(CASE WHEN s3_003701_data_intr_read_news_independent                   ='Y' THEN 1 ELSE 0 END) AS INDEPENDENT
       ,SUM(CASE WHEN s3_003713_data_intr_read_news_times                         ='Y' THEN 1 ELSE 0 END) AS TIMES
       ,SUM(CASE WHEN s3_003739_data_intr_read_news_independent_on_sunday         ='Y' THEN 1 ELSE 0 END) AS INDEPENDENT_ON_SUNDAY
       ,SUM(CASE WHEN s3_003743_data_intr_read_news_observer                      ='Y' THEN 1 ELSE 0 END) AS OBSERVER
       ,SUM(CASE WHEN s3_003745_data_intr_read_news_scotland_on_sunday            ='Y' THEN 1 ELSE 0 END) AS SOS
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_PAPER
FROM ${CBAF_DB_LIVE_SCHEMA}.EXPERIAN_LIFESTYLE AS LIFE
LEFT JOIN ${CBAF_DB_LIVE_SCHEMA}.PLAYPEN_EXPERIAN_LIFESTYLE AS PLAY
ON LIFE.cb_key_household = PLAY.cb_key_household
GROUP BY LIFE.cb_key_household
GO

-- Create Index
CREATE HG INDEX PAPER_HH_KEY ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_PAPER (cb_key_household)
GO

-- Update ADSMART Table
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET Newspaper_Readership = CASE WHEN (SCOTSMAN + TELEGRAPH + SUNDAY_TELEGRAPH + SUNDAY_TIMES + FT + HERALD + DGHS + GUARDIAN
                                      + INDEPENDENT + TIMES + INDEPENDENT_ON_SUNDAY + OBSERVER + SOS) > 0 AND
                                     (EXPRESS + DAILY_MAIL + MIRROR + EVENING_STANDARD + THE_RECORD + DAILY_STAR + SUN
                                      + MAIL_ON_SUNDAY + NOTW + SUNDAY_EXPRESS + SUNDAY_MAIL + SUNDAY_MIRROR + SUNDAY_POST
                                      + SUNDAY_SPORT + THE_PEOPLE) > 0 THEN 'Reads Both'
                                WHEN (SCOTSMAN + TELEGRAPH + SUNDAY_TELEGRAPH + SUNDAY_TIMES + FT + HERALD + DGHS + GUARDIAN
                                      + INDEPENDENT + TIMES + INDEPENDENT_ON_SUNDAY + OBSERVER + SOS) > 0 THEN 'Reads Broadsheet'
                                WHEN (EXPRESS + DAILY_MAIL + MIRROR + EVENING_STANDARD + THE_RECORD + DAILY_STAR + SUN
                                      + MAIL_ON_SUNDAY + NOTW + SUNDAY_EXPRESS + SUNDAY_MAIL + SUNDAY_MIRROR + SUNDAY_POST
                                      + SUNDAY_SPORT + THE_PEOPLE) > 0 THEN 'Reads Tabloid'
                                WHEN (SCOTSMAN + TELEGRAPH + SUNDAY_TELEGRAPH + SUNDAY_TIMES + FT + HERALD + DGHS + GUARDIAN
                                      + INDEPENDENT + TIMES + INDEPENDENT_ON_SUNDAY + OBSERVER + SOS) = 0 AND
                                     (EXPRESS + DAILY_MAIL + MIRROR + EVENING_STANDARD + THE_RECORD + DAILY_STAR + SUN
                                      + MAIL_ON_SUNDAY + NOTW + SUNDAY_EXPRESS + SUNDAY_MAIL + SUNDAY_MIRROR + SUNDAY_POST
                                      + SUNDAY_SPORT + THE_PEOPLE) = 0 THEN 'Reads None'
                                ELSE Newspaper_Readership
                           END
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_PAPER AS TP
ON AD.cb_key_household = TP.cb_key_household
GO

DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_PAPER
GO

MESSAGE 'Populate field NEWS_READERSHIP - END' type status to client
GO

/************************************
 *                                  *
 *        Line Rental Status        *
 *                                  *
 ************************************/

MESSAGE 'Populate field LINE_RENTAL_STATUS - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_LINE_RENTAL'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_LINE_RENTAL  already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_LINE_RENTAL
  END

MESSAGE 'CREATE TABLE TEMP_LINE_RENTAL' type status to client
GO

SELECT account_number
       ,MAX(effective_to_dt) AS end_date
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_LINE_RENTAL
FROM ${CBAF_DB_LIVE_SCHEMA}.cust_subs_hist
WHERE subscription_sub_type = 'SKY TALK LINE RENTAL'
AND status_code IN ('A','a','R','r','CRQ','crq')
GROUP BY account_number
GO

-- Create Index
CREATE HG INDEX LINE_RENTAL ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_LINE_RENTAL (ACCOUNT_NUMBER)
GO

-- Update ADSMART Table
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET Line_rental_status = CASE WHEN end_date > TODAY()                                  THEN 'Has Sky Line rental'
                              WHEN DATEDIFF(dd,end_date,TODAY()) <= 365                THEN 'No LR, downgraded in last 0 - 12 months'
                              WHEN DATEDIFF(dd,end_date,TODAY()) BETWEEN 366 AND 730  THEN 'No LR and hasn''t downgraded in last 24 mths+, had LR previously'
                              WHEN DATEDIFF(dd,end_date,TODAY()) >  730                THEN 'No LR,  downgraded 24 months+'
                              ELSE Line_rental_status
                         END
FROM  ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_LINE_RENTAL AS TLR
ON AD.ACCOUNT_NUMBER = TLR.ACCOUNT_NUMBER
GO

DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_LINE_RENTAL

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
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_OPT_OUT'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_OPT_OUT  already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_OPT_OUT
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
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_OPT_OUT
FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_SINGLE_ACCOUNT_VIEW AS SAV
INNER JOIN ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD
ON AD.ACCOUNT_NUMBER = SAV.ACCOUNT_NUMBER
GO

CREATE HG INDEX TOO ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_OPT_OUT (ACCOUNT_NUMBER)
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
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
FROM  ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_OPT_OUT AS TOO
ON AD.ACCOUNT_NUMBER = TOO.ACCOUNT_NUMBER
GO

DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_OPT_OUT
GO

MESSAGE 'Populate field marketing_opt_out - END' type status to client
GO

/************************************
 *                                  *
 *             On/Off Net           *
 *                                  *
 ************************************/
MESSAGE 'Populate field onnet_fibre - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_BPE'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_BPE  already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_BPE
  END

MESSAGE 'CREATE TABLE TEMP_BPE' type status to client
GO


-- 1) Get BROADBAND_POSTCODE_EXCHANGE postcodes

SELECT cb_address_postcode AS postcode
           ,MAX(mdfcode) AS exchID
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_BPE
FROM ${CBAF_DB_LIVE_SCHEMA}.BROADBAND_POSTCODE_EXCHANGE
GROUP BY postcode

UPDATE ${CBAF_DB_LIVE_SCHEMA}.TEMP_BPE SET postcode = REPLACE(postcode,' ','') -- Remove spaces for matching

-- 2) Get BB_POSTCODE_TO_EXCHANGE postcodes

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_PTE'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_BPE  already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_PTE
  END

MESSAGE 'CREATE TABLE TEMP_PTE' type status to client
GO

SELECT postcode
       ,MAX(exchange_id) AS exchID
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_PTE
FROM ${CBAF_DB_LIVE_SCHEMA}.BB_POSTCODE_TO_EXCHANGE
GROUP BY postcode
GO

UPDATE ${CBAF_DB_LIVE_SCHEMA}.TEMP_PTE SET postcode = REPLACE(postcode,' ','')  -- Remove spaces for matching
GO

-- 3) Combine postcode lists taking BB_POSTCODE_TO_EXCHANGE exchange_id's where possible
IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_ONNET_LOOKUP'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_ONNET_LOOKUP  already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_ONNET_LOOKUP
  END

MESSAGE 'CREATE TABLE TEMP_ONNET_LOOKUP' type status to client
GO


SELECT COALESCE(pte.postcode, bpe.postcode) AS postcode
      ,COALESCE(pte.exchID, bpe.exchID) AS exchange_id
      ,'OFFNET' AS exchange
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_ONNET_LOOKUP
FROM ${CBAF_DB_LIVE_SCHEMA}.TEMP_BPE AS BPE
FULL JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_PTE AS PTE ON bpe.postcode = pte.postcode
GO

-- 4) Update with latest Easynet exchange information

UPDATE ${CBAF_DB_LIVE_SCHEMA}.temp_onnet_lookup
   SET exchange = 'ONNET'
  FROM ${CBAF_DB_LIVE_SCHEMA}.temp_onnet_lookup AS base
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.easynet_rollout_data AS easy ON base.exchange_id = easy.exchange_id
WHERE easy.exchange_status = 'ONNET'
GO

CREATE HG INDEX POSTCODE ON ${CBAF_DB_LIVE_SCHEMA}.temp_onnet_lookup (EXCHANGE)
GO

-- 5) Connect with SAV data to pull back account number and Fibre Area flag
IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_ACT_NO'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_ACT_NO  already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_ACT_NO
  END

MESSAGE 'CREATE TABLE TEMP_ACT_NO' type status to client
GO

SELECT CASE WHEN EXCHANGE = 'ONNET' THEN 1 ELSE 0 END AS EXCHANGE
       ,ACCOUNT_NUMBER
       ,CASE WHEN bt_fibre_area = 'Yes' THEN 1 ELSE 0 END AS FIBRE_AREA
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_ACT_NO
FROM ${CBAF_DB_LIVE_SCHEMA}.temp_onnet_lookup AS TOL
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.CUST_SINGLE_ACCOUNT_VIEW AS SAV
ON TOL.postcode = SAV.cust_postcode_key
GO

-- 6) Dedupe SAV records

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_DUDUPED_ONNET'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_DUDUPED_ONNET already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_DUDUPED_ONNET
  END

MESSAGE 'CREATE TABLE TEMP_DUDUPED_ONNET' type status to client
GO

SELECT ACCOUNT_NUMBER
       ,MAX(EXCHANGE) AS EXCHANGE_STATUS
       ,MAX(FIBRE_AREA) AS BT_FIBRE_AREA
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_DUDUPED_ONNET
FROM ${CBAF_DB_LIVE_SCHEMA}.TEMP_ACT_NO
GROUP BY ACCOUNT_NUMBER
GO

CREATE HG INDEX ACTNO ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_DUDUPED_ONNET (ACCOUNT_NUMBER)
GO

-- 7) Update Adsmart Table

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET onnet_fibre = CASE WHEN TDO.exchange_status = 1 AND bt_fibre_area = 1  THEN 'On net, has fibre'
                       WHEN TDO.exchange_status = 1 AND bt_fibre_area = 0  THEN 'On net, no fibre'
                       WHEN TDO.exchange_status = 0 AND bt_fibre_area = 1  THEN 'Off net, has fibre'
                       WHEN TDO.exchange_status = 0 AND bt_fibre_area = 0  THEN 'Off net, no fibre'
                       ELSE onnet_fibre
                  END
FROM  ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_DUDUPED_ONNET AS TDO
ON AD.ACCOUNT_NUMBER = TDO.ACCOUNT_NUMBER
GO

DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_BPE
DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_PTE
DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.temp_onnet_lookup
DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_ACT_NO
DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_DUDUPED_ONNET
GO

MESSAGE 'Populate field onnet_fibre - END' type status to client
GO


/************************************
 *                                  *
 *     Recent Customer Issue        *
 *                                  *
 ************************************/
MESSAGE 'Populate field recent_customer_issue - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_CUST_ISSUE1'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_CUST_ISSUE1 already exists - Drop and recreate' type status to client
    DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_CUST_ISSUE1
  END

MESSAGE 'CREATE TABLE TEMP_CUST_ISSUE1' type status to client
GO

SELECT ACCOUNT_NUMBER
       ,MAX(opened_date) AS opened_date
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_CUST_ISSUE1
FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_TTM_CASE_HIST
GROUP BY ACCOUNT_NUMBER
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_CUST_ISSUE2'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_CUST_ISSUE2 already exists - Drop and recreate' type status to client
    DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_CUST_ISSUE2
  END

MESSAGE 'CREATE TABLE TEMP_CUST_ISSUE2' type status to client

GO

SELECT ACCOUNT_NUMBER
       ,MAX(CREATED_DATE) AS created_date
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_CUST_ISSUE2
FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_TECH_ENQUIRY
GROUP BY ACCOUNT_NUMBER
GO

-- Create Index
CREATE HG INDEX TCI1_ACT ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_CUST_ISSUE1 (account_number)
GO

CREATE HG INDEX TCI2_ACT ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_CUST_ISSUE2 (account_number)
GO

-- Update ADSMART Table
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET recent_customer_issue  = CASE WHEN DATEDIFF(DD,opened_date,TODAY())  <=7 OR DATEDIFF(DD,created_date,TODAY()) <=7 THEN 'Has Issue'
                                  ELSE recent_customer_issue
                             END
FROM  ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD
LEFT JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_CUST_ISSUE1 AS TCI1
     ON AD.account_number = TCI1.account_number
LEFT JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_CUST_ISSUE2 AS TCI2
     ON AD.account_number = TCI2.account_number
GO

DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_CUST_ISSUE1
DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_CUST_ISSUE2
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
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_MULTI_CUST_ISSUE1'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MULTI_CUST_ISSUE1 already exists - Drop and recreate' type status to client
    DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_CUST_ISSUE1
  END

MESSAGE 'CREATE TABLE TEMP_MULTI_CUST_ISSUE1' type status to client
GO

SELECT ACCOUNT_NUMBER
       ,DATEDIFF(DD,opened_date,TODAY()) AS TIMEGAP
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE1
FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_TTM_CASE_HIST
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_MULTI_CUST_ISSUE2'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MULTI_CUST_ISSUE2 already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE2
  END

MESSAGE 'CREATE TABLE TEMP_MULTI_CUST_ISSUE2' type status to client
GO

SELECT ACCOUNT_NUMBER
       ,DATEDIFF(DD,created_date,TODAY()) AS TIMEGAP
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE2
FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_TECH_ENQUIRY
GO

-- Create Index
CREATE HG INDEX TCI1_ACT ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE1 (account_number)
GO

CREATE HG INDEX TCI2_ACT ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE2 (account_number)
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_MULTI_CUST_ISSUE3'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MULTI_CUST_ISSUE3 already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE3
  END

MESSAGE 'CREATE TABLE TEMP_MULTI_CUST_ISSUE3' type status to client
GO

SELECT ACCOUNT_NUMBER
       ,COUNT(TIMEGAP) AS TG
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE3
FROM ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE1
WHERE TIMEGAP <=30
GROUP BY ACCOUNT_NUMBER
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_MULTI_CUST_ISSUE3A'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MULTI_CUST_ISSUE3A already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE3A
  END

MESSAGE 'CREATE TABLE TEMP_MULTI_CUST_ISSUE3A' type status to client
GO

SELECT ACCOUNT_NUMBER
       ,COUNT(TIMEGAP) AS TG
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE3A
FROM ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE2
WHERE TIMEGAP <=30
GROUP BY ACCOUNT_NUMBER
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_MULTI_CUST_ISSUE4'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MULTI_CUST_ISSUE4 already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE4
  END

MESSAGE 'CREATE TABLE TEMP_MULTI_CUST_ISSUE4' type status to client
GO

SELECT ACCOUNT_NUMBER
       ,COUNT(TIMEGAP) AS TG
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE4
FROM ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE1
WHERE TIMEGAP <=90
GROUP BY ACCOUNT_NUMBER
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_MULTI_CUST_ISSUE4A'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MULTI_CUST_ISSUE4A already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE4A
  END

MESSAGE 'CREATE TABLE TEMP_MULTI_CUST_ISSUE4A' type status to client
GO

SELECT ACCOUNT_NUMBER
       ,COUNT(TIMEGAP) AS TG
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE4A
FROM ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE2
WHERE TIMEGAP <=90
GROUP BY ACCOUNT_NUMBER
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_MULTI_CUST_ISSUE5'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MULTI_CUST_ISSUE5 already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE5
  END

MESSAGE 'CREATE TABLE TEMP_MULTI_CUST_ISSUE5' type status to client
GO

SELECT ACCOUNT_NUMBER
       ,COUNT(TIMEGAP) AS TG
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE5
FROM ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE1
WHERE TIMEGAP <=365
GROUP BY ACCOUNT_NUMBER
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_MULTI_CUST_ISSUE5A'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MULTI_CUST_ISSUE5A already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE5A
  END

MESSAGE 'CREATE TABLE TEMP_MULTI_CUST_ISSUE5A' type status to client
GO

SELECT ACCOUNT_NUMBER
       ,COUNT(TIMEGAP) AS TG
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE5A
FROM ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE2
WHERE TIMEGAP <=365
GROUP BY ACCOUNT_NUMBER
GO

-- Create Index
CREATE HG INDEX TCI3_ACT ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE3 (account_number)
CREATE HG INDEX TCI3A_ACT ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE3A (account_number)
CREATE HG INDEX TCI4_ACT ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE4 (account_number)
CREATE HG INDEX TCI4A_ACT ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE4A (account_number)
CREATE HG INDEX TCI5_ACT ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE5 (account_number)
CREATE HG INDEX TCI5A_ACT ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE5A (account_number)
GO

-- Update ADSMART Table
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET multiple_customer_issues = CASE WHEN (TCI3.TG + TCI3A.TG) > 1 THEN 'Has had issue last 1 mths'
                                    WHEN (TCI4.TG + TCI4A.TG) > 1 THEN 'Has had issue last 3 mths'
                                    WHEN (TCI5.TG + TCI5A.TG) > 1 THEN 'Has had issue last 12 mths'
                                    ELSE multiple_customer_issues
                               END
FROM  ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD
LEFT JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE3   AS TCI3
     ON AD.account_number = TCI3.account_number
LEFT JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE3A  AS TCI3A
     ON AD.account_number = TCI3A.account_number
LEFT JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE4   AS TCI4
     ON AD.account_number = TCI4.account_number
LEFT JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE4A  AS TCI4A
     ON AD.account_number = TCI4A.account_number
LEFT JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE5   AS TCI5
     ON AD.account_number = TCI5.account_number
LEFT JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE5A  AS TCI5A
     ON AD.account_number = TCI5A.account_number
GO

DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE1
DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE2
DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE3
DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE3A
DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE4
DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE4A
DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE5
DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_MULTI_CUST_ISSUE5A
GO

MESSAGE 'Populate field multiple_customer_issues - END' type status to client
GO

/************************************
 *                                  *
 *         Age Group - 16-24        *
 *                                  *
 ************************************/

MESSAGE 'Populate field age_group - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_AGE_GROUP'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_AGE_GROUP already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_AGE_GROUP
  END

MESSAGE 'CREATE TABLE TEMP_AGE_GROUP' type status to client
GO

SELECT  CON.cb_key_household
       ,CON.cb_key_individual
       ,CON.p_actual_age
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_AGE_GROUP
FROM
(select cb_key_individual, max(CB_ROW_ID) AS MAX_ROW_ID
from ${CBAF_DB_LIVE_SCHEMA}.experian_consumerview
GROUP BY cb_key_individual) AS DUPE
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.experian_consumerview AS CON
ON DUPE.cb_key_individual = CON.cb_key_individual and DUPE.MAX_ROW_ID = CON.cb_row_id
GO

-- Create Index
CREATE HG INDEX ix_cbkeyhh ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_AGE_GROUP (cb_key_household)
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_AGE_GROUP_MAX'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_AGE_GROUP_MAX already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_AGE_GROUP_MAX
  END

MESSAGE 'CREATE TABLE TEMP_AGE_GROUP_MAX' type status to client
GO

SELECT  cb_key_household
       ,MAX(CASE WHEN p_actual_age >= 16 AND p_actual_age < 25 THEN 1 ELSE 0 END) AS HH_Has_Age_16to24
       ,MAX(CASE WHEN p_actual_age >= 25 AND p_actual_age < 35 THEN 1 ELSE 0 END) AS HH_Has_Age_25to34
       ,MAX(CASE WHEN p_actual_age >= 35 AND p_actual_age < 45 THEN 1 ELSE 0 END) AS HH_Has_Age_35to44
       ,MAX(CASE WHEN p_actual_age >= 45 AND p_actual_age < 55 THEN 1 ELSE 0 END) AS HH_Has_Age_45to54
       ,MAX(CASE WHEN p_actual_age >= 55 AND p_actual_age < 65 THEN 1 ELSE 0 END) AS HH_Has_Age_55to64
       ,MAX(CASE WHEN p_actual_age >= 65                       THEN 1 ELSE 0 END) AS HH_Has_Age_Over_65
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_AGE_GROUP_MAX
FROM ${CBAF_DB_LIVE_SCHEMA}.TEMP_AGE_GROUP
GROUP BY cb_key_household
GO

-- Create Index
CREATE HG INDEX ix_cbkeyhh2 ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_AGE_GROUP_MAX (cb_key_household)
GO

-- Update ADSMART Table
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET age_group = CASE WHEN HH_Has_Age_16to24 = 0 AND HH_Has_Age_25to34 = 1 THEN 'Youngest adult is 25 - 34'
                     WHEN HH_Has_Age_16to24 = 1                           THEN 'Youngest adult is 16- 24'
                     ELSE age_group
                END
FROM  ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_AGE_GROUP_MAX AS TAG
ON AD.cb_key_household = TAG.cb_key_household
GO

DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_AGE_GROUP
DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_AGE_GROUP_MAX
GO


MESSAGE 'Populate field age_group - END' type status to client
GO

/************************************
 *                                  *
 *    Mobile (is on a contract)     *
 *                                  *
 ************************************/
MESSAGE 'Populate field mobile_contract - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_CONTRACT_MOBILE'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_CONTRACT_MOBILE already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_CONTRACT_MOBILE
  END

MESSAGE 'CREATE TABLE TEMP_CONTRACT_MOBILE' type status to client
GO


select cv.cb_key_household
       ,MAX(CAST(mobile_phone_is_on_contract_tariff_percentile as int)) as mobile_phone_is_on_contract_tariff_percentile
INTO   ${CBAF_DB_LIVE_SCHEMA}.TEMP_CONTRACT_MOBILE
FROM ${CBAF_DB_LIVE_SCHEMA}.EXPERIAN_CONSUMERVIEW AS CV
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.PERSON_PROPENSITIES_GRID_NEW AS pr   ON cv.p_pixel_v2 = pr.ppixel2011 AND pr.mosaic_uk_2009_type = cv.Pc_mosaic_uk_type
GROUP BY cb_key_household
GO

-- Create Index
CREATE HG INDEX ix_cbkeyp1 ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_CONTRACT_MOBILE (cb_key_household)
GO

-- Update ADSMART Table
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET mobile_contract = CASE WHEN mobile_phone_is_on_contract_tariff_percentile >= 85 THEN 'On Contract'
                           ELSE 'Unknown'
                     END
FROM  ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_CONTRACT_MOBILE AS TCM
ON AD.cb_key_household = TCM.cb_key_household
GO

DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_CONTRACT_MOBILE

MESSAGE 'Populate field mobile_contract - END' type status to client
GO

/************************************
 *                                  *
 *    Monthly average mobile bill   *
 *                                  *
 ************************************/

MESSAGE 'Populate field mobile_avg_monthly_bill - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_MOBILE_BILL'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MOBILE_BILL already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_MOBILE_BILL
  END

MESSAGE 'CREATE TABLE TEMP_MOBILE_BILL' type status to client
GO

/************************************
 *                                  *
 * Type of shopper/ Fashion Segment *
 *                                  *
 ************************************/
MESSAGE 'Populate field Type_of_Shopper - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_FASHION'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_FASHION already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_FASHION
  END

MESSAGE 'CREATE TABLE TEMP_FASHION' type status to client
GO

SELECT cb_key_household
       ,max(p_fashion_segments) as p_fashion_segments
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_FASHION
FROM ${CBAF_DB_LIVE_SCHEMA}.EXPERIAN_CONSUMERVIEW
GROUP BY cb_key_household
GO

	
-- Create Index
CREATE HG INDEX TF ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_FASHION (cb_key_household)
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_FASHION2'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_FASHION2 already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_FASHION2
  END

MESSAGE 'CREATE TABLE TEMP_FASHION2' type status to client
GO

SELECT cb_key_household
       ,MAX(p_fashion_segments) as p_fashion_segments
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_FASHION2
FROM ${CBAF_DB_LIVE_SCHEMA}.TEMP_FASHION
GROUP BY cb_key_household
GO

CREATE HG INDEX TF2 ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_FASHION2 (cb_key_household)
GO

-- Update ADSMART Table
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET TYPE_OF_SHOPPER = CASE WHEN p_fashion_segments = 'F01' THEN 'F1'
                            WHEN p_fashion_segments = 'F02' THEN 'F2'
                            WHEN p_fashion_segments = 'F03' THEN 'F3'
                            WHEN p_fashion_segments = 'F04' THEN 'F4'
                            WHEN p_fashion_segments = 'F05' THEN 'F5'
                            WHEN p_fashion_segments = 'F06' THEN 'F6'
                            WHEN p_fashion_segments = 'F07' THEN 'F7'
                            WHEN p_fashion_segments = 'F08' THEN 'F8'
                            WHEN p_fashion_segments = 'F09' THEN 'F9'
                            WHEN p_fashion_segments = 'M01' THEN 'M1'
                            WHEN p_fashion_segments = 'M02' THEN 'M2'
                            WHEN p_fashion_segments = 'M03' THEN 'M3'
                            WHEN p_fashion_segments = 'M04' THEN 'M4'
                            WHEN p_fashion_segments = 'M05' THEN 'M5'
                            WHEN p_fashion_segments = 'M06' THEN 'M6'
                            WHEN p_fashion_segments = 'M07' THEN 'M7'
                            WHEN p_fashion_segments = 'M08' THEN 'M8'
                            WHEN p_fashion_segments = 'M09' THEN 'M9'
                            WHEN p_fashion_segments = '99' THEN 'Unknown'
                            ELSE p_fashion_segments
                       END
FROM  ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_FASHION2 AS TF2
ON AD.cb_key_household  = TF2.cb_key_household
GO

DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_FASHION
DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_FASHION2

MESSAGE 'Populate field Type_of_Shopper - END' type status to client
GO

/************************************
 *                                  *
 *       Public Sector Mosaic       *
 *                                  *
 ************************************/
MESSAGE 'Populate field Public_Sector_Mosaic - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_MOSAIC'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MOSAIC already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_MOSAIC
  END

MESSAGE 'CREATE TABLE TEMP_MOSAIC' type status to client
GO

SELECT   exp_cb_key_household
        ,max(h_mosaic_public_sector_group_num) AS h_mosaic_public_sector_group_num
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_MOSAIC
FROM ${CBAF_DB_LIVE_SCHEMA}.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD
GROUP BY exp_cb_key_household;
GO

-- Create Index
CREATE HG INDEX MOSAIC_HH_KEY ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_MOSAIC (exp_cb_key_household)
GO

-- Update ADSMART Table
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET Public_Sector_Mosaic = CASE WHEN h_mosaic_public_sector_group_num = '01' THEN 'Group A'
                                WHEN h_mosaic_public_sector_group_num = '02' THEN 'Group B'
                                WHEN h_mosaic_public_sector_group_num = '03' THEN 'Group C'
                                WHEN h_mosaic_public_sector_group_num = '04' THEN 'Group D'
                                WHEN h_mosaic_public_sector_group_num = '05' THEN 'Group E'
                                WHEN h_mosaic_public_sector_group_num = '06' THEN 'Group F'
                                WHEN h_mosaic_public_sector_group_num = '07' THEN 'Group G'
                                WHEN h_mosaic_public_sector_group_num = '08' THEN 'Group H'
                                WHEN h_mosaic_public_sector_group_num = '09' THEN 'Group I'
                                WHEN h_mosaic_public_sector_group_num = '10' THEN 'Group J'
                                WHEN h_mosaic_public_sector_group_num = '11' THEN 'Group K'
                                WHEN h_mosaic_public_sector_group_num = '12' THEN 'Group L'
                                WHEN h_mosaic_public_sector_group_num = '13' THEN 'Group M'
                                WHEN h_mosaic_public_sector_group_num = '14' THEN 'Group N'
                                WHEN h_mosaic_public_sector_group_num = '15' THEN 'Group O'
                                ELSE Public_Sector_Mosaic
                           END
FROM  ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_MOSAIC AS TM
ON cb_key_household = exp_cb_key_household
GO

DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_MOSAIC

MESSAGE 'Populate field Public_Sector_Mosaic - END' type status to client
GO

/************************************
 *                                  *
 *       Number of Cars in HH       *
 *                                  *
 ************************************/
MESSAGE 'Populate field number_of_cars - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_CARS'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_CARS already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_CARS
  END

MESSAGE 'CREATE TABLE TEMP_CARS' type status to client
GO

SELECT  cv.cb_key_household
       ,MAX(CAST(own_1_car_or_light_van_percentile as int)) as own_1_car
       ,MAX(CAST(own_2_cars_or_light_vans_percentile as int)) as own_2_cars
       ,MAX(CAST(own_3_cars_or_light_vans_percentile as int)) as own_3_or_more_cars
INTO   ${CBAF_DB_LIVE_SCHEMA}.TEMP_CARS
FROM ${CBAF_DB_LIVE_SCHEMA}.EXPERIAN_CONSUMERVIEW AS CV
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.HOUSEHOLD_PROPENSITIES_GRID_NEW AS pr   ON cv.h_pixel_v2 = pr.hpixel2011 AND pr.mosaic_uk_2009_type = cv.Pc_mosaic_uk_type
GROUP BY cb_key_household
GO

-- Create Index
CREATE HG INDEX CARS_HH_KEY ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_CARS (cb_key_household)
GO

-- Update ADSMART Table
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET number_of_cars = CASE WHEN own_3_or_more_cars > 95 THEN 'Owns 3+ Cars'
                          WHEN own_2_cars         > 91 THEN 'Owns 2 Cars'
                          WHEN own_1_car          > 71 THEN 'Owns 1 Car'
                          ELSE number_of_cars
                     END
FROM  ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_CARS AS CARS
ON AD.cb_key_household = CARS.cb_key_household
GO

DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_CARS

MESSAGE 'Populate field number_of_cars - END' type status to client
GO

/************************************
 *                                  *
 *      Senior Decision Makers      *
 *                                  *
 ************************************/
MESSAGE 'Populate field senior_decision_maker - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_DECISION_MAKERS'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_DECISION_MAKERS already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_DECISION_MAKERS
  END

MESSAGE 'CREATE TABLE TEMP_DECISION_MAKERS' type status to client
GO

SELECT   exp_cb_key_household
        ,MAX(h_directorships_fine) AS h_directorships_fine
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_DECISION_MAKERS
FROM ${CBAF_DB_LIVE_SCHEMA}.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD
GROUP BY exp_cb_key_household;
GO

-- Create Index
CREATE HG INDEX TDM_HH_KEY ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_DECISION_MAKERS (exp_cb_key_household)
GO

-- Update ADSMART Table
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET senior_decision_maker = CASE WHEN h_directorships_fine = '1' THEN 'Owner, Less than 50 Employees'
                                 WHEN h_directorships_fine = '2' THEN 'Owner, More than 50 Employees'
                                 ELSE senior_decision_maker
                            END
FROM  ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_DECISION_MAKERS AS TDM
ON cb_key_household = exp_cb_key_household
GO

DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_DECISION_MAKERS

MESSAGE 'Populate field senior_decision_maker - END' type status to client
GO

/************************************
 *                                  *
 *           Pet ownership          *
 *                                  *
 ************************************/

MESSAGE 'Populate field PET_OWNERSHIP - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_PETS'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_PETS already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_PETS
  END

MESSAGE 'CREATE TABLE TEMP_PETS' type status to client
GO

SELECT  cv.cb_key_household
       ,MAX(CAST(have_a_cat_percentile as int)) as have_a_cat_percentile
       ,MAX(CAST(have_a_dog_percentile as int)) as have_a_dog_percentile
INTO   ${CBAF_DB_LIVE_SCHEMA}.TEMP_PETS
FROM ${CBAF_DB_LIVE_SCHEMA}.EXPERIAN_CONSUMERVIEW AS CV
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.HOUSEHOLD_PROPENSITIES_GRID_NEW AS pr   ON cv.h_pixel_v2 = pr.hpixel2011 AND pr.mosaic_uk_2009_type = cv.Pc_mosaic_uk_type
GROUP BY cb_key_household
GO
-- Create Index
CREATE HG INDEX TP_HH_KEY ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_PETS (cb_key_household)
GO

-- Update ADSMART Table
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET pet_ownership = CASE WHEN have_a_cat_percentile >= 94 AND have_a_dog_percentile >= 93 THEN 'Has Cat and Dog'
                         WHEN have_a_cat_percentile >= 94                                 THEN 'Has Cat'
                         WHEN have_a_dog_percentile >= 93                                 THEN 'Has Dog'
                         ELSE pet_ownership
                     END
FROM  ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_PETS AS PETS
ON AD.cb_key_household = PETS.cb_key_household
GO

DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_PETS

MESSAGE 'Populate field PET_OWNERSHIP - END' type status to client
GO

/************************************
 *                                  *
 *     Breakdown Renewal Month      *
 *                                  *
 ************************************/

MESSAGE 'Populate field breakdown_renwal_month - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_RENEWAL_MONTH'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_RENEWAL_MONTH already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_RENEWAL_MONTH
  END

MESSAGE 'CREATE TABLE TEMP_RENEWAL_MONTH' type status to client
GO


SELECT  MAX(CAST(s2_000175_data_insu_vehi_bdwn_renewal_month_breakdown AS int)) AS renewal_month
        ,cb_key_household
INTO    ${CBAF_DB_LIVE_SCHEMA}.TEMP_RENEWAL_MONTH
FROM ${CBAF_DB_LIVE_SCHEMA}.EXPERIAN_LIFESTYLE
GROUP BY cb_key_household
GO

-- Create Index
CREATE HG INDEX TRM_HH_KEY ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_RENEWAL_MONTH (cb_key_household)
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_TIME_TO_RENEWAL'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_TIME_TO_RENEWAL already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_TIME_TO_RENEWAL
  END

MESSAGE 'CREATE TABLE TEMP_TIME_TO_RENEWAL' type status to client
GO

SELECT cb_key_household
       ,CASE WHEN renewal_month >= DATEPART(mm,TODAY()) THEN (renewal_month - DATEPART(mm,TODAY()))
             WHEN renewal_month <  DATEPART(mm,TODAY()) THEN 12-(DATEPART(mm,TODAY())-renewal_month)
        END AS TIME_TO_RENEWAL
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_TIME_TO_RENEWAL
FROM ${CBAF_DB_LIVE_SCHEMA}.TEMP_RENEWAL_MONTH
GO

CREATE HG INDEX renewal ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_TIME_TO_RENEWAL (cb_key_household)
GO

-- Update ADSMART Table
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET breakdown_renwal_month = CASE WHEN TIME_TO_RENEWAL = 4 THEN 'Has Car, Renewal Due in 4 months'
                                  WHEN TIME_TO_RENEWAL = 3 THEN 'Has Car, Renewal Due in 3 months'
                                  WHEN TIME_TO_RENEWAL = 2 THEN 'Has Car, Renewal Due in 2 months'
                                  WHEN TIME_TO_RENEWAL = 1 THEN 'Has Car, Renewal Due in 1 month'
				ELSE 'Unknown'
                             END
FROM  ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_TIME_TO_RENEWAL AS TTTR
ON AD.cb_key_household = TTTR.cb_key_household
GO

DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_RENEWAL_MONTH
DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_TIME_TO_RENEWAL

MESSAGE 'Populate field breakdown_renwal_month - END' type status to client
GO







-- Adsmart L3 Drop 1 Other Attributes END
-- Adsmart Tactical Soln. START
MESSAGE 'Count of records on ALL_CLIENTS_CUSTOM_ATTRIBUTE_TACTICAL - START' type status to client
GO
select count(*) FROM ${OM_SCHEMA}.ALL_CLIENTS_CUSTOM_ATTRIBUTE_TACTICAL
GO
MESSAGE 'Count of records on ALL_CLIENTS_CUSTOM_ATTRIBUTE_TACTICAL - COMPLETE' type status to client
GO
MESSAGE 'Populate TACTICAL ATTRIBUTE fields - START' type status to client
GO
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART A
SET    A.tactical_fa1  = B.tactical_fa1,
       A.tactical_fa2  = B.tactical_fa2,
       A.tactical_fa3  = B.tactical_fa3,
       A.tactical_fa4  = B.tactical_fa4,
       A.tactical_fa5  = B.tactical_fa5,
       A.tactical_fa6  = B.tactical_fa6,
       A.tactical_fa7  = B.tactical_fa7,
       A.tactical_fa8  = B.tactical_fa8,
       A.tactical_fa9  = B.tactical_fa9,
       A.tactical_fa10 = B.tactical_fa10,
       A.tactical_fa11 = B.tactical_fa11,
       A.tactical_fa12 = B.tactical_fa12,
       A.tactical_fa13 = B.tactical_fa13,
       A.tactical_fa14 = B.tactical_fa14,
       A.tactical_fa15 = B.tactical_fa15,
       A.tactical_fa16 = B.tactical_fa16,
       A.tactical_fa17 = B.tactical_fa17,
       A.tactical_fa18 = B.tactical_fa18,
       A.tactical_fa19 = B.tactical_fa19,
       A.tactical_fa20 = B.tactical_fa20,
       A.tactical_fa21 = B.tactical_fa21,
       A.tactical_fa22 = B.tactical_fa22,
       A.tactical_fa23 = B.tactical_fa23,
       A.tactical_fa24 = B.tactical_fa24,
       A.tactical_fa25 = B.tactical_fa25,
       A.tactical_fa26 = B.tactical_fa26,
       A.tactical_fa27 = B.tactical_fa27,
       A.tactical_fa28 = B.tactical_fa28,
       A.tactical_fa29 = B.tactical_fa29,
       A.tactical_fa30 = B.tactical_fa30
FROM   ${OM_SCHEMA}.ALL_CLIENTS_CUSTOM_ATTRIBUTE_TACTICAL B
WHERE  A.account_number = B.account_number
GO
MESSAGE 'Populate TACTICAL ATTRIBUTE fields - COMPLETE' type status to client
GO
-- Adsmart Tactical Soln. END
-- CCN1808 : ADD sports_ppv_customers and activated_sky_sports_5  START

MESSAGE 'Populate sports_ppv_customers  and activated_sky_sports_5  fields - START' type status to client
GO
MESSAGE '    create PPV temp table' type status to client
GO

select      account_number
            ,MAX(CASE WHEN ppv_genre='BOXING'    THEN 1 ELSE 0 END) AS boxing_ppv
            ,MAX(CASE WHEN ppv_genre='WRESTLING' THEN 1 ELSE 0 END) AS wrestling_ppv
into        #PPV
from        ${CBAF_DB_LIVE_SCHEMA}.cust_product_charges_ppv
where       ppv_service='EVENT'
--and         cast(event_dt as date)>@profile_date -@window_length
and         cast(event_dt as date)> (TODAY() - 548)
and         ppv_cancelled_dt = '9999-09-09'
group by    account_number
GO

create hg index idx1 on #PPV(account_number);
GO

MESSAGE '    create CCN1808_TMP_ADSMART table' type status to client
GO
create table ${CBAF_DB_DATA_SCHEMA}.CCN1808_TMP_ADSMART(
            account_number                      varchar(20) NULL DEFAULT NULL,
            boxing_ppv                          tinyint     NULL DEFAULT NULL,
            wrestling_ppv                       tinyint     NULL DEFAULT NULL,
            sports_ppv_customers                varchar(20) NULL DEFAULT 'Neither',
            activated_sky_sports_5              varchar(3)  NULL DEFAULT 'No');
GO

MESSAGE '    insert account_number into CCN1808_TMP_ADSMART table' type status to client
GO
insert into ${CBAF_DB_DATA_SCHEMA}.CCN1808_TMP_ADSMART(account_number)
select      account_number
from        ${CBAF_DB_DATA_SCHEMA}.ADSMART
order by    account_number
GO

MESSAGE '    update boxing_ppv, wrestling_ppv in CCN1808_TMP_ADSMART table' type status to client
GO
update      ${CBAF_DB_DATA_SCHEMA}.CCN1808_TMP_ADSMART
set         boxing_ppv=case when b.boxing_ppv =1       then 1 else 0 end,
            wrestling_ppv=case when b.wrestling_ppv =1 then 1 else 0 end
from        ${CBAF_DB_DATA_SCHEMA}.CCN1808_TMP_ADSMART as a
left join   #PPV as b
on          a.account_number=b.account_number
GO

MESSAGE '    update sports_ppv_customers in CCN1808_TMP_ADSMART table' type status to client
GO
update      ${CBAF_DB_DATA_SCHEMA}.CCN1808_TMP_ADSMART
set         sports_ppv_customers = case when boxing_ppv=1 and wrestling_ppv=1 then 'Both'
                                        when boxing_ppv=1 and wrestling_ppv=0 then 'Boxing Only'
                                        when boxing_ppv=0 and wrestling_ppv=1 then 'Wrestling Only'
                                                                              else 'Neither'
                                   end
GO

MESSAGE '    populate SS5 temp table' type status to client
GO
SELECT      account_number,
            max(case when subscription_type = 'A-LA-CARTE' and subscription_sub_type = 'SKYSPORTS5'  THEN 1 ELSE 0 END) AS ss5
INTO        #SS5
FROM        ${CBAF_DB_LIVE_SCHEMA}.cust_subs_hist
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
update      ${CBAF_DB_DATA_SCHEMA}.CCN1808_TMP_ADSMART
set         activated_sky_sports_5=case when b.ss5 =1 then 'Yes' else 'No' end
from        ${CBAF_DB_DATA_SCHEMA}.CCN1808_TMP_ADSMART as a
left join   #SS5 as b
on          a.account_number=b.account_number
GO

MESSAGE '    update main ADSMART table' type status to client
GO
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART A
SET    A.sports_ppv_customers   = B.sports_ppv_customers,
       A.activated_sky_sports_5 = B.activated_sky_sports_5
FROM   ${CBAF_DB_DATA_SCHEMA}.CCN1808_TMP_ADSMART B
WHERE  A.account_number = B.account_number
GO

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.CCN1808_TMP_ADSMART
GO
MESSAGE 'Populate sports_ppv_customers and activated_sky_sports_5 fields - COMPLETE' type status to client
GO
-- CCN1808 : ADD sports_ppv_customers and activated_sky_sports_5  END

-- Drop 2 Fixed Attributes Start

/************************************
 *                                  *
 *        SIMPLE_SEGMENTATION       *
 *                                  *
 ************************************/

MESSAGE 'POPULATE SIMPLE_SEGMENTATION FIELDS - STARTS' type status to client
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART 
SET SIMPLE_SEGMENTATION = CASE 	
			        WHEN LOWER(b.segment) LIKE '%support%'		THEN 	'Support'
				WHEN LOWER(b.segment) LIKE '%secure%'		THEN	'Secure'
				WHEN LOWER(b.segment) LIKE '%stimulate%'	THEN	'Stimulate'
				WHEN LOWER(b.segment) LIKE '%stabilise'		THEN	'Stabilise'
				WHEN LOWER(b.segment) LIKE '%start'		THEN	'Start'
			    ELSE 'Unknown' END
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART AS a 
JOIN ${CBAF_DB_LIVE_SCHEMA}.SIMPLE_SEGMENTS AS b 
		ON a.account_number = b.account_number 
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
INTO ${CBAF_DB_DATA_SCHEMA}.TEMP_MODELS_INT_TO_PURC
FROM MODELS.MODEL_SCORES
WHERE UPPER(MODEL_NAME) LIKE '%UPLIFT' 
	AND ((UPPER(MODEL_NAME) LIKE 'SPORT%' AND MODEL_RUN_DATE = @SPORTS_RUN_DATE) 
	OR (UPPER(MODEL_NAME) LIKE 'MOVIE%' AND MODEL_RUN_DATE = @MOVIES_RUN_DATE))
GROUP BY ACCOUNT_NUMBER , MODEL_NAME
GO

CREATE HG INDEX IDX1 ON ${CBAF_DB_DATA_SCHEMA}.TEMP_MODELS_INT_TO_PURC (ACCOUNT_NUMBER)
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET INTENTION_TO_PURCHASE_MOVIES = CASE WHEN TMP.DECILE IN (1,2,3,4,5,6,7,8,9,10) THEN  CAST(TMP.DECILE AS VARCHAR(10))  
					ELSE 'Unknown' END
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART AS BASE
JOIN ${CBAF_DB_DATA_SCHEMA}.TEMP_MODELS_INT_TO_PURC AS TMP ON BASE.ACCOUNT_NUMBER = TMP.ACCOUNT_NUMBER AND UPPER(TMP.MODEL_NAME) LIKE '%UPLIFT' AND UPPER(TMP.MODEL_NAME) LIKE 'MOVIE%' 
GO 

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET INTENTION_TO_PURCHASE_SPORTS = CASE WHEN TMP.DECILE IN (1,2,3,4,5,6,7,8,9,10) THEN  CAST(TMP.DECILE AS VARCHAR(10))
					ELSE 'Unknown' END
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART AS BASE
JOIN ${CBAF_DB_DATA_SCHEMA}.TEMP_MODELS_INT_TO_PURC AS TMP ON BASE.ACCOUNT_NUMBER = TMP.ACCOUNT_NUMBER AND UPPER(TMP.MODEL_NAME) LIKE '%UPLIFT' AND UPPER(TMP.MODEL_NAME) LIKE 'SPORT%' 
GO

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.TEMP_MODELS_INT_TO_PURC
GO

MESSAGE 'POPULATE INTENTION_TO_PURCHASE_MOVIES & SPORTS - COMPLETED' type status to client
GO



/****************************************************************
 *                                       			*
 *        MOBILE AVERAGE BILL , 2ND MORTGAGE AND EARLY ADOPTER  *
 * 								*
 ****************************************************************/

MESSAGE 'POPULATE MOBILE AVERAGE BILL , 2ND MORTGAGE AND EARLY ADOPTER - Started' type status to client
GO

SELECT    CV.CB_KEY_HOUSEHOLD HH_KEY
	, C.ACCOUNT_NUMBER 
        , MAX(CAST(I_LOVE_HUNTING_OUT_THE_LATEST_TECHNOLOGY_PRODUCTS_AND_SERVICES_BEFORE_ANYONE_ELSE_CATCHES_ON_TO_THEM_PERCENTILE AS INT)) AS EARLY_ADOPTERS
	, MAX(CAST(I_M_ALWAYS_KEEN_TO_USE_NEW_TECHNOLOGY_PRODUCTS_AS_SOON_AS_THEY_ARE_AVAILABLE_PERCENTILE AS INT)) AS INNOVATORS
	, MAX(CAST(MONTHLY_EXPENDITURE_ON_MOBILE_PHONE_10_29_99_PERCENTILE AS INT)) AS MOBILE_10_29_99
	, MAX(CAST(MONTHLY_EXPENDITURE_ON_MOBILE_PHONE_30_49_99_PERCENTILE AS INT)) AS MOBILE_30_49_99
	, MAX(CAST(MONTHLY_EXPENDITURE_ON_MOBILE_PHONE_50_OR_MORE_PERCENTILE AS INT)) AS MOBILE_50
	, CASE WHEN (INNOVATORS >93) OR (EARLY_ADOPTERS > 86) THEN 1 ELSE 0 END AS EARLY_ADOPTER
	, CASE WHEN (MOBILE_50 >92) 			      THEN '£50+' 
			WHEN (MOBILE_30_49_99 >87)      THEN '£30 - £49.99' 
			WHEN (MOBILE_10_29_99 >70)      THEN '£10 - £29.99' 
			ELSE 'Unknown' END AS MOBILE_EXPENDITURE
	, MAX(CAST(OWN_PROPERTIES_OTHER_THAN_MAIN_RESIDENCE_RENT_OUT_ALL_OR_SOMETIMES_PERCENTILE AS INT)) AS MORTGAGE_2ND
INTO ${CBAF_DB_DATA_SCHEMA}.TMP_MAB_2MORT_EA
FROM ${CBAF_DB_LIVE_SCHEMA}.EXPERIAN_CONSUMERVIEW AS CV
JOIN ${CBAF_DB_LIVE_SCHEMA}.PERSON_PROPENSITIES_GRID_NEW AS PR 
	ON CV.P_PIXEL_V2 = PR.PPIXEL2011 
	AND PR.MOSAIC_UK_2009_TYPE = CV.H_MOSAIC_UK_TYPE
JOIN  ${CBAF_DB_DATA_SCHEMA}.ADSMART AS C ON CV.CB_KEY_HOUSEHOLD = C.CB_KEY_HOUSEHOLD 
GROUP BY HH_KEY, ACCOUNT_NUMBER
GO

CREATE HG INDEX IDQW ON ${CBAF_DB_DATA_SCHEMA}.TMP_MAB_2MORT_EA(ACCOUNT_NUMBER)
GO

CREATE HG INDEX IDW ON ${CBAF_DB_DATA_SCHEMA}.TMP_MAB_2MORT_EA(HH_KEY)
GO 


UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET  	 BASE.MOBILE_AVG_MONTHLY_BILL 	= COALESCE (TMP.MOBILE_EXPENDITURE, 'Unknown')							-- MOBILE EXPENDITURE
	,BASE.EARLY_ADOPTER 		= CASE 	WHEN TMP.EARLY_ADOPTER = 1 	THEN 'Early Adopters' 	ELSE 'Unknown' END		-- EARLY ADOPTER
	,BASE.SECOND_MORTGAGE		= CASE 	WHEN TMP.MORTGAGE_2ND >= 90 	THEN 'Yes' 	ELSE 'Unknown' END			-- 2ND MORTGAGE
FROM 	${CBAF_DB_DATA_SCHEMA}.ADSMART AS BASE
JOIN  	${CBAF_DB_DATA_SCHEMA}.TMP_MAB_2MORT_EA AS TMP ON BASE.ACCOUNT_NUMBER = TMP.ACCOUNT_NUMBER AND 
	BASE.CB_KEY_HOUSEHOLD = TMP.HH_KEY
GO

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.TMP_MAB_2MORT_EA 
GO

MESSAGE 'POPULATE MOBILE AVERAGE BILL , 2ND MORTGAGE AND EARLY ADOPTER - COMPLETED' type status to client
GO

/****************************************
 *                                      *
 *     MOSAIC 2014 GROUPS AND TYPES  	*
 *					*
 ***************************************/

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(TNAME)='TMP_MOSAIC'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE TMP_MOSAIC ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE ${CBAF_DB_DATA_SCHEMA}.TMP_MOSAIC
    END
MESSAGE 'CREATE TABLE TMP_MOSAIC' TYPE STATUS TO CLIENT
GO


SELECT 	  ACCOUNT_NUMBER
	, BASE.CB_KEY_HOUSEHOLD
	, MAX(H_MOSAIC_UK_TYPE_2014)			AS H_MOSAIC_UK_6_TYPE
	, MAX(H_MOSAIC_UK_GROUP_2014)			AS H_MOSAIC_UK_6_GROUP
INTO ${CBAF_DB_DATA_SCHEMA}.TMP_MOSAIC FROM ${CBAF_DB_LIVE_SCHEMA}.EXPERIAN_CONSUMERVIEW  AS CV
JOIN  ${CBAF_DB_DATA_SCHEMA}.ADSMART AS BASE ON CV.CB_KEY_HOUSEHOLD = BASE.CB_KEY_HOUSEHOLD
GROUP BY  ACCOUNT_NUMBER
	, BASE.CB_KEY_HOUSEHOLD
GO

CREATE HG INDEX IDEF ON ${CBAF_DB_DATA_SCHEMA}.TMP_MOSAIC(ACCOUNT_NUMBER)
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART 
SET  A.MOSAIC_2014_TYPES        = CASE  WHEN H_MOSAIC_UK_6_TYPE LIKE '01' THEN  'World-Class Wealth'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '02' THEN  'Uptown Elite'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '03' THEN  'Penthouse Chic'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '04' THEN  'Metro High-Flyers'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '05' THEN  'Premium Fortunes'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '06' THEN  'Diamond Days'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '07' THEN  'Alpha Families'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '08' THEN  'Bank of Mum and Dad'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '09' THEN  'Empty-Nest Adventure'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '10' THEN  'Wealthy Landowners'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '11' THEN  'Rural Vogue'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '12' THEN  'Scattered Homesteads'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '13' THEN  'Village Retirement'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '14' THEN  'Satellite Settlers'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '15' THEN  'Local Focus'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '16' THEN  'Outlying Seniors'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '17' THEN  'Far-Flung Outposts'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '18' THEN  'Legacy Elders'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '19' THEN  'Bungalow Haven'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '20' THEN  'Classic Grandparents'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '21' THEN  'Solo Retirees'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '22' THEN  'Boomerang Boarders'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '23' THEN  'Family Ties'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '24' THEN  'Fledgling Free'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '25' THEN  'Dependable Me'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '26' THEN  'Cafés and Catchments'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '27' THEN  'Thriving Independence'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '28' THEN  'Modern Parents'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '29' THEN  'Mid-Career Convention'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '30' THEN  'Primary Ambitions'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '31' THEN  'Affordable Fringe'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '32' THEN  'First-Rung Futures'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '33' THEN  'Contemporary Starts'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '34' THEN  'New Foundations'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '35' THEN  'Flying Solo'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '36' THEN  'Solid Economy'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '37' THEN  'Budget Generations'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '38' THEN  'Childcare Squeeze'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '39' THEN  'Families with Needs'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '40' THEN  'Make Do & Move On'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '41' THEN  'Disconnected Youth'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '42' THEN  'Midlife Stopgap'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '43' THEN  'Renting A Room'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '44' THEN  'Inner City Stalwarts'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '45' THEN  'Crowded Kaleidoscope'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '46' THEN  'High Rise Residents'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '47' THEN  'Streetwise Singles'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '48' THEN  'Low Income Workers'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '49' THEN  'Dependent Greys'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '50' THEN  'Pocket Pensions'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '51' THEN  'Aided Elderly'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '52' THEN  'Estate Veterans'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '53' THEN  'Seasoned Survivors'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '54' THEN  'Down-to-Earth Owners'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '55' THEN  'Offspring Overspill'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '56' THEN  'Self Supporters'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '57' THEN  'Community Elders'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '58' THEN  'Cultural Comfort'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '59' THEN  'Asian Heritage'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '60' THEN  'Ageing Access'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '61' THEN  'Career Builders'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '62' THEN  'Central Pulse'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '63' THEN  'Flexible Workforce'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '64' THEN  'Bus-Route Renters'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '65' THEN  'Learners & Earners'
                                        WHEN H_MOSAIC_UK_6_TYPE LIKE '66' THEN  'Student Scene'
                                        ELSE 'Unknown'   END
        ,A.MOSAIC_2014_GROUPS   = CASE  WHEN H_MOSAIC_UK_6_GROUP LIKE 'A' THEN  'City Propensity'
                                        WHEN H_MOSAIC_UK_6_GROUP LIKE 'B' THEN  'Prestige Positions'
                                        WHEN H_MOSAIC_UK_6_GROUP LIKE 'C' THEN  'Country Living'
                                        WHEN H_MOSAIC_UK_6_GROUP LIKE 'D' THEN  'Rural Reality'
                                        WHEN H_MOSAIC_UK_6_GROUP LIKE 'E' THEN  'Senior Security'
                                        WHEN H_MOSAIC_UK_6_GROUP LIKE 'F' THEN  'Suburban Stability'
                                        WHEN H_MOSAIC_UK_6_GROUP LIKE 'G' THEN  'Domestic Success'
                                        WHEN H_MOSAIC_UK_6_GROUP LIKE 'H' THEN  'Aspiring Homemakers'
                                        WHEN H_MOSAIC_UK_6_GROUP LIKE 'I' THEN  'Family Basics'
                                        WHEN H_MOSAIC_UK_6_GROUP LIKE 'J' THEN  'Transient Renters'
                                        WHEN H_MOSAIC_UK_6_GROUP LIKE 'K' THEN  'Municipal Challenge'
                                        WHEN H_MOSAIC_UK_6_GROUP LIKE 'L' THEN  'Vintage Value'
                                        WHEN H_MOSAIC_UK_6_GROUP LIKE 'M' THEN  'Modest Traditions'
                                        WHEN H_MOSAIC_UK_6_GROUP LIKE 'N' THEN  'Urban Cohesion'
                                        WHEN H_MOSAIC_UK_6_GROUP LIKE 'O' THEN  'Rental Hubs'
                                        ELSE 'Unknown'	 END
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART AS A
JOIN ${CBAF_DB_DATA_SCHEMA}.TMP_MOSAIC	AS B ON A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER AND A.CB_KEY_HOUSEHOLD = B.CB_KEY_HOUSEHOLD
GO
 
DROP TABLE ${CBAF_DB_DATA_SCHEMA}.TMP_MOSAIC
GO

/****************************************
 *                                      *
 *     SKY GENERATED HOME MOVER         *
 *                                      *
 ***************************************/
SELECT
                ACCOUNT_NUMBER
                , CASE
                        WHEN UPPER(HOME_MOVE_STATUS) = 'PRE HOME MOVE'  THEN 'Pre Home Move'
                        WHEN UPPER(HOME_MOVE_STATUS) = 'PENDING'        THEN 'Pending Home Move'
                        WHEN UPPER(HOME_MOVE_STATUS) = 'IN-PROGRESS'    THEN 'Post Home Move 0 - 30 Days'
                        WHEN UPPER(HOME_MOVE_STATUS) = 'POST HOME MOVE' AND DATEDIFF(dd, EFFECTIVE_FROM_DT, GETDATE()) BETWEEN 0 AND 30         THEN 'Post Home Move 0 - 30 Days'
                        WHEN UPPER(HOME_MOVE_STATUS) = 'POST HOME MOVE' AND DATEDIFF(dd, EFFECTIVE_FROM_DT, GETDATE()) BETWEEN 30 AND 60        THEN 'Post Home Move 31 - 60 Days'
                        WHEN UPPER(HOME_MOVE_STATUS) = 'POST HOME MOVE' AND DATEDIFF(dd, EFFECTIVE_FROM_DT, GETDATE()) BETWEEN 60 AND 90        THEN 'Post Home Move 61 - 90 Days'
                        WHEN UPPER(HOME_MOVE_STATUS) = 'NONE'  AND DATEDIFF(dd, EFFECTIVE_FROM_DT, GETDATE()) BETWEEN 0 AND 30         		THEN 'Post Home Move 91 - 120 Days'
                        WHEN UPPER(HOME_MOVE_STATUS) = 'NONE'  AND DATEDIFF(dd, EFFECTIVE_FROM_DT, GETDATE()) > 30	             		THEN 'None'
                        ELSE 'Unknown' END AS HOME_MOVE_STATUS
INTO ${CBAF_DB_DATA_SCHEMA}.TMP_MOVERS
FROM (SELECT *, RANK() OVER( PARTITION BY ACCOUNT_NUMBER ORDER BY EFFECTIVE_FROM_DT DESC , DW_LAST_MODIFIED_DT DESC ) AS RANKK
      FROM  ${CBAF_DB_LIVE_SCHEMA}.CUST_HOME_MOVE_STATUS_HIST ) AS B
WHERE RANKK = 1 AND EFFECTIVE_FROM_DT > DATEADD(DD, -120, GETDATE())
GO

CREATE HG INDEX ID1 ON ${CBAF_DB_DATA_SCHEMA}.TMP_MOVERS(ACCOUNT_NUMBER)
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
  SET SKY_GENERATED_HOME_MOVER = COALESCE (HOME_MOVE_STATUS, 'Unknown')
FROM  ${CBAF_DB_DATA_SCHEMA}.ADSMART AS A
JOIN ${CBAF_DB_DATA_SCHEMA}.TMP_MOVERS AS B ON A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
GO

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.TMP_MOVERS
GO
				

-- Drop 2 Fixed Attributes Ends



-- Adsmart Drop 3 Internal Attributes

/************************************************
 *                                  
 * Household_Composition_Men and Household_Composition_women  *
 *                                  
************************************************/
 
MESSAGE 'POPULATE FIELD FOR HH_Composition_Men and HH_Composition_women' TYPE STATUS TO CLIENT
GO
 
IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR='${CBAF_DB_DATA_SCHEMA}'
              AND UPPER(TNAME)='TEMP_AGE_HOUSE'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE TEMP_AGE_HOUSE ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE ${CBAF_DB_DATA_SCHEMA}.TEMP_AGE_HOUSE
    END

MESSAGE 'CREATE TABLE TEMP_AGE_HOUSE' TYPE STATUS TO CLIENT
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR='${CBAF_DB_DATA_SCHEMA}'
              AND UPPER(TNAME)='TEMP_HOUSE'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE TEMP_HOUSE ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE ${CBAF_DB_DATA_SCHEMA}.TEMP_HOUSE
    END

MESSAGE 'CREATE TABLE TEMP_HOUSE' TYPE STATUS TO CLIENT
GO
-- create who lives in the household data
--code_location_21

--drop table TEMP_HOUSE;
--commit;

SELECT cb_key_household
        ,MF =  (CASE WHEN p_gender = '0' then 1                 --male
                    WHEN p_gender = '1' then 100                --female
                    WHEN p_gender = 'U' then 1000               --unknow
                    ELSE 9000                                   --missing data
               END)
INTO ${CBAF_DB_DATA_SCHEMA}.TEMP_HOUSE
FROM ${CBAF_DB_LIVE_SCHEMA}.EXPERIAN_CONSUMERVIEW
GROUP BY cb_key_household, MF
ORDER BY cb_key_household
GO

--select top 10 * from TEMP_HOUSE;
--drop table TEMP_HOUSE_COUNT;
--commit;

select cb_key_household, sum(MF) AS House_binary
INTO ${CBAF_DB_DATA_SCHEMA}.TEMP_HOUSE_COUNT
FROM ${CBAF_DB_DATA_SCHEMA}.TEMP_HOUSE
GROUP BY cb_key_household
GO
--drop table TEMP_HOUSE;


/*
house_binary    UFM FLAG
1               M only household
100             F only household
101             FM household
1001            UM household
1100            UF household
1101            UFM household
1000            U household
*/


--select top 10 * from sk_prod.EXPERIAN_CONSUMERVIEW;
-- MALE
----age of 2nd male in household to replace Max

--drop table temp_AGEM_2;
--commit;

/*  */

SELECT cb_key_household
        ,(case when p_actual_age  >= 16 and p_actual_age < 25 then 0
                when p_actual_age >= 25 and p_actual_age < 35 then 1
                when p_actual_age >= 35 and p_actual_age < 45 then 2
                when p_actual_age >= 45 and p_actual_age < 55 then 3
                when p_actual_age >= 55 and p_actual_age < 65 then 4
                when p_actual_age >= 65                       then 5
                else null end) as person_age_band
      ,rank() over(PARTITION BY cb_key_household ORDER BY person_age_band) AS rank_id
--        ,MAX(cast(person_age AS integer )) AS MAX_AGE
        ,p_gender
        --person_age
INTO ${CBAF_DB_DATA_SCHEMA}.temp_AGEM
FROM ${CBAF_DB_LIVE_SCHEMA}.EXPERIAN_CONSUMERVIEW
WHERE p_gender = '0'
GROUP BY cb_key_household, person_age_band, p_gender
GO

SELECT cb_key_household
        ,(case when p_actual_age  >= 16 and p_actual_age < 25 then 0
                when p_actual_age >= 25 and p_actual_age < 35 then 1
                when p_actual_age >= 35 and p_actual_age < 45 then 2
                when p_actual_age >= 45 and p_actual_age < 55 then 3
                when p_actual_age >= 55 and p_actual_age < 65 then 4
                when p_actual_age >= 65                       then 5
                else null end) as person_age_band
      ,rank() over(PARTITION BY cb_key_household ORDER BY person_age_band DESC) AS rank_id
--        ,MAX(cast(person_age AS integer )) AS MAX_AGE
        ,p_gender
        --person_age
INTO ${CBAF_DB_DATA_SCHEMA}.temp_AGEM_2
FROM ${CBAF_DB_LIVE_SCHEMA}.EXPERIAN_CONSUMERVIEW
WHERE p_gender = '0'
GROUP BY cb_key_household, person_age_band, p_gender
GO

--select top 10 * from temp_AGEM;

--party 1

--drop table TempAge1M;
--commit;

SELECT cb_key_household, p_gender AS Male, person_age_band AS M1_AGE
INTO ${CBAF_DB_DATA_SCHEMA}.TempAge1M
FROM ${CBAF_DB_DATA_SCHEMA}.temp_ageM
WHERE rank_id = 1
GO

--party 2

--drop table TempAge2M;
--commit;

SELECT cb_key_household, p_gender AS Male, person_age_band AS M2_AGE
INTO ${CBAF_DB_DATA_SCHEMA}.TempAge2M
FROM ${CBAF_DB_DATA_SCHEMA}.temp_ageM_2
WHERE rank_id = 1
GO


--FEMALE
--add rank to data

--drop table temp_AGEF_2;
--commit;

SELECT cb_key_household
        ,(case when p_actual_age  >= 16 and p_actual_age < 25 then 0
                when p_actual_age >= 25 and p_actual_age < 35 then 1
                when p_actual_age >= 35 and p_actual_age < 45 then 2
                when p_actual_age >= 45 and p_actual_age < 55 then 3
                when p_actual_age >= 55 and p_actual_age < 65 then 4
                when p_actual_age >= 65                       then 5
                else null end) as person_age_band
        ,rank() over(PARTITION BY cb_key_household ORDER BY person_age_band) AS rank_id
--        ,MAX(cast(person_age AS integer )) AS MAX_AGE
        ,p_gender
        --person_age
INTO ${CBAF_DB_DATA_SCHEMA}.temp_AGEF
FROM ${CBAF_DB_LIVE_SCHEMA}.EXPERIAN_CONSUMERVIEW
WHERE p_gender = '1'
GROUP BY cb_key_household, person_age_band, p_gender
GO

SELECT cb_key_household
        ,(case when p_actual_age  >= 16 and p_actual_age < 25 then 0
                when p_actual_age >= 25 and p_actual_age < 35 then 1
                when p_actual_age >= 35 and p_actual_age < 45 then 2
                when p_actual_age >= 45 and p_actual_age < 55 then 3
                when p_actual_age >= 55 and p_actual_age < 65 then 4
                when p_actual_age >= 65                       then 5
                else null end) as person_age_band
        ,rank() over(PARTITION BY cb_key_household ORDER BY person_age_band DESC) AS rank_id
--        ,MAX(cast(person_age AS integer )) AS MAX_AGE
        ,p_gender
        --person_age
INTO ${CBAF_DB_DATA_SCHEMA}.temp_AGEF_2
FROM ${CBAF_DB_LIVE_SCHEMA}.EXPERIAN_CONSUMERVIEW
WHERE p_gender = '1'
GROUP BY cb_key_household, person_age_band, p_gender
GO
--party 1
--drop table TempAge1F;
--commit;

SELECT cb_key_household, p_gender AS Female, person_age_band AS F1_AGE
INTO ${CBAF_DB_DATA_SCHEMA}.TempAge1F
FROM ${CBAF_DB_DATA_SCHEMA}.temp_ageF
WHERE rank_id = 1
GO

--party 2
--drop table TempAge2F;
--commit;

SELECT cb_key_household, p_gender AS Female, person_age_band AS F2_AGE
INTO ${CBAF_DB_DATA_SCHEMA}.TempAge2F
FROM ${CBAF_DB_DATA_SCHEMA}.temp_ageF_2
WHERE rank_id = 1
GO


--- temp Age and household file

--drop table AGE_HOUSE;
--commit;


create table ${CBAF_DB_DATA_SCHEMA}.TEMP_AGE_HOUSE (
        cb_key_household                bigint NULL DEFAULT NULL
        ,House_binary                   integer NULL DEFAULT NULL
        ,male                           integer NULL DEFAULT NULL
        ,M1_Age                         integer NULL DEFAULT NULL
        ,M2_Age                         integer NULL DEFAULT NULL
        ,F1_Age                         integer NULL DEFAULT NULL
        ,F2_Age                         integer NULL DEFAULT NULL
        ,Female                         integer NULL DEFAULT NULL
        ,MIRROR_MEN_MIN                 varchar(30) NULL DEFAULT 'Unknown'
        ,MIRROR_WOMEN_MIN               varchar(30) NULL DEFAULT 'Unknown'
)
GO

Insert into ${CBAF_DB_DATA_SCHEMA}.TEMP_AGE_HOUSE (cb_key_household,House_binary,male,M1_Age)
SELECT A.cb_key_household,House_binary,male,M1_Age
FROM  ${CBAF_DB_DATA_SCHEMA}.TEMP_HOUSE_COUNT AS A,  ${CBAF_DB_DATA_SCHEMA}.TempAge1M AS B
WHERE A.cb_key_household *= B.cb_key_household --left join
GO

CREATE HG INDEX ID_ ON ${CBAF_DB_DATA_SCHEMA}.TEMP_AGE_HOUSE(cb_key_household)
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.TEMP_AGE_HOUSE
SET    M2_Age = aff.M2_Age
      FROM ${CBAF_DB_DATA_SCHEMA}.TEMP_AGE_HOUSE  AS Base
         INNER JOIN ${CBAF_DB_DATA_SCHEMA}.TempAge2M AS aff
         ON base.cb_key_household = aff.cb_key_household
GO


UPDATE ${CBAF_DB_DATA_SCHEMA}.TEMP_AGE_HOUSE
SET    F1_Age             = aff.F1_Age
     , female             = aff.female
      FROM ${CBAF_DB_DATA_SCHEMA}.TEMP_AGE_HOUSE  AS Base
         INNER JOIN ${CBAF_DB_DATA_SCHEMA}.TempAge1F AS aff
         ON base.cb_key_household = aff.cb_key_household
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.TEMP_AGE_HOUSE
SET    F2_Age             = aff.F2_Age
      FROM ${CBAF_DB_DATA_SCHEMA}.TEMP_AGE_HOUSE  AS Base
         INNER JOIN ${CBAF_DB_DATA_SCHEMA}.TempAge2F AS aff
         ON base.cb_key_household = aff.cb_key_household
GO

--define mirror segments

--select top 100 * from AGE_HOUSE;

update ${CBAF_DB_DATA_SCHEMA}.TEMP_AGE_HOUSE
   SET
        MIRROR_MEN_MIN  = CASE WHEN house_binary in (1,101,1001,1101) AND M1_Age in (0) AND (M2_Age is null OR M2_Age in (0)) THEN 'All Men in HH 16-24'
                               WHEN house_binary in (1,101,1001,1101) AND M1_Age in (0)                                       THEN 'Youngest male is 16-24'
                               WHEN house_binary in (1,101,1001,1101) AND M1_Age in (1)                                       THEN 'Youngest male is 25-34'
                               WHEN house_binary in (1,101,1001,1101) AND M1_Age in (2)                                       THEN 'Youngest male is 35-44'
                               WHEN house_binary in (1,101,1001,1101) AND M1_Age in (3)                                       THEN 'Youngest male is 45-54'
                               WHEN house_binary in (1,101,1001,1101) AND M1_Age in (4,5)                                     THEN 'Youngest male is 55+'
                               WHEN house_binary in (100)                                                                     THEN 'Women only HH'
                               WHEN house_binary in (1000,1001,1101)                                                          THEN 'Unknown'
 ELSE 'Unknown'
                               END

    , MIRROR_WOMEN_MIN  = CASE WHEN house_binary in (100,101,1101) AND F1_Age in (0) AND (F2_Age is null OR F2_Age in (0)) THEN 'All Women in HH 16-24'
                               WHEN house_binary in (100,101,1101) AND F1_Age in (0)                                       THEN 'Youngest female is 16-24'
                               WHEN house_binary in (100,101,1101) AND F1_Age in (1)                                       THEN 'Youngest female is 25-34'
                               WHEN house_binary in (100,101,1101) AND F1_Age in (2)                                       THEN 'Youngest female is 35-44'
                               WHEN house_binary in (100,101,1101) AND F1_Age in (3)                                       THEN 'Youngest female is 45-54'
                               WHEN house_binary in (100,101,1101) AND F1_Age in (4,5)                                     THEN 'Youngest female is 55+'
                               WHEN house_binary in (1)                                                                    THEN 'Men only HH'
                               WHEN house_binary in (1000,1100,1101)                                                       THEN 'Unknown'
ELSE 'Unknown'
                               END
GO							   
							   
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
  SET Household_Composition_Men   = coalesce(B.MIRROR_MEN_MIN, 'Unknown')
     ,Household_Composition_Women = coalesce(B.MIRROR_WOMEN_MIN, 'Unknown')
FROM  ${CBAF_DB_DATA_SCHEMA}.ADSMART AS A
JOIN ${CBAF_DB_DATA_SCHEMA}.TEMP_AGE_HOUSE AS B ON A.cb_key_household = B.cb_key_household
GO

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.TEMP_HOUSE
DROP TABLE ${CBAF_DB_DATA_SCHEMA}.TEMP_HOUSE_COUNT
DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_AGEM
DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_AGEM_2
DROP TABLE ${CBAF_DB_DATA_SCHEMA}.TempAge1M
DROP TABLE ${CBAF_DB_DATA_SCHEMA}.TempAge2M
DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_AGEF
DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_AGEF_2
DROP TABLE ${CBAF_DB_DATA_SCHEMA}.TempAge1F
DROP TABLE ${CBAF_DB_DATA_SCHEMA}.TempAge2F
DROP TABLE ${CBAF_DB_DATA_SCHEMA}.TEMP_AGE_HOUSE
GO

-----------------------------------------------------------------
----------------- Baby:	 	Expectant Mum------------------------
-----------------------------------------------------------------

MESSAGE 'POPULATE FIELD FOR EXPECTANT_MUM' TYPE STATUS TO CLIENT
GO

DECLARE @next_month DATE

SELECT @next_month = dateadd(dd, 7, today())

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET EXPECTANT_MUM = CASE
        WHEN due_date_1 > @next_month THEN 'Yes' --return the first day of the next month (included)
        ELSE 'Unknown'  END
FROM ${CBAF_DB_DATA_SCHEMA}.adsmart a
LEFT JOIN (SELECT account_number, MIN(due_date) AS due_date_1
			from  ${CBAF_DB_LIVE_SCHEMA}.EMMAS_DIARY  a
			join ${CBAF_DB_LIVE_SCHEMA}.cust_single_account_view b
			on a.cb_key_household = b.cb_key_household
			WHERE b.cb_key_household NOT IN (SELECT DISTINCT cb_key_household FROM ${CBAF_DB_LIVE_SCHEMA}.BABY_MPS) -- exclusion list
			AND due_date is not null 
			GROUP BY account_number
			) b
ON a.account_number = b.account_number
GO

------------------------------------------------------------------------------------
----------------- Baby: 	Age of Youngest Baby in Household-----------------------
------------------------------------------------------------------------------------
MESSAGE 'POPULATE FIELD FOR AGE_OF_YOUNGEST_BABY_IN_HOUSEHOLD' TYPE STATUS TO CLIENT
GO
 
IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR='${CBAF_DB_DATA_SCHEMA}'
              AND UPPER(TNAME)='temp_v317_Emmas_view'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE temp_v317_Emmas_view ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_v317_Emmas_view
    END

MESSAGE 'CREATE TABLE temp_v317_Emmas_view' TYPE STATUS TO CLIENT
GO

DECLARE @next_month DATE

SELECT @next_month = dateadd(dd, 7, today())

SELECT *, DATEDIFF(MONTH, date_of_birth, @next_month)-1 AS age_child
INTO ${CBAF_DB_DATA_SCHEMA}.temp_v317_Emmas_view
FROM (
    SELECT account_number
       ,MAX(CASE WHEN due_date < @next_month then due_date
            ELSE birth_day END) as date_of_birth
    FROM ${CBAF_DB_LIVE_SCHEMA}.EMMAS_DIARY  a
    JOIN ${CBAF_DB_LIVE_SCHEMA}.CUST_SINGLE_ACCOUNT_VIEW b on a.cb_key_household = b.cb_key_household
    WHERE a.cb_key_household NOT IN (SELECT DISTINCT cb_key_household FROM ${CBAF_DB_LIVE_SCHEMA}.BABY_MPS) -- exclusion list
    GROUP BY account_number
	    ) f
WHERE date_of_birth > DATEADD(YEAR, -4 , @next_month)

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART 
SET AGE_OF_YOUNGEST_BABY_IN_HOUSEHOLD = CASE WHEN age_child BETWEEN 0    AND 3   THEN '0-3 months'
                                            WHEN age_child BETWEEN 4    AND 6   THEN '4-6 months'
                                            WHEN age_child BETWEEN 7    AND 12  THEN '7-12 months'
                                            WHEN age_child BETWEEN 13   AND 18  THEN '13-18 months'
                                            WHEN age_child BETWEEN 19   AND 24  THEN '19-24 months'
                                            WHEN age_child > 24 THEN '25+ months' 	-- UPDATED 2015-02-02
                                            ELSE 'Unknown' end
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART a
LEFT JOIN ${CBAF_DB_DATA_SCHEMA}.temp_v317_Emmas_view b 
ON a.account_number = b.account_number
GO

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_v317_Emmas_view
GO

------------------- Baby: 	Pregnant and Number of Children in Household
MESSAGE 'POPULATE FIELD FOR PREGNANT_AND_NUMBER_OF_CHILDREN_IN_HOUSEHOLD' TYPE STATUS TO CLIENT
GO


DECLARE @next_month DATE

SELECT @next_month = dateadd(dd, 7, today())

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET PREGNANT_AND_NUMBER_OF_CHILDREN_IN_HOUSEHOLD = CASE WHEN cast(number_of_siblings as integer) = 0 then 'First Child'
                                     WHEN cast(number_of_siblings as integer) = 1 then '1 Child'
                                     WHEN cast(number_of_siblings as integer) = 2 then '2 Child'
                                     WHEN cast(number_of_siblings as integer) > 2 then '3+ Child'
                                     ELSE 'Unknown' end
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART a
LEFT JOIN (SELECT account_number,number_of_siblings, MIN(due_date) AS due_date_1 
			from  ${CBAF_DB_LIVE_SCHEMA}.EMMAS_DIARY   a
			join ${CBAF_DB_LIVE_SCHEMA}.cust_single_account_view b
			on a.cb_key_household = b.cb_key_household
			WHERE b.cb_key_household NOT IN (SELECT DISTINCT cb_key_household FROM ${CBAF_DB_LIVE_SCHEMA}.BABY_MPS) -- exclusion list
			GROUP BY account_number,number_of_siblings
			HAVING  due_date_1 > @next_month
			) b 
on a.account_number = b.account_number
GO

-----------------------------------------------------------------
----------------- AFFLUENCE_BAND ------------------------
-----------------------------------------------------------------

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET AFFLUENCE_BAND = 'Unknown'
WHERE AFFLUENCE_BAND is null

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET FIBRE_AVAILABLE  = 'No'
WHERE FIBRE_AVAILABLE is null

-----------------------------------------------------------------
----------------- HOMEMOVER ------------------------
-----------------------------------------------------------------



----------------------------------------------------------------
------------------- HOME_INSURANCE_RENEWAL_MONTH
-----------------------------------------------------------------


MESSAGE 'POPULATE FIELD FOR HOME_INSURANCE_RENEWAL_MONTH' TYPE STATUS TO CLIENT
GO


UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET HOME_INSURANCE_RENEWAL_MONTH = CASE WHEN home_policy_start_month = '1' THEN 'January'
                                        WHEN home_policy_start_month = '2'        THEN 'February'
                                        WHEN home_policy_start_month = '3'        THEN 'March'
                                        WHEN home_policy_start_month = '4'        THEN 'April'
                                        WHEN home_policy_start_month = '5'        THEN 'May'
                                        WHEN home_policy_start_month = '6'        THEN 'June'
                                        WHEN home_policy_start_month = '7'        THEN 'July'
                                        WHEN home_policy_start_month = '8'        THEN 'August'
                                        WHEN home_policy_start_month = '9'        THEN 'September'
                                        WHEN home_policy_start_month = '10'   THEN 'October'
                                        WHEN home_policy_start_month = '11'   THEN 'November'
                                        WHEN home_policy_start_month = '12'   THEN 'December'
                                        ELSE 'Unknown' END
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART a
LEFT JOIN ${CBAF_DB_LIVE_SCHEMA}.CALL_CREDIT_HOME_INS b ON a.cb_key_household = b.cb_key_household



-----------------------------------------------------------------
------------------- HOME_TYPE AND SOUTH_FACING_GARDEN
-----------------------------------------------------------------

MESSAGE 'POPULATE FIELD FOR HOME_TYPE AND SOUTH_FACING_GARDEN' TYPE STATUS TO CLIENT
GO


UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET HOUSE_TYPE  = CASE   WHEN home_type = 'Semi Detached House' THEN 'Semi Detached'
                            WHEN home_type = 'Detached House' THEN 'Detached'
                            WHEN home_type = 'Terraced House' THEN 'Terraced'
                            WHEN home_type = 'Other Floor Flat' THEN 'Maisonette or Flat'
                            ELSE 'Unknown' END
    , SOUTH_FACING_GARDEN  = CASE WHEN rear_orientation = 'Y' then 'Yes'
                                    ELSE 'Unknown' END
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART  a
LEFT JOIN ${CBAF_DB_LIVE_SCHEMA}.CALL_CREDIT_HOME b ON a.cb_key_household = b.cb_key_household


-----------------------------------------------------------------
------------------- CAR_INSURANCE_RENEWAL_MONTH
-----------------------------------------------------------------



IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR='${CBAF_DB_DATA_SCHEMA}'
              AND UPPER(TNAME)='calcredit_motor_table'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE calcredit_motor_table ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE ${CBAF_DB_DATA_SCHEMA}.calcredit_motor_table
    END

MESSAGE 'CREATE TABLE calcredit_motor_table' TYPE STATUS TO CLIENT
GO

DECLARE @curr_month DATE = today()

SELECT cb_key_household
        , cb_key_individual
        , CASE  WHEN Veh_Start_Date_MM = '1'  THEN 'January'
                WHEN Veh_Start_Date_MM = '2'  THEN 'February'
                WHEN Veh_Start_Date_MM = '3'  THEN 'March'
                WHEN Veh_Start_Date_MM = '4'  THEN 'April'
                WHEN Veh_Start_Date_MM = '5'  THEN 'May'
                WHEN Veh_Start_Date_MM = '6'  THEN 'June'
                WHEN Veh_Start_Date_MM = '7'  THEN 'July'
                WHEN Veh_Start_Date_MM = '8'  THEN 'August'
                WHEN Veh_Start_Date_MM = '9'  THEN 'September'
                WHEN Veh_Start_Date_MM = '10' THEN 'October'
                WHEN Veh_Start_Date_MM = '11' THEN 'November'
                WHEN Veh_Start_Date_MM = '12' THEN 'December'
                ELSE 'Unknown' END as renew_month
        , CASE WHEN cast(Veh_Start_Date_MM AS INTEGER) - month(@curr_month) < 0 THEN cast(Veh_Start_Date_MM AS INTEGER) - month(@curr_month) + 12
                        ELSE cast(Veh_Start_Date_MM AS INTEGER) - month(@curr_month) END AS date_diff
        , CAST(0 AS BIT ) AS head
 INTO ${CBAF_DB_DATA_SCHEMA}.calcredit_motor_table
 FROM ${CBAF_DB_LIVE_SCHEMA}.CALL_CREDIT_CAR_INS
 WHERE Veh_Start_Date_MM IS NOT NULL


COMMIT
CREATE HG INDEX ed1 ON ${CBAF_DB_DATA_SCHEMA}.calcredit_motor_table (cb_key_household)
CREATE HG INDEX ed2 ON ${CBAF_DB_DATA_SCHEMA}.calcredit_motor_table (cb_key_individual)
COMMIT


UPDATE ${CBAF_DB_DATA_SCHEMA}.calcredit_motor_table
SET head = 1
FROM ${CBAF_DB_DATA_SCHEMA}.calcredit_motor_table as a
JOIN ${CBAF_DB_LIVE_SCHEMA}.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD as b ON a.cb_key_individual = b.exp_cb_key_individual

SELECT *, ROW_NUMBER () over (PARTITION BY cb_key_household ORDER BY head DESC, date_diff, renew_month, cb_key_individual desc ) AS rank_no
INTO #t1
FROM ${CBAF_DB_DATA_SCHEMA}.calcredit_motor_table

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET CAR_INSURANCE_RENEWAL_MONTH  = COALESCE(renew_month,'Unknown')
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART a
LEFT JOIN #t1 b ON a.cb_key_household = b.cb_key_household AND rank_no = 1

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.calcredit_motor_table
COMMIT


-----------------------------------------------------------------
-------------------- TIME_SINCE_CAR_PURCHASE/MAKE_OF_CAR/VEHICLE_TYPE_IN_HOUSEHOLD
-----------------------------------------------------------------


MESSAGE 'CREATE TABLE calcredit_motors_table' TYPE STATUS TO CLIENT
GO

DROP TABLE IF EXISTS ${CBAF_DB_DATA_SCHEMA}.calcredit_motors_table

DECLARE @next_month DATE
SET @next_month = dateadd(mm, 1, dateadd(dd, - day(today()) + 1, today()))


SELECT cb_key_household
    ,cb_key_individual
    ,Veh_Purchase_Date
    ,DATEDIFF(YEAR, CONVERT(DATE, Veh_Purchase_Date), @next_month) - 1 AS time_since_car_purchase
    , CASE  WHEN LOWER(Veh_Make_Comb) LIKE 'ford'       THEN 'Ford'
            WHEN LOWER(Veh_Make_Comb) LIKE 'vauxhall'   THEN 'Vauxhall'
            WHEN LOWER(Veh_Make_Comb) LIKE 'peugeot'    THEN 'Peugeot'
            WHEN LOWER(Veh_Make_Comb) LIKE 'volkswagen' THEN 'Volkswagen'
            WHEN LOWER(Veh_Make_Comb) LIKE 'renault'    THEN 'Renault'
            WHEN LOWER(Veh_Make_Comb) LIKE 'citroen'    THEN 'Citroen'
            WHEN LOWER(Veh_Make_Comb) LIKE 'fiat'       THEN 'Fiat'
            WHEN LOWER(Veh_Make_Comb) LIKE 'toyota'     THEN 'Toyota'
            WHEN LOWER(Veh_Make_Comb) LIKE 'nissan'     THEN 'Nissan'
            WHEN LOWER(Veh_Make_Comb) LIKE 'audi'       THEN 'Audi'
            WHEN LOWER(Veh_Make_Comb) LIKE 'honda'      THEN 'Honda'
            WHEN LOWER(Veh_Make_Comb) LIKE 'rover'      THEN 'Rover'
            WHEN LOWER(Veh_Make_Comb) LIKE 'mazda'      THEN 'Mazda'
            WHEN LOWER(Veh_Make_Comb) LIKE 'seat'       THEN 'Seat'
            WHEN Veh_Make_Comb IS NULL                  THEN 'Unknown'
            ELSE 'Other Manufacturers' END AS make_of_car
    ,CASE   WHEN cast(number_of_cars as integer) = 1 THEN 'One'
            WHEN cast(number_of_cars as integer) = 2 THEN 'Two'
            WHEN cast(number_of_cars as integer) > 2 THEN 'Three+'
            ELSE 'Unknown' END as number_cars_in_hh
    ,CASE   WHEN CAST(LEFT(Veh_Purchase_Date, 4) AS INTEGER) - CAST(Veh_Year_Of_Reg AS INTEGER) = 0                THEN 'Less than 1 year'
            WHEN CAST(LEFT(Veh_Purchase_Date, 4) AS INTEGER) - CAST(Veh_Year_Of_Reg AS INTEGER)    BETWEEN 1 AND 3 THEN '1-3 years'
            WHEN CAST(LEFT(Veh_Purchase_Date, 4) AS INTEGER) - CAST(Veh_Year_Of_Reg AS INTEGER)    BETWEEN 4 AND 6 THEN '4-6 years'
            WHEN CAST(LEFT(Veh_Purchase_Date, 4) AS INTEGER) - CAST(Veh_Year_Of_Reg AS INTEGER)    BETWEEN 7 AND 9 THEN '7-9 years'
            WHEN CAST(LEFT(Veh_Purchase_Date, 4) AS INTEGER) - CAST(Veh_Year_Of_Reg AS INTEGER)    BETWEEN 10 AND 12 THEN '10-12 years'
            WHEN CAST(LEFT(Veh_Purchase_Date, 4) AS INTEGER) - CAST(Veh_Year_Of_Reg AS INTEGER)> 12                 THEN '12 years+'
            ELSE 'Unknown' END as age_of_car
    , CASE  WHEN Veh_Classification_Comb = 'A' THEN 'Mini'
            WHEN Veh_Classification_Comb =  'B' THEN 'Super Mini'
            WHEN Veh_Classification_Comb = 'C' THEN 'Medium'
            WHEN Veh_Classification_Comb =  'D' THEN 'Medium'
            WHEN Veh_Classification_Comb =  'E' THEN 'Executive'
            WHEN Veh_Classification_Comb =  'F' THEN 'Luxury Saloon'
            WHEN Veh_Classification_Comb = 'G' THEN 'Specialist Sports'
            WHEN Veh_Classification_Comb =  'H' THEN 'Car Unclassified'
            WHEN Veh_Classification_Comb = 'I' THEN 'MPVS'
            WHEN Veh_Classification_Comb =  'J' THEN 'Caravan'
            WHEN Veh_Classification_Comb =  'M' THEN 'Motor Bike'
            WHEN Veh_Classification_Comb =  'Q' THEN 'Unknown'
            WHEN Veh_Classification_Comb = 'U' THEN 'Unknown'
            WHEN Veh_Classification_Comb = 'V' THEN 'Van, Lorry or Minivan'
            WHEN Veh_Classification_Comb = 'X' THEN 'Motor Home'
            WHEN Veh_Classification_Comb = 'Z' THEN 'Unknown'
            ELSE 'Unknown' END AS Veh_Class
    , Veh_Year_Of_Reg
    , CAST(0 AS BIT ) AS head
INTO ${CBAF_DB_DATA_SCHEMA}.calcredit_motors_table
FROM ${CBAF_DB_LIVE_SCHEMA}.CALL_CREDIT_CAR         ----###CALLCREDIT_CARDETAILS###

COMMIT
CREATE HG INDEX ed1 ON ${CBAF_DB_DATA_SCHEMA}.calcredit_motors_table (cb_key_household)
CREATE HG INDEX ed2 ON ${CBAF_DB_DATA_SCHEMA}.calcredit_motors_table (cb_key_individual)
COMMIT

UPDATE ${CBAF_DB_DATA_SCHEMA}.calcredit_motors_table
SET head = 1
FROM ${CBAF_DB_DATA_SCHEMA}.calcredit_motors_table as a
JOIN ${CBAF_DB_LIVE_SCHEMA}.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD as b ON a.cb_key_individual = b.exp_cb_key_individual

SELECT *, ROW_NUMBER () over (PARTITION BY cb_key_household ORDER BY head DESC, Veh_Purchase_Date, Veh_Year_Of_Reg, cb_key_individual desc ) AS rank_
INTO ${CBAF_DB_DATA_SCHEMA}.calcredit_motors_table_t1
FROM ${CBAF_DB_DATA_SCHEMA}.calcredit_motors_table

COMMIT
CREATE HG INDEX ed1 ON ${CBAF_DB_DATA_SCHEMA}.calcredit_motors_table_t1 (cb_key_household)
CREATE HG INDEX ed2 ON ${CBAF_DB_DATA_SCHEMA}.calcredit_motors_table_t1 (cb_key_individual)
COMMIT


UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
             SET  MAKE_OF_CAR = COALESCE(b.make_of_car, 'Unknown')
                , NUMBER_OF_CARS_IN_HOUSEHOLD = COALESCE(b.number_cars_in_hh, 'Unknown')
                , TIME_SINCE_CAR_PURCHASE = COALESCE(b.age_of_car, 'Unknown')
                , VEHICLE_TYPE_IN_HOUSEHOLD = COALESCE(b.Veh_Class, 'Unknown')

FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART a
LEFT JOIN ${CBAF_DB_DATA_SCHEMA}.calcredit_motors_table_t1 b ON a.cb_key_household = b.cb_key_household AND  rank_ = 1

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
              SET TIME_SINCE_CAR_PURCHASE  = CASE WHEN b.time_since_car_purchase = 0    THEN 'Less than 1 year'
                                              WHEN b.time_since_car_purchase BETWEEN 1 AND 3    THEN '1-3 years'
                                              WHEN b.time_since_car_purchase BETWEEN 4 AND 6    THEN '4-6 years'
                                              WHEN b.time_since_car_purchase BETWEEN 7 AND 9    THEN '7-9 years'
                                              WHEN b.time_since_car_purchase BETWEEN 10 AND 12  THEN '10-12 years'
                                              WHEN b.time_since_car_purchase > 12   THEN '12 years+'
                                              ELSE 'Unknown'  END

FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART a
LEFT JOIN ${CBAF_DB_DATA_SCHEMA}.calcredit_motors_table_t1 b ON a.cb_key_household = b.cb_key_household AND  rank_ = 1

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.calcredit_motors_table
COMMIT

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.calcredit_motors_table_t1
COMMIT



-----------------------------------------------------------------
------------------ MOBILE_PHONE_NETWORK
----------------------------------------------------------------

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR='${CBAF_DB_DATA_SCHEMA}'
              AND UPPER(TNAME)='Calcredit_mobile_table'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE Calcredit_mobile_table ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE ${CBAF_DB_DATA_SCHEMA}.Calcredit_mobile_table
    END

MESSAGE 'CREATE TABLE Calcredit_mobile_table' TYPE STATUS TO CLIENT
GO

SELECT cb_key_household

          , CASE WHEN count_ > 1 THEN 'Multiple Networks in HH'
            ELSE provider_ END AS MOBILE_PHONE_NETWORK
INTO ${CBAF_DB_DATA_SCHEMA}.Calcredit_mobile_table
        FROM (
            SELECT cb_key_household
                , CASE WHEN Mobile_Network_Provider = 'T-MOBILE' THEN 'EE'
                    WHEN Mobile_Network_Provider = 'ORANGE' THEN 'EE'
                    WHEN Mobile_Network_Provider = 'EE' THEN 'EE'
                    WHEN Mobile_Network_Provider = 'O2' THEN 'O2'
                    WHEN Mobile_Network_Provider = '3 NETWORK' THEN 'Three Network'
                    WHEN Mobile_Network_Provider = 'VODAFONE' THEN 'Vodafone'
                    ELSE 'Unknown'
--                    ELSE 'Other Network'
                    END as provider_
                , COUNT(*) over (PARTITION BY cb_key_household) AS count_
                FROM ${CBAF_DB_LIVE_SCHEMA}.CALL_CREDIT_MOBILE b
                GROUP BY cb_key_household, provider_
            ) f
GROUP BY cb_key_household, MOBILE_PHONE_NETWORK

MESSAGE 'Updating ADSMART for MOBILE_PHONE_NETWORK' TYPE STATUS TO CLIENT
GO



UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
-- SET MOBILE_PHONE_NETWORK_HE = COALESCE(MOBILE_PHONE_NETWORK_HE, 'Unknown') -- **
SET a.MOBILE_PHONE_NETWORK = COALESCE(b.MOBILE_PHONE_NETWORK, 'Unknown')
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART a
LEFT JOIN ${CBAF_DB_DATA_SCHEMA}.Calcredit_mobile_table b ON a.cb_key_household = b.cb_key_household

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.Calcredit_mobile_table
COMMIT


-------------------------------------------------------------------------------------------------------
-- STB_PRE_REGISTRATION                 
-------------------------------------------------------------------------------------------------------
MESSAGE 'POPULATE FIELD FOR STB_PRE_REGISTRATION' TYPE STATUS TO CLIENT
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR='${CBAF_DB_DATA_SCHEMA}'
              AND lower(TNAME)='temp_quarterly_release_1_stb_pre_registration'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE temp_quarterly_release_1_stb_pre_registration ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_quarterly_release_1_stb_pre_registration
    END

MESSAGE 'CREATE TABLE temp_quarterly_release_1_stb_pre_registration' TYPE STATUS TO CLIENT
GO

SELECT account_number
		, CASE WHEN registration_status = 'Registered' THEN 'Yes'
				WHEN registration_status = 'Validated' THEN 'Yes'
				WHEN registration_status = 'De-Registered' THEN 'No' END as STB_PRE_REGISTRATION_1
INTO ${CBAF_DB_DATA_SCHEMA}.temp_quarterly_release_1_stb_pre_registration
FROM ${CBAF_DB_LIVE_SCHEMA}.ETHAN_REGISTRATIONS_UK_CUSTOMERS
GROUP BY account_number
		, STB_PRE_REGISTRATION_1
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.adsmart
SET a.STB_PRE_REGISTRATION = COALESCE(b.STB_PRE_REGISTRATION_1, 'Unknown')
FROM ${CBAF_DB_DATA_SCHEMA}.adsmart a
LEFT JOIN ${CBAF_DB_DATA_SCHEMA}.temp_quarterly_release_1_stb_pre_registration b ON a.account_number = b.account_number
GO

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_quarterly_release_1_stb_pre_registration
GO


-----------------------------------------
----- MOBILE_DEVICE_OS
-----------------------------------------
MESSAGE 'POPULATE FIELD FOR MOBILE_DEVICE_OS' TYPE STATUS TO CLIENT
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR='${CBAF_DB_DATA_SCHEMA}'
              AND lower(TNAME)='temp_skygo_mobile_os'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE temp_skygo_mobile_os ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_skygo_mobile_os
    END

MESSAGE 'CREATE TABLE temp_skygo_mobile_os' TYPE STATUS TO CLIENT
GO

SELECT account_number
    , CASE WHEN count_inner > 1 THEN 'Both'
           ELSE MOBILE_DEVICE_OS 
	  END AS MOBILE_DEVICE_OS
INTO ${CBAF_DB_DATA_SCHEMA}.temp_skygo_mobile_os
        FROM (
            SELECT account_number
                , CASE WHEN site_name = 'GOIO' OR site_name = 'SMIO' then 'IOS'
                       WHEN site_name = 'GOAN' OR site_name = 'SMAN' then 'Android'
                  END as MOBILE_DEVICE_OS
                , COUNT(*) over (PARTITION BY account_number) AS count_inner
            FROM ${CBAF_DB_LIVE_SCHEMA}.SKY_PLAYER_USAGE_DETAIL
            WHERE site_name in ('GOIO', 'GOAN', 'SMIO', 'SMAN')
            GROUP BY account_number, MOBILE_DEVICE_OS
            ) f
GROUP BY account_number, MOBILE_DEVICE_OS
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.adsmart
SET a.MOBILE_DEVICE_OS = COALESCE(b.MOBILE_DEVICE_OS, 'Unknown')
FROM ${CBAF_DB_DATA_SCHEMA}.adsmart a
LEFT JOIN ${CBAF_DB_DATA_SCHEMA}.temp_skygo_mobile_os b 
ON a.account_number = b.account_number
GO

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_skygo_mobile_os
GO

------------------------------------------
-- ASIA PACK
------------------------------------------
MESSAGE 'POPULATE FIELD FOR ASIA_PACK' TYPE STATUS TO CLIENT
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR='${CBAF_DB_DATA_SCHEMA}'
              AND lower(TNAME)='temp_asia_pack'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE temp_asia_pack ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_asia_pack
    END
	
MESSAGE 'CREATE TABLE temp_asia_pack' TYPE STATUS TO CLIENT
GO

SELECT account_number
      ,cast('Has Asia Pack'  as varchar(41)) AS Asia                         -- 2015-04-30 length was initialized at 13
INTO ${CBAF_DB_DATA_SCHEMA}.temp_asia_pack
FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_SUBS_HIST 
WHERE subscription_type = 'ENHANCED' 
        and subscription_sub_type = 'SKYASIA'
   and status_code in ('AC','AB','PC')                          --Active Status Codes
   and effective_from_dt <= getdate()                       
   and effective_to_dt > getdate()
   and effective_from_dt<>effective_to_dt
GO

-- Create Index
CREATE HG INDEX cwd  ON ${CBAF_DB_DATA_SCHEMA}.temp_asia_pack(account_number)

INSERT INTO ${CBAF_DB_DATA_SCHEMA}.temp_asia_pack
SELECT DISTINCT account_number
              , 'Previously had Asia Pack' AS Asia
FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_SUBS_HIST 
WHERE subscription_type = 'ENHANCED' 
        and subscription_sub_type = 'SKYASIA'
        AND status_code NOT in ('AC','AB','PC','PA')                            -- **WHY NOT ACTIVE? is it referred to the sub_type or the account?
        AND account_number NOT IN (SELECT account_number FROM ${CBAF_DB_DATA_SCHEMA}.temp_asia_pack)
GO

INSERT INTO ${CBAF_DB_DATA_SCHEMA}.temp_asia_pack
SELECT DISTINCT account_number
              , 'Has never had Asia Pack' AS Asia
FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_SUBS_HIST 
WHERE subscription_type = 'ENHANCED' 
        and subscription_sub_type = 'SKYASIA'
        AND status_code in ('PA') 
        AND account_number NOT IN (SELECT account_number FROM ${CBAF_DB_DATA_SCHEMA}.temp_asia_pack)
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.adsmart
SET ASIA_PACK = COALESCE (CASE WHEN Asia IS NOT NULL  THEN Asia 
                                ELSE 'Has never had Asia Pack'
                          END, 'Unknown')
FROM ${CBAF_DB_DATA_SCHEMA}.adsmart as a 
LEFT JOIN ${CBAF_DB_DATA_SCHEMA}.temp_asia_pack as b ON a.account_number = b.account_number 
GO

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_asia_pack
GO

------------------------------------------------------------------------------------------------------------
--------------------------------------------- BROADBAND_IP -------------------------------------------------
------------------------------------------------------------------------------------------------------------

MESSAGE 'POPULATE FIELD FOR BROADBAND_IP' TYPE STATUS TO CLIENT
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR='${CBAF_DB_DATA_SCHEMA}'
              AND lower(TNAME)='temp_broadband_ip_1'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE temp_broadband_ip_1 ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_broadband_ip_1
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
    INTO ${CBAF_DB_DATA_SCHEMA}.temp_broadband_ip_1
FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_ANYTIME_PLUS_DOWNLOADS a
JOIN ${CBAF_DB_DATA_SCHEMA}.adsmart b ON a.account_number = b.account_number
WHERE network_code IS NOT NULL
GROUP BY a.account_number, network_code_1
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR='${CBAF_DB_DATA_SCHEMA}'
              AND lower(TNAME)='temp_broadband_ip_2'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE temp_broadband_ip_2 ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_broadband_ip_2
    END

MESSAGE 'CREATE TABLE temp_broadband_ip_2' TYPE STATUS TO CLIENT
GO

SELECT account_number
     , network_code_1
     , latest_date
     , rank() OVER (PARTITION BY account_number ORDER BY latest_date ) AS rankk
INTO ${CBAF_DB_DATA_SCHEMA}.temp_broadband_ip_2 
FROM ${CBAF_DB_DATA_SCHEMA}.temp_broadband_ip_1
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.adsmart
SET BROADBAND_IP = coalesce(network_code_1, 'No IP Data')              -- Name changed to match Excel definition  Update 2015-02-06
FROM ${CBAF_DB_DATA_SCHEMA}.adsmart a
LEFT JOIN (SELECT * FROM ${CBAF_DB_DATA_SCHEMA}.temp_broadband_ip_2  WHERE rankk = 1) b ON a.account_number = b.account_number
GO

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_broadband_ip_1
DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_broadband_ip_2
GO

------------------------------------------------------------------------------------------------------------
--------------------------------------------- BARB_TV_REGIONS -------------------------------------------------
------------------------------------------------------------------------------------------------------------

MESSAGE 'POPULATE FIELD FOR BARB_TV_REGIONS' TYPE STATUS TO CLIENT
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR='${CBAF_DB_DATA_SCHEMA}'
              AND lower(TNAME)='temp_barb_regions'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE temp_barb_regions ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_barb_regions
    END

MESSAGE 'CREATE TABLE temp_barb_regions' TYPE STATUS TO CLIENT
GO

SELECT 
		  a.cb_address_postcode_area
		, a.account_number
		, b.barb_desc_itv
INTO ${CBAF_DB_DATA_SCHEMA}.temp_barb_regions
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART AS a 
LEFT JOIN ${CBAF_DB_LIVE_SCHEMA}.BARB_TV_REGIONS AS b 
ON TRIM(a.cb_address_postcode) = TRIM(b.cb_address_postcode)
WHERE account_number IS NOT NULL
GO

-- Create Index
CREATE HG INDEX hg1 ON ${CBAF_DB_DATA_SCHEMA}.temp_barb_regions(account_number)
CREATE LF INDEX hg2 ON ${CBAF_DB_DATA_SCHEMA}.temp_barb_regions(cb_address_postcode_area)
CREATE LF INDEX hg3 ON ${CBAF_DB_DATA_SCHEMA}.temp_barb_regions(barb_desc_itv)
GO


UPDATE ${CBAF_DB_DATA_SCHEMA}.adsmart
SET BARB_TV_REGIONS = CASE 	WHEN b.cb_address_postcode_area IN ('JE','GY') THEN 'Channel Islands'
							WHEN barb_desc_itv LIKE 'Meridian (exc. Channel Islands)' THEN 'Meridian'
							WHEN barb_desc_itv IN ('Central Scotland', 'North Scotland')  THEN 'Scotland'
							ELSE COALESCE (barb_desc_itv, 'Unknown') END 
FROM ${CBAF_DB_DATA_SCHEMA}.adsmart AS a 							
LEFT JOIN ${CBAF_DB_DATA_SCHEMA}.temp_barb_regions AS b 
ON a.account_number = b.account_number 
GO

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_barb_regions
GO


/***************************************************************************************
 *                                                                                      *
 *                          BUY_AND_KEEP_USAGE_OVER_LAST_12_MONTHS   					*
 *                                                                                      *
 ***************************************************************************************/

SELECT account_number
	, COUNT (*) hits  
INTO #uk_buy_keep_accounts
FROM ${CBAF_DB_LIVE_SCHEMA}.SKY_STORE_TRANSACTIONS
WHERE   product = 'EST'
	AND digital_state = 'COMPLETED' 
	AND order_date >= DATEADD (MONTH, -12, getdate())
GROUP BY account_number 
GO

CREATE HG INDEX id1 ON #uk_buy_keep_accounts (account_number)
CREATE LF INDEX id2 ON #uk_buy_keep_accounts (hits)
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET BUY_AND_KEEP_USAGE_OVER_LAST_12_MONTHS = COALESCE(CASE 	WHEN hits = 1 				THEN 'Yes, 1 in last 12 mths'
															WHEN hits BETWEEN 2 AND 4 	THEN 'Yes, 2-4 in last 12 mths'
															WHEN hits BETWEEN 5 AND 7 	THEN 'Yes, 5-7 in last 12 mths'
															WHEN hits  > 7 				THEN 'Yes, 7+ in last 12 mths'
															ELSE 'Never Bought' END 
															, 'Unknown')
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART AS a 
LEFT JOIN #uk_buy_keep_accounts AS b ON a.account_number = b.account_number 
GO

DROP TABLE #uk_buy_keep_accounts
GO



--------------------------------------------
--------- 1- Movies On Demand
--------------------------------------------
MESSAGE 'Update field movies_on_demand in ADSMART Table' type status to client
GO
 
IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR='${CBAF_DB_DATA_SCHEMA}'
              AND lower(TNAME)='temp_adsmart_q2_on_demand_raw'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE TEMP_ADSMART_Q2_ON_DEMAND_RAW ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE ${CBAF_DB_DATA_SCHEMA}.TEMP_ADSMART_Q2_ON_DEMAND_RAW
    END

MESSAGE 'CREATE TABLE TEMP_ADSMART_Q2_ON_DEMAND_RAW' TYPE STATUS TO CLIENT
GO 

SELECT cala.account_number
        ,MAX(last_modified_dt) last_dt
    INTO ${CBAF_DB_DATA_SCHEMA}.TEMP_ADSMART_Q2_ON_DEMAND_RAW
    FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_ANYTIME_PLUS_DOWNLOADS cala
         INNER JOIN ${CBAF_DB_DATA_SCHEMA}.adsmart AS sav ON cala.account_number = sav.account_number
                                       AND last_modified_dt <= now()
   WHERE UPPER(genre_desc) LIKE UPPER('%MOVIE%')
     AND provider_brand IN ('Sky Disney','Sky Disney HD','Sky Movies','Sky Movies HD')
GROUP BY cala.account_number
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.adsmart SET MOVIES_ON_DEMAND = 'Never'
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.adsmart as bas
     SET MOVIES_ON_DEMAND = CASE WHEN DATEDIFF (day, last_dt, getDATE())  <= 91                   THEN 'Downloaded movies 0-3 months'
                                 WHEN DATEDIFF (day, last_dt, getDATE())  BETWEEN 92 AND 182      THEN 'Downloaded movies 4-6 months'
                                 WHEN DATEDIFF (day, last_dt, getDATE())  >= 183                  THEN 'Downloaded movies 7+ months'
                                 ELSE 'Never'
                             END
    FROM ${CBAF_DB_DATA_SCHEMA}.TEMP_ADSMART_Q2_ON_DEMAND_RAW AS sub
    WHERE bas.account_number = sub.account_number
GO

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.TEMP_ADSMART_Q2_ON_DEMAND_RAW
GO

--------------------------------------------
--------- HOUSEHOLD_CAMPAIGN_DEMAND
--------------------------------------------
MESSAGE 'Update field HOUSEHOLD_CAMPAIGN_DEMAND in ADSMART Table' type status to client
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR='${CBAF_DB_DATA_SCHEMA}'
              AND lower(TNAME)='temp_household_campaign_demand'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE TEMP_HOUSEHOLD_CAMPAIGN_DEMAND ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_household_campaign_demand
    END

MESSAGE 'CREATE TABLE TEMP_HOUSEHOLD_CAMPAIGN_DEMAND' TYPE STATUS TO CLIENT
GO

SELECT account_number, max (hh_band) AS hh_bands
INTO ${CBAF_DB_DATA_SCHEMA}.temp_household_campaign_demand
FROM ${CBAF_DB_LIVE_SCHEMA}.HOUSEHOLD_CAMPAIGN_DEMAND
GROUP BY account_number 
GO 

-- Create Index
CREATE HG INDEX h1 ON ${CBAF_DB_DATA_SCHEMA}.temp_household_campaign_demand (account_number)
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.adsmart
SET household_campaign_demand = COALESCE(b.hh_bands, 'Percent 0-9')
FROM ${CBAF_DB_DATA_SCHEMA}.adsmart as a 
LEFT JOIN ${CBAF_DB_DATA_SCHEMA}.temp_household_campaign_demand AS b ON a.account_number = b.account_number 
GO

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_household_campaign_demand
GO

--------------------------------------------
--------- TECI
--------------------------------------------
UPDATE ${CBAF_DB_DATA_SCHEMA}.adsmart
SET technology_engagement_customer_index = COALESCE(cluster_name, 'Unknown')
FROM ${CBAF_DB_DATA_SCHEMA}.adsmart AS a
LEFT JOIN ${CBAF_DB_LIVE_SCHEMA}.TECI_current_score AS b ON a.account_number = b.account_number
GO

--------------------------------------------
--------- Local authority
--------------------------------------------

UPDATE ${CBAF_DB_DATA_SCHEMA}.adsmart
SET  local_authority = COALESCE(b.government_boundary, 'Unknown') 
FROM ${CBAF_DB_DATA_SCHEMA}.adsmart AS a 
LEFT JOIN ${CBAF_DB_LIVE_SCHEMA}.UK_LOCAL_AUTHORITY_AREAS AS b ON REPLACE(TRIM(a.cb_address_postcode),' ','') = REPLACE(TRIM(b.postcode),' ', '')
GO


/************************************
 *                                  *
 *         viewing_propensity		*
 *                                  *
 ************************************/


UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART 
SET  a.household_viewing_propensity  = CASE 	WHEN VIEWING_BASED_ATTRIBUTES LIKE '01' THEN 'Percent 90-99'
									WHEN VIEWING_BASED_ATTRIBUTES LIKE '02' THEN 'Percent 80-89'
									WHEN VIEWING_BASED_ATTRIBUTES LIKE '03' THEN 'Percent 70-79'
									WHEN VIEWING_BASED_ATTRIBUTES LIKE '04' THEN 'Percent 60-69'
									WHEN VIEWING_BASED_ATTRIBUTES LIKE '05' THEN 'Percent 50-59'
									WHEN VIEWING_BASED_ATTRIBUTES LIKE '06' THEN 'Percent 40-49'
									WHEN VIEWING_BASED_ATTRIBUTES LIKE '07' THEN 'Percent 30-39'
									WHEN VIEWING_BASED_ATTRIBUTES LIKE '08' THEN 'Percent 20-29'
									WHEN VIEWING_BASED_ATTRIBUTES LIKE '09' THEN 'Percent 10-19'
									WHEN VIEWING_BASED_ATTRIBUTES LIKE '10' THEN 'Percent 0-9'
									ELSE 'Unknown'
									END
FROM   ${CBAF_DB_DATA_SCHEMA}.ADSMART AS a 
LEFT JOIN  ${CBAF_DB_LIVE_SCHEMA}.VIEWING_ATTRIBUTES AS b on a.ACCOUNT_NUMBER = b.ACCOUNT_NUMBER
GO


/************************************
 *                                  *
 *        Engagement Matrix Score   *
 *                                  *
 ************************************/
		
DECLARE @m VARCHAR (6)
SELECT @m = MAX(observation_month)
FROM ${CBAF_DB_LIVE_SCHEMA}.M004_ENGAGEMENT_SCORE_H

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET ENGAGEMENT_MATRIX_SCORE = CASE WHEN UPPER (engagement_segment) LIKE '%HIGH%' THEN 'High'
                                	WHEN UPPER (engagement_segment) LIKE '%MED%'  THEN 'Medium'
									WHEN UPPER (engagement_segment) LIKE '%LOW%'  THEN 'Low'
									ELSE 'Unknown' END 
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART AS a 
JOIN ${CBAF_DB_LIVE_SCHEMA}.M004_ENGAGEMENT_SCORE_H AS b ON a.account_number = b.account_number AND observation_month = @m

COMMIT 					  
GO

/************************************
 *                                  *
 *         YOUNGEST_ADULT_HOUSEHOLD *
 *                                  *
 ************************************/

MESSAGE 'Populate field age_group - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_DATA_SCHEMA}'
              AND UPPER(tname)='TEMP_AGE_GROUP'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_AGE_GROUP already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_DATA_SCHEMA}.TEMP_AGE_GROUP
  END

MESSAGE 'CREATE TABLE TEMP_AGE_GROUP' type status to client
GO

SELECT  CON.cb_key_household
       ,CON.cb_key_individual
       ,CON.p_actual_age
INTO ${CBAF_DB_DATA_SCHEMA}.TEMP_AGE_GROUP
FROM
(select cb_key_individual, max(CB_ROW_ID) AS MAX_ROW_ID
from ${CBAF_DB_LIVE_SCHEMA}.experian_consumerview
GROUP BY cb_key_individual) AS DUPE
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.experian_consumerview AS CON
ON DUPE.cb_key_individual = CON.cb_key_individual and DUPE.MAX_ROW_ID = CON.cb_row_id
GO

-- Create Index
CREATE HG INDEX ix_cbkeyhh ON ${CBAF_DB_DATA_SCHEMA}.TEMP_AGE_GROUP (cb_key_household)
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_DATA_SCHEMA}'
              AND UPPER(tname)='TEMP_AGE_GROUP_MAX'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_AGE_GROUP_MAX already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_DATA_SCHEMA}.TEMP_AGE_GROUP_MAX
  END

MESSAGE 'CREATE TABLE TEMP_AGE_GROUP_MAX' type status to client
GO

SELECT  cb_key_household
       ,MAX(CASE WHEN p_actual_age >= 16 AND p_actual_age < 25 THEN 1 ELSE 0 END) AS HH_Has_Age_16to24
       ,MAX(CASE WHEN p_actual_age >= 25 AND p_actual_age < 35 THEN 1 ELSE 0 END) AS HH_Has_Age_25to34
       ,MAX(CASE WHEN p_actual_age >= 35 AND p_actual_age < 45 THEN 1 ELSE 0 END) AS HH_Has_Age_35to44
       ,MAX(CASE WHEN p_actual_age >= 45 AND p_actual_age < 55 THEN 1 ELSE 0 END) AS HH_Has_Age_45to54
       ,MAX(CASE WHEN p_actual_age >= 55                       THEN 1 ELSE 0 END) AS HH_Has_Age_Over_55
      
INTO ${CBAF_DB_DATA_SCHEMA}.TEMP_AGE_GROUP_MAX
FROM ${CBAF_DB_DATA_SCHEMA}.TEMP_AGE_GROUP
GROUP BY cb_key_household
GO

-- Create Index
CREATE HG INDEX ix_cbkeyhh2 ON ${CBAF_DB_DATA_SCHEMA}.TEMP_AGE_GROUP_MAX (cb_key_household)
GO


-- Update ADSMART Table
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET YOUNGEST_ADULT_HOUSEHOLD = CASE 	
						WHEN HH_Has_Age_16to24 = 1          THEN 'Youngest adult is 16-24'
						WHEN HH_Has_Age_25to34 = 1 	    THEN 'Youngest adult is 25-34'
						WHEN HH_Has_Age_35to44 = 1 	    THEN 'Youngest Adult is 35-44'
						WHEN HH_Has_Age_45to54 = 1 	    THEN 'Youngest Adult is 45-54'
						WHEN HH_Has_Age_Over_55 = 1 	    THEN 'Youngest Adult is 55+'
                     ELSE 'Unknown'
                END
FROM  ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD
INNER JOIN ${CBAF_DB_DATA_SCHEMA}.TEMP_AGE_GROUP_MAX AS TAG
ON AD.cb_key_household = TAG.cb_key_household
GO

-- Update ADSMART Table
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET YOUNGEST_ADULT_HOUSEHOLD = CASE 	
			        	WHEN CL_CURRENT_AGE BETWEEN 16 AND 24 	THEN 'Youngest adult is 16-24'
					WHEN CL_CURRENT_AGE BETWEEN 24 AND 34 	THEN 'Youngest adult is 25-34'
					WHEN CL_CURRENT_AGE BETWEEN 34 AND 44 	THEN 'Youngest Adult is 35-44'
					WHEN CL_CURRENT_AGE BETWEEN 45 AND 54 	THEN 'Youngest Adult is 45-54'
					WHEN CL_CURRENT_AGE >= 55	 	THEN 'Youngest Adult is 55+'
                                        ELSE YOUNGEST_ADULT_HOUSEHOLD
                               END
FROM  ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.CUST_SINGLE_ACCOUNT_VIEW AS sav
ON AD.account_number = sav.account_number
WHERE AD.cb_key_household NOT IN (SELECT cb_key_household FROM ${CBAF_DB_DATA_SCHEMA}.TEMP_AGE_GROUP_MAX) 
GO

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.TEMP_AGE_GROUP
DROP TABLE ${CBAF_DB_DATA_SCHEMA}.TEMP_AGE_GROUP_MAX
GO


MESSAGE 'Populate field age_group - END' type status to client
GO
