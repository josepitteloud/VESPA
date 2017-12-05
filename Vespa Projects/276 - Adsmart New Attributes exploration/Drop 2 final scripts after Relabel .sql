/* **************************************
 		Project:    ADSMART-NEW ATTRIBUTES DROP2
		Purpose:    CREATE FIELDS FOR Drop 2:
			- Sky Generated Home Mover
			- A/B Testing
			- Simple segmentation
			- Intention to Purchase Sports / Intention to Purchase Movies
			- Mobile Average Bill , 2nd mortgage and Early adopter
			- MOSAIC 2014 Groups And Types
			- Sky GO
			- WRESTLING/BOXING PPV IN LAST 18 MONTHS 
			- SKY SPORTS 5 ACTIVATION 
			
		Version:    1
		Updated:    20141204
		Analyst:    JOSE PITTELOUD / ANTHONY MAWBY
		DATA   :    OLIVE
 		Notes: 
			- Simple Segmentation: needs confirmation if the model is available in  productionised table and confirm is we are using the new segments names
			- 2nd Mortgage: The original field name (2nd_mortgage) is causing error when the script is run. Changed to "Second_mortgage". Needs to be updated in the other platforms/tools
*********************************************** */


--------------------------------------------------------
---------------------------- Sky Generated Home Mover
--------------------------------------------------------
SELECT 
		account_number
		, CASE 	
			WHEN home_move_status = 'Pre Home Move' 						THEN 'Pre Home Move' 
			WHEN home_move_status = 'Pending' 					THEN 'Pending Home Move'
			WHEN home_move_status = 'In-Progress'					THEN 'In-Progress Home Move'
			WHEN home_move_status = 'Post Home Move' 	AND DATEDIFF(dd, effective_from_dt, getdate()) BETWEEN 0 AND 30 	THEN 'Post Home Move 0-30 days'
			WHEN home_move_status = 'Post Home Move' 	AND DATEDIFF(dd, effective_from_dt, getdate()) BETWEEN 31 AND 60 	THEN 'Post Home Move 31-60 days'
			WHEN home_move_status = 'Post Home Move' 	AND DATEDIFF(dd, effective_from_dt, getdate()) BETWEEN 61 AND 90 	THEN 'Post Home Move 61-90 days'
			WHEN home_move_status = 'None' 				AND DATEDIFF(dd, effective_from_dt, getdate()) BETWEEN 1 AND 30 	THEN 'Post Home Move 91-120 days'
			WHEN home_move_status = 'None'				AND DATEDIFF(dd, effective_from_dt, getdate()) BETWEEN > 30 		THEN 'None'
			ELSE 'Unknown' END AS home_move_status
INTO #movers
FROM (SELECT *, rank() OVER( PARTITION BY account_number ORDER BY effective_from_dt DESC , dw_lasT_modified_dt DESC ) AS rankk
             FROM  CUST_HOME_MOVE_STATUS_HIST ) as b
WHERE rankk = 1 AND effective_from_dt > DATEADD(dd, -120, GETDATE())
COMMIT 
CREATE HG INDEX id1 ON #movers(account_number)
COMMIT 
UPDATE ####THETABLE#### 							------- Replace by adsmart master table 
		SET SKY_GENERATED_HOME_MOVER = COALESCE (home_move_status, 'Unknown')
FROM ####THETABLE####  as a 						------- Replace by adsmart master table 
JOIN #movers as b ON a.account_number = b.account_number

DROP TABLE #movers
COMMIT 
--------------------------------------------------------
---------------------------- A/B Testing
--------------------------------------------------------
UPDATE ####THETABLE#### 							------- Replace by adsmart master table 
		SET AB_TESTING  = ROUND(CAST(RIGHT(CAST (account_number AS VARCHAR) ,2) AS INT)/5,0)+1 
FROM ####THETABLE####  as a 						------- Replace by adsmart master table 
--------------------------------------------------------
---------------------------- Simple segmentation
--------------------------------------------------------
/*			**************************** NEEDS REVISION:
											- Productionized tables
											- New names were released. We need confirmation on which version are we usign
											
											
											
											
											
											
											
UPDATE ####THETABLE#### 							------- Replace by adsmart master table 		--- DELETE unnecessary Rows 
SET a.SIMPLE_SEGMENTATION =  CASE 	
									WHEN LOWER(b.segment) LIKE '%support%'		THEN 	'Support'
									WHEN LOWER(b.segment) LIKE '%secure%'		THEN	'Secure'
									WHEN LOWER(b.segment) LIKE '%stimulate%'	THEN	'Stimulate'
									WHEN LOWER(b.segment) LIKE '%stabilise'		THEN	'Stabilise'
													ELSE 'Unknown' END
FROM ####THETABLE####  as a 						------- Replace by adsmart master table 
JOIN zubizaa.SIMPLE_SEGMENTATION as b ON a.account_number = b.account_number
*/
--------------------------------------------------------
---------------------------- Intention to Purchase Sports / Intention to Purchase Movies
--------------------------------------------------------
DECLARE @movies AS DATE 
DECLARE @sport AS DATE 

SET @movies = (	SELECT  max(model_run_date) 			
				FROM models.model_scores
				WHERE UPPER(model_name) LIKE '%UPLIFT' AND UPPER(model_name) LIKE 'MOVIE%')
SET @sport = (	SELECT  max(model_run_date) 			
				FROM models.model_scores
				WHERE UPPER(model_name) LIKE '%UPLIFT' AND UPPER(model_name) LIKE 'SPORT%')				
				
				
SELECT 
	  account_number 
	, model_name
	, MAX(Decile) Decile
INTO #models
FROM models.model_scores
WHERE UPPER(model_name) LIKE '%UPLIFT' 
		AND ((UPPER(model_name) LIKE 'SPORT%' AND model_run_date = @sport) 
			OR (UPPER(model_name) LIKE 'MOVIE%' AND model_run_date = @movies))
GROUP BY 
	  account_number 
	, model_name

COMMIT
CREATE HG INDEX idx1 ON #model (account_number)
COMMIT

UPDATE ####THETABLE#### 							------- Replace by adsmart master table 
SET INTENTION_TO_PURCHASE_MOVIES = CASE WHEN b.Decile IN ('1','2','3','4','5','6','7','8','9','10') THEN  b.Decile  ELSE 'Unknown' END)
FROM ####THETABLE####  	AS a 						------- Replace by adsmart master table 
JOIN #models 			AS b ON a.account_number = b.account_number AND UPPER(model_name) LIKE '%UPLIFT' AND UPPER(model_name) LIKE 'MOVIE%' 

COMMIT 

UPDATE ####THETABLE#### 							------- Replace by adsmart master table 
SET INTENTION_TO_PURCHASE_SPORTS = CASE WHEN b.Decile IN ('1','2','3','4','5','6','7','8','9','10') THEN  b.Decile  ELSE 'Unknown' END)
FROM ####THETABLE####  	AS a 						------- Replace by adsmart master table 
JOIN #models 			AS b ON a.account_number = b.account_number AND UPPER(model_name) LIKE '%UPLIFT' AND UPPER(model_name) LIKE 'SPORT%' 
COMMIT 

--------------------------------------------------------
---------------------------- Mobile Average Bill , 2nd mortgage and Early adopter
--------------------------------------------------------
SELECT
      cv.cb_key_household HH_key
	, c.account_number 
    , MAX(CAST(i_love_hunting_out_the_latest_technology_products_and_services_before_anyone_else_catches_on_to_them_percentile as int)) as early
	, MAX(CAST(i_m_always_keen_to_use_new_technology_products_as_soon_as_they_are_available_percentile as int)) as Innovators
	, MAX(CAST(monthly_expenditure_on_mobile_phone_10_29_99_percentile as int)) as mobile_10_29_99
	, MAX(CAST(monthly_expenditure_on_mobile_phone_30_49_99_percentile as int)) as mobile_30_49_99
	, MAX(CAST(monthly_expenditure_on_mobile_phone_50_or_more_percentile as int)) as mobile_50
	, CASE WHEN (Innovators >93) OR (early > 86) THEN 1 ELSE 0 END AS early_adopter
	, CASE 	WHEN (mobile_50 >92) 				THEN '£50+' 
			WHEN (mobile_phone_30_49_99 >87) 	THEN '£30 – £49' 
			WHEN (mobile_phone_10_29_99 >70) 	THEN '£10 - £29' 
			ELSE 'UNKNOWN' END AS mobile_expenditure
	, MAX(CAST(own_properties_other_than_main_residence_rent_out_all_or_sometimes_percentile as int)) as mortgage_2nd
INTO #experian
FROM EXPERIAN_CONSUMERVIEW        as cv
JOIN PERSON_PROPENSITIES_GRID_NEW as pr   ON cv.p_pixel_v2 = pr.ppixel2011 and pr.mosaic_uk_2009_type = cv.h_mosaic_uk_type
JOIN  ####THETABLE#### 	as c ON cv.cb_key_household = c.cb_key_household  					------- Replace by adsmart master table 
GROUP BY HH_key, account_number
COMMIT
CREATE HG INDEX idqw ON #experian(account_number)
CREATE HG INDEX idw ON #experian(HH_key)
COMMIT 


UPDATE ####THETABLE#### 
SET  a.Mobile_Avg_Monthly_Bill 	= COALESCE (b.mobile_expenditure, 'Unknown')									-- MOBILE Expenditure
	,a.EARLY_ADOPTER 		= CASE 	WHEN b.early_adopter = 1 	THEN 'Early Adopters' 	ELSE 'Unknown' END		-- Early Adopter
	,a.Second_Mortgage		= CASE 	WHEN b.mortgage_2nd >= 90 	THEN 'Yes' 	ELSE 'Unknown' END					-- 2nd mortgage
FROM ####THETABLE####  	AS a 																------- Replace by adsmart master table 
JOIN #experian			AS b ON a.account_number = b.account_number AND a.cb_key_household = b.hh_key
DROP TABLE #experian 
COMMIT 

--------------------------------------------------------
---------------------------- MOSAIC 2014 Groups And Types
--------------------------------------------------------
SELECT 
	  account_number
	, cb_key_household
	, MAX(filler_char21)			AS h_mosaic_uk_6_type
	, MAX(filler_char17)			AS h_mosaic_uk_6_group
INTO #mosaic
FROM EXPERIAN_CONSUMERVIEW        as cv
JOIN  ####THETABLE#### 	as c ON cv.cb_key_household = c.cb_key_household 					------- Replace by adsmart master table 
GROUP BY 
	  account_number
	, cb_key_household

COMMIT 
CREATE HG INDEX idef ON #mosaic(account_number)
COMMIT 


UPDATE ####THETABLE#### 
SET  a.MOSAIC_2014_TYPES 	= CASE 	WHEN h_mosaic_uk_6_type LIKE '01' THEN 'World-Class Wealth'
									WHEN h_mosaic_uk_6_type LIKE '02' THEN 'Uptown Elite'
									WHEN h_mosaic_uk_6_type LIKE '03' THEN 'Penthouse Chic'
									WHEN h_mosaic_uk_6_type LIKE '04' THEN 'Metro High-Flyers'
									WHEN h_mosaic_uk_6_type LIKE '05' THEN 'Premium Fortunes'
									WHEN h_mosaic_uk_6_type LIKE '06' THEN 'Diamond Days'
									WHEN h_mosaic_uk_6_type LIKE '07' THEN 'Alpha Families'
									WHEN h_mosaic_uk_6_type LIKE '08' THEN 'Bank of Mum and Dad'
									WHEN h_mosaic_uk_6_type LIKE '09' THEN 'Empty-Nest Adventure'
									WHEN h_mosaic_uk_6_type LIKE '10' THEN 'Wealthy Landowners'
									WHEN h_mosaic_uk_6_type LIKE '11' THEN 'Rural Vogue'
									WHEN h_mosaic_uk_6_type LIKE '12' THEN 'Scattered Homesteads'
									WHEN h_mosaic_uk_6_type LIKE '13' THEN 'Village Retirement'
									WHEN h_mosaic_uk_6_type LIKE '14' THEN 'Satellite Settlers'
									WHEN h_mosaic_uk_6_type LIKE '15' THEN 'Local Focus'
									WHEN h_mosaic_uk_6_type LIKE '16' THEN 'Outlying Seniors'
									WHEN h_mosaic_uk_6_type LIKE '17' THEN 'Far-Flung Outposts'
									WHEN h_mosaic_uk_6_type LIKE '18' THEN 'Legacy Elders'
									WHEN h_mosaic_uk_6_type LIKE '19' THEN 'Bungalow Haven'
									WHEN h_mosaic_uk_6_type LIKE '20' THEN 'Classic Grandparents'
									WHEN h_mosaic_uk_6_type LIKE '21' THEN 'Solo Retirees'
									WHEN h_mosaic_uk_6_type LIKE '22' THEN 'Boomerang Boarders'
									WHEN h_mosaic_uk_6_type LIKE '23' THEN 'Family Ties'
									WHEN h_mosaic_uk_6_type LIKE '24' THEN 'Fledgling Free'
									WHEN h_mosaic_uk_6_type LIKE '25' THEN 'Dependable Me'
									WHEN h_mosaic_uk_6_type LIKE '26' THEN 'Cafés and Catchments'
									WHEN h_mosaic_uk_6_type LIKE '27' THEN 'Thriving Independence'
									WHEN h_mosaic_uk_6_type LIKE '28' THEN 'Modern Parents'
									WHEN h_mosaic_uk_6_type LIKE '29' THEN 'Mid-Career Convention'
									WHEN h_mosaic_uk_6_type LIKE '30' THEN 'Primary Ambitions'
									WHEN h_mosaic_uk_6_type LIKE '31' THEN 'Affordable Fringe'
									WHEN h_mosaic_uk_6_type LIKE '32' THEN 'First-Rung Futures'
									WHEN h_mosaic_uk_6_type LIKE '33' THEN 'Contemporary Starts'
									WHEN h_mosaic_uk_6_type LIKE '34' THEN 'New Foundations'
									WHEN h_mosaic_uk_6_type LIKE '35' THEN 'Flying Solo'
									WHEN h_mosaic_uk_6_type LIKE '36' THEN 'Solid Economy'
									WHEN h_mosaic_uk_6_type LIKE '37' THEN 'Budget Generations'
									WHEN h_mosaic_uk_6_type LIKE '38' THEN 'Childcare Squeeze'
									WHEN h_mosaic_uk_6_type LIKE '39' THEN 'Families with Needs'
									WHEN h_mosaic_uk_6_type LIKE '40' THEN 'Make Do & Move On'
									WHEN h_mosaic_uk_6_type LIKE '41' THEN 'Disconnected Youth'
									WHEN h_mosaic_uk_6_type LIKE '42' THEN 'Midlife Stopgap'
									WHEN h_mosaic_uk_6_type LIKE '43' THEN 'Renting A Room'
									WHEN h_mosaic_uk_6_type LIKE '44' THEN 'Inner City Stalwarts'
									WHEN h_mosaic_uk_6_type LIKE '45' THEN 'Crowded Kaleidoscope'
									WHEN h_mosaic_uk_6_type LIKE '46' THEN 'High Rise Residents'
									WHEN h_mosaic_uk_6_type LIKE '47' THEN 'Streetwise Singles'
									WHEN h_mosaic_uk_6_type LIKE '48' THEN 'Low Income Workers'
									WHEN h_mosaic_uk_6_type LIKE '49' THEN 'Dependent Greys'
									WHEN h_mosaic_uk_6_type LIKE '50' THEN 'Pocket Pensions'
									WHEN h_mosaic_uk_6_type LIKE '51' THEN 'Aided Elderly'
									WHEN h_mosaic_uk_6_type LIKE '52' THEN 'Estate Veterans'
									WHEN h_mosaic_uk_6_type LIKE '53' THEN 'Seasoned Survivors'
									WHEN h_mosaic_uk_6_type LIKE '54' THEN 'Down-to-Earth Owners'
									WHEN h_mosaic_uk_6_type LIKE '55' THEN 'Offspring Overspill'
									WHEN h_mosaic_uk_6_type LIKE '56' THEN 'Self Supporters'
									WHEN h_mosaic_uk_6_type LIKE '57' THEN 'Community Elders'
									WHEN h_mosaic_uk_6_type LIKE '58' THEN 'Cultural Comfort'
									WHEN h_mosaic_uk_6_type LIKE '59' THEN 'Asian Heritage'
									WHEN h_mosaic_uk_6_type LIKE '60' THEN 'Ageing Access'
									WHEN h_mosaic_uk_6_type LIKE '61' THEN 'Career Builders'
									WHEN h_mosaic_uk_6_type LIKE '62' THEN 'Central Pulse'
									WHEN h_mosaic_uk_6_type LIKE '63' THEN 'Flexible Workforce'
									WHEN h_mosaic_uk_6_type LIKE '64' THEN 'Bus-Route Renters'
									WHEN h_mosaic_uk_6_type LIKE '65' THEN 'Learners & Earners'
									WHEN h_mosaic_uk_6_type LIKE '66' THEN 'Student Scene'
									ELSE 'Unknown' END	
	,a.MOSAIC_2014_GROUPS 		= CASE 	WHEN h_mosaic_uk_6_group LIKE 'A' THEN 'City Prosperity'
										WHEN h_mosaic_uk_6_group LIKE 'B' THEN 'Prestige Positions'
										WHEN h_mosaic_uk_6_group LIKE 'C' THEN 'Country Living'
										WHEN h_mosaic_uk_6_group LIKE 'D' THEN 'Rural Reality'
										WHEN h_mosaic_uk_6_group LIKE 'E' THEN 'Senior Security'
										WHEN h_mosaic_uk_6_group LIKE 'F' THEN 'Suburban Stability'
										WHEN h_mosaic_uk_6_group LIKE 'G' THEN 'Domestic Success'
										WHEN h_mosaic_uk_6_group LIKE 'H' THEN 'Aspiring Homemakers'
										WHEN h_mosaic_uk_6_group LIKE 'I' THEN 'Family Basics'
										WHEN h_mosaic_uk_6_group LIKE 'J' THEN 'Transient Renters'
										WHEN h_mosaic_uk_6_group LIKE 'K' THEN 'Municipal Challenge'
										WHEN h_mosaic_uk_6_group LIKE 'L' THEN 'Vintage Value'
										WHEN h_mosaic_uk_6_group LIKE 'M' THEN 'Modest Traditions'
										WHEN h_mosaic_uk_6_group LIKE 'N' THEN 'Urban Cohesion'
										WHEN h_mosaic_uk_6_group LIKE 'O' THEN 'Rental Hubs'
										ELSE 'Unknown' END
FROM ####THETABLE####  	AS a 																------- Replace by adsmart master table 
JOIN #mosaic			AS b ON a.account_number = b.account_number AND a.cb_key_household = b.cb_key_household
 
DROP TABLE #mosaic
COMMIT



--------------------------------------------------------
---------------------------- Sky GO
--------------------------------------------------------

SELECT 
    account_number
    , SkyGo_usage_segment = CASE  WHEN    skygo_latest_usage_date >=  DATEADD(MM, -3, GETDATE() )     	THEN 'Active'			-- Active user: has used Skygo in the past 3 months
                                WHEN    skygo_latest_usage_date <   DATEADD(MM, -3, GETDATE() )  		THEN 'Lapsed'				-- Lapsed > 3 Month: has not used Skygo in the past 3 months
                                WHEN    skygo_latest_usage_date IS NULL                             	THEN 'Registered but never used'
																										ELSE    'Non registered' END
    , RANK () OVER (PARTITION BY account_number ORDER BY skygo_latest_usage_date DESC, skygo_first_stream_date DESC, cb_row_id DESC) rankk
INTO #skygo
from SKY_OTT_USAGE_SUMMARY_ACCOUNT
WHERE account_number is not null
COMMIT

DELETE FROM #skygo WHERE rankk >1
CREATE UNIQUE INDEX skygo1 ON #skygo(account_number)
COMMIT

UPDATE  ####THETABLE####
SET VIEWING_OF_SKY_GO = COALESCE(SkyGo_usage_segment, 'Unknown')
FROM ####THETABLE####  	AS a 																------- Replace by adsmart master table 
JOIN #skygo		AS b ON a.account_number = b.account_number  

DROP TABLE #skygo
COMMIT 					



/*------------------------------------------------------------------------------------------------------------------
        Project:    ADSMART-NEW ATTRIBUTES DROP2
        Program:    OLIVE - NEW ATTRIBUTES - SK_PROD_ADSMART v3 18 months PPV
            Purpose:    CREATE 2 FIELDS FOR SK_PROD.ADSMART BASE 1) WRESTLING/BOXING PPV IN LAST 18 MONTHS 2) SKY SPORTS 5 ACTIVATION  
        Version:    1
        Created:    20141016
        Analyst:    ANTHONY MAWBY
        DATA   :    OLIVE
------------------------------------------------------------------------------------------------------------------*/
--(A) ASSIGN DATES
--(B) OBTAIN BASE
--(C) OBTAIN PPV
--(D) OBTAIN SKY SPORTS 5
--(E) CHECK VOLUMES


--(A) ASSIGN DATES
create variable     @profile_date   date;
create variable     @window_length  integer;     
set                 @profile_date   ='2014-10-15';
set                 @window_length  =548; --18 months;

--(B) OBTAIN BASE

if object_id ('new_attributes_sk_prod_adsmart') is not null
then drop table new_attributes_sk_prod_adsmart
end if;

create table new_attributes_sk_prod_adsmart(
                account_number                      varchar(20)
                ,boxing_ppv                         tinyint
                ,wrestling_ppv                      tinyint
                ,sports_ppv_customers               varchar(20)
                ,activated_sky_sports_5             varchar(3)
);
commit;

SELECT          distinct account_number
INTO            #base
FROM            adsmart 
;


insert into     new_attributes_sk_prod_adsmart(account_number)
select          account_number
from            #base
order by        account_number
;
commit;
create hg index idx1 on new_attributes_sk_prod_adsmart(account_number);
commit;
   
--(C) OBTAIN PPV
select      account_number
            ,MAX(CASE WHEN ppv_genre='BOXING' THEN 1 ELSE 0 END) AS boxing_ppv
            ,MAX(CASE WHEN ppv_genre='WRESTLING' THEN 1 ELSE 0 END) AS wrestling_ppv
into        #PPV
from        cust_product_charges_ppv
where       ppv_service='EVENT'
and         cast(event_dt as date)>@profile_date -@window_length
and         ppv_cancelled_dt = '9999-09-09'
group by    account_number
;
commit;
create hg index idx1 on #ppv(account_number);
commit;

update          new_attributes_sk_prod_adsmart
set             boxing_ppv=case when b.boxing_ppv =1 then 1 else 0 end
                ,wrestling_ppv=case when b.wrestling_ppv =1 then 1 else 0 end
from            new_attributes_sk_prod_adsmart as a
left join       #ppv as b
on              a.account_number=b.account_number
;
commit;

update          new_attributes_sk_prod_adsmart
set             Sports_PPV_Customers =case
                when boxing_ppv=1 and wrestling_ppv=1 then 'Both'
                when boxing_ppv=1 and wrestling_ppv=0 then 'Boxing Only'
                when boxing_ppv=0 and wrestling_ppv=1 then 'Wrestling Only'
                else 'neither' end
                
;
commit;



--(D) OBTAIN SKY SPORTS 5

SELECT          account_number
                ,max(case when subscription_type = 'A-LA-CARTE' and subscription_sub_type = 'SKYSPORTS5'  THEN 1 ELSE 0 END) AS ss5
INTO            #ss5
FROM            cust_subs_hist
WHERE           subscription_sub_type  IN ('SKYSPORTS5')
AND             effective_from_dt <> effective_to_dt
AND             effective_from_dt <= @profile_date
AND             effective_to_dt    >  @profile_date
GROUP BY        account_number;
commit;
create hg index idx1 on #ss5(account_number);
commit;


update          new_attributes_sk_prod_adsmart
set             activated_sky_sports_5 = case when b.ss5 =1 then 'Yes' else 'No' end
from            new_attributes_sk_prod_adsmart as a
left join       #ss5 as b
on              a.account_number=b.account_number
;
commit;

--(E) CHECK VOLUMES
select          boxing_ppv
                ,wrestling_ppv
                ,sports_ppv_customers
                ,activated_sky_sports_5   
                ,count(distinct(account_number)) as volume
from            new_attributes_sk_prod_adsmart
group by        boxing_ppv                        
                ,wrestling_ppv                     
                ,sports_ppv_customers
                ,activated_sky_sports_5  
;
    