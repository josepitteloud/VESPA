/*************************************

DMP

************************************/

IF object_id('DMP_accounts_attributes_1') IS NOT NULL DROP TABLE DMP_accounts_attributes_1
IF object_id('DMP_accounts_attributes_2') IS NOT NULL DROP TABLE DMP_accounts_attributes_2
IF object_id('DMP_accounts_attributes_SAM_No_account') IS NOT NULL DROP TABLE DMP_accounts_attributes_SAM_No_account
IF object_id('DMP_accounts_attributes_Consolidated') IS NOT NULL DROP VIEW DMP_accounts_attributes_Consolidated
IF object_id('DMP_accounts_attributes_samID') IS NOT NULL DROP TABLE DMP_accounts_attributes_samID

COMMIT 

CREATE TABLE DMP_accounts_attributes_1
	( row_id 							BIGINT 		IDENTITY
	, account_number 					VARCHAR(20) DEFAULT '-1'
	, cb_key_household 					BIGINT		DEFAULT null
	, cb_address_postcode 				VARCHAR(10)	DEFAULT 'U'	
	, cust_email_allowed				VARCHAR(1) 	DEFAULT 'U'	
	, cust_postal_mail_allowed			VARCHAR(1) 	DEFAULT 'U'	
	, cust_telephone_contact_allowed 	VARCHAR(1) 	DEFAULT 'U'	
	, Virgin							VARCHAR(1) 	DEFAULT 'N' -- FROM HENSIRIR.SK4498_Q3_PLANNING_VIRGIN_PROSPECTS_HHSX (target_segment)
	, BT_model_decile					VARCHAR(1)	DEFAULT 'N' 
	, DTV_end_dt 	 					DATE	 	DEFAULT '1900-01-01'
	, BB_end_dt  	 					DATE	 	DEFAULT '1900-01-01'
	, Broadband_package 				VARCHAR(50) DEFAULT 'None'
	, Renter_flag						VARCHAR(1)	DEFAULT 'U'
	, p_hms_verified_flag				VARCHAR(1) 	DEFAULT 'N'
	, Movies_Downgrade_12M				BIT 		DEFAULT 0 
	, Sports_Downgrade_12M				BIT 		DEFAULT 0 
	, skyfibre_enabled					VARCHAR(1) 	DEFAULT 'U'
	, skyfibre_estimated_enabled_date 	DATE	 	DEFAULT '1900-01-01'
	, CQM_model_score					INT 		DEFAULT -1
	, ppv_viewed_90D					BIT 		DEFAULT 0 
--, samprofileid						BIGINT 		DEFAULT NULL
	, Simple_segment					TINYINT		DEFAULT 0
	, DTV_account_status				VARCHAR(2) 	DEFAULT 'U'
	, DTV_Package						VARCHAR(50) DEFAULT 'Unknown'
	, Telephone							VARCHAR(50) DEFAULT 'None'
	) 
	

CREATE TABLE DMP_accounts_attributes_2
	( row_id 							BIGINT 		IDENTITY
	, account_number 					VARCHAR(20) DEFAULT '-1'
	, cb_key_household 					BIGINT		DEFAULT -1
	, current_age						VARCHAR(1)	DEFAULT '00'
	, h_mosaic_uk_group					VARCHAR(1) 	DEFAULT 'U'	
	, h_household_composition			VARCHAR(2) 	DEFAULT 'U'
	, cust_gender						VARCHAR(1) 	DEFAULT 'U'
	, social_class						VARCHAR(2) 	DEFAULT 'U' --lukcat_fr_de_nrs
	, cb_address_postcode				VARCHAR(10) DEFAULT 'U'
	, Connurbation						VARCHAR(50) DEFAULT 'UNKNOWN'	
	, sky_go_reg						VARCHAR(2) 	DEFAULT '0' -- site_sk
	, Sky_bet_status					VARCHAR(12) DEFAULT  'Unknown' --cu_cust_status_desc				
	, Primary_box_desc 					VARCHAR(250) DEFAULT 'Unknown' 					
	, type_of_box						VARCHAR(1) 	DEFAULT  'U'
	, VOD_flag							BIT 		DEFAULT 0 	 -- x_content_type_desc
	, BB_exchange_status				VARCHAR(10) DEFAULT 'UNKNOWN' --	exchange_status
    , MDU_enabled                       VARCHAR(1)  DEFAULT 'U'
    , MDU_property                      VARCHAR(1)  DEFAULT 'U'
	, cable_postcode					BIT 		DEFAULT 0 )

COMMIT 


CREATE TABLE DMP_accounts_attributes_SAM_No_account
	( row_id 							BIGINT 		IDENTITY
	, sam_profile_id					BIGINT		DEFAULT NULL
	, cb_key_household					BIGINT		DEFAULT NULL
	, cb_address_postcode				VARCHAR(10) DEFAULT 'U'
	, Virgin							VARCHAR(1) 	DEFAULT 'N' 
	, BT_model_decile					VARCHAR(1)	DEFAULT 'N' 
	, skyfibre_enabled					VARCHAR(1) 	DEFAULT 'U'
	, skyfibre_estimated_enabled_date 	DATE	 	DEFAULT '1900-01-01'
	, CQM_model_score					INT			DEFAULT -1
	, Renter_flag						VARCHAR(1)	DEFAULT 'U'
	, p_hms_verified_flag				VARCHAR(1) 	DEFAULT 'N'
	, h_mosaic_uk_group					VARCHAR(1) 	DEFAULT 'U'	
	, h_household_composition			VARCHAR(2) 	DEFAULT 'U'
	, social_class						VARCHAR(2) 	DEFAULT 'U' 
	, Connurbation						VARCHAR(50) DEFAULT 'UNKNOWN'	
	, BB_exchange_status				VARCHAR(10) DEFAULT 'UNKNOWN'  --	exchange_status
	, MDU_enabled						VARCHAR(1)	DEFAULT 'U'
	, MDU_property						VARCHAR(1)	DEFAULT 'U'
	, cable_postcode					BIT 		DEFAULT 0 )	
	
COMMIT
--------------------------------------------------
------------------------- SELECTING all SAM profiles
--------------------------------------------------	
SELECT DISTINCT 
	  account_number
	, samprofileid 					
	, cb_key_household					
INTO DMP_accounts_attributes_samID
FROM SK_PROD.SAM_REGISTRANT 
WHERE x_user_type in ('Primary', 'Secondary','primary','secondary') 
	AND marked_as_deleted = 'N'

--------------------------------------------------
------------------------- Populating SAM profiles without linked an account
--------------------------------------------------		
INSERT INTO DMP_accounts_attributes_SAM_No_account (sam_profile_id 	, cb_key_household)
SELECT 
	  samprofileid 					
	, cb_key_household	
FROM DMP_accounts_attributes_samID
WHERE account_number IS NULL

--------------------------------------------------
------------------------- Populating with accounts from SAV
--------------------------------------------------
TRUNCATE TABLE DMP_accounts_attributes_1

INSERT INTO DMP_accounts_attributes_1 
	( account_number
	, cust_email_allowed
	, cust_postal_mail_allowed
	, cust_telephone_contact_allowed
	, DTV_account_status
	, cb_key_household 
	, cb_address_postcode
	)
SELECT DISTINCT  
	account_number 
	, CASE WHEN UPPER(cust_email_allowed) 				= 'Y' THEN 'Y' ELSE 'N' END
	, CASE WHEN UPPER(cust_postal_mail_allowed) 		= 'Y' THEN 'Y' ELSE 'N' END
	, CASE WHEN UPPER(cust_telephone_contact_allowed) 	= 'Y' THEN 'Y' ELSE 'N' END 
	, COALESCE (prod_latest_dtv_status_code, 'UN' )
	, cb_key_household 
	, cb_address_postcode
FROM SK_PROD.CUST_SINGLE_ACCOUNT_VIEW 
WHERE 
	    cust_active_dtv = 1 
	AND account_number IS NOT NULL

COMMIT 
--------------------------------------------------
------------------------- Creating Indexes
--------------------------------------------------
CREATE HG INDEX id1 ON DMP_accounts_attributes_1(account_number)
CREATE HG INDEX id2 ON DMP_accounts_attributes_1(row_id)
CREATE HG INDEX id3 ON DMP_accounts_attributes_1(cb_key_household)
CREATE HG INDEX id4 ON DMP_accounts_attributes_1(cb_address_postcode) 
COMMIT 

--------------------------------------------------
------------------------- Virgin: Table includes prospect HHs that are most likely to be VM
--------------------------------------------------


UPDATE DMP_accounts_attributes_1
SET Virgin = 'Y'
FROM DMP_accounts_attributes_1 AS a
JOIN HENSIRIR.SK4498_Q3_PLANNING_VIRGIN_PROSPECTS_HHSX AS b ON a.cb_key_household = b.cb_key_household 

---- UPDATING SAM profiles w/o accounts
UPDATE DMP_accounts_attributes_SAM_No_account
SET Virgin = 'Y'
FROM DMP_accounts_attributes_SAM_No_account AS a
JOIN HENSIRIR.SK4498_Q3_PLANNING_VIRGIN_PROSPECTS_HHSX AS b ON a.cb_key_household = b.cb_key_household 

--------------------------------------------------
------------------------- BT High Deciles
--------------------------------------------------
UPDATE DMP_accounts_attributes_1
SET BT_model_decile = 'Y' 
FROM DMP_accounts_attributes_1 AS a 
JOIN ZUBIZAA.M002_BT_PROPENSITY_MODEL AS b ON a.cb_key_household = b.cb_key_household AND decile IN (1,2,3)

---- UPDATING SAM profiles w/o accounts
UPDATE DMP_accounts_attributes_SAM_No_account
SET BT_model_decile = 'Y' 
FROM DMP_accounts_attributes_SAM_No_account AS a 
JOIN ZUBIZAA.M002_BT_PROPENSITY_MODEL AS b ON a.cb_key_household = b.cb_key_household AND decile IN (1,2,3)
--------------------------------------------------
------------------------- DOWNGRADES
--------------------------------------------------
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
                    INTO downgrades
    FROM sk_prod.cust_subs_hist AS csh
         inner join sk_prod.cust_entitlement_lookup AS ncel
                    ON csh.current_short_description = ncel.short_description
         inner join sk_prod.cust_entitlement_lookup AS ocel
                    ON csh.previous_short_description = ocel.short_description
         inner join DMP_accounts_attributes_1 AS Base
                    ON csh.account_number = base.account_number
WHERE csh.effective_from_dt >= DATEADD(MM, -12, GETDATE())  -- Date range
    AND csh.effective_to_dt > csh.effective_from_dt
    AND subscription_sub_type='DTV Primary Viewing'
    AND status_code IN ('AC','PC','AB')   -- Active records
    AND (current_premiums  < old_premiums -- Decrease in premiums
        OR current_movies < old_movies    -- Decrease in movies
        OR current_sports < old_sports)   -- Decrease in sports
    AND csh.ent_cat_prod_changed = 'Y'    -- The package has changed - VERY IMPORTANT
    AND csh.prev_ent_cat_product_id<>'?'  -- This is not an Aquisition

COMMIT 

DELETE FROM downgrades where rank_id >1

COMMIT 

ALTER table     downgrades ADD   Premiums_downgrades  integer
ALTER table     downgrades ADD   Movies_downgrades  integer
ALTER table     downgrades ADD   Sports_downgrades  integer

commit
-- case statement to work out movie, sports and total downgrades
UPDATE downgrades
SET
	 Premiums_downgrades =   CASE WHEN old_premiums > current_premiums THEN 1  ELSE 0  END
	,Movies_downgrades  =    CASE WHEN old_movies > current_movies     THEN 1  ELSE 0  END
	,Sports_downgrades  =    CASE WHEN old_sports > current_Sports     THEN 1  ELSE 0  END
FROM downgrades
COMMIT 
CREATE HG INDEX idac ON downgrades(Account_number)
COMMIT 

UPDATE DMP_accounts_attributes_1
SET   Movies_Downgrade_12M 	= Movies_downgrades
	, Sports_Downgrade_12M	= Sports_downgrades
FROM DMP_accounts_attributes_1 AS a 
JOIN downgrades 	AS b ON a.account_number = b.account_number 

DROP TABLE downgrades 	

COMMIT 

--------------------------------------------------
------------------------- Simple Segmentation
--------------------------------------------------
UPDATE DMP_accounts_attributes_1
SET Simple_segment	= CASE WHEN CAST(LEFT(segment,1)  AS INT) = 5 THEN 0 ELSE CAST(LEFT(segment,1)  AS INT) END 
FROM DMP_accounts_attributes_1 AS a 
JOIN AMBEKARS.CM_SEGMENTS	AS b ON a.account_number = b.account_number

COMMIT

--------------------------------------------------
------------------------- Sky Store
--------------------------------------------------
SELECT 
	  account_number
	, MAX(ppv_viewed_dt) ppv_viewed_dt1
INTO SkyStore
FROM SK_PROD.CUST_PRODUCT_CHARGES_PPV
WHERE 
	account_number IS NOT NULL
	AND ppv_viewed_dt <= GETDATE()
	AND ppv_viewed_dt >= DATEADD(dd, -90, GETDATE())
GROUP BY  account_number


UPDATE DMP_accounts_attributes_1
SET ppv_viewed_90D = 1
FROM DMP_accounts_attributes_1 AS a 
JOIN SkyStore	AS b ON a.account_number = b.account_number

DROP TABLE SkyStore
COMMIT

--------------------------------------------------
------------------------- Customer quality measure
--------------------------------------------------
UPDATE DMP_accounts_attributes_1
SET CQM_model_score = COALESCE(model_score, -1)
FROM DMP_accounts_attributes_1 AS a 
JOIN SK_PROD.ID_V_UNIVERSE_ALL AS b ON a.cb_key_household  = b.cb_key_household 

COMMIT

---- UPDATING SAM profiles w/o accounts
UPDATE DMP_accounts_attributes_SAM_No_account
SET CQM_model_score = COALESCE(model_score, -1)
FROM DMP_accounts_attributes_SAM_No_account AS a 
JOIN SK_PROD.ID_V_UNIVERSE_ALL AS b ON a.cb_key_household  = b.cb_key_household 

COMMIT
--------------------------------------------------
------------------------- Sky Fibre
--------------------------------------------------
SELECT 
	  cb_address_postcode
	, x_skyfibre_enabled
	, MAX(x_skyfibre_estimated_enabled_date) x_skyfibre_estimated_enabled_date1
INTO sky_fibre 
FROM SK_PROD.BT_FIBRE_POSTCODE
GROUP BY 
	  cb_address_postcode
	, x_skyfibre_enabled

COMMIT
CREATE HG INDEX idde ON sky_fibre(cb_address_postcode) 
COMMIT 
	
UPDATE DMP_accounts_attributes_1
SET 
	  skyfibre_enabled = COALESCE(b.x_skyfibre_enabled,'U')
	, skyfibre_estimated_enabled_date =Coalesce(b.x_skyfibre_estimated_enabled_date1, '1900-01-01')
FROM DMP_accounts_attributes_1 AS a 
JOIN sky_fibre AS b ON a.cb_address_postcode = b.cb_address_postcode 


UPDATE DMP_accounts_attributes_SAM_No_account
SET 
	  skyfibre_enabled = COALESCE(b.x_skyfibre_enabled,'U')
	, skyfibre_estimated_enabled_date = Coalesce(b.x_skyfibre_estimated_enabled_date1, '1900-01-01')
FROM DMP_accounts_attributes_SAM_No_account AS a 
JOIN sky_fibre AS b ON a.cb_address_postcode = b.cb_address_postcode 

DROP TABLE sky_fibre
COMMIT

--------------------------------------------------
------------------------- EXPERIAN FLAGS
--------------------------------------------------
SELECT
      a.cb_key_household
    , h_tenure
    , c.p_hms_verified_flag
	, h_mosaic_uk_group
	, h_household_composition
    , rank() OVER(PARTITION BY b.cb_key_household ORDER BY b.cb_key_individual ASC) rankk
INTO sky_experian
FROM (	SELECT cb_key_household FROM DMP_accounts_attributes_1 
		UNION 
		SELECT cb_key_household FROM DMP_accounts_attributes_SAM_No_account) 	AS a
LEFT JOIN sk_prod.EXPERIAN_consumerview AS b on a.cb_key_household = b.cb_key_household
LEFT JOIN sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD AS c ON a.cb_key_household = c.exp_cb_key_household


DELETE FROM  sky_experian 
WHERE rankk > 1 

COMMIT
CREATE HG INDEX idde ON sky_experian(cb_key_household) 
COMMIT 

UPDATE DMP_accounts_attributes_1
SET   Renter_flag			= COALESCE (b.h_tenure, 'U')
	, a.p_hms_verified_flag = COALESCE (b.p_hms_verified_flag, 'N')
FROM DMP_accounts_attributes_1 AS a 
JOIN sky_experian AS b ON a.cb_key_household = b.cb_key_household


UPDATE DMP_accounts_attributes_SAM_No_account
SET   Renter_flag			= COALESCE (b.h_tenure, 'U')
	, a.p_hms_verified_flag = COALESCE (b.p_hms_verified_flag, 'N')
	, a.h_mosaic_uk_group	= COALESCE (b.h_mosaic_uk_group, 'U')
	, a.h_household_composition = COALESCE(b.h_household_composition , 'U')
FROM DMP_accounts_attributes_SAM_No_account AS a 
JOIN sky_experian AS b ON a.cb_key_household = b.cb_key_household


DROP TABLE sky_experian
COMMIT



--------------------------------------------------
------------------------- Update from CUST_SUBS_HIST
--------------------------------------------------
--------------------------------------------------
------------------------- Broadband package
--------------------------------------------------
Select distinct base.account_number
           ,CASE WHEN current_product_sk=43373 THEN 'New Sky Broadband Unlimited'
                 WHEN current_product_sk=42128 THEN 'Sky Broadband Unlimited'
                 WHEN current_product_sk=42129 THEN 'Sky Broadband Everyday'
                 WHEN current_product_sk=42130 THEN 'Sky Broadband Lite'
                 WHEN current_product_sk=42131 THEN 'Broadband Connect'
				 WHEN current_product_sk=43494 THEN 'Sky Broadband Unlimited Fibre'
				 WHEN current_product_sk=43543 THEN 'Sky Fibre Unlimited Pro'
				 WHEN current_product_sk=44523 THEN 'Sky Broadband Unlimited Pro'
                 ELSE 'Other'
                 END AS BB_type
               ,rank() over(PARTITION BY base.account_number ORDER BY effective_to_dt desc) AS rank_id
               ,effective_to_dt
        ,count(*) AS total
INTO bb
FROM sk_prod.cust_subs_hist AS CSH
    INNER JOIN DMP_accounts_attributes_1 AS Base     ON csh.account_number = base.account_number
WHERE subscription_sub_type = 'Broadband DSL Line'
   AND csh.effective_from_dt <= GETDATE()
   AND csh.effective_to_dt > GETDATE()
      AND effective_from_dt != effective_to_dt
      AND (status_code IN ('AC','AB') OR (status_code='PC' AND prev_status_code NOT IN ('?','RQ','AP','UB','BE','PA') )
            OR (status_code='CF' AND prev_status_code='PC')
            OR (status_code='AP' AND sale_type='SNS Bulk Migration'))
GROUP BY base.account_number, bb_type, effective_to_dt

COMMIT
DELETE FROM bb where rank_id >1
COMMIT

SELECT DISTINCT 
	  account_number
	, BB_type
    , RANK() over(PARTITION BY account_number ORDER BY BB_type desc) AS rank_id
INTO bbb
FROM bb

COMMIT
DELETE FROM bbb WHERE rank_id >1
COMMIT 
CREATE   HG INDEX idx10 ON BB(account_number)
COMMIT 

UPDATE DMP_accounts_attributes_1
SET Broadband_package = BB_type
FROM DMP_accounts_attributes_1 AS a 
JOIN bbb	AS b ON a.account_number = b.account_number

DROP TABLE bb
DROP TABLE BBB
COMMIT

--------------------------------------------------
------------------------- Telephone Package
--------------------------------------------------
SELECT DISTINCT base.account_number
       ,CASE 	WHEN UCASE(current_product_description) LIKE '%UNLIMITED%' 				THEN 'Unlimited'
				WHEN UCASE(current_product_description) LIKE '%WEEKENDS%'				THEN 'Weekends'
				WHEN UCASE(current_product_description) LIKE '%ANYTIME UK%'				THEN 'Anytime UK'
				WHEN UCASE(current_product_description) LIKE '%ANYTIME INTERNATIONAL%'	THEN 'Anytime International'
				WHEN UCASE(current_product_description) LIKE '%EXTRA%'					THEN 'Extra'
				WHEN UCASE(current_product_description) LIKE '%OFF PEAK%'				THEN 'Off Peak'
				WHEN UCASE(current_product_description) LIKE '%ANYTIME%'				THEN 'Anytime'
				WHEN UCASE(current_product_description) LIKE '%FREETIME%'				THEN 'Freetime'
				ELSE 'Other'
          END as talk_product
      ,rank() over(PARTITION BY base.account_number ORDER BY effective_to_dt desc) AS rank_id
      ,effective_to_dt
INTO talk
FROM sk_prod.cust_subs_hist AS CSH
INNER JOIN DMP_accounts_attributes_1 AS Base    ON csh.account_number = base.account_number
WHERE subscription_sub_type = 'SKY TALK SELECT'
     AND(     status_code = 'A'
          OR (status_code = 'FBP' AND prev_status_code IN ('PC','A'))
          OR (status_code = 'RI'  AND prev_status_code IN ('FBP','A'))
          OR (status_code = 'PC'  AND prev_status_code = 'A'))
     AND effective_to_dt != effective_from_dt
     AND csh.effective_from_dt <= GETDATE()
     AND csh.effective_to_dt 	> GETDATE()
GROUP BY base.account_number
		, talk_product,effective_to_dt

COMMIT

DELETE FROM talk where rank_id >1
COMMIT

--      create index on talk
CREATE   HG INDEX idx09 ON talk(account_number)
commit

--      update DMP tables
UPDATE DMP_accounts_attributes_1
SET  Telephone = talk.talk_product
FROM DMP_accounts_attributes_1  AS Base
INNER JOIN talk AS talk   ON base.account_number = talk.account_number
ORDER BY base.account_number

COMMIT 

DROP TABLE talk
COMMIT 

--------------------------------------------------
------------------------- DTV Package
--------------------------------------------------
SELECT 
	  CSH.account_number 
	, current_product_description
	, rank() over(PARTITION BY base.account_number ORDER BY effective_to_dt desc) AS rank_id
    , effective_to_dt
INTO SkyDTV
FROM sk_prod.cust_subs_hist AS CSH
INNER JOIN DMP_accounts_attributes_1 AS Base    ON csh.account_number = base.account_number
WHERE csh.subscription_sub_type ='DTV Primary Viewing'
       AND csh.subscription_type = 'DTV PACKAGE'
       AND csh.status_code in ('AC','AB','PC')
       AND csh.effective_from_dt < GETDATE()
       AND csh.effective_to_dt   >=  GETDATE()
       AND csh.effective_from_dt != csh.effective_to_dt

DELETE FROM SkyDTV WHERE rank_id > 1
COMMIT 
CREATE HG INDEX idx1 ON SkyDTV(account_number)
COMMIT

UPDATE DMP_accounts_attributes_1
SET  DTV_Package = COALESCE(SkyDTV.current_product_description, 'Unknown')
FROM DMP_accounts_attributes_1  AS Base
INNER JOIN SkyDTV  ON base.account_number = SkyDTV.account_number
ORDER BY base.account_number

COMMIT 

DROP TABLE SkyDTV
COMMIT 

--------------------------------------------------
------------------------- •	Contract end date 
--------------------------------------------------
SELECT
     b.account_number
    , CAST(COALESCE(END_DT, END_DT_CALC, DATEADD(mm, MIN_TERM_MONTHS, START_DT)) AS DATE) AS END_DTT
	, RANK() OVER (PARTITION BY a.account_number ORDER BY END_DTT DESC, a.CB_row_id) AS rankk
INTO skyEND1
from SK_PROD.CUST_CONTRACT_AGREEMENTS as a
JOIN DMP_accounts_attributes_1  AS b ON a.account_number = b.account_number
where subscription_type = 'Primary DTV'
AND b.account_number is not null

DELETE FROM skyEND1 WHERE rankk > 1
COMMIT 
CREATE HG INDEX wef ON skyEND1(account_number)
COMMIT 

SELECT
     b.account_number
    ,CAST(COALESCE(END_DT, END_DT_CALC, DATEADD(mm, MIN_TERM_MONTHS, START_DT)) AS DATE) AS END_DTT
	, RANK() OVER (PARTITION BY a.account_number ORDER BY END_DTT DESC, a.CB_row_id) AS rankk
INTO skyEND2
from SK_PROD.CUST_CONTRACT_AGREEMENTS as a 
JOIN DMP_accounts_attributes_1  AS b ON a.account_number = b.account_number
where subscription_type = 'Broadband'
AND b.account_number is not null

DELETE FROM skyEND2 WHERE rankk > 1
COMMIT 
CREATE HG INDEX wef ON skyEND2(account_number)
COMMIT 

UPDATE DMP_accounts_attributes_1
SET  DTV_end_dt = COALESCE(END_DTT, '1900-01-01')
FROM DMP_accounts_attributes_1 AS a
JOIN skyEND1 AS b ON a.account_number = b.account_number

UPDATE DMP_accounts_attributes_1
SET  BB_end_dt =  COALESCE(END_DTT, '1900-01-01')
FROM DMP_accounts_attributes_1 AS a
JOIN skyEND2 AS b ON a.account_number = b.account_number

DROP TABLE skyEND1
DROP TABLE skyEND2
COMMIT 

--------------------------------------------------
--------------------------------------------------
--------------------------------------------------
------------------------- Populating with accounts from SAV - Table 2
--------------------------------------------------
--------------------------------------------------
--------------------------------------------------
INSERT INTO DMP_accounts_attributes_2
	( account_number
	, cb_key_household 
	, cb_address_postcode
	, current_age
	, h_mosaic_uk_group
	, h_household_composition
	, cust_gender
	, social_class
	, Connurbation

	)
SELECT DISTINCT  
	account_number 
	, cb_key_household 
	, cb_address_postcode
	, COALESCE(CAST(cl_current_age AS VARCHAR), '00')
	, COALESCE(mosaic_segments_cd, 'U')
	, COALESCE(household_composition_cd, 'U')
	, COALESCE(CASE WHEN cust_gender = '?' THEN 'U' ELSE cust_gender END, 'U')
	, COALESCE(CASE WHEN social_class LIKE 'Uncl%' THEN 'U' ELSE social_class END, 'U')
	, region
FROM SK_PROD.CUST_SINGLE_ACCOUNT_VIEW 
WHERE 
	    cust_active_dtv = 1 
	AND account_number IS NOT NULL

COMMIT 


CREATE HG INDEX id1 ON DMP_accounts_attributes_2(account_number)
CREATE HG INDEX id2 ON DMP_accounts_attributes_2(row_id)
CREATE HG INDEX id3 ON DMP_accounts_attributes_2(cb_key_household)

COMMIT 
--------------------------------------------------
------------------------- Social Class in the SAM w/o accounts
--------------------------------------------------
SELECT b.cb_key_household
	, lukcat_fr_de_nrs
	, RANK() OVER (PARTITION BY b.cb_key_household ORDER BY lukcat_fr_de_nrs, a.cb_row_id) rankk
INTO SkySC
FROM SK_PROD.CACI_SOCIAL_CLASS as a
JOIN DMP_accounts_attributes_SAM_No_account as b ON a.cb_key_household = b.cb_key_household

DELETE FROM SkySC WHERE rankk >1 

COMMIT 
CREATE HG INDEX idx ON SkySC(cb_key_household)
COMMIT 

UPDATE DMP_accounts_attributes_SAM_No_account
SET social_class = lukcat_fr_de_nrs
FROM DMP_accounts_attributes_SAM_No_account AS a 
JOIN SkySC AS b ON a.cb_key_household = b.cb_key_household

DROP TABLE SkySC
COMMIT 

--------------------------------------------------
------------------------- Sky Go/Player Registrant
--------------------------------------------------

SELECT 
	 a.account_number 
	, flag = MAX(CASE WHEN  site_sk IN (1,2,3,4,5,6,7,8,16,18,19,20,-1) THEN '1' ELSE '0' END) 
INTO SkyGo
FROM DMP_accounts_attributes_2 AS a
JOIN SK_PROD.SKY_PLAYER_USAGE_DETAIL AS b ON a.account_number = b.account_number
WHERE activity_dt >= DATEADD(mm, -6, GETDATE())
GROUP BY a.account_number

COMMIT 
CREATE HG INDEX idx ON SkyGo(account_number)
COMMIT 
	
UPDATE DMP_accounts_attributes_2
SET sky_go_reg = COALESCE(flag, '0')
FROM DMP_accounts_attributes_2 AS a 
JOIN SkyGo AS b ON a.account_number  = b.account_number 

DROP TABLE SkyGo
COMMIT

--------------------------------------------------
------------------------- Sky Bet Account
--------------------------------------------------
SELECT 
	  account_number 
	, cu_cust_status_desc
	, rank() OVER (PARTITION BY account_number ORDER BY cu_dw_last_modified_dt, cb_row_id DESC) AS rankk
INTO SkyBet
FROM SK_PROD.SKYBET_CUSTOMER
WHERE account_number is not null

DELETE FROM SkyBet WHERE rankk >1

COMMIT
CREATE HG INDEX idw ON SkyBet(account_number)
COMMIT 

UPDATE DMP_accounts_attributes_2
SET Sky_bet_status = COALESCE(cu_cust_status_desc, 'Unknown')
FROM DMP_accounts_attributes_2 AS a 
JOIN SkyBet AS b ON a.account_number  = b.account_number 

DROP TABLE SkyBet
COMMIT

--------------------------------------------------
------------------------- Sky box type - Primary Box
--------------------------------------------------
SELECT 
    account_number
    , x_box_type||' - '||x_model_number||' - '||x_decoder_nds_number_prefix_4||' - '||x_description AS Primary_box_desc
    , CASE WHEN (x_subscription_sub_type LIKE 'DTV Primary Viewing') OR (x_subscription_sub_type IS NULL) THEN 'P' ELSE 'S' END type_of_box
    , DENSE_RANK() OVER (PARTITION BY account_number ORDER BY type_of_box, status_start_dt DESC) rankk
INTO SkyBox
FROM SK_PROD.CUST_SET_TOP_BOX
WHERE active_box_flag ='Y'

DELETE FROM SkyBox WHERE rankk > 1

COMMIT
CREATE HG INDEX idr ON SkyBox(account_number)
COMMIT

UPDATE DMP_accounts_attributes_2
SET a.Primary_box_desc 	= COALESCE(b.Primary_box_desc, 'Unknown')
	, a.type_of_box 	= COALESCE(b.type_of_box, 'U')
FROM DMP_accounts_attributes_2 as a 
JOIN SkyBox AS b ON a.account_number  = b.account_number 

DROP TABLE SkyBox
COMMIT

--------------------------------------------------
------------------------- On Demand connected customer
--------------------------------------------------
SELECT 
       account_number
     , MAX(CASE WHEN (x_content_type_desc = 'PROGRAMME') 
					AND (x_download_size_mb > 0) 
					AND x_actual_downloaded_size_mb /x_download_size_mb > 0.5 
							THEN 1 ELSE 0 END) VOD_flag -- Only PROGRAMMES and over 50% of the total download
INTO SkyVOD
FROM SK_PROD.CUST_ANYTIME_PLUS_DOWNLOADS
WHERE exhibition_dt > DATEADD(dd, -90, GETDATE())
AND x_download_size_mb >0
AND account_number IS NOT NULL
GROUP BY account_number
HAVING VOD_flag = 1 

COMMIT
CREATE HG INDEX idv ON SkyVOD(account_number)
COMMIT

UPDATE DMP_accounts_attributes_2
SET VOD_flag = b.VOD_flag
FROM DMP_accounts_attributes_2 as a 
JOIN SkyVOD AS b ON a.account_number  = b.account_number 

DROP TABLE SkyVOD
COMMIT 

--------------------------------------------------
------------------------- Broadband on-net / offnet
--------------------------------------------------
SELECT
      postcode
    , exchange_status
INTO skyOnNet
FROM CITEAM.PC_BASE


COMMIT
CREATE HG INDEX idde ON skyOnNet(postcode)
COMMIT

UPDATE DMP_accounts_attributes_2
SET  BB_exchange_status = COALESCE(b.exchange_status, 'Unknown')
FROM DMP_accounts_attributes_2 AS a
JOIN skyOnNet AS b ON a.cb_address_postcode = b.postcode

UPDATE DMP_accounts_attributes_SAM_No_account
SET  BB_exchange_status = COALESCE(b.exchange_status, 'Unknown')
FROM DMP_accounts_attributes_SAM_No_account AS a
JOIN skyOnNet AS b ON a.cb_address_postcode = b.postcode

DROP TABLE skyOnNet
COMMIT
--------------------------------------------------
------------------------- MDU enabled property
--------------------------------------------------
SELECT cb_key_household
    , MDU_enabled =  MAX(CASE WHEN (sky_homes_property_new  in (2,3) AND valid_occurrences_premise >= 4 AND valid_flag = 1) THEN 'Y' ELSE 'N' END)
    , MDU_property = MAX(CASE WHEN (sky_homes_property_new  in (2,3,4,5,6) AND valid_occurrences_premise >= 4 AND valid_flag = 1) THEN 'Y' ELSE 'N' END)
INTO SkyMUD
FROM SK_PROD.ORD_SURVEY_ADDR_POINT
GROUP BY cb_key_household

COMMIT
CREATE HG INDEX idfe ON SkyMUD(cb_key_household)
COMMIT


UPDATE 	DMP_accounts_attributes_2
SET a.MDU_enabled 		= COALESCE(b.MDU_enabled, 'U')
	, a.MDU_property 	= COALESCE(b.MDU_property, 'U')
FROM DMP_accounts_attributes_2 AS a 
JOIN SkyMUD AS b ON a.cb_key_household = b.cb_key_household

UPDATE 	DMP_accounts_attributes_SAM_No_account
SET a.MDU_enabled 		= COALESCE(b.MDU_enabled, 'U')
	, a.MDU_property	= COALESCE(b.MDU_property, 'U')
FROM DMP_accounts_attributes_SAM_No_account AS a 
JOIN SkyMUD AS b ON a.cb_key_household = b.cb_key_household

DROP TABLE SkyMUD
COMMIT
--------------------------------------------------
------------------------- Cable
--------------------------------------------------
SELECT
       CASE WHEN cable_postcode IN ('Y','y') THEN '1' ELSE '0' END AS cable_postcode1
     , cb_address_postcode
INTO skyCable
FROM SK_PROD.BROADBAND_POSTCODE_EXCHANGE


COMMIT
CREATE HG INDEX idde ON skyCable(cb_address_postcode)
COMMIT

UPDATE DMP_accounts_attributes_2
SET  a.cable_postcode = b.cable_postcode1
FROM DMP_accounts_attributes_2 AS a
JOIN skyCable AS b ON a.cb_address_postcode = b.cb_address_postcode

UPDATE DMP_accounts_attributes_SAM_No_account
SET  a.cable_postcode = b.cable_postcode1
FROM DMP_accounts_attributes_SAM_No_account AS a
JOIN skyCable AS b ON a.cb_address_postcode = b.cb_address_postcode

DROP TABLE skyCable
COMMIT


--------------------------------------------------
------------------------- Connurbation for SAM ID's w/o accounts
--------------------------------------------------

SELECT DISTINCT
    a.cb_address_postcode
    , region
    , RANK() OVER (PARTITION BY region ORDER BY cb_row_id DESC) rankk
INTO SkyRegion
FROM SK_PROD.CUST_SINGLE_ACCOUNT_VIEW AS a
JOIN DMP_accounts_attributes_SAM_No_account AS b ON a.cb_address_postcode = b.cb_address_postcode

DELETE FROM SkyRegion WHERE rankk > 1

COMMIT
CREATE HG INDEX idqw ON Skyregion(cb_address_postcode)
COMMIT

UPDATE DMP_accounts_attributes_SAM_No_account
SET connurbation = COALESCE(region, 'UNKNOWN')
FROM DMP_accounts_attributes_SAM_No_account AS a
JOIN SkyRegion AS b ON a.cb_address_postcode = b.cb_address_postcode

IF EXISTS (SELECT top 1 row_id FROM DMP_accounts_attributes_SAM_No_account WHERE connurbation IS NULL )
BEGIN
    UPDATE      DMP_accounts_attributes_SAM_No_account
    SET connurbation =  COALESCE(barb_desc_itv, 'UNKNOWN')
    FROM DMP_accounts_attributes_SAM_No_account AS   a
    JOIN sk_prod.BARB_TV_regions AS b ON a.cb_address_postcode = b.cb_address_postcode
    WHERE connurbation IS NULL
END

DROP TABLE SkyRegion
COMMIT

COMMIT


CREATE VIEW DMP_accounts_attributes_Consolidated
AS
SELECT 
     COALESCE(samprofileid, 0) sam_profile_id
	, a.*
	, b.current_age
    , h_mosaic_uk_group
    , h_household_composition
    , cust_gender
    , social_class
    , Connurbation
    , sky_go_reg
    , Sky_bet_status
    , Primary_box_desc
    , type_of_box
    , VOD_flag
    , BB_exchange_status
    , MDU_enabled
    , MDU_property
    , cable_postcode
FROM DMP_accounts_attributes_1 AS a
JOIN DMP_accounts_attributes_2 AS b ON a.account_number = b.account_number
LEFT JOIN DMP_accounts_attributes_samID      AS c ON a.account_number = c.account_number AND c.account_number is not null 
UNION
SELECT
sam_profile_id
,row_id
,'xx' -- account number
,COALESCE(cb_key_household,0)
,COALESCE(cb_address_postcode,'U')
,'U'    -- cust_email_allowed
,'U'    -- cust_postal_mail_allowed
,'U'    -- cust_telephone_contact_allowed
,Virgin
,BT_model_decile
,'1900-01-01'    -- DTV_end_dt
,'1900-01-01'    -- BB_end_dt
,'None'     -- Broadband_package
,Renter_flag
,p_hms_verified_flag
,0    -- Movies_Downgrade_12M
,0    -- Sports_Downgrade_12M
,skyfibre_enabled
,COALESCE (skyfibre_estimated_enabled_date,'1900-01-01')
,CQM_model_score
,0    -- ppv_viewed_90D
,0    -- Simple Segmentation
,'U'    -- DTV_account_status
,'U'    -- DTV_Package
,'U'    -- Telephone
,'U'    -- Age
,h_mosaic_uk_group
,h_household_composition
,'U' gender
,social_class
,Connurbation
,'U'
,'U'
,'U'
,'U'    -- type of box
,0 VOD_flag
,BB_exchange_status
,MDU_enabled
,MDU_property
,cable_postcode
FROM DMP_accounts_attributes_SAM_No_account

COMMIT







