/* SELECT Account_number
	, call_type
	, initial_sct_grouping
	,
INTO inbound_calls_mcKinsey
FROM calls_details
INNER JOIN valerl.offer_details_mckinsey_v2 c ON a.account_number = c.account_number
WHERE CAST(call_date AS DATE) BETWEEN '2012-06-27' AND '2016-06-30';
INTO MCKINSEY_TA_HIST 
FROM
FROM CITEAM.View_CUST_CALLS_HIST
WHERE event_dt BETWEEN '2014-06-27' AND '2016-06-30' AND typeofevent = 'TA' AND DTH = 1;

GRANT ALL ON MCKINSEY_TA_HIST  TO noryd;

COMMIT;
*/

/******************************************************************************************************
	   TA_HIST ---- To confirm with Dinesh
	   
******************************************************************************** */
	   
SELECT account_number
	, cast(NULL AS VARCHAR(50)) AS cb_key_household
	, event_dt
	, total_calls
	, saves
	, typeofevent
	, DTV AS DTH
	, BB
	, Skytalk
	, WLR
	, HD
	, MS
	, Sports
	, Movies
	, cast(NULL AS INT) AS SKY_Q
	, cast(NULL AS INT) AS HD_PACK
	, cast(NULL AS INT) AS SGE
	, cast(NULL AS VARCHAR(50)) AS Bundle
	, bb_package
	, bb_package_group
	, Skytalk_package
	, skytalk_package_group
	, cast(NULL AS DATE) AS DTH_act_date
	, cast(NULL AS DATE) AS dth_fisrt_act_date
	, cast(NULL AS DATE) AS dth_last_act_date
	, cast(NULL AS INT) AS tenure_days
	, cast(NULL AS VARCHAR(50)) AS DTH_subscription_id
	, cast(NULL AS VARCHAR(50)) AS BB_subscription_id
	, cast(NULL AS VARCHAR(50)) AS LR_subscription_id
	, cast(NULL AS VARCHAR(50)) AS TALK_subscription_id
	, cast(NULL AS VARCHAR(50)) AS SGE_subscription_id
	, cast(NULL AS VARCHAR(50)) AS HD_subscription_id
	, cast(NULL AS VARCHAR(50)) AS MS_subscription_id
	, cast(NULL AS VARCHAR(50)) AS HD_PACK_subscription_id
	, cast(NULL AS VARCHAR(50)) AS standalonesurcharge_subscription_id
	, cast(NULL AS VARCHAR(50)) AS ORDER_CREATED_BY
	, cast(NULL AS VARCHAR(50)) AS ORDER_COMMUNICATION_TYPE
	, cast(NULL AS VARCHAR(50)) AS ORDER_SALE_TYPE
	, cast(NULL AS VARCHAR(50)) AS ORDER_STATUS
	, cast(NULL AS VARCHAR(50)) AS RTM_LEVEL_1
	, cast(NULL AS VARCHAR(50)) AS RTM_LEVEL_2
	, cast(NULL AS VARCHAR(50)) AS RTM_LEVEL_3
	, cast(NULL AS VARCHAR(50)) AS SERVICE_CALL_TYPE
	, cast(NULL AS VARCHAR(50)) AS SKILL_GROUP
	, cast(NULL AS VARCHAR(50)) AS ORDER_TYPE
	, cast(0 AS INT) AS Sports_ANY_ADDED
	, cast(0 AS INT) AS MOVIES_ANY_ADDED
	, cast(0 AS INT) AS SPORTS_ADDED
	, cast(0 AS INT) AS MOVIES_ADDED
	, cast(0 AS INT) AS SINGLE_SPORTS_ADDED
	, cast(0 AS INT) AS DUAL_SPORTS_ADDED
	, cast(0 AS INT) AS SINGLE_MOVIES_ADDED
	, cast(0 AS INT) AS DUAL_MOVIES_ADDED
	, cast(0 AS INT) AS FAMILY_ADDED
	, cast(0 AS INT) AS VARIETY_ADDED
	, cast(0 AS INT) AS ORIGINAL_ADDED
	, cast(0 AS INT) AS SKYQ_ADDED
	, cast(0 AS INT) AS HD_LEGACY_ADDED
	, cast(0 AS INT) AS HD_BASIC_ADDED
	, cast(0 AS INT) AS HD_PREMIUM_ADDED
	, cast(0 AS INT) AS MULTISCREEN_ADDED
	, cast(0 AS INT) AS MULTISCREEN_PLUS_ADDED
	, cast(0 AS INT) AS SKY_PLUS_ADDED
	, cast(0 AS INT) AS SKY_GO_EXTRA_ADDED
	, cast(0 AS INT) AS NOW_TV_ADDED
	, cast(0 AS INT) AS BB_ANY_ADD
	, cast(0 AS INT) AS BB_UNLIMITED_ADDED
	, cast(0 AS INT) AS BB_LITE_ADDED
	, cast(0 AS INT) AS BB_FIBRE_CAP_ADDED
	, cast(0 AS INT) AS BB_FIBRE_UNLIMITED_ADDED
	, cast(0 AS INT) AS BB_FIBRE_UNLIMITED_PRO_ADDED
	, cast(0 AS INT) AS TALK_ANY_ADD
	, cast(0 AS INT) AS TALKU_ADDED
	, cast(0 AS INT) AS TALKW_ADDED
	, cast(0 AS INT) AS TALKF_ADDED
	, cast(0 AS INT) AS TALKA_ADDED
	, cast(0 AS INT) AS TALKP_ADDED
	, cast(0 AS INT) AS TALKO_ADDED
	, cast(0 AS INT) AS Sports_ANY_REMOVED
	, cast(0 AS INT) AS MOVIES_ANY_REMOVED
	, cast(0 AS INT) AS SPORTS_REMOVED
	, cast(0 AS INT) AS MOVIES_REMOVED
	, cast(0 AS INT) AS SINGLE_SPORTS_REMOVED
	, cast(0 AS INT) AS DUAL_SPORTS_REMOVED
	, cast(0 AS INT) AS SINGLE_MOVIES_REMOVED
	, cast(0 AS INT) AS DUAL_MOVIES_REMOVED
	, cast(0 AS INT) AS FAMILY_REMOVED
	, cast(0 AS INT) AS VARIETY_REMOVED
	, cast(0 AS INT) AS ORIGINAL_REMOVED
	, cast(0 AS INT) AS SKYQ_REMOVED
	, cast(0 AS INT) AS HD_LEGACY_REMOVED
	, cast(0 AS INT) AS HD_BASIC_REMOVED
	, cast(0 AS INT) AS HD_PREMIUM_REMOVED
	, cast(0 AS INT) AS MULTISCREEN_REMOVED
	, cast(0 AS INT) AS MULTISCREEN_PLUS_REMOVED
	, cast(0 AS INT) AS SKY_PLUS_REMOVED
	, cast(0 AS INT) AS SKY_GO_EXTRA_REMOVED
	, cast(0 AS INT) AS NOW_TV_REMOVED
	, cast(0 AS INT) AS BB_ANY_REMOVED
	, cast(0 AS INT) AS BB_UNLIMITED_REMOVED
	, cast(0 AS INT) AS BB_LITE_REMOVED
	, cast(0 AS INT) AS BB_FIBRE_CAP_REMOVED
	, cast(0 AS INT) AS BB_FIBRE_UNLIMITED_REMOVED
	, cast(0 AS INT) AS BB_FIBRE_UNLIMITED_PRO_REMOVED
	, cast(0 AS INT) AS TALK_ANY_REMOVED
	, cast(0 AS INT) AS TALKU_REMOVED
	, cast(0 AS INT) AS TALKW_REMOVED
	, cast(0 AS INT) AS TALKF_REMOVED
	, cast(0 AS INT) AS TALKA_REMOVED
	, cast(0 AS INT) AS TALKP_REMOVED
	, cast(0 AS INT) AS TALKO_REMOVED
	, cast(0 AS INT) AS PRE_ORDER_TOTAL_PREMIUMS
	, cast(0 AS INT) AS PRE_ORDER_TOTAL_SPORTS
	, cast(0 AS INT) AS PRE_ORDER_TOTAL_MOVIES
	, cast(0 AS INT) AS PRE_ORDER_DUAL_SPORTS
	, cast(0 AS INT) AS PRE_ORDER_SINGLE_SPORTS
	, cast(0 AS INT) AS PRE_ORDER_DUAL_MOVIES
	, cast(0 AS INT) AS PRE_ORDER_SINGLE_MOVIES
	, cast(NULL AS INT) AS POST_ORDER_TOTAL_PREMIUMS
	, cast(NULL AS INT) AS POST_ORDER_TOTAL_SPORTS
	, cast(NULL AS INT) AS POST_ORDER_TOTAL_MOVIES
	, cast(NULL AS INT) AS POST_ORDER_DUAL_SPORTS
	, cast(NULL AS INT) AS POST_ORDER_SINGLE_SPORTS
	, cast(NULL AS INT) AS POST_ORDER_DUAL_MOVIES
	, cast(NULL AS INT) AS POST_ORDER_SINGLE_MOVIES
	, cast(0 AS INT) AS DTH_cancellation
	, CAST(NULL AS VARCHAR(100)) 	AS postcode
	, cast(0 AS INT) 				AS fibre
	, cast(0 AS INT) 				AS Cable
INTO MCKINSEY_TA_HIST 
FROM CITEAM.View_CUST_CALLS_HIST
WHERE  event_dt between '2014-02-15' AND '2017-03-16' 
	AND typeofevent='TA' 
	AND DTH=1
	AND account_number IS NOT NULL;



CREATE HG INDEX id1 ON MCKINSEY_TA_HIST (account_number);
CREATE DATE INDEX id2 ON MCKINSEY_TA_HIST  (event_dt);
CREATE HG   INDEX id3 ON MCKINSEY_TA_HIST  (cb_key_household);
GRANT ALL ON MCKINSEY_TA_HIST  to noryd,vespa_group_low_security, rka07, citeam ;
COMMIT;

-- 6,971,543 Row(s) affected
--------------------------------------------------------
UPDATE MCKINSEY_TA_HIST 
SET a.cb_key_household = cast(b.cb_key_household AS VARCHAR(50))
FROM MCKINSEY_TA_HIST  a
INNER JOIN cust_single_account_view b ON a.account_number = b.account_number;

--------------------------------------------------------
UPDATE MCKINSEY_TA_HIST 
SET a.ORDER_CREATED_BY = b.ORDER_CREATED_BY
	, a.ORDER_COMMUNICATION_TYPE = b.ORDER_COMMUNICATION_TYPE
	, a.ORDER_SALE_TYPE = b.ORDER_SALE_TYPE
	, a.ORDER_STATUS = b.ORDER_STATUS
	, a.RTM_LEVEL_1 = b.RTM_LEVEL_1
	, a.RTM_LEVEL_2 = b.RTM_LEVEL_2
	, a.RTM_LEVEL_3 = b.RTM_LEVEL_3
	, a.SERVICE_CALL_TYPE = b.SERVICE_CALL_TYPE
	, a.SKILL_GROUP = b.SKILL_GROUP
	, a.ORDER_TYPE = b.ORDER_TYPE
	, a.SPORTS_ADDED = b.SPORTS_ADDED
	, a.MOVIES_ADDED = b.MOVIES_ADDED
	, a.SINGLE_SPORTS_ADDED = b.SINGLE_SPORTS_ADDED
	, a.DUAL_SPORTS_ADDED = b.DUAL_SPORTS_ADDED
	, a.SINGLE_MOVIES_ADDED = b.SINGLE_MOVIES_ADDED
	, a.DUAL_MOVIES_ADDED = b.DUAL_MOVIES_ADDED
	, a.FAMILY_ADDED = b.FAMILY_ADDED
	, a.VARIETY_ADDED = b.VARIETY_ADDED
	, a.ORIGINAL_ADDED = b.ORIGINAL_ADDED
	, a.SKYQ_ADDED = b.SKYQ_ADDED
	, a.HD_LEGACY_ADDED = b.HD_LEGACY_ADDED
	, a.HD_BASIC_ADDED = b.HD_BASIC_ADDED
	, a.HD_PREMIUM_ADDED = b.HD_PREMIUM_ADDED
	, a.MULTISCREEN_ADDED = b.MULTISCREEN_ADDED
	, a.MULTISCREEN_PLUS_ADDED = b.MULTISCREEN_PLUS_ADDED
	, a.SKY_PLUS_ADDED = b.SKY_PLUS_ADDED
	, a.SKY_GO_EXTRA_ADDED = b.SKY_GO_EXTRA_ADDED
	, a.NOW_TV_ADDED = b.NOW_TV_ADDED
	, a.BB_ANY_ADD = CASE WHEN (b.BB_UNLIMITED_ADDED = 1 OR b.BB_LITE_ADDED = 1 OR b.BB_FIBRE_CAP_ADDED = 1 OR b.BB_FIBRE_UNLIMITED_ADDED = 1 OR b.BB_FIBRE_UNLIMITED_PRO_ADDED = 1) THEN 1 ELSE 0 END
	, a.BB_UNLIMITED_ADDED = b.BB_UNLIMITED_ADDED
	, a.BB_LITE_ADDED = b.BB_LITE_ADDED
	, a.BB_FIBRE_CAP_ADDED = b.BB_FIBRE_CAP_ADDED
	, a.BB_FIBRE_UNLIMITED_ADDED = b.BB_FIBRE_UNLIMITED_ADDED
	, a.BB_FIBRE_UNLIMITED_PRO_ADDED = b.BB_FIBRE_UNLIMITED_PRO_ADDED
	, a.TALK_ANY_ADD = CASE WHEN (b.TALKU_ADDED = 1 OR b.TALKW_ADDED = 1 OR b.TALKF_ADDED = 1 OR b.TALKA_ADDED = 1 OR b.TALKP_ADDED = 1 OR b.TALKO_ADDED = 1) THEN 1 ELSE 0 END
	, a.TALKU_ADDED = b.TALKU_ADDED
	, a.TALKW_ADDED = b.TALKW_ADDED
	, a.TALKF_ADDED = b.TALKF_ADDED
	, a.TALKA_ADDED = b.TALKA_ADDED
	, a.TALKP_ADDED = b.TALKP_ADDED
	, a.TALKO_ADDED = b.TALKO_ADDED
	, a.SPORTS_REMOVED = b.SPORTS__REMOVED
	, a.MOVIES_REMOVED = b.MOVIES_REMOVED
	, a.SINGLE_SPORTS_REMOVED = b.SINGLE_SPORTS_REMOVED
	, a.DUAL_SPORTS_REMOVED = b.DUAL_SPORTS_REMOVED
	, a.SINGLE_MOVIES_REMOVED = b.SINGLE_MOVIES_REMOVED
	, a.DUAL_MOVIES_REMOVED = b.DUAL_MOVIES_REMOVED
	, a.FAMILY_REMOVED = b.FAMILY_REMOVED
	, a.VARIETY_REMOVED = b.VARIETY_REMOVED
	, a.ORIGINAL_REMOVED = b.ORIGINAL_REMOVED
	, a.SKYQ_REMOVED = b.SKYQ_REMOVED
	, a.HD_LEGACY_REMOVED = b.HD_LEGACY_REMOVED
	, a.HD_BASIC_REMOVED = b.HD_BASIC_REMOVED
	, a.HD_PREMIUM_REMOVED = b.HD_PREMIUM_REMOVED
	, a.MULTISCREEN_REMOVED = b.MULTISCREEN_REMOVED
	, a.MULTISCREEN_PLUS_REMOVED = b.MULTISCREEN_PLUS_REMOVED
	, a.SKY_PLUS_REMOVED = b.SKY_PLUS_REMOVED
	, a.SKY_GO_EXTRA_REMOVED = b.SKY_GO_EXTRA_REMOVED
	, a.NOW_TV_REMOVED = b.NOW_TV_REMOVED
	, a.BB_ANY_REMOVED = CASE WHEN (b.BB_UNLIMITED_REMOVED = 1 OR b.BB_LITE_REMOVED = 1 OR b.BB_FIBRE_CAP_REMOVED = 1 OR b.BB_FIBRE_UNLIMITED_REMOVED = 1 OR b.BB_FIBRE_UNLIMITED_PRO_REMOVED = 1) THEN 1 ELSE 0 END
	, a.BB_UNLIMITED_REMOVED = b.BB_UNLIMITED_REMOVED
	, a.BB_LITE_REMOVED = b.BB_LITE_REMOVED
	, a.BB_FIBRE_CAP_REMOVED = b.BB_FIBRE_CAP_REMOVED
	, a.BB_FIBRE_UNLIMITED_REMOVED = b.BB_FIBRE_UNLIMITED_REMOVED
	, a.BB_FIBRE_UNLIMITED_PRO_REMOVED = b.BB_FIBRE_UNLIMITED_PRO_REMOVED
	, a.TALK_ANY_REMOVED = CASE WHEN (b.TALKU_REMOVED = 1 OR b.TALKW_REMOVED = 1 OR b.TALKF_REMOVED = 1 OR b.TALKA_REMOVED = 1 OR b.TALKP_REMOVED = 1 OR b.TALKO_REMOVED = 1) THEN 1 ELSE 0 END
	, a.TALKU_REMOVED = b.TALKU_REMOVED
	, a.TALKW_REMOVED = b.TALKW_REMOVED
	, a.TALKF_REMOVED = b.TALKF_REMOVED
	, a.TALKA_REMOVED = b.TALKA_REMOVED
	, a.TALKP_REMOVED = b.TALKP_REMOVED
	, a.TALKO_REMOVED = b.TALKO_REMOVED
	, a.PRE_ORDER_TOTAL_PREMIUMS = b.PRE_ORDER_TOTAL_PREMIUMS
	, a.PRE_ORDER_TOTAL_SPORTS = b.PRE_ORDER_TOTAL_SPORTS
	, a.PRE_ORDER_TOTAL_MOVIES = b.PRE_ORDER_TOTAL_MOVIES
	, a.PRE_ORDER_DUAL_SPORTS = b.PRE_ORDER_DUAL_SPORTS
	, a.PRE_ORDER_SINGLE_SPORTS = b.PRE_ORDER_SINGLE_SPORTS
	, a.PRE_ORDER_DUAL_MOVIES = b.PRE_ORDER_DUAL_MOVIES
	, a.PRE_ORDER_SINGLE_MOVIES = b.PRE_ORDER_SINGLE_MOVIES
	, a.POST_ORDER_TOTAL_PREMIUMS = b.POST_ORDER_TOTAL_PREMIUMS
	, a.POST_ORDER_TOTAL_SPORTS = b.POST_ORDER_TOTAL_SPORTS
	, a.POST_ORDER_TOTAL_MOVIES = b.POST_ORDER_TOTAL_MOVIES
	, a.POST_ORDER_DUAL_SPORTS = b.POST_ORDER_DUAL_SPORTS
	, a.POST_ORDER_SINGLE_SPORTS = b.POST_ORDER_SINGLE_SPORTS
	, a.POST_ORDER_DUAL_MOVIES = b.POST_ORDER_DUAL_MOVIES
	, a.POST_ORDER_SINGLE_MOVIES = b.POST_ORDER_SINGLE_MOVIES
FROM MCKINSEY_TA_HIST  A
INNER JOIN CITEAM.DM_ORDERS b ON a.account_number = b.account_number AND a.event_dt = b.ORDER_DT;



UPDATE MCKINSEY_TA_HIST 
SET a.postcode = upper(b.cb_address_postcode)
FROM MCKINSEY_TA_HIST  a
INNER JOIN cust_single_account_view b ON a.account_number = b.account_number;

--###FIBRE
UPDATE MCKINSEY_TA_HIST 
SET a.fibre = 1
FROM MCKINSEY_TA_HIST  a
INNER JOIN (
	SELECT a.event_dt
		, a.postcode
	FROM MCKINSEY_TA_HIST  a
	INNER JOIN BT_FIBRE_POSTCODE AS BFP ON REPLACE(a.postcode, ' ', '') = upper(REPLACE(BFP.cb_address_postcode, ' ', '')) AND BFP.fibre_enabled_perc >= 75 AND BFP.first_fibre_enabled_date <= a.event_dt
	GROUP BY a.event_dt
		, a.postcode
	) AS b ON a.event_dt = b.event_dt AND a.postcode = b.postcode;

--Cable Area
UPDATE MCKINSEY_TA_HIST 
SET BASE.Cable = CASE WHEN COALESCE(lower(bb.cable_postcode), 'n') = 'y' THEN 1 ELSE 0 END
FROM MCKINSEY_TA_HIST  AS BASE
LEFT JOIN broadband_postcode_exchange AS bb ON REPLACE(ISNULL(BASE.postcode, ''), ' ', '') = upper(replace(bb.cb_address_postcode, ' ', ''));

--tenure
UPDATE MCKINSEY_TA_HIST 
SET dth_fisrt_act_date = b.dt
	, dth_last_act_date = b.dt1
FROM MCKINSEY_TA_HIST  a
INNER JOIN (
	SELECT a.account_number
		, a.event_dt
		, min(effective_from_dt) AS dt
		, max(CASE WHEN prev_status_code IN ('PO', 'SC') THEN effective_from_dt ELSE NULL END) AS dt1
	FROM MCKINSEY_TA_HIST  a
	INNER JOIN cust_subs_hist b ON a.account_number = b.account_number AND b.subscription_sub_type = 'DTV Primary Viewing' 
			AND b.effective_from_dt <= a.event_dt 
			AND b.status_code = 'AC' 
			AND b.status_code_changed = 'Y'
	GROUP BY a.account_number
		, a.event_dt
	) AS b ON a.account_number = b.account_number AND a.event_dt = b.event_dt;

UPDATE MCKINSEY_TA_HIST 
SET DTH_act_date = CASE WHEN dth_last_act_date IS NULL THEN dth_fisrt_act_date ELSE dth_last_act_date END
	, tenure_days = (event_dt - DTH_act_date);

--bundle
UPDATE MCKINSEY_TA_HIST 
SET Bundle = CASE WHEN UPPER(b.current_short_description) LIKE '%1M1024%' THEN 'SKY Q' WHEN UPPER(b.current_product_description) LIKE 'VARIETY%' THEN 'Variety' WHEN UPPER(b.current_product_description) LIKE 'ORIGINAL%' THEN 'Original' WHEN UPPER(b.current_product_description) LIKE 'FAMILY%' THEN 'Family' WHEN UPPER(b.current_product_description) LIKE '%KID%' OR UPPER(b.current_product_description) LIKE '%SKY WORLD%' OR UPPER(b.current_product_description) LIKE '%MIX%' THEN 'Kids,Mix,World' ELSE 'Other' END
FROM MCKINSEY_TA_HIST  AS a
INNER JOIN (
	SELECT csh.current_product_description
		, csh.account_number
		, base.event_dt
		, csh.current_short_description
	FROM MCKINSEY_TA_HIST  AS base
	INNER JOIN cust_subs_hist AS csh ON csh.account_number = base.account_number 
							AND csh.effective_from_dt <= event_dt - 1 
							AND csh.effective_to_dt >= event_dt - 1 
							AND csh.effective_to_dt > csh.effective_from_dt 
							AND csh.subscription_sub_type = 'DTV Primary Viewing' 
							AND csh.status_code IN ('AC', 'AB', 'PC')	) AS b ON a.account_number = b.account_number AND a.event_dt = b.event_dt;

--subscripionid and product holding
UPDATE MCKINSEY_TA_HIST 
SET HD_PACK = 1
	, HD_PACK_subscription_id = subscription_id
FROM MCKINSEY_TA_HIST  AS BASE
INNER JOIN (
	SELECT CSH.account_number
		, event_dt
		, CSH.subscription_id
	FROM cust_subs_hist AS CSH
	INNER JOIN MCKINSEY_TA_HIST  AS BASE ON BASE.account_number = CSH.account_number
	WHERE csh.subscription_sub_type = 'HD Pack' 
			AND csh.status_code IN ('AC', 'AB', 'PC') 
			AND CSH.status_code_changed = 'Y' 
			AND csh.effective_from_dt <= event_dt - 1 
			AND csh.effective_to_dt >= event_dt - 1 
			AND csh.effective_from_dt < effective_to_dt
	GROUP BY CSH.account_number
		, event_dt
		, CSH.subscription_id
	) AS MR ON MR.account_number = BASE.account_number AND MR.event_dt = BASE.event_dt;

UPDATE MCKINSEY_TA_HIST 
SET SGE = 1
	, SGE_subscription_id = subscription_id
FROM MCKINSEY_TA_HIST  AS BASE
INNER JOIN (
	SELECT CSH.account_number
		, event_dt
		, CSH.subscription_id
	FROM cust_subs_hist AS CSH
	INNER JOIN MCKINSEY_TA_HIST  AS BASE ON BASE.account_number = CSH.account_number
	WHERE csh.subscription_sub_type = 'Sky Go Extra' 
				AND CSH.subscription_type = 'A-LA-CARTE'
				AND csh.status_code IN ('AC', 'AB', 'PC') 
				AND CSH.status_code_changed = 'Y' 
				AND csh.effective_from_dt <= event_dt - 1 
				AND csh.effective_to_dt >= event_dt - 1 
				AND csh.effective_from_dt < effective_to_dt
	GROUP BY CSH.account_number
		, event_dt
		, CSH.subscription_id
	) AS MR ON MR.account_number = BASE.account_number AND MR.event_dt = BASE.event_dt;

UPDATE MCKINSEY_TA_HIST 
SET DTH_subscription_id = subscription_id
FROM MCKINSEY_TA_HIST  AS BASE
INNER JOIN (
	SELECT CSH.account_number
		, event_dt
		, CSH.subscription_id
	FROM cust_subs_hist AS CSH
	INNER JOIN MCKINSEY_TA_HIST  AS BASE ON BASE.account_number = CSH.account_number
	WHERE csh.subscription_sub_type = 'DTV Extra Subscription' 
				AND csh.status_code IN ('AC', 'AB', 'PC') 
				AND CSH.status_code_changed = 'Y' 
				AND csh.effective_from_dt <= event_dt - 1 
				AND csh.effective_to_dt >= event_dt - 1 
				AND csh.effective_from_dt < effective_to_dt
	GROUP BY CSH.account_number
		, event_dt
		, CSH.subscription_id
	) AS MR ON MR.account_number = BASE.account_number AND MR.event_dt = BASE.event_dt;

UPDATE MCKINSEY_TA_HIST 
SET BB_subscription_id = subscription_id
FROM MCKINSEY_TA_HIST  AS BASE
INNER JOIN (
	SELECT CSH.account_number
		, event_dt
		, CSH.subscription_id
	FROM cust_subs_hist AS CSH
	INNER JOIN MCKINSEY_TA_HIST  AS BASE ON BASE.account_number = CSH.account_number
	WHERE csh.subscription_sub_type = 'Broadband DSL Line' 
			AND csh.status_code IN ('AC', 'AB', 'PC', 'PT', 'CF', 'BCRQ') 
			AND CSH.status_code_changed = 'Y' 
			AND csh.effective_from_dt <= event_dt - 1 
			AND csh.effective_to_dt >= event_dt - 1 
			AND csh.effective_from_dt < effective_to_dt
	GROUP BY CSH.account_number
		, event_dt
		, CSH.subscription_id
	) AS MR ON MR.account_number = BASE.account_number AND MR.event_dt = BASE.event_dt;

UPDATE MCKINSEY_TA_HIST 
SET LR_subscription_id = subscription_id
FROM MCKINSEY_TA_HIST  AS BASE
INNER JOIN (
	SELECT CSH.account_number
		, event_dt
		, CSH.subscription_id
	FROM cust_subs_hist AS CSH
	INNER JOIN MCKINSEY_TA_HIST  AS BASE ON BASE.account_number = CSH.account_number
	WHERE csh.subscription_sub_type = 'SKY TALK LINE RENTAL' 
			AND csh.status_code IN ('A', 'CRQ', 'R', 'BCRQ') 
			AND CSH.status_code_changed = 'Y' 
			AND csh.effective_from_dt <= event_dt - 1 
			AND csh.effective_to_dt >= event_dt - 1 
			AND csh.effective_from_dt < effective_to_dt
	GROUP BY CSH.account_number
		, event_dt
		, CSH.subscription_id
	) AS MR ON MR.account_number = BASE.account_number AND MR.event_dt = BASE.event_dt;

UPDATE MCKINSEY_TA_HIST 
SET TALK_subscription_id = subscription_id
FROM MCKINSEY_TA_HIST  AS BASE
INNER JOIN (
	SELECT CSH.account_number
		, event_dt
		, CSH.subscription_id
	FROM cust_subs_hist AS CSH
	INNER JOIN MCKINSEY_TA_HIST  AS BASE ON BASE.account_number = CSH.account_number
	WHERE csh.subscription_sub_type = 'SKY TALK SELECT' AND csh.status_code IN ('A', 'PC', 'FBP', 'RI', 'FBI', 'BCRQ') 
			AND CSH.status_code_changed = 'Y' 
			AND csh.effective_from_dt <= event_dt - 1 AND csh.effective_to_dt >= event_dt - 1 AND csh.effective_from_dt < effective_to_dt
	GROUP BY CSH.account_number
		, event_dt
		, CSH.subscription_id
	) AS MR ON MR.account_number = BASE.account_number AND MR.event_dt = BASE.event_dt;

UPDATE MCKINSEY_TA_HIST 
SET HD_subscription_id = subscription_id
FROM MCKINSEY_TA_HIST  AS BASE
INNER JOIN (
	SELECT CSH.account_number
		, event_dt
		, CSH.subscription_id
	FROM cust_subs_hist AS CSH
	INNER JOIN MCKINSEY_TA_HIST  AS BASE ON BASE.account_number = CSH.account_number
	WHERE csh.subscription_sub_type = 'DTV HD' AND csh.status_code IN ('AC', 'AB', 'PC') 
			AND CSH.status_code_changed = 'Y' 
			AND csh.effective_from_dt <= event_dt - 1 AND csh.effective_to_dt >= event_dt - 1 AND csh.effective_from_dt < effective_to_dt
	GROUP BY CSH.account_number
		, event_dt
		, CSH.subscription_id
	) AS MR ON MR.account_number = BASE.account_number AND MR.event_dt = BASE.event_dt;

UPDATE MCKINSEY_TA_HIST 
SET MS_subscription_id = subscription_id
FROM MCKINSEY_TA_HIST  AS BASE
INNER JOIN (
	SELECT CSH.account_number
		, event_dt
		, CSH.subscription_id
	FROM cust_subs_hist AS CSH
	INNER JOIN MCKINSEY_TA_HIST  AS BASE ON BASE.account_number = CSH.account_number
	WHERE csh.subscription_sub_type = 'DTV Extra Subscription' AND csh.status_code IN ('AC', 'AB', 'PC') AND CSH.status_code_changed = 'Y' 
	AND csh.effective_from_dt <= event_dt - 1 AND csh.effective_to_dt >= event_dt - 1 AND csh.effective_from_dt < effective_to_dt
	GROUP BY CSH.account_number
		, event_dt
		, CSH.subscription_id
	) AS MR ON MR.account_number = BASE.account_number AND MR.event_dt = BASE.event_dt;

UPDATE MCKINSEY_TA_HIST 
SET standalonesurcharge_subscription_id = subscription_id
FROM MCKINSEY_TA_HIST  AS BASE
INNER JOIN (
	SELECT CSH.account_number
		, event_dt
		, CSH.subscription_id
	FROM cust_subs_hist AS CSH
	INNER JOIN MCKINSEY_TA_HIST  AS BASE ON BASE.account_number = CSH.account_number
	WHERE csh.subscription_sub_type = 'STANDALONESURCHARGE' AND csh.status_code IN ('AC', 'AB', 'PC') 
			AND CSH.status_code_changed = 'Y' 
			AND csh.effective_from_dt <= event_dt - 1 
			AND csh.effective_to_dt >= event_dt - 1 
			AND csh.effective_from_dt < effective_to_dt
	GROUP BY CSH.account_number
		, event_dt
		, CSH.subscription_id
	) AS MR ON MR.account_number = BASE.account_number AND MR.event_dt = BASE.event_dt;

UPDATE MCKINSEY_TA_HIST 
SET DTH_cancellation = 1
FROM MCKINSEY_TA_HIST  AS BASE
INNER JOIN (
	SELECT CSH.account_number
		, event_dt
		, CSH.subscription_id
	FROM cust_subs_hist AS CSH
	INNER JOIN MCKINSEY_TA_HIST  AS BASE ON BASE.account_number = CSH.account_number
	WHERE csh.subscription_sub_type = 'DTV Extra Subscription' AND csh.status_code IN ('PC') AND csh.prev_status_code IN ('AC', 'AB') AND CSH.status_code_changed = 'Y' AND csh.effective_from_dt = event_dt AND csh.effective_to_dt > event_dt
	GROUP BY CSH.account_number
		, event_dt
		, CSH.subscription_id
	) AS MR ON MR.account_number = BASE.account_number AND MR.event_dt = BASE.event_dt;

-----------------------------------------------------------------------------------------------------
--product lapse tenure
ALTER TABLE MCKINSEY_TA_HIST  ADD (
	sports_UPgrade_date DATE
	, movies_UPgrade_date DATE
	, sports_latest_act_date DATE
	, movies_latest_act_date DATE
	, sports_downgrade_date DATE
	, movies_downgrade_date DATE
	, MS_first_act_date DATE
	, MS_latest_act_date DATE
	, MS_churn_dt DATE
	, HD_PACK_first_act_date DATE
	, HD_PACK_churn_dt DATE
	, HD_PACK_latest_act_date DATE
	, SKYGOE_first_act_date DATE
	, SKYGOE_latest_act_date DATE
	, SKYGOE_churn_dt DATE
	, HD_legacy_first_act_date DATE
	, HD_legacy_latest_act_date DATE
	, HD_legacy_churn_dt DATE
	, HD_base_first_act_date DATE
	, HD_base_latest_act_date DATE
	, HD_base_churn_dt DATE
	, BB_latest_act_date DATE
	, BB_first_act_date DATE
	, BB_churn_dt DATE
	, HD_act_date DATE
	, HD_churn_date DATE
	, HD_Prems_latest_act_date DATE
	, HD_prems_churn_dt DATE
	, LR_first_act_date DATE
	, LR_latest_act_date DATE
	, LR_churn_dt DATE
	, talk_first_act_date DATE
	, talk_latest_act_date DATE
	, talk_churn_dt DATE
	);

UPDATE MCKINSEY_TA_HIST 
SET sports_downgrade_date = b.dt1
	, movies_downgrade_date = b.dt2
FROM MCKINSEY_TA_HIST  a
INNER JOIN (
	SELECT a.account_number
		, a.event_dt
		, max(CASE WHEN b.typeofevent = 'SD' THEN a.event_dt ELSE NULL END) AS dt1
		, max(CASE WHEN b.typeofevent = 'MD' THEN a.event_dt ELSE NULL END) AS dt2
	FROM MCKINSEY_TA_HIST  a
	INNER JOIN citeam.view_cust_package_movements_hist b ON a.account_number = b.account_number AND b.event_dt BETWEEN a.dth_act_date AND a.event_dt AND b.typeofevent IN ('MD', 'SD')
	GROUP BY a.account_number
		, a.event_dt
	) AS b ON a.account_number = b.account_number AND a.event_dt = b.event_dt;

UPDATE MCKINSEY_TA_HIST 
SET sports_UPgrade_date = b.dt1
	, movies_UPgrade_date = b.dt2
FROM MCKINSEY_TA_HIST  a
INNER JOIN (
	SELECT a.account_number
		, a.event_dt
		, max(CASE WHEN b.typeofevent = 'SU' THEN a.event_dt ELSE NULL END) AS dt1
		, max(CASE WHEN b.typeofevent = 'MU' THEN a.event_dt ELSE NULL END) AS dt2
	FROM MCKINSEY_TA_HIST  a
	INNER JOIN citeam.view_cust_package_movements_hist b ON a.account_number = b.account_number AND b.event_dt BETWEEN a.dth_act_date AND a.event_dt AND b.typeofevent IN ('MU', 'SU')
	GROUP BY a.account_number
		, a.event_dt
	) AS b ON a.account_number = b.account_number AND a.event_dt = b.event_dt;

UPDATE MCKINSEY_TA_HIST 
SET sports_latest_act_date = CASE WHEN sports = 1 AND sports_UPgrade_date IS NOT NULL THEN sports_UPgrade_date ELSE dth_act_date END
	, movies_latest_act_date = CASE WHEN movies = 1 AND movies_UPgrade_date IS NOT NULL THEN movies_UPgrade_date ELSE dth_act_date END;

UPDATE MCKINSEY_TA_HIST 
SET BASE.MS_first_act_date = MR.MS_first_act_date
	, BASE.MS_latest_act_date = MR.MS_latest_act_date
FROM MCKINSEY_TA_HIST  AS BASE
INNER JOIN (
	SELECT CSH.account_number
		, BASE.event_dt
		, MAX(CASE WHEN CSH.prev_status_code IN ('PO', 'SC') THEN CSH.effective_from_dt ELSE NULL END) AS MS_latest_act_date -- the most recent activation date post a reinstate
		, MIN(CSH.effective_from_dt) AS MS_first_act_date -- first ever activation date
	FROM cust_subs_hist AS CSH
	INNER JOIN MCKINSEY_TA_HIST  AS BASE ON BASE.account_number = CSH.account_number
	WHERE csh.subscription_sub_type = 'DTV Extra Subscription' AND csh.status_code = 'AC' AND CSH.status_code_changed = 'Y' AND CSH.effective_from_dT < CSH.effective_to_dt AND CSH.effective_from_dt BETWEEN BASE.dth_act_date AND BASE.event_dt
	GROUP BY CSH.account_number
		, BASE.event_dt
	) AS MR ON MR.account_number = BASE.account_number AND MR.event_dt = BASE.event_dt;

UPDATE MCKINSEY_TA_HIST 
SET MS_latest_act_date = CASE WHEN MS_latest_act_date IS NULL THEN MS_first_act_date ELSE MS_latest_act_date END;

UPDATE MCKINSEY_TA_HIST 
SET MS_churn_dt = MS_churn_act_date
FROM MCKINSEY_TA_HIST  AS BASE
INNER JOIN (
	SELECT CSH.account_number
		, BASE.event_dt
		, MAX(CSH.effective_from_dt) AS MS_churn_act_date -- first ever activation date
	FROM cust_subs_hist AS CSH
	INNER JOIN MCKINSEY_TA_HIST  AS BASE ON BASE.account_number = CSH.account_number
	WHERE csh.subscription_sub_type = 'DTV Extra Subscription' AND csh.status_code IN ('PO', 'SC') AND csh.prev_status_code IN ('AC', 'AB', 'PC'
			) AND CSH.status_code_changed = 'Y' AND CSH.effective_from_dt BETWEEN MS_latest_act_date AND event_dt
	GROUP BY CSH.account_number
		, BASE.event_dt
	) AS MR ON MR.account_number = BASE.account_number AND MR.event_dt = BASE.event_dt;

-------------------------------------------C04  - SGE Start Date
UPDATE MCKINSEY_TA_HIST 
SET BASE.SKYGOE_first_act_date = MR.SKYGOE_first_act_date
	, BASE.SKYGOE_latest_act_date = MR.SKYGOE_latest_act_date
FROM MCKINSEY_TA_HIST  AS BASE
INNER JOIN (
	SELECT CSH.account_number
		, BASE.event_dt
		, MAX(CASE WHEN CSH.prev_status_code IN ('PO', 'SC') THEN CSH.effective_from_dt ELSE NULL END) AS SKYGOE_latest_act_date -- the most recent activation date post a reinstate
		, MIN(CSH.effective_from_dt) AS SKYGOE_first_act_date -- first ever activation date
	FROM cust_subs_hist AS CSH
	INNER JOIN MCKINSEY_TA_HIST  AS BASE ON BASE.account_number = CSH.account_number
	WHERE csh.subscription_sub_type = 'Sky Go Extra' AND CSH.subscription_type = 'A-LA-CARTE' AND csh.status_code = 'AC' AND CSH.effective_from_dT < CSH.effective_to_dt AND CSH.status_code_changed = 'Y' AND CSH.effective_from_dt BETWEEN dth_act_date AND event_dt
	GROUP BY CSH.account_number
		, BASE.event_dt
	) AS MR ON MR.account_number = BASE.account_number AND MR.event_dt = BASE.event_dt;

UPDATE MCKINSEY_TA_HIST 
SET SKYGOE_latest_act_date = CASE WHEN SKYGOE_latest_act_date IS NULL THEN SKYGOE_first_act_date ELSE SKYGOE_latest_act_date END;

-------------------------------------------C05  - SGE Churn Date
UPDATE MCKINSEY_TA_HIST 
SET SKYGOE_churn_dt = SKYGOE_churn_act_date
FROM MCKINSEY_TA_HIST  AS BASE
INNER JOIN (
	SELECT CSH.account_number
		, BASE.event_dt
		, MAX(CSH.effective_from_dt) AS SKYGOE_churn_act_date -- first ever activation date
	FROM cust_subs_hist AS CSH
	INNER JOIN MCKINSEY_TA_HIST  AS BASE ON BASE.account_number = CSH.account_number
	WHERE csh.subscription_sub_type = 'Sky Go Extra' AND CSH.subscription_type = 'A-LA-CARTE' AND csh.status_code IN ('PO', 'SC') AND csh.prev_status_code IN ('AC', 'AB', 'PC') 
	AND CSH.status_code_changed = 'Y' AND CSH.effective_from_dt BETWEEN SKYGOE_latest_act_date AND event_dt
	GROUP BY CSH.account_number
		, BASE.event_dt
	) AS MR ON MR.account_number = BASE.account_number AND MR.event_dt = BASE.event_dt;

-------------------------------------------C06  - HD Legacy Start Date
UPDATE MCKINSEY_TA_HIST 
SET BASE.HD_legacy_first_act_date = MR.HD_legacy_first_act_date
	, BASE.HD_legacy_latest_act_date = MR.HD_legacy_latest_act_date
FROM MCKINSEY_TA_HIST  AS BASE
INNER JOIN (
	SELECT CSH.account_number
		, BASE.event_dt
		, MAX(CASE WHEN CSH.prev_status_code IN ('PO', 'SC') THEN CSH.effective_from_dt ELSE NULL END) AS HD_legacy_latest_act_date -- the most recent activation date post a reinstate
		, MIN(CSH.effective_from_dt) AS HD_legacy_first_act_date -- first ever activation date
	FROM cust_subs_hist AS CSH
	INNER JOIN MCKINSEY_TA_HIST  AS BASE ON BASE.account_number = CSH.account_number
	WHERE csh.subscription_sub_type = 'DTV HD' AND csh.current_product_sk = 687 AND csh.status_code = 'AC' AND CSH.effective_from_dT < CSH.effective_to_dt AND CSH.status_code_changed = 'Y' AND CSH.effective_from_dt BETWEEN dth_act_date AND event_dt
	GROUP BY CSH.account_number
		, BASE.event_dt
	) AS MR ON MR.account_number = BASE.account_number AND MR.event_dt = BASE.event_dt;

UPDATE MCKINSEY_TA_HIST 
SET HD_legacy_latest_act_date = CASE WHEN HD_legacy_latest_act_date IS NULL THEN HD_legacy_first_act_date ELSE HD_legacy_latest_act_date END;

-------------------------------------------C07  - HD Legacy Churn Date
UPDATE MCKINSEY_TA_HIST 
SET HD_legacy_churn_dt = HD_legacy_churn_act_date
FROM MCKINSEY_TA_HIST  AS BASE
INNER JOIN (
	SELECT CSH.account_number
		, BASE.event_dt
		, MAX(CSH.effective_from_dt) AS HD_legacy_churn_act_date -- first ever activation date
	FROM cust_subs_hist AS CSH
	INNER JOIN MCKINSEY_TA_HIST  AS BASE ON BASE.account_number = CSH.account_number
	WHERE csh.subscription_sub_type = 'DTV HD' AND csh.current_product_sk = 687 AND csh.status_code IN ('PO', 'SC') AND csh.prev_status_code IN ('AC', 'AB', 'PC') 
	AND CSH.status_code_changed = 'Y' AND CSH.effective_from_dt BETWEEN HD_legacy_latest_act_date AND event_dt
	GROUP BY CSH.account_number
		, BASE.event_dt
	) AS MR ON MR.account_number = BASE.account_number AND MR.event_dt = BASE.event_dt;

------------------------------------------C08  - HD Basic Start Date
UPDATE MCKINSEY_TA_HIST 
SET BASE.HD_base_first_act_date = MR.HD_base_first_act_date
	, BASE.HD_base_latest_act_date = MR.HD_base_latest_act_date
FROM MCKINSEY_TA_HIST  AS BASE
INNER JOIN (
	SELECT CSH.account_number
		, BASE.event_dt
		, MAX(CASE WHEN CSH.prev_status_code IN ('PO', 'SC') THEN CSH.effective_from_dt ELSE NULL END) AS HD_base_latest_act_date -- the most recent activation date post a reinstate
		, MIN(CSH.effective_from_dt) AS HD_base_first_act_date -- first ever activation date
	FROM cust_subs_hist AS CSH
	INNER JOIN MCKINSEY_TA_HIST  AS BASE ON BASE.account_number = CSH.account_number
	WHERE csh.subscription_sub_type = 'DTV HD' AND csh.current_product_sk = 43678 AND csh.status_code = 'AC' AND CSH.effective_from_dT < CSH.effective_to_dt AND CSH.status_code_changed = 'Y' AND CSH.effective_from_dt BETWEEN dth_act_date AND event_dt
	GROUP BY CSH.account_number
		, BASE.event_dt
	) AS MR ON MR.account_number = BASE.account_number AND MR.event_dt = BASE.event_dt;

UPDATE MCKINSEY_TA_HIST 
SET HD_base_latest_act_date = CASE WHEN HD_base_latest_act_date IS NULL THEN HD_base_first_act_date ELSE HD_base_latest_act_date END;

------------------------------------------C09  - HD Basic Churn Date
UPDATE MCKINSEY_TA_HIST 
SET HD_base_churn_dt = HD_base_churn_act_date
FROM MCKINSEY_TA_HIST  AS BASE
INNER JOIN (
	SELECT CSH.account_number
		, BASE.event_dt
		, MAX(CSH.effective_from_dt) AS HD_base_churn_act_date -- first ever activation date
	FROM cust_subs_hist AS CSH
	INNER JOIN MCKINSEY_TA_HIST  AS BASE ON BASE.account_number = CSH.account_number
	WHERE csh.subscription_sub_type = 'DTV HD' AND csh.current_product_sk = 43678 AND csh.status_code IN ('PO', 'SC') AND csh.prev_status_code IN ('AC', 'AB', 'PC') AND CSH.status_code_changed = 'Y' AND CSH.effective_from_dt BETWEEN HD_base_latest_act_date AND event_dt
	GROUP BY CSH.account_number
		, BASE.event_dt
	) AS MR ON MR.account_number = BASE.account_number AND MR.event_dt = BASE.event_dt;

------------------------------------------C10  - HD Pack Start Date
UPDATE MCKINSEY_TA_HIST 
SET BASE.HD_PACK_first_act_date = MR.HD_prems_first_act_date
	, BASE.HD_PACK_latest_act_date = MR.HD_prems_latest_act_date
FROM MCKINSEY_TA_HIST  AS BASE
INNER JOIN (
	SELECT CSH.account_number
		, BASE.event_dt
		, MAX(CASE WHEN CSH.prev_status_code IN (
						'PO'
						, 'SC'
						) THEN CSH.effective_from_dt ELSE NULL END) AS HD_prems_latest_act_date -- the most recent activation date post a reinstate
		, MIN(CSH.effective_from_dt) AS HD_prems_first_act_date -- first ever activation date
	FROM cust_subs_hist AS CSH
	INNER JOIN MCKINSEY_TA_HIST  AS BASE ON BASE.account_number = CSH.account_number
	WHERE csh.subscription_sub_type = 'HD Pack' AND csh.status_code = 'AC' AND CSH.effective_from_dT < CSH.effective_to_dt AND CSH.status_code_changed = 'Y' AND CSH.effective_from_dt BETWEEN dth_act_date AND event_dt
	GROUP BY CSH.account_number
		, BASE.event_dt
	) AS MR ON MR.account_number = BASE.account_number AND MR.event_dt = BASE.event_dt;

UPDATE MCKINSEY_TA_HIST 
SET HD_PACK_latest_act_date = CASE WHEN HD_PACK_latest_act_date IS NULL THEN HD_PACK_first_act_date ELSE HD_PACK_latest_act_date END;

------------------------------------------C11  - HD Pack Churn Date
UPDATE MCKINSEY_TA_HIST 
SET HD_PACK_churn_dt = dt
FROM MCKINSEY_TA_HIST  AS BASE
INNER JOIN (
	SELECT CSH.account_number
		, BASE.event_dt
		, MAX(CSH.effective_from_dt) AS dt -- first ever activation date
	FROM cust_subs_hist AS CSH
	INNER JOIN MCKINSEY_TA_HIST  AS BASE ON BASE.account_number = CSH.account_number
	WHERE csh.subscription_sub_type = 'HD Pack' AND csh.status_code IN (
			'PO'
			, 'SC'
			) AND csh.prev_status_code IN (
			'AC'
			, 'AB'
			, 'PC'
			) AND CSH.status_code_changed = 'Y' AND CSH.effective_from_dt BETWEEN dth_act_date AND event_dt
	GROUP BY CSH.account_number
		, BASE.event_dt
	) AS MR ON MR.account_number = BASE.account_number AND MR.event_dt = BASE.event_dt;

------------------------------------------C12  - HD Date Corrects
UPDATE MCKINSEY_TA_HIST 
SET HD_act_date = CASE WHEN HD_base_latest_act_date IS NULL AND HD_legacy_latest_act_date IS NOT NULL THEN HD_legacy_latest_act_date WHEN HD_base_latest_act_date IS NOT NULL AND HD_legacy_latest_act_date IS NULL THEN HD_base_latest_act_date WHEN HD_base_latest_act_date IS NOT NULL AND HD_legacy_latest_act_date IS NOT NULL AND HD_base_churn_dt < HD_base_latest_act_date THEN HD_base_latest_act_date WHEN HD_base_latest_act_date IS NOT NULL AND HD_legacy_latest_act_date IS NOT NULL AND HD_base_churn_dt = HD_base_latest_act_date THEN HD_legacy_latest_act_date ELSE NULL END;

UPDATE MCKINSEY_TA_HIST 
SET HD_churn_date = CASE WHEN HD_base_churn_dt IS NULL AND HD_legacy_churn_dt IS NOT NULL THEN HD_legacy_churn_dt WHEN HD_base_churn_dt IS NOT NULL AND HD_legacy_churn_dt IS NULL THEN HD_base_churn_dt WHEN HD_base_churn_dt >= HD_legacy_churn_dt THEN HD_base_churn_dt WHEN HD_base_churn_dt < HD_legacy_churn_dt THEN HD_legacy_churn_dt ELSE NULL END;

UPDATE MCKINSEY_TA_HIST 
SET HD_Prems_latest_act_date = CASE WHEN HD_legacy_latest_act_date >= HD_pack_latest_act_date THEN HD_legacy_latest_act_date WHEN HD_legacy_latest_act_date < HD_pack_latest_act_date THEN HD_pack_latest_act_date WHEN HD_legacy_latest_act_date IS NOT NULL THEN HD_legacy_latest_act_date WHEN HD_pack_latest_act_date IS NOT NULL THEN HD_pack_latest_act_date ELSE NULL END;

UPDATE MCKINSEY_TA_HIST 
SET HD_prems_churn_dt = CASE WHEN HD_Pack_latest_act_date = HD_pack_latest_act_date THEN HD_pack_churn_dt WHEN HD_Pack_latest_act_date = HD_legacy_latest_act_date THEN HD_legacy_churn_dt ELSE NULL END;

------------------------------------------C14  - BB Start Date
UPDATE MCKINSEY_TA_HIST 
SET BASE.BB_first_act_date = MR.BB_first_act_date
	, BASE.bb_latest_act_date = MR.BB_latest_act_date
FROM MCKINSEY_TA_HIST  AS BASE
INNER JOIN (
	SELECT CSH.account_number
		, BASE.event_dt
		, MAX(CASE WHEN CSH.prev_status_code IN (
						'PO'
						, 'SC'
						) THEN CSH.effective_from_dt ELSE NULL END) AS BB_latest_act_date -- the most recent activation date post a reinstate
		, MIN(CSH.effective_from_dt) AS BB_first_act_date -- first ever activation date
	FROM cust_subs_hist AS CSH
	INNER JOIN MCKINSEY_TA_HIST  AS BASE ON BASE.account_number = CSH.account_number
	WHERE csh.subscription_sub_type = 'Broadband DSL Line' AND (
			csh.status_code IN (
				'AC'
				, 'AB'
				) OR (
				csh.status_code = 'PC' AND prev_status_code NOT IN (
					'?'
					, 'RQ'
					, 'AP'
					, 'UB'
					, 'BE'
					, 'PA'
					)
				) OR (csh.status_code = 'CF' AND prev_status_code = 'PC') OR (csh.status_code = 'AP' AND sale_type = 'SNS Bulk Migration')
			) AND CSH.status_code_changed = 'Y' AND CSH.effective_from_dt <= event_dt AND CSH.effective_from_dT < CSH.effective_to_dt
	GROUP BY CSH.account_number
		, BASE.event_dt
	) AS MR ON MR.account_number = BASE.account_number AND MR.event_dt = BASE.event_dt;

UPDATE MCKINSEY_TA_HIST 
SET bb_latest_act_date = CASE WHEN bb_latest_act_date IS NULL THEN BB_first_act_date ELSE bb_latest_act_date END;

------------------------------------------C15  - BB Churn Date
UPDATE MCKINSEY_TA_HIST 
SET bb_churn_dt = broadband_churn_act_date
FROM MCKINSEY_TA_HIST  AS BASE
INNER JOIN (
	SELECT CSH.account_number
		, d.event_dt
		, MAX(CSH.effective_from_dt) AS broadband_churn_act_date -- first ever activation date
	FROM cust_subs_hist AS csh
	LEFT JOIN MCKINSEY_TA_HIST  d ON csh.account_number = d.account_number
	WHERE subscription_sub_type = 'Broadband DSL Line' AND status_code_changed = 'Y' AND prev_status_code NOT IN (
			'PO'
			, 'SC'
			, 'CN'
			) AND status_code IN (
			'PO'
			, 'SC'
			, 'CN'
			) AND Status_reason <> 'Moving Home' AND CSH.effective_from_dt BETWEEN bb_latest_act_date AND event_dt
	GROUP BY CSH.account_number
		, d.event_dt
	) AS MR ON MR.account_number = BASE.account_number AND MR.event_dt = BASE.event_dt;

------------------------------------------C14  - BB Start Date
UPDATE MCKINSEY_TA_HIST 
SET BASE.LR_first_act_date = MR.BB_first_act_date
	, BASE.LR_latest_act_date = MR.BB_latest_act_date
FROM MCKINSEY_TA_HIST  AS BASE
INNER JOIN (
	SELECT CSH.account_number
		, BASE.event_dt
		, MAX(CASE WHEN CSH.prev_status_code IN ('CN') THEN CSH.effective_from_dt ELSE NULL END) AS BB_latest_act_date -- the most recent activation date post a reinstate
		, MIN(CSH.effective_from_dt) AS BB_first_act_date -- first ever activation date
	FROM cust_subs_hist AS CSH
	INNER JOIN MCKINSEY_TA_HIST  AS BASE ON BASE.account_number = CSH.account_number
	WHERE csh.subscription_sub_type = 'SKY TALK LINE RENTAL' AND csh.status_code IN ('A') AND CSH.status_code_changed = 'Y' AND CSH.effective_from_dt <= event_dt AND CSH.effective_from_dT < CSH.effective_to_dt
	GROUP BY CSH.account_number
		, BASE.event_dt
	) AS MR ON MR.account_number = BASE.account_number AND MR.event_dt = BASE.event_dt;

UPDATE MCKINSEY_TA_HIST 
SET LR_latest_act_date = CASE WHEN LR_latest_act_date IS NULL THEN LR_first_act_date ELSE LR_latest_act_date END;

------------------------------------------C15  - BB Churn Date
UPDATE MCKINSEY_TA_HIST 
SET LR_churn_dt = broadband_churn_act_date
FROM MCKINSEY_TA_HIST  AS BASE
INNER JOIN (
	SELECT CSH.account_number
		, d.event_dt
		, MAX(CSH.effective_from_dt) AS broadband_churn_act_date -- first ever activation date
	FROM cust_subs_hist AS csh
	LEFT JOIN MCKINSEY_TA_HIST  d ON csh.account_number = d.account_number
	WHERE subscription_sub_type = 'SKY TALK LINE RENTAL' AND status_code_changed = 'Y' AND prev_status_code NOT IN ('CN') AND status_code IN ('CN') AND Status_reason <> 'Moving Home' AND CSH.effective_from_dt BETWEEN LR_latest_act_date AND event_dt
	GROUP BY CSH.account_number
		, d.event_dt
	) AS MR ON MR.account_number = BASE.account_number AND MR.event_dt = BASE.event_dt;

------------------------------------------C14  - BB Start Date
UPDATE MCKINSEY_TA_HIST 
SET BASE.talk_first_act_date = MR.BB_first_act_date
	, BASE.talk_latest_act_date = MR.BB_latest_act_date
FROM MCKINSEY_TA_HIST  AS BASE
INNER JOIN (
	SELECT CSH.account_number
		, BASE.event_dt
		, MAX(CASE WHEN CSH.prev_status_code IN ('CN') THEN CSH.effective_from_dt ELSE NULL END) AS BB_latest_act_date -- the most recent activation date post a reinstate
		, MIN(CSH.effective_from_dt) AS BB_first_act_date -- first ever activation date
	FROM cust_subs_hist AS CSH
	INNER JOIN MCKINSEY_TA_HIST  AS BASE ON BASE.account_number = CSH.account_number
	WHERE csh.subscription_sub_type = 'SKY TALK SELECT' AND csh.status_code IN ('A') AND CSH.status_code_changed = 'Y' AND CSH.effective_from_dt <= event_dt AND CSH.effective_from_dT < CSH.effective_to_dt
	GROUP BY CSH.account_number
		, BASE.event_dt
	) AS MR ON MR.account_number = BASE.account_number AND MR.event_dt = BASE.event_dt;

UPDATE MCKINSEY_TA_HIST 
SET talk_latest_act_date = CASE WHEN talk_latest_act_date IS NULL THEN Talk_first_act_date ELSE talk_latest_act_date END;

------------------------------------------C15  - BB Churn Date
UPDATE MCKINSEY_TA_HIST 
SET talk_churn_dt = churn_act_date
FROM MCKINSEY_TA_HIST  AS BASE
INNER JOIN (
	SELECT CSH.account_number
		, d.event_dt
		, MAX(CSH.effective_from_dt) AS churn_act_date -- first ever activation date
	FROM cust_subs_hist AS csh
	LEFT JOIN MCKINSEY_TA_HIST  d ON csh.account_number = d.account_number
	WHERE subscription_sub_type = 'SKY TALK SELECT' AND status_code_changed = 'Y' AND prev_status_code NOT IN ('CN') AND status_code IN ('CN') AND Status_reason <> 'Moving Home' AND CSH.effective_from_dt BETWEEN talk_latest_act_date AND event_dt
	GROUP BY CSH.account_number
		, d.event_dt
	) AS MR ON MR.account_number = BASE.account_number AND MR.event_dt = BASE.event_dt;



	----------------------------------------------------------------------------
	-- TABLE 12 - VALUE calls event
	----------------------------------------------------------------------------
	/*
SELECT *
INTO PAT_family
FROM (SELECT a.account_number
,CAST (visit_dt AS DATE) AS event_dt
,shs_year_quarter
,shs_year_month
,'WEB' as event_type
,MAX(CASE WHEN page LIKE '%hd_family_pack%' THEN 1 ELSE 0 END) AS family_dg
,CAST(0 AS INT) AS Saved
,CAST(NULL AS INT) AS churn_flag
,CAST(0 AS INT) AS TV_DG
,CAST(0 AS INT) AS TV_migration
,CAST(0 AS INT) AS BB_DG
,CAST(0 AS INT) AS BB_migration
,CAST(0 AS INT) AS Talk_DG
,CAST(0 AS INT) AS Talk_migration
,CAST(NULL AS VARCHAR(100)) AS HD_movement
,CAST(NULL AS VARCHAR(100)) AS Premiums_movement
,CAST(NULL AS VARCHAR(100)) AS Bundle_movement
,CAST(NULL AS VARCHAR(100)) AS SGE_movement
,CAST(NULL AS VARCHAR(100)) AS MS_movement
,CAST(NULL AS VARCHAR(100)) AS BB_movement
,CAST(NULL AS VARCHAR(100)) AS Talk_movement
FROM williamsr.manage_raw AS a
INNER JOIN cust_subs_hist as csh ON a.account_number = csh.account_number
INNER JOIN sky_calendar AS b ON event_dt = b.Calendar_date
WHERE CAST (visit_dt AS DATE) BETWEEN @start_dt AND @end_dt
AND page  LIKE '%downgrade%' --Downgrade web page
AND csh.subscription_sub_type = 'DTV HD' AND csh.current_product_sk = 43678 --Select HD Basic that is included with family
AND csh.status_code = 'AC' --('AC','AB','PC')
AND csh.effective_from_dt <= (event_dt-1)
AND csh.effective_to_dt > (event_dt-1)
AND csh.effective_to_dt > csh.effective_from_dt
GROUP BY a.account_number
,event_dt
,shs_year_quarter
,shs_year_month
,event_type

*/
