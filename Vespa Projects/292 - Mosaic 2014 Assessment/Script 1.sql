/*
				$$$
				I$$$
				I$$$
	  $$$$$$$$ 	I$$$   $$$$$ 	 $$$  ZDD 	DDDDDDD.
	,$$$$$$$$ 	I$$$  $$$$$$$ 	$$$   ODD  ODDDZ 7DDDD
	?$$$, 		I$$$ $$$$. $$$$ $$$	= ODD DDD       NDD
	 $$$$$$$$= 	I$$$$$$$ 	$$$$.$$$  ODD +DD$ 	    +DD$
		  :$$$$~I$$$ $$$$ 	$$$$$$    ODD  DDN 	    NDD.
,. 			$$$+I$$$  $$$$ 	  $$$$=   ODD   NDDN  NDDN
	  $$$$$$$$$ I$$$   $$$$  .$$$ 	  ODD    ZDDDDDDDN
							  $$$ 				 . $DDZ
							 $$$ 				   ,NDDDDDDD
							$$$?

		CUSTOMER INTELLIGENCE SERVICES

-----------------------------------------------------------------------------------

**Project Name: Mosaic 2014 evaluation
**Analysts: Jose Pitteloud - jose.pitteloud~@skyiq.co.uk
**Lead(s): Jose Pitteloud - jose.pitteloud~@skyiq.co.uk
**Stakeholder: SIG
**Due Date: 
**Project Code (Insight Collation): V292 - Mosaic 214 Assessment
**SharePoint Folder: http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/01%20Analysis%20Requests/V292%20-%20Mosaic%202014%20Assessment/
**Business Brief:
<Business reason for the code (Answering WHAT and WHY))>

**Stats:
<Relevant stats of the code such as Running time>

-----------------------------------------------------------------------------------

*/

-- Creating a view with relevant fields and matching variables names
CREATE VIEW Mosaic_2014
AS
SELECt 
         filler_num2     h_mosaic_uk_6_affinity_decile
 	,h_mosaic_uk_type_affinity_decile   
		,filler_num3     h_mosaic_uk_6_affinity_percentile
	,h_mosaic_uk_type_affinity_percentile
		,filler_num4     h_mosaic_uk_6_affinity_score
	,h_mosaic_uk_type_affinity_score
		,filler_num5     h_mosaic_uk_6_distance_measure
	,h_mosaic_uk_distance_measure
		,filler_char17   h_mosaic_uk_6_group
	,h_mosaic_uk_group
        ,filler_char18   h_mosaic_uk_6_second_best_type
	,h_mosaic_uk_second_best_type
        ,filler_char19   h_mosaic_uk_6_segment
	,h_mosaic_uk_segment
        ,filler_char20   h_mosaic_uk_6_segment_alternative
	,h_mosaic_uk_segment_alternative
        ,filler_char21   h_mosaic_uk_6_type
	,h_mosaic_uk_type
        ,filler_char22   p_mosaic_uk_6_type
	,p_mosaic_uk_type
        ,filler_char23   pc_mosaic_uk_6_type
	, pc_mosaic_uk_type
		, cb_key_household
		, cb_key_individual
		, cb_address_postcode_area
 FROM 		sk_prod.EXPERIAN_CONSUMERVIEW		AS exc
 LEFT JOIN  sk_prod.CUST_SINGLE_ACCOUNT_VIEW 	AS sav ON sav.cb_key_household = exc.cb_key_household AND 
 COMMIT
 
 -- Creating aggregated view 1 by households, individual and postal area
CREATE VIEW Mosaic_2014_agg1_v2
AS
 SELECT
         h_mosaic_uk_6_group
        , h_mosaic_uk_group
        , h_mosaic_uk_6_segment
        , h_mosaic_uk_segment
        , h_mosaic_uk_6_type
        , h_mosaic_uk_type
        , count(DISTINCT cb_key_household) hh_count
        , count(DISTINCT cb_key_individual) ind_count
		, CASE WHEN sav.cb_key_household IS NULL THEN 0 ELSE 1 END Sky_flag
		, CASE WHEN sbv.cb_key_household IS NULL THEN 0 ELSE 1 END panel_flag
		, CASE WHEN ads.HH_key IS NULL THEN 0 ELSE 1 END Ads_flag
 FROM MOsaic_2014 						AS mos
 LEFT JOIN sk_prod.cust_subs_hist 	AS sav ON sav.cb_key_household = mos.cb_key_household  AND sav.subscription_sub_type IN ('DTV Primary Viewing')   AND sav.status_code IN ('AC','AB','PC')
														AND sav.effective_from_dt <= today()   AND sav.cb_key_household > 0   AND sav.account_number IS NOT NULL   AND sav.service_instance_id IS NOT NULL
 LEFT JOIN vespa_analyst.vespa_SINGLE_BOX_view  AS sbv ON sbv.account_number = sav.account_number AND status_vespa = 'Enabled' AND panel_id_vespa in (11,12)
 LEFT JOIN adsmart_topic_accounts_03_2014		AS ads ON ads.HH_key = mos.cb_key_household AND adsmartable_flag = 1 
 GRoUP BY
         h_mosaic_uk_6_group
        , h_mosaic_uk_group
        , h_mosaic_uk_6_segment
        , h_mosaic_uk_segment
        , h_mosaic_uk_6_type
        , h_mosaic_uk_type
		, Sky_flag
		, panel_flag
		, Ads_flag
commit


SELECT DISTINCT 
	sav.account_number
	, cb_key_household 		AS hh_key
	, CASE WHEN sbv.account_number  is not null THEN 1 ELSE 0 END panel_flag
	, CASE WHEN ads.acct            is not null THEN 1 ELSE 0 END Adsmartable_flag
	, rank () OVER (partition BY sav.account_number ORDER BY sav.cb_row_id) AS rank1
INTO mosaic_sky_accounts
FROM sk_prod.CUST_SINGLE_ACCOUNT_VIEW as sav
LEFT JOIN vespa_analysts.vespa_SINGLE_BOX_view  AS sbv ON sbv.account_number = sav.account_number AND status_vespa like 'Enabled'
LEFT JOIN adsmart_topic_accounts_03_2014               AS ads ON ads.acct = sav.account_number AND adsmartable_flag = 1
where sav.CUST_ACTIVE_DTV =1
AND ads.adsmartable_flag =1 and ads.CUST_VIEWING_DATA_CAPTURE_ALLOWED ='Y'
commit
CREATE HG INDEX idx1 ON mosaic_sky_accounts(account_number)
CREATE HG INDEX idx2 ON mosaic_sky_accounts(hh_key)
commit

 DELETE from mosaic_sky_accounts where rank1>1

 --------------------------- Extracting Broadband accounts
SELECT         csh.Account_number
INTO    mosaic_BB_accounts
FROM    sk_prod.cust_subs_hist csh
WHERE           status_code = 'AC'
        AND     Prev_status_code NOT IN ('AB','AC','PC')
        AND     subscription_sub_type ='Broadband DSL Line'
        AND     status_code_changed = 'Y'
		AND 	effective_to_dt > today()
--------------------------- Extracting SkyGo accounts
SELECT  DISTINCT 
	  account_number
	, cb_key_household
		
INTO mosaic_skygo
FROM sk_prod.SKY_PLAYER_USAGE_DETAIL AS usage
WHERE cb_data_date >= DATEADD(Year,-1, today()) 
        AND cb_data_date < today();
commit
DELETE FROM mosaic_skygo WHERE cb_key_household =0;
DELETE FROM mosaic_skygo WHERE cb_key_household is null;
commit

CREATE HG INDEX d1232 ON mosaic_skygo(cb_key_household)
commit
--------------------------- Extracting NowTV accounts
SELECT DISTINCT cb_key_household, 1 ewr
INTO mosaic_NowTV
FROM sk_prod.NOW_TV_ACCOUNT
WHERE accountstatus = 'Activated' AND cb_key_household >0;
COMMIT;

CREATE HG INDEX d123 ON mosaic_NowTV(cb_key_household);
commit

---------------------------

SELECT
         h_mosaic_uk_6_group
        , h_mosaic_uk_group
        , h_mosaic_uk_6_segment
        , h_mosaic_uk_segment
        , h_mosaic_uk_6_type
        , h_mosaic_uk_type
        , count(DISTINCT mos.cb_key_household) hh_count
        , count(DISTINCT mos.cb_key_individual) ind_count
        , count(DISTINCT ac.hh_key) sky_count
        , COUNT(DISTINCT CASE WHEN ac.panel_flag =1             THEN ac.hh_key ELSE NULL END) vespa_count
        , COUNT(DISTINCT CASE WHEN ac.Adsmartable_flag =1       THEN ac.hh_key ELSE NULL END) ads_count
INTO mosaic_2014_group_sky
FROM mosaic_2014_t            AS mos
LEFT JOIN mosaic_sky_accounts AS ac ON mos.cb_key_household = ac.hh_key
GROUP BY
         h_mosaic_uk_6_group
        , h_mosaic_uk_group
        , h_mosaic_uk_6_segment
        , h_mosaic_uk_segment
        , h_mosaic_uk_6_type
        , h_mosaic_uk_type
commit


SELECT
         h_mosaic_uk_6_group
        , h_mosaic_uk_group
        , h_mosaic_uk_6_type
        , h_mosaic_uk_type
        , COUNT(DISTINCT mos.cb_key_household) hh_count
        , COUNT(DISTINCT sg.cb_key_household) SkyGO_count
        , COUNT(DISTINCT nw.cb_key_household) NowTV_count

                INTO mosaic_2014_group_sky_2
FROM mosaic_2014_t            AS mos
LEFT JOIN mosaic_NowTV AS nw ON mos.cb_key_household = nw.cb_key_household
LEFT JOIN mosaic_skygo AS sg ON mos.cb_key_household = sg.cb_key_household
GROUP BY
         h_mosaic_uk_6_group
        , h_mosaic_uk_group
        , h_mosaic_uk_6_type
        , h_mosaic_uk_type
commit
