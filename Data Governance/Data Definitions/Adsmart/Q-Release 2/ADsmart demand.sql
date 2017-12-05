/*
---------------------------------------------- NETEZZA
SELECT
 	 MNTH
 	,ACCOUNT_NUMBER
	,SUM(HH_VALUE) AS HH_VALUE
	,COUNT(*) AS HH_CAMPS
FROM
   (SELECT
         ACCOUNT_NUMBER
          ,DATE_TRUNC('MONTH',DATE) AS MNTH
          ,DK_CAMPAIGN_DIM
          ,CASE WHEN UNIVERSE_SIZE=0 THEN 0 ELSE CAMPAIGN_BUDGET/CAST(UNIVERSE_SIZE AS DOUBLE PRECISION) END AS HH_VALUE
	FROM 		SMI_ACCESS..V_ADSMART_CAMPAIGN_FACT
	INNER JOIN 	SMI_ACCESS..V_CAMPAIGN_DIM_MEDIA 			ON KEY_HASH=DK_CAMPAIGN_DIM 
	INNER JOIN 	SMI_ACCESS..V_HOUSEHOLD_SEGMENT_FACT		ON DK_SEGMENT_DATE_DIM=CAMPAIGN_START_DATE 	AND V_ADSMART_CAMPAIGN_FACT.DK_ADSMART_SEGMENT_DIM = V_HOUSEHOLD_SEGMENT_FACT.DK_ADSMART_SEGMENT_DIM
	WHERE SALES_PRODUCT_NAME NOT LIKE '%Trial%'
	) A
GROUP BY 1,2
ORDER BY 3 DESC

---------------------------------------------- OLIVE

tt FACT_ADSMART_CAMPAIGN
tt FACT_HOUSEHOLD_SEGMENT
tt DIM_ADSMART_CAMPAIGN

SELECT 
	month_
	, account_number
	, SUM(hh_value) AS S_hh_value
	, COUNT(*) AS HH_camps
FROM(SELECT 
		account_number
		, 
		, b.adsmart_campaign_key
		,CASE WHEN campaign_target_impressions = 0 THEN 0 ELSE campaign_actual_impressions/CAST(campaign_target_impressions AS DOUBLE) END AS HH_VALUE
	FROM FACT_ADSMART_CAMPAIGN      AS a
	JOIN DIM_ADSMART_CAMPAIGN       AS b ON a.adsmart_campaign_key                  = b.adsmart_campaign_key
	JOIN FACT_HOUSEHOLD_SEGMENT AS c ON b.adsmart_campaign_start_date      = RIGHT(CAST(segment_date_key AS VARCHAR),2)||'/'||SUBSTRING (CAST(segment_date_key AS VARCHAR), 5,2)||'/'||SUBSTRING (CAST(segment_date_key AS VARCHAR), 3,2)
	) AS a 
GROUP BY 
	*/
	
	
UPDATE ###ADSMART###
SET HOUSEHOLD_CAMPAIGN_DEMAND = COALESCE(HH_BANDS, 'Percent 0-9')
FROM ###ADSMART### As a 
LEFT JOIN ###_THE_SOURCE_TABLE_#### AS b ON a.account_number= b.account_number 
COMMIT 

