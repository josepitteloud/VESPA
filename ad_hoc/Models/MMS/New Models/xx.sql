-- UP rentals 
SELECT b.account_number
		, MAX(ppv_ordered_dt) AS max_dt
		, base_dt
INTO #temp_rental_usage_over_last_12_months
FROM CUST_PRODUCT_CHARGES_PPV 	AS a 
JOIN cs_raw 					AS b  ON a.account_number = b.account_number AND a.ppv_ordered_dt BETWEEN DATEADD(DAY, 1 ,base_dt) AND DATEADD(MONTH, 1,base_dt) -- Rentals within the next 30 days after the observation date
WHERE ( ca_product_id LIKE 'PVOD%' OR ca_product_id LIKE 'NAM%' OR ca_product_id LIKE 'VCM%')
			AND ppv_cancelled_dt = '9999-09-09'
GROUP BY b.account_number

COMMIT
CREATE HG INDEX id1 	ON #temp_rental_usage_over_last_12_months (account_number)
CREATE DATE INDEX id2 	ON #temp_rental_usage_over_last_12_months (max_dt)
COMMIT

UPDATE cs_raw
SET Up_Rental	= CASE 	WHEN cps.max_dt IS NOT NULL THEN 1 ELSE 0 END 
FROM cs_raw AS a 
LEFT JOIN #temp_rental_usage_over_last_12_months	AS cps ON a.account_number = cps.account_number AND a.base_dt = cps.base_dt
GO

DROP TABLE #temp_rental_usage_over_last_12_months

--------- UP_buy_and_Keep 
		
SELECT b.account_number
	, MAX(est_latest_purchase_dt) AS max_dt
	, base_dt
INTO #temp_buy_and_keep_usage_recency
FROM FACT_EST_CUSTOMER_SNAPSHOT	AS a 
JOIN cs_raw						AS b ON a.account_number = b.account_number AND a.est_latest_purchase_dt BETWEEN DATEADD(DAY, 1 ,base_dt) AND DATEADD(MONTH, 1,base_dt) -- Rentals within the next 30 days after the observation date
WHERE est_latest_purchase_dt IS NOT NULL 
		OR est_first_purchase_dt IS NOT NULL 	
GROUP BY b.account_number

COMMIT
CREATE HG INDEX id1 	ON #temp_buy_and_keep_usage_recency (account_number)
CREATE DATE INDEX id2 	ON #temp_buy_and_keep_usage_recency (base_dt)
CREATE DATE INDEX id3 	ON #temp_buy_and_keep_usage_recency (max_dt)
COMMIT

UPDATE cs_raw a
SET UP_buy_and_Keep	 = CASE WHEN cps.max_dt IS NOT NULL THEN 1 ELSE 0 END 
FROM cs_raw a
LEFT JOIN #temp_buy_and_keep_usage_recency AS cps ON a.account_number = cps.account_number AND a.base_dt = b.base_dt

DROP TABLE #temp_buy_and_keep_usage_recency
COMMIT
----------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------

UPDATE cs_raw
SET    SkyQ_eligible = CSHP_T.PrimaryBoxType
FROM   cs_raw AS base
INNER JOIN (SELECT  stb.account_number
				  ,MIN(CASE WHEN x_description  in ('Sky Q Silver','Sky Q Mini','Sky Q 2TB box','Sky Q','Sky Q 1TB box') THEN 0 --- Known box descriptions
							WHEN UPPER(x_description) LIKE '%SKY Q%'	THEN 0													--- Any other new model
							ELSE 1 END ) AS PrimaryBoxType
			FROM  cust_set_top_box AS stb
			WHERE   stb.x_active_box_flag_new = 'Y' 
					AND account_number IS NOT NULL
			GROUP BY  stb.account_number
		) AS CSHP_T ON CSHP_T.account_number = base.account_number


















