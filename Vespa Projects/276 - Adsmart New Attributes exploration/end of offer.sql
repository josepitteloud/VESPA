/* *****************************
		Adsmart L3 Drop 3 
		End of offer
		
		Coded by: Jose Pitteloud
		Version: 2014-12-11
		
********************************/

SELECT b.account_number
	, CASE WHEN lower (offer_dim_description) LIKE '%sport%' THEN 1 ELSE 0 END 		AS sport_flag
	, CASE WHEN lower (offer_dim_description) LIKE '%movie%' THEN 1 ELSE 0 END 		AS movie_flag
	, CASE 	WHEN 	x_subscription_type IN ('SKY TALK','BROADBAND') 					THEN 'BBT'
			WHEN 	x_subscription_type LIKE 'ENHANCED' AND 
					x_subscription_sub_type LIKE 'Broadband DSL Line' 					THEN 'BBT'
			WHEN 	x_subscription_type IN ('DTV PACKAGE','A-LA-CARTE','ENHANCED')	
				AND sport_flag = 1
				AND movie_flag = 0														THEN 'Sports'
			WHEN 	x_subscription_type IN ('DTV PACKAGE','A-LA-CARTE','ENHANCED')	
				AND sport_flag = 0
				AND movie_flag = 1														THEN 'Movies'
			WHEN 	x_subscription_type IN ('DTV PACKAGE','A-LA-CARTE','ENHANCED')	
				AND sport_flag = 1
				AND movie_flag = 1														THEN 'Top Tier'				
			WHEN 	x_subscription_type IN ('DTV PACKAGE','A-LA-CARTE','ENHANCED')	
				AND sport_flag = 0
				AND movie_flag = 0														THEN 'TV Pack      '	
			ELSE 'Unknown' 	END 											AS Offer_type
	, CASE WHEN offer_end_dt >= GETDATE() THEN 1 ELSE 0 END 				AS live_offer
	, DATE(offer_end_dt) 													AS offer_end_date
	, ABS(DATEDIFF(dd, offer_end_date, getDATE())) 							AS days_from_today
	, rank() OVER(PARTITION BY b.account_number 			ORDER BY live_offer DESC, days_from_today,cb_row_id) 			AS rankk_1
	, rank() OVER(PARTITION BY b.account_number, Offer_type ORDER BY live_offer DESC, days_from_today,cb_row_id) 			AS rankk_2
	, CAST (0 AS bit) 														AS main_offer 
INTO 	Adsmart_end_of_offer_raw
FROM    cust_product_offers AS CPO  
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

DELETE FROM Adsmart_end_of_offer_raw WHERE rankk_2 > 1 				-- To keep the latest offer by each offer type 
COMMIT
CREATE HG INDEX id1 ON Adsmart_end_of_offer_raw(account_number)
COMMIT
-----------		Identifying Accounts with more than one active offer
SELECT 
	account_number
	, COUNT(*) offers
	, MAX(live_offer)			AS live_offer_c
	, MIN(CASE 	WHEN offer_end_date >  GETDATE() THEN DATEDIFF(dd, getDATE(), offer_end_date) 	ELSE NULL END) 	AS live_date	
	, MIN(CASE	WHEN offer_end_date <= GETDATE() THEN DATEDIFF(dd,  offer_end_date, getDATE()) 	ELSE NULL END)	AS past_date
INTO Adsmart_end_of_offer_aggregated
FROM Adsmart_end_of_offer_raw
GROUP BY account_number
HAVING offers > 1

COMMIT
CREATE HG INDEX id2 ON Adsmart_end_of_offer_aggregated(account_number)
COMMIT

UPDATE Adsmart_end_of_offer_raw
SET main_offer = CASE WHEN  	b.live_offer_c = a.live_offer 
							AND (CASE WHEN live_offer_c =1 	THEN b.live_date 
															ELSE b.past_date END) = a.days_from_today THEN 1 ELSE 0 END
FROM Adsmart_end_of_offer_raw 			AS a 
JOIN Adsmart_end_of_offer_aggregated 	AS b ON a.account_number = b.account_number

-----------		Deleting offers which end date is not the min date 
DELETE FROM Adsmart_end_of_offer_raw		AS a
WHERE  	main_offer = 0 
COMMIT
-----------		Updating multi offers
UPDATE Adsmart_end_of_offer_raw
SET Offer_type = 'Multi offer'
FROM Adsmart_end_of_offer_raw AS a 
JOIN (SELECT account_number, count(*) hits FROM Adsmart_end_of_offer_raw GROUP BY account_number HAVING hits > 1) AS b ON a.account_number = b.account_number 
-----------		DEleting duplicates
DELETE FROM Adsmart_end_of_offer_raw WHERE rankk_1 > 1 				-- To keep the latest offer by each offer type 
COMMIT

-----------		Updating Adsmart table

UPDATE adsmart
SET ####End_of_offer### = CASE WHEN b.account_number IS NULL THEN 'No Offer Ever'
							ELSE TRIM(offer_type) ||' – '||
									CASE 	WHEN days_from_today IS NULL 								THEN 'No info on dates'
											WHEN live_offer = 1 AND days_from_today  > 90 				THEN 'Live, ends over 90 days'
											WHEN live_offer = 1 AND days_from_today  BETWEEN 31 AND 90	THEN 'Live, ends in 31 – 90 Days'
											WHEN live_offer = 1 AND days_from_today  <= 30 				THEN 'Live, ends next 30 days'
											WHEN live_offer = 0 AND days_from_today  > 90 				THEN 'Expired, ended over 90 days ago'
											WHEN live_offer = 0 AND days_from_today  BETWEEN 31 AND 90	THEN 'Expired, ended 31 – 90 Days ago'
											WHEN live_offer = 0 AND days_from_today  <= 30 				THEN 'Expired, ended in last 30 days'
											ELSE 'No Offer Ever' END 
							END 
FROM adsmart as a 
LEFT JOIN Adsmart_end_of_offer_raw as b ON a.account_number = b.account_number

-----
/* 		QA


SELECT DISTINCT TRIM(offer_type) ||' – '||
                                    CASE    WHEN days_from_today IS NULL                                THEN 'No info on dates'
                                            WHEN live_offer = 1 AND days_from_today  > 90               THEN 'Live, ends over 90 days'
                                            WHEN live_offer = 1 AND days_from_today  BETWEEN 31 AND 90  THEN 'Live, ends in 31 – 90 Days'
                                            WHEN live_offer = 1 AND days_from_today  <= 30              THEN 'Live, ends next 30 days'
                                            WHEN live_offer = 0 AND days_from_today  > 90               THEN 'Expired, ended over 90 days ago'
                                            WHEN live_offer = 0 AND days_from_today  BETWEEN 31 AND 90  THEN 'Expired, ended 31 – 90 Days ago'
                                            WHEN live_offer = 0 AND days_from_today  <= 30              THEN 'Expired, ended in last 30 days'
                                            ELSE 'No Offer Ever' END offer
                                            , COUNT(*) hits
FROM adsmart as a
JOIN Adsmart_end_of_offer_raw as b ON a.account_number = b.account_number
GROUP BY offer

offer	hits
BBT - Expired, ended 31 - 90 Days ago	63500
BBT - Expired, ended in last 30 days	32630
BBT - Expired, ended over 90 days ago	324164
BBT - Live, ends in 31 - 90 Days	251821
BBT - Live, ends next 30 days	66433
BBT - Live, ends over 90 days	761974
Movies - Expired, ended 31 - 90 Days ago	8644
Movies - Expired, ended in last 30 days	5050
Movies - Expired, ended over 90 days ago	151089
Movies - Live, ends in 31 - 90 Days	24124
Movies - Live, ends next 30 days	10762
Movies - Live, ends over 90 days	94017
Multi offer - Expired, ended 31 - 90 Days ago	9907
Multi offer - Expired, ended in last 30 days	5476
Multi offer - Expired, ended over 90 days ago	32247
Multi offer - Live, ends in 31 - 90 Days	34549
Multi offer - Live, ends next 30 days	14734
Multi offer - Live, ends over 90 days	176088
No Offer Ever	5027474
Sports - Expired, ended 31 - 90 Days ago	7646
Sports - Expired, ended in last 30 days	4474
Sports - Expired, ended over 90 days ago	126432
Sports - Live, ends in 31 - 90 Days	28742
Sports - Live, ends next 30 days	9364
Sports - Live, ends over 90 days	45736
TV Pack - Expired, ended 31 - 90 Days ago	43458
TV Pack - Expired, ended in last 30 days	25636
TV Pack - Expired, ended over 90 days ago	333402
TV Pack - Live, ends in 31 - 90 Days	213644
TV Pack - Live, ends next 30 days	99456
TV Pack - Live, ends over 90 days	1094214
Top Tier - Expired, ended 31 - 90 Days ago	30622
Top Tier - Expired, ended in last 30 days	18548
Top Tier - Expired, ended over 90 days ago	61858
Top Tier - Live, ends in 31 - 90 Days	54945
Top Tier - Live, ends next 30 days	29855
Top Tier - Live, ends over 90 days	83036



/*
