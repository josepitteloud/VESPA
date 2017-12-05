SELECT
          cala.account_number
        , MAX(last_modified_dt) last_dt
INTO  #ADSMART_Q2_on_demand_raw
FROM CUST_ANYTIME_PLUS_DOWNLOADS cala
INNER JOIN ##ADSMART##  AS sav ON cala.account_number = sav.account_number 
AND last_modified_dt <= now()
WHERE UPPER(genre_desc) LIKE UPPER('%MOVIE%')
AND provider_brand IN ('Sky Disney','Sky Disney HD','Sky Movies','Sky Movies HD')
GROUP BY cala.account_number
commit



UPDATE ##ADSMART## 
SET MOVIES_ON_DEMAND = CASE     WHEN DATEDIFF (day, last_dt, getDATE())  <= 90                   THEN 'Downloaded movies 0-3 months'
								WHEN DATEDIFF (day, last_dt, getDATE())  BETWEEN 91 AND 180      THEN 'Downloaded movies 4-6 months'
								WHEN DATEDIFF (day, last_dt, getDATE())  >= 181                  THEN 'Downloaded movies 7+ months'
								ELSE 'Never'
								END As Movies_on_demand
FROM ##ADSMART##  AS a
LEFT JOIN #ADSMART_Q2_on_demand_raw   AS b ON a.account_number = b.account_number
GROUP BY
        Movies_on_demand

		
DROP TABLE #ADSMART_Q2_on_demand_raw
COMMIT 


