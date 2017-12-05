/*

**Project Name:                         ADSMART - Quarterly Release 2
**Lead(s):                              Jose Pitteloud         (jose.pitteloud@skyiq.co.uk)
**Stakeholder:                          ADSMART team
**Due Date:                             05/10/2015
**Business Brief:

        This script holds the updates statements to build the attributes included in the list
**Atributes:
   1- Movies On Demand
   2- Sky Generated Home movers
   3- TECI
Notes:
   *** Replace the placeholder ##ADSMART## for the right ADSMART table
   *** The table mckanej.TECI_current_score  needs to be replaced by the production table, once it's available
*/


--------------------------------------------
--------- 1- Movies On Demand
--------------------------------------------
  SELECT cala.account_number
        ,MAX(last_modified_dt) last_dt
    INTO #ADSMART_Q2_on_demand_raw
    FROM CUST_ANYTIME_PLUS_DOWNLOADS cala
         INNER JOIN ##ADSMART##  AS sav ON cala.account_number = sav.account_number
                                       AND last_modified_dt <= now()
   WHERE UPPER(genre_desc) LIKE UPPER('%MOVIE%')
     AND provider_brand IN ('Sky Disney','Sky Disney HD','Sky Movies','Sky Movies HD')
GROUP BY cala.account_number

commit

  UPDATE ##ADSMART##
     SET MOVIES_ON_DEMAND = 'Unknown'

  UPDATE ##ADSMART## as bas
     SET MOVIES_ON_DEMAND = CASE WHEN DATEDIFF (day, last_dt, getDATE())  <= 91                   THEN 'Downloaded movies 0-3 months'
                                 WHEN DATEDIFF (day, last_dt, getDATE())  BETWEEN 92 AND 182      THEN 'Downloaded movies 4-6 months'
                                 WHEN DATEDIFF (day, last_dt, getDATE())  >= 183                  THEN 'Downloaded movies 7+ months'
                                 ELSE 'Never'
                             END
    FROM #ADSMART_Q2_on_demand_raw AS sub
   WHERE bas.account_number = sub.account_number


    DROP TABLE #ADSMART_Q2_on_demand_raw
  COMMIT


--------------------------------------------
--------- 2- Sky Generated Home movers
--------------------------------------------
--------------------------------------------------------
-- The only change is the recoding of the 'In-Progress' to  'Post Home Move 0-30 days'
--------------------------------------------------------
  SELECT account_number
        ,CASE WHEN home_move_status = 'Pre Home Move'     THEN 'Pre Home Move'
              WHEN home_move_status = 'Pending'           THEN 'Pending Home Move'
              WHEN home_move_status = 'In-Progress'       THEN 'Post Home Move 0 - 30 Days'
              WHEN home_move_status = 'Post Home Move' AND DATEDIFF(dd, effective_from_dt, getdate()) BETWEEN 0  AND 30  THEN 'Post Home Move 0 - 30 Days'
              WHEN home_move_status = 'Post Home Move' AND DATEDIFF(dd, effective_from_dt, getdate()) BETWEEN 31 AND 60  THEN 'Post Home Move 31 - 60 Days'
              WHEN home_move_status = 'Post Home Move' AND DATEDIFF(dd, effective_from_dt, getdate()) BETWEEN 61 AND 90  THEN 'Post Home Move 61 - 90 Days'
              WHEN home_move_status = 'Post Home Move' AND DATEDIFF(dd, effective_from_dt, getdate()) BETWEEN 91 AND 120 THEN 'Post Home Move 91 - 120 Days'
              WHEN home_move_status = 'None'           AND DATEDIFF(dd, effective_from_dt, getdate()) > 30               THEN 'None'
              ELSE 'Unknown' END AS home_move_status
    INTO #movers
    FROM (  SELECT account_number
                  ,home_move_status
                  ,effective_from_dt
                  ,rank() OVER(PARTITION BY account_number
                                   ORDER BY effective_from_dt   DESC
                                           ,dw_last_modified_dt DESC
                              ) AS rankk
              FROM CUST_HOME_MOVE_STATUS_HIST
         ) as b
   WHERE rankk = 1
     AND effective_from_dt > DATEADD(dd, -120, GETDATE())

  COMMIT
  CREATE HG INDEX id1 ON #movers(account_number)

  UPDATE ##ADSMART##
     SET SKY_GENERATED_HOME_MOVER = 'Unknown'

  UPDATE ##ADSMART##
     SET SKY_GENERATED_HOME_MOVER = home_move_status
    FROM ##ADSMART##  as a
         JOIN #movers as b ON a.account_number = b.account_number

    DROP TABLE #movers
  COMMIT


--------------------------------------------
--------- 3- TECI
--------------------------------------------
  UPDATE ##ADSMART##
     SET TECI = COALESCE(cluster_name, 'Unknown')
    FROM ##ADSMART##      AS a
         LEFT JOIN mckanej.TECI_current_score  AS b ON a.account_number = b.account_number

--------------------------------------------
--------- 4- Local authority
--------------------------------------------
---- By definition there will be only 1 row per postcode in the UK_LOCAL_AUTHORITY_AREAS  table
---- REPLACE ##ADSMART## by the final Adsmart table

UPDATE ##ADSMART##
SET  LOCAL_AUTHORITY = COALESCE (b.government_boundary, 'Unknown')
FROM ##ADSMART## AS a 
LEFT JOIN UK_LOCAL_AUTHORITY_AREAS AS b ON REPLACE(TRIM(a.cb_address_postcode),' ','' = REPLACE(TRIM(b.postcode),' ', '')
COMMIT

--------------------------------------------
--------- 5- HOUSEHOLD_CAMPAIGN_DEMAND
--------------------------------------------
---- 
---- REPLACE ##ADSMART## by the final Adsmart table

SELECT account_number, max (hh_band) AS hh_band
INTO HH_CAMPAIGN_DEMAND
FROM #HOUSEHOLD_CAMPAIGN_DEMAND
GROUP BY account_number 
COMMIT 
CREATE HG INDEX h1 ON #HH_CAMPAIGN_DEMAND (account_number)
commit 

UPDATE ###ADSMART###
SET HOUSEHOLD_CAMPAIGN_DEMAND = COALESCE(hh_band, 'Percent 0-9')
FROM ###ADSMART### As a 
LEFT JOIN #HH_CAMPAIGN_DEMAND AS b ON a.account_number= b.account_number 
COMMIT 

DROP TABLE #HH_CAMPAIGN_DEMAND
COMMIT 

/*
Changes made in QA
----------------------
line 35 query corrected
line 63 corrected
line 64 removed word BETWEEN
line 36-37 changed 90 to 91 for 3 months
line 37-38 changed 180 to 182 for 6 months
line 67 limited to the fields reqd
replaced occurrences of ##ADSMART## with ADSMART_QA
added 'Unknown' defaults
formatting

Note
----
The code only contains 3 of the 7 attributes that are required
*/




