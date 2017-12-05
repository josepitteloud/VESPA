/******************************************************************************
**
**  Project Vespa: PROJECT  V186 - Callcredit Insurance Data Eval 1
**  	Data preparation and Basic Quality Checks
**
**  This script will 
**	the CalCredit Insurance Renewal Data as part of the Analytic 
**	Brief - Analytical Task 5 and 6
**
**	Related Documents:
**		- VESPA_INSURANCE DATA EVALUATION 1.sql
**		- VESPA_INSURANCE DATA EVALUATION 2.sql
**		- VESPA_INSURANCE DATA EVALUATION 3.sql
**
**	Code Sections:
**
**
**	Written by Jose Pitteloud
******************************************************************************/



---------------     Counting Totals for Motor, Home & Both Renewals
SELECT
COUNT(*) TOTAL_COUNT
, COUNT(CASE WHEN home_renewal_indicator = 'Y' THEN 1 ELSE NULL END) Home
, COUNT(CASE WHEN home_renewal_indicator = 'Y' AND  motor_renewal_indicator = 'Y' THEN 1 ELSE NULL END) Both
, COUNT(CASE WHEN motor_renewal_indicator = 'Y' THEN 1 ELSE NULL END) Motor
FROM sk_prod.VESPA_INSURANCE_DATA


SELECT home_renewal_month_code  Home
  , motor_renewal_month_code    Motor
  ,count(*)                     Total
FROM sk_prod.VESPA_INSURANCE_DATA
GROUP BY home_renewal_month_code
  , motor_renewal_month_code
  
  
-------------  CREATING a Summarized Table by HH
SELECT sk_prod.VESPA_INSURANCE_DATA.cb_key_household
  , COUNT(CASE WHEN home_renewal_indicator = 'Y' THEN 1 ELSE NULL END) Home
  , COUNT(CASE WHEN home_renewal_indicator = 'Y' AND  motor_renewal_indicator = 'Y' THEN 1 ELSE NULL END) Both
  , COUNT(CASE WHEN motor_renewal_indicator = 'Y' THEN 1 ELSE NULL END) Motor
  , COUNT(DISTINCT cb_key_individual) Total_ind_CB
  , COUNT(*)  TOTAL
INTO VESPA_INSURANCE_HH_GROUP
FROM sk_prod.VESPA_INSURANCE_DATA
GROUP BY sk_prod.VESPA_INSURANCE_DATA.cb_key_household

---------------COUNTING PEOPLE IN EACH GROUP

SELECT
  'Individual W/ more than 1 request (row)' mGroup
  , COUNT (*) HH
  , SUM (TOTAL) TOTAL_Rows 
FROM VESPA_INSURANCE_HH_GROUP
WHERE Total_ind_CB =1 AND TOTAL >1
  UNION
SELECT
  'HH with more than 1 request (row) (+1 Individual)'
  , COUNT (*) HH
  , SUM (TOTAL) TOTAL_Rows 
FROM VESPA_INSURANCE_HH_GROUP
WHERE Total_ind_CB >1 AND TOTAL >1
  UNION
  SELECT
  ' HH with only 1 request (1 row & 1 individual)'
  , COUNT (*) HH
  , SUM (TOTAL) TOTAL_Rows 
FROM VESPA_INSURANCE_HH_GROUP
WHERE Total_ind_CB =1 AND TOTAL =1
  

---------------COUNTING SPLIT BY INSURANCE TYPE IN EACH GROUP

SELECT 
  'Split HH with only 1 request' mGroup
  , sum(Home) Total_Home
  , sum(Motor) Total_Motor
  , sum(Both) Total_both
FROM VESPA_INSURANCE_HH_GROUP
WHERE Total_ind_CB =1 AND TOTAL =1

UNION

SELECT 
  'Split Individual W/ more than 1 request' mGroup
  , sum(Home) Total_Home
  , sum(Motor) Total_Motor
  , sum(Both) Total_both
FROM VESPA_INSURANCE_HH_GROUP
WHERE Total_ind_CB =1 AND TOTAL >1

UNION

SELECT 
  'Split HH with more than 1 request' mGroup
  , sum(Home) Total_Home
  , sum(Motor) Total_Motor
  , sum(Both) Total_both
FROM VESPA_INSURANCE_HH_GROUP
WHERE Total_ind_CB >1 AND TOTAL >1






