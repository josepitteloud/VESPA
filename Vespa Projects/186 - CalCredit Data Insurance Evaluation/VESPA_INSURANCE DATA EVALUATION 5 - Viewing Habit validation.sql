/* *****************************************************************************
**
**  Project Vespa: PROJECT  V186 - Callcredit Insurance Data Eval 5
**  	Viewing habit difference check
**
**  This script will check for viewing habits differences between the VESPA panel 
**	the CalCredit Insurance Renewal Data as part of the Analytic 
**	Brief - Analytical Task 7. 
**
**
**	Related Documents:
**		- VESPA_INSURANCE DATA EVALUATION 1.sql
**		- VESPA_INSURANCE DATA EVALUATION 2.sql
**		- VESPA_INSURANCE DATA EVALUATION 3.sql
**		- VESPA_INSURANCE DATA EVALUATION 4.sql
**
**	Code Sections:
**		A0 - CREATING TABLE WITH FLAGS
**		A1 - CREATING JOIN TABLE WITH SEGMENT INFO
**		A2 - SEGMENT COUNT
**		A3 - VALIDATION
**
**	Written by Jose Pitteloud
******************************************************************************/


-------------------------------A0 - CREATING TABLE WITH FLAGS
SELECT DISTINCT
  s.account_number
  , s.household_key
  , COUNT(CASE WHEN e.home_renewal_indicator ='Y' AND e.motor_renewal_indicator ='N' THEN 1 ELSE NULL END )[Home_Flag]
  , COUNT(CASE WHEN e.home_renewal_indicator ='N' AND e.motor_renewal_indicator ='Y' THEN 1 ELSE NULL END )[Motor_Flag]
  , COUNT(CASE WHEN e.home_renewal_indicator ='Y' AND e.motor_renewal_indicator ='Y' THEN 1 ELSE NULL END )[Both_Flag]
  , COUNT(CASE WHEN e.home_renewal_indicator ='Y' THEN 1 ELSE NULL END ) Only_Home_Flag
  , COUNT(CASE WHEN e.motor_renewal_indicator ='Y' THEN 1 ELSE NULL END) Only_Motor_Flag
  , COUNT(CASE WHEN e.cb_key_household is not null THEN 1 ELSE NULL END )[Insurance]
INTO VESPA_INSURANCE_CROSS_FLAG  
FROM vespa AS s
    LEFT OUTER JOIN sk_prod.VESPA_INSURANCE_DATA AS e ON e.cb_key_household = s.household_key
    WHERE  s.household_key is not null 
GROUP BY s.account_number
  , s.household_key

-------------------------------A1 - CREATING JOIN TABLE WITH SEGMENT INFO
SELECT 
  isnull(a.account_number, r.account_number) account_number
  , a.Both_Flag
  , a.Home_Flag
  , a.household_key
  , a.Motor_Flag
  , a.Insurance
  , a.Only_Home_Flag
  , a.Only_Motor_Flag
  , r.segment_id
  , r.band
  , r.reference_value
INTO VESPA_INSURANCE_CROSS_SEGMENTS
FROM VESPA_INSURANCE_CROSS_FLAG  AS a
FULL OUTER JOIN rombaoad.segment_collation_values as r on r.account_number = a.account_number
WHERE segment_id in (1,2,26,32,
		64,79,103,203,204,205,206,208,209,210
		,211,212,218,219,220,221,222,223,235) --ONLY MAJOR SEGMENTS SELECTED

-----------------------------A2 - SEGMENT COUNT
SELECT 
    COUNT(account_number) account_number
    ,COUNT(Both_Flag) Both_Flag
    ,COUNT(Home_Flag) Home_Flag
    ,COUNT(household_key) household_key
    ,COUNT(Motor_Flag) Motor_Flag
    ,COUNT(Insurance) Insurance
    ,COUNT(Only_Home_Flag) Only_Home_Flag
    ,COUNT(Only_Motor_Flag) Only_Motor_Flag
    ,COUNT(segment_id) segment_id
    ,COUNT(band) band
    ,COUNT(reference_value) reference_value
FROM VESPA_INSURANCE_CROSS_SEGMENTS


----------------------------A3 - VALIDATION
SELECT 
  Both_Flag
  , Home_Flag
  , Motor_Flag
  , Insurance
  , Only_Home_Flag
  , Only_Motor_Flag
  , segment_id
  , band
  , count(account_number) Qty
FROM VESPA_INSURANCE_CROSS_SEGMENTS
WHERE Segment_id in (64,79,208,205,103,222,210)
GROUP BY Both_Flag
  , Home_Flag
  , Motor_Flag
  , Insurance
  , Only_Home_Flag
  , Only_Motor_Flag
  , segment_id
  , band;

 ------ FURTHER ANALYSIS WAS MADE USING SPSS PACKAGE
 ------ DATA WAS EXTRACTED DIRECTLY FROM THE SPSS CONSOLE FROM TABLE 	VESPA_INSURANCE_CROSS_SEGMENTS
  
  
  
/* DUMP CODE 		***************************************************
SELECT * 
FROM  rombaoad.segment_collation_metadata
WHERE 
  segment_id in (1,2,26,32,
64,79,103,203,204,205,206,208,209,210,211,212,218,219,220,221,222,223,235)

sp_columns VESPA_INSURANCE_CROSS_SEGMENTS

SELECT count(*), COUNT(DISTINCT r.account_number)
FROM rombaoad.segment_collation_values as r 
inner join vespa as v on v.account_number = r.account_number
WHERE  segment_id = 79;

select top 10 * FROM VESPA_INSURANCE_CROSS_SEGMENTS
WHERE NON_Insurance = 1 and Only_Home_Flag = 1

VESPA_INSURANCE_CROSS_FLAG

1,2,26,32,
64,79,103,203,204,205,206,208,209,210,211,212,218,219,220,221,222,223,235


GRANT SELECT ON VESPA_INSURANCE_CROSS_FLAG TO shirokk
commit

SELECT 
-- segment_id
   COUNT(*)
FROM VESPA_INSURANCE_CROSS_SEGMENTS
GROUP BY segment_id
where non_insurance=1 and 
SELECT top 100 *
FROM VESPA_INSURANCE_CROSS_SEGMENTS
--rombaoad.segment_collation_values--VESPA_INSURANCE_CROSS_SEGMENTS
WHERE segment_id is Null

*************************************************************/




