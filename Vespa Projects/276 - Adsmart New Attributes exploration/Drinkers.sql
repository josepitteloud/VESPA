/* *****************************
		Adsmart L3 Drop 3 
		Drinkers Segmentation Definition
		
		Description:
			-Using Experian Clientele segmentation we need to allocate each household to a drinking segment. Clientele has 14 groups
			-Using expenditure in food (High/Low) and expenditure in alcohol (High/Low) we produced 4 main groups. 
			-Cliente is at postcode level so we need to define rules to allocate/exclude individual household within postcodes
			-The process will follow:
				- Define exclusion rules for the groups
				- Calculate the UK drinking weekly sessions per person considering exclusion rules 
				- Define a threshold for minimum occasions rate per week per person
				- Allocate household to the matching group
			-Groups definition: 
				- HH (High food / High Alcohol)
				- HL (High food / Low Alcohol)
				- LH (Low food / High Alcohol)
				- LL (Low food / Low Alcohol)
				
		Lead: 	Rahki Drennan / Jose Pitteloud		
		Coded by: Jose Pitteloud
	Sections:
			A01	-	Exclusion rules
			A02	-	Calculate Uk distribution
			A03	-	Define threshold
			
*********************************/

------------------------------------------------------------------------------------------------------
--	A01	-	Define exclusion rules for the groups
--	Defining Postcodes and raw groups

SELECT 	cb_address_postcode
		, GROUP_1_HH = SUM(CAST(pc_clientele_02_big_night_out AS int) 
						+ CAST(pc_clientele_08_out_for_dinner AS INT))
		, GROUP_2_HL = SUM(CAST(pc_clientele_04_family_fun AS int) 
						+ CAST(pc_clientele_11_leisurely_lunch AS INT) 
						+ CAST(pc_clientele_12_weekend_lunch AS INT)
						+ CAST(pc_clientele_14_sociable_suburbs AS INT))
		, GROUP_3_HL = SUM(CAST(pc_clientele_06_pub_play AS int) 
						+ CAST(pc_clientele_07_evening_local AS INT) 
						+ CAST(pc_clientele_09_student_drinks AS INT) 
						+ CAST(pc_clientele_10_out_on_the_town AS INT) 
						)
		, GROUP_4_LL = SUM(CAST(pc_clientele_05_daytime_local AS int) 
						+ CAST(pc_clientele_01_me_and_my_pint AS INT) 
						+ CAST(pc_clientele_13_catch_up AS INT))
INTO adsmart_drop3_Drinkers_raw
FROM CONSUMERVIEW_POSTCODE
GROUP BY cb_address_postcode

COMMIT  
CREATE HG INDEX efk ON adsmart_drop3_Drinkers_raw (cb_address_postcode)
COMMIT 

------------------------------------------------------------------------------------------------------
--	A02	-	Calculate UK distribution


SELECT
    cb_address_postcode
    , cb_key_household
    , COUNT(*)      AS individuals
    , CASE WHEN h_number_of_children_in_household_2011 IN ('1','2','3','4') THEN 1 ELSE 0 END AS HH_children
    , MAX(CASE WHEN p_gender = '0'  THEN CAST (p_actual_age AS INT) ELSE 0 END ) max_male_age
    , MAX(CASE WHEN p_gender = '1'  THEN CAST (p_actual_age AS INT) ELSE 0 END ) max_female_age
    , MIN(p_actual_age) min_age
    , CAST(NULL AS VARCHAR(20))         AS Drinker_Group
    , RAND(cb_key_household + DATEPART(us, GETDATE())) random_number
    , CAST (0 AS TINYINT) AS Drinkers_group
INTO adsmart_drop3_Drinkers_postcode
FROM EXPERIAN_CONSUMERVIEW
GROUP BY
    cb_address_postcode
    , cb_key_household
    , HH_children

	
SELECT 
	a.cb_address_postcode
	, COUNT(a.cb_key_household) 	AS 	total_hh
	, SUM(a.individuals) 			AS 	total_ind
	, SUM(a.HH_children)			AS 	total_hh_w_children
	, SUM(CASE WHEN a.HH_children = 1 THEN a.individuals ELSE 0 END) total_Ind_W_children
---------------------------------------------------------------------------------------------
	, SUM(CASE WHEN a.max_male_age 	>= 55 OR max_female_age >= 55  THEN 1 ELSE 0 END) 	AS Senior_HH
	, SUM(CASE WHEN a.max_male_age 	>= 55 THEN 1 ELSE 0 END) 							AS Senior_male_HH
	, SUM(CASE WHEN a.max_female_age >= 55 THEN 1 ELSE 0 END) 							AS Senior_female_HH
---------------------------------------------------------------------------------------------	
	, SUM(CASE WHEN a.max_male_age >= 55 OR max_female_age >= 55  THEN a.individuals ELSE 0 END) 	AS Senior_individual
	, SUM(CASE WHEN a.max_male_age >= 55 THEN a.individuals 	ELSE 0 END) 						AS Senior_male_individual
	, SUM(CASE WHEN a.max_female_age >= 55 THEN a.individuals 	ELSE 0 END) 						AS Senior_female_individual
---------------------------------------------------------------------------------------------
	, SUM(CASE WHEN a.min_age <= 25	THEN 1 ELSE 0 END) 								AS Student_HH
	, SUM(CASE WHEN a.min_age <= 25	THEN a.individuals ELSE 0 END) 					AS Student_individual
---------------------------------------------------------------------------------------------
	, SUM(b.GROUP_1_HH)																AS Group_1_total
	, SUM(b.GROUP_2_HL)																AS Group_2_total
	, SUM(b.GROUP_3_HL)																AS Group_3_total
	, SUM(b.GROUP_4_LL)																AS Group_4_total
---------------------------------------------------------------------------------------------
	, GROUP_1_avg_HH 	= Group_1_total/ ((total_hh - total_hh_w_children) + (total_hh_w_children * 0.5))
	, GROUP_2_avg_HH	= Group_2_total/ ((total_hh - total_hh_w_children)* 0.5 + total_hh_w_children)
	, GROUP_3_avg_HH 	= Group_3_total/ ((total_hh - Student_HH)* 0.7 + Student_HH)
	, GROUP_4_avg_HH 	= Group_4_total/ ((total_hh - Senior_HH) * 0.7 + Senior_HH)
---------------------------------------------------------------------------------------------
	, GROUP_1_avg_Ind 	= Group_1_total/ ((total_ind - total_Ind_W_children) + (total_Ind_W_children * 0.5))
	, GROUP_2_avg_Ind 	= Group_2_total/ ((total_ind - total_Ind_W_children)* 0.5 + total_Ind_W_children)
	, GROUP_3_avg_Ind 	= Group_3_total/ ((total_ind - Student_individual)  * 0.7 + Student_individual)
	, GROUP_4_avg_Ind 	= Group_4_total/ ((total_ind - Senior_individual)   * 0.7 + Senior_individual)
---------------------------------------------------------------------------------------------	
	, Total_ocasion 	= Group_1_total + Group_2_total + Group_3_total + Group_4_total
	, Big_average_hh	= GROUP_1_avg_HH + GROUP_2_avg_HH + GROUP_3_avg_HH + GROUP_4_avg_HH
	, Big_average_ind	= GROUP_1_avg_Ind + GROUP_2_avg_Ind + GROUP_3_avg_Ind + GROUP_4_avg_Ind
INTO adsmart_drop3_Drinkers_postcode_summary						
FROM adsmart_drop3_Drinkers_postcode	AS a 
JOIN adsmart_drop3_Drinkers_raw			AS b ON a.cb_address_postcode = b.cb_address_postcode
GROUP BY a.cb_address_postcode

COMMIT
CREATE HG INDEX idwd ON adsmart_drop3_Drinkers_postcode_summary(cb_address_postcode)
COMMIT

------------------------------------------------------------------------------------------------------
--	A02	-	Calculate UK distribution
	
DECLARE @lower_limit DECIMAL(4,2)
SET @lower_limit = 0.1 -- Ocassions per Individual per week

SELECT
      a.cb_address_postcode
    , cb_key_household
    , CASE
            WHEN b.Group_2_total > (0.6*b.Group_1_total) AND b.Group_2_total > (0.6*b.Group_3_total) AND b.Group_2_total > (0.6*b.Group_4_total) THEN 2
            WHEN b.Group_1_total > b.Group_2_total AND b.Group_1_total > (0.7*b.Group_3_total) AND b.Group_1_total > b.Group_4_total THEN 1
            WHEN b.Group_4_total > b.Group_1_total AND b.Group_4_total > b.Group_2_total AND b.Group_4_total > (0.7*b.Group_3_total) THEN 4
            WHEN b.Group_3_total > b.Group_1_total AND b.Group_3_total > b.Group_2_total AND b.Group_3_total > b.Group_4_total THEN 3
            ELSE 9 END Main_Group
    , CASE  WHEN Main_Group = 1 THEN
            CASE    WHEN b.Group_2_total > b.Group_3_total AND b.Group_2_total > b.Group_4_total AND (b.Group_2_total/ b.Group_1_total) > 0.1 THEN 2
                    WHEN b.Group_3_total > b.Group_2_total AND b.Group_3_total > b.Group_4_total AND (b.Group_3_total/ b.Group_1_total) > 0.3 THEN 3
                    WHEN b.Group_4_total > b.Group_2_total AND b.Group_4_total > b.Group_3_total AND (b.Group_4_total/ b.Group_1_total) > 0.3 THEN 4
                    ELSE 8 END
            WHEN Main_Group = 2 THEN
            CASE    WHEN b.Group_1_total > b.Group_3_total AND b.Group_1_total > b.Group_4_total AND (b.Group_1_total/ b.Group_2_total) > 0.3 THEN 1
                    WHEN b.Group_3_total > b.Group_1_total AND b.Group_3_total > b.Group_4_total AND (b.Group_3_total/ b.Group_2_total) > 0.3 THEN 3
                    WHEN b.Group_4_total > b.Group_1_total AND b.Group_4_total > b.Group_3_total AND (b.Group_4_total/ b.Group_2_total) > 0.1 THEN 4
                    ELSE 8 END
            WHEN Main_Group = 3 THEN
            CASE    WHEN b.Group_1_total > b.Group_2_total AND b.Group_1_total > b.Group_4_total THEN 1
                    WHEN b.Group_2_total > b.Group_1_total AND b.Group_2_total > b.Group_4_total THEN 2
                    WHEN b.Group_4_total > b.Group_1_total AND b.Group_4_total > b.Group_2_total THEN 4
                    ELSE 11 END
            WHEN Main_Group = 4 THEN
            CASE    WHEN b.Group_1_total > b.Group_2_total AND b.Group_1_total > b.Group_3_total AND (b.Group_1_total/ b.Group_4_total) > 0.3 THEN 1
                    WHEN b.Group_2_total > b.Group_1_total AND b.Group_2_total > b.Group_3_total AND (b.Group_2_total/ b.Group_4_total) > 0.1 THEN 2
                    WHEN b.Group_3_total > b.Group_1_total AND b.Group_3_total > b.Group_2_total AND (b.Group_4_total/ b.Group_4_total) > 0.3 THEN 3
                    ELSE 8 END
            ELSE 7 END                      AS  Second_Group
    , CASE
            WHEN Main_Group = 1 AND HH_children = 0 THEN 1
            WHEN Main_Group = 1 AND HH_children = 1 AND random_number > 0.3     THEN 1
            WHEN Main_Group = 1 AND HH_children = 1 AND random_number <= 0.3    THEN Second_Group

            WHEN Main_Group = 2 THEN 2
                    --WHEN Main_Group = 2 AND HH_children = 1 THEN 2
                    --WHEN Main_Group = 2 AND HH_children = 0 AND random_number >= 0.3  THEN 2
                    --WHEN Main_Group = 2 AND HH_children = 0 AND random_number < 0.3   THEN Second_Group

            WHEN Main_Group = 4 AND (a.max_male_age >= 55 OR max_female_age >= 55 ) THEN 4
            WHEN Main_Group = 4 AND random_number <= 0.8                            THEN 4
            WHEN Main_Group = 4 AND random_number > 0.8                             THEN Second_Group

            WHEN Main_Group = 3 AND min_age <= 25   THEN 3
            WHEN Main_Group = 3 AND min_age > 25    AND random_number <= 0.6    THEN 3
            WHEN Main_Group = 3 AND min_age > 25    AND random_number > 0.6     THEN Second_Group

            ELSE 10 END AS Allocated_Group
INTO adsmart_drop3_Drinkers_allocated
FROM adsmart_drop3_Drinkers_postcode            AS a
JOIN adsmart_drop3_Drinkers_postcode_summary    AS b ON a.cb_address_postcode = b.cb_address_postcode AND Big_average_ind > @lower_limit
WHERE cv.cb_key_household > 0             	AND cv.account_number IS NOT NULL
  

UPDATE ###ADSMART###
SET Drinkers_group = CASE Allocated_Group 	WHEN 1 THEN 'Eat Drink and be Merry'
											WHEN 2 THEN	'Cheers'
											WHEN 3 THEN 'Food Glorious Food'
											WHEN 4 THEN 'Just the one'
											ELSE 'Unknown' 
											END
FROM ###ADSMART### as a 
JOIN adsmart_drop3_Drinkers_allocated as b ON a.cb_key_household = b.cb_key_household

COMMIT
DROP TABLE adsmart_drop3_Drinkers_allocated
DROP TABLE adsmart_drop3_Drinkers_postcode
DROP TABLE adsmart_drop3_Drinkers_postcode_summary
DROP TABLE adsmart_drop3_Drinkers_raw
COMMIT
	
	
-----------------			QA
/*
SELECT
    Allocated_Group, main_group, second_group
    , COUNT(*) hits
FROM adsmart_drop3_Drinkers_allocated as a 
JOIN adsmartables_20141126 	AS cv ON a.cb_key_household = cv.cb_key_household 
	WHERE cv.cb_key_household > 0             	AND cv.account_number IS NOT NULL
GROUP BY     Allocated_Group, main_group, second_group
ORDER BY     Allocated_Group, main_group, second_group
*/


	


