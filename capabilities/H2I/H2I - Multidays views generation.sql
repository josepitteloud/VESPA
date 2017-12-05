--SC3I_weightings
CREATE OR REPLACE VIEW M11_SC3I_weightings AS 
SELECT '41903' AS dt, 66 AS ID, 'Jose P' AS owner, *  FROM vespa_shared.SC3I_weightings_20140921_066 UNION 
SELECT '41904' AS dt, 66 AS ID, 'Jose P' AS owner  ,* FROM vespa_shared.SC3I_weightings_20140922_066 UNION 
SELECT '41905' AS dt, 66 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.SC3I_weightings_20140923_066 UNION 
SELECT '41903' AS dt, 65 AS ID, 'Hoi' AS owner, * FROM tanghoi.SC3I_weightings_20140921_065 UNION 
SELECT '41904' AS dt, 65 AS ID, 'Hoi' AS owner, * FROM tanghoi.SC3I_weightings_20140922_065 UNION 
SELECT '41905' AS dt, 65 AS ID, 'Hoi' AS owner ,* FROM tanghoi.SC3I_weightings_20140923_065 UNION
SELECT '41903' AS dt, 67 AS ID, 'Hoi' AS owner, * FROM tanghoi.SC3I_weightings_20140921_067 UNION 
SELECT '41904' AS dt, 67 AS ID, 'Hoi' AS owner, * FROM tanghoi.SC3I_weightings_20140922_067 UNION 
SELECT '41905' AS dt, 67 AS ID, 'Hoi' AS owner ,* FROM tanghoi.SC3I_weightings_20140923_067 UNION
SELECT '41903' AS dt, 68 AS ID, 'Jose P' AS owner, *  FROM vespa_shared.SC3I_weightings_20140921_068 UNION 
SELECT '41904' AS dt, 68 AS ID, 'Jose P' AS owner , *  FROM vespa_shared.SC3I_weightings_20140922_068 UNION 
SELECT '41905' AS dt, 68 AS ID, 'Jose P' AS owner, *  FROM vespa_shared.SC3I_weightings_20140923_068
--v289_m12_dailychecks_base
CREATE OR REPLACE VIEW M12_dailychecks AS 
SELECT '41903' AS dt, 66 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.V289_M12_historic_results_20140921_066 UNION 
SELECT '41904' AS dt, 66 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.V289_M12_historic_results_20140922_066 UNION 
SELECT '41905' AS dt, 66 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.V289_M12_historic_results_20140923_066 UNION 
SELECT '41903' AS dt, 65 AS ID, 'Hoi' AS owner ,* FROM tanghoi.V289_M12_historic_results_20140921_065 UNION 
SELECT '41904' AS dt, 65 AS ID, 'Hoi' AS owner ,* FROM tanghoi.V289_M12_historic_results_20140922_065 UNION 
SELECT '41905' AS dt, 65 AS ID, 'Hoi' AS owner ,* FROM tanghoi.V289_M12_historic_results_20140923_065 UNION

SELECT '41903' AS dt, 67 AS ID, 'Hoi' AS owner ,* FROM tanghoi.V289_M12_historic_results_20140921_067 UNION 
SELECT '41904' AS dt, 67 AS ID, 'Hoi' AS owner ,* FROM tanghoi.V289_M12_historic_results_20140922_067 UNION 
SELECT '41905' AS dt, 67 AS ID, 'Hoi' AS owner ,* FROM tanghoi.V289_M12_historic_results_20140923_067 UNION

SELECT '41903' AS dt, 68 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.V289_M12_historic_results_20140921_068 UNION 
SELECT '41904' AS dt, 68 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.V289_M12_historic_results_20140922_068 UNION 
SELECT '41905' AS dt, 68 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.V289_M12_historic_results_20140923_068 
COMMIT 

--v289_S12_weighted_duration_skyview
CREATE OR REPLACE VIEW M12_dailychecks_weighted AS 
SELECT '41903' AS dt, 66 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.v289_S12_weighted_duration_skyview_20140921_066 UNION 
SELECT '41904' AS dt, 66 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.v289_S12_weighted_duration_skyview_20140922_066 UNION 
SELECT '41905' AS dt, 66 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.v289_S12_weighted_duration_skyview_20140923_066 UNION 
SELECT '41903' AS dt, 65 AS ID, 'Hoi' AS owner ,* FROM tanghoi.v289_S12_weighted_duration_skyview_20140921_065 UNION 
SELECT '41904' AS dt, 65 AS ID, 'Hoi' AS owner ,* FROM tanghoi.v289_S12_weighted_duration_skyview_20140922_065 UNION 
SELECT '41905' AS dt, 65 AS ID, 'Hoi' AS owner ,* FROM tanghoi.v289_S12_weighted_duration_skyview_20140923_065 UNION
SELECT '41903' AS dt, 67 AS ID, 'Hoi' AS owner ,* FROM tanghoi.v289_S12_weighted_duration_skyview_20140921_067 UNION 
SELECT '41904' AS dt, 67 AS ID, 'Hoi' AS owner ,* FROM tanghoi.v289_S12_weighted_duration_skyview_20140922_067 UNION 
SELECT '41905' AS dt, 67 AS ID, 'Hoi' AS owner ,* FROM tanghoi.v289_S12_weighted_duration_skyview_20140923_067 UNION
SELECT '41903' AS dt, 68 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.v289_S12_weighted_duration_skyview_20140921_068 UNION 
SELECT '41904' AS dt, 68 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.v289_S12_weighted_duration_skyview_20140922_068 UNION 
SELECT '41905' AS dt, 68 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.v289_S12_weighted_duration_skyview_20140923_068 
COMMIT

--V289_s12_v_genderage_distribution
CREATE OR REPLACE VIEW M12_genderage_DIST AS 
SELECT '41903' AS dt, 66 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.V289_s12_v_genderage_distribution_20140921_066 UNION 
SELECT '41904' AS dt, 66 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.V289_s12_v_genderage_distribution_20140922_066 UNION 
SELECT '41905' AS dt, 66 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.V289_s12_v_genderage_distribution_20140923_066 UNION 
SELECT '41903' AS dt, 65 AS ID, 'Hoi' AS owner ,* FROM tanghoi.V289_s12_v_genderage_distribution_20140921_065 UNION 
SELECT '41904' AS dt, 65 AS ID, 'Hoi' AS owner ,* FROM tanghoi.V289_s12_v_genderage_distribution_20140922_065 UNION 
SELECT '41905' AS dt, 65 AS ID, 'Hoi' AS owner ,* FROM tanghoi.V289_s12_v_genderage_distribution_20140923_065 UNION

SELECT '41903' AS dt, 67 AS ID, 'Hoi' AS owner ,* FROM tanghoi.V289_s12_v_genderage_distribution_20140921_067 UNION 
SELECT '41904' AS dt, 67 AS ID, 'Hoi' AS owner ,* FROM tanghoi.V289_s12_v_genderage_distribution_20140922_067 UNION 
SELECT '41905' AS dt, 67 AS ID, 'Hoi' AS owner ,* FROM tanghoi.V289_s12_v_genderage_distribution_20140923_067 UNION
SELECT '41903' AS dt, 68 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.V289_s12_v_genderage_distribution_20140921_068 UNION 
SELECT '41904' AS dt, 68 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.V289_s12_v_genderage_distribution_20140922_068 UNION 
SELECT '41905' AS dt, 68 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.V289_s12_v_genderage_distribution_20140923_068 
COMMIT

--v289_m12_piv_distributions
create OR REPLACE view M12_PIV_DIST_VIEW as
SELECT '41903' AS dt, 66 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.v289_m12_piv_distributions_20140921_066 UNION 
SELECT '41904' AS dt, 66 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.v289_m12_piv_distributions_20140922_066 UNION 
SELECT '41905' AS dt, 66 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.v289_m12_piv_distributions_20140923_066 UNION 
SELECT '41903' AS dt, 65 AS ID, 'Hoi' AS owner ,* FROM tanghoi.v289_m12_piv_distributions_20140921_065 UNION 
SELECT '41904' AS dt, 65 AS ID, 'Hoi' AS owner ,* FROM tanghoi.v289_m12_piv_distributions_20140922_065 UNION 
SELECT '41905' AS dt, 65 AS ID, 'Hoi' AS owner ,* FROM tanghoi.v289_m12_piv_distributions_20140923_065 UNION 

SELECT '41903' AS dt, 67 AS ID, 'Hoi' AS owner ,* FROM tanghoi.v289_m12_piv_distributions_20140921_067 UNION 
SELECT '41904' AS dt, 67 AS ID, 'Hoi' AS owner ,* FROM tanghoi.v289_m12_piv_distributions_20140922_067 UNION 
SELECT '41905' AS dt, 67 AS ID, 'Hoi' AS owner ,* FROM tanghoi.v289_m12_piv_distributions_20140923_067 UNION
SELECT '41903' AS dt, 68 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.v289_m12_piv_distributions_20140921_068 UNION 
SELECT '41904' AS dt, 68 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.v289_m12_piv_distributions_20140922_068 UNION 
SELECT '41905' AS dt, 68 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.v289_m12_piv_distributions_20140923_068 

--V289_s12_v_hhsize_distribution
create OR REPLACE view "pitteloudj"."M12_hhsize_dist" as
 SELECT '41903' AS dt, 66 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.V289_s12_v_hhsize_distribution_20140921_066 UNION 
SELECT '41904' AS dt, 66 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.V289_s12_v_hhsize_distribution_20140922_066 UNION 
SELECT '41905' AS dt, 66 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.V289_s12_v_hhsize_distribution_20140923_066 UNION 
SELECT '41903' AS dt, 65 AS ID, 'Hoi' AS owner ,* FROM tanghoi.V289_s12_v_hhsize_distribution_20140921_065 UNION 
SELECT '41904' AS dt, 65 AS ID, 'Hoi' AS owner ,* FROM tanghoi.V289_s12_v_hhsize_distribution_20140922_065 UNION 
SELECT '41905' AS dt, 65 AS ID, 'Hoi' AS owner ,* FROM tanghoi.V289_s12_v_hhsize_distribution_20140923_065 UNION 
SELECT '41903' AS dt, 67 AS ID, 'Hoi' AS owner ,* FROM tanghoi.V289_s12_v_hhsize_distribution_20140921_067 UNION 
SELECT '41904' AS dt, 67 AS ID, 'Hoi' AS owner ,* FROM tanghoi.V289_s12_v_hhsize_distribution_20140922_067 UNION 
SELECT '41905' AS dt, 67 AS ID, 'Hoi' AS owner ,* FROM tanghoi.V289_s12_v_hhsize_distribution_20140923_067 UNION 
SELECT '41903' AS dt, 68 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.V289_s12_v_hhsize_distribution_20140921_068 UNION 
SELECT '41904' AS dt, 68 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.V289_s12_v_hhsize_distribution_20140922_068 UNION 
SELECT '41905' AS dt, 68 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.V289_s12_v_hhsize_distribution_20140923_068 

--v289_s12_overall_consumption_hhlevel
create OR REPLACE view "pitteloudj"."M12_overall_VIEW" as
SELECT '41903' AS dt, 66 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.v289_s12_overall_consumption_hhlevel_20140921_066 UNION 
SELECT '41904' AS dt, 66 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.v289_s12_overall_consumption_hhlevel_20140922_066 UNION 
SELECT '41905' AS dt, 66 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.v289_s12_overall_consumption_hhlevel_20140923_066 UNION 
SELECT '41903' AS dt, 65 AS ID, 'Hoi' AS owner ,* FROM tanghoi.v289_s12_overall_consumption_hhlevel_20140921_065 UNION 
SELECT '41904' AS dt, 65 AS ID, 'Hoi' AS owner ,* FROM tanghoi.v289_s12_overall_consumption_hhlevel_20140922_065 UNION 
SELECT '41905' AS dt, 65 AS ID, 'Hoi' AS owner ,* FROM tanghoi.v289_s12_overall_consumption_hhlevel_20140923_065 UNION

SELECT '41903' AS dt, 67 AS ID, 'Hoi' AS owner ,* FROM tanghoi.v289_s12_overall_consumption_hhlevel_20140921_067 UNION 
SELECT '41904' AS dt, 67 AS ID, 'Hoi' AS owner ,* FROM tanghoi.v289_s12_overall_consumption_hhlevel_20140922_067 UNION 
SELECT '41905' AS dt, 67 AS ID, 'Hoi' AS owner ,* FROM tanghoi.v289_s12_overall_consumption_hhlevel_20140923_067 UNION
SELECT '41903' AS dt, 68 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.v289_s12_overall_consumption_hhlevel_20140921_068 UNION 
SELECT '41904' AS dt, 68 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.v289_s12_overall_consumption_hhlevel_20140922_068 UNION 
SELECT '41905' AS dt, 68 AS ID, 'Jose P' AS owner ,* FROM vespa_shared.v289_s12_overall_consumption_hhlevel_20140923_068   
  
  
  
  	
CREATE OR REPLACE view M12_avgminwatched_x_genderage AS
	SELECT  scaling_date
		,source, ID
		,CASE when source = 'BARB' and age = '0-19' then 'Undefined' else gender end AS gender
		,age
		,COUNT(distinct individuals)                        AS sample
		,SUM(weights)                                       AS weighted_sample
		,SUM(minutes_watched)			                    AS total_mins_watched
		,SUM(minutes_watched_scaled)	                    AS total_mins_scaled_watched
		,avg(minutes_watched)/60.00                         AS avg_hh_watched
		,SUM(minutes_watched_scaled)/weighted_sample/60.00  AS avg_hh_watched_scaled
		,SUM(minutes_watched_pv)			                    AS total_mins_watched_pv
		,SUM(minutes_watched_scaled_pv)	                    	AS total_mins_scaled_watched_pv
		,avg(minutes_watched_pv)/60.00                         	AS avg_hh_watched_pv
		,SUM(minutes_watched_scaled_pv)/weighted_sample/60.00	AS avg_hh_watched_scaled_pv
FROM    ( SELECT  scaling_date
					, ID
					,source
					,age
					,gender
					,household||'-'||person         AS individuals
					,MIN(ukbase)                    AS weights
					,SUM(CASE when viewing_type_flag = 0 then duration_mins else null end)             AS minutes_watched
					,SUM(CASE when viewing_type_flag = 0 then duration_weighted_mins else null end)    AS minutes_watched_scaled
					,SUM(CASE when viewing_type_flag = 1 then duration_mins else null end)             AS minutes_watched_pv
					,SUM(CASE when viewing_type_flag = 1 then duration_weighted_mins else null end)    AS minutes_watched_scaled_pv
			FROM    M12_dailychecks_weighted
			group   by  scaling_date, ID
						,source
						,age
						,gender
						,individuals
		)   AS base
group   by  scaling_date, ID
			,source
			,gender
			,age


 			
CREATE OR REPLACE VIEW M12_weighted_sumary			AS 
SELECT source, scaling_date, age, gender, daypart, ID
	, viewing_type_flag
	, COUNT(DISTINCT household) unique_hhs
	, COUNT(DISTINCT household||'-'||person) unique_ind
	, SUM (ukbase) sum_ukbase
	, SUM (viewersbase) sum_viewerbase
	, SUM (duration_mins) duration
	, SUM (duration_weighted_mins) duration_w 
FROM M12_dailychecks_weighted
GROUP BY 
source, scaling_date, age, gender, daypart, ID
	, viewing_type_flag
	
	
	
	
if  exists(  SELECT tname ,* FROM syscatalog where creator = user_name() and upper(tname) = upper('M12_weighted_sumary_head') and     tabletype = 'VIEW') 
drop view M12_weighted_sumary_head
 			
CREATE VIEW M12_weighted_sumary_head			AS 
SELECT source, scaling_date, v.age, v.gender, v.daypart, v.perc
	, viewing_type_flag
	, CASE 	WHEN M.account_number 	IS NULL THEN CAST(head AS VARCHAR)
			WHEN v.household 		IS NULL THEN CAST(person_head AS VARCHAR)
			ELSE NULL END 						AS head_of_hh
	, COUNT(DISTINCT household) unique_hhs
	, COUNT(DISTINCT household||'-'||v.person) unique_ind
	, SUM (ukbase) sum_ukbase
	, SUM (viewersbase) sum_viewerbase
	, SUM (duration_mins) duration
	, SUM (duration_weighted_mins) duration_w 
FROM M12_dailychecks_weighted as V
LEFT JOIN vespa_shared.V289_M08_SKY_HH_composition_v as M ON v.household = M.account_number AND v.person = m.HH_person_number
LEFT JOIN skybarb as S ON CAST(S.house_id AS VARCHAR) = v.household AND v.person = S.person
GROUP BY 
	source
	, scaling_date
	, v.age
	, v.gender
	, v.daypart
	, v.perc
	, viewing_type_flag	
	, head_of_hh