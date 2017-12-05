----------------CREATING Ordnace summary table
TRUNCATE TABLE 		MDU_ORD_count
commit;
INSERT INTO 		MDU_ORD_count
SELECT      ord.cb_key_household
            , 0
			, COUNT (DISTINCT account_number) Acct_hits
			, COUNT (DISTINCT ord.cb_row_id)AS HH_ADD
			,0
FROM        sk_prod.ord_survey_addr_point ord
LEFT JOIN 	sk_prod.cust_single_account_view sav   on ord.cb_key_household = sav.cb_key_household
WHERE       valid_flag = 1
GROUP BY    ord.cb_key_household;
commit;
----------------CREATING Address Plus summary table			
TRUNCATE TABLE 		MDU_addr_base3
commit;
INSERT INTO 		MDU_addr_base3			
SELECT      add3.cb_key_household
            , 0
			, COUNT (DISTINCT account_number) Acct_hits
			, COUNT (DISTINCT add3.cb_row_id) AS hits
INTO 		MDU_addr_base3 
FROM        sk_prodreg.addressbase_plus as add3
LEFT JOIN 	sk_prod.cust_single_account_view sav   on add3.cb_key_household = sav.cb_key_household
GROUP BY    add3.cb_key_household;
commit;
----------------CREATING combined summary table
TRUNCATE TABLE MDU_ALL_count
commit;
INSERT INTO MDU_ALL_count
SELECT 
	  isnull(a.cb_key_household , 0) AS ord_HH_key
	, isnull(b.cb_key_household , 0) AS adr_HH_key
	, a.HH_ADD ord_HH
	, b.hits  adr_HH	
	, a.Acct_hits ord_Acct
	, b.Acct_hits adr_Acct	
FROM MDU_ORD_count as a 
FUll OUTER JOIN MDU_addr_base3 as b ON a.cb_key_household = b.cb_key_household
commit;
----------------Summary of tables 	(separated)		
SELECT 
    'Addr3 COUNT' DB
    , count(*) Unique_HH 
    , sum(hits) Total_HH
    , sum(Acct_hits) Total_Acct
FROM MDU_addr_base3
UNION
SELECT 
    'Ord COUNT' DB
    , count(*) Unique_HH 
    , sum(HH_ADD) Total_HH
    , sum(Acct_hits) Total_Acct
FROM MDU_ORD_count
----------------Combined Stats
SELECT 
      count(*) rowcount    
    , sum(CASE WHEN ord_HH_key <> 0 AND adr_HH_key <> 0 THEN 1 ELSE NULL END) matching
    , sum(CASE WHEN ord_HH_key <> 0 AND adr_HH_key = 0 THEN 1 ELSE NULL END) ord_valid
    , sum(CASE WHEN ord_HH_key = 0 AND adr_HH_key <> 0 THEN 1 ELSE NULL END) addr_valid
    , sum(CASE WHEN ord_HH <> adr_HH THEN 1 ELSE 0 END) non_matching_HH_count
FROM MDU_ALL_count
----------------- Comparing MDU structures
SELECT top 10 * 
from MDU_ALL_count 
WHERE ord_HH = adr_HH AND adr_HH >3

SELECT * FROM  sk_prodreg.addressbase_plus WHERE cb_key_household ='1961748861026304'
SELECT * FROM  sk_prod.ord_survey_addr_point WHERE cb_key_household ='1961748861026304'
----------------
SELECT 
	count(CASE WHEN a.multi_occ_count = 0 THEN 1 ELSE null) non_multi
	, count(CASE WHEN a.multi_occ_count <> 0 THEN 1 ELSE null)  multi
FROM sk_prodreg.addressbase_plus as a
JOIN MDU_ALL_count as b ON a.cb_key_household = b.cb_key_household
WHERE adr_HH > 3


SELECT * FROM sk_prod.ord_survey_addr_point WHERE os_block = 40665702867494
SELECT * FROM sk_prodreg.addressbase_plus WHERE UPRN = 10023491608


----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
TRUNCATE TABLE MDU_uprn_add3;
INSERT INTO MDU_uprn_add3
SELECT 
	UPRN
	, v.c_uprn as child_uprn
	, multi_occ_count MDU

FROM sk_prodreg.addressbase_plus as d
LEFT JOIN 	(SELECT 
					parent_uprn p_uprn
				, 	count(uprn)  AS c_uprn
			FROM sk_prodreg.addressbase_plus
			WHERE parent_uprn IS NOT NULL 
				AND (state in (2,3) OR state is null)			-- 2= In use 	3= unoccupied 		null = Unknown
				AND change_type <>'D' 				-- Filter Deleted
				AND(CLASS NOT LIKE 'U%' 		-- Unclassified
					OR CLASS NOT LIKE 'O%'		-- Ornamental objects 
					OR CLASS NOT LIKE 'L%'		-- LAND??
					OR class NOT LIKE 'CA%'     -- Commercial Agricultural 
					OR class NOT LIKE 'CC6%'    -- Community Services Cemetery/ Crematorium/ Graveyard.
					OR class NOT LIKE 'CC9%'    -- Public Household Waste Recycling
					OR class NOT LIKE 'CC10%'   -- Recycling Site
					OR class NOT LIKE 'CL2%'    -- Leisure Holiday/Campsite
					OR class NOT LIKE 'CL9%'    -- Beach Hut (Recreational, Non-Residential Use Only)
					OR class NOT LIKE 'CR11%'   -- Retail Automated Teller Machine (ATM)
					OR class NOT LIKE 'CS%'     -- Storage Land 
					OR class NOT LIKE 'CT2%'    -- Transport Bus Shelter
					OR class NOT LIKE 'CT3%'    -- Car/Coach/ Commercial Vehicle/ Taxi Parking/Park And Ride Site
					OR class NOT LIKE 'CT5%'    -- Marina
					OR class NOT LIKE 'CT6%'    -- Mooring
					OR class NOT LIKE 'CT7%'    -- Railway Asset
					OR class NOT LIKE 'CT9%'    -- Transport Track/Way
					OR class NOT LIKE 'CT10%'   -- Vehicle Storage
					OR class NOT LIKE 'CT11%'   -- Transport Related infrastructure
					OR class NOT LIKE 'CT12%'   -- Overnight Lorry Park
					OR class NOT LIKE 'CU%'     -- Utility 
					OR class NOT LIKE 'CX8%'    -- Emergency/Rescue service Police Box/Kiosk
					OR class NOT LIKE 'CZ%'     -- Information 
					OR class NOT LIKE 'RC1%'    -- Residential Car Park Space 
					OR class NOT LIKE 'RG2%'    -- Garage 
					) 
			GROUP BY p_uprn
			) AS v ON v.p_uprn  = d.uprn
WHERE 
	parent_uprn IS NULL 
	AND (state in (2,3) OR state is null)			---- 2= In use 	3= unoccupied
	AND change_type <>'D' 				-- Filter Deleted
	AND(	CLASS NOT LIKE 'U%' -- Unclassified
		OR CLASS NOT LIKE 'O%'	-- Ornamental objects 
		OR CLASS NOT LIKE 'L%'	-- LAND??
		OR class NOT LIKE 'CA%'       -- Commercial Agricultural 
		OR class NOT LIKE 'CC6%'      -- Community Services Cemetery/ Crematorium/ Graveyard.
		OR class NOT LIKE 'CC9%'      -- Public Household Waste Recycling
		OR class NOT LIKE 'CC10%'     -- Recycling Site
		OR class NOT LIKE 'CL2%'      -- Leisure Holiday/Campsite
		OR class NOT LIKE 'CL9%'      -- Beach Hut (Recreational, Non-Residential Use Only)
		OR class NOT LIKE 'CR11%'     -- Retail Automated Teller Machine (ATM)
		OR class NOT LIKE 'CS%'       -- Storage Land 
		OR class NOT LIKE 'CT2%'      -- Transport Bus Shelter
		OR class NOT LIKE 'CT3%'      -- Car/Coach/ Commercial Vehicle/ Taxi Parking/Park And Ride Site
		OR class NOT LIKE 'CT5%'      -- Marina
		OR class NOT LIKE 'CT6%'      -- Mooring
		OR class NOT LIKE 'CT7%'      -- Railway Asset
		OR class NOT LIKE 'CT9%'      -- Transport Track/Way
		OR class NOT LIKE 'CT10%'     -- Vehicle Storage
		OR class NOT LIKE 'CT11%'     -- Transport Related infraestructure
		OR class NOT LIKE 'CT12%'     -- Overnight Lorry Park
		OR class NOT LIKE 'CU%'       -- Utility 
		OR class NOT LIKE 'CX8%'      -- Emergency/Rescue service Police Box/Kiosk
		OR class NOT LIKE 'CZ%'       -- Information 
		OR class NOT LIKE 'RC1%'      -- Residential Car Park Space 
		OR class NOT LIKE 'RG2%'      -- Garage 
		)

----------------------------------------

SELECT 
  CASE WHEN (child_uprn = 0 OR child_uprn is null) THEN 0
          WHEN child_uprn = 1 THEN 1
          WHEN child_uprn = 2 THEN 2
          WHEN child_uprn = 3 THEN 3
          WHEN child_uprn >= 4 THEN 4
          ELSE -1
        END AS CALC_MDU
  ,CASE WHEN (MDU = 0 OR MDU is null) THEN 0
          WHEN MDU = 1 THEN 1
          WHEN MDU = 2 THEN 2
          WHEN MDU = 3 THEN 3
          WHEN MDU >= 4 THEN 4
          ELSE -1 END AS MDU_FLAG      
   , count(UPRN) AS HITS
FROM MDU_uprn_add3 
GROUP BY CALC_MDU, MDU_FLAG


---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT 
    os_block
  , MAX(household_count_osblock)    AS max_occ
  , MAX(household_count_osblock-invalid_occurrences_osblock)    AS    net_occ
  , MAX(valid_occurrences_osblock)      AS    valid_occ_os_block
  , MAX(valid_occurrences_premise)      AS    valid_occ_premise
  , COUNT(DISTINCT cb_key_premise)      AS    dist_premises
  , count(*) AS hits
INTO MDU_ord_count_1
FROM        sk_prod.ord_survey_addr_point 
GROUP BY 
  os_block

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT 
  CASE WHEN (max_occ = 0 OR max_occ is null) THEN 0
          WHEN max_occ = 1 THEN 1
          WHEN max_occ = 2 THEN 2
          WHEN max_occ = 3 THEN 3
          WHEN max_occ >= 4 THEN 4
          ELSE -1
  END AS max_occ_t
  , CASE WHEN (net_occ = 0 OR net_occ is null) THEN 0
        WHEN net_occ = 1 THEN 1
        WHEN net_occ = 2 THEN 2
        WHEN net_occ = 3 THEN 3
        WHEN net_occ >= 4 THEN 4
        ELSE -1
  END AS net_occ_t
  , count(os_block)

FROM MDU_ord_count_1

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT 
  CASE WHEN (valid_occ_os_block = 0 OR valid_occ_os_block is null) THEN 0
          WHEN valid_occ_os_block = 1 THEN 1
          WHEN valid_occ_os_block = 2 THEN 2
          WHEN valid_occ_os_block = 3 THEN 3
          WHEN valid_occ_os_block >= 4 THEN 4
          ELSE -1
  END AS max_occ_t
  , CASE WHEN (valid_occ_premise = 0 OR valid_occ_premise is null) THEN 0
        WHEN valid_occ_premise = 1 THEN 1
        WHEN valid_occ_premise = 2 THEN 2
        WHEN valid_occ_premise = 3 THEN 3
        WHEN valid_occ_premise >= 4 THEN 4
        ELSE -1
  END AS net_occ_t
  , count(os_block)

FROM MDU_ord_count_1-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------



SELECT  
	  COUNT(DISTINCT cb_key_household) HH
	, SUM(sky_homes_property_new) Sky_homes
	, SUM(valid_occurrences_osblock) Occurrences
	, SUM(CASE WHEN ord.valid_occurrences_osblock > 3 then 1 else 0 end) 		AS SUM_MDU
    , SUM(CASE WHEN ord.valid_occurrences_osblock < 3 then 1 else 0 end)		AS SUM_SDU
FROM        sk_prod.ord_survey_addr_point ord
WHERE       valid_flag = 1 

SELECT  
	  COUNT(DISTINCT cb_key_household) HH
	, SUM(abp.multi_occ_count) Occurrences
	, SUM(abp.multi_occ_count+1) Occurrences_plus_1
	, SUM(CASE WHEN abp.multi_occ_count < 2 then 1 else 0 end ) 		AS SUM_MDU
    , SUM(CASE WHEN abp.multi_occ_count < 2 then 1 else 0 end)			AS SUM_SDU
FROM        sk_prodreg.addressbase_plus abp
WHERE       valid_flag = 1 



----drop table mdu_base_analysis_match;

-------------------IDENTIFYING MDUs AS IDENTIFIED BY CURRENT METHOD-------------------
SELECT      ord.cb_key_household
            ,sav.account_number
            ,ord.sky_homes_property_new
            ,ord.valid_occurrences_osblock
            ,CASE WHEN ord.valid_occurrences_osblock > 3 then 1 else 0 end 		AS current_mdu_flag
            ,CASE WHEN ord.valid_occurrences_osblock < 3 then 1 else 0 end 		AS current_sdu_flag
			,CASE WHEN account_number is not null then 1 else 0 end				AS current_cust_flag
INTO 		mdu_base_analysis_match
FROM        sk_prod.ord_survey_addr_point ord
LEFT JOIN 	sk_prod.cust_single_account_view sav   on ord.cb_key_household = sav.cb_key_household
WHERE       valid_flag = 1
GROUP BY    ord.cb_key_household
			,sav.account_number
			,sky_homes_property_new
			,valid_occurrences_osblock;

----------------------33444211
/* SELECT top 100 * FROM mdu_base_analysis_match WHERE equal_hh_key_flag = 0 and abp_cb_key_household is not null;

---------------------CALCULATING CURRENT CUSTOMER FLAG--------------------
 ADDED into previos query
alter table mdu_base_analysis_match
add current_cust_flag  integer default 0;



update mdu_base_analysis_match
set current_cust_flag = CASE WHEN account_number is not null then 1
                                else 0
                                end;
*/
-------------------33444211
---------------------------------CHECKS FOR ASHA METHOD----------------------------
/*
SELECT top 1* FROM sk_prod.existing_irs;
SELECT min(cb_data_date), max(cb_data_date) FROM sk_prod.existing_irs;
SELECT min(referraldatetime), max(referraldatetime) FROM sk_prod.tp_AshaReferral;
SELECT top 1 * FROM CITeam.sk2070_TA_Agents;
SELECT top 10 * FROM sk_prodreg.addressbase_plus_20130607;
SELECT top 10 * FROM sk_prodreg.addressbase_plus_20130621;
SELECT count(*), multi_occ_count FROM sk_prodreg.addressbase_plus GROUP BY multi_occ_count;
*/

-----------------GETTING DATA FROM ADDRESS BASE PLUS--------------
 SELECT     abp.cb_key_household 	AS abp_cb_key_household
			,1 						AS addr_base_plus_flag
			,abp.multi_occ_count
			,CASE WHEN abp.multi_occ_count > 2 then 1 else 0 end 	AS abp_mdu_flag
			,CASE WHEN abp.multi_occ_count < 2 then 1 else 0 end 	AS abp_sdu_flag
			,abp.street_description
			,abp.sub_building_name
			,abp.thoroughfare_name
			,abp.town_name
			,abp.postcode
			,abp.building_name
			,abp.cb_address_line_1
			,abp.cb_address_line_2
			,abp.cb_address_line_3
			,abp.cb_address_line_4
			,uprn
			,usrn
INTO 		addr_base3
FROM    	mdu_base_analysis_match base
LEFT JOIN 	sk_prodreg.addressbase_plus 	AS abp 	ON abp.cb_key_household = base.cb_key_household
GROUP BY    abp_cb_key_household
			,abp.multi_occ_count
			,abp.street_description
			,abp.sub_building_name
			,abp.thoroughfare_name
			,abp.town_name
			,abp.postcode
			,abp.building_name
			,abp.cb_address_line_1
			,abp.cb_address_line_2
			,abp.cb_address_line_3
			,abp.cb_address_line_4
			,uprn
			,usrn
			;
---------------25984810
------set temporary option Query_Temp_Space_Limit = 50000000;


alter table mdu_base_analysis_match
add (uprn bigint default null
    ,usrn bigint default null
	,street_description varchar(50) default null
    ,sub_building_name varchar(50) default null
    ,thoroughfare_name varchar(50) default null
    ,town_name         varchar(50) default null
    ,postcode          varchar(50) default null
	,abp_cb_key_household bigint default null
    ,addr_base_plus_flag  integer default 0
    ,multi_occ_count      integer default 0
    ,abp_mdu_flag         integer default 0
    ,abp_sdu_flag         integer default 0
	,multi_occ_count_v2 integer default 0)


update mdu_base_analysis_match base
set  base.abp_cb_key_household  =  temp.abp_cb_key_household
    ,base.addr_base_plus_flag   =  temp.addr_base_plus_flag
    ,base.multi_occ_count       =  temp.multi_occ_count
    ,base.abp_mdu_flag          =  temp.abp_mdu_flag
    ,base.abp_sdu_flag          =  temp.abp_sdu_flag
    ,street_description         =  temp.street_description
    ,sub_building_name          =  temp.sub_building_name
    ,thoroughfare_name          =  temp.thoroughfare_name
    ,town_name                  =  temp.town_name
    ,postcode                   =  temp.postcode
    ,building_name              = temp.building_name
    ,cb_address_line_1          = temp.cb_address_line_1
    ,cb_address_line_2          = temp.cb_address_line_2
    ,cb_address_line_3          = temp.cb_address_line_3
    ,cb_address_line_4         = temp.cb_address_line_4
    ,uprn                       =  temp.uprn
    ,usrn                       =  temp.usrn
FROM addr_base3 temp
WHERE base.cb_key_household = temp.abp_cb_key_household;

/*
------------------32,835,815

SELECT count(*), addr_base_plus_flag FROM mdu_base_analysis_match GROUP BY addr_base_plus_flag;
SELECT count(*), multi_occ_count FROM mdu_base_analysis_match GROUP BY multi_occ_count;


SELECT top 1* FROM sk_prodreg.addressbase_plus WHERE cb_key_household = 1961696327368704;
SELECT count(*), equal_hh_key_flag FROM mdu_base_analysis_match WHERE addr_base_plus_flag = 1 GROUP BY equal_hh_key_flag;
SELECT count(*), multi_occ_count FROM mdu_base_analysis_match WHERE addr_base_plus_flag = 1 and equal_hh_key_flag = 1 GROUP BY multi_occ_count;

SELECT top 100 * FROM mdu_base_analysis_match;

alter table mdu_base_analysis_match
drop multi_occ_count_v2
drop current_flag;
alter table mdu_base_analysis_match
drop abp_flag;
*/

alter table mdu_base_analysis_match
add ;

update mdu_base_analysis_match
set multi_occ_count_v2 = multi_occ_count+1;

--------------------MDU, SDU FLAGS----------------------------
alter table mdu_base_analysis_match
add (current_flag  varchar(20) default null
   ,abp_flag      varchar(20) default null);



update mdu_base_analysis_match
set current_flag= CASE WHEN valid_occurrences_osblock > 3 then 'mdu'
                                else 'sdu'
                                end
   ,abp_flag    =  CASE WHEN multi_occ_count_v2 > 3 then 'mdu'
                                else 'sdu'
                                end;


--------------------33444211




/*
SELECT count(*),current_flag, abp_flag FROM mdu_base_analysis_match GROUP BY current_flag, abp_flag;


1961696327368704
1961696339951616
1961696341000192
1961696342048768
1961696350437376
1961725306863616
1961725310009344
1961725311057920
1961725312106496
1961725313155072


1961725329932288
1961725327835136
1961725326786560
1961725325737984
1961725324689408
1961725323640832
1961725322592256
1961725320495104
1961725318397952
1961725316300800


1963272132624384
1963272125284352
1963272124235776
1963272123187200
1963272122138624
1963272120041472
1963272118992896
1963272117944320
1963272116895744
1963272115847168


69820914724765696
69820914725814272
69820914727911424
69820914728960000
69820914730008576
69820914731057152
69820914732105728
69820914734202880
69820914736300032
69820914738397184



1961725314203648
1961725316300800
1961725318397952
1961725320495104
1961725322592256
1961725323640832
1961725324689408
1961725325737984
1961725326786560
1961725327835136
1961725329932288


SELECT top 10* FROM sk_prodreg.addressbase_plus WHERE cb_key_household = 1961725314203648;
SELECT top 10* FROM sk_prodreg.addressbase_plus WHERE cb_key_household = 1961725316300800;
SELECT top 10* FROM sk_prodreg.addressbase_plus WHERE cb_key_household = 1961725318397952;
SELECT top 10* FROM sk_prodreg.addressbase_plus WHERE cb_key_household = 1961725320495104;
SELECT top 10* FROM sk_prodreg.addressbase_plus WHERE cb_key_household = 1961725322592256;
SELECT top 10* FROM sk_prodreg.addressbase_plus WHERE cb_key_household = 1961725323640832;
SELECT top 10* FROM sk_prodreg.addressbase_plus WHERE cb_key_household = 1961725324689408;
SELECT top 10* FROM sk_prodreg.addressbase_plus WHERE cb_key_household = 1961725325737984;
SELECT top 10* FROM sk_prodreg.addressbase_plus WHERE cb_key_household = 1961725326786560;

SELECT top 10* FROM sk_prodreg.addressbase_plus WHERE cb_key_household = 1961725327835136;



*/



---------------------------------CALCULATING PIVOT--------------------------------
SELECT count(*), valid_occurrences_osblock, current_mdu_flag, current_sdu_flag, multi_occ_count_v2, abp_mdu_flag, abp_sdu_flag
FROM mdu_base_analysis_match
GROUP BY valid_occurrences_osblock, current_mdu_flag, current_sdu_flag, multi_occ_count_v2, abp_mdu_flag, abp_sdu_flag;

grant all on mdu_base_analysis_match to public;

/*


SELECT top 10 * FROM mdu_base_analysis_match
SELECT distinct multi_occ_count FROM sk_prodreg.addressbase_plus WHERE street_description = 'A198 (23) FROM GREEN CRAIGS TO KIRK ROAD'
SELECT count(*),valid_occurrences_osblock FROM mdu_base_analysis_match GROUP BY valid_occurrences_osblock;

SELECT distinct  street_description,  sub_building_name
                , thoroughfare_name
                , town_name, postcode, building_name, cb_address_line_1, cb_address_line_2,
                cb_address_line_3, cb_address_line_4, multi_occ_count_v2, valid_occurrences_osblock, uprn, usrn
                into ---drop table
                        temp3
                FROM mdu_base_analysis_match WHERE current_flag = 'mdu' and abp_flag = 'sdu';





SELECT top 10 * FROM temp3





SELECT top 5000* FROM temp3 WHERE street_description is not null and building_name is not null ;

SELECT top 10 * FROM sk_prodreg.addressbase_plus;

SELECT top 10 * FROM sk_prodreg.addressbase_plus WHERE street_description = ('UNION STREET') and

SELECT count(*),multi_occ_count_v2 FROM mdu_base_analysis_match GROUP BY multi_occ_count_v2;

SELECT count(*),multi_occ_count_v2,valid_occurrences_osblock FROM mdu_base_analysis_match GROUP BY multi_occ_count_v2,valid_occurrences_osblock;

SELECT count(*), valid_occurrences_osblock



cb_source_cd	
cb_data_date	
cb_row_id	
cb_seq_id	
address_point_status_flag	
building_name	
building_number	
change_date	
change_type	
delivery_point_suffix	
department_name	
dependent_locality_name	
dependent_thoroughfare_name	
double_dependent_locality_name	
eastings_char	
eastings_num	
fsd_rating	
fsd_value	
household_count_osblock	
household_count_premise	
invalid_occurrences_osblock	
invalid_occurrences_premise	
multi_occupancy_count	
northings_char	
northings_num	
not_used	
occurrences_osblock	
occurrences_premise	
organisation_name	
os_block	
osapr	
osblock_count_premise	
po_box_number	
post_town_name	
postcode	
pq_value	
rm_version	
sky_homes_property	
sky_homes_property_new	
sub_building_name	
thoroughfare_name	
valid_flag	
valid_occurrences_osblock	
valid_occurrences_premise	
cb_address_addkey	
cb_address_barcode	
cb_address_buildingname	
cb_address_buildingno	
cb_address_county	
cb_address_deplocality	
cb_address_depstreet	
cb_address_dps	
cb_address_line_1	
cb_address_line_2	
cb_address_line_3	
cb_address_line_4	
cb_address_line_5	
cb_address_line_6	
cb_address_locality	
cb_address_mailable_flag	
cb_address_organisation	
cb_address_orgkey	
cb_address_paf_date	
cb_address_postcodecb_address_postcode_area	
cb_address_postcode_incode	
cb_address_postcode_outcode	
cb_address_postcode_sector	
cb_address_raw_line_1	
cb_address_raw_line_2	
cb_address_raw_line_3	
cb_address_raw_line_4	
cb_address_raw_line_5	
cb_address_raw_line_6	
cb_address_raw_postcode	
cb_address_status	
cb_address_street	
cb_address_subbuilding	
cb_address_town	
cb_key_household	
cb_key_family	
cb_key_individual	
cb_key_premise

*/


