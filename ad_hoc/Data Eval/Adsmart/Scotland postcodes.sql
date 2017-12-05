------------- SELECTING ITV regions and postocdes 
SELECT 
    barb_desc_itv
  , cb_address_postcode_area
  , cb_address_postcode_outcode
  , cb_address_postcode
INTO adsmart_region_scotland
FROM sk_prod.BARB_TV_REGIONS
WHERE barb_desc_itv in ('North Scotland', 'Central Scotland', 'Border')
  
ALTER TABLE adsmart_region_scotland
ADD COLUMN region_v2 varchar(50) default null, metropolitan_area_and_itv_region varchar(100) default null
--------	 Populating the new columns
UPDATE adsmart_region_scotland base
SET region_v2 = region,
	metropolitan_area_and_itv_region = region

--------- Updating Border region according Sky definition	
UPDATE adsmart_region_scotland base
SET base.region_v2 = case when (base.region_v2 = 'Border') and
                                                     base.cb_address_postcode_area in ('AB','DD','DG','EH','FK','G','HS','IV','KA','KY','ML','PA','PH','TD')
                                                     then 'Border-Scotland'
                                                 when (base.region_v2 = 'Border') and
                                                     base.cb_address_postcode_area not in ('AB','DD','DG','EH','FK','G','HS','IV','KA','KY','ML','PA','PH','TD')
                                                     then 'Border-England'
                                            else base.region_v2 end
---------- SELECTING TOWNS to Metropolitan area definition	
SELECT TRIM(UPPER(cb_address_town))           as postcode_town
        , cb_address_postcode_area pc_area
INTO temp
FROM sk_prod.experian_consumerview
WHERE upper(pc_town) in ('GLASGOW'
							, 'EAST KILBRIDE'
							, 'CUMBERNAULD'
							, 'KILMARNOCK'
							, 'DUMBARTON'
							, 'LIVINGSTON' 
							, 'EDINBURGH')
GROUP BY							
	  pc_area 
	, postcode_town
											
UPDATE adsmart_region_scotland 
SET metropolitan_area_and_itv_region = CASE when a.postcode_town =  'GLASGOW' 	OR cb_address_postcode_area = 'G' then 'Glasgow metropolitan area'
											when a.postcode_town =  'EAST KILBRIDE' then 'Glasgow metropolitan area'
											when a.postcode_town =  'CUMBERNAULD' 	then 'Glasgow metropolitan area'
											when a.postcode_town =  'KILMARNOCK' 	OR cb_address_postcode_area = 'KA' then 'Glasgow metropolitan area'
											when a.postcode_town =  'DUMBARTON' 	then 'Glasgow metropolitan area'
											when a.postcode_town =  'EDINBURGH' 	OR cb_address_postcode_area = 'EH 'then 'Edinburgh metropolitan area'
											when a.postcode_town =  'LIVINGSTON' 	then 'Edinburgh metropolitan area'
										ELSE metropolitan_area_and_itv_region END
FROM adsmart_region_scotland AS BASE 
JOIN temp as a ON BASE.cb_address_postcode_area = a.pc_area
Commit; 
DROP TABLE temp
Commit
------------   	UPDATING Metropolitan areas
UPDATE adsmart_region_scotland base
SET base.metropolitan_area_and_itv_region = case when (base.metropolitan_area_and_itv_region = 'Border') and
                                                     base.cb_address_postcode_area in ('AB','DD','DG','EH','FK','G','HS','IV','KA','KY','ML','PA','PH','TD')
                                                     then 'Border-Scotland'
                                                 when (base.metropolitan_area_and_itv_region = 'Border') and
                                                     base.cb_address_postcode_area not in ('AB','DD','DG','EH','FK','G','HS','IV','KA','KY','ML','PA','PH','TD')
                                                     then 'Border-England'
                                            else base.metropolitan_area_and_itv_region end			

------------ UPDATING North Scotland region name according Sky standard											
UPDATE  		adsmart_region_scotland base
  SET metropolitan_area_and_itv_region = CASE WHEN metropolitan_area_and_itv_region = 'North Scotland' then 'Northern Scotland' ELSE metropolitan_area_and_itv_region END
  SET region_v2 = CASE WHEN region_v2 = 'North Scotland' then 'Northern Scotland' ELSE region_v2 END