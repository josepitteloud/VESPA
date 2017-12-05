--------------------- Twenty 
CREATE VIEW v317_Twenty_view
AS 
SELECT 
    set_code,
    'Twenty' model_name,
------------------------------------------------------
	a.cb_row_id,
    a.cb_key_household,
    a.cb_key_family,
    a.cb_key_individual,
    a.cb_address_postcode,
    a.cb_address_postcode_outcode,
    a.cb_address_postcode_area,
    a.cb_address_town,
------------------------------------------------------
    a.userdatefield_1     AS Due_Date,
    a.userdatefield_2     AS DOB_Sibling_1,
    a.userdatefield_3     AS DOB_Sibling_2,
    a.userfield_6         AS DOB_Sibling_3,
    a.userfield_7         AS DOB_Sibling_4,
	------------------------------------------------------
    a.userfield_2         AS gender_Sibling_1,
    a.userfield_3         AS gender_Sibling_2,
    a.userfield_4         AS gender_Sibling_3,
	------------------------------------------------------
    a.userfield_5         AS number_of_sibling,
    a.userfield_1			AS serial,
	CASE WHEN b.cb_key_household IS NULL THEN 0 ELSE 1 END AS adsmart_flag
FROM V317_Emmas_data_raw AS a
LEFT JOIN (SELECT DISTINCT cb_key_household 
			FROM adsmartables_20141126  AS cv 
			WHERE cv.cb_key_household > 0  AND cv.account_number IS NOT NULL) as b ON a.cb_key_household = b.cb_key_household 
WHERE set_code in ('S_0613','S_0614', 'S_0620', 'S_0621')