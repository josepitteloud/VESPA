--------------------- Emmas Dairy

CREATE VIEW v317_Emmas_view
AS 
SELECT 
    set_code,
    'Emmas' model_name,
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
	userfield_1 AS reference,
	userdatefield_2 AS Baby_DOB,
	userdatefield_1 AS Baby_Due_Date,
	userfield_2 AS Child_1_Gender,
	userfield_3 AS Child_2_Gender,
	userfield_4 AS Child_3_Gender,
	userfield_5 AS Number_of_Kids,
	userdatefield_3 AS Child1_DOB,
	userfield_6 AS Child2_DOB,
	userfield_7 AS Child3_DOB,

	CASE WHEN b.cb_key_household IS NULL THEN 0 ELSE 1 END AS adsmart_flag,
	weighting
	
FROM V317_Emmas_data_raw AS a
LEFT JOIN (SELECT DISTINCT cb_key_household 
			FROM adsmartables_20141126  AS cv 
			WHERE cv.cb_key_household > 0  AND cv.account_number IS NOT NULL) as b ON a.cb_key_household = b.cb_key_household 
LEFT JOIN (  SELECT DISTINCT sav.cb_key_household, l.scaling_segment_ID
			FROM CUST_SINGLE_ACCOUNT_VIEW   as sav
			JOIN vespa_analysts.SC2_intervals           as l    ON sav.account_number = l.account_number AND  '2014-12-15'  between l.reporting_starts and l.reporting_ends
			) AS v1 ON v1.cb_key_household = a.cb_key_household
LEFT JOIN  vespa_analysts.SC2_weightings as w ON w.scaling_day = '2014-12-15' AND w.scaling_segment_ID = v1.scaling_segment_ID
WHERE set_code = 'S_0622'




SELECT COUNT (*)
	, COUNT(userdtfield_1)
	, COUNT(userdtfield_2)
	, COUNT(userdtfield_3)
	, COUNT(userfield_1)
	, COUNT(userfield_2)
	, COUNT(userfield_3)
	, COUNT(userfield_4)
	, COUNT(userfield_5)
	, COUNT(userfield_6)
	, COUNT(userfield_7)
	, COUNT(userfield_8)
	, COUNT(userintfield_1)
	, COUNT(userintfield_2)
	, COUNT(userintfield_3)
FROM V317_Emmas_data_raw
WHERE set_code = 'S_0622'

