/*SELECT DISTINCT postal_county frOM WEATHER_POST_DISTRICT_REFERENCE

SELECT count(*) FROM sk_prod.EXPERIAN_CONSUMERVIEW
SELECT cb_address_county, count(*) FROM sk_prod.CACI_Social_class
GROUP BY cb_address_county

sp_columns CUST_SINGLE_ACCOUNT_VIEW 

cb_address_county           county		
	
SELECT Date_of_viewing , COUNT(*)  qty 
  FROM WEATHER_VIEWING_DATA_DEC_1H 
  WHERE account_number is not null
  GROUP BY Date_of_viewing   
  
  */


--      WEATHER_VIEWING_SAMPLE_DEC12_1H
SELECT 'sep' ,count(*) FROM WEATHER_VIEWING_DATA_SEP
UNION
SELECT 'dec', count(*) FROM WEATHER_VIEWING_DATA_DEC_1H
 
 
 
 
SELECT   
    account_number,
    --cb_key_household,
    CAST(event_start_date_time_utc as DATE) Date_of_viewing, 
    CASE   WHEN DATEPART(hh, event_start_date_time_utc) BETWEEN 0 AND 7 THEN '0'
			 WHEN DATEPART(hh, event_start_date_time_utc) BETWEEN 8 AND 11 THEN '1'
			 WHEN DATEPART(hh, event_start_date_time_utc) BETWEEN 12 AND 15 THEN '2'
			 WHEN DATEPART(hh, event_start_date_time_utc) BETWEEN 16 AND 19 THEN '3'
			 WHEN DATEPART(hh, event_start_date_time_utc) BETWEEN 20 AND 23 THEN '4'
			ELSE '99'
	  END                 Time_of_Day,
        genre_description,
    sub_genre_description,
    SUM(DATEDIFF(minute, event_start_date_time_utc, CASE WHEN (capped_full_flag = 1 OR capped_partial_flag = 1)  
            THEN  capping_end_date_time_utc 
            ELSE  event_end_date_time_utc END)) duration_capped
  INTO WEATHER_VIEWING_DATA_OCT
  FROM Sk_prod.VESPA_EVENTS_ALL
  WHERE event_start_date_time_utc BETWEEN '2012-10-01' AND '2012-10-31 23:59:59'  
  GROUP BY   
    account_number,
    Date_of_viewing,
    Time_of_Day,
    genre_description,
    sub_genre_description

 
 
INSERT INTO   WEATHER_VIEWING_SAMPLE_DEC12_1H
SELECT  
    cb_address_postcode_district District,
    children_in_hh children,
    employment_lifestage,
    h_affluence,
    h_lifestage,
    household_composition hh_compo,
    mosaic_segments,
    Date_of_viewing,
    Time_of_Day,
    genre_description,
    sub_genre_description,
    sum(duration_capped) Duration

FROM WEATHER_SAV_DATA as a
JOIN WEATHER_VIEWING_DATA_OCT as b ON a.account_number = b.account_number
GROUP BY 
    cb_address_postcode_district,
    children_in_hh,
    employment_lifestage,
    h_affluence,
    h_lifestage,
    household_composition,
    mosaic_segments,
    Date_of_viewing,
    Time_of_Day,
    genre_description,
    sub_genre_description


SELECT  type_of_viewing_event,
month (event_start_date_time_utc ) mes,
  COUNT(*) as records
from sk_prod.vespa_events_all 
WHERE cast(event_start_date_time_utc as date ) BETWEEN  '2012-09-01' AND  '2012-12-31'
GROUP BY type_of_viewing_event,
month (event_start_date_time_utc ) 


SELECT cb_address_postcode_district,
  COUNT (DISTINCT account_number) acct
FROM WEATHER_SAV_DATA
GROUP  BY cb_address_postcode_district




SELECT 
cb_address_postcode_district District,
Date_of_viewing,
COUNT(DISTINCT b.account_number)
FROM WEATHER_SAV_DATA as a
JOIN WEATHER_VIEWING_DATA_OCT as b ON a.account_number = b.account_number
GROUP BY District,
Date_of_viewing


