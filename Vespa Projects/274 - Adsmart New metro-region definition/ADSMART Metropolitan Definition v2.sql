--------------------------------------
--	ADSMART New Metropolitan area definition 
-- 	This script creates a table with actual and New Metropolitan  and region information by account
--  
-- Write: by Jose Pitteloud
-- DATE: 05/02/2014
---------------------------------------

CREATE TABLE ADSMART_Metropolitan_raw
	( 	  ID 				int 	Identity
		, household_key 	bigint 		default null
		, postcode 			varchar(10) default null
		, postal_area 		varchar(4) default null
		, postal_district 	varchar(6) default null
		, actual_metro_area varchar(60) default null
		, new_metro_area	varchar(60) default null
		, sky_account		varchar(20) default null 
		, SKY_active bit default 0

	)
commit
------------------------------------------------ INSERTING ACCOUNTS INFO FROM SAV
INSERT INTO ADSMART_Metropolitan_raw (household_key
					, 	postcode	, postal_area		, postal_district
					,  	actual_metro_area	, sky_account)	
SELECT
        cb_key_household
        , TRIM(REPLACE(cb_address_postcode,'  ', ' ')) postcode
        , cb_address_postcode_area
		, cb_address_postcode_outcode
        , region
		, account_number
FROM
        sk_prod.CUST_SINGLE_ACCOUNT_VIEW as sav
commit
------------------------------------------------ CREATING INDEXES
CREATE hg INDEX idxacc1 ON ADSMART_Metropolitan_raw(sky_account);
CREATE hg INDEX idxhh1 ON ADSMART_Metropolitan_raw(household_key);
CREATE hg INDEX idxhpost1 ON ADSMART_Metropolitan_raw(postal_district);
COMMIT;
------------------------------------------------ FLAGGING ACTIVE ACCOUNTS
UPDATE ADSMART_Metropolitan_raw 
set SKY_active = 1 
WHere EXISTS (SELECT account_number from  sk_prod.CUST_SINGLE_ACCOUNT_VIEW as sav 
		where sav.cb_key_household = household_key 
				AND sky_account = sav.account_number
				AND cust_active_dtv =1)
commit
------------------------------------------------------------------------------------------------------------
------------------------------------	UPDATE new metropolitan area definition
------------------------------------------------------------------------------------------------------------
-- Metropolitan update by postal DISTRICT
UPDATE ADSMART_Metropolitan_raw
SET new_metro_area = 'Birmingham metropolitan area'
WHERE 
  postal_district like ('B0%') OR postal_district like ('B1%')
  OR postal_district like ('B2%') OR postal_district like ('B3%')
  OR postal_district like ('B4%') OR postal_district like ('B5%')
  OR postal_district like ('B6%') OR postal_district like ('B7%')
  OR postal_district like ('B8%') OR postal_district like ('B9%')
  OR postal_district in ('DY1', 'DY2','DY3', 'DY4','DY5', 'DY6','DY7','DY8','DY9', 'DY10','DY11', 'DY12','DY13')
  OR postal_district in ('WV1', 'WV2','WV3', 'WV4','WV5', 'WV6','WV7','WV8','WV9', 'WV10','WV11', 'WV12','WV13','WV14')
  OR postal_district in ('WS1', 'WS2','WS3', 'WS4','WS5', 'WS6','WS7','WS8','WS9', 'WS10','WS11', 'WS12','WS13','WS14')
  OR (postal_district like ('CV%') AND postal_district not like ('CV36'));

--    Liverpool
UPDATE ADSMART_Metropolitan_raw
SET new_metro_area = 'Liverpool/Birkenhead metropolitan area'
WHERE 
  postal_district like ('L0%') OR postal_district like ('L1%')
  OR postal_district like ('L2%') OR postal_district like ('L3%')
  OR postal_district like ('L4%') OR postal_district like ('L5%')
  OR postal_district like ('L6%') OR postal_district like ('L7%')
  OR postal_district like ('L8%') OR postal_district like ('L9%')
  OR postal_district like ('CH%')
  OR (postal_district like ('WN%') AND postal_district not like ('WN7'))
  OR postal_district in ('PR7','PR8','PR9','WA1','WA2','WA4','WA5','WA6','WA7','WA8','WA9','WA10','WA11','WA12')
  OR postal_district in ('CW8','CW9');

--    Manchester 
UPDATE ADSMART_Metropolitan_raw
SET new_metro_area = 'Manchester metropolitan area'
WHERE 
     postal_district like ('M0%') OR postal_district like ('M1%')  OR postal_district like ('M2%') OR postal_district like ('M3%')
  OR postal_district like ('M4%') OR postal_district like ('M5%')  OR postal_district like ('M6%') OR postal_district like ('M7%')
  OR postal_district like ('M8%') OR postal_district like ('M9%')
  OR postal_district like ('OL%')
  OR postal_district like ('BL%')
  OR (postal_district like ('SK%') AND postal_district not like ('SK17'))
  OR postal_district in ('WN7','WA3','WA13','WA14','WA15','WA16');

--    Brighton 
UPDATE ADSMART_Metropolitan_raw
SET new_metro_area = 'Brighton/Worthing/Littlehampton metropolitan area'
WHERE 
     postal_district like ('BN%')
  OR postal_district in ('RH10','RH11', 'RH12','RH13','RH14','RH15','RH16','RH17','RH18','RH19','RH20')
  OR postal_district in ('TN6','TN7','TN20','TN21','TN22');

--    Cardiff
UPDATE ADSMART_Metropolitan_raw
SET new_metro_area = 'Cardiff and South Wales valleys metropolitan area'
WHERE 
      postal_district like ('CF%')
  OR postal_district in ('NP1','NP2','NP3','NP4','NP5','NP6','NP9','NP10','NP11','NP12','NP13','NP14')
  OR postal_district in ('NP17','NP18','NP19','NP20','NP21','NP22','NP23','NP24','NP26');

--    Leicester
UPDATE ADSMART_Metropolitan_raw
SET new_metro_area = 'Leicester metropolitan area'
WHERE 
     postal_district like ('LE%');

--    Bristol 
UPDATE ADSMART_Metropolitan_raw
SET new_metro_area = 'Bristol metropolitan area'
WHERE 
     postal_district like ('BS%')
  OR postal_district in ('BA1','BA2','BA3','BA5','BA15');

--    Portsmouth 
UPDATE ADSMART_Metropolitan_raw
SET new_metro_area = 'Portsmouth/Southampton metropolitan area'
WHERE 
     postal_district like ('PO%')
  OR postal_district like ('SO%')
  OR postal_district in ('SP1','SP2','SP4','SP5','SP6','SP9','SP10','SP11');

--    Newcastle
UPDATE ADSMART_Metropolitan_raw
SET new_metro_area = 'Newcastle-Sunderland metropolitan area'
WHERE 
     postal_district like ('SR%')
  OR (postal_district like ('DH%') AND postal_district not like ('DH8'))
  OR (postal_district like ('NE%') AND postal_district not in ('NE18','NE19','NE20','NE43','NE44','NE45','NE46','NE47'));

--    Leeds
UPDATE ADSMART_Metropolitan_raw
SET new_metro_area = 'Leeds-Bradford metropolitan area'
WHERE 
     postal_district like ('LS%')
  OR postal_district like ('WF%')
  OR (postal_district like ('BD%') AND postal_district not in ('BD23','BD24'));

--    Edinburgh
UPDATE ADSMART_Metropolitan_raw
SET new_metro_area = 'Edinburgh metropolitan area'
WHERE 
     (postal_district like ('EH%') AND postal_district not in ('EH31','EH39','EH40','EH41''EH42','EH43','EH44','EH45','EH46'))
  OR postal_district in ('KY1','KY2','KY3','KY4','KY5','KY11','KY12');

--    Belfast
UPDATE ADSMART_Metropolitan_raw
SET new_metro_area = 'Belfast metropolitan area'
WHERE 
     postal_district in ('BT1','BT2','BT3','BT4','BT5','BT10','BT11','BT12','BT13','BT14','BT15','BT16','BT17','BT18','BT19','BT20','BT21','BT22','BT23','BT24')
  OR postal_district in ('BT26','BT27','BT28','BT29','BT30','BT32','BT35','BT36','BT37','BT38','BT39','BT40','BT41','BT42','BT45','BT57')
  OR postal_district in ('BT61','BT62','BT63','BT64','BT65','BT66','BT67','BT68','BT69','BT70','BT71','BT74','BT75','BT76','BT77')
  OR postal_district in ('BT80','BT92','BT93','BT94');

--    Nottingham
UPDATE ADSMART_Metropolitan_raw
SET new_metro_area = 'Nottingham-Derby metropolitan area'
WHERE     
     (postal_district like ('NG%') AND postal_district not in('NG22','NG23''NG24','NG31','NG32','NG33','NG34'))
  OR (postal_district like ('DE%') AND postal_district not in ('DE4','DE6','DE45'));

--    Sheffield
UPDATE ADSMART_Metropolitan_raw
SET new_metro_area = 'Sheffield metropolitan area'
WHERE     
     postal_district like ('S0%') OR postal_district like ('S1%')  OR postal_district like ('S2%') OR postal_district like ('S3%')
  OR postal_district like ('S4%') OR postal_district like ('S5%')  OR postal_district like ('S6%') OR postal_district like ('S7%')
  OR postal_district like ('S8%') OR postal_district like ('S9%')
  OR postal_district in ('DN1','DN2','DN3','DN4','DN5','DN11','DN12');

--    Glascow
UPDATE ADSMART_Metropolitan_raw
SET new_metro_area = 'Glasgow metropolitan area'
WHERE     
     postal_district in ('ML1','ML2','ML3','ML4','ML5','ML6','ML9','ML10')
  OR (postal_district like ('KA%') AND postal_district not in ('KA19','KA26'))    
  OR(postal_district like ('G0%') OR postal_district like ('G1%') OR postal_district like ('G2%') OR postal_district like ('G3%')
  OR postal_district like ('G4%') OR postal_district like ('G5%') OR postal_district like ('G7%')
  OR postal_district like ('G9%'))
  OR (postal_district like ('G6%') AND postal_district not like ('G63'))    
  OR (postal_district like ('G8%') AND postal_district not in ('G82','G83','G84'))    ;
  
 COMMIT;
------------------------------------------------------------------------------------------------------------
UPDATE ADSMART_Metropolitan_raw
SET 
         actual_metro_area = TRIM(REPLACE(  actual_metro_area, '  ', ' ' ))
        , new_metro_area = TRIM(REPLACE(  new_metro_area, '  ', ' ' ))
        , postal_area = TRIM(REPLACE(  postal_area, '  ', ' ' ))
        , postal_district = TRIM(REPLACE(  postal_district, '  ', ' ' ))
COMMIT
	
		