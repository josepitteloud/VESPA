
 ------------ 1.- CHECKING BARB REGION

  SELECT DISTINCT
    region,
    count(*) qty
  FROM sk_prod.ADSMART as a
  JOIN pitteloudj.Adsmart_region_metropolitan_1 as s on s.account_number = a.account_number AND s.adsmartable =1
  GROUP BY 
    region

  -- ISBA REGION VALUES
  SELECT DISTINCT
  isba_tv_region
  FROM sk_prod.ADSMART 

  -- ITV REGIONS in Master table
  SELECT top 1 *-- barb_desc_itv  
  from sk_prod.BARB_TV_regions
  -- BBC REGIONS in Master table
  SELECT DISTINCT barb_desc_bbc  from sk_prod.BARB_TV_regions;
SP_columns BARB_TV_regions


-- ---------- 2.- CHECKING UNKnown REGION

-- TOTAL ADSMART ACCOUNTS
SELECT 'TOTAL Adsmart rows' row
        , count(*) Total FROM sk_prod.ADSMART
UNION
SELECT 'TOTAL ADSMART null region' row
        ,count(*) Total FROM sk_prod.ADSMART
WHERE region is null
Union
SELECT 'TOTAL ADSMARTABLES null region' row
        ,count(*) Total 
FROM sk_prod.ADSMART  as a
JOIN pitteloudj.Adsmart_region_metropolitan_1 as s on s.account_number = a.account_number AND s.adsmartable =1
WHERE region is null
-- TOP Postal Areas with Null region
SELECT 
   a.cb_address_postcode_area
  , SUM(CASE WHEN b.region is null THEN 1 ELSE 0 END) Nullacct
  , SUM(CASE WHEN b.region is null THEN 0 ELSE 1 END) notNullcount
  , COUNT(*) qty
FROM sk_prod.ADSMART as b
JOIN sk_prod.CUST_SINGLE_ACCOUNT_VIEW as a ON  b.account_number = a.account_number
JOIN pitteloudj.Adsmart_region_metropolitan_1 as s on s.account_number = b.account_number AND s.adsmartable =1
GROUP BY cb_address_postcode_area
ORDER BY qty desc

-- SELECTING postal codes with null regions
SELECT DISTINCT 
     a.cb_address_postcode_area
  ,  a.cb_address_postcode
  , count(DISTINCT b.account_number) qty
INTO Adsmart_region_nulls_raw_2
FROM sk_prod.ADSMART as b
JOIN sk_prod.CUST_SINGLE_ACCOUNT_VIEW as a ON  b.account_number = a.account_number
WHERE b.region is null  
GROUP BY a.cb_address_postcode_area
  ,  a.cb_address_postcode

SELECT a.cb_address_postcode--count(DISTINCT a.cb_address_postcode)
FROM Adsmart_region_nulls_raw_2 as a
JOIN sk_prod.BARB_TV_regions AS c ON replace(c.cb_address_postcode,'  ', ' ') = replace(a.cb_address_postcode,'  ', ' ')
SELECT 
  count(DISTINCT cb_address_postcode_area) as Areas
, count(DISTINCT cb_address_postcode) as postcodes
FROM Adsmart_region_nulls_raw_2

SELECT 
  count(DISTINCT cb_address_postcode_area) as Areas
, count(DISTINCT cb_address_postcode) as postcodes
, count(*) acct
FROM sk_prod.ADSMART as b 
JOIN sk_prod.CUST_SINGLE_ACCOUNT_VIEW as a ON  b.account_number = a.account_number And a.cb_address_postcode_area='IM'
JOIN pitteloudj.Adsmart_region_metropolitan_1 as s on s.account_number = a.account_number AND s.adsmartable =1




-- Checking missing postcodes in the Region table

SELECT DISTINCT 
     a.cb_address_postcode_area
  ,  a.cb_address_postcode
INTO Adsmart_region_nulls_raw
FROM sk_prod.ADSMART as b
JOIN sk_prod.CUST_SINGLE_ACCOUNT_VIEW as a ON  b.account_number = a.account_number
LEFT JOIN sk_prod.BARB_TV_regions AS c ON replace(c.cb_address_postcode,'  ', ' ') = replace(a.cb_address_postcode,'  ', ' ')
WHERE b.region is null ;



SELECT cb_address_postcode_area area, count(*) FROM Adsmart_region_nulls_raw
GROUP BY area

-- Grouping areas by region by REgion table

SELECT DISTINCT 
  cb_address_postcode_outcode
  , count(DISTINCT barb_desc_itv) regions_not_null
  , count(DISTINCT isnull(barb_desc_itv,'null')) regions
  , 0 with_null_flag
  , 0 only_null_flag
INTO Adsmart_region_all_districts
FROM sk_prod.BARB_TV_regions  
GROUP BY cb_address_postcode_outcode


UPDATE Adsmart_region_all_districts
SET with_null_flag = 1
WHERE regions - regions_not_null > 0 AND regions_not_null > 0

UPDATE Adsmart_region_all_districts
SET only_null_flag = 1
WHERE regions_not_null = 0

SELECT count(*)
FROM Adsmart_region_all_districts
WHERE regions = regions_not_null
commit;

Select 
    count(*) TOTAL 
  , sum(with_null_flag)  W_null
  , sum(only_null_flag)  Only_null
FROM Adsmart_region_all_districts

SELECT DISTINCT a.* , b.barb_desc_itv
INTO Adsmart_region_districts_fixed_1
FROM Adsmart_region_all_districts AS a
INNER JOIN sk_prod.BARB_TV_regions  as b ON b.cb_address_postcode_outcode = a.cb_address_postcode_outcode and b.barb_desc_itv is not null
WHERE with_null_flag = 1 and regions_not_null = 1 

ALTER TABLE Adsmart_region_all_districts
ADD proc_flag bit default 0

UPDATE Adsmart_region_all_districts
SET proc_flag =1 
FROM Adsmart_region_all_districts as a 
WHERE regions = regions_not_null
commit


UPDATE Adsmart_region_all_districts
SET proc_flag =1 
FROM Adsmart_region_all_districts as a 
JOIN Adsmart_region_districts_fixed_1 as b ON a.cb_address_postcode_outcode=b.cb_address_postcode_outcode
commit

-----   Only Null Areas

SELECT DISTINCT a.* , '                               ' Region 
INTO Adsmart_region_districts_fixed_2
FROM Adsmart_region_all_districts AS a
WHERE only_null_flag = 1;

UPDATE Adsmart_region_districts_fixed_2
SET region = CASE WHEN cb_address_postcode_outcode in ('PA88','PA87','PA86','PA85','PA84','PA83','PA82','PA81','PA40','PA39','G9','G80','G59') THEN 'Central Scotland'
                  WHEN cb_address_postcode_outcode in ('PE18', 'PE17', 'NN99', 'MK98')      THEN 'East-of-England'
                  WHEN cb_address_postcode_outcode in ('NP9','NP6','NP5','NP3','NP2','NP1','CF8','CF7','CF6','CF4','CF30','CF2','CF1')         THEN 'HTV Wales'
                  WHEN cb_address_postcode_outcode in ('SN42','SN17','BS77','BS19','BS18','BS17','BS12','BS0' )                       THEN 'HTV West'
                  WHEN cb_address_postcode_outcode in ('WD2','WD1','WC99','W1Y','W1X','W1V','W1R','W1P','W1N','W1M','W1E','SL60','SE99'
                                             ,'SE1P','S69','S31','S30','S19','RM50','RG3','NW26','NW1W'
                                             ,'NE89','N1P','LU95','GU13','EC88','EC50','EC4P','EC3P','EC2P','EC1P','CR90','BR98')   THEN 'London'
                  WHEN cb_address_postcode_outcode in ('SO9','SO5','SO4','SO3','SO2','SO13','SO1','RG15','RG13','RG11','BN52','BN51','BN50','BN4') THEN 'Meridian'
                  WHEN cb_address_postcode_outcode in ('NG70','ST55','RG16','OX8','OX6','LE99','LE94','LE41','LE21','DE2''B22')            THEN 'Midlands'  
                  WHEN cb_address_postcode_outcode in ('TS90','SR88','SR43')      THEN 'North-East'
                  WHEN cb_address_postcode_outcode in ('IV35','IV34','IV33','AB9','AB5','AB4','AB3','AB2','AB1')    THEN 'Northern Scotland'
                  WHEN cb_address_postcode_outcode in ('M61','M52','M10','L73','L66','L65','L64','L63','L62','L61','L60','L49','L48','L47','L46','L45','L44'
                                                ,'L43','L42','L41','CH34','CH33','CH32','CH31','CH30','CH29','CH28','CH27','CH26','CH25','BB0')    THEN 'North-West'
                  WHEN cb_address_postcode_outcode in ('BT99')    THEN 'Ulster'
                  WHEN cb_address_postcode_outcode in ('YO95','YO6','YO5','YO40','YO4','YO3','YO2','HU55')    THEN 'Yorkshire'
                  ELSE 'XXXXXXX'
                  END

commit;

UPDATE Adsmart_region_all_districts
SET proc_flag =1 
FROM Adsmart_region_all_districts as a 
JOIN Adsmart_region_districts_fixed_2 as b ON a.cb_address_postcode_outcode=b.cb_address_postcode_outcode
commit

---------     Areas with more than 1 unique region
SELECT DISTINCT a.* , '                               ' Region 
INTO Adsmart_region_districts_fixed_3
FROM Adsmart_region_all_districts AS a
WHERE proc_flag = 0;

UPDATE Adsmart_region_districts_fixed_3
SET region = CASE WHEN cb_address_postcode_outcode in ('CM1','CM3','CM6','NN11','NN17','PE6')      THEN 'East-of-England'
                  WHEN cb_address_postcode_outcode in ('GL16','NP16','SY10','SY13')         THEN 'HTV Wales'
                  WHEN cb_address_postcode_outcode in ('GL11','GL7' )                       THEN 'HTV West'
                  WHEN cb_address_postcode_outcode in ('HP23','LU1','LU6','RG12','RG42','RH12','SG1','SS4','SS5')   THEN 'London'
                  WHEN cb_address_postcode_outcode in ('GU27','SN4','SN8','SP4','TN6') THEN 'Meridian'
                  WHEN cb_address_postcode_outcode in ('RG8','SK17')            THEN 'Midlands'  
                  WHEN cb_address_postcode_outcode in ('ST7','ST8')    THEN 'North-West'
                  WHEN cb_address_postcode_outcode in ('BT99')    THEN 'Ulster'
                  WHEN cb_address_postcode_outcode in ('BD23','NG17')    THEN 'Yorkshire'
                  ELSE 'XXXXXXX'
                  END
Commit

-- UPDATE statement for ADSMART statement
/*
  UPDATE sk_prod.ADSMART
  SET region = a.barb_desc_itv
  FROM sk_prod.ADSMART as a
  JOIN sk_prod.CUST_SINGLE_ACCOUNT_VIEW as sav ON  a.account_number = sav.account_number
  JOIN Adsmart_region_districts_fixed_1 as b on sav.cb_address_postcode_outcode = b.cb_address_postcode_outcode
  JOIN Adsmart_region_districts_fixed_2 as c on sav.cb_address_postcode_outcode = c.cb_address_postcode_outcode
  JOIN Adsmart_region_districts_fixed_3 as d on sav.cb_address_postcode_outcode = d.cb_address_postcode_outcode
  
*/


/*--------------------- 4.- Metropolitan areas assignment ------------- */

SELECT  
    a.account_number
  , a.region as actual_region
  , sav.region as new_region
  , sav.cb_address_postcode_outcode as postal_area
  , sav.cb_address_postcode as postcode
  , 0 Change_flag
INTO Adsmart_region_metropolitan_1
FROM sk_prod.ADSMART as a
JOIN sk_prod.CUST_SINGLE_ACCOUNT_VIEW as sav ON  a.account_number = sav.account_number;

-- Re-setting region to Master region definition
UPDATE Adsmart_region_metropolitan_1
SET a.new_region = b.barb_desc_itv
FROM Adsmart_region_metropolitan_1 as a
INNER JOIN sk_prod.BARB_TV_regions  as b ON b.cb_address_postcode = a.postcode and b.barb_desc_itv is not null;

-- Fixing old region names
UPDATE Adsmart_region_metropolitan_1
SET new_region = CASE WHEN new_region = 'East Of England'                       THEN 'East-of-England'
                      WHEN new_region = 'Meridian (exc. Channel Islands)'       THEN 'Meridian' 
                      WHEN new_region = 'South West'                            THEN 'South-West'
                      WHEN new_region = 'North West'                            THEN 'North-West'
                      WHEN new_region = 'North East'                            THEN 'North-East'
                      WHEN new_region = 'North Scotland'                        THEN 'Northern Scotland'
                ELSE new_region 
              END
WHERE actual_region in ('East-of-England', 'Meridian','South-West', 'North-West', 'Northern Scotland', 'North-East') 
OR

UPDATE Adsmart_region_metropolitan_1
SET new_region = 'Border-Scotland'
WHERE new_region = 'Border' 
    AND actual_region = 'Border-Scotland'
UPDATE Adsmart_region_metropolitan_1
SET new_region = 'Border-England'
WHERE new_region = 'Border' 
    AND actual_region = 'Border-England'

commit;


-- Metropolitan update by postcode
UPDATE Adsmart_region_metropolitan_1
SET new_region = 'Birmingham metropolitan area'
WHERE 
  postal_area like ('B0%') OR postal_area like ('B1%')
  OR postal_area like ('B2%') OR postal_area like ('B3%')
  OR postal_area like ('B4%') OR postal_area like ('B5%')
  OR postal_area like ('B6%') OR postal_area like ('B7%')
  OR postal_area like ('B8%') OR postal_area like ('B9%')
  OR postal_area in ('DY1', 'DY2','DY3', 'DY4','DY5', 'DY6','DY7','DY8','DY9', 'DY10','DY11', 'DY12','DY13')
  OR postal_area in ('WV1', 'WV2','WV3', 'WV4','WV5', 'WV6','WV7','WV8','WV9', 'WV10','WV11', 'WV12','WV13','WV14')
  OR postal_area in ('WS1', 'WS2','WS3', 'WS4','WS5', 'WS6','WS7','WS8','WS9', 'WS10','WS11', 'WS12','WS13','WS14')
  OR (postal_area like ('CV%') AND postal_area not like ('CV36'));

--    Liverpool
UPDATE Adsmart_region_metropolitan_1
SET new_region = 'Liverpool/Birkenhead metropolitan area'
WHERE 
  postal_area like ('L0%') OR postal_area like ('L1%')
  OR postal_area like ('L2%') OR postal_area like ('L3%')
  OR postal_area like ('L4%') OR postal_area like ('L5%')
  OR postal_area like ('L6%') OR postal_area like ('L7%')
  OR postal_area like ('L8%') OR postal_area like ('L9%')
  OR postal_area like ('CH%')
  OR (postal_area like ('WN%') AND postal_area not like ('WN7'))
  OR postal_area in ('PR7','PR8','PR9','WA1','WA2','WA4','WA5','WA6','WA7','WA8','WA9','WA10','WA11','WA12')
  OR postal_area in ('CW8','CW9');

--    Manchester 
UPDATE Adsmart_region_metropolitan_1
SET new_region = 'Manchester metropolitan area'
WHERE 
     postal_area like ('M0%') OR postal_area like ('M1%')  OR postal_area like ('M2%') OR postal_area like ('M3%')
  OR postal_area like ('M4%') OR postal_area like ('M5%')  OR postal_area like ('M6%') OR postal_area like ('M7%')
  OR postal_area like ('M8%') OR postal_area like ('M9%')
  OR postal_area like ('OL%')
  OR postal_area like ('BL%')
  OR (postal_area like ('SK%') AND postal_area not like ('SK17'))
  OR postal_area in ('WN7','WA3','WA13','WA14','WA15','WA16');

--    Brighton 
UPDATE Adsmart_region_metropolitan_1
SET new_region = 'Brighton/Worthing/Littlehampton metropolitan area'
WHERE 
     postal_area like ('BN%')
  OR postal_area in ('RH10','RH11', 'RH12','RH13','RH14','RH15','RH16','RH17','RH18','RH19','RH20')
  OR postal_area in ('TN6','TN7','TN20','TN21','TN22');

--    Cardiff
UPDATE Adsmart_region_metropolitan_1
SET new_region = 'Cardiff and South Wales valleys metropolitan area'
WHERE 
      postal_area like ('CF%')
  OR postal_area in ('NP1','NP2','NP3','NP4','NP5','NP6','NP9','NP10','NP11','NP12','NP13','NP14')
  OR postal_area in ('NP17','NP18','NP19','NP20','NP21','NP22','NP23','NP24','NP26');

--    Leicester
UPDATE Adsmart_region_metropolitan_1
SET new_region = 'Leicester metropolitan area'
WHERE 
     postal_area like ('LE%');

--    Bristol 
UPDATE Adsmart_region_metropolitan_1
SET new_region = 'Bristol metropolitan area'
WHERE 
     postal_area like ('BS%')
  OR postal_area in ('BA1','BA2','BA3','BA5','BA15');

--    Portsmouth 
UPDATE Adsmart_region_metropolitan_1
SET new_region = 'Portsmouth/Southampton metropolitan area'
WHERE 
     postal_area like ('PO%')
  OR postal_area like ('SO%')
  OR postal_area in ('SP1','SP2','SP4','SP5','SP6','SP9','SP10','SP11');

--    Newcastle
UPDATE Adsmart_region_metropolitan_1
SET new_region = 'Newcastle-Sunderland metropolitan area'
WHERE 
     postal_area like ('SR%')
  OR (postal_area like ('DH%') AND postal_area not like ('DH8'))
  OR (postal_area like ('NE%') AND postal_area not in ('NE18','NE19','NE20','NE43','NE44','NE45','NE46','NE47'));

--    Leeds
UPDATE Adsmart_region_metropolitan_1
SET new_region = 'Leeds-Bradford metropolitan area'
WHERE 
     postal_area like ('LS%')
  OR postal_area like ('WF%')
  OR (postal_area like ('BD%') AND postal_area not in ('BD23','BD24'));

--    Edinburgh
UPDATE Adsmart_region_metropolitan_1
SET new_region = 'Edinburgh metropolitan area'
WHERE 
     (postal_area like ('EH%') AND postal_area not in ('EH31','EH39','EH40','EH41''EH42','EH43','EH44','EH45','EH46'))
  OR postal_area in ('KY1','KY2','KY3','KY4','KY5','KY11','KY12');

--    Belfast
UPDATE Adsmart_region_metropolitan_1
SET new_region = 'Belfast metropolitan area'
WHERE 
     postal_area in ('BT1','BT2','BT3','BT4','BT5','BT10','BT11','BT12','BT13','BT14','BT15','BT16','BT17','BT18','BT19','BT20','BT21','BT22','BT23','BT24')
  OR postal_area in ('BT26','BT27','BT28','BT29','BT30','BT32','BT35','BT36','BT37','BT38','BT39','BT40','BT41','BT42','BT45','BT57')
  OR postal_area in ('BT61','BT62','BT63','BT64','BT65','BT66','BT67','BT68','BT69','BT70','BT71','BT74','BT75','BT76','BT77')
  OR postal_area in ('BT80','BT92','BT93','BT94');

--    Nottingham
UPDATE Adsmart_region_metropolitan_1
SET new_region = 'Nottingham-Derby metropolitan area'
WHERE     
     (postal_area like ('NG%') AND postal_area not in('NG22','NG23''NG24','NG31','NG32','NG33','NG34'))
  OR (postal_area like ('DE%') AND postal_area not in ('DE4','DE6','DE45'));

--    Sheffield
UPDATE Adsmart_region_metropolitan_1
SET new_region = 'Sheffield metropolitan area'
WHERE     
     postal_area like ('S0%') OR postal_area like ('S1%')  OR postal_area like ('S2%') OR postal_area like ('S3%')
  OR postal_area like ('S4%') OR postal_area like ('S5%')  OR postal_area like ('S6%') OR postal_area like ('S7%')
  OR postal_area like ('S8%') OR postal_area like ('S9%')
  OR postal_area in ('DN1','DN2','DN3','DN4','DN5','DN11','DN12');

--    Glascow
UPDATE Adsmart_region_metropolitan_1
SET new_region = 'Glasgow metropolitan area'
WHERE     
     postal_area in ('ML1','ML2','ML3','ML4','ML5','ML6','ML9','ML10')
  OR (postal_area like ('KA%') AND postal_area not in ('KA19','KA26'))    
  OR(postal_area like ('G0%') OR postal_area like ('G1%') OR postal_area like ('G2%') OR postal_area like ('G3%')
  OR postal_area like ('G4%') OR postal_area like ('G5%') OR postal_area like ('G7%')
  OR postal_area like ('G9%'))
  OR (postal_area like ('G6%') AND postal_area not like ('G63'))    
  OR (postal_area like ('G8%') AND postal_area not in ('G82','G83','G84'))    ;


-- Flaging changes
UPDATE Adsmart_region_metropolitan_1
SET Change_flag = 1
WHERE actual_region <> new_region

-- FLAGGING null to not null change 
UPDATE Adsmart_region_metropolitan_1
SET Change_flag = 1
WHERE  actual_region IS NULL 
  AND new_region IS NOT NULL 

commit;

SELECT 
    count(*) TOTAL_adsmart_account
  , sum (Change_flag)   Total_changes     
FROM Adsmart_region_metropolitan_1


SELECT new_region
  , count(*) qty
  FROM Adsmart_region_metropolitan_1
  GROUP BY new_region


-------------       QA
SELECT top 50 *
FROM Adsmart_region_metropolitan_1
WHERE Change_flag = 1
AND actual_region = :par1 AND new_region = :par2

SELECT DISTINCT top 10 
 --   adsmart_isba_tv_region	
 -- , cb_address_postcode
 -- , government_region	
 -- , cb_address_postcode_area
 -- , isba_tv_region	
   cb_address_postcode_district
  , region	
FROM sk_prod.CUST_SINGLE_ACCOUNT_VIEW
WHERE cb_address_postcode_district ='ST5'
account_number in ('210174733746')

SELECT top 10 * FROM Adsmart_region_metropolitan_1
Where actual_region is null and new_region is not null

SELECT DISTINCT top 100 postal_area
FROM Adsmart_region_metropolitan_1
WHERE Change_flag = 1
AND actual_region = :par1 AND new_region = :par2

-----------------------------------------------
SELECT  
  actual_region
  , new_region      
  , count(*)
FROM Adsmart_region_metropolitan_1
WHERe Adsmartable = 1
GROUP BY 
  actual_region
  , new_region      


/* ---------- Channel Island review ----------*/

SELECT top 10 * from sk_prod.ADSMART

 SELECT 
    region,
    count(1) qty
  FROM sk_prod.ADSMART as a
  JOIN pitteloudj.Adsmart_region_metropolitan_1 as s on s.account_number = a.account_number AND s.adsmartable =1
  WHERE region = 'Channel Islands'
  GROUP BY 
    region

SELECT top 10 
   a.account_number
  ,b.weighting as overall_project_weighting
--INTO #temp1 
FROM vespa_analysts.SC2_intervals   as a
JOIN vespa_analysts.SC2_weightings  as b ON a.scaling_segment_ID = b.scaling_segment_ID AND cast('2013-10-15' as date) = b.scaling_day
JOIN sk_prod.Adsmart                as c ON c.account_number = a.account_number --AND c.region ='Channel Islands'
WHERE cast('2013-10-15' as date) between a.reporting_starts and a.reporting_ends

SELECT
TOP 10 
a.account_number
,b.weighting as overall_project_weighting
from  vespa_analysts.SC2_intervals as a
inner join vespa_analysts.SC2_weightings as b
on  cast('2013-10-05' as date) = b.scaling_day
and a.scaling_segment_ID = b.scaling_segment_ID
and cast('2013-10-05' as date) between a.reporting_starts and a.reporting_ends
JOIN sk_prod.Adsmart                as c ON c.account_number = a.account_number AND c.region ='Channel Islands'

COMMIT



---------------   Accounts review

SELECT 
    account_status
  , count(account_number )
FROM sk_prod.Cust_SINGLE_ACCOUNT_VIEW
WHERE  
 cb_address_postcode_outcode like ('B0%') OR cb_address_postcode_outcode like ('B1%')
  OR cb_address_postcode_outcode like ('B2%') OR cb_address_postcode_outcode like ('B3%')
  OR cb_address_postcode_outcode like ('B4%') OR cb_address_postcode_outcode like ('B5%')
  OR cb_address_postcode_outcode like ('B6%') OR cb_address_postcode_outcode like ('B7%')
  OR cb_address_postcode_outcode like ('B8%') OR cb_address_postcode_outcode like ('B9%')
GROUP BY 
  account_status

SELECT qty, count(cb_key_household)
FROM (SELECT 
        cb_key_household
        , count(*) qty
      FROM sk_prod.Cust_SINGLE_ACCOUNT_VIEW
      WHERE  
        cb_address_postcode_outcode like ('B0%') OR cb_address_postcode_outcode like ('B1%')
        OR cb_address_postcode_outcode like ('B2%') OR cb_address_postcode_outcode like ('B3%')
        OR cb_address_postcode_outcode like ('B4%') OR cb_address_postcode_outcode like ('B5%')
        OR cb_address_postcode_outcode like ('B6%') OR cb_address_postcode_outcode like ('B7%')
        OR cb_address_postcode_outcode like ('B8%') OR cb_address_postcode_outcode like ('B9%')
      GROUP BY cb_key_household)as v
GROUP BY qty

SELECT 
  count(DISTINCT account_number) uniq_account 
  , count(account_number) Total_account 
FROM sk_prod.Cust_SINGLE_ACCOUNT_VIEW
WHERE account_number is not null 
  AND (
        cb_address_postcode_outcode like ('B0%') OR cb_address_postcode_outcode like ('B1%')
        OR cb_address_postcode_outcode like ('B2%') OR cb_address_postcode_outcode like ('B3%')
        OR cb_address_postcode_outcode like ('B4%') OR cb_address_postcode_outcode like ('B5%')
        OR cb_address_postcode_outcode like ('B6%') OR cb_address_postcode_outcode like ('B7%')
        OR cb_address_postcode_outcode like ('B8%') OR cb_address_postcode_outcode like ('B9%'))

SELECT count(cb_key_household) FROM sk_prod.Cust_SINGLE_ACCOUNT_VIEW
 WHERE  
        cb_address_postcode_outcode like ('B0%') OR cb_address_postcode_outcode like ('B1%')
        OR cb_address_postcode_outcode like ('B2%') OR cb_address_postcode_outcode like ('B3%')
        OR cb_address_postcode_outcode like ('B4%') OR cb_address_postcode_outcode like ('B5%')
        OR cb_address_postcode_outcode like ('B6%') OR cb_address_postcode_outcode like ('B7%')
        OR cb_address_postcode_outcode like ('B8%') OR cb_address_postcode_outcode like ('B9%')

SELECT  top 10 * from Adsmart_region_metropolitan_1

SELECT count(*)
fROM Adsmart_region_metropolitan_1
WHERE actual_region like 'Birmingham metropolitan%'


SELECT count(*)
fROM Adsmart_region_metropolitan_1
WHERE new_region like 'Birmingham metropolitan%'



SELECT count(*)
fROM Adsmart_region_metropolitan_1
WHERE  postal_area like ('B0%') OR postal_area like ('B1%')
        OR postal_area like ('B2%') OR postal_area like ('B3%')
        OR postal_area like ('B4%') OR postal_area like ('B5%')
        OR postal_area like ('B6%') OR postal_area like ('B7%')
        OR postal_area like ('B8%') OR postal_area like ('B9%')




SELECT 'actual region HH' type 
      , COUNT(a.Account_number) act
      , count(DISTINCT sav.cb_key_household) HH
fROM Adsmart_region_metropolitan_1 as a
JOIN sk_prod.Cust_SINGLE_ACCOUNT_VIEW as sav ON sav.account_number = a.account_number
WHERE actual_region like 'Birmingham metropolitan%'
      AND sav.account_status like '%Active'
UNION
SELECT 'New region HH' type 
      , COUNT(a.Account_number) act
      , count(DISTINCT sav.cb_key_household) hh
fROM Adsmart_region_metropolitan_1 as a
JOIN sk_prod.Cust_SINGLE_ACCOUNT_VIEW as sav ON sav.account_number = a.account_number
WHERE new_region like 'Birmingham metropolitan%'
      AND sav.account_status like '%Active'

UNION
SELECT 'Postal areas B HH' type 
      , COUNT(a.Account_number) act
      , count(DISTINCT sav.cb_key_household) hh
fROM Adsmart_region_metropolitan_1 as a
JOIN sk_prod.Cust_SINGLE_ACCOUNT_VIEW as sav ON sav.account_number = a.account_number
WHERE  (postal_area like ('B0%') OR postal_area like ('B1%')
        OR postal_area like ('B2%') OR postal_area like ('B3%')
        OR postal_area like ('B4%') OR postal_area like ('B5%')
        OR postal_area like ('B6%') OR postal_area like ('B7%')
        OR postal_area like ('B8%') OR postal_area like ('B9%'))
        AND sav.account_status like '%Active'




SELECT 'SAV actual region HH' type 
      , COUNT(sav.Account_number) act
      , count(DISTINCT sav.cb_key_household) HH
FROM sk_prod.Cust_SINGLE_ACCOUNT_VIEW as sav 
WHERE region like 'Birmingham metropolitan%'
      AND sav.account_status like '%Active'
UNION
SELECT 'SAV New region HH' type
      , COUNT(sav.Account_number) act
      , count(DISTINCT sav.cb_key_household) HH
FROM sk_prod.Cust_SINGLE_ACCOUNT_VIEW as sav 
WHERE  (cb_address_postcode_outcode like ('B0%') OR cb_address_postcode_outcode like ('B1%')
  OR cb_address_postcode_outcode like ('B2%') OR cb_address_postcode_outcode like ('B3%')
  OR cb_address_postcode_outcode like ('B4%') OR cb_address_postcode_outcode like ('B5%')
  OR cb_address_postcode_outcode like ('B6%') OR cb_address_postcode_outcode like ('B7%')
  OR cb_address_postcode_outcode like ('B8%') OR cb_address_postcode_outcode like ('B9%')
  OR cb_address_postcode_outcode in ('DY1', 'DY2','DY3', 'DY4','DY5', 'DY6','DY7','DY8','DY9', 'DY10','DY11', 'DY12','DY13')
  OR cb_address_postcode_outcode in ('WV1', 'WV2','WV3', 'WV4','WV5', 'WV6','WV7','WV8','WV9', 'WV10','WV11', 'WV12','WV13','WV14')
  OR cb_address_postcode_outcode in ('WS1', 'WS2','WS3', 'WS4','WS5', 'WS6','WS7','WS8','WS9', 'WS10','WS11', 'WS12','WS13','WS14')
  OR (cb_address_postcode_outcode like ('CV%') AND cb_address_postcode_outcode not like ('CV36')))
  AND sav.account_status like '%Active'
UNION
SELECT 'SAV Postal areas B HH' type 
      , COUNT(sav.Account_number) act
      , count(DISTINCT sav.cb_key_household) HH
FROM sk_prod.Cust_SINGLE_ACCOUNT_VIEW as sav
WHERE  (cb_address_postcode_outcode like ('B0%') OR cb_address_postcode_outcode like ('B1%')
        OR cb_address_postcode_outcode like ('B2%') OR cb_address_postcode_outcode like ('B3%')
        OR cb_address_postcode_outcode like ('B4%') OR cb_address_postcode_outcode like ('B5%')
        OR cb_address_postcode_outcode like ('B6%') OR cb_address_postcode_outcode like ('B7%')
        OR cb_address_postcode_outcode like ('B8%') OR cb_address_postcode_outcode like ('B9%'))
        AND sav.account_status like '%Active'

SELECT region , count(*) FROM sk_prod.ADSMART

GROUP BY region 
SELECT top 1 * from sk_prod.ADSMART 
SELECT 
  postcode
  , count(DISTINCT actual_region) Hits
FROM Adsmart_region_metropolitan_1
WHERE actual_region is not null
GROUP  BY 
  postcode
  HAVING Hits>1

SELECT DISTINCT actual_region 
from Adsmart_region_metropolitan_1

SELECT top 10 a.account_number, cb_address_town, a.region, b.cb_address_postcode
FROM sk_prod.adsmart as a
JOIN sk_prod.CUST_SINGLE_ACCOUNT_VIEW as b ON a.account_number = b.account_number
WHERE a.region = 'Midlands' 
and b.cb_address_postcode = 'B1   1BT';

SELECT top 10 a.account_number, cb_address_town, a.region, b.cb_address_postcode
FROM sk_prod.adsmart as a
JOIN sk_prod.CUST_SINGLE_ACCOUNT_VIEW as b ON a.account_number = b.account_number
WHERE a.region like 'Bir%' 
and b.cb_address_postcode = 'B1   1BT'

SELECT DISTINCT cb_address_town
FROM sk_prod.CUST_SINGLE_ACCOUNT_VIEW
WHERE cb_address_postcode = 'B1   1BT'
/*
SELECT * from Adsmart_region_all_districts WHERE proc_flag =0 AND only_null_flag = 1
 
SELECT  top 10 * from Adsmart_region_metropolitan_1

SELECT top 10 * from Adsmart_region_all_districts

SELECT DISTINCT a.region 
FROM sk_prod.adsmart as a
JOIN sk_prod.CUST_SINGLE_ACCOUNT_VIEW as sav ON  a.account_number = sav.account_number
WHERE sav.cb_address_postcode_outcode like 'NG17%'


SELECT DISTINCT cb_address_postcode_outcode FROM sk_prod.BARB_TV_regions
WHERE 
cb_address_postcode_outcode like ('WN%') AND cb_address_postcode_outcode not like ('WN7')


SELECT TOP 10 * FROM sk_prod.BARB_TV_regions
SELECT count(*)
FROM sk_prod.BARB_TV_regions
WHERE postcode like '%  %';

SELECT DISTINCT
barb_desc_itv,
from sk_prod.BARB_TV_regions

SP_columns ADSMART_HISTORY
FROM sk_prod.CUST_SINGLE_ACCOUNT_VIEW 
JOIN sk_prod.BARB_TV_regions as c ON c.cb_address_postcode = replace(a.cb_address_postcode,'  ', ' ')

SELECT top 100 * from sk_prod.BARB_TV_regions
WHERE cb_address_postcode like 'BT5 9%'

SELECT 
  viewing_panel_id a
  , count(*) qty
FROM sk_prod.ADSMART
GROUP BY a

SELECT 
  viewing_panel_id a
  , count(*) qty
FROM sk_prod.ADSMART
GROUP BY a

sp_columns CUST_SINGLE_ACCOUNT_VIEW

select  top 10
        account_number	
        ,tax_date	
        ,balance_zero_dt	
        ,payment_due_dt	
, *
FROM    sk_prod.cust_bills 
	where balance_zero_dt > payment_due_dt
ORDER by tax_date desc

*/




SELECT top 10 * from SetTop

SELECT base.account_number
      ,CASE  WHEN x_pvr_type ='PVR6'                                THEN 1
             WHEN x_pvr_type ='PVR5'                                THEN 1
             WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Samsung' THEN 1
             WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Pace'    THEN 1
                                                                    ELSE 0
       END AS Adsmartable
      ,SUM(Adsmartable) AS T_AdSm_box
INTO SetTop
FROM
(
           --------------------------------------------------------------------------
           -- B02: Extracting Active Boxes per account (one line per box per account)
           --------------------------------------------------------------------------
           select  *
           from    (
                        --------------------------------------------------------------------
                        -- B01: Ranking STB based on service instance id to dedupe the table
                        --------------------------------------------------------------------
                        Select  account_number
                        ,x_pvr_type
                        ,x_personal_storage_capacity
                        ,currency_code
                        ,x_manufacturer
                        ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) active_flag
                        from    sk_prod.CUST_SET_TOP_BOX
                        where box_installed_dt < getdate()

           )       as box
           where   active_flag = 1

)       as active_boxes

inner join sk_prod.ADSMART as Base
on active_boxes.account_number = Base.account_number

GROUP BY base.account_number
,x_pvr_type
,x_manufacturer


select account_number, sum(T_AdSm_box) AS T_ADMS
into kjdl
from SetTop
GROUP BY account_number;
commit;


CREATE   unique clustered INDEX idx10 ON kjdl(account_number);
commit;









SELECT 
  base.region
  , CASE WHEN sav.CUST_VIEWING_DATA_CAPTURE_ALLOWED = 'Y' then 'Y' else 'N' end as Adsmartable_Consent
  , COUNT (DISTINCT base.cb_key_household) HH 
FROM sk_prod.ADSMART base
JOIN -- adding in the adsmart consent flag
    (Select
		distinct account_number,
		CUST_VIEWING_DATA_CAPTURE_ALLOWED
    from sk_prod.cust_single_account_view
    where CUST_VIEWING_DATA_CAPTURE_ALLOWED = 'Y'
		and cust_viewing_capture_allwd_start_dt < '2013-11-07'
    group by account_number,CUST_VIEWING_DATA_CAPTURE_ALLOWED) Sav  	ON Base.account_number = Sav.account_number
JOIN kjdl AS ST        			ON base.account_number = ST.account_number AND ST.T_ADMS > 0 
GROUP BY base.region, Adsmartable_Consent

ALTER TABLE Adsmart_region_metropolitan_1
add adsmartable bit default 0;
commit

UPDATE Adsmart_region_metropolitan_1 
SET base.adsmartable = 1 
FROM Adsmart_region_metropolitan_1 base
JOIN -- adding in the adsmart consent flag
    (Select
		distinct account_number,
		CUST_VIEWING_DATA_CAPTURE_ALLOWED
    from sk_prod.cust_single_account_view
    where CUST_VIEWING_DATA_CAPTURE_ALLOWED = 'Y'
		and cust_viewing_capture_allwd_start_dt < '2013-11-07'
    group by account_number,CUST_VIEWING_DATA_CAPTURE_ALLOWED) Sav  	ON base.account_number = Sav.account_number
JOIN kjdl AS ST        			ON base.account_number = ST.account_number AND ST.T_ADMS > 0  

CREATE   unique clustered INDEX idx1 ON Adsmart_region_metropolitan_1(account_number);

SELECT new_region, count(DISTINCT sav.cb_key_household) hh
FROM Adsmart_region_metropolitan_1 base
JOIN sk_prod.cust_single_account_view  AS SAV ON base.account_number = sav.account_number
WHERE Adsmartable=1
GROUP BY new_region


SELECT top 1 * FROM Adsmart_region_metropolitan_1



------------------------ ULSTER County CHECKs

SELECT DISTINCT TOP 10
          a.region
        , b.region
        , a.cb_address_county
        , a.cb_address_locality
        , a.cb_address_postcode_area
        , a.cb_address_town
FROM sk_prod.CUST_SINGLE_ACCOUNT_VIEW as a
JOIN sk_prod.ADSMART as b       ON a.account_number = b.account_number
WHERE lower(a.region) like 'uls%'
        OR (lower(cb_address_county) like 'donegal'
        OR lower(cb_address_county) like 'monaghan'
        OR lower(cb_address_county) like 'cavan')