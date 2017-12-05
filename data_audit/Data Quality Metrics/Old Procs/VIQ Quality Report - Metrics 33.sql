/******************************************************************************
*Index vs. All Sky and Vespa Panel
*	Region
*	Package
*	Tenure
*	Box Type            --- missing
*	Lifestage
*	HH Box Composition  
*	Value Segment
*	Network Status Indicator
******************************************************************************/
-- DROP TABLE VIQ_ALL_Boxes
-- DROP TABLE VIQ_panel_account		
-- DROP TABLE VIQ_Indexes

CREATE TABLE VIQ_ALL_Boxes
	(account_status varchar(50)
  , account_number varchar (20)
	, region	varchar(70)
	, current_package	varchar(50)
	, tenure	varchar(20)
	, box_type	varchar(30)
	, h_lifestage	varchar(50)
	, HH_Box_Comp varchar(50)
	, value_seg varchar(15)
	, Net_Status int
	, in_Panel int not null DEFAULT 0
--	, cb_key_household	int
	)

INSERT INTO VIQ_ALL_Boxes
( 	account_status		, account_number
	, region			, current_package
	, tenure			, h_lifestage
	)
	
SELECT DISTINCT 
  	account_status
	, account_number
	, region
	, current_package
	, tenure
	, h_lifestage
FROM sk_prod.CUST_SINGLE_ACCOUNT_VIEW
WHERE account_status like '%Active%'
COMMIT;

CREATE HG INDEX idx1 ON VIQ_ALL_Boxes(account_number)
COMMIT;
------------------------------------------------------------------------------
---------------------Updating Value Segments------
------------------------------------------------------------------------------
UPDATE VIQ_ALL_Boxes
SET value_seg = COALESCE(vsd.value_seg, 'Bedding In') 
FROM VIQ_ALL_Boxes as AA
	JOIN sk_prod.VALUE_SEGMENTS_DATA as vsd ON AA.account_number = vsd.account_number;
------------------------------------------------------------------------------
---------------------Updating Panel Status------
------------------------------------------------------------------------------
SELECT account_number
		, MAX(Panel_ID_Vespa) Panel_ID_Vespa
INTO VIQ_panel_account		
FROM vespa_analysts.vespa_single_box_view as vsd
GROUP BY account_number;
COMMIT;

CREATE HG INDEX idx1 ON VIQ_panel_account(account_number)
COMMIT;

UPDATE VIQ_ALL_Boxes
SET in_Panel = Panel_ID_Vespa
FROM VIQ_ALL_Boxes as AA
	JOIN VIQ_panel_account AS vsd ON AA.account_number = vsd.account_number
 WHERE Panel_ID_Vespa is not null;
commit;	

DROP TABLE VIQ_panel_account
COMMIT;	
------------------------------------------------------------------------------
---------------------Defining Universe -- HH Box Composition------
------------------------------------------------------------------------------

SELECT    csh.service_instance_id
          ,csh.account_number
          ,subscription_sub_type
          ,rank() over (PARTITION BY csh.service_instance_id ORDER BY csh.account_number, csh.cb_row_id desc) AS rank
INTO accounts -- drop table accounts
FROM sk_prod.cust_subs_hist as csh
  INNER JOIN VIQ_ALL_Boxes AS ss
        ON csh.account_number = ss.account_number
WHERE  csh.subscription_sub_type IN ('DTV Primary Viewing','DTV Extra Subscription')     --the DTV sub Type
   AND csh.status_code IN ('AC','AB','PC')                  --Active Status Codes
   AND csh.effective_from_dt <= getdate ()
   AND csh.effective_to_dt > getdate ()
   AND csh.effective_from_dt <> effective_to_dt;
commit;

-- De-dupe active boxes
DELETE FROM accounts WHERE rank>1;
commit;

CREATE HG INDEX idx14 ON accounts(service_instance_id);
commit;

-- Identify HD boxes
SELECT  stb.service_instance_id
       ,SUM(CASE WHEN current_product_description LIKE '%HD%'     THEN 1  ELSE 0 END) AS HD
       ,SUM(CASE WHEN current_product_description LIKE '%HD%1TB%'
                   or current_product_description LIKE '%HD%2TB%' THEN 1  ELSE 0 END) AS HD1TB -- combine 1 and 2 TB
INTO hda -- drop table hda
FROM sk_prod.CUST_SET_TOP_BOX AS stb
        INNER JOIN accounts AS acc
        ON stb.service_instance_id = acc.service_instance_id
WHERE box_installed_dt <= getdate ()
        AND box_replaced_dt   > getdate ()
        AND current_product_description like '%HD%'
GROUP BY stb.service_instance_id;
commit;

CREATE HG INDEX idx14 ON hda(service_instance_id);
commit;

SELECT  
       acc.account_number
       ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Extra Subscription' THEN 1 ELSE 0  END) AS MR
       ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Sky+'               THEN 1 ELSE 0  END) AS SP
       ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV HD'                 THEN 1 ELSE 0  END) AS HD
       ,MAX(CASE  WHEN hda.HD = 1                                         THEN 1 ELSE 0  END) AS HDstb
       ,MAX(CASE  WHEN hda.HD1TB = 1                                      THEN 1 ELSE 0  END) AS HD1TBstb
INTO scaling_box_level_viewing
FROM sk_prod.cust_subs_hist AS csh
        INNER JOIN accounts AS acc
        ON csh.service_instance_id = acc.service_instance_id --< Limits to your universe
                LEFT OUTER JOIN sk_prod.cust_entitlement_lookup cel
                ON csh.current_short_description = cel.short_description
                        LEFT OUTER JOIN hda
                        ON csh.service_instance_id = hda.service_instance_id --< Links to the HD Set Top Boxes
 WHERE csh.effective_FROM_dt <= getdate ()
   AND csh.effective_to_dt    > getdate ()
   AND csh.status_code IN  ('AC','AB','PC')
   AND csh.SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing','DTV Sky+', 'DTV Extra Subscription','DTV HD' )
   AND csh.effective_FROM_dt <> csh.effective_to_dt
GROUP BY acc.service_instance_id ,acc.account_number;
commit;

drop table accounts; commit;
drop table hda; commit;


-- Identify boxtype of each box and whether it is a primary or a secondary box
SELECT  tgt.account_number
       ,SUM(CASE WHEN MR=1 THEN 1 ELSE 0 END) AS mr_boxes
       ,MAX(CASE WHEN MR=0 AND ((tgt.HD =1 AND HD1TBstb = 1) OR (tgt.HD =1 AND HDstb = 1))         THEN 4 -- HD ( inclusive of HD1TB)
                 WHEN MR=0 AND ((tgt.SP =1 AND tgt.HD1TBstb = 1) OR (tgt.SP =1 AND tgt.HDstb = 1)) THEN 3 -- HDx ( inclusive of HD1TB)
                 WHEN MR=0 AND tgt.SP =1                                                           THEN 2 -- Skyplus
                 ELSE                                                                              1 END) AS pb -- FDB
       ,MAX(CASE WHEN MR=1 AND ((tgt.HD =1 AND HD1TBstb = 1) OR (tgt.HD =1 AND HDstb = 1))         THEN 4 -- HD ( inclusive of HD1TB)
                 WHEN MR=1 AND ((tgt.SP =1 AND tgt.HD1TBstb = 1) OR (tgt.SP =1 AND tgt.HDstb = 1)) THEN 3 -- HDx ( inclusive of HD1TB)
                 WHEN MR=1 AND tgt.SP =1                                                           THEN 2 -- Skyplus
                 ELSE                                                                              1 END) AS sb -- FDB
        ,convert(varchar(20), null) as universe
        ,convert(varchar(30), null) as boxtype
  INTO boxtype_ac -- drop table boxtype_ac
  FROM scaling_box_level_viewing AS tgt
GROUP BY tgt.account_number;
commit;

-- Build the combined flags
update boxtype_ac
set universe = CASE WHEN mr_boxes = 0 THEN 'Single box HH'
                         WHEN mr_boxes = 1 THEN 'Dual box HH'
                         ELSE 'Multiple box HH' END
    ,boxtype  =
        CASE WHEN       mr_boxes = 0 AND  pb =  3 AND sb =  1   THEN  'HDx & No_secondary_box'
             WHEN       mr_boxes = 0 AND  pb =  4 AND sb =  1   THEN  'HD & No_secondary_box'
             WHEN       mr_boxes = 0 AND  pb =  2 AND sb =  1   THEN  'Skyplus & No_secondary_box'
             WHEN       mr_boxes = 0 AND  pb =  1 AND sb =  1   THEN  'FDB & No_secondary_box'
             WHEN       mr_boxes > 0 AND  pb =  4 AND sb =  4   THEN  'HD & HD' -- If a hh has HD  then all boxes have HD (therefore no HD and HDx)
             WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  3) OR (pb =  3 AND sb =  4)  THEN  'HD & HD'
             WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  2) OR (pb =  2 AND sb =  4)  THEN  'HD & Skyplus'
             WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  1) OR (pb =  1 AND sb =  4)  THEN  'HD & FDB'
             WHEN       mr_boxes > 0 AND  pb =  3 AND sb =  3                            THEN  'HDx & HDx'
             WHEN       mr_boxes > 0 AND (pb =  3 AND sb =  2) OR (pb =  2 AND sb =  3)  THEN  'HDx & Skyplus'
             WHEN       mr_boxes > 0 AND (pb =  3 AND sb =  1) OR (pb =  1 AND sb =  3)  THEN  'HDx & FDB'
             WHEN       mr_boxes > 0 AND  pb =  2 AND sb =  2                            THEN  'Skyplus & Skyplus'
             WHEN       mr_boxes > 0 AND (pb =  2 AND sb =  1) OR (pb =  1 AND sb =  2)  THEN  'Skyplus & FDB'
                        ELSE   'FDB & FDB' END
;
commit;

CREATE HG INDEX idx1 ON boxtype_ac(account_number);
commit;

UPDATE VIQ_ALL_Boxes
SET HH_Box_Comp = universe,
     AA.box_type = bx.boxtype
FROM VIQ_ALL_Boxes as AA
	JOIN boxtype_ac AS bx ON AA.account_number = bx.account_number
 WHERE universe is not null; 
 Commit;
 
 DROP TABLE boxtype_ac
 DROP TABLE scaling_box_level_viewing
 Commit;
------------------------------------------------------
----- CREATING THE INDEX TABLE
-------------------------------------------------------

IF object_id('pitteloudj.VIQ_Indexes') IS NULL
CREATE TABLE VIQ_Indexes
( Metric_ID int, 
  Metric_Desc varchar(20),
  Panel int,
  Metric_Label varchar(100),
  Metric_Value int, 
  Date_Created Datetime  
  )

INSERT INTO VIQ_Indexes ( Metric_ID ,   Metric_Desc,  Panel ,  Metric_Label ,  Metric_Value, Date_Created)
SELECT 
	331 , 'Region' , in_Panel	, region
	, COUNT (account_number) qty, getdate()
FROM VIQ_ALL_Boxes
GROUP BY 
  account_status	, in_Panel	, region			

INSERT INTO VIQ_Indexes ( Metric_ID ,   Metric_Desc,  Panel ,  Metric_Label ,  Metric_Value, Date_Created)
SELECT 
	332   , 'Package'   , in_Panel	, current_package
	, COUNT (account_number) qty, getdate()
FROM VIQ_ALL_Boxes
GROUP BY 
  account_status	, in_Panel	, current_package

INSERT INTO VIQ_Indexes ( Metric_ID ,   Metric_Desc,  Panel ,  Metric_Label ,  Metric_Value, Date_Created)
SELECT 
	333   , 'Tenure'   , in_Panel	, tenure
	, COUNT (account_number) qty, getdate()
FROM VIQ_ALL_Boxes
GROUP BY 
  account_status	, in_Panel	, tenure

INSERT INTO VIQ_Indexes ( Metric_ID ,   Metric_Desc,  Panel ,  Metric_Label ,  Metric_Value, Date_Created)
SELECT 
	334   , 'Lifestage'   , in_Panel	, h_lifestage
	, COUNT (account_number) qty, getdate()
FROM VIQ_ALL_Boxes
GROUP BY 
  account_status	, in_Panel	, h_lifestage
  
INSERT INTO VIQ_Indexes ( Metric_ID ,   Metric_Desc,  Panel ,  Metric_Label ,  Metric_Value, Date_Created)
SELECT 
	335   , 'Value Segment'   , in_Panel	, value_seg
	, COUNT (account_number) qty, getdate()
FROM VIQ_ALL_Boxes
GROUP BY 
  account_status	, in_Panel, value_seg

INSERT INTO VIQ_Indexes ( Metric_ID ,   Metric_Desc,  Panel ,  Metric_Label ,  Metric_Value, Date_Created)
SELECT 
	336   , 'HH Box Composition'   , in_Panel	, HH_Box_Comp
	, COUNT (account_number) qty, getdate()
FROM VIQ_ALL_Boxes
GROUP BY 
  account_status	, in_Panel, HH_Box_Comp;
COMMIT;

DROP TABLE VIQ_ALL_Boxes
COMMIT;

-- SELECT * FROM VIQ_Indexes
-- DROP TABLE VIQ_Indexes