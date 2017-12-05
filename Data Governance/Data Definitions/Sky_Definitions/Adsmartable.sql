/*###############################################################################
# Created on:   21/09/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for those customers who have AdSmartable
#		boxes and how many of those
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 21/09/2012  TKD   v01 - initial version
# 16/01/2013  TKD   V01.1 - Added what Adsmart does for documentation purposes
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Process									    #####
-- ##############################################################################################################

---------------------------------------------------------------------------------
--  Number of Adsmartable boxes  [NO_OF_ADSMART_STB]
---------------------------------------------------------------------------------
SELECT base.account_number
--      ,x_pvr_type
--      ,x_manufacturer
      ,CASE  WHEN x_pvr_type ='PVR6'                                THEN 1
             WHEN x_pvr_type ='PVR5'                                THEN 1
             WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Samsung' THEN 1
             WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Pace'    THEN 1
--             WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Thomson' THEN 1
                                                                    ELSE 0
       END AS Adsmartable
into #set_top_ads
FROM   sk_prod.CUST_SET_TOP_BOX  AS SetTop
        inner join VIQ_HH_ACCOUNT_TMP as Base
         on SetTop.account_number = Base.account_number
--where box_replaced_dt = '9999-09-09';
WHERE box_installed_dt <= today() AND box_replaced_dt > today(); --not replaced

commit;


select account_number,
      SUM(Adsmartable) AS T_AdSm_box,
      MAX(Adsmartable) AS HH_HAS_ADSMART_STB
INTO #SetTop2
FROM #set_top_ads
GROUP BY account_number;
commit;


--      create index on #SetTop2
CREATE   HG INDEX idx10 ON #SetTop2(account_number);

--      update VIQ_HH_ACCOUNT_TMP
UPDATE VIQ_HH_ACCOUNT_TMP
SET  NO_OF_ADSMART_STB = ST.T_AdSm_box
,base.HH_HAS_ADSMART_STB = st.HH_HAS_ADSMART_STB
FROM VIQ_HH_ACCOUNT_TMP  AS Base
  INNER JOIN #SetTop2 AS ST
        ON base.account_number = ST.account_number;
commit;


-- delete temp file
drop table #SetTop2;
drop table #set_top_ads;
commit;

------------------------------------ADSMART CODE--------------------------------

SELECT base.account_number
--      ,x_pvr_type
--      ,x_manufacturer
      ,CASE  WHEN x_pvr_type ='PVR6'                                THEN 1
             WHEN x_pvr_type ='PVR5'                                THEN 1
             WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Samsung' THEN 1
             WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Pace'    THEN 1
--             WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Thomson' THEN 1
                                                                    ELSE 0
       END AS Adsmartable
      ,SUM(Adsmartable) AS T_AdSm_box
INTO SetTop
FROM   sk_prod.CUST_SET_TOP_BOX  AS SetTop
        inner join AdSmart as Base
         on SetTop.account_number = Base.account_number
         where box_replaced_dt = '9999-09-09'
         GROUP BY base.account_number
                ,x_pvr_type
                ,x_manufacturer
                ,box_replaced_dt;
commit;

--DROP TABLE kjdl;
--commit;

select distinct(account_number), sum(T_AdSm_box) AS T_ADMS
into kjdl
from SetTop
GROUP BY account_number;
commit;

--      create index on SetTop
CREATE   HG INDEX idx10 ON kjdl(account_number);
commit;

--      update AdSmart file
UPDATE AdSmart
SET  T_AdSm_box = ST.T_ADMS
FROM AdSmart  AS Base
  INNER JOIN kjdl AS ST
        ON base.account_number = ST.account_number;
commit;




-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


