/*###############################################################################
# Created on:   19/09/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for defining the availability of cable in the
#		area of their household
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 19/09/2012  TKD   v01 - initial version
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Process									    #####
-- ##############################################################################################################

---------------------------------------------------------------------------------
--  Cable Availibility
---------------------------------------------------------------------------------
SELECT account_number
      ,CASE  WHEN cable_postcode ='N' THEN 'N'
             WHEN cable_postcode ='n' THEN 'N'
             WHEN cable_postcode ='Y' THEN 'Y'
             WHEN cable_postcode ='y' THEN 'Y'
                                      ELSE 'N/A'
       END AS Cable_area
into #cable
  FROM VIQ_HH_ACCOUNT_TMP as ads
       LEFT OUTER JOIN sk_prod.broadband_postcode_exchange  AS bb
       ON postcode_no_space = replace(bb.cb_address_postcode,' ','');
commit;


--      create index
CREATE   HG INDEX idx06 ON #cable(account_number);

--      update CABLE_AVAILABLE
UPDATE VIQ_HH_ACCOUNT_TMP
SET  CABLE_AVAILABLE = cab.Cable_area
FROM VIQ_HH_ACCOUNT_TMP  AS Base
  INNER JOIN #cable AS cab
        ON base.account_number = cab.account_number;
commit;

drop table #cable;
commit;



-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


