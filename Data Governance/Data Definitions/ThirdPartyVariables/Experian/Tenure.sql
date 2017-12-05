/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying the tenure of a property.
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 20/08/2012  TKD   v01 - initial version
#
###############################################################################*/


-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


/*###############################################################################
#h_tenure	Tenure	
#
#Tenure is a household level demographic variable that identifies 
#whether a property is owner occupied, council/housing association or privately rented
#
###############################################################################*/

--VIQ

select top 100 cb_key_household,
case h_tenure_v2
when '0' then 'Owner occupied'
when '1' then 'Privately rented'
when '2' then 'Council / housing association'
when 'U' then 'Unclassified'
else null end
from sk_prod.experian_consumerview
where h_tenure_v2 is not null

--VIQ END

--ADSMART

SELECT TOP 100 CB_KEY_HOUSEHOLD,
CASE WHEN base.h_tenure_v2 in ('1','2') THEN  'No'
WHEN base.h_tenure_v2 =  ('0')     THEN  'Yes'
ELSE null
END 
from sk_prod.experian_consumerview base
where h_tenure is not null

--ADSMART END

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
