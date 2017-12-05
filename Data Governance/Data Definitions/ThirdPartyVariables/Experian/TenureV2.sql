/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for the tenure of a property (Version 2).
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


/*###############################################################################
#h_tenure_v2	Tenure V2		
#
#
#Tenure is a household level demographic variable that identifies 
#whether a property is owner occupied, council/housing association or privately rented
#
###############################################################################*/



-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household,
case h_tenure_v2
when '0' then 'Owner occupied'
when '1' then 'Privately rented'
when '2' then	'Council / housing association'
when 'U' then	'Unclassified'
else null end 'household_tenure'
from sk_prod.experian_consumerview
where h_tenure_v2 is not null

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################