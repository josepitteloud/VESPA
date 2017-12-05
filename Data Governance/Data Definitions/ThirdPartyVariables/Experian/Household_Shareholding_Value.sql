/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying the Shareholding value 
#		within a household.
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
#h_shareholding_value	Household shareholding value band	
#
#Shareholding Value identifies the cumulative value of shares held by individuals with the same surname at an address.
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household, 
case household_shareholding_value 
when '0' then 'No shares'
when '1' then 'Low value (<�10,000)'
when '2' then 'High value (>�10,000)'
else null end household_shareholding_value
from sk_prod.experian_consumerview
where household_shareholding_value is not null


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
