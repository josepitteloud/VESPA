/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for the outstanding mortgage remaining
#		on a household.
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
#h_outstanding_mortgage	Outstanding mortgage	
#
#Outstanding Mortgage identifies the value of the outstanding mortgage at an address
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household, 
case household_outstanding_mortgage 
when '0' then 'No outstanding mortgage'
when '1' then '<£20k outstanding'
when '2' then '£20k-£50k outstanding'
when '3' then '£50k-£100k outstanding'
when '4' then '£100k+ outstanding'
when 'U' then 'Unclassified'
else null end 'Household Outstanding Mortgage'
from sk_prod.experian_consumerview
where household_outstanding_mortgage is not null


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
