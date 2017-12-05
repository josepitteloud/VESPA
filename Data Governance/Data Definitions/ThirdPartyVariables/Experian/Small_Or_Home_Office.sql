/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying if a property is a
#		small or home office.
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
#h_small_or_home_office	SOHO flag (Small Office Home Office)	
#
#Small or Home Office (sometimes referred to as a SoHo) is a 
#household level financial variable that identifies whether 
#one or more of the occupants of the household runs a business from home.
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household,
case small_or_home_office
when '0' then 'Not small or home office'
when '1'  then 'Small or home office'
else null end 'Small or Home Office'
from sk_prod.experian_consumerview
where small_or_home_office is not null

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
