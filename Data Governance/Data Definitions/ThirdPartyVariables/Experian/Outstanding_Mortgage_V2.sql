/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying the outstanding 
#		mortgage on an address (Version 2).
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
#h_outstanding_mortgage_v2	Outstanding Mortgage V2	
#
#Outstanding Mortgage identifies the value of the outstanding mortgage at an address.  
#Note that this relates to a residential mortgage i.e. held by the residents of the property
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 ck_key_household,
case h_outstanding_mortgage_v2 
when '0' then	'No Mortgage'
when '1' then	'Under £50000'
when '2' then	'£50000 - £99999'
when '3' then	'£100000 - £149999'
when '4' then	'£150000 - £249999'
when '5' then	'£250000+'
when 'U' then	'Unclassified'
else null end
from sk_prod.experian_consumerview
where h_outstanding_mortgage_v2 is not null


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
