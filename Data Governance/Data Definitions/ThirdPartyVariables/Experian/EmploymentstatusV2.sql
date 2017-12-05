/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying employment_status v2
# 		for an individual within a household.
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
#
#p_employment_status_v2 Employment status V2 
#
#Employment Status is a person level variable, which identifies the individual’s employment status, 
#for example employed full time or unemployed
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household, 
case p_employment_status_v2 
when '0' then 	'Employed Full Time'
when '1' then	'Student / Other Economically Active'
when '2' then	'Part time / Housewife'
when '3' then	'Retired'
when 'U' then	'Unallocated'
else null end 'p_employment_status_v2'
from sk_prod.experian_consumerview
where p_employment_status_v2 is not null

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
