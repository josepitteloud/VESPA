/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying a person's employment
#		status.
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
#person_employment_status Employment status  
#
#Employment Status is a person level variable, which identifies the individual’s employment status, 
#for example employed full time or unemployed
#
###############################################################################*/


-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household, 
case person_employment_status_v2
when '0' then 	'Employed Full Time'
when '1' then	'Student / Other Economically Active'
when '2' then	'Part time / Housewife'
when '3' then	'Retired'
when 'U' then	'Unallocated'
else null end 'p_employment_status_v2'
from sk_prod.experian_consumerview
where person_employment_status_v2 is not null

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
