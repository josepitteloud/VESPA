/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for an individual's marital status
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
#marital_status	Marital status
#
#Marital Status is a person level demographic variable that identifies the marital status of each individual living at an address. 
#Marital Status is created by analysing the combination of adults living at an address. 
#Where a couple can be identified by looking at surname combinations and individual’s ages they are classified as married
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household,
case marital_status
when '0' then 'Single'
when '1'	then 'Married'
when 'U'	then 'Unclassified'
else null end 'Marital Status'
from sk_prod.experian_consumerview
where marital_status is not null

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
