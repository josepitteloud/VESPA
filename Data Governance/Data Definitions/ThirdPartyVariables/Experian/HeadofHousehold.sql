/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying a household head with 
#		any household.
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
#head_of_household	Head of household

#Head of Household is a person level demographic variable that identifies the individual(s)
#most likely to be head of the household at an address.
#Wherever possible a male and a female are classed as head of household
###############################################################################*/


-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household,
case head_of_household
when '0' then 'not head of household'
when '1'	then 'head of household'
when 'U'	then 'Unclassified'
else null end 'Head of household'
from sk_prod.experian_consumerview
where head_of_household is not null

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
