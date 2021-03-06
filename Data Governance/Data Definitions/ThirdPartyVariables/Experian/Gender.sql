/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying gender of
#		members of a household.
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
#p_gender	Gender	
#
#Gender is a person level demographic variable that identifies the gender of each individual living at an address
###############################################################################*/


-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################

select top 100 cb_key_household,
case p_gender 
when '0' then 	'Male'
when '1' then 	'Female'
when 'U' then 	'Unclassified'
else 'Unclassified' end 'Gender'
from sk_prod.experian_consumerview
where p_gender is not null

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
