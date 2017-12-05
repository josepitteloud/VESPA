/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying a person type for an
#		an individual within a household.
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
#person_type	Person Type	
#
#
#Identifies the relationships of an individual with others living at an address
#
###############################################################################*/


-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 ck_key_household,
case person_type 
when  'A'	then 'Solus Male'
when 'B'	then 'Solus Female'
when 'C'	then 'Mixed house Male'
when 'D'	then 'Mixed House Female'
when 'E'	then 'Pseudo Male'
when 'F'	then 'Pseudo Female'
when 'U' then 'Unclassified'
else 'Unclassified' end 'Person Type'
from sk_prod.Experian_consumerview
where person_type is not null


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
