/*###############################################################################
# Created on:   16/01/2013
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for identifying the Mirror segments
#		of any children within the Household
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 16/01/2013  TKD   v01 - initial version
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Process									    #####
-- ##############################################################################################################

select top 100 cb_key_household
,Mirror_has_children  =  CASE WHEN convert(integer, h_number_of_children_in_household_2011) > 0 THEN 'Y'
                              WHEN convert(integer, h_number_of_children_in_household_2011) = 0 THEN 'N'
                         ELSE 'M' END
from sk_prod.experian_consumerview
where h_number_of_children_in_household_2011 is not null


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################

