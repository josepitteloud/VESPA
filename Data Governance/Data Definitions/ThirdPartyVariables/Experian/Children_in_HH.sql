/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying if 
#			there are any children in the household.
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 20/08/2012  TKD   v01 - initial version
# 22/02/2013  TKD   v02 - reworked
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################

SELECT TOP 100 cb_key_household,
CHILDREN_IN_HH =        CASE WHEN convert(integer, h_number_of_children_in_household_2011) > 0 THEN 'Y'
                              WHEN convert(integer, h_number_of_children_in_household_2011) = 0 THEN 'N'
                         ELSE 'UN' END CHILDREN_IN_HH
INTO #temp_AGEF
FROM sk_prod.EXPERIAN_CONSUMERVIEW
GROUP BY cb_key_household;

commit;


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################