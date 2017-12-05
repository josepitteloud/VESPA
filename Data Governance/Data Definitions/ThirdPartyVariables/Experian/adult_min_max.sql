
/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying the household composition
#		of a household.
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
#--Attribute Name
#
#Household_oldest_adult_age
#Household_youngest_adult_age
#
#--Attribute Descriptive name
#
#Identifies at a <Household> Level the age of the oldest and youngest adult person 
#who resides at that property, based on the Experian data.
#
#--Attribute Description
#
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


SELECT   CV.cb_key_household
        ,max(p_actual_age) as Max_age
        ,min(p_actual_age) as Min_age
INTO #age
FROM sk_prod.EXPERIAN_CONSUMERVIEW AS CV
GROUP BY cv.cb_key_household;

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
