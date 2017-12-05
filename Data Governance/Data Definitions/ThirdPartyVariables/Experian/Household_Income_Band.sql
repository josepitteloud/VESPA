/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying the likely household 
#		income of a household.
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
#h_income_band	Household income band	
#
#Household Income identifies the likely household income at an address.
###############################################################################*/


-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household, 
case household_income_bands 
when '0' then '< �10,000'
when '1' then '�10,000 - �14,999'
when '2' then '�15,000 - �19,999'
when '3' then '�20,000 - �24,999'
when '4' then '�25,000 - �29,999'
when '5' then '�30,000 - �39,999'
when '6' then '�40,000 - �49,999'
when '7' then '�50,000 - �59,999'
when '8' then '�60,000 - �74,999'
when '9' then '�75,000 +'
when 'U' then 'Unclassified'
else null end 'Household Income' from
sk_prod.experian_consumerview
where household_income_bands is not null

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
