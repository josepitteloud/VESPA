
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
#h_household_composition	
#
#--Attribute Descriptive name
#
#Household Composition	
#
#--Attribute Description
#
#Household Composition is a household level demographic 
#variable that identifies the type of family living at an address
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household,
case h_household_composition
when '00' then 'Families'
when '01' then 'Extended family'
when '02' then 'Extended household'
when '03' then 'Pseudo family'
when '04' then 'Single male'
when '05' then 'Single female'
when '06' then 'Male homesharers'
when '07' then 'Female homesharers'
when '08' then 'Mixed homesharers'
when '09' then 'Abbreviated male families'
when '10' then 'Abbreviated female families'
when '11' then 'Multi-occupancy dwelling'
when 'U' then 'Unclassified'
else null end 'Household Composition'
from sk_prod.experian_consumerview
where h_household_composition is not null

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
