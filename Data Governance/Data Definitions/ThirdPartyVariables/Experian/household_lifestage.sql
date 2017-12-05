
/*###############################################################################
# Created on:   10/01/2013
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying the 
#		shows the combined stage of life and family status
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
#h_lifestage	
#
#--Attribute Descriptive name
#
#Household Lifestage	
#
#--Attribute Description
#
#Household Lifestage is a household level demographic 
#segmentation that shows the combined stage of life and family status.
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################

select top 100 cb_key_household,
case h_lifestage
when '00' then 	'Very young family'
when '01' then 	'Very young single'
when '02' then 	'Very young homesharers'
when '03' then 	'Young family'
when '04' then 	'Young single'
when '05' then 	'Young homesharers'
when '06' then 	'Mature family'
when '07' then 	'Mature singles'
when '08' then 	'Mature homesharers'
when '09' then 	'Older family'
when '10' then 	'Older single'
when '11' then 	'Older homesharers'
when '12' then 	'Elderly family'
when '13' then 	'Elderly single'
when '14' then 	'Elderly homesharers'
when 'U ' then 	'Unclassified'
else null end
from sk_prod.experian_consumerview
where h_lifestage is not null

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
