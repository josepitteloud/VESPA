/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for the postcode mosaic for Scottish
#		households.
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
#pc_mosaic_scotland_group	Postcode mosaic Scotland group	
#
#Mosaic Scotland is a geodemographic classification which provides the most detailed and meaningful information 
#about people living in Scotland. Covering 2.4 million households, Mosaic Scotland classifies each Scottish postcode 
#into one of 10 groups and 44 types.  
#Mosaic Scotland is designed specifically to help identify the characteristics 
#that make living in Scotland different from the rest of the UK. 
#It paints a rich picture of Scottish consumers in terms of socio-demographics, lifestyles and behaviour, 
#and provides a detailed understanding of Scottish society. 
#It is an essential tool for any organisation that wishes to understand more about Scottish consumers, 
#and develop successful marketing solutions that are tailored to the needs of the Scottish marketplace.
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household, 
case pc_mosaic_scotland_group 
when 'A' then 	'Upper Echelons'
when 'B' then 	'Families on the Move'
when 'C' then 	'Small Town Propriety'
when 'D' then 	'Country Lifestyles'
when 'E' then 	'Urban Sophisticates'
when 'F' then 	'Town Centre Singles'
when 'G' then 	'Renters Now Owning '
when 'H' then 	'Low Income Families'
when 'I' then 	'State Beneficiaries'
when 'J' then 	'Shades of Grey'
when 'U' then   	'Unclassified'
when '0' then 	'Record not in Scotland'
else 'Unclassified' end 'Postcode mosaic Scotland group'
from sk_prod.experian_consumerview
where pc_mosaic_scotland_group is not null

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
