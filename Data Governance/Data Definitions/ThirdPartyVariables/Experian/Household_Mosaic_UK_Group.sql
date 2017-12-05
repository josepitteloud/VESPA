/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for the UK Household Mosaic 
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
# 09/01/2013  TKD   V02 - Modified to use h_mosaic_uk_group column
#
###############################################################################*/

/*###############################################################################
#h_mosaic_uk_group	Household mosaic UK group	
#
#Mosaic UK is a geodemographic classification that paints a rich picture of UK consumers in terms of 
#socio-demographics, lifestyles and behaviour, and provides a detailed understanding of UK society. 
#It is an essential tool for any organisation that wishes to understand more about UK consumers, 
#and develop successful marketing solutions that are tailored to the needs of the UK marketplace.  
#Mosaic UK classifies all UK consumers into 67 distinct lifestyle types and 
#15 groups which comprehensively describe their socio-economic and socio-cultural behaviour.
#
###############################################################################*/


-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household, 
case h_mosaic_uk_group 
when 'A' then 'Alpha Territory'
when 'B' then 'Professional Rewards'
when 'C' then 'Rural Solitude'
when 'D' then 'Small Town Diversity'
when 'E' then 'Active Retirement'
when 'F' then 'Suburban Mindsets'
when 'G' then 'Careers and Kids'
when 'H' then 'New Homemakers'
when 'I' then 'Ex-Council Community'
when 'J' then 'Claimant Cultures'
when 'K' then 'Upper Floor Living'
when 'L' then 'Elderly Needs'
when 'M' then 'Industrial Heritage'
when 'N' then 'Terraced Melting Pot'
when 'O' then 'Liberal Opinions'
when 'U' then 'Unclassified'
else null end 'Mosaic UK Household Group' 
from sk_prod.experian_consumerview
where h_mosaic_uk_group is not null


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
