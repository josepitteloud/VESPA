/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying the Mosaic
#		of Scottish households.
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
#h_mosaic_scotland_type	Household mosaic Scotland type	
#
#Mosaic Scotland is a geodemographic classification which provides 
#the most detailed and meaningful information about people living in Scotland. 
#Covering 2.4 million households, Mosaic Scotland classifies each Scottish postcode into one of 10 groups and 44 types.  
#Mosaic Scotland is designed specifically to help identify the characteristics that make living in Scotland different 
#from the rest of the UK. It paints a rich picture of Scottish consumers in terms of socio-demographics, 
#lifestyles and behaviour, and provides a detailed understanding of Scottish society. 
#It is an essential tool for any organisation that wishes to understand more about Scottish consumers, 
#and develop successful marketing solutions that are tailored to the needs of the Scottish marketplace.
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################

select top 100 cb_key_household, 
case mosaic_scotland_household_type 
when '01' then 'Captains of Industry'
when '02' then 'Wealth of Experience'
when '03' then 'New Influentials'
when '04' then 'Successful Managers'
when '05' then 'White Collar Owners'
when '06' then 'Emerging High Status'
when '07' then 'New Suburbanites'
when '08' then 'Settling In'
when '09' then 'Military Might'
when '10' then 'Songs of Praise'
when '11' then 'Ageing in Suburbia'
when '12' then 'Blue Collar Owners'
when '13' then 'Towns in Miniature'
when '14' then 'Rural Playgrounds'
when '15' then 'Agrarian Heartlands'
when '16' then 'Isolated Farmsteads'
when '17' then 'Scenic Wonderland'
when '18' then 'Far Away Islanders'
when '19' then 'Prestige Tenements'
when '20' then 'Studio Singles'
when '21' then 'Rucksack and Bicycle'
when '22' then 'College and Campus'
when '23' then 'Inner City Transience'
when '24' then 'Cosmopolitan Chic'
when '25' then 'Tenement Lifestyles'
when '26' then 'Downtown Flatlets'
when '27' then '30 Something Singles'
when '28' then 'Small Town Pride'
when '29' then 'Dignified Seniors'
when '30' then 'Sought after Schemes'
when '31' then 'Rustbelt Renaissance'
when '32' then 'Planners Paradise'
when '33' then 'Smokestack Survivors'
when '34' then 'Quality City Schemes'
when '35' then 'Lathe and Loom'
when '36' then 'Indebted Families'
when '37' then 'Pockets of Poverty'
when '38' then 'Mid Rise Breadline'
when '39' then 'Room and Kitchen'
when '40' then 'Families in the Sky'
when '41' then 'Elders 4-in-a-Block'
when '42' then 'Greys in Small Flats'
when '43' then 'Skyline Seniors'
when '44' then 'Twilight Infirmity'
when '99' then 'Unclassified'
when '00' then 'Record not in Scotland'
else null end 'Household mosaic Scotland type'
from sk_prod.experian_consumerview
where mosaic_scotland_household_type is not null


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
