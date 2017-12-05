/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying the mosaic type
#		of members of a household.
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
#pc_mosaic_uk_type	Postcode mosaic UK type	 
#
#Mosaic UK 2009 is a geodemographic classification that paints a rich picture of 
#UK consumers in terms of socio-demographics, lifestyles and behaviour, 
#and provides a detailed understanding of UK society. 
#It is an essential tool for any organisation that wishes to understand more 
#about UK consumers, and develop successful marketing solutions 
#that are tailored to the needs of the UK marketplace.  
#Mosaic UK classifies all UK consumers into 67 distinct lifestyle types 
#and 15 groups which comprehensively describe their socio-economic and socio-cultural behaviour.
###############################################################################*/


-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################

select top 100 cb_key_household,
case h_mosaic_uk_2009_type
when '01' then 	'Global Power Brokers'
when '02' then 	'Voices of Authority'
when '03' then 	'Business Class'
when '04' then 	'Serious Money'
when '05' then 	'Mid-Career Climbers'
when '06' then 	'Yesterday''s Captains'
when '07' then 	'Distinctive Success'
when '08' then 	'Dormitory Villagers'
when '09' then 	'Escape to the Country'
when '10' then 	'Parish Guardians'
when '11' then 	'Squires Among Locals'
when '12' then 	'Country Loving Elders'
when '13' then 	'Modern Agribusiness'
when '14' then 	'Farming Today'
when '15' then 	'Upland Struggle'
when '16' then 	'Side Street Singles'
when '17' then 	'Jacks of All Trades'
when '18' then 	'Hardworking Families'
when '19' then 	'Innate Conservatives'
when '20' then 	'Golden Retirement'
when '21' then 	'Bungalow Quietude'
when '22' then 	'Beachcombers'
when '23' then 	'Balcony Downsizers'
when '24' then 	'Garden Suburbia'
when '25' then 	'Production Managers'
when '26' then 	'Mid-Market Families'
when '27' then 	'Shop Floor Affluence'
when '28' then 	'Asian Attainment'
when '29' then 	'Footloose Managers'
when '30' then 	'Soccer Dads and Mums'
when '31' then 	'Domestic Comfort'
when '32' then 	'Childcare Years'
when '33' then 	'Military Dependants'
when '34' then 	'Buy-to-Let Territory'
when '35' then 	'Brownfield Pioneers'
when '36' then 	'Foot on the Ladder'
when '37' then 	'First to Move In'
when '38' then 	'Settled Ex-Tenants'
when '39' then 	'Choice Right to Buy'
when '40' then 	'Legacy of Labour'
when '41' then 	'Stressed Borrowers'
when '42' then 	'Worn-Out Workers'
when '43' then 	'Streetwise Kids'
when '44' then 	'New Parents in Need'
when '45' then 	'Small Block Singles'
when '46' then 	'Tenement Living'
when '47' then 	'Deprived View'
when '48' then 	'Multicultural Towers'
when '49' then 	'Re-Housed Migrants'
when '50' then 	'Pensioners in Blocks'
when '51' then 	'Sheltered Seniors'
when '52' then 	'Meals on Wheels'
when '53' then 	'Low Spending Elders'
when '54' then 	'Clocking Off'
when '55' then 	'Backyard Regeneration'
when '56' then 	'Small Wage Owners'
when '57' then 	'Back-to-Back Basics'
when '58' then 	'Asian Identities'
when '59' then 	'Low-Key Starters'
when '60' then 	'Global Fusion'
when '61' then 	'Convivial Homeowners'
when '62' then 	'Crash Pad Professionals'
when '63' then 	'Urban Cool'
when '64' then 	'Bright Young Things'
when '65' then 	'Anti-Materialists'
when '66' then 	'University Fringe'
when '67' then 	'Study Buddies'
when '99' then 	'Unclassified'
else null end 'h_mosaic_uk_type'
from sk_prod.experian_consumerview

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################