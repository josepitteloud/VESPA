/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying the HH FSS Type of a 
#		household.
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
#h_fss_type	Household FSS type	
#
#Household Financial Strategy Segments (FSS) is a  household level segmentation 
#developed to help financial services companies target their financial services products and services.
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household,
case household_fss2_type
when '01'	then 'Up & Coming Elite'
when '02' 	then	'Professional Solos'
when '03' 	then	'Opportunities and Overdrafts'
when '04' 	then	'Looking to the Future'
when '05' 	then	'Limited Livelihoods'
when '06' 	then	'Carefree Kick-off'
when '07' 	then	'Books and Beer'
when '08' 	then	'Getting By Alone'
when '09' 	then	'Solitary Effort'
when '10' 	then	'Straining the Budget'
when '11' 	then	'Child-raising Challenge'
when '12' 	then	'Poor Prospects'
when '13' 	then	'Fully Committed Funds'
when '14' 	then	'Independent Investors'
when '15' 	then	'Confident Consumers'
when '16' 	then	'Family Focused Finance'
when '17' 	then	'Work-life Balance'
when '18' 	then	'Overspending Optimists'
when '19' 	then	'Savvy Big Spenders'
when '20' 	then	'Downscale Mortgagees'
when '21' 	then	'Hocked to the Hilt'
when '22' 	then	'Cream of the Crop'
when '23' 	then	'Corporate Top Dogs'
when '24' 	then	'Smart Money'
when '25' 	then	'Property Tycoons'
when '26' 	then	'Conservative Accumulators'
when '27' 	then	'Asset-rich Achievers'
when '28' 	then	'Dependable Comfort'
when '29' 	then	'Rat Race Escapees'
when '30' 	then	'Conventional Progression'
when '31' 	then	'Cautious Borrowers'
when '32' 	then	'Venerable Workforce'
when '33' 	then	'Family Values'
when '34' 	then	'On Course for Retirement'
when '35' 	then	'Inadequate Provisions'
when '36' 	then	'Pennywise Economy'
when '37' 	then	'Sunset Singles'
when '38' 	then	'Seasoned State Reliance'
when '39' 	then	'Greys in the Pink'
when '40' 	then	'Well-off Down-traders'
when '41' 	then	'Vintage Couples'
when '42' 	then	'Low Cash Flow Elders'
when '43' 	then	'Old-fashioned Prudence'
when '44' 	then	'Shoestring Seniors'
when '45' 	then	'Pensioners in Need'
when '99' 	then	'Unclassified'
else null end 'household_fss2_type'
from sk_prod.experian_consumerview
where household_fss2_type is not null

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
