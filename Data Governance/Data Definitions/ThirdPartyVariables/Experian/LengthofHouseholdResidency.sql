/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying length of time the 
#		longest residing head of the household is been at the address
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
#h_length_of_residency	Length of residency - household level	
#
#At household level, Length of Residency identifies the length of time 
#that the longest residing head of household has been at the same address
#
###############################################################################*/


-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household,
case h_length_of_residency 
when '00' then 	'Up to 1 year'
when '01' then 	'1 year'
when '02' then 	'2 years'
when '03' then 	'3 year'
when '04' then 	'4 year'
when '05' then 	'5 years'
when '06' then 	'6 year'
when '07' then 	'7 year'
when '08' then 	'8 year'
when '09' then 	'9 year'
when '10' then 	'10 year'
when '11' then 	'11+ year'
when 'U' then 'Unclassified'
else 'Unclassified' end 'Length of residency - household level'
from sk_prod.experian_consumerview
where h_length_of_residency is not null


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
