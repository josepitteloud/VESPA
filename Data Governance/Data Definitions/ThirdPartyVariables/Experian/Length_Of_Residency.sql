/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying the length of time
#		an individual has been at the same address
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
#p_length_of_residency	Years at address	
#
#
#Length of Residency identifies the length of time 
#that an individual has been at the same address.
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household,
case p_length_of_residency 
when '00' then 	'Up to 1 year'
when '01' then 	'1 year'
when '02' then 	'2 years'
when '03' then 	'3 years'
when '04' then 	'4 years'
when '05' then 	'5 years'
when '06' then 	'6 years'
when '07' then 	'7 years'
when '08' then 	'8 years'
when '09' then 	'9 years'
when '10' then 	'10 years'
when '11' then 	'11+ years'
when 'U' then 	'Unclassified'
else 'Unclassified' end 'Years at address'
from sk_prod.experian_consumerview
where p_length_of_residency is not null


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
