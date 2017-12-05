/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying building type of a 
#		household (Version 2).
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
#h_property_type_v2	Property Type V2	
#
#Property Type is a household level variable that identifies the type of building of an address
#
###############################################################################*/


-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household, 
case 
h_property_type_v2 
when '0' then 	'Purpose built flats'
when '1' then	'Converted flats'
when '2' then	'Farm'
when '3' then	'Named building'
when '4' then	'Other type'
when 'U' then	'Unknown'
else null end 'Property Type V2'
from sk_prod.experian_consumerview
where h_property_type_v2 is not null


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
