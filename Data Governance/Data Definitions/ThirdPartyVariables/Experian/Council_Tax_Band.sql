/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying the council tax band
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

Description

h_property_council_taxation	Council Tax band   	

Property Council Taxation is a household level segmentation based on actual council tax bands 
for the vast majority of residential properties in England, Wales and Scotland 
(Northern Ireland does not use the Council Taxation system). 
Where actual data cannot be matched to a household, then a dominant postcode value is used

###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household, 
case h_property_council_taxation
when '0' then 'England - Up to £40,000, 
Wales - Up to £30,000, 
Scotland - Up to £27,0'
when '1' then 'England - £40,001 to £52,000, 
Wales - £30,001 to £39,000, 
Scotland - £27,001 to £35,0000'
when '2' then 'England - £52,001 to £68,000, 
Wales - £39,001 to £51,000, 
Scotland - £35,001 to £45,000'
when '3' then 'England - £68,001 to £88,000, 
Wales - £51,001 to £66,000, 
Scotland - £45,001 to £58,000'
when '4' then 'England - £88,001 to £120,000, 
Wales - £66,001 to £90,000, 
Scotland - £58,001 to £80,000'
when '5' then 'England - £120,001 to £160,000, 
Wales - £90,001 to £120,000, 
Scotland - £80,001 to £106,000'
when '6' then 'England - £160,001 to £320,000, 
Wales - £120,001 to £240,000, 
Scotland - £106,001 to £212,000'
when '7' then 'England - Over £320,000, 
Wales - Over £240,000, 
Scotland - Over £212,000'
when 'U' then 'Unclassified'
else null end 'Council Tax Band'
from sk_prod.experian_consumerview
where h_property_council_taxation is not null


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
