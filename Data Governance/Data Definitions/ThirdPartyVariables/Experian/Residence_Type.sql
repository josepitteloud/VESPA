/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying a property's residence
#		type.
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
#h_residence_type	Residence type	
#
#Residence Type is a household level demographic variable that identifies 
#whether a property is terraced, semi-detached, detached, a flat or a bungalow
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household,
case residence_type 
when '0' then 'Detached'
when '1' then 'Semi-detached'
when '2' then 'Bungalow'
when '3' then 'Terraced'
when '4' then 'Flat'
when 'U' then 'Unclassified'
else null end Residence_Type
from sk_prod.experian_consumerview
where residence_type is not null

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
