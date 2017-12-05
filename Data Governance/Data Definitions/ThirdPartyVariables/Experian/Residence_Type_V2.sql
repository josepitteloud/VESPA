/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying a residence type of a
#		property (version 2).
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
#h_residence_type_v2	Residence Type V2	
#
#
#Residence Type is a household level demographic variable that identifies 
#whether a property is terraced, semi-detached, detached, a flat or a bungalow
#
###############################################################################*/


-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 ck_key_household, 
case h_residence_type_v2 
when '0' then	'Detached'
when '1'	then 'Semi-detached'
when '2'	then 'Bungalow'
when '3'	then 'Terraced'
when '4'	then 'Flat'
when 'U'	then 'Unclassified'
else null end 'Residence Type V2'
from sk_prod.experian_consumerview
where h_residence_type_v2 is not null


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
