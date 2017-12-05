/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying ABC1 members of a 
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
#h_family_lifestage	Family Lifestage	
#
#Family Lifestage is a household level demographic segmentation 
#that shows the combined stage of life and family status, including children
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household, 
case h_family_lifestage 
when '00' then 'Young singles/homesharers'
when '01' then 'Young family no children <18'
when '02' then 'Young family with children <18'
when '03' then 'Young household with children <18'
when '04' then 'Mature singles/homesharers'
when '05' then 'Mature family no children <18'
when '06' then 'Mature family with children <18'
when '07' then 'Mature household with children <18'
when '08' then 'Older single'
when '09' then 'Older family no children <18'
when '10' then 'Older family/household with children<18'
when '11' then 'Elderly single'
when '12' then 'Elderly family no children <18'
when 'U' then 'Unclassified'
else null end 'Family Lifestage'
from sk_prod.experian_consumerview
where h_family_lifestage is not null

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
