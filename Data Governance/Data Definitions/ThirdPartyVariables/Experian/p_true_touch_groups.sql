/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying true_touch group types
#		for each individual within the household.
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
#p_true_touch_group	Person true touch group	
#
#TrueTouch classifies all UK consumers into 22 types and 
#6 groups based upon their channel preferences, motivation and promotional orientation
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household, 
case p_true_touch_groups 
when  '1' then	'Experienced Netizens'
when '2' then 	'Cyber Tourists'
when '3' then 	'Digital Culture'
when '4' then 	'Modern Media Margins'
when '5' then 	'Traditional Approach'
when '6' then 	'New Tech Novices'
when 'U' then 	'Unclassified'
else 'Unclassified' end 'Person true touch groups'
from sk_prod.experian_consumerview
where p_true_touch_groups is not null

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
