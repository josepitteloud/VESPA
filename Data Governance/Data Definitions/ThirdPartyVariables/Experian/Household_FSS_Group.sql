/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying hh FSS Group within a  
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
#h_fss_group	Household FSS group
#
#Financial Strategy Segments (FSS) is a person and household level segmentation
#developed to help financial services companies target their  products and services.
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household,
case h_fss2_groups
when 'A' then 	'Successful Start'
when 'B' then 	'Happy Housemates'
when 'C' then 	'Surviving Singles'
when 'D' then 	'On the Bread Line'
when 'E' then 	'Flourishing Families'
when 'F' then 	'Credit-hungry Families'
when 'G' then 	'Gilt-edged Lifestyles'
when 'H' then 	'Mid-life Affluence'
when 'I' then 	'Modest Mid-years'
when 'J' then 	'Advancing Status'
when 'K' then 	'Ageing Workers'
when 'L' then 	'Wealthy Retirement'
when 'M' then 	'Elderly Deprivation'
when 'U' then    	'Unclassified'
else 'Unclassified' end
from sk_prod.experian_consumerview
where h_fss2_groups is not null

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
