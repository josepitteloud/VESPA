/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying the financial
#		stress within a household.
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
#p_financial_stress	Financial stress	
#
#Financial Stress (sometimes referred to as Level of Indebtedness) 
#identifies an individual’s potential to become over-stretched and struggle with further payments
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household, 
case person_financial_stress 
when '0' then 'Very low'
when '1' then 'Low'
when '2' then 'Medium'
when '3' then 'High'
when '4' then 'Very high'
when 'U' then 'Unclassified'
else null end 'Person Financial Stress'
from sk_prod.experian_consumerview
where person_financial_stress is not null

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
