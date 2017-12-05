/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying HH FSS V3 Group of a 
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
#h_fss_v3_group	Household FSS V3 Group	
#
#
#Financial Strategy Segments (FSS) is Experian’s leading consumer classification 
#focused on financial behaviours and was developed to support financial services companies 
#target their products and services.  
#FSS identifies the underlying factors that influence consumer behaviour segmenting 
#the UK into 93 distinct person types, 50 Household types and 15 Household Groups 
#each with unique characteristics. 
#These distinct financial types comprehensively describe their typical financial product holdings, 
#behavioural and future intentions as well as summarising their key 
#socio-economic and demographic characteristics.
###############################################################################*/


-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################



select top 100 cb_key_household, 
CASE h_fss_v3_group                    
WHEN    'A' THEN    'A Bright Futures'
WHEN    'B' THEN    'B Single Endeavours'
WHEN    'C' THEN    'C Young Essentials'
WHEN    'D' THEN    'D Growing Rewards'
WHEN    'E' THEN    'E Family Interest'
WHEN    'F' THEN    'F Accumulated Wealth'
WHEN    'G' THEN    'G Consolidating Assets'
WHEN    'H' THEN    'H Balancing Budgets'
WHEN    'I' THEN    'I Stretched Finances'
WHEN    'J' THEN    'J Established Reserves'
WHEN    'K' THEN    'K Seasoned Economy'
WHEN    'L' THEN    'L Platinum Pensions'
WHEN    'M' THEN    'M Sunset Security'
WHEN    'N' THEN    'N Traditional Thrift'
WHEN    'U' THEN    'U Unallocated'
ELSE                'Missing'
END 'Household FSS V3 Group'
from sk_prod.experian_consumerview
where h_fss_v3_group is not null


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
