/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying types of person likely 
#		likely to make decisions for the household
#		
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


-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


/*###############################################################################
#h_decision_maker_type	Decision maker type	
#
#
#Decision Maker Type is a household level demographic variable that identifies the 
#type of person or persons who are likely to make decisions for the whole household
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household, 
case decision_maker_type 
when '00' then 'Male - young'
when '01' then 'Male - middle'
when '02' then 'Male - old'
when '03' then 'Female - young'
when '04' then 'Female - middle'
when '05' then 'Female - old'
when '06' then 'Couple - young'
when '07' then 'Couple - middle'
when '08' then 'Couple - old'
when '09' then 'Sharers - young'
when '10' then 'Sharers - middle'
when '11' then 'Sharers - old'
when 'U' then 'Unclassified'
else null end 'Decision maker type'
from sk_prod.experian_consumerview
where decision_maker_type is not null

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
