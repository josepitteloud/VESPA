/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying months peoples insurance
#		renewals are up
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
#renewal_month	Renewal Month	
#
#Derived Insurance Renewal Month - Home
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################

select top 100 cb_key_household,
case renewal_month 
when '1' then 'January'
when '2' then 'February'
when '3' then 'March'
when '4' then 'April'
when '5' then 'May'
when '6' then 'June'
when '7' then 'July'
when '8' then 'August'
when '9' then 'September'
when '10' then 'October'
when '11' then 'November'
when '12' then 'December'
else 'Unclassified'
end from sk_prod.experian_consumerview
where renewal_month is not null

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################

