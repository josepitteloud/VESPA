/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying a HH FSS v3 type for
#		a household
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
#h_fss_v3_type	Household FSS V3 Type
#
#Financial Strategy Segments (FSS) is Experian’s leading consumer classification
#focused on financial behaviours and was developed to support financial services companies
#target their products and services.
#FSS identifies the underlying factors that influence consumer behaviour
#segmenting the UK into 93 distinct person types, 50 Household types and 15 Household Groups
#each with unique characteristics. These distinct financial types comprehensively describe
#their typical financial product holdings, behavioural and future intentions
#as well as summarising their key socio-economic and demographic characteristics.
###############################################################################*/


-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household,
case h_fss_v3_type
when '01' then	'Equity Ambitions'
when '02'	then 	'Portable Assets'
when '03'	then 	'Early Settlers'
when '04'	then 	'First Foundations'
when '05'	then 	'Urban Opportunities'
when '06'	then 	'Flexible Margins'
when '07'	then 	'Tomorrow''s Earners'
when '08'	then 	'Entry-level Workers'
when '09'	then 	'Cash Stretchers'
when '10'	then 	'Career Priorities'
when '11'	then 	'Upward Movers'
when '12'	then 	'Family Progression'
when '13'	then 	'Savvy Switchers'
when '14'	then 	'New Nesters'
when '15'	then 	'Security Seekers'
when '16'	then 	'Premier Portfolios'
when '17'	then 	'Fast-track Fortunes'
when '18'	then 	'Asset Accruers'
when '19'	then 	'Self-made Success'
when '20'	then 	'Golden Outlook'
when '21'	then 	'Sound Positions'
when '22'	then 	'Single Accumulators'
when '23'	then 	'Mid-range Gains'
when '24'	then 	'Extended Outlay'
when '25'	then 	'Modest Mortgages'
when '26'	then 	'Overworked Resources'
when '27'	then 	'Self-reliant Realists'
when '28'	then 	'Canny Owners'
when '29'	then 	'Squeezed Families'
when '30'	then 	'Pooled Kitty'
when '31'	then 	'High Demands'
when '32'	then 	'Value Hunters'
when '33'	then 	'Low Cost Living'
when '34'	then 	'Guaranteed Provision'
when '35'	then 	'Steady Savers'
when '36'	then 	'Deferred Assurance'
when '37'	then 	'Practical Preparers'
when '38'	then 	'Persistent Workers'
when '39'	then 	'Lifelong Low-spenders'
when '40'	then 	'Experienced Renters'
when '41'	then 	'Sage Investors'
when '42'	then 	'Dignified Elders'
when '43'	then 	'Comfortable Legacy'
when '44'	then 	'Semi-retired Families'
when '45'	then 	'Cautious Stewards'
when '46'	then 	'Classic Moderation'
when '47'	then 	'Quiet Simplicity'
when '48'	then 	'Senior Sufficiency'
when '49'	then 	'Ageing Fortitude'
when '50'	then 	'State Veterans'
when '99'	then 	'Unallocated'
else null end 'Household FSS V3 Type'
from sk_prod.experian_consumerview
where h_fss_v3_type is not null

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
