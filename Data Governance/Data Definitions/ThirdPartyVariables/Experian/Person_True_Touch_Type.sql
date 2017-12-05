/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying the True Touch Type
#		of an individual within a household.
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
#p_true_touch_type	Person true touch type	
#
#TrueTouch classifies all UK consumers into 22 types and 6 groups based upon their channel preferences, 
#motivation and promotional orientation
#
###############################################################################*/


-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household,
case p_true_touch_types 
when '01' then 'Information@speed'
when '02' then 	'Practical Surfers'
when '03' then 	'Remote Info-junkies'
when '04' then 	'Intellectual Digerati'
when '05' then 	'E-tail Explorers'
when '06' then 	'Cautious E-converts'
when '07' then 	'Internet Dabblers'
when '08' then 	'Web Connectors'
when '09' then 	'Gadget-mad Technophiles'
when '10' then 	'Real-time Friends'
when '11' then 	'Ceulluar Society'
when '12' then 	'Plug-and-Play'
when '13' then 	'txt m8s'
when '14' then 	'Techno-trailers'
when '15' then 	'Catalogue Conventionals'
when '16' then 	'Paper-based Opinions'
when '17' then 	'Ad-averse Listeners'
when '18' then 	'Local Shoppers'
when '19' then 	'TV Influence'
when '20' then 	'Personal Preference'
when '21' then 	'Virtual Experimenters'
when '22' then 	'Borderline Online'
when '99' then 	'Unclassified'
else 'Unclassified' end 'Person true touch types'
from sk_prod.experian_consumerview
where p_true_touch_types is not null

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
