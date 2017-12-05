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

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################




select top 100 cb_key_household,
CASE h_mosaic_uk_2009_type    
WHEN    '01'  THEN    0		--Global Power Brokers
WHEN    '02'  THEN    0		--Voices of Authority
WHEN    '03'  THEN    0		--Business Class
WHEN    '04'  THEN    0		--Serious Money
WHEN    '05'  THEN    0		--Mid-Career Climbers
WHEN    '06'  THEN    0		--Yesterday's Captains
WHEN    '07'  THEN    0		--Distinctive Success
WHEN    '08'  THEN    0		--Dormitory Villagers
WHEN    '09'  THEN    0		--Escape to the Country
WHEN    '10'  THEN    0		--Parish Guardians
WHEN    '11'  THEN    0		--Squires Among Locals
WHEN    '15'  THEN    0		--Upland Struggle
WHEN    '20'  THEN    0		--Golden Retirement
WHEN    '22'  THEN    0		--Beachcombers
WHEN    '29'  THEN    0		--Footloose Managers
WHEN    '30'  THEN    0		--Soccer Dads and Mums
WHEN    '31'  THEN    0		--Domestic Comfort
WHEN    '33'  THEN    0		--Military Dependants
WHEN    '61'  THEN    0		--Convivial Homeowners
WHEN    '62'  THEN    0		--Crash Pad Professionals
WHEN    '63'  THEN    0		--Urban Cool
WHEN    '65'  THEN    0		--Anti-Materialists
WHEN    '66'  THEN    0		--University Fringe
ELSE                  NULL
END
from sk_prod.experian_consumerview;

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
