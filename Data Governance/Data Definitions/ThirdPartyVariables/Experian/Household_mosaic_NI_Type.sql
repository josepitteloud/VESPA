/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying a household mosaic
#		for Northern Ireland
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
#h_mosaic_ni_type	Household mosaic Northern Ireland type	
#
#Mosaic Northern Ireland is a geodemographic classification which provides the most detailed 
#and meaningful information about people living in Northern Ireland. It classifies all consumers into one of 9 Groups and 36 Types.  
#
#mosaic_n_ireland_household_type 
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################

select top 100 cb_key_household, 
case mosaic_n_ireland_household_type 
when '01' then 'A01 Business Leadership'
when '02' then 'A02 Monied Empty Nesters'
when '03' then 'A03 Cultural Avant Garde'
when '04' then 'B04 High Achievers'
when '05' then 'B05 Public Service Ethos'
when '06' then 'B06 Commuting Country'
when '07' then 'C07 Community Stalwarts'
when '08' then 'C08 Stable Family Semis'
when '09' then 'C09 Multi-family Semis'
when '10' then 'C10 Middle of the Road'
when '11' then 'C11 Suburban Retirement'
when '12' then 'C12 Centres of Tourism'
when '13' then 'D13 Small Town Success'
when '14' then 'D14 Rural Fringe'
when '15' then 'D15 Young Mortgagees'
when '16' then 'D16 Barracks Life'
when '17' then 'D17 New Areas'
when '18' then 'E18 Small Town Estates'
when '19' then 'E19 Blue Collar Thrift'
when '20' then 'F20 Terraced Melting Pot'
when '21' then 'F21 Local & Commercial'
when '22' then 'F22 Student Culture'
when '23' then 'G23 High Rise Residents'
when '24' then 'G24 Pensioners in Flats'
when '25' then 'G25 Market Town Seniors'
when '26' then 'G26 Breadline Pensioners'
when '27' then 'H27 Proud Traditions'
when '28' then 'H28 Hard Pressed Owners'
when '29' then 'H29 Moving up, Staying On'
when '30' then 'H30 Cycles of Poverty'
when '31' then 'I31 Remote, Some Industry'
when '32' then 'I32 Rural Entrepreneurs'
when '33' then 'I33 Marginal Farmland'
when '34' then 'I34 Rural, Some Commuting'
when '35' then 'I35 Farming Heartland'
when '36' then 'I36 Ancestral Memories'
when '99' then 'Unclassified'
when '00' then 'Record not in Northern Ireland'
else null end 'Household mosaic Northern Ireland'
from sk_prod.experian_consumerview
where mosaic_n_ireland_household_type is not null

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
