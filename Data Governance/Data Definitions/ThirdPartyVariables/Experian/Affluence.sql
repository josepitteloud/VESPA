/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying affluence within a 
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


/*#################################################################################
--Description
--p_affluence	Affluence

Affluence is a person level variable that identifies an individual’s affluence based on a number of key variables.  
An individual's affluence was determined by considering the following variables:
Income
Property value
Net worth.

These variables were then compared to the national averages.
 Levels of over-representation or under-representation were combined to create an Affluence score. 
This score was then ranked and banded into 20 bands (semi-deciles).
#################################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household, 
        ,h_affluence_v2_desc = Case when h_affluence_v2 in ('00','01','02') then 'Very Low'
                                  when h_affluence_v2 in ('03','04', '05') then 'Low'
                                  WHEN h_affluence_v2 in ('06','07','08')  then 'Mid Low'
                                  WHEN h_affluence_v2 in ('09','10','11')  then 'Mid'
                                  WHEN h_affluence_v2 in ('12','13','14')  then 'Mid High'
                                  WHEN h_affluence_v2 in ('15','16','17')  then 'High'
                                  WHEN h_affluence_v2 in ('18','19')       then 'Very High'
                                  else null
                               end
from sk_prod.experian_consumerview
where h_affluence_v2_desc is not null

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
