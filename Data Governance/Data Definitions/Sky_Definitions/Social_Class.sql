/*###############################################################################
# Created on:   17/12/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for defining the social class of a household
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 17/12/2012  TKD   v01 - initial version
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Process									    #####
-- ##############################################################################################################

--example below based on adsmart feed

--stage 1

--note that due to data constraints, in the event of multiple social class values in a household.
--the query below chooses it by head_of_household, social_class asc (so A would come before B where
--the household has both assigned) and if nothing else the greater cb_row_id (id assigned during processing)

select  distinct c.cb_row_id
        ,c.cb_key_individual
        ,c.cb_key_household
        ,c.lukcat_fr_de_nrs AS social_grade
        ,playpen.p_head_of_household
        ,rank() over(PARTITION BY c.cb_key_household ORDER BY playpen.p_head_of_household desc, c.lukcat_fr_de_nrs asc, c.cb_row_id desc) as rank_id
into caci_sc
from sk_prod.CACI_SOCIAL_CLASS as c,
     sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD as playpen,
     sk_prod.experian_consumerview as e
where e.exp_cb_key_individual = playpen.exp_cb_key_individual
  and e.cb_key_individual = c.cb_key_individual
  and c.cb_address_dps is NOT NULL
order by c.cb_key_household;
commit;

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################

