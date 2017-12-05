/*###############################################################################
# Created on:   19/12/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying if a household has any
#		housewifes within it.
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 19/12/2012  TKD   v01 - initial version
#
###############################################################################*/



-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


Select cv.cb_key_household
    ,cv.cb_key_individual
    ,playpen.p_head_of_household
    ,playpen.p_employment_status_v2
    ,lifestyle.S2_000101_data_PRIM_PERS_JOBS_OCCUPATION_BAND as OCCUPATION_BAND
    ,(case when playpen.p_employment_status_v2 = '2' or lifestyle.S2_000101_data_PRIM_PERS_JOBS_OCCUPATION_BAND = 'M'
           then 1 else 0 end) as Housewife_in_HH
    ,rank() over(PARTITION BY cv.cb_key_household ORDER BY Housewife_in_HH desc, cv.cb_row_id) AS rank_id
into Housewife_in_HH
FROM sk_prod.EXPERIAN_CONSUMERVIEW AS CV
        inner join sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD as playpen
on cv.exp_cb_key_individual = playpen.exp_cb_key_individual
        left join sk_prod.EXPERIAN_LIFESTYLE as lifestyle
on cv.cb_key_individual = lifestyle.cb_key_individual
WHERE cv.cb_change_date= @max_change_date;
commit;

delete from Housewife_in_HH where rank_id > 1;
commit;

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
