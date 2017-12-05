/*###############################################################################
# Created on:   10/01/2013
# Created by:   Tony Kinnaird (TKD)
# Description:  An indicator as to if a household has certain sports or movie
#		packages
#
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 10/01/2013  TKD   v01 - initial version
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Original Query								    #####
-- ##############################################################################################################


Select  csh.account_number
        ,max(cel.sport_1) as sky_sports_1
        ,max(cel.sport_2) as sky_sports_2
        ,Max(cel.movie_1) as movies_1
        ,Max(cel.movie_2) as movies_2
into sports_movies_active
 FROM sk_prod.cust_subs_hist AS csh
           LEFT OUTER JOIN sk_prod.cust_entitlement_lookup cel on csh.current_short_description = cel.short_description
     WHERE csh.status_code in ('AC','AB','PC')
       AND csh.effective_from_dt <= @today
       AND csh.effective_to_dt    > @today
       AND csh.effective_from_dt <> csh.effective_to_dt
       AND account_number is not null
  GROUP BY csh.account_number;
commit;

Update ADsmart
SET  base.sky_sports_1 = sm.sky_sports_1
    ,base.sky_sports_2 = sm.sky_sports_2
    ,base.movies_1 = sm.movies_1
    ,base.movies_2 = sm.movies_2
FROM ADsmart as base INNER JOIN sports_movies_active AS sm
ON base.account_number = sm.account_number;
commit;

drop table sports_movies_active;
commit;



-- ##############################################################################################################
-- ##### STEP 1.0 Ended									                    #####
-- ##############################################################################################################
