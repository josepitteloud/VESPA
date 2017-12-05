/*###############################################################################
# Created on:   21/09/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for those customers who have reduced their
#		movies or sports package over the last 12 months.
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 21/09/2012  TKD   v01 - initial version
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Process									    #####
-- ##############################################################################################################

---------------------------------------------------------------------------------
--  Movies and Sports Downgrades  [PREV_MOVIES_DOWNGRADE],[PREV_SPORTS_DOWNGRADE]
---------------------------------------------------------------------------------

SELECT    csh.Account_number
         ,ncel.prem_movies + ncel.prem_sports AS current_premiums
         ,ocel.prem_movies + ocel.prem_sports AS old_premiums
         ,ncel.prem_movies                    AS current_movies
         ,ocel.prem_movies                    AS old_movies
         ,                   ncel.prem_sports AS current_sports
         ,                   ocel.prem_sports AS old_sports
         ,rank() over(PARTITION BY base.account_number ORDER BY effective_to_dt desc) AS rank_id
         ,effective_to_dt
         ,effective_from_dt,
         0 Movies_downgrades,
         0 Sports_downgrades
                    INTO #downgrades
    FROM sk_prod.cust_subs_hist AS csh
         inner join sk_prod.cust_entitlement_lookup AS ncel
                    ON csh.current_short_description = ncel.short_description
         inner join sk_prod.cust_entitlement_lookup AS ocel
                    ON csh.previous_short_description = ocel.short_description
         inner join VIQ_HH_ACCOUNT_TMP AS Base
                    ON csh.account_number = base.account_number
WHERE csh.effective_from_dt >= DATEADD(day,-365,today())  -- Date range
    AND csh.effective_to_dt > csh.effective_from_dt
    AND subscription_sub_type='DTV Primary Viewing'
    AND status_code IN ('AC','PC','AB')   -- Active records
    AND (current_premiums  < old_premiums  -- Decrease in premiums
        OR current_movies < old_movies    -- Decrease in movies
        OR current_sports < old_sports)    -- Decrease in sports
    AND csh.ent_cat_prod_changed = 'Y'    -- The package has changed - VERY IMPORTANT
    AND csh.prev_ent_cat_product_id<>'?'  -- This is not an Aquisition
;

commit;

DELETE FROM #downgrades where rank_id >1;
commit;

-- case statement to work out movie, sports and total downgrades
UPDATE #downgrades
SET
 Movies_downgrades  =    CASE WHEN old_movies > current_movies     THEN 1
                              ELSE 0                             END
,Sports_downgrades  =    CASE WHEN old_sports > current_Sports     THEN 1
                              ELSE 0                             END
FROM #downgrades;
commit;


--      create index on #downgrades
CREATE   HG INDEX idx13 ON #downgrades(account_number);
commit;

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


