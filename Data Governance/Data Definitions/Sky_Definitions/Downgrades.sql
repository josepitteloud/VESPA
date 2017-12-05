 /*###############################################################################
# Created on:   08/01/2013
# Created by:   Tony Kinnaird (TKD)
# Description:  By taking the current and most recent previous package, we can
#		derive if the customer has downgraded on any of their main
#		Sky Packages
#
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 08/01/2013  TKD   v01 - initial version
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Process									    #####
-- ##############################################################################################################


-------------------------------------------------  02 - Downgrades to packages
--drop table downgrades
SELECT    csh.Account_number
         ,ncel.prem_movies + ncel.prem_sports AS current_premiums
         ,ocel.prem_movies + ocel.prem_sports AS old_premiums
         ,ncel.prem_movies                    AS current_movies
         ,ocel.prem_movies                    AS old_movies
         ,                   ncel.prem_sports AS current_sports
         ,                   ocel.prem_sports AS old_sports
         ,rank() over(PARTITION BY base.account_number ORDER BY effective_to_dt desc) AS rank_id
         ,effective_to_dt
         ,effective_from_dt
                    INTO downgrades
    FROM sk_prod.cust_subs_hist AS csh
         inner join sk_prod.cust_entitlement_lookup AS ncel
                    ON csh.current_short_description = ncel.short_description
         inner join sk_prod.cust_entitlement_lookup AS ocel
                    ON csh.previous_short_description = ocel.short_description
         inner join AdSmart AS Base
                    ON csh.account_number = base.account_number
WHERE csh.effective_from_dt >= @date_minus__12  -- Date range
    AND csh.effective_to_dt > csh.effective_from_dt
--    AND csh.effective_to_dt >= @date_minus__12  -- Date range
    AND subscription_sub_type='DTV Primary Viewing'
    AND status_code IN ('AC','PC','AB')   -- Active records
    AND (current_premiums  < old_premiums  -- Decrease in premiums
        OR current_movies < old_movies    -- Decrease in movies
        OR current_sports < old_sports)    -- Decrease in sports
    AND csh.ent_cat_prod_changed = 'Y'    -- The package has changed - VERY IMPORTANT
    AND csh.prev_ent_cat_product_id<>'?'  -- This is not an Aquisition
;
commit;

DELETE FROM downgrades where rank_id >1;
commit;

ALTER table     downgrades ADD   Premiums_downgrades  integer; commit;
ALTER table     downgrades ADD   Movies_downgrades  integer; commit;
ALTER table     downgrades ADD   Sports_downgrades  integer; commit;


-- case statement to work out movie, sports and total downgrades
UPDATE downgrades
SET
 Premiums_downgrades =   CASE WHEN old_premiums > current_premiums THEN 1  ELSE 0  END
,Movies_downgrades  =    CASE WHEN old_movies > current_movies     THEN 1  ELSE 0  END
,Sports_downgrades  =    CASE WHEN old_sports > current_Sports     THEN 1  ELSE 0  END
FROM downgrades;
commit;

--      create index on downgrades
CREATE   HG INDEX idx13 ON downgrades(account_number);
commit;


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


