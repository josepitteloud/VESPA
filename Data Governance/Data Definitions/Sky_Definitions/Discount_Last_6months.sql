/*###############################################################################
# Created on:   21/09/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for those customers who have been offered a
#		discount in the last 6 months.
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
--  Recently or currently on discount  [DISCOUNT_OFFER_LAST_6M]
--  need to fix this need to get the date adjusted
---------------------------------------------------------------------------------
SELECT  base.account_number,
        max(CASE WHEN offer_end_dt between DATEADD(day,-180,today()) and DATEADD(day,+365,today()) --this could be just "offer_end_dt > DATEADD(day,-180,today())"
                THEN '1'
                ELSE '0'        END)
                AS INOFFER
INTO     #offers
FROM     sk_prod.cust_product_offers AS CPO  inner join VIQ_HH_ACCOUNT_TMP AS Base
                    ON CPO.account_number = base.account_number
WHERE    offer_id                NOT IN (SELECT offer_id
                                         FROM citeam.sk2010_offers_to_exclude)
        AND offer_end_dt          > today()
        AND offer_amount          < 0
        AND offer_dim_description   NOT IN ('PPV #1 Administration Charge','PPV EURO1 Administration Charge')
        AND UPPER (offer_dim_description) NOT LIKE '%VIP%'
        AND UPPER (offer_dim_description) NOT LIKE '%STAFF%'
        AND UPPER (offer_dim_description) NOT LIKE 'PRICE PROTECTION%'
GROUP BY base.account_number;

commit;

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


