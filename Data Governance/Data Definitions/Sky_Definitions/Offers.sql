 /*###############################################################################
# Created on:   08/01/2013
# Created by:   Tony Kinnaird (TKD)
# Description:  An indicator as to whether the account currently has any offers on
#		it
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


SELECT  base.account_number
         ,Max(CASE WHEN offer_end_dt          > @today THEN '1'
                                ELSE '0'        END) AS Current_offer
         ,Max(CASE WHEN offer_end_dt          < DATEADD(day,+30,@today) THEN '1'
                                ELSE '0'        END) AS offer_expires30
         ,count(*) as InOffer
INTO     offers
FROM     sk_prod.cust_product_offers AS CPO  inner join AdSmart AS Base
                    ON CPO.account_number = base.account_number
WHERE    offer_id                NOT IN (SELECT offer_id
                                         FROM citeam.sk2010_offers_to_exclude)
        AND offer_end_dt          > @today
        AND offer_amount          < 0
        AND offer_dim_description   NOT IN ('PPV 1 Administration Charge','PPV EURO1 Administration Charge')
        AND UPPER (offer_dim_description) NOT LIKE '%VIP%'
        AND UPPER (offer_dim_description) NOT LIKE '%STAFF%'
        AND UPPER (offer_dim_description) NOT LIKE 'PRICE PROTECTION%'
GROUP BY base.account_number;
commit;


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


