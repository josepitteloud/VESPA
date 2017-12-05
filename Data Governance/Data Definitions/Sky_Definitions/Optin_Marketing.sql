/*###############################################################################
# Created on:   21/09/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for understanding if a customer is considered
#		as opted-in for Marketing purposes.
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
--  Marketing Opt in - need to check defintion is it all or any  [MARKETING_OPT_IN]
/******  THIS PRODUCES DUPLICATE ACCOUNTS.....  */
---------------------------------------------------------------------------------
SELECT sav.account_number
      ,min(CASE WHEN sav.cust_email_allowed = 'Y'             THEN 1 ELSE 0 END) AS Email_Mkt_OptIn
      ,min(CASE WHEN sav.cust_postal_mail_allowed = 'Y'       THEN 1 ELSE 0 END) AS Mail_Mkt_OptIn
      ,min(CASE WHEN sav.cust_telephone_contact_allowed = 'Y' THEN 1 ELSE 0 END) AS Tel_Mkt_OptIn
      ,min(CASE WHEN sav.cust_sms_allowed = 'Y'               THEN 1 ELSE 0 END) AS Txt_Mkt_OptIn
      ,min(CASE WHEN sav.cust_email_allowed = 'Y'
        AND sav.cust_postal_mail_allowed = 'Y'
        AND sav.cust_sms_allowed = 'Y'
        AND sav.cust_telephone_contact_allowed = 'Y'      THEN 1 ELSE 0 END) AS All_Mkt_OptIn
      ,min(CASE WHEN sav.cust_email_allowed = 'Y'
        OR sav.cust_postal_mail_allowed = 'Y'
        OR sav.cust_sms_allowed = 'Y'
        OR sav.cust_telephone_contact_allowed = 'Y'       THEN 1 ELSE 0 END) AS Any_Mkt_OptIn

INTO #Opt_Ins
FROM sk_prod.cust_single_account_view AS sav
        inner join VIQ_HH_ACCOUNT_TMP as VIQ
           on sav.account_number = VIQ.account_number
WHERE cust_active_dtv = 1
GROUP BY sav.account_number;
commit;


create unique index viq_idx2 on #Opt_Ins (account_number asc);
commit;

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


