/*###############################################################################
# Created on:   14/01/2013
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for at what stage in the cycle
#		a household is with Sky
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 14/01/2013  TKD   v01 - initial version
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Process									    #####
-- ##############################################################################################################

------------------------------------------------------------------------------------
-- sky customer lifestage - based on customer tenure
------------------------------------------------------------------------------------
--code_location_23
--drop table life;
--commit;

select distinct a.account_number
        ,case when datediff(day,acct_first_account_activation_dt,@today) <=   91 then 'A) Welcome'
              when datediff(day,acct_first_account_activation_dt,@today) <=  300 then 'B) Mid'
              when datediff(day,acct_first_account_activation_dt,@today) <=  420 then 'C) End'
              when datediff(day,acct_first_account_activation_dt,@today) >   420 then 'D) 15+'
              else                                                                    'E) missing'
              end as Sky_cust_life
        ,rank() over(PARTITION BY a.account_number ORDER BY acct_first_account_activation_dt desc) AS rank_id
         INTO life
    from ADSMART AS A LEFT JOIN sk_prod.cust_single_account_view as SAV
                 ON A.account_number = SAV.Account_number
    where cust_active_dtv = 1
    group by a.account_number, Sky_cust_life,acct_first_account_activation_dt;
commit;

DELETE FROM  life where rank_id >1;
commit;

--update file with Sky_cust_life data
UPDATE AdSmart
SET    Sky_cust_life             = SCL.Sky_cust_life
      FROM AdSmart  AS Base
         INNER JOIN life AS SCL
         ON base.account_number = SCL.account_number;
commit;

drop table life;
commit;




-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


