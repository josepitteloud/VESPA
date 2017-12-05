/*
	DECLARE @start_dt 	DATE 
	DECLARE @end_dt 	DATE 
	SET @start_dt 	= '2014-02-15'
	SET @end_dt 	= '2017-03-16' 
*/
/* *******************************************************************
                                                FULL_ACTIVE_OFFER_BASE
********************************************************************* */


CREATE OR REPLACE VIEW MCKINSEY_FULL_ACTIVE_OFFER_BASE AS 
 
SELECT
      a.account_number,
       a.subscription_id,
       a.currency_code,
       a.subscription_sub_type,
       a.effective_from_dt,
       a.effective_to_dt,
       a.status_code_changed,
       a.status_code,
       a.prev_status_code,
       a.ent_cat_prod_changed,
       a.current_product_description,
       b.prem_movies,
       b.prem_sports
from cust_subs_hist a
left join cust_entitlement_lookup b on a.current_short_description = b.short_description
WHERE effective_to_dt >= '2014-02-15'
group by
        a.account_number,
        a.subscription_id,
        a.currency_code,
        a.subscription_sub_type,
        a.effective_from_dt,
        a.effective_to_dt,
        a.status_code_changed,
        a.prev_status_code,
        a.status_code,
        a.ent_cat_prod_changed,
        a.current_product_description,
        b.prem_movies,
        b.prem_sports

COMMIT 
GRANT SELECT ON MCKINSEY_FULL_ACTIVE_OFFER_BASE TO vespa_group_low_security, rko04, citeam		

GO 

