

/* *******************************************************************
                        MCKINSEY_SKYSTORE_RENTALS

********************************************************************* */


CREATE OR REPLACE VIEW MCKINSEY_SKYSTORE_RENTALS AS 

SELECT a.account_number
        , fin_currency_code
        , DATE (ppv_ordered_dt)				AS ppv_ordered_dt
        , COUNT(a.account_number)       AS volume
        , SUM(charge_amount_incl_tax)   AS revenue
FROM cust_product_charges_ppv AS a
JOIN cust_single_account_view AS b ON a.account_number = b.account_number
WHERE ppv_ordered_dt        BETWEEN  '2014-02-15' AND '2017-03-16' 
GROUP BY a.account_number
        , fin_currency_code
        , ppv_ordered_dt        


COMMIT 
GRANT SELECT ON MCKINSEY_SKYSTORE_RENTALS TO vespa_group_low_security, rko04, citeam		

GO

