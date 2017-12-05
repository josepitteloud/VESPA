

/* *******************************************************************
                        MCKINSEY_HARDWARE_OFFERS
********************************************************************* */



CREATE OR REPLACE VIEW MCKINSEY_HARDWARE_OFFERS AS

SELECT account_number
		,created_date
		,discount_amount
		,discount_type
		,first_order_id
		,glob_auto
		,is1_description
		,is2_description
		,offer_description
		,offer_id
		,pac
		,product_description
		,product_price
		,product_type
		,service_instance_id
		,src_system_id
		,standard_pricing_flag
		,working_location
FROM OFFERS_DETAILS
WHERE created_date BETWEEN  '2014-02-15' AND '2017-03-16' 

COMMIT
GRANT SELECT ON MCKINSEY_HARDWARE_OFFERS TO vespa_group_low_security, rko04, citeam

GO


