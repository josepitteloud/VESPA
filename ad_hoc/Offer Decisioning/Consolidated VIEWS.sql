/*
	DECLARE @start_dt 	DATE 
	DECLARE @end_dt 	DATE 
	SET @start_dt 	= '2014-02-15'
	SET @end_dt 	= '2017-03-16' 
*/
/* *******************************************************************
                                                FULL_BOX_HIST
********************************************************************* */
CREATE OR REPLACE VIEW MCKINSEY_FULL_BOX_HIST AS 
SELECT account_number
	, created_dt
	, x_model_number
	, x_manufacturer
	, x_box_type
	, box_installed_dt 
	, box_replaced_dt
FROM cust_set_top_box AS a
WHERE box_replaced_dt >= '2014-02-15'
GROUP BY account_number
	, created_dt
	, x_model_number
	, x_manufacturer
	, x_box_type
	, box_installed_dt 
	, box_replaced_dt
	

COMMIT 
GRANT SELECT ON MCKINSEY_FULL_BOX_HIST TO vespa_group_low_security, rko04, citeam		

GO 


/*
	DECLARE @start_dt 	DATE 
	DECLARE @end_dt 	DATE 
	SET @start_dt 	= '2014-02-15'
	SET @end_dt 	= '2017-03-16' 
*/
/* *******************************************************************
					FULL_CALLS_HIST 
********************************************************************* */
CREATE OR REPLACE VIEW MCKINSEY_FULL_CALLS_HIST AS 

SELECT a.account_number
	, cast(created_date AS DATE) AS Event_Date
	, count(*) AS Calls
	, 0 AS saves
	, 'IC' AS TypeOfEvent
--    into MCKINSEY_FULL_CALLS_HIST_NEW_1
FROM cust_contact AS a
WHERE cast(created_date AS DATE) BETWEEN  '2014-02-15' AND '2017-03-16' 
	AND contact_channel = 'I PHONE COMMUNICATION' 
	AND contact_grouping_identity IS NOT NULL
GROUP BY a.account_number
		, event_date
UNION
SELECT a.account_number
	, event_dt AS Event_Date
	, count(*) AS Calls
	, sum(saves) AS saves
	, 'TA' AS TypeOfEvent
FROM view_cust_calls_hist a
WHERE typeofevent = 'TA' 
	AND event_dt BETWEEN  '2014-02-15' AND '2017-03-16' 
GROUP BY a.account_number
	, event_date
UNION
SELECT a.account_number
	, event_dt AS Event_Date
	, count(*) AS Calls
	, sum(saves) AS saves
	, 'PAT' AS TypeOfEvent
FROM view_cust_calls_hist a
WHERE typeofevent = 'PAT' 
	AND event_dt  BETWEEN  '2014-02-15' AND '2017-03-16' 
GROUP BY a.account_number
	, event_date
	
	
COMMIT 
GRANT SELECT ON MCKINSEY_FULL_CALLS_HIST TO vespa_group_low_security, rko04, citeam

GO



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


/*
	DECLARE @start_dt 	DATE 
	DECLARE @end_dt 	DATE 
	SET @start_dt 	= '2014-02-15'
	SET @end_dt 	= '2017-03-16' 
*/

/* *******************************************************************
                        FULL_SIMPLE_SEGMENT_HIST
********************************************************************* */
CREATE OR REPLACE VIEW MCKINSEY_FULL_SIMPLE_SEGMENT_HIST AS 
SELECT account_number
	, segment
	, segment_lev2
	, observation_date
FROM simple_segments_history
WHERE observation_date >= DATEADD(MONTH, - 3, '2014-02-15')
GROUP BY account_number
	, segment
	, segment_lev2
	, observation_date
	
--181555154 Row(s) affected
COMMIT 
GRANT SELECT ON MCKINSEY_FULL_SIMPLE_SEGMENT_HIST TO vespa_group_low_security, rko04, citeam

GO 

/*
	DECLARE @start_dt 	DATE 
	DECLARE @end_dt 	DATE 
	SET @start_dt 	= '2014-02-15'
	SET @end_dt 	= '2017-03-16' 
*/
/* *******************************************************************
                                                FULL_OFFER_HIST
********************************************************************* */
 
CREATE OR REPLACE VIEW  MCKINSEY_FULL_OFFER_HIST AS 
select    account_number
        , subscription_id
        , subs_type
--      , currency_code
        , offer_id
        , offer_dim_description
        , Offer_Value
        , offer_duration
        , Total_Offer_Value_Yearly
        , Offer_Start_Dt_Actual
        , Offer_End_Dt_Actual
        , Intended_Offer_Start_Dt
        , Intended_Offer_End_Dt
        , Intended_Total_Offer_Value_Yearly
        , Status_Change_Date
        , Initial_Effective_Dt
        , created_dt
        , created_by_id
        , Intended_offer_amount
        , offer_status
        , src_system_id
        , order_id
        , ORIG_PORTFOLIO_OFFER_ID
   --   , Offer_Period
        , subs_first_act_dt
        , subs_latest_act_dt
        , sports_act_dt
        , movies_act_dt
        , dtv_first_act_dt
        , dtv_latest_act_dt
        , dtv_act_dt
        , bb_first_act_dt
        , bb_latest_act_dt
        , bb_act_dt
        , status_change
        , package_movement
        , ta, pat
        , coe
        , any_call
        , Offer_Segment
        , Offer_Segment_Detail
        , Offer_Segment_Grouped
        , offer_segment_grouped_1
--into noryd.MCKINSEY_FULL_OFFER_HIST
from CITEAM.offer_usage_all
WHERE  created_dt BETWEEN  '2014-02-15' AND '2017-03-16'  
	OR Intended_Offer_End_Dt BETWEEN  '2014-02-15' AND '2017-03-16' 
	
  
 ----106164661 Row(s) affected
 GO
 
COMMIT 
GRANT SELECT ON MCKINSEY_FULL_OFFER_HIST TO vespa_group_low_security, rko04, citeam

GO

/*
	DECLARE @start_dt 	DATE 
	DECLARE @end_dt 	DATE 
	SET @start_dt 	= '2014-02-15'
	SET @end_dt 	= '2017-03-16' 
*/

/* *******************************************************************
                                                FULL_CALL_DETAIL 
********************************************************************* */
 
CREATE OR REPLACE VIEW MCKINSEY_FULL_CALL_DETAIL AS 
SELECT
   account_number
  ,call_date
  ,initial_sct_grouping
  ,initial_working_location
  ,final_working_location
  ,final_sct_grouping
  ,start_date_time
  ,total_transfers
FROM calls_details
WHERE call_date BETWEEN  '2014-02-15' AND '2017-03-16' 


COMMIT 
GRANT SELECT ON MCKINSEY_FULL_CALL_DETAIL TO vespa_group_low_security, rko04, citeam		

GO

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

/*
	DECLARE @start_dt 	DATE 
	DECLARE @end_dt 	DATE 
	SET @start_dt 	= '2014-02-15'
	SET @end_dt 	= '2017-03-16' 
*/
/* *******************************************************************
                                                CONTRACT_HIST
********************************************************************* */
CREATE OR REPLACE VIEW MCKINSEY_FULL_CONTRACT_HIST AS 
SELECT DISTINCT 
	  created_dt
	, DW_created_dt
	, dw_last_modified_dt
	, Start_dt
	, end_dt
	, end_dt_calc
	, last_modified_dt
	, min_term_months
	, subscription_id
	, subscription_type
	, account_number
	, agreement_item_type_code
	, created_by_id
	
FROM cust_contract_agreements
WHERE created_dt BETWEEN '2014-02-15' AND '2017-03-16'

COMMIT 
GRANT SELECT ON MCKINSEY_FULL_CONTRACT_HIST TO vespa_group_low_security, rko04, citeam

GO

