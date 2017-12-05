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

