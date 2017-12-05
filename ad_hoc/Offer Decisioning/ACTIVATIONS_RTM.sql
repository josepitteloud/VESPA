

/*
	DECLARE @start_dt 	DATE 
	DECLARE @end_dt 	DATE 
	SET @start_dt 	= '2014-02-15'
	SET @end_dt 	= '2017-03-16' 
*/
/* *******************************************************************
                                                
********************************************************************* */

SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;
 
WITH Activations
AS (
	SELECT acq.account_number
		, acq.Event_dt
		, acq.Acquisition_Type
		, ord.order_dt
		, ord.order_number
		, RTM_LEVEL_1
		, RTM_LEVEL_2
		, RTM_LEVEL_3
		, row_number() OVER (PARTITION BY acq.account_number, acq.event_dt ORDER BY ord.order_dt DESC, order_number DESC) Order_Rnk
	FROM citeam.DTV_ACQUISITION_MART acq
	LEFT JOIN citeam.dm_orders ord ON ord.account_number = acq.account_number AND ord.order_dt BETWEEN acq.event_dt - 90 
											AND acq.event_dt AND ord.order_status NOT IN ('APPCAN', 'CANCLD') 
											AND Family_added + Variety_added + Original_added + SkyQ_Added > 0 
											AND Family_removed + Variety_removed + Original_removed + SkyQ_removed = 0
	WHERE acq.event_dt BETWEEN '2014-02-15' AND '2017-03-16'
			AND acq.subscription_sub_type = 'DTV Primary Viewing'
		--     and acq.status_code = 'AC'
		--order by acq.account_number,acq.Event_dt,acq.Acquisition_Type,ord.order_dt desc,order_number desc;--3740981 Row(s) affected
	)

SELECT account_number
	, Event_dt
	, Acquisition_Type
	, RTM_LEVEL_1
	, RTM_LEVEL_2
	, RTM_LEVEL_3
INTO MCKINSEY_ACTIVATIONS_RTM
FROM Activations 
WHERE Order_Rnk = 1
 
 
 COMMIT 
GRANT SELECT ON MCKINSEY_ACTIVATIONS_RTM TO vespa_group_low_security, rko04, citeam

GO

