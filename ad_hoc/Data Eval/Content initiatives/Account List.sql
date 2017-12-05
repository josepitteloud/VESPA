SELECT sbo.account_number
	, a.subscriber_id
	, COUNT(1) AS Logs
	, sav.prod_active_broadband_package_desc
	, sav.current_package
	, sav.cb_key_household
INTO CONT_INIT_accounts_with_logs
FROM vespa_analysts.panel_data AS a
INNER JOIN vespa_analysts.vespa_single_box_view AS sbo ON a.subscriber_id = sbo.subscriber_id
INNER JOIN sk_prod.cust_single_account_view AS sav ON sbo.account_number = sav.account_number
WHERE a.data_received = 1
	AND a.panel = 12
	AND a.dt BETWEEN '2013-10-01'
		AND '2013-11-30 23:59:59'
GROUP BY sbo.account_number
	, a.subscriber_id
	, sav.prod_active_broadband_package_desc
	, sav.current_package
	, sav.cb_key_household



SELECT ct.account_number
	, sum(CASE 
			WHEN Raw_AvDNumProgs__All_TV < 0
				THEN 0
			ELSE Raw_AvDNumProgs__All_TV
			END) AS Raw_Daily_Num_Progs_Viewed
	, sum(CASE 
			WHEN Raw_AvDNumCompleteProgs__All_TV < 0
				THEN 0
			ELSE Raw_AvDNumCompleteProgs__All_TV
			END) AS Raw_Daily_Num_Complete_Progs_Viewed
	, sum((
			CASE 
				WHEN Raw_AvDVw__All_TV < 0
					THEN 0
				ELSE Raw_AvDVw__All_TV
				END
			) / 60 / 60) AS Raw_Daily_All_TV_Viewing_Duration_Hours
	, sum((
			CASE 
				WHEN Raw_SOV__All_Pay_TV < 0
					THEN 0
				ELSE Raw_SOV__All_Pay_TV
				END
			) * (
			CASE 
				WHEN Raw_AvDVw__All_TV < 0
					THEN 0
				ELSE Raw_AvDVw__All_TV
				END
			) / 60 / 60) AS Raw_Daily_Pay_TV_Viewing_Duration_Hours
	, CASE 
		WHEN Raw_Daily_Pay_TV_Viewing_Duration_Hours >= 1
			THEN 1
		ELSE 0
		END [Daily_Pay_TV_Viewing_Duration_>=_1_Hour]
INTO CONT_INIT_raw
FROM VESPA_SHARED.Aggr_Summary_Raw_All a
INNER JOIN CONT_INIT_accounts_with_logs ct ON ct.account_number = a.account_number
WHERE period_key = 11
GROUP BY ct.account_number;



SELECT 
a.*
, Raw_Daily_Num_Progs_Viewed	
, Raw_Daily_Num_Complete_Progs_Viewed	
, Raw_Daily_All_TV_Viewing_Duration_Hours	
, Raw_Daily_Pay_TV_Viewing_Duration_Hours	
, [Daily_Pay_TV_Viewing_Duration_>=_1_Hour] as Less_1_hour_Flag
INTO CONT_INIT_LIST
FROM CONT_INIT_accounts_with_logs as a
JOIN CONT_INIT_raw as b ON a.account_number = b.account_number

