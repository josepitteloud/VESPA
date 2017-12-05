


/* ==========================================================================================
VARIABLES TO INCLUDE
		
		- DTV_TA_calls_1m
		- RTM
		- Talk_tenure
		- my_sky_login_3m
		- BB_all_calls_1m
		- BB_offer_rem_and_end
		- home_owner_status
		- BB_tenure
		- talk_type
		- simple_segment


 ====================================================================================================================================================================================== */
 
--====================================================================================================================================================================================
---=======================		SABB Churn Table creation
--====================================================================================================================================================================================
 		
CREATE TABLE BB_SABB_Churn_segments_lookup
	(	 
		 my_sky_login_3m			INT  	DEFAULT 0
		,BB_all_calls_1m			INT 	DEFAULT 0
		,BB_offer_rem_and_end		INT		DEFAULT -9999
		,home_owner_status			VARCHAR(20) DEFAULT 'UNKNOWN'
		,BB_tenure					INT 	DEFAULT 0
		,talk_type					VARCHAR (30)	DEFAULT 'NONE'
		,node 						TINYINT			DEFAULT 0
		,segment					VARCHAR(20)		DEFAULT NULL
		,observation_date			TIMESTAMP		DEFAULT getdate()
		)

COMMIT 		
		
CREATE LF INDEX id1 ON 	BB_SABB_Churn_segments_lookup(my_sky_login_3m)
CREATE LF INDEX id2 ON 	BB_SABB_Churn_segments_lookup(BB_all_calls_1m)
CREATE LF INDEX id3 ON 	BB_SABB_Churn_segments_lookup(BB_offer_rem_and_end)
CREATE LF INDEX id4 ON 	BB_SABB_Churn_segments_lookup(home_owner_status)
CREATE LF INDEX id5 ON 	BB_SABB_Churn_segments_lookup(BB_tenure)
CREATE LF INDEX id6 ON 	BB_SABB_Churn_segments_lookup(talk_type)
CREATE LF INDEX id7 ON 	BB_SABB_Churn_segments_lookup(segment)
COMMIT 
		
SELECT DISTINCT my_sky_login_3m			INTO #t1 FROM BB_forecast_segmenting_variables 	/* REPLACE by cust_fcast_weekly_base */
SELECT DISTINCT BB_all_calls_1m			INTO #t2 FROM BB_forecast_segmenting_variables 	/* REPLACE by cust_fcast_weekly_base */
SELECT DISTINCT BB_offer_rem_and_end	INTO #t3 FROM BB_forecast_segmenting_variables 	/* REPLACE by cust_fcast_weekly_base */
SELECT DISTINCT home_owner_status		INTO #t4 FROM BB_forecast_segmenting_variables 	/* REPLACE by cust_fcast_weekly_base */
SELECT DISTINCT BB_tenure 				INTO #t5 FROM BB_forecast_segmenting_variables 	/* REPLACE by cust_fcast_weekly_base */
SELECT DISTINCT talk_type 				INTO #t6 FROM BB_forecast_segmenting_variables 	/* REPLACE by cust_fcast_weekly_base */

INSERT INTO		BB_SABB_Churn_segments_lookup
					(
					 my_sky_login_3m
					,BB_all_calls_1m
					,BB_offer_rem_and_end
					,home_owner_status
					,BB_tenure
					,talk_type
					)
SELECT  my_sky_login_3m
					,BB_all_calls_1m
					,BB_offer_rem_and_end
					,home_owner_status
					,BB_tenure
					,talk_type
FROM #t1 
CROSS JOIN #t2
CROSS JOIN #t3
CROSS JOIN #t4
CROSS JOIN #t5
CROSS JOIN #t6

COMMIT 

DROP TABLE #t1
DROP TABLE #t2
DROP TABLE #t3
DROP TABLE #t4
DROP TABLE #t5
DROP TABLE #t6

GO

--====================================================================================================================================================================================
---=======================		Product Churn Table creation
--====================================================================================================================================================================================
CREATE  TABLE BB_TP_Product_Churn_segments_lookup
	(	 DTV_TA_calls_1m			INT 			DEFAULT 0
		,RTM						VARCHAR (20)	DEFAULT 'UNKNOWN'
		,Talk_tenure				INT 			DEFAULT 0
		,simple_segment 			VARCHAR (30)	DEFAULT 'UNKNOWN'
		,my_sky_login_3m			INT 			DEFAULT 0
		,BB_all_calls_1m			INT				DEFAULT 0
		,node 						TINYINT			DEFAULT 0
		,segment					VARCHAR(20)		DEFAULT NULL
		,observation_date			TIMESTAMP		DEFAULT GETDATE()
		)
COMMIT 		
		
CREATE LF INDEX id1 ON 	BB_TP_Product_Churn_segments_lookup(DTV_TA_calls_1m)
CREATE LF INDEX id2 ON 	BB_TP_Product_Churn_segments_lookup(RTM)
CREATE LF INDEX id3 ON 	BB_TP_Product_Churn_segments_lookup(Talk_tenure)
CREATE LF INDEX id4 ON 	BB_TP_Product_Churn_segments_lookup(simple_segment)
CREATE LF INDEX id5 ON 	BB_TP_Product_Churn_segments_lookup(my_sky_login_3m)
CREATE LF INDEX id6 ON 	BB_TP_Product_Churn_segments_lookup(BB_all_calls_1m)
CREATE LF INDEX id7 ON 	BB_TP_Product_Churn_segments_lookup(segment)
COMMIT 


SELECT DISTINCT DTV_TA_calls_1m			INTO #t1 FROM BB_forecast_segmenting_variables 	/* REPLACE by cust_fcast_weekly_base */
SELECT DISTINCT RTM 					INTO #t2 FROM BB_forecast_segmenting_variables 	/* REPLACE by cust_fcast_weekly_base */
SELECT DISTINCT Talk_tenure				INTO #t3 FROM BB_forecast_segmenting_variables 	/* REPLACE by cust_fcast_weekly_base */
SELECT DISTINCT simple_segment			INTO #t4 FROM cust_fcast_weekly_base 
SELECT DISTINCT my_sky_login_3m			INTO #t5 FROM BB_forecast_segmenting_variables 	/* REPLACE by cust_fcast_weekly_base */
SELECT DISTINCT BB_all_calls_1m			INTO #t6 FROM BB_forecast_segmenting_variables 	/* REPLACE by cust_fcast_weekly_base */

INSERT INTO		BB_TP_Product_Churn_segments_lookup
					(
					 DTV_TA_calls_1m
					,RTM
					,Talk_tenure
					,simple_segment
					,my_sky_login_3m
					,BB_all_calls_1m
					)
SELECT  DTV_TA_calls_1m
		,RTM
		,Talk_tenure
		,COALESCE(simple_segment,'UNKNOWN' )
		,my_sky_login_3m
		,BB_all_calls_1m
FROM #t1 
CROSS JOIN #t2
CROSS JOIN #t3
CROSS JOIN #t4
CROSS JOIN #t5
CROSS JOIN #t6

COMMIT 

DROP TABLE #t1
DROP TABLE #t2
DROP TABLE #t3
DROP TABLE #t4
DROP TABLE #t5
DROP TABLE #t6

GO
--====================================================================================================================================================================================
---=======================		Segments population - SABB
--====================================================================================================================================================================================



UPDATE BB_SABB_Churn_segments_lookup
SET node			 = CASE WHEN BB_offer_rem_and_end IN (-9999,-3) AND BB_tenure  = 1                                                                                                       	THEN 9
							WHEN BB_offer_rem_and_end IN (-9999,-3) AND BB_tenure  IN (2,-1,0, 3)   				                                                                            THEN 10
							WHEN BB_offer_rem_and_end IN (-9999,-3) AND BB_tenure  IN (4,5) 				                                                                                	THEN 11	-- = 499	
							WHEN BB_offer_rem_and_end IN (-9999,-3) AND BB_tenure  = 6		                                                                                         			THEN 12	--<= 641
							WHEN BB_offer_rem_and_end IN (-9999,-3) AND BB_tenure  = 7 AND my_sky_login_3m = 0 AND home_owner_status IN ('Council Rent','Private Rent','UNKNOWN')            	THEN 60	/* <= 1593 */ 
							WHEN BB_offer_rem_and_end IN (-9999,-3) AND BB_tenure  = 7 AND my_sky_login_3m = 0 AND home_owner_status IN ('Owner')                                            	THEN 61 /* <= 1593 */ 
							WHEN BB_offer_rem_and_end IN (-9999,-3) AND BB_tenure  = 7 AND my_sky_login_3m > 0 AND home_owner_status IN ('Council Rent','Owner')                             	THEN 62 /* <= 1593 */ 
							WHEN BB_offer_rem_and_end IN (-9999,-3) AND BB_tenure  = 7 AND my_sky_login_3m > 0 AND home_owner_status IN ('Private Rent','UNKNOWN')                           	THEN 63 /* <= 1593 */ 
							WHEN BB_offer_rem_and_end IN (-9999,-3) AND BB_tenure  = 8 AND my_sky_login_3m = 0 AND talk_type NOT IN ('Sky Pay As You Talk','Sky Talk Evenings and Weekends') 	THEN 64 /* > 1593 */ 
							WHEN BB_offer_rem_and_end IN (-9999,-3) AND BB_tenure  = 8 AND my_sky_login_3m = 0 AND talk_type IN ('Sky Pay As You Talk','Sky Talk Evenings and Weekends')     	THEN 65 /* > 1593 */ 
							WHEN BB_offer_rem_and_end IN (-9999,-3) AND BB_tenure  = 8 AND my_sky_login_3m > 0                                                                               	THEN 35 /* > 1593 */ 
							WHEN BB_offer_rem_and_end = -2  AND my_sky_login_3m = 0 AND talk_type 	  IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited')         			THEN 36 
							WHEN BB_offer_rem_and_end = -2  AND my_sky_login_3m = 0 AND talk_type NOT IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited') AND home_owner_status IN ('Council Rent','Private Rent','UNKNOWN')   THEN 66
							WHEN BB_offer_rem_and_end = -2  AND my_sky_login_3m = 0 AND talk_type NOT IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited') AND home_owner_status IN ('Owner')                                   THEN 67
							WHEN BB_offer_rem_and_end = -2  AND my_sky_login_3m > 0 AND talk_type NOT IN ('Sky Talk Anytime','Sky Talk Freetime','Sky Talk Anytime Extra') AND home_owner_status IN ('Council Rent','Owner')                          THEN 68
							WHEN BB_offer_rem_and_end = -2  AND my_sky_login_3m = 1 AND talk_type NOT IN ('Sky Talk Anytime','Sky Talk Freetime','Sky Talk Anytime Extra') AND home_owner_status IN ('Private Rent','UNKNOWN')                        THEN 82
							WHEN BB_offer_rem_and_end = -2  AND my_sky_login_3m > 1 AND talk_type NOT IN ('Sky Talk Anytime','Sky Talk Freetime','Sky Talk Anytime Extra') AND home_owner_status IN ('Private Rent','UNKNOWN')                        THEN 83
							WHEN BB_offer_rem_and_end = -2  AND my_sky_login_3m > 0 AND talk_type IN ('Sky Talk Anytime','Sky Talk Freetime','Sky Talk Anytime Extra')                        	THEN 39
							WHEN BB_offer_rem_and_end = -1  AND my_sky_login_3m = 0 AND talk_type NOT IN ('Sky Talk International Extra','Sky Talk Anytime Extra')                            	THEN 40
							WHEN BB_offer_rem_and_end = -1  AND my_sky_login_3m = 0 AND talk_type IN ('Sky Talk International Extra','Sky Talk Anytime Extra')                                	THEN 41
							WHEN BB_offer_rem_and_end = -1  AND my_sky_login_3m > 0 AND home_owner_status IN ('Council Rent','Owner','UNKNOWN')                                               	THEN 42
							WHEN BB_offer_rem_and_end = -1  AND my_sky_login_3m > 0 AND home_owner_status IN ('Private Rent')                                                                 	THEN 43
							WHEN BB_offer_rem_and_end = 0   AND my_sky_login_3m = 0 AND talk_type IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited')     AND home_owner_status IN ('Council Rent','Private Rent','UNKNOWN')   THEN 70
							WHEN BB_offer_rem_and_end = 0   AND my_sky_login_3m = 0 AND talk_type IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited')     AND home_owner_status IN ('Owner')                                   THEN 71
							WHEN BB_offer_rem_and_end = 0   AND my_sky_login_3m = 0 AND talk_type NOT IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited') AND home_owner_status IN ('Council Rent','Owner')                    THEN 72
							WHEN BB_offer_rem_and_end = 0   AND my_sky_login_3m = 0 AND talk_type NOT IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited') AND home_owner_status IN ('Private Rent','UNKNOWN')                  THEN 73
							WHEN BB_offer_rem_and_end = 0   AND my_sky_login_3m > 0 AND talk_type IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited')                       				THEN 46
							WHEN BB_offer_rem_and_end = 0   AND my_sky_login_3m > 0 AND talk_type NOT IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited')                   				THEN 47
							WHEN BB_offer_rem_and_end = 1   AND talk_type NOT IN ('Sky Pay As You Talk','Sky Talk Evenings and Weekends')                                                          				THEN 21
							WHEN BB_offer_rem_and_end = 1   AND talk_type IN ('Sky Pay As You Talk')                                                                                               				THEN 22
							WHEN BB_offer_rem_and_end = 1	AND talk_type IN ('Sky Talk Evenings and Weekends') AND my_sky_login_3m = 0 AND home_owner_status IN ('Council Rent','Owner')          				THEN 74
							WHEN BB_offer_rem_and_end = 1	AND talk_type IN ('Sky Talk Evenings and Weekends') AND my_sky_login_3m = 0 AND home_owner_status IN ('Private Rent','UNKNOWN')        				THEN 75
							WHEN BB_offer_rem_and_end = 1	AND talk_type IN ('Sky Talk Evenings and Weekends') AND my_sky_login_3m > 0                                                            				THEN 49
							WHEN BB_offer_rem_and_end = 2	AND home_owner_status IN ('Council Rent')                                                                                              				THEN 24
							WHEN BB_offer_rem_and_end = 2	AND home_owner_status IN ('Owner') AND talk_type NOT IN ('Sky Pay As You Talk','Sky Talk Evenings and Weekends')                       				THEN 50
							WHEN BB_offer_rem_and_end = 2	AND home_owner_status IN ('Owner') AND talk_type IN ('Sky Pay As You Talk','Sky Talk Evenings and Weekends')                           				THEN 51
							WHEN BB_offer_rem_and_end = 2	AND home_owner_status IN ('Private Rent','UNKNOWN') AND my_sky_login_3m = 0 AND BB_tenure IN (0,-1,1,2)                                				THEN 76
							WHEN BB_offer_rem_and_end = 2	AND home_owner_status IN ('Private Rent','UNKNOWN') AND my_sky_login_3m = 0 AND BB_tenure IN (3,4,5,6,7,8)                             				THEN 77
							WHEN BB_offer_rem_and_end = 2	AND home_owner_status IN ('Private Rent','UNKNOWN') AND my_sky_login_3m > 0                                                            				THEN 53
							WHEN BB_offer_rem_and_end = 3	AND home_owner_status IN ('Council Rent')                                                                                              				THEN 27
							WHEN BB_offer_rem_and_end = 3	AND home_owner_status IN ('Owner') AND talk_type NOT IN ('Sky Pay As You Talk','Sky Talk Evenings and Weekends')                       				THEN 54
							WHEN BB_offer_rem_and_end = 3	AND home_owner_status IN ('Owner') AND talk_type IN ('Sky Pay As You Talk','Sky Talk Evenings and Weekends')                           				THEN 55
							WHEN BB_offer_rem_and_end = 3	AND home_owner_status IN ('Private Rent','UNKNOWN') AND my_sky_login_3m = 0				                                                            THEN 56
							WHEN BB_offer_rem_and_end = 3	AND home_owner_status IN ('Private Rent','UNKNOWN') AND my_sky_login_3m > 0                                                            				THEN 57
							WHEN BB_offer_rem_and_end = 4	AND BB_all_calls_1m = 0 AND home_owner_status IN ('Council Rent','Private Rent','UNKNOWN') AND BB_tenure IN (0,-1,1,2,3,4) AND my_sky_login_3m = 0 	THEN 84
							WHEN BB_offer_rem_and_end = 4	AND BB_all_calls_1m = 0 AND home_owner_status IN ('Council Rent','Private Rent','UNKNOWN') AND BB_tenure IN (0,-1,1,2,3,4)	AND my_sky_login_3m > 0 THEN 85
							WHEN BB_offer_rem_and_end = 4	AND BB_all_calls_1m = 0 AND home_owner_status IN ('Council Rent','Private Rent','UNKNOWN') AND BB_tenure IN (5,6,7,8)                               THEN 79
							WHEN BB_offer_rem_and_end = 4	AND BB_all_calls_1m = 0 AND home_owner_status IN ('Owner') AND talk_type IN ('Sky Pay As You Talk','NONE','Sky Talk Anytime Extra')                 THEN 80
							WHEN BB_offer_rem_and_end = 4	AND BB_all_calls_1m = 0 AND home_owner_status IN ('Owner') AND talk_type NOT IN ('Sky Pay As You Talk','NONE','Sky Talk Anytime Extra')             THEN 81
							WHEN BB_offer_rem_and_end = 4	AND BB_all_calls_1m > 0                                                                                                                             THEN 31  
							ELSE 0
                         END
		

UPDATE BB_SABB_Churn_segments_lookup
SET segment = CASE 	WHEN node IN ( 22,  46,  49,  70,  75,  71,  83,  53,  43,  82,  73,  57,  63,  47,  68,  42,  62,  12,  39,  11,  35) THEN 'High Risk'
					WHEN node IN ( 21,  74,  72,  40,  36,  66,  60,  65,  77,  31,  84,  56,  76,  10,  41,  67) THEN 'Medium Risk'
					WHEN node IN ( 61,  51,  64,  24,  50,  27,  55,  85,  81,  79,  80,  54,  9) THEN 'Low Risk'
					ELSE 'No segment'
				END
GO 

--====================================================================================================================================================================================
---=======================		Segments population - Product churn
--====================================================================================================================================================================================

UPDATE BB_TP_Product_Churn_segments_lookup
SET node = CASE WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m = 0 AND Simple_Segment IN ('1 Secure','3 Stimulate','2 Stimulate') AND Talk_tenure IN (0,1)			THEN  32
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m = 0 AND Simple_Segment IN ('1 Secure','3 Stimulate','2 Stimulate') AND Talk_tenure IN (2,3)			THEN  33
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m = 0 AND Simple_Segment IN ('1 Secure','3 Stimulate','2 Stimulate') AND Talk_tenure IN (3,4,5,6,7)		THEN  34
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m = 0 AND Simple_Segment IN ('1 Secure','3 Stimulate','2 Stimulate') AND Talk_tenure IN (8,9,10)			THEN  35
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m = 0 AND Simple_Segment IN ('2 Start','UNKNOWN') AND RTM IN ('UNKNOWN','Digital')                   	THEN  36
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m = 0 AND Simple_Segment IN ('2 Start','UNKNOWN') AND RTM IN ('Direct')                              	THEN  37
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m = 0 AND Simple_Segment IN ('2 Start','UNKNOWN') AND RTM IN ('Homes & Indies','Retail')             	THEN  38
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m = 0 AND Simple_Segment LIKE '%Support%'	AND RTM IN ('UNKNOWN','Digital','Homes & Indies')       	THEN  39
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m = 0 AND Simple_Segment LIKE '%Support%' 	AND RTM IN ('Direct','Retail')                  			THEN  40
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m = 0 AND Simple_Segment IN ('5 Stabilise','4 Stabilise','6 Suspense','5 Suspense') AND Talk_tenure IN (0,1)			THEN  41
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m = 0 AND Simple_Segment IN ('5 Stabilise','4 Stabilise','6 Suspense','5 Suspense') AND Talk_tenure IN (2,3,4,5) 	THEN  42
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m = 0 AND Simple_Segment IN ('5 Stabilise','4 Stabilise','6 Suspense','5 Suspense') AND Talk_tenure IN (6,7,8,9,10)	THEN  43
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m = 1 AND RTM IN ('UNKNOWN','Digital')                                                             					THEN  18
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m = 1 AND RTM IN ('Direct') AND Simple_Segment IN ('1 Secure','3 Stimulate','2 Stimulate','4 Support','3 Support')  	THEN  44
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m = 1 AND RTM IN ('Direct') AND Simple_Segment IN ('2 Start','5 Stabilise','4 Stabilise','UNKNOWN','6 Suspense','5 Suspense')	THEN  45
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m = 1 AND RTM IN ('Homes & Indies','Retail')                                                    		THEN  20
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m = 2 AND Talk_tenure IN (0,1,2)																		THEN  21
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m = 2 AND Talk_tenure IN (3,4,5,6) AND Simple_Segment IN ('1 Secure','4 Support','3 Support')		THEN  46
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m = 2 AND Talk_tenure IN (3,4,5,6) AND Simple_Segment NOT IN ('1 Secure','4 Support','3 Support')    THEN  47
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m = 2 AND Talk_tenure IN (7,8)	 																	THEN  23
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m = 2 AND Talk_tenure IN (9,10)                                                           			THEN  24
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m > 2 AND Talk_tenure IN (0,1,2) AND Simple_Segment NOT IN ('5 Stabilise','4 Stabilise')    			THEN  48
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m > 2 AND Talk_tenure IN (0,1,2) AND Simple_Segment LIKE '%Stabilise%'            					THEN  49
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m > 2 AND Talk_tenure IN (3,4,5,6) AND Simple_Segment IN ('1 Secure','4 Support','3 Support')   		THEN  50
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m > 2 AND Talk_tenure IN (3,4,5,6) AND Simple_Segment IN ('2 Start','UNKNOWN')                       THEN  51
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m > 2 AND Talk_tenure IN (3,4,5,6) AND Simple_Segment IN ('3 Stimulate','2 Stimulate','5 Stabilise','6 Suspense','4 Stabilise','5 Suspense')  THEN  52
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m > 2 AND Talk_tenure IN (7,8) AND RTM NOT IN ('Direct')                                     		THEN  53
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m > 2 AND Talk_tenure IN (7,8) AND RTM IN ('Direct')                                              	THEN  54
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m > 2 AND Talk_tenure = 9																			THEN  28
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m = 0 AND my_sky_login_3m > 2 AND Talk_tenure = 10																			THEN  29
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m > 0 AND Talk_tenure IN (0,1)		                                                                                  		THEN  11
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m > 0 AND Talk_tenure IN (2,3,4)                                                                                     		THEN  12
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m > 0 AND Talk_tenure >= 5 AND my_sky_login_3m <= 1                                                                          THEN  30
				WHEN DTV_TA_calls_1m = 0 AND BB_all_calls_1m > 0 AND Talk_tenure >= 5 AND my_sky_login_3m > 1                                                                           THEN  31
				WHEN DTV_TA_calls_1m > 0 AND Simple_Segment IN ('1 Secure','2 Start','3 Stimulate','2 Stimulate','UNKNOWN')                              								THEN  5
				WHEN DTV_TA_calls_1m > 0 AND Simple_Segment IN ('4 Support','5 Stabilise','4 Stabilise','6 Suspense','5 Suspense','3 Support')                         					THEN  6
				ELSE 0
			END

UPDATE BB_TP_Product_Churn_segments_lookup
SET segment = CASE 	WHEN node IN ( 5, 31, 12, 6, 30, 41, 11, 51, 52, 49, 54, 47, 50, 45, 46)	THEN 'High Risk'
					WHEN node IN ( 23, 28, 53, 24, 29, 44, 42, 37, 43, 38, 21, 20, 48, 18)  	THEN 'Medium Risk'
					WHEN node IN ( 40, 39, 36, 32, 33, 34, 35)                       			THEN 'Low Risk'
					ELSE 'No segment' 
				END

COMMIT
GO

--====================================================================================================================================================================================
--====================================================================================================================================================================================
--====================================================================================================================================================================================