------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
-----------------																				 -----------------------
-----------------					Session size definition process 							 -----------------------
-----------------																				 -----------------------
------------------------------------------------------------------------------------------------------------------------

----------------- Note: 18/07/2014 ####Overlapping###  needs to be defined by Angel in the dp event table. 


CREATE VARIABLE @account 	 varchar(20);  	---Account Number
CREATE VARIABLE @subs		 decimal(10);		---Subscriber ID
CREATE VARIABLE @iter		 tinyint;			---Max iteration x account
CREATE VARIABLE @cont		 tinyint;			---counter for subs iteration
CREATE VARIABLE @event 		 bigint;			---for event iteration
CREATE VARIABLE @length 	 DECIMAL(7,6);	---MC proportional length
CREATE VARIABLE @random 	 FLOAT;			---MC Random number 
CREATE VARIABLE @s_size 	 tinyint;			---Event session size
CREATE VARIABLE @adj_hh		 tinyint;			---Adjusted HH size (for MC Multibox process only)
CREATE VARIABLE @hh_size	 tinyint;			---HH size
commit 

SELECT DISTINCT 
	  account_number
	, subscriber_id
	, primary_box 			------ 
	, rank() OVER (PARTITION BY account_number, ORDER BY ####primary_box#### DESC, subscriber_id DESC) AS Rank_1
	, proc_flag_1 = 0
	, proc_flag_2 = 0
INTO #accounts
FROM angeld.V289_M07_dp_data 

COMMIT 

CREATE HG INDEX id1 ON #accounts(account_number)
CREATE HG INDEX id2 ON #accounts(subscriber_id)


COMMIT 

MESSAGE '#account table done'  TYPE WARNING TO CLIENT

SELECT event_ID
		, CAST(NULL AS tinyint) session_size
		, CASE WHEN ###Overlap### > 0 	THEN 0 ELSE 1 END AS ovelap_flag
INTO temp_event		
FROM angeld.V289_M07_dp_data

COMMIT
CREATE UNIQUE INDEX ide1 ON #temp_event(event_ID)
COMMIT

MESSAGE 'event table done'  TYPE WARNING TO CLIENT


------------	Loop by account
WHILE EXISTS (SELECT top 1 account_number FROM #accounts WHERE proc_flag_1 =0)			--------	ACCOUNT LOOP 
BEGIN 
	SET  	@account 	= (SELECT top 1 account_number FROM #accounts WHERE proc_flag_1 =0)
	SET 	@cont = 1 
	SET 	@iter		= (SELECT MAX(rank_1) 	FROM #accounts WHERE  account_number= @account)
	
	--------------- SUBSCRIBER LOOP
	
	WHILE @cont <= @iter  
	BEGIN 
		SET  	@subs		= (SELECT subscriber_id 	FROM #accounts WHERE  account_number= @account  AND rank_1 = @cont)
		
		SELECT  
			mr.event_id
			, account_number
			, subscriber_id 
			, event_Start_utc
			, CASE WHEN hhsize > 8 THEN 8 ELSE hhsize END as hhsize 
			, segment_ID
			, random		= 	RAND(mr.event_id + DATEPART(us, GETDATE()) 
			, ####Overlapping### 
			, ev_proc_flag 	= 	CASE WHEN tev.session_size IS NOT NULL THEN 1 ELSE 0 END ) 
			, session_size 	= 	COALESCE (tev.session_size, CAST (1 AS tinyint) )
			--	, channel_pack				Not Needed if Segment ID is included	
			--	, session_daypart
			--	, programme_genre
		INTO #events
		FROM angeld.V289_M07_dp_data AS mr
		LEFT JOIN temp_event as tev ON mr.event_id = tev.event_id 
		WHERE account_numer = @account
		ORDER BY event_Start_utc
		
		COMMIT 
		
		CREATE UNIQUE 	INDEX idxe1 	ON #events (event_ID)
		CREATE HG 		INDEX id1 		ON #events(account_number)
		CREATE HG 		INDEX id2 		ON #events(subscriber_id)
		
		COMMIT 
		-------------------------------- MC process 
		WHILE EXISTS (SELECT top 1 event_id FROM #events WHERE ev_proc_flag = 0)
		BEGIN 
			SET @event = (SELECT top 1 event_id FROM #events WHERE ev_proc_flag = 0)
			SET @Random = (SELECT random FROM #events WHERE event_id = @event)
			SET @hh_size = (SELECT hhsize FROM #events WHERE event_id = @event)
			
			SELECT 
				  mx.session_size
				, proportion
				, SUM (proportion)  OVER    ( ORDER BY mx.session_size ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) 	AS Lower_limit
                , SUM (proportion)  OVER    ( ORDER BY mx.session_size ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 	AS Upper_limit
			INTO #MC_event
			FROM v289_sessionsize_matrix 	AS mx
			JOIN #events 					AS ev ON mx.segment_ID = ev.segment_id AND ev.hh_size = mx.hh_size AND ev.event_id = @event
			ORDER BY session_size

			------------------------ MULTI BOX SELECTION		(Single box doesn't need adjustment in the probability vector) 
			IF (SELECT ####Overlapping### FROM #events WHERE event_id = @event ) = 0 OR  (SELECT ####Overlapping### FROM #events WHERE event_id = @event ) IS NULL 
			BEGIN 
				IF @cont = 1 			----- PRIMARY BOXES
					SET @adj_hh	 = @hh_size - (COUNT ####Overlapping### )+ 1 
				ELSE 					----- Secondary Boxes
					SET @adj_hh	 = @hh_size - (SELECT SUM session_size FROM #events WHERE  ####Overlapping###  = ####Overlapping### session) + @cont
				DELETE FROM #MC_event WHERE session_size > @adj_hh
				COMMIT 
			END 
			-----------		NORMALIZING the sum of probabilities to 1 
			SET @length = (SELECT SUM (proportion)	FROM 	#MC_event)		
			IF @length <> 1  
				UPDATE  #MC_event 
				SET proportion = proportion / @length
					, upper_limit = upper_limit / @length
					, lower_limit = lower_limit / @length
			COMMIT 
			------------	Assigning Session Size 
			SET @s_size = (SELECT TOP 1 session_size FROM #MC_event WHERE @Random BETWEEN lower_limit AND upper_limit)
			------------	Appending session size in the tables 
			UPDATE #events
			SET ev_proc_flag = 1
				, session_size = @s_size
			WHERE event_id = @event
			
			UPDATE temp_event 
			SET session_size = @s_size
			WHERE event_id = @event		
			
			COMMIT 
			
		END	
		SET @cont = @cont + 1 
	END 

	UPDATE #accounts 
	SET  proc_flag_1 =1 
	WHERE account_number = @account
	DROP TABLE #events
	DROP TABLE #MC_event
    COMMIT

END 