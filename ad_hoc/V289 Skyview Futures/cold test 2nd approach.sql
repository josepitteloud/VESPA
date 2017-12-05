------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
-----------------                                                                                -----------------------
-----------------                   Session size definition process                              -----------------------
-----------------                                                                                -----------------------
------------------------------------------------------------------------------------------------------------------------
TRUNCATE TABLE session_log

INSERT INTO session_log (Description, value_count)
SELECT 'Process Started', @@rowcount


-----------------	Session VARIABLEs Definition
CREATE VARIABLE @account     varchar(20);     ---Account Number
CREATE VARIABLE @subs        decimal(10);     ---Subscriber ID
CREATE VARIABLE @iter        tinyint;         ---Max iteration x account
CREATE VARIABLE @cont        tinyint;         ---counter for subs iteration
CREATE VARIABLE @event       bigint;          ---for event iteration
CREATE VARIABLE @length      DECIMAL(7,6);    ---MC proportional length
CREATE VARIABLE @random      FLOAT;           ---MC Random number
CREATE VARIABLE @s_size      tinyint;         ---Event session size
CREATE VARIABLE @adj_hh      tinyint;         ---Adjusted HH size (for MC Multibox process only)
CREATE VARIABLE @hh_size     tinyint;         ---HH size
CREATE VARIABLE @segment     tinyint;         ---Segment_ID
CREATE VARIABLE @batch       tinyint;         ---Overlap Batch
CREATE VARIABLE @row_id		 INT;
CREATE VARIABLE @event_id  		BIGINT;
commit

-----------------	temp_event Table Creation
IF OBJECT_ID('temp_event') IS NOT NULL DROP TABLE temp_event
SELECT             			TOP 10000   
        event_ID
        , dt.account_number
		, dt.subscriber_id
		, CASE WHEN hhsize > 8 THEN 8 ELSE hhsize END as hhsize
		, COALESCE (dt.segment_ID, 157) AS segment_ID
		, random1       =   RAND(dt.event_id + DATEPART(us, GETDATE()))
		, overlap       =   ov.overlap_size 
        , COALESCE(dt.overlap_batch,0) 	AS overlap_batch 
        , CAST(0 AS tinyint) session_size
INTO temp_event
FROM angeld.V289_M07_dp_data AS dt
LEFT JOIN (SELECT
					  count (event_id) 	AS overlap_size
					, account_number
					, Overlap_batch
			FROM angeld.V289_M07_dp_data
			GROUP BY Overlap_batch, account_number
			)   AS ov ON ov.account_number = dt.account_number AND ov.Overlap_batch = dt.overlap_batch

INSERT INTO session_log (Description, value_count)
SELECT 'temp_event table created', @@rowcount
			
COMMIT

CREATE HG INDEX ide1 ON temp_event(event_ID)
CREATE LF INDEX ide2 ON temp_event(overlap_batch)
CREATE LF INDEX ide3 ON temp_event(segment_ID)
CREATE LF INDEX ide4 ON temp_event(hhsize)
COMMIT

-----------------	Single box events update
UPDATE temp_event
SET ev.session_size = mx.session_size
FROM temp_event as ev
JOIN pitteloudj.v289_sessionsize_matrix_default   AS mx ON  mx.segment_ID = ev.segment_id 
														AND ev.hhsize = mx.hhsize 
														AND random1 > lower_limit 
														AND random1 <= upper_limit
WHERE Overlap_batch = 0 OR overlap = 1


INSERT INTO session_log (Description, value_count)
SELECT 'Single box events updated', @@rowcount



--------    ACCOUNT LOOP
WHILE EXISTS (SELECT top 1 event_ID FROM temp_event WHERE session_size = 0 AND hhsize is not null)           --------    ACCOUNT LOOP
BEGIN
	SET rowcount 1
    SET     @account    = (SELECT account_number FROM  temp_event  WHERE session_size =0 and hhsize is not null AND Overlap_batch > 0)
    SET     @cont = 1
	SET     @hh_size    = (SELECT hhsize FROM temp_event WHERE account_number = @account)
	    SET rowcount 0
	IF OBJECT_ID('events') IS NOT NULL DROP TABLE events
	SELECT
              *
			, row_id = row_number() over(order by subscriber_id)
            , ev_proc_flag  = 	CAST (0 AS BIT)
			, box_rank 		= 	rank() OVER (PARTITION BY account_number ORDER BY subscriber_id DESC)
			, adj_hh = hhsize - overlap + 1
	INTO events
    FROM temp_event 
    WHERE account_number = @account 
			AND session_size = 0 
			AND hhsize is not null
    ORDER BY subscriber_id
	
	COMMIT 
	
	CREATE HG 		INDEX idxe1     ON events (event_ID)
    CREATE LF       INDEX id1       ON events (overlap_batch)
    CREATE HG       INDEX id2       ON events (subscriber_id)
	CREATE LF       INDEX box       ON events (box_rank)
	
	COMMIT
	
	SET     @iter       = (SELECT MAX(box_rank)    FROM events )
	WHILE @cont <= @iter
    BEGIN		
		WHILE EXISTS (SELECT top 1 session_size FROM events WHERE session_size = 0 AND box_rank = @cont AND ev_proc_flag = 0)
		BEGIN 
			SET @row_id 	= (SELECT top 1 row_id 	FROM events WHERE session_size = 0 AND box_rank = @cont AND ev_proc_flag = 0)
			SET @event_id 	= (SELECT event_id 		FROM events WHERE row_id = @row_id)
			SET @batch		= (SELECT Overlap_batch	FROM events WHERE row_id = @row_id)
			SET @random     = (SELECT random1       FROM events WHERE row_id = @row_id)
			SET @adj_hh     = (SELECT adj_hh        FROM events WHERE row_id = @row_id)
            IF @cont > 1                           ----- Secondary Boxes
                    SET @adj_hh  = @hh_size - (SELECT SUM (session_size) FROM events WHERE Overlap_batch = @batch) + @cont	
			----------------- Probability Vector Extraction 
            IF OBJECT_ID('MC_event') IS NOT NULL DROP TABLE MC_event
            SELECT
                  mx.session_size
                , proportion
                , Lower_limit
                , Upper_limit
            INTO MC_event
            FROM pitteloudj.v289_sessionsize_matrix_default    AS mx
            JOIN events                    AS ev ON mx.segment_ID = ev.segment_id AND ev.hhsize = mx.hhsize AND ev.row_id = @row_id
            ORDER BY mx.session_size

            ------------------------ MULTI BOX SELECTION        (Single box doesn't need adjustment in the probability vector)


--			DELETE FROM MC_event WHERE session_size > @adj_hh
			COMMIT
            -----------------	NORMALIZING the sum of probabilities to 1
            SET @length = (SELECT SUM (proportion)  FROM    MC_event)
            SET @random = @random * @length

            COMMIT
            -----------------	Assigning Session Size
            SET @s_size = (SELECT TOP 1 session_size FROM MC_event WHERE @Random BETWEEN lower_limit AND upper_limit)

            -----------------	Appending session size in the tables
            UPDATE events
            SET ev_proc_flag = 1
                , session_size = @s_size
            WHERE row_id = @row_id

            UPDATE temp_event
            SET session_size = @s_size
            WHERE event_id = @event_id AND overlap_batch = @batch
            COMMIT
    
        END
        SET @cont = @cont + 1
        COMMIT
    END
	DROP TABLE events
    COMMIT
END
/*
UPDATE V289_M07_dp_data
SET dt.session_size = te.session_size
FROM V289_M07_dp_data AS dt
INNER JOIN temp_event AS te ON te.event_id = dt.event_id AND te.overlap_batch = dt.overlap_batch

DROP TABLE temp_event 
DROP TABLE accounts


*/


/*          QA
SELECT top 100 * FROM temp_event WHERE session_size is not null
SELECT top 100 * FROM accounts
SELECT top 100 * FROM events
SELECT top 100 * FROM MC_event
*/