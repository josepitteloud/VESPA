

CREATE TABLE SKY_Q_completion_data
( 	  account_number VARCHAR (12)  -- Customer Account Number	
	, compl_dt DATE 				-- Q Completion Date	
	, compl_wk int					-- Completion Week	
	, cust_type VARCHAR ( 20) 		-- Customer Type	
	, new_cust	VARCHAR ( 20)		-- New or Existing	
	, bb_provider VARCHAR ( 20)		-- Broadband Provider	
	, First_flag VARCHAR ( 20)		-- Q First Install Flag	
	, Q_Ref	BIGINT					-- Q Job Ref
	, visit_type VARCHAR ( 20)		-- Type of Visit	
	, Visit_desc VARCHAR ( 60) 		-- Q Visit Description	
	, subs_holding VARCHAR ( 60)	-- Q Subs Holding	
	, call_more_1 VARCHAR ( 5)		-- Call more than once flag	
	, Total_calls INT 				-- Called more than once total calls	
	, call_0_2 VARCHAR ( 5)			-- 0-2 day call flag	
	, t_call_0_2 INT				-- 0-2 day total calls	
	, call_0_7 VARCHAR ( 5)			-- 0-7 day call flag	
	, t_call_0_7 INT				-- 0-7 day total calls	
	, call_0_14 VARCHAR ( 5) 		-- 0-14 day call flag	
	, t_call_0_14 INT				-- 0-14 day total calls	
	, Service_Visit_Flag VARCHAR ( 40) -- Service Visit Flag
	, Service_Visit_time VARCHAR ( 40) -- Service_Visit_Flag Service Visit Timings
	, A_RAG_0_2 tinyint				-- Amber RAG 0-2 days after completion
	, A_RAG_0_7 tinyint				-- Amber RAG 0-7 days after completion
	, A_RAG_0_14 tinyint			-- Amber RAG 0-14 days after completion
	, A_RAG_compl tinyint			-- Amber RAG the day of completion
	, R_RAG_0_2 tinyint				-- Red RAG days 0-2 after completion
	, R_RAG_0_7 tinyint				-- Red RAG days 0-7 after completion
	, R_RAG_0_14 tinyint			-- Red RAG days 0-14 after completion
	, R_RAG_compl tinyint			-- Red RAG the day of completion
	
		)

Begin Parallel IQ 
CREATE UNIQUE HG     Index idxID                on SKY_Q_completion_data(account_number);
Create WD            Index idxPac			    on SKY_Q_completion_data(bb_provider);
Create Date          Index idxStartDate         on SKY_Q_completion_data(compl_dt);
Create WD	         Index idxEndDate           on SKY_Q_completion_data(subs_holding);
Create WD	         Index idxEn2ee		        on SKY_Q_completion_data(visit_type);
End Parallel IQ



SELECT 
	a.account_number
	, x.v_card
	, compl_dt AS dx
	, A_RAG_0_2 		= SUM(CASE WHEN  DATEDIFF (DAY, dx, dt) BETWEEN -1 AND 2  AND RAG = 9500 THEN 1 ELSE  0 END)
	, A_RAG_0_7			= SUM(CASE WHEN  DATEDIFF (DAY, dx, dt) BETWEEN -1 AND 7  AND RAG = 9500 THEN 1 ELSE  0 END)
	, A_RAG_0_14		= SUM(CASE WHEN  DATEDIFF (DAY, dx, dt) BETWEEN -1 AND 14 AND RAG = 9500 THEN 1 ELSE  0 END)
	, A_RAG_compl		= SUM(CASE WHEN  DATEDIFF (DAY, dx, dt) BETWEEN -1 AND 0  AND RAG = 9500 THEN 1 ELSE  0 END)
	, R_RAG_0_2			= SUM(CASE WHEN  DATEDIFF (DAY, dx, dt) BETWEEN -1 AND 2  AND RAG = 15000 THEN 1 ELSE  0 END)
	, R_RAG_0_7			= SUM(CASE WHEN  DATEDIFF (DAY, dx, dt) BETWEEN -1 AND 7  AND RAG = 15000 THEN 1 ELSE  0 END)
	, R_RAG_0_14		= SUM(CASE WHEN  DATEDIFF (DAY, dx, dt) BETWEEN -1 AND 14 AND RAG = 15000 THEN 1 ELSE  0 END)	
	, R_RAG_compl		= SUM(CASE WHEN  DATEDIFF (DAY, dx, dt) BETWEEN -1 AND 0  AND RAG = 15000 THEN 1 ELSE  0 END)
into sky_q_TEMP	
FROM 	SKY_Q_completion_data AS a 
JOIN SKY_Q_MESH_RAG AS x ON x.account_number = a.account_number 
group by a.account_number, dx 