/**************** PART A: PLACEHOLDER FOR VIRTUAL PANEL BALANCE ****************/
-- This section nominally decides which boxes are considered to be on the panel
-- for each day. There could be a bunch of influences here:
--   * Completeness of returned data in multiroom households
--   * Regularity of returned data for panel stability / box reliability
--   * Virtual panel balance decisions (using the wekly segmentation) - NYIP
-- The output is a table of account numbers and scaling segment IDs. Which is
-- the other reason why it depends on the segmentation build.

CREATE OR REPLACE PROCEDURE V289_M11_02_SC3_v1_1__prepare_panel_members_clean
	 @profiling_date DATE -- Thursday to use for scaling
	,@scaling_day DATE -- Day for which to do scaling
	,@batch_date DATETIME = now () -- Day on which build was kicked off

AS
BEGIN

	-- -- Test input arguments
	-- CREATE OR REPLACE VARIABLE @profiling_date date    =       '2015-02-05';
	-- CREATE OR REPLACE VARIABLE @scaling_day    date    =       '2015-02-06';
	-- CREATE OR REPLACE VARIABLE @batch_date             datetime        =       now();
	-- CREATE OR REPLACE VARIABLE @dp_tname     varchar(50);
	-- CREATE OR REPLACE VARIABLE @query            varchar(5000);
	-- CREATE OR REPLACE VARIABLE @from_dt  integer;
	-- CREATE OR REPLACE VARIABLE @to_dt            integer;
	
	-- DECLARE      @profiling_date date    =       '2015-02-05'
	-- DECLARE  @scaling_day    date    =       '2015-02-06'
	-- DECLARE  @batch_date             datetime = now()
	-- COMMIT
	/**************** A00: CLEANING OUT ALL THE OLD STUFF ****************/
	MESSAGE cast(now() as timestamp)||' | M11.2 Start 'TO CLIENT
	TRUNCATE TABLE SC3_todays_panel_members
	COMMIT -- (^_^)
	
	/***************************** A01.1: Initialising extract variables ************************/
	DECLARE @dp_tname VARCHAR(50)
	DECLARE @query VARCHAR(5000)
	DECLARE @from_dt INTEGER
	DECLARE @to_dt INTEGER

	COMMIT

	SET @dp_tname = 'SK_PROD.VESPA_DP_PROG_VIEWED_'
	SELECT @from_dt = cast((DATEFORMAT (dateadd(hour, 6, @scaling_day),'YYYYMMDD') || '00') AS INTEGER)
	SELECT @to_dt = cast((DATEFORMAT (dateadd(hour, 30, @scaling_day),'YYYYMMDD') || '23') AS INTEGER)
	SET @dp_tname = @dp_tname || datepart(year, @scaling_day) || right(('00' || cast(datepart(month, @scaling_day) AS VARCHAR(2))), 2)
	COMMIT -- (^_^)

	-- Prepare to catch the week's worth of logs:
	CREATE TABLE #raw_logs_dump_temp (
		account_number VARCHAR(20) NOT NULL
		,service_instance_id VARCHAR(30) NOT NULL
		)

	COMMIT -- (^_^)
	SET @query = 'insert into #raw_logs_dump_temp ' 
		|| 'select         distinct ' 
		|| 'account_number ' 
		|| ',service_instance_id ' 
		|| 'from ' || @dp_tname 
		|| ' where dk_event_start_datehour_dim between ' || @from_dt || ' and ' || @to_dt 
		|| ' and           (panel_id = 12 or panel_id = 11) ' || 'and    account_number is not null ' || 'and    service_instance_id is not null '
	
	EXECUTE (@query)
	
	MESSAGE cast(now() as timestamp)||' | M11.1 #raw_logs_dump_temp creation. Rows:'||@@rowcount TO CLIENT
	
	COMMIT -- (^_^)
	CREATE hg INDEX idx1 ON #raw_logs_dump_temp (account_number)
	CREATE hg INDEX idx2 ON #raw_logs_dump_temp (service_instance_id)
	COMMIT -- (^_^)

	CREATE TABLE #raw_logs_dump (
		account_number VARCHAR(20) NOT NULL
		,service_instance_id VARCHAR(30) NOT NULL
		)

	COMMIT -- (^_^)

	INSERT INTO #raw_logs_dump
	SELECT DISTINCT account_number
		,service_instance_id
	FROM #raw_logs_dump_temp

	MESSAGE cast(now() as timestamp)||' | M11.1 #raw_logs_dump creation. Rows:'||@@rowcount TO CLIENT
	
	COMMIT -- (^_^)
	CREATE INDEX some_key ON #raw_logs_dump (account_number)
	COMMIT -- (^_^)

	SELECT account_number
		,count(DISTINCT service_instance_id) AS box_count
		,convert(TINYINT, NULL) AS expected_boxes
		,convert(INT, NULL) AS scaling_segment_id
	INTO #panel_options
	FROM #raw_logs_dump
	GROUP BY account_number

	MESSAGE cast(now() as timestamp)||' | M11.1 #panel_options creation. Rows:'||@@rowcount TO CLIENT
	
	COMMIT -- (^_^)
	CREATE UNIQUE INDEX fake_pk ON #panel_options (account_number)
	COMMIT -- (^_^)

	DROP TABLE #raw_logs_dump
	COMMIT -- (^_^)

	-- Getting this list of accounts isn't enough, we also want to know if all the boxes
	-- of the household have returned data.
	UPDATE #panel_options
	SET expected_boxes = sbss.expected_boxes
		,scaling_segment_id = sbss.vespa_scaling_segment_id
	FROM #panel_options
	INNER JOIN SC3_Sky_base_segment_snapshots AS sbss ON #panel_options.account_number = sbss.account_number
	WHERE sbss.profiling_date = @profiling_date

	COMMIT -- (^_^)
	TRUNCATE TABLE SC3_todays_panel_members
	COMMIT -- (^_^)

	-- First moving the unique account numbers in...
	INSERT INTO SC3_todays_panel_members (
		account_number
		,scaling_segment_id
		)
	SELECT account_number
		,scaling_segment_id
	FROM #panel_options
	WHERE expected_boxes >= box_count
		AND scaling_segment_id IS NOT NULL
	COMMIT -- (^_^)
	
	MESSAGE cast(now() as timestamp)||' | M11.1 #SC3_todays_panel_members insert. Rows:'||@@rowcount TO CLIENT
	
	-- Clean up
	DROP TABLE #panel_options

	COMMIT -- (^_^)
END;-- of procedure "V289_M11_02_SC3_v1_1__prepare_panel_members"

COMMIT;
