
    --------------------------------------------------------------------------------
    -- A00) SET UP OF METADATA VARIABLES.
    --------------------------------------------------------------------------------
-- metadata variables


create or replace procedure V306_CP2_M05_Build_Day_Caps
											@target_date		date	=	NULL     -- Date of daily table caps to cache
										,	@iteration_number	int		=	NULL     -- current iteration
as begin


	execute M00_2_output_to_logger '@ M05 : V306_CP2_M05_Build_Day_Caps day iteration'
	COMMIT
			
	--------------------------------------------------------------------------------
	-- A01) SET UP.
	--------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : A01) SET UP'
	COMMIT

	/*create or replace variable*/ declare @targetDateIsWeekend int
	commit
	
	set @targetDateIsWeekend=(case when datepart(weekday,@target_date) in (1,7) then 1 else 0 end)
	commit
	
	/*create or replace variable*/ declare @current_metadata_row_number int -- the id of the row that contains current data
	commit

	set	@current_metadata_row_number	=	(
												select	max(row_id)
												from	CP2_metadata_table
												where
														@target_date	between	EFFECTIVE_FROM
																		and		EFFECTIVE_TO
													and	BANK_HOLIDAY_WEEKEND	=	@targetDateIsWeekend
											)
	commit
	
	-- create and populate variables
	/*create or replace variable*/ declare @playback_days          tinyint         -- How many days back worth of playback viewing data to consider
	commit --; --^^ to be removed
	set     @playback_days = 28                     -- Want to be able to treat timeshifting of up to 28 days
	commit --; --^^ to be removed

	commit --; --^^ to be removed
	/*create or replace variable*/ declare @BARB_day_cutoff_time   time            -- because we don't know if the day starts at 2AM or 4 or 6 or 9 and this means it's easily changed
	commit --; --^^ to be removed
	set     @BARB_day_cutoff_time = '02:00:00'      -- Treating the day as 2AM to 2AM. Currently not really using this though?
	commit --; --^^ to be removed

	-- The following parameters are currently static in the metadata table
	commit --; --^^ to be removed
	/*create or replace variable*/ declare @max_cap_bound_minutes  integer         -- The largest a capping bound can possibly be. This is important as we have to get viewing records out of the previous day's table to cap ea
	commit --; --^^ to be removed
	set     @max_cap_bound_minutes = (select MAXIMUM_CUT_OFF from CP2_metadata_table where row_id=@current_metadata_row_number )--120
	commit --; --^^ to be removed

	commit --; --^^ to be removed
	/*create or replace variable*/ declare @min_cap_bound_minutes  integer         -- The smallest a capping bound can possibly be
	commit --; --^^ to be removed
	set     @min_cap_bound_minutes = (select MINIMUM_CUT_OFF from CP2_metadata_table where row_id=@current_metadata_row_number )--20
	commit --; --^^ to be removed

	commit --; --^^ to be removed
	/*create or replace variable*/ declare @min_view_duration_sec  tinyint         -- The bound below which views are ignored, in seconds
	commit --; --^^ to be removed
	set     @min_view_duration_sec  = (select SHORT_DURATION_CAP_THRESHOLD from CP2_metadata_table where row_id=@current_metadata_row_number )--6
	commit --; --^^ to be removed

	commit --; --^^ to be removed
	/*create or replace variable*/ declare @uncapped_sample_pop_max integer        -- The maximum number of uncapped events to consider per bucket for selecting new end times for each capped event
	commit --; --^^ to be removed
	set     @uncapped_sample_pop_max = (select SAMPLE_MAX_POP from CP2_metadata_table where row_id=@current_metadata_row_number )--10000        -- About 2.5 hours to process the matching loop for one day; good thing we're scheduling these overnight
	commit --; --^^ to be removed
																																									-- Update: Now pulled back to 10k given we're also batching by initial channel now too

	commit --; --^^ to be removed
	/*create or replace variable*/ declare @var_sql                varchar(15000)   -- For dynamic SQL over daily tables, though we're only capping one day so there's no looping
	commit --; --^^ to be removed
	/*create or replace variable*/ declare @QA_catcher             integer

	commit --; --^^ to be removed
	/*create or replace variable*/ declare @gmt_start                              date                     -- To capture when the clocks go back in Autumn
	commit --; --^^ to be removed
	/*create or replace variable*/ declare @bst_start                              date                     -- To capture when the clocks go forward in Spring
	commit --; --^^ to be removed

	commit --; --^^ to be removed
	set @bst_start = dateadd(dy, -(datepart(dw, datepart(year, today()) || '-03-31') -1),datepart(year, today()) || '-03-31')  -- to get last Sunday in March
	commit --; --^^ to be removed
	set @gmt_start = dateadd(dy, -(datepart(dw, datepart(year, today()) || '-10-31') -1),datepart(year, today()) || '-10-31')  -- to get last Sunday in October
	commit --; --^^ to be removed

	-- Dev purposes only:
	--set @target_date = '2012-01-30';
	-- For dev build, we're going through to the end of Feb.

	execute M00_2_output_to_logger  'A01: CP2 caching caps for ' || convert(varchar(10),@target_date,123)
	commit --;-- ^^ originally a commit


	--------------------------------------------------------------------------------
	-- Removing redundant tables which do not longer exist or has been renamed
	--------------------------------------------------------------------------------
	commit --; --^^ to be removed
	if object_id('CP2_Viewing_Records')                 is not null drop table CP2_Viewing_Records
	commit --; --^^ to be removed
	if object_id('CP2_01_Viewing_Records')              is not null drop table CP2_01_Viewing_Records
	commit --; --^^ to be removed
	if object_id('Uncleansed_Viewing_Totals')           is not null drop table Uncleansed_Viewing_Totals
	commit --; --^^ to be removed
	if object_id('CP2_4BARB_internal_viewing')          is not null drop table CP2_4BARB_internal_viewing
	commit --; --^^ to be removed
	if object_id('CP2_4BARB_view_endpoints')            is not null drop table CP2_4BARB_view_endpoints
	commit --; --^^ to be removed
	if object_id('cumulative_playback_corrections')     is not null drop table cumulative_playback_corrections
	commit --; --^^ to be removed
	if object_id('cumulative_playback_corrections_2')   is not null drop table cumulative_playback_corrections_2
	commit --; --^^ to be removed
	if object_id('CP2_viewing_control_cap_distrib')     is not null drop table CP2_viewing_control_cap_distrib
	commit --; --^^ to be removed
	if object_id('CP2_viewing_control_distribs')        is not null drop table CP2_viewing_control_distribs
	commit --; --^^ to be removed
	if object_id('CP2_viewing_control_totals')          is not null drop table CP2_viewing_control_totals
	commit --; --^^ to be removed


	-- select 'start B', now(); --^^ just for debug

	--------------------------------------------------------------------------------
	-- B) - Get The viewing Data
	--------------------------------------------------------------------------------

	--------------------------------------------------------------------------------
	-- B02 - Get the viewing data
	--------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : B02 - Get the viewing data'
	COMMIT

	if object_id('Capping2_01_Viewing_Records') is not null drop table Capping2_01_Viewing_Records
	commit --; --^^ to be removed
	create table Capping2_01_Viewing_Records (
					ID_Key                              bigint          primary key identity
					,cb_row_ID                          bigint          not null
					,Account_Number                     varchar(20)     not null
					,Subscriber_Id                      integer
					,X_Type_Of_Viewing_Event            varchar(40)     not null
					,Adjusted_Event_Start_Time          datetime
					,X_Adjusted_Event_End_Time          datetime
					,X_Viewing_Start_Time               datetime
					,X_Viewing_End_Time                 datetime
					,Tx_Start_Datetime_UTC              datetime
					,X_Event_Duration                   decimal(10,0)
					,X_Programme_Viewed_Duration        decimal(10,0)
					,Programme_Trans_Sk                 bigint          not null
					,Service_Key                        bigint
					,daily_table_date                   date            not null
					,live                               bit
					,genre                              varchar(25)
					,sub_genre                          varchar(25)
					,epg_channel                        varchar(30)
					,channel_name                       varchar(30)
					,program_air_date                   date
					,program_air_datetime               datetime
					,event_start_day                    integer         default null
					,event_start_hour                   integer         default null
					,box_subscription                   varchar(1)      default 'U'
					,initial_genre                      varchar(30)     default null
					,initial_channel_name               varchar(30)     default null
					,Initial_Service_Key                bigint          default null
					,pack                               varchar(100)    default null
					,pack_grp                           varchar(30)     default null
					,bucket_id                          integer         default null
	)

	commit --; --^^ to be removed
	create hg   index idx2 on Capping2_01_Viewing_Records (Subscriber_Id)
	commit --; --^^ to be removed
	create dttm index idx3 on Capping2_01_Viewing_Records (Adjusted_Event_Start_Time)
	commit --; --^^ to be removed
	create dttm index idx4 on Capping2_01_Viewing_Records (X_Viewing_Start_Time)
	commit --; --^^ to be removed
	create hg   index idx5 on Capping2_01_Viewing_Records (bucket_id)
	commit --; --^^ to be removed
	create lf   index idx6 on Capping2_01_Viewing_Records (event_start_hour)
	commit --; --^^ to be removed
	create lf   index idx7 on Capping2_01_Viewing_Records (event_start_day)
	commit --; --^^ to be removed



	commit --; --^^ to be removed
	/*create or replace variable*/ declare @varBroadcastMinDate  int
	commit --; --^^ to be removed
	/*create or replace variable*/ declare @varEventStartHour    int
	commit --; --^^ to be removed
	/*create or replace variable*/ declare @varEventEndHour      int
	commit --; --^^ to be removed

	set @varBroadcastMinDate  = (dateformat(@target_date - @playback_days, 'yyyymmdd00'))
	commit --; --^^ to be removed
	set @varEventStartHour    = (dateformat(@target_date - 1, 'yyyymmdd23'))      -- Event to start no earlier than at 23:00 on the previous day
	commit --; --^^ to be removed
	set @varEventEndHour      = (dateformat(@target_date, 'yyyymmdd23'))          -- Event to start no later than at 23:59 on the next day
	commit --; --^^ to be removed

	insert into	Capping2_01_Viewing_Records (
													cb_row_ID
												,	Account_Number
												,	Subscriber_Id
												,	X_Type_Of_Viewing_Event
												,	Adjusted_Event_Start_Time
												,	X_Adjusted_Event_End_Time
												,	X_Viewing_Start_Time
												,	X_Viewing_End_Time
												,	Tx_Start_Datetime_UTC
												,	X_Event_Duration
												,	X_Programme_Viewed_Duration
												,	Programme_Trans_Sk
												,	Service_Key
												,	daily_table_date
												,	live
												,	genre
												,	sub_genre
												,	epg_channel
												,	channel_name
												,	program_air_date
												,	program_air_datetime
												,	event_start_day
												,	event_start_hour
											)
	select
			pk_viewing_prog_instance_fact              	as Cb_Row_Id
		,	Account_Number                             	as Account_Number
		,	Subscriber_Id                              	as Subscriber_Id
		,	Type_Of_Viewing_Event                      	as X_Type_Of_Viewing_Event
		,	EVENT_START_DATE_TIME_UTC                  	as Adjusted_Event_Start_Time
		,	EVENT_END_DATE_TIME_UTC                    	as X_Adjusted_Event_End_Time
		,	INSTANCE_START_DATE_TIME_UTC               	as X_Viewing_Start_Time
		,	INSTANCE_END_DATE_TIME_UTC                 	as X_Viewing_End_Time
		,	BROADCAST_START_DATE_TIME_UTC             	as Tx_Start_Datetime_UTC
		,	Duration									as X_Event_Duration
		,	datediff(second,INSTANCE_START_DATE_TIME_UTC, INSTANCE_END_DATE_TIME_UTC)
														as X_Programme_Viewed_Duration
		,	dk_programme_instance_dim                  	as Programme_Trans_Sk
		,	Service_Key                                	as Service_Key
		,	@target_date                               	as Daily_Table_Date
		,	case
				when	live_recorded = 'LIVE'	then	1
				else									0
			end											as live
		,	case
				when	Genre_Description is null	then	'Unknown'
				else										Genre_Description
			end											as genre
		,	case
				when	Sub_Genre_Description is null	then	'Unknown'
				else											Sub_Genre_Description
			end											as sub_genre
		,	null                                       	as epg_channel               -- epg_channel
		,	channel_name                               	as channel_name
		,	date(BROADCAST_START_DATE_TIME_UTC)        	as program_air_date
		,	BROADCAST_START_DATE_TIME_UTC              	as program_air_datetime
		,	datepart	(		day
							,	case
									when (EVENT_START_DATE_TIME_UTC between  dateadd(hh, 1, convert(datetime, @bst_start)) and dateadd(hh, 2, convert(datetime, @gmt_start))) then dateadd(hour, 1, EVENT_START_DATE_TIME_UTC) -- cortb added (2014-04-22) to check to see we are in BST or GMT
									else EVENT_START_DATE_TIME_UTC -- we're in GMT
								end
						)                            	as event_start_day
		,	datepart	(		hour
							,	case
									when (EVENT_START_DATE_TIME_UTC between  dateadd(hh, 1, convert(datetime, @bst_start)) and dateadd(hh, 2, convert(datetime, @gmt_start))) then dateadd(hour, 1, EVENT_START_DATE_TIME_UTC) -- cortb added (2014-04-22) to check to see we are in BST or GMT
									else EVENT_START_DATE_TIME_UTC -- we're in GMT
								end
						)                            	as event_start_hour
		/*,datepart(day, -- cortb commented out (2014-04-22) as this was outdated
												case
																when (EVENT_START_DATE_TIME_UTC <  '2012-03-25 01:00:00') then EVENT_START_DATE_TIME_UTC                      -- prior Mar 12 - no change, consider UTC = local
																when (EVENT_START_DATE_TIME_UTC <  '2012-10-28 02:00:00') then dateadd(hour, 1, EVENT_START_DATE_TIME_UTC)    -- Mar 12-Oct 12 => DST, add 1 hour to UTC (http://www.timeanddate.com/worldclock/timezone.html?n=136)
																when (EVENT_START_DATE_TIME_UTC <  '2013-03-31 01:00:00') then EVENT_START_DATE_TIME_UTC                      -- Oct 12-Mar 13 => UTC = Local
																when (EVENT_START_DATE_TIME_UTC <  '2013-10-27 02:00:00') then dateadd(hour, 1, EVENT_START_DATE_TIME_UTC)    -- Mar 13-Oct 13 => DST, add 1 hour to UTC
																when (EVENT_START_DATE_TIME_UTC <  '2014-03-30 01:00:00') then EVENT_START_DATE_TIME_UTC                      -- Oct 13-Mar 14 => UTC = Local
																		else NULL                                                                                                   -- the scrippt will have to be updated past Mar 2014
														end)                            as event_start_day
		,datepart(hour,
												case
																when (EVENT_START_DATE_TIME_UTC <  '2012-03-25 01:00:00') then EVENT_START_DATE_TIME_UTC                      -- prior Mar 12 - no change, consider UTC = local
																when (EVENT_START_DATE_TIME_UTC <  '2012-10-28 02:00:00') then dateadd(hour, 1, EVENT_START_DATE_TIME_UTC)    -- Mar 12-Oct 12 => DST, add 1 hour to UTC (http://www.timeanddate.com/worldclock/timezone.html?n=136)
																when (EVENT_START_DATE_TIME_UTC <  '2013-03-31 01:00:00') then EVENT_START_DATE_TIME_UTC                      -- Oct 12-Mar 13 => UTC = Local
																when (EVENT_START_DATE_TIME_UTC <  '2013-10-27 02:00:00') then dateadd(hour, 1, EVENT_START_DATE_TIME_UTC)    -- Mar 13-Oct 13 => DST, add 1 hour to UTC
																when (EVENT_START_DATE_TIME_UTC <  '2014-03-30 01:00:00') then EVENT_START_DATE_TIME_UTC                      -- Oct 13-Mar 14 => UTC = Local
																		else NULL                                                                                                   -- the scrippt will have to be updated past Mar 2014
														end)                            as event_start_hour */
	from Capping2_00_Raw_Uncapped_Events
	where
			live_recorded in ('LIVE','RECORDED')
		and Duration > @min_view_duration_sec                             -- Maintain minimum event duration
		and INSTANCE_START_DATE_TIME_UTC < INSTANCE_END_DATE_TIME_UTC     -- Remove 0sec instances
		-- and Panel_id = 12                                              -- THIS IS SUPPOSED TO HANDLED IN THE INPUT TABLE
		-- and type_of_viewing_event <> 'Non viewing event'               -- THIS IS SUPPOSED TO HANDLED IN THE INPUT TABLE
		-- and type_of_viewing_event is not null                          -- THIS IS SUPPOSED TO HANDLED IN THE INPUT TABLE
		and DK_BROADCAST_START_DATEHOUR_DIM >= @varBroadcastMinDate
		and account_number is not null
		and DK_EVENT_START_DATEHOUR_DIM >= @varEventStartHour             -- Start with 2300 hours on the previous day to pick UTC records in DST time (DST = UTC + 1 between April & October)
		and DK_EVENT_START_DATEHOUR_DIM <= @varEventEndHour               -- End up with additional records for the next day, up to 04:00am
		and subscriber_id is not null                                     -- There shouldnt be any nulls, but there are
		and BROADCAST_START_DATE_TIME_UTC is not null
	commit --;-- ^^ originally a commit

	-- Start off the control totals:
	if object_id('Capping2_tmp_Uncleansed_Viewing_Totals') is not null drop table Capping2_tmp_Uncleansed_Viewing_Totals
	commit --; --^^ to be removed

	select
			subscriber_id
		,	round(sum(datediff(ss, X_Viewing_Start_Time, X_Viewing_End_Time)) / 60.0, 0)	as	total_box_viewing
	into	Capping2_tmp_Uncleansed_Viewing_Totals
	from	Capping2_01_Viewing_Records
	where	daily_table_date	=	@target_date
	group by	subscriber_id
	commit --;-- ^^ originally a commit

	delete from	CP2_QA_daily_average_viewing
	where	build_date	=	@target_date
	commit --;-- ^^ originally a commit

	insert into	CP2_QA_daily_average_viewing	(
														build_date
													,	subscriber_count
													,	average_uncleansed_viewing
												)
	select
			@target_date
		,	count(1)
		,	avg(total_box_viewing)
	from	Capping2_tmp_Uncleansed_Viewing_Totals
	commit --;-- ^^ originally a commit

	if object_id('Capping2_tmp_Uncleansed_Viewing_Totals') is not null drop table Capping2_tmp_Uncleansed_Viewing_Totals
	commit --;-- ^^ originally a commit


	set @QA_catcher = -1
	commit --; --^^ to be removed

	select	@QA_catcher = count(1)
	from	Capping2_01_Viewing_Records
	commit --; --^^ to be removed

	execute M00_2_output_to_logger  'B01: Extract raw viewing completed' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	--select 'start C', now(); --^^ just for debug


	-------------------------------------------------------------------------------------------------
	-- SECTION C: CREATE THE CAPS
	-------------------------------------------------------------------------------------------------

	-------------------------------------------------------------------------------------------------
	-- C01) ASSEMBLE REQUIRED DAILY VIEWING DATA
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : C01) ASSEMBLE REQUIRED DAILY VIEWING DATA'
	COMMIT

	if object_id('Capping2_tmp_View_dupe_Culling_1') is not null drop table Capping2_tmp_View_dupe_Culling_1
	commit --; --^^ to be removed

	-- First off: Kick out the duplicates out that come in from the weird day wrapping stuff
	select
			subscriber_id
		,	adjusted_event_start_time
		,	X_Viewing_Start_Time
		,	min(ID_Key)				as Min_ID_Key
	into	Capping2_tmp_View_dupe_Culling_1
	from	Capping2_01_Viewing_Records
	group by
			subscriber_id
		,	adjusted_event_start_time
		,	X_Viewing_Start_Time
	commit --;-- ^^ originally a commit

	create unique index idx1 on Capping2_tmp_View_dupe_Culling_1 (Min_ID_Key)
	commit --;-- ^^ originally a commit

			-- Delete records with non-existing ID_Key in the deduped table
	delete from	Capping2_01_Viewing_Records
	from
					Capping2_01_Viewing_Records			a
		left join	Capping2_tmp_View_dupe_Culling_1	b	on	a.ID_Key	=	b.Min_ID_Key
	where b.Min_ID_Key is null
	commit --;-- ^^ originally a commit


	-- For logging and flagging and QA and stuff:

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from Capping2_tmp_View_dupe_Culling_1
	commit --; --^^ to be removed

	execute M00_2_output_to_logger  'C01: Midway 1/3 (Deduplication)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	if object_id('Capping2_tmp_View_dupe_Culling_1') is not null drop table Capping2_tmp_View_dupe_Culling_1
	commit --;-- ^^ originally a commit

	-- Deduplication complete!
	if object_id('Capping2_tmp_Cumulative_Playback_Corrections') is not null drop table Capping2_tmp_Cumulative_Playback_Corrections
	commit --; --^^ to be removed
	
	if object_id('Capping2_tmp_Cumulative_Playback_Corrections_2') is not null drop table Capping2_tmp_Cumulative_Playback_Corrections_2
	commit --; --^^ to be removed

	-- Need to fix the broken viewing times for the playback records: Still debugging this section ########
	select
			cb_row_id
		,	subscriber_id
		,	adjusted_event_start_time
		,	x_programme_viewed_duration
		,	rank()	over	(
								partition by
										subscriber_id
									,	adjusted_event_start_time
								order by
										x_viewing_start_time
									,	tx_start_datetime_utc	desc
									,	x_viewing_end_time
									,	cb_row_id
							)	as	sequencer
		-- We've got one nasty duplicate thing in here; see cb_row_id 6463255125634728347 vs 6463255125650111897
		-- on the 11th of February: same subscriber ID and event start time, different end times, stuff like
		-- that shouldn't happen. Probably ordering by viewing end time is going to do terrible things to the
		-- viewing data (given it's for a correction in the playback sequence) but whatever, if it's a big deal
		-- the unit tests will catch it, and right now we just want something to run. If this doesn't work, we'll
		-- just clip everything from the conflicting events out of the data set, the loss won't be big at all.
	into	Capping2_tmp_Cumulative_Playback_Corrections
	from	Capping2_01_Viewing_Records
	where	live	=	0
	commit --;-- ^^ originally a commit

	-- Slight annoyance: Sybase won't let you order by anything other than numeric
	-- fields, so we still need this funny in-between table...
	create unique index sequencing_key on Capping2_tmp_Cumulative_Playback_Corrections (subscriber_id, adjusted_event_start_time, sequencer)
	commit --;-- ^^ originally a commit

	select
			cb_row_ID
		,	cast	(
						sum(x_programme_viewed_duration)	over	(
																		partition by
																				subscriber_id
																			,	adjusted_event_start_time
																		order by	sequencer
																	)	as	int
					)	as	x_cumul_programme_viewed_duration 	--Jon - this is the crazy thing where Sybase didn't like this field because it was an
																--integer-expression, so we have to convert it to an integer
	into	Capping2_tmp_Cumulative_Playback_Corrections_2
	from	Capping2_tmp_Cumulative_Playback_Corrections
	commit --;-- ^^ originally a commit

	create unique index fake_pk on Capping2_tmp_Cumulative_Playback_Corrections_2 (cb_row_ID)
	commit --; --^^ to be removed

	-- Push those back into the viewing table...
	update	Capping2_01_Viewing_Records
	set
			x_viewing_end_time	=	dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
		,	x_viewing_start_time	=	dateadd(second,-x_programme_viewed_duration,dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time))
	from
					Capping2_01_Viewing_Records
		inner join	Capping2_tmp_Cumulative_Playback_Corrections_2	as	cpc2	on	Capping2_01_Viewing_Records.cb_row_id	=	cpc2.cb_row_id
	commit --; --^^ to be removed

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from Capping2_tmp_Cumulative_Playback_Corrections_2
	commit --; --^^ to be removed


	IF object_id('Capping2_tmp_Cumulative_Playback_Corrections') is not null drop table Capping2_tmp_Cumulative_Playback_Corrections
	commit --; --^^ to be removed
	IF object_id('Capping2_tmp_Cumulative_Playback_Corrections_2') is not null drop table Capping2_tmp_Cumulative_Playback_Corrections_2
	commit --; --^^ to be removed


	execute M00_2_output_to_logger  'C01: Midway 2/3 (Patch durations)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-- Finally, eliminate these "illegitimate" viewing records:

	--remove illegitimate playback views - these views are those that start on event end time and go beyond event end time
	delete from Capping2_01_Viewing_Records
	where X_Adjusted_Event_End_Time<x_viewing_end_time
	and x_viewing_start_time>=X_Adjusted_Event_End_Time
	commit --; --^^ to be removed
	-- Small, tiny, minisclue proportion of stuff...
	--2,596,912, 30m left
	--reset x_viewing_end_times for playback views
	
	update	Capping2_01_Viewing_Records
	set	x_viewing_end_time=X_Adjusted_Event_End_Time
	where
			X_Adjusted_Event_End_Time	<	x_viewing_end_time
		and	x_viewing_start_time		<	X_Adjusted_Event_End_Time
	commit --;-- ^^ originally a commit
	-- similarly tiny proportion.

	
	-- That table "Capping2_01_Viewing_Records" is the one that we take as our ball of viewing data.
	-- Okay, so we want some basic counts of total events and things like that, maybe even a
	-- profile of event duration distribution...
	if object_id('Capping2_tmp_Uncapped_Viewing_Totals') is not null drop table Capping2_tmp_Uncapped_Viewing_Totals
	commit --; --^^ to be removed

	select
			subscriber_id
		,	round(sum(datediff(ss, X_Viewing_Start_Time, X_Viewing_End_Time)) / 60.0, 0)	as total_box_viewing
	into	Capping2_tmp_Uncapped_Viewing_Totals
	from	Capping2_01_Viewing_Records
	where	daily_table_date	=	@target_date
	group by	subscriber_id
	commit --;-- ^^ originally a commit

	select @QA_catcher = avg(total_box_viewing)
	from Capping2_tmp_Uncapped_Viewing_Totals
	commit --;-- ^^ originally a commit

	update	CP2_QA_daily_average_viewing
	set	average_uncapped_viewing	=	@QA_catcher
	where	build_date	=	@target_date
	commit --;-- ^^ originally a commit

	if object_id('Capping2_tmp_Uncapped_Viewing_Totals') is not null drop table Capping2_tmp_Uncapped_Viewing_Totals
	commit --; --^^ to be removed

	-- We've got a bunch of instances where we clear any current control totals out of the QA tables
	-- so that there's some safety against re-running the procedure on the sameday.
	delete from CP2_QA_viewing_control_totals
	where build_date = @target_date

	commit --;-- ^^ originally a commit

	insert into CP2_QA_viewing_control_totals
	select
			@target_date
		,	convert(varchar(20), '1.) Collect')
		,	program_air_date
		,	live
		,	genre
		,	count(1)	as viewing_records
		,	round(sum(coalesce(datediff(second, X_Viewing_Start_Time, X_Viewing_End_Time),0)) / 60.0 / 60 / 24.0, 2)
	from	Capping2_01_Viewing_Records
	group by
			program_air_date
		,	live
		,	genre
	commit --;-- ^^ originally a commit

	-- Distribution of event profiles will get done later...

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from Capping2_01_Viewing_Records
	commit --; --^^ to be removed

	execute M00_2_output_to_logger  'C01: Complete! (Data cleansing)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- C02) CONDENSE VIEWING DATA INTO LISTING OF EVENTS
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : C02) CONDENSE VIEWING DATA INTO LISTING OF EVENTS'
	COMMIT

	-- One Week is getting turned into CP2_event_listing...
	--IF object_id('one_week') is not null drop table one_week
	if object_id('CP2_event_listing') is not null drop table CP2_event_listing
	commit --; --^^ to be removed

	create table CP2_event_listing	(
											Subscriber_Id                      integer         not null
										,	account_number                     varchar(20)     not null
										,	fake_cb_row_id                     bigint          not null    -- we just need it to break some ties it'll still be unique
										,	X_Type_Of_Viewing_Event            varchar(40)     not null
										,	Adjusted_Event_Start_Time          datetime        not null
										,	X_Adjusted_Event_End_Time          datetime
										,	event_start_hour                   tinyint
										,	event_start_day                    tinyint
										,	X_Event_Duration                   decimal(10,0)
										,	event_dur_mins                     integer
										,	live                               bit
										,	initial_genre                      varchar(25)
										,	initial_sub_genre                  varchar(25)
										,	initial_channel_name               varchar(30)     -- This guy gets populated as uppercase and trimmed
										,	Initial_Service_Key                bigint
										,	program_air_date                   date
										,	program_air_datetime               datetime
										,	num_views                          int
										,	num_genre                          int
										,	num_sub_genre                      int
										,	viewed_duration                    int

										-- These guys are a channel categorisation lookup
										,	pack                               varchar(100)
										,	pack_grp                           varchar(30)
										-- We also use P/S box flags:
										,	box_subscription                   varchar(1)

										-- Columns used in applying caps:
										,	bucket_id                          integer         -- Composite lookup for: event_start_hour, event_start_day, initial_channel_name, Live
										,	max_dur_mins                       int             default null
										,	capped_event                       bit             default 0

										-- Yeah, structure is always good:
										,	primary key (Subscriber_Id, Adjusted_Event_Start_Time) -- So we... *shouldn't* have any more than one event starting at the same time per box... might have to manage some deduplication...
									)
	-- We'll also need indices on this guy...
	commit --; --^^ to be removed

	create index    for_joins           on CP2_event_listing (account_number)
	commit --; --^^ to be removed
	create index    start_time_index    on CP2_event_listing (Adjusted_Event_Start_Time)
	commit --; --^^ to be removed
	create index    init_channel_index  on CP2_event_listing (initial_channel_name)
	commit --; --^^ to be removed
	create index for_the_joining_group  on CP2_event_listing (event_start_hour, event_start_day, initial_genre, box_subscription, pack_grp, Live)
	commit --; --^^ to be removed
	create index by_bucket_index        on CP2_event_listing (bucket_id, pack_grp, box_subscription)
	commit --;-- ^^ originally a commit

	--obtain event view
	insert into CP2_event_listing	(
											Subscriber_Id
										,	account_number
										,	fake_cb_row_id
										,	Adjusted_Event_Start_Time
										,	X_Type_Of_Viewing_Event
										,	X_Adjusted_Event_End_Time
										,	X_Event_Duration
										,	event_start_hour
										,	event_start_day
										,	Live
										,	num_views
										,	num_genre
										,	num_sub_genre
										,	viewed_duration
										,	event_dur_mins
										,	pack_grp
										,	box_subscription
										,	bucket_id
										,	initial_genre
										,	initial_sub_genre
										,	initial_channel_name
										,	Initial_Service_Key
									)
	select
			Subscriber_Id
		,	min(account_number)            -- should be unique given the subscriber_id
		,	min(cb_row_id)                 -- we just need something unique to break some ties
		,	Adjusted_Event_Start_Time
		,	min(X_Type_Of_Viewing_Event)   -- should also be determined give nsubscriber ID and Adjusted_Event_Start_Time
		,	min(X_Adjusted_Event_End_Time) --
		,	min(X_Event_Duration)          --
		,	min(event_start_hour)          --
		,	min(event_start_day)           --
		,	min(Live)                      -- All these min(.) values should be determined by
		,	count(1)
		,	count(distinct genre)
		,	count(distinct sub_genre)
		,	sum(x_programme_viewed_duration)
		,	cast(min(x_event_duration) / 60 as int)
		-- Other columns we have to specifically mention because Sybase can't handle defaults going into compound indices
		,	null                           -- pack_grp needs it
		,	null                           -- same with box_subscription
		,	null                           -- and bucket_id
		-- So trying to update all the events with initial genre / channel goes badly, so
		-- we're going to hack in a guess here which will probably be wrong for every event
		-- with num_views>=2 but it does mean we don't have to update records with num_views=1
		-- and that might help us dodge the temp space errors we're getting. Maybe :-/
		,	min(genre)
		,	min(sub_genre)
		,	upper(trim(min(channel_name)))
		,	min(Service_Key)
	from	Capping2_01_Viewing_Records
	group by
			Subscriber_Id
		,	Adjusted_Event_Start_Time
	commit --;-- ^^ originally a commit

	-- OK, event listing is now build.

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_event_listing
	commit --; --^^ to be removed

	execute M00_2_output_to_logger  'C02: Complete! (Event listing)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- C03.1) ATTACH METADATA: GENRE AT EVENT START
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : C03.1) ATTACH METADATA: GENRE AT EVENT START'
	COMMIT

	IF object_id('CP2_First_Programmes_In_Event') IS NOT NULL DROP TABLE CP2_First_Programmes_In_Event
	commit --; --^^ to be removed

	create table CP2_First_Programmes_In_Event	(
														subscriber_id                       integer         not null
													,	adjusted_event_start_time          datetime        not null
													-- For the genre assignement bits:
													,	genre                              varchar(25)
													,	sub_genre                          varchar(25)
													,	channel_name                       varchar(30)
													,	Service_Key                        bigint
													-- Things needed to assign caps to end of first program viewed (sectino C02.e)
													,	X_Adjusted_Event_End_Time          datetime
													,	x_viewing_end_time                 datetime
													,	sequencer                          integer         -- only needed for deduplication
													,	primary key	(
																			subscriber_id
																		,	adjusted_event_start_time
																		,	sequencer
																	)
												)
	commit --; --^^ to be removed

	-- Build table for first viewing record in each event
	insert into CP2_First_Programmes_In_Event
	select
		-- OK, so we're clipping CP2_First_Programmes_In_Event down to things that actually get referenced:
			subscriber_id
		,	adjusted_event_start_time
		,	genre
		,	sub_genre
		,	channel_name
		,	Service_Key
		-- Things needed to assign caps to end of first program viewed (sectino C02.e)
		,	X_Adjusted_Event_End_Time
		,	x_viewing_end_time
		,	rank() over	(
							partition by
									subscriber_id
								,	adjusted_event_start_time
							order by
									x_viewing_start_time
								,	cb_row_id desc
						)
	from	Capping2_01_Viewing_Records
	commit --;-- ^^ originally a commit
	--34723382
	-- delete all records which aren't necessary due to trank
	delete from	CP2_First_Programmes_In_Event
	where	sequencer	<>	1
	commit --;-- ^^ originally a commit

	/* Kept for reference while still in dev: we changed a few of the names to more sensible things
	--add channel, genre and sub genre
	alter table one_week                            -- => "CP2_event_listing"
	add genre_at_event_start_time varchar(30),      -- => "CP2_event_listing.initial_genre"
	add sub_genre_at_event_start_time varchar(30),  -- => "CP2_event_listing.initial_sub_genre"
	add channel_at_event_start_time varchar(30),    -- => "CP2_event_listing.initial_channel_name"
	add pack varchar(100) default null,             -- => "CP2_event_listing.pack"
	add pack_grp varchar(20) default null;          -- => "CP2_event_listing.pack_grp"
	*/

	-- Okay, so doing the full update gives us errors, so we're again going to roll out the trick
	-- that we only need to worry about updating the records that aren't first in event.
	commit --; --^^ to be removed
	update	CP2_event_listing
	set
			initial_genre          = fpie.genre
		,	initial_sub_genre      = fpie.sub_genre
		,	Initial_Service_Key    = fpie.Service_Key
	from
					CP2_event_listing
		inner join	CP2_First_Programmes_In_Event	as	fpie	on	CP2_event_listing.subscriber_id				=	fpie.subscriber_id
																and	CP2_event_listing.adjusted_event_start_time	=	fpie.adjusted_event_start_time
	where	CP2_event_listing.num_views	>	1
	-- Awesome, temp space issue averted. Going to have to keep an eye on that, and generally
	-- try to amange the "first item in event" things like this...
	commit --;-- ^^ originally a commit

	-- After that, the table still gets used in a later section when we might
	-- opt to cap the event to end of first viewing record (ie end of first
	-- programme)

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_event_listing
	where Initial_Service_Key is not null
	commit --; --^^ to be removed

	execute M00_2_output_to_logger  'C03.1: Complete! (Genre at start)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- C03.2) ATTACH METADATA: CHANNEL PACKS & PACK GROUPINGS
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : C03.2) ATTACH METADATA: CHANNEL PACKS & PACK GROUPINGS'
	COMMIT

	--add pack & network [Update: Network no longer in play]
	update	CP2_event_listing base
	set	base.pack	=	trim(cl.channel_pack)
	from	Vespa_Analysts.Channel_Map_Prod_Service_Key_Attributes as cl
	where base.Initial_Service_Key	=	cl.Service_Key
	commit --;-- ^^ originally a commit
	-- Would be much better to update the channel lookup, but hey...

	--add pack groups
	update CP2_event_listing
	set pack_grp = coalesce(pack, 'Other')
	from CP2_event_listing
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_event_listing
	where pack_grp <> 'Other'
	commit --; --^^ to be removed

	execute M00_2_output_to_logger  'C03.2: Complete! (Pack grouping)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- C03.3) ATTACH METADATA: PRIMARY / SECONDARY BOX
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : C03.3) ATTACH METADATA: PRIMARY / SECONDARY BOX'
	COMMIT

	-- Yeah, in the new build we just pull the reference in from the weekly profiling build...
	update CP2_event_listing
	set CP2_event_listing.box_subscription = bl.PS_flag
	from CP2_box_lookup as bl
	where CP2_event_listing.subscriber_id = bl.subscriber_id
	-- That's much easier than going back to the customer database for each day separately
	commit --;-- ^^ originally a commit
	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_event_listing
	where box_subscription in ('P', 'S')
	commit --; --^^ to be removed

	execute M00_2_output_to_logger  'C03.3: Complete! (Primary / secondary box)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- C03.4) ATTACH METADATA: CAPPING BUCKET ID
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : C03.4) ATTACH METADATA: CAPPING BUCKET ID'
	COMMIT

	-- This guy is a composite key that summarises event_start_hour, event_start_day, initial_genre
	-- and live into one integer that's easy to use (/index/join).
	delete from CP2_capping_buckets
	commit --;-- ^^ originally a commit

	insert into CP2_capping_buckets (
											event_start_hour
										,	event_start_day
										,	initial_genre
										,	live
									)
	select
			event_start_hour
		,	event_start_day
		,	initial_genre
		,	live
	from CP2_event_listing
	group by
			event_start_hour
		,	event_start_day
		,	initial_genre
		,	live
	commit --;-- ^^ originally a commit

	-- Push the bucket keys back onto the event listings:

	update	CP2_event_listing
	set		CP2_event_listing.bucket_id	=	cb.bucket_id
	from
					CP2_event_listing
		inner join	CP2_capping_buckets	as	cb	on  CP2_event_listing.event_start_hour = cb.event_start_hour
												and CP2_event_listing.event_start_day  = cb.event_start_day
												and CP2_event_listing.initial_genre    = cb.initial_genre
												and CP2_event_listing.live             = cb.live
	commit --;-- ^^ originally a commit

	-- We'll put bucket_id on the viewing data in section D01 after we've put the
	-- metadata there too.

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_event_listing
	where bucket_id is not null
	commit --; --^^ to be removed

	execute M00_2_output_to_logger  'C03.4: Complete! (Bucket IDs)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- C04) BUILDING N-TILES FOR CAPPING
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : C04) BUILDING N-TILES FOR CAPPING'
	COMMIT

	-- OK so from here I might change the strategy and not mess around that much with how these
	-- structures actually work (besides plugging them into the appropriately normalised structures).
	-- I'm 600 lines through with twice as much to go, need to switch up the approach if we're to
	-- have any hope of actually getting this completed. Here's a list of tables we renamed so as to
	-- offer at leaast *some* clarity as to what's going on, or at least, which tables are owned and
	-- used by the Capping 2 process:
	--      ntiles_week     -> CP2_ntiles_week
	--      nt_4_19         -> CP2_nt_4_19
	--      nt_20_3         -> CP2_nt_20_3
	--      nt_lp           -> CP2_nt_lp
	--      week_caps       -> CP2_calculated_viewing_caps
	--      h23_3           -> CP2_h23_3
	--      h4_14           -> CP2_h4_14
	--      h15_19          -> CP2_h15_19
	--      h20_22          -> CP2_h20_22
	--      lp              -> CP2_lp
	-- Other tables which are still floating around but will probably get killed when we normalise
	-- things out:
	--      CP2_QA_viewing_control_cap_distrib
	--      all_events              <- we already have CP2_event_listing, don't think we need this table. Update: it's GONE
	--      uncapped                => CP2_uncapped_events_lookup
	--      capped_events           => CP2_capped_events_lookup
	--      capped_events2          => CP2_capped_events_with_endpoints
	--      batching_lookup         -- depreciated - well, kind of replaced with CP2_capping_buckets, though buckets do different things
	--

	-- So yeah, this bit is super not-safe for accidental multiple runs. One might drop the tables
	-- the other is still using, and then it all goes way south.

	if object_id('CP2_ntiles_week') is not null drop table CP2_ntiles_week
	commit --; --^^ to be removed

	--calculate ntiles for caps
	select
			Live
		,	cast(adjusted_event_start_time as date) as event_date -- do we need this now we're processing one day at a time?
		,	event_start_day
		,	event_start_hour
		,	box_subscription
		,	pack_grp
		,	initial_genre
		,	event_dur_mins
		,	ntile(200) over	(
								partition by
										Live
									,	event_start_day
								order by	x_event_duration
							)										as	ntile_lp
		,	ntile(200) over	(
								partition by
										Live
									,	event_start_day
									,	event_start_hour
									,	box_subscription
									,	pack_grp
									,	initial_genre
								order by	x_event_duration
							)										as	ntile_1	-- incl. partition by box_subscription
		,	ntile(200) over (
								partition by
										Live
									,	event_start_day
									,	event_start_hour
									,	pack_grp
									,	initial_genre 
								order by	x_event_duration
							)										as	ntile_2	-- excl. partition by box_subscription
		,	x_event_duration
		,	viewed_duration
		,	num_views
	into	CP2_ntiles_week
	from	CP2_event_listing
	where	x_event_duration	<	86400 -- 86400 seconds in a day or something
	commit --;-- ^^ originally a commit

	-- Wait... One_week doesn't get used past this? does the CP2_event_listing get used past this?
	-- Not yet, heh, this is the last use of it... okey... looks like we'll be able to either trim
	-- some stuff out or renormalise some things or generally tidy up...


	--create indexes
	create hng index idx1 on CP2_ntiles_week(event_start_day)
	commit --; --^^ to be removed
	create hng index idx2 on CP2_ntiles_week(event_start_hour)
	commit --; --^^ to be removed
	--create hng index idx3 on CP2_ntiles_week(Live); -- Not any more, not now it's a bit
	create hng index idx4 on CP2_ntiles_week(box_subscription)
	commit --; --^^ to be removed
	create hng index idx5 on CP2_ntiles_week(pack_grp)
	commit --; --^^ to be removed
	create hng index idx6 on CP2_ntiles_week(initial_genre)
	commit --;-- ^^ originally a commit
	-- Which of these do we need? which ones are actually helping?


	--select distinct event_date,event_start_day from CP2_ntiles_week

	--check data
	--select count(*),sum(num_views) from CP2_ntiles_week
	--count(*)        sum(CP2_ntiles_week.num_views)
	--25928067        34274204

	--select count(*),sum(num_views) from one_week where band_dur_days = 0
	--25928067        34274204

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_ntiles_week
	commit --; --^^ to be removed

	execute M00_2_output_to_logger  'C04: Complete! (n-Tile generation)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- C05) TABLES OF N-TILES (?)
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : C05) TABLES OF N-TILES (?)'
	COMMIT

	-- All the different caps tables for the different regimes of capping

	--create capping limits for start hours 4-19
	if object_id('CP2_nt_4_19') is not null drop table CP2_nt_4_19
	commit --; --^^ to be removed

	SELECT
			Live
		,	event_date
		,	event_start_day
		,	event_start_hour
		,	box_subscription
		,	pack_grp
		,	initial_genre
		,	ntile_1
		,	min(event_dur_mins) as min_dur_mins
		,	max(event_dur_mins) as max_dur_mins
		,	PERCENTILE_disc(0.5) WITHIN GROUP (ORDER BY event_dur_mins) as median_dur_mins
		,	count(*) as num_events
		,	sum(num_views) as tot_views
		,	sum(x_event_duration) as event_duration
		,	sum(viewed_duration) as viewed_duration
	into	CP2_nt_4_19
	FROM	CP2_ntiles_week
	where
			event_start_hour	>=	4
		and event_start_hour	<=	19
		and	Live				=	1
	group by
			Live
		,	event_date
		,	event_start_day
		,	event_start_hour
		,	box_subscription
		,	pack_grp
		,	initial_genre
		,	ntile_1
	commit --;-- ^^ originally a commit

	--create indexes
	create hng index idx1 on CP2_nt_4_19(event_start_day)
	commit --; --^^ to be removed
	create hng index idx2 on CP2_nt_4_19(event_start_hour)
	commit --; --^^ to be removed
	create hng index idx4 on CP2_nt_4_19(box_subscription)
	commit --; --^^ to be removed
	create hng index idx5 on CP2_nt_4_19(pack_grp)
	commit --; --^^ to be removed
	create hng index idx6 on CP2_nt_4_19(initial_genre)

	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_nt_4_19
	commit --; --^^ to be removed

	execute M00_2_output_to_logger  'C05: Midway 1/3 (_nt_4_19)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	--create capping limits start hours 20-3
	if object_id('CP2_nt_20_3') is not null drop table CP2_nt_20_3
	commit --; --^^ to be removed

	SELECT
			Live
		,	event_date
		,	event_start_day
		,	event_start_hour
		,	box_subscription
		,	pack_grp
		,	initial_genre
		,	ntile_2
		,	min(event_dur_mins) as min_dur_mins
		,	max(event_dur_mins) as max_dur_mins
		,	PERCENTILE_disc(0.5) WITHIN GROUP (ORDER BY event_dur_mins) as median_dur_mins
		,	count(*) as num_events
		,	sum(num_views) as tot_views
		,	sum(x_event_duration) as event_duration
		,	sum(viewed_duration) as viewed_duration
	into	CP2_nt_20_3
	FROM	CP2_ntiles_week
	where
			event_start_hour	in (20,21,22,23,0,1,2,3)
		and	Live				=	1
	group by
			Live
		,	event_date
		,	event_start_day
		,	event_start_hour
		,	box_subscription
		,	pack_grp
		,	initial_genre
		,	ntile_2
	commit --;-- ^^ originally a commit

	--create indexes
	create hng index idx1 on CP2_nt_20_3(event_start_day)
	commit --; --^^ to be removed
	create hng index idx2 on CP2_nt_20_3(event_start_hour)
	commit --; --^^ to be removed
	create hng index idx4 on CP2_nt_20_3(pack_grp)
	commit --; --^^ to be removed
	create hng index idx5 on CP2_nt_20_3(initial_genre)
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_nt_20_3
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'C05: Midway 2/3 (_nt_20_3)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-----------------------------
	-- --create capping limits for all live events
	-- execute drop_local_table 'CP2_nt_live'
	-- commit --; --^^ to be removed
	
	-- SELECT
			-- Live
		-- ,	event_date
		-- ,	event_start_day
		-- ,	event_start_hour
		-- ,	box_subscription
		-- ,	pack_grp
		-- ,	initial_genre
		-- ,	case
				-- when	event_start_hour	between	4 and 19	then	ntile_1
				-- else													ntile_2
			-- end															as ntile_live
		-- ,	min(event_dur_mins)											as min_dur_mins
		-- ,	max(event_dur_mins)											as max_dur_mins
		-- ,	PERCENTILE_disc(0.5) WITHIN GROUP (ORDER BY event_dur_mins)	as median_dur_mins
		-- ,	count(*)													as num_events
		-- ,	sum(num_views)												as tot_views
		-- ,	sum(x_event_duration)										as event_duration
		-- ,	sum(viewed_duration)										as viewed_duration
	-- into	CP2_nt_live
	-- FROM	CP2_ntiles_week
	-- where	Live	=	1
	-- group by
			-- Live
		-- ,	event_date
		-- ,	event_start_day
		-- ,	event_start_hour
		-- ,	box_subscription
		-- ,	pack_grp
		-- ,	initial_genre
		-- ,	ntile_live
	-- commit --;-- ^^ originally a commit

	-- --create indexes
	-- create hng index idx1 on CP2_nt_live(event_start_day)
	-- commit --; --^^ to be removed
	-- create hng index idx2 on CP2_nt_live(event_start_hour)
	-- commit --; --^^ to be removed
	-- create hng index idx4 on CP2_nt_live(box_subscription)
	-- commit --; --^^ to be removed
	-- create hng index idx5 on CP2_nt_live(pack_grp)
	-- commit --; --^^ to be removed
	-- create hng index idx6 on CP2_nt_live(initial_genre)

	-- commit --;-- ^^ originally a commit
	-----------------------------



	--create capping limits for playback
	if object_id('CP2_nt_lp') is not null drop table CP2_nt_lp
	commit --; --^^ to be removed

	SELECT
			Live
		,	event_start_day
		,	ntile_lp
		,	min(event_dur_mins) as min_dur_mins
		,	max(event_dur_mins) as max_dur_mins
		,	PERCENTILE_disc(0.5) WITHIN GROUP (ORDER BY event_dur_mins) as median_dur_mins
		,	count(*) as num_events
		,	sum(num_views) as tot_views
		,	sum(x_event_duration) as event_duration
		,	sum(viewed_duration) as viewed_duration
	into	CP2_nt_lp
	FROM	CP2_ntiles_week
	where	Live	=	0
	group by
			Live
		,	event_start_day
		,	ntile_lp
	commit --;-- ^^ originally a commit

	--create indexes
	create hng index idx1 on CP2_nt_lp(event_start_day)
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_nt_lp
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'C05: Complete! (_nt_lp)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- C06) ALL KINDS OF DIFFERENT CAPPING TABLES
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : C06) ALL KINDS OF DIFFERENT CAPPING TABLES'
	COMMIT

	/*create or replace variable*/ declare @median_dur_min int commit
	/*create or replace variable*/ declare @threshold_nontile int commit
	/*create or replace variable*/ declare @threshold_ntile int commit
	/*create or replace variable*/ declare @playback_ntile int commit
	/*create or replace variable*/ declare @startTime time commit
	/*create or replace variable*/ declare @endTime time commit
	/*create or replace variable*/ declare @startTime1 time commit -- used when there are two hour ranges
	/*create or replace variable*/ declare @endTime1 time commit
	/*create or replace variable*/ declare @startTime2 time commit -- used when there are two hour ranges
	/*create or replace variable*/ declare @endTime2 time commit

	-----------------------------
	

	--obtain max cap limits for live events

	------------------------------------------------------------------------------------------
	-- execute drop_local_table 'CP2_h_all'
	-- commit

	-- --identify ntile threshold for ALL event start hours
	-- select
			-- Live
		-- ,	event_date
		-- ,	event_start_day
		-- ,	event_start_hour
		-- ,	case
				-- when	event_start_hour	between	4 and 19	then	NT.box_subscription
				-- else													NULL
			-- end						as	box_subscription
		-- ,	pack_grp
		-- ,	initial_genre
		-- ,	min	(
					-- case
						-- when	median_dur_mins	>=	MET.BOX_SHUT_DOWN			then	ntile_live
						-- else													null
					-- end
				-- )					as	pri_ntile
		-- ,	max(ntile_live)			as	sec_ntile
		-- ,	case
				-- when	pri_ntile is null	then	sec_ntile - MET.THRESHOLD_NONTILE
				-- else								pri_ntile - MET.THRESHOLD_NTILE
			-- end						as	cap_ntile
		-- ,	cast(null as integer)	as	min_dur_mins
	-- into	CP2_h_all
	-- from
					-- CP2_nt_live		AS	NT
		-- inner join	(
                	-- select
                            -- START_TIME
                        -- ,   END_TIME
                        -- ,   BANK_HOLIDAY_WEEKEND
                        -- ,   COMMON_PARAMETER_GROUP
                        -- ,	max(BOX_SHUT_DOWN)		OVER (PARTITION BY BANK_HOLIDAY_WEEKEND, COMMON_PARAMETER_GROUP) AS	BOX_SHUT_DOWN
                        -- ,	max(THRESHOLD_NONTILE)	OVER (PARTITION BY BANK_HOLIDAY_WEEKEND, COMMON_PARAMETER_GROUP) AS	THRESHOLD_NONTILE
                    	-- ,	max(THRESHOLD_NTILE)	OVER (PARTITION BY BANK_HOLIDAY_WEEKEND, COMMON_PARAMETER_GROUP) AS	THRESHOLD_NTILE
						-- ,	EFFECTIVE_FROM
						-- ,	EFFECTIVE_TO
                	-- from	CP2_metadata_table
					-- )				AS	MET		ON		NT.EVENT_START_HOUR			BETWEEN	DATEPART(HOUR,MET.START_TIME)
																					-- AND		DATEPART(HOUR,MET.END_TIME)
												-- AND     NT.event_date		  		BETWEEN MET.EFFECTIVE_FROM
																					-- AND     MET.EFFECTIVE_TO
												-- AND		MET.BANK_HOLIDAY_WEEKEND	=		@targetDateIsWeekend
	-- where	Live	=	1
	-- group by
			-- Live
		-- ,	event_date
		-- ,	event_start_day
		-- ,	event_start_hour
		-- ,	box_subscription
		-- ,	pack_grp
		-- ,	initial_genre
		-- ,	MET.THRESHOLD_NONTILE
		-- ,	MET.THRESHOLD_NTILE
	-- commit --;-- ^^ originally a commit

	-- update	CP2_h_all t1
	-- set		min_dur_mins	=	t2.min_dur_mins
	-- from	CP2_nt_live t2
	-- where
			-- t1.Live				=	t2.Live
		-- and t1.event_start_day	=	t2.event_start_day
		-- and t1.event_start_hour	=	t2.event_start_hour
		-- and t1.box_subscription	=	t2.box_subscription
		-- and t1.pack_grp			=	t2.pack_grp
		-- and t1.initial_genre	=	t2.initial_genre
		-- and t1.cap_ntile		=	t2.ntile_live
	-- commit
	------------------------------------------------------------------------------------------
	
	if object_id('CP2_h23_3') is not null drop table CP2_h23_3
	commit --; --^^ to be removed

	--identify ntile threshold for event start hours 23-3
	select
			Live
		,	event_date
		,	event_start_day
		,	event_start_hour
		,	pack_grp
		,	initial_genre
		,	min	(
					case
						when	median_dur_mins	>=	MET.BOX_SHUT_DOWN			then	ntile_2
						else													null
					end
				)					as	pri_ntile
		,	max(ntile_2)			as	sec_ntile
		,	case
				when	pri_ntile is null	then	sec_ntile - MET.THRESHOLD_NONTILE
				else								pri_ntile - MET.THRESHOLD_NTILE
			end						as	cap_ntile
		,	cast(null as integer)	as min_dur_mins
	into	CP2_h23_3
	from
					CP2_nt_20_3			AS	NT
		inner join	CP2_metadata_table	AS	MET		ON		NT.EVENT_START_HOUR			BETWEEN	DATEPART(HOUR,MET.START_TIME)
																						AND		DATEPART(HOUR,MET.END_TIME)
    												AND     NT.event_date		  		BETWEEN MET.EFFECTIVE_FROM
    																					AND     MET.EFFECTIVE_TO
    												AND		MET.BANK_HOLIDAY_WEEKEND	=		@targetDateIsWeekend
	where
			event_start_hour	in (23,0,1,2,3)
		and	Live				=	1
	group by
			Live
		,	event_date
		,	event_start_day
		,	event_start_hour
		,	pack_grp
		,	initial_genre
		,	MET.THRESHOLD_NONTILE
		,	MET.THRESHOLD_NTILE
	commit --;-- ^^ originally a commit

	update	CP2_h23_3 t1
	set		min_dur_mins	=	t2.min_dur_mins
	from	CP2_nt_20_3 t2
	where
			t1.Live				=	t2.Live
		and t1.event_start_day	=	t2.event_start_day
		and t1.event_start_hour	=	t2.event_start_hour
		and t1.pack_grp			=	t2.pack_grp
		and t1.initial_genre	=	t2.initial_genre
		and t1.cap_ntile		=	t2.ntile_2
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_h23_3
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'C06: Midway 1/4 (_h23_3)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit


	-----------------------------

	if object_id('CP2_h4_14') is not null drop table CP2_h4_14
	commit --; --^^ to be removed



	--identify ntile threshold for event start hours 4-14
	select
			Live
		,	event_date
		,	event_start_day
		,	event_start_hour
		,	box_subscription
		,	pack_grp
		,	initial_genre
		,	min	(
					case
						when	median_dur_mins >= MET.BOX_SHUT_DOWN	then	ntile_1
						else												null
					end
				)					as	pri_ntile
		,	max(ntile_1)			as	sec_ntile
		,	case
				when	pri_ntile is null		then	sec_ntile - MET.THRESHOLD_NONTILE
				else									pri_ntile - MET.THRESHOLD_NTILE
			end						as	cap_ntile
		,	cast(null as integer)	as	min_dur_mins
	into	CP2_h4_14
	from
					CP2_nt_4_19			AS	NT
		inner join	CP2_metadata_table	AS	MET		ON		NT.EVENT_START_HOUR			BETWEEN	DATEPART(HOUR,MET.START_TIME)
																						AND		DATEPART(HOUR,MET.END_TIME)
    												AND     NT.event_date		  		BETWEEN MET.EFFECTIVE_FROM
    																					AND     MET.EFFECTIVE_TO
    												AND		MET.BANK_HOLIDAY_WEEKEND	=		@targetDateIsWeekend
	where
			event_start_hour	in	(4,5,6,7,8,9,10,11,12,13,14)
		and	Live				=	1
	group by
			Live
		,	event_date
		,	event_start_day
		,	event_start_hour
		,	box_subscription
		,	pack_grp
		,	initial_genre
		,	MET.THRESHOLD_NONTILE
		,	MET.THRESHOLD_NTILE
	commit --;-- ^^ originally a commit

	update	CP2_h4_14 		t1
	set		min_dur_mins	=	t2.min_dur_mins
	from	CP2_nt_4_19		t2
	where
			t1.Live				=	t2.Live
		and t1.event_start_day	=	t2.event_start_day
		and t1.event_start_hour	=	t2.event_start_hour
		and t1.box_subscription	=	t2.box_subscription
		and t1.pack_grp			=	t2.pack_grp
		and t1.initial_genre	=	t2.initial_genre
		and t1.cap_ntile		=	t2.ntile_1
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_h4_14
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'C06: Midway 2/4 (_h4_14)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-----------------------------


	if object_id('CP2_h15_19') is not null drop table CP2_h15_19
	commit --; --^^ to be removed

	--identify ntile threshold for event start hours 15-19
	select
			Live
		,	event_date
		,	event_start_day
		,	event_start_hour
		,	box_subscription
		,	pack_grp
		,	initial_genre
		,	max(ntile_1) as ntile
		,	ntile - MET.THRESHOLD_NTILE	as	cap_ntile
		,	cast(null as integer)		as	min_dur_mins
	into	CP2_h15_19
	from			CP2_nt_4_19			AS	NT
		inner join	CP2_metadata_table	AS	MET		ON		NT.EVENT_START_HOUR			BETWEEN	DATEPART(HOUR,MET.START_TIME)
																						AND		DATEPART(HOUR,MET.END_TIME)
    												AND     NT.event_date		  		BETWEEN MET.EFFECTIVE_FROM
    																					AND     MET.EFFECTIVE_TO
    												AND		MET.BANK_HOLIDAY_WEEKEND	=		@targetDateIsWeekend
	where
			event_start_hour	in	(15,16,17,18,19)
		and	Live				=	1
	group by
			Live
		,	event_date
		,	event_start_day
		,	event_start_hour
		,	box_subscription
		,	pack_grp
		,	initial_genre
		,	MET.THRESHOLD_NTILE
	commit --;-- ^^ originally a commit

	update	CP2_h15_19		t1
	set		min_dur_mins	=	t2.min_dur_mins
	from	CP2_nt_4_19		t2
	where
			t1.Live					=	t2.Live
		and t1.event_start_day		=	t2.event_start_day
		and t1.event_start_hour		=	t2.event_start_hour
		and t1.box_subscription		=	t2.box_subscription
		and t1.pack_grp				=	t2.pack_grp
		and t1.initial_genre		=	t2.initial_genre
		and t1.cap_ntile			=	t2.ntile_1
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_h15_19
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'C06: Midway 3/4 (_h15_19)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-----------------------------


	if object_id('CP2_h20_22') is not null drop table CP2_h20_22
	commit --; --^^ to be removed

	--identify ntile threshold for event start hours 20-22
	select
			Live
		,	event_date
		,	event_start_day
		,	event_start_hour
		,	pack_grp
		,	initial_genre
		,	min	(
					case
						when	median_dur_mins	>=	((23-event_start_hour-1)*60) + MET.BOX_SHUT_DOWN	then	ntile_2
						else																					null
					end
				)					as	pri_ntile
		,	max(ntile_2)			as	sec_ntile
		,	case
				when	pri_ntile is null		then	sec_ntile - MET.THRESHOLD_NONTILE
				else									pri_ntile - MET.THRESHOLD_NTILE
			end						as	cap_ntile
		,	cast(null as integer)	as	min_dur_mins
	into	CP2_h20_22
	from
					CP2_nt_20_3			AS	NT
		inner join	CP2_metadata_table	AS	MET		ON		NT.EVENT_START_HOUR			BETWEEN	DATEPART(HOUR,MET.START_TIME)
																						AND		DATEPART(HOUR,MET.END_TIME)
    												AND     NT.event_date		  		BETWEEN MET.EFFECTIVE_FROM
    																					AND     MET.EFFECTIVE_TO
    												AND		MET.BANK_HOLIDAY_WEEKEND	=		@targetDateIsWeekend
	where
			event_start_hour	in	(20,21,22)
		and	Live				=	1
	group by
			Live
		,	event_date
		,	event_start_day
		,	event_start_hour
		,	pack_grp
		,	initial_genre
		,	MET.THRESHOLD_NONTILE
		,	MET.THRESHOLD_NTILE
	commit --;-- ^^ originally a commit

	update	CP2_h20_22		t1
	set		min_dur_mins	=	t2.min_dur_mins
	from CP2_nt_20_3		t2
	where
			t1.Live					=	t2.Live
		and t1.event_start_day		=	t2.event_start_day
		and t1.event_start_hour		=	t2.event_start_hour
		and t1.pack_grp				=	t2.pack_grp
		and t1.initial_genre		=	t2.initial_genre
		and t1.cap_ntile			=	t2.ntile_2
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_h20_22
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'C06: Complete! (_h20_22)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- C07) BUILDING CENTRAL LISTING OF DERIVED CAPS
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : C07) BUILDING CENTRAL LISTING OF DERIVED CAPS'
	COMMIT

	delete from CP2_calculated_viewing_caps
	commit --; --^^ to be removed

	--identify caps for each variable dimension
	insert into	CP2_calculated_viewing_caps	(
													Live
												,	event_start_day
												,	event_start_hour
												,	box_subscription
												,	pack_grp
												,	initial_genre
												-- Again, managing the cannot-handle-defaults-into-multi-column-indices thing
												,	bucket_id
											)
	select
			Live
		,	event_start_day
		,	event_start_hour
		,	box_subscription
		,	pack_grp
		,	initial_genre
		,	null	as	bucket_id
	from	CP2_ntiles_week
	group by
			Live
		,	event_start_day
		,	event_start_hour
		,	box_subscription
		,	pack_grp
		,	initial_genre
		,	bucket_id
	commit --;-- ^^ originally a commit

	-- This is the last use of CP2_ntiles_week... if we reconstruct it's behaviour up to this point, we're good


	-- Throw on the bucket_id, it'll help us join a few things later:
	update	CP2_calculated_viewing_caps
	set		CP2_calculated_viewing_caps.bucket_id	=	cb.bucket_id
	from
					CP2_calculated_viewing_caps
		inner join	CP2_capping_buckets		as	cb		on  CP2_calculated_viewing_caps.event_start_day  = cb.event_start_day
														and CP2_calculated_viewing_caps.event_start_hour = cb.event_start_hour
														and CP2_calculated_viewing_caps.initial_genre    = cb.initial_genre
														and CP2_calculated_viewing_caps.Live             = cb.Live
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_calculated_viewing_caps
	where bucket_id is not null
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'C07: Midway 1/3 (Buckets)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-- doesn't split out across the pack group and the box subscription, but it still helps.

	-- If we do want to throw on an integer key for the buckets, we need to use:
	--      event_start_day, event_start_hour, pack_grp, initial_genre, live
	-- Sometimes the box subscription isn't used, that has to stay out as a different
	-- thing. Still doing the multi-column join then, but at least the integer key
	-- will be a *bit* better. Update: we now have the cap_bucket_id field on the
	-- CP2_calculated_viewing_caps table, able we'll come up with some other workaround for the
	-- caps that are applied uniformly across box subscription type.

	-- update threshold table with cap limits
	update	CP2_calculated_viewing_caps	t1
	set		max_dur_mins	=	t2.min_dur_mins
	from	CP2_h23_3					t2
	where
			t1.Live				=	t2.Live
		and t1.event_start_day	=	t2.event_start_day
		and t1.event_start_hour	=	t2.event_start_hour
		and t1.pack_grp			=	t2.pack_grp
		and t1.initial_genre	=	t2.initial_genre
	commit --;-- ^^ originally a commit

	update	CP2_calculated_viewing_caps t1
	set		max_dur_mins	=	t2.min_dur_mins
	from	CP2_h4_14 t2
	where
			t1.Live				=	t2.Live
		and t1.event_start_day	=	t2.event_start_day
		and t1.event_start_hour	=	t2.event_start_hour
		and t1.box_subscription	=	t2.box_subscription
		and t1.pack_grp			=	t2.pack_grp
		and t1.initial_genre	=	t2.initial_genre
	commit --;-- ^^ originally a commit

	update	CP2_calculated_viewing_caps t1
	set	max_dur_mins	=	t2.min_dur_mins
	from	CP2_h15_19 t2
	where
			t1.Live				=	t2.Live
		and t1.event_start_day	=	t2.event_start_day
		and t1.event_start_hour	=	t2.event_start_hour
		and t1.box_subscription	=	t2.box_subscription
		and t1.pack_grp			=	t2.pack_grp
		and t1.initial_genre	=	t2.initial_genre
	commit --;-- ^^ originally a commit

	update	CP2_calculated_viewing_caps t1
	set	max_dur_mins	=	t2.min_dur_mins
	from	CP2_h20_22 t2
	where
			t1.Live				=	t2.Live
		and t1.event_start_day	=	t2.event_start_day
		and t1.event_start_hour	=	t2.event_start_hour
		and t1.pack_grp			=	t2.pack_grp
		and t1.initial_genre	=	t2.initial_genre
	commit --; --^^ to be removed

	----------------------------------------------------------------
	-- update	CP2_calculated_viewing_caps	t1
	-- set		max_dur_mins	=	t2.min_dur_mins
	-- from	CP2_h_all	t2
	-- where
			-- t1.Live				=	t2.Live
		-- and t1.event_start_day	=	t2.event_start_day
		-- and t1.event_start_hour	=	t2.event_start_hour
		-- and t1.box_subscription	=	t2.box_subscription
		-- and t1.pack_grp			=	t2.pack_grp
		-- and t1.initial_genre	=	t2.initial_genre
	-- commit --;-- ^^ originally a commit
	----------------------------------------------------------------

	
	commit --;-- ^^ originally a commit
	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_calculated_viewing_caps
	where max_dur_mins is not null
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'C07: Midway 2/3 (Live only)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

    set @playback_ntile=(select max(PLAYBACK_NTILE) from cp2_metadata_table where BANK_HOLIDAY_WEEKEND	=	@targetDateIsWeekend) -- hereeeeeeeeeeeeeeeeeeeeeee

	if object_id('CP2_lp') is not null drop table CP2_lp
	commit --; --^^ to be removed

	--identify ntile threshold for playback events
	select
			Live
		,	event_start_day
		,	@playback_ntile /*max(ntile_lp)*/ as ntile
		,	ntile					as	cap_ntile
		,	cast(null as integer)	as	min_dur_mins
	into	CP2_lp
	FROM	CP2_nt_lp
	where	Live	=	0
	group by
			Live
		,	event_start_day
	commit --; --^^ to be removed

	update	CP2_lp t1
	set		min_dur_mins	=	t2.min_dur_mins
	from	CP2_nt_lp t2
	where
			t1.Live				=	t2.Live
		and t1.event_start_day	=	t2.event_start_day
		and t1.cap_ntile		=	t2.ntile_lp
	commit --;-- ^^ originally a commit

	--update playback limits in caps table
	update	CP2_calculated_viewing_caps t1
	set		max_dur_mins	=	t2.min_dur_mins
	from	CP2_lp t2
	where
			t1.Live				=	t2.Live
		and t1.event_start_day	=	t2.event_start_day
		and t1.max_dur_mins		is null
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_calculated_viewing_caps
	where max_dur_mins is not null
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'C07: Complete! (Central cap listing)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- C08) GLOBAL CAPPING BOUNDS
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : C08) GLOBAL CAPPING BOUNDS'
	COMMIT

	--reset capping limits that are less than the lower limit (see variables in the setup section)
	update	CP2_calculated_viewing_caps
	set	max_dur_mins	=	@min_cap_bound_minutes
	where
			(
					max_dur_mins	<	@min_cap_bound_minutes
				or	max_dur_mins	is null
			)
		and	Live	=	1
	commit --; --^^ to be removed

	--reset capping limits that are more than upper limit (see variables in the setup section)
	update	CP2_calculated_viewing_caps
	set		max_dur_mins = @max_cap_bound_minutes
	where max_dur_mins > @max_cap_bound_minutes
	and Live=1

	commit --;-- ^^ originally a commit
	set @QA_catcher = -1
	commit --; --^^ to be removed

	select	@QA_catcher	=	count(1)
	from	CP2_calculated_viewing_caps
	where	max_dur_mins	in	(
										@min_cap_bound_minutes
									,	@max_cap_bound_minutes
								)
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'C08: Complete! (Global cap bounds)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- C09) DISTRIBUTION OF CAPPING BOUNDS JUST FOR QA
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : C09) DISTRIBUTION OF CAPPING BOUNDS JUST FOR QA'
	COMMIT

	-- Note that this isn't a profile over the use of caps, it's just on the caps that as get built;
	-- there's no extra weight here for caps that get used more often. Oh, also, all our caps should
	-- be between 20 and 120, so that's just a hundred entries that we can just go out and graph...

	delete from CP2_QA_viewing_control_cap_distrib
	where build_date = @target_date

	commit --;-- ^^ originally a commit

	-- OK, here we're using the cumulative ranking duplication trick since we don't have any unique
	-- keys to force the rank to be unique over entries;
	insert into CP2_QA_viewing_control_cap_distrib	(
															build_date
														,	max_dur_mins
														,	cap_instances
													)
	select
			@target_date
		,	max_dur_mins
		,	count(1)
	from	CP2_calculated_viewing_caps
	group by	max_dur_mins
	commit --;-- ^^ originally a commit

	/* ##QA##EQ##: Extraction query: graph this guy in Excel I guess
	select * from CP2_QA_viewing_control_cap_distrib
	order by max_dur_mins;
	*/

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_QA_viewing_control_cap_distrib
	where build_date = @target_date
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'C09: Complete! (Cap distributions)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit


	--select 'start D', now(); --^^ just for debug

	-------------------------------------------------------------------------------------------------
	-- D) APPLYING CAPS TO VIEWING DATA
	-------------------------------------------------------------------------------------------------

	-------------------------------------------------------------------------------------------------
	-- D01) ATTACH CUSTOMER METADATA TO VIEWING
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : D01) ATTACH CUSTOMER METADATA TO VIEWING'
	COMMIT

	/* Now on core table creation
	--add primary/secondary flag to views so thresholds can be applied
	alter table Capping2_01_Viewing_Records
	--add src_system_id varchar(50),                -- This is only ever populated, never used, we don't need it
	add box_subscription varchar(1) default 'U';
	*/

	update	Capping2_01_Viewing_Records
	set	box_subscription = bl.PS_flag
	from
					Capping2_01_Viewing_Records	as	vr
		inner join	CP2_box_lookup				as	bl	on	vr.subscriber_id	=	bl.subscriber_id
	commit --;-- ^^ originally a commit

	--add genre, channel and pack to views so thresholds can be applied
	/* These guys have been added to the permanent table, and have also been given better names in some cases:
	alter table Capping2_01_Viewing_Records
	add initial_genre varchar(30),
	add channel_at_event_start_time varchar(30),            => initial_channel_name
	add pack varchar(100) default null,
	add pack_grp varchar(20) default null;
	*/

	-- A lot of this processing is just a duplication of stuff on the viewing records
	-- table rather than the events table, but, whatever.

	-- Wait, why does this happen here? I thought we were capping on the events level table?
	-- Maybe we only use that to build the caps, and then the capping happens directly on viewing
	-- data? We'l check how that works.

	-- ahahah awesome, this guy is giving us Query Temp space errors on it. Thing is,
	-- we're joining both tables by subscriber_id and adjusted_event_start_time, while
	-- we have both pairs indexed in both tables (both have other things in the index
	-- afterwards, but the multicolumn indices still work over just the initial columns)
	/* NEEDING REFACTORING:
	update Capping2_01_Viewing_Records
	set Capping2_01_Viewing_Records.initial_genre        = t2.genre
				,Capping2_01_Viewing_Records.initial_channel_name = t2.channel_name
	from Capping2_01_Viewing_Records
	inner join CP2_First_Programmes_In_Event t2
	on  Capping2_01_Viewing_Records.subscriber_id             = t2.subscriber_id
	and Capping2_01_Viewing_Records.adjusted_event_start_time = t2.adjusted_event_start_time;
	*/

	-- Okay, so we can't join in the first event stuff from the other table, Sybase is
	-- too small. But the initial genre is going to be exactly the genre for the first
	-- item in each event, which is going to permit simpler treatment for a huge portion
	-- of the data set:
	update	Capping2_01_Viewing_Records
	set
			Capping2_01_Viewing_Records.initial_genre			=	genre
		,	Capping2_01_Viewing_Records.initial_channel_name	=	channel_name
	where	adjusted_event_start_time	=	x_viewing_start_time
	commit --;-- ^^ originally a commit

	-- and now we can tack on the updates for subsequent viewing items:
	--    update Capping2_01_Viewing_Records
	--    set Capping2_01_Viewing_Records.initial_genre        = t2.genre
	--       ,Capping2_01_Viewing_Records.initial_channel_name = t2.channel_name
	--    from Capping2_01_Viewing_Records
	--    inner join CP2_First_Programmes_In_Event t2
	--    on  Capping2_01_Viewing_Records.subscriber_id             = t2.subscriber_id
	--    and Capping2_01_Viewing_Records.adjusted_event_start_time = t2.adjusted_event_start_time
	--    where Capping2_01_Viewing_Records.initial_genre is null
	--    commit

	--replaced the above code with this as I ran into the temp tablespace issue.  This code needs
	--QA'd!!
	if object_id('temp_chan_genre') is not null drop table temp_chan_genre
	commit --; --^^ to be removed

	select
			rec.cb_row_id
		,	event.genre
		,	event.channel_name
	into	temp_chan_genre
	from
			CP2_First_Programmes_In_Event event
		,	Capping2_01_Viewing_Records rec
	where
			event.subscriber_id				=	rec.subscriber_id
		and event.adjusted_event_start_time	=	rec.adjusted_event_start_time
		and rec.initial_genre				is null
	commit --;-- ^^ originally a commit

	create index tmp_idx_genre on temp_chan_genre (cb_row_id)
	commit --; --^^ to be removed


	update	Capping2_01_Viewing_Records
	set
			Capping2_01_Viewing_Records.initial_genre        = t2.genre
		,	Capping2_01_Viewing_Records.initial_channel_name = t2.channel_name
	from
					Capping2_01_Viewing_Records
		inner join	temp_chan_genre		t2		on	Capping2_01_Viewing_Records.cb_row_id	=	t2.cb_row_id
	commit --;-- ^^ originally a commit

	if object_id('temp_chan_genre') is not null drop table temp_chan_genre
	commit --; --^^ to be removed

	-- query temp space issues averted! (though, it only reduced the size of the update
	-- my a factor of three, and the panel is probably growing more than that, so it
	-- might just turn up again following the ramp up and that'd be funny too.)

	-- OK, now we have the metadata on the viewing items, we can get the bucket IDs too:
	update	Capping2_01_Viewing_Records
	set	Capping2_01_Viewing_Records.bucket_id	=	cb.bucket_id
	from
					Capping2_01_Viewing_Records
		inner join	CP2_capping_buckets			as	cb	on	Capping2_01_Viewing_Records.event_start_hour	=	cb.event_start_hour
														and Capping2_01_Viewing_Records.event_start_day  	=	cb.event_start_day
														and Capping2_01_Viewing_Records.initial_genre    	=	cb.initial_genre
														and Capping2_01_Viewing_Records.live             	=	cb.live
	commit --;-- ^^ originally a commit

	-- Wait, we already added this to the event listing, do we really need it on the
	-- viewing records too? whatever...
	--update Capping2_01_Viewing_Records base
	--   set base.pack = t2.channel_pack
	--  from Vespa_Analysts.Channel_Map_Cbi_Prod_Service_Key_Attributes as t2
	-- where base.Initial_Service_Key = t2.Service_Key
	--commit

	--add pack group so thresholds can be applied
	--update Capping2_01_Viewing_Records
	--set pack_grp = coalesce(pack, 'Other')
	--from Capping2_01_Viewing_Records
	--
	--commit

	-- Okay, so at this point, the control totals should till be identical...
	delete from	CP2_QA_viewing_control_totals
	where	(
					data_state	like	'2%'
				or	data_state	like	'3%'
				or	data_state	like	'4%'
			)
	and	build_date	=	@target_date
	commit --;-- ^^ originally a commit

	-- (clearing out all future control totals to so as to not cause any confusion)
	insert into CP2_QA_viewing_control_totals
	select
			@target_date
		,	convert(varchar(20),'2.) Pre-Cap') -- aliases are handled in table construction
		,	program_air_date
		,	live
		,	genre
		,	count(1)
		,	round	(
							sum	(
									coalesce	(
														datediff(second, X_Viewing_Start_Time, X_Viewing_End_Time)
													,	0
												)
								) / 60.0 / 60 / 24.0
						,	2
					)
	from	Capping2_01_Viewing_Records
	group by
			program_air_date
		,	live
		,	genre
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select	@QA_catcher	= count(1)
	from	Capping2_01_Viewing_Records
	where
			pack				is not null
		and bucket_id			is not null
		and box_subscription	in ('P', 'S')
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'D01: Complete! (Metadata on viewing)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- D02) COMPARE VIEWING DATA TO CAPS: DETERMINE CAPPING APPLICATION
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : D02) COMPARE VIEWING DATA TO CAPS: DETERMINE CAPPING APPLICATION'
	COMMIT

	-- We're not ever using max_dur_mins on the viewing data table, it just gets applied to
	-- events, and even then it's only used to mark the ones that get capped so that we can
	-- assign endpoints of the uncapped guys to the capped items. So we're not even going to
	-- bother putting the duration on the viewing table (like it originally was), instead only
	-- put it on the table at the event aggregation level

	update	CP2_event_listing
	set	max_dur_mins	=	caps.max_dur_mins
	from
					CP2_event_listing			as	base
		inner join	CP2_calculated_viewing_caps	as	caps	on	base.bucket_id        =	caps.bucket_id
															and base.pack_grp         =	caps.pack_grp
															and base.box_subscription =	caps.box_subscription
	commit --;-- ^^ originally a commit
	-- (Do we actually need it while it's on the viewing data? do we only ever need it
	-- when it's on whole events? Actually, we never even use it on the events table either,
	-- because we use it to assign new event end times based on the distribution of other
	-- viewing events in the bucket; it only gets used to build the capped_event and then
	-- never gets seen again.)


	-- Follwing that, we'll make this decision about which of the events need to get capped:
	update	CP2_event_listing
	set	capped_event	=	case
								when dateadd(minute, max_dur_mins, adjusted_event_start_time) >= X_Adjusted_Event_End_Time	then	0
								else																								1
							end
	commit --; --^^ to be removed

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_event_listing
	where capped_event = 1
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'D02: Midway 1/3 (Find cappables)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-- Okay, update: trying to select the uncapped evens like this is just crazy, there are
	-- to many of them even before the ramp up. Instead, we're going to trim it down to just
	-- a sample of uncapped events, governed by the "@uncapped_sample_pop_max" at the top of the
	-- script, and use those to determine the capping bounds. This might give us a reasonable
	-- number of events all terminating at exactly the same second, and that'll only get worse
	-- when the boxes are scaled up to the Sky base, but whatever, it'll give us *some* kind
	-- of capping, as opposed to the temp space errors we're seeing at the moment. But, that
	-- was always going to be the case with this approach to capping anyway, because we're not
	-- fitting within a distribuion, we're just matching the endpoint of some other lucky
	-- viewing event.

	-- Because of how Sybase whines about how window functions work, we keep having to select
	-- these into different tables too, awesome.

	-- First stage: append a random number to do the random selection for us
	if object_id('CP2_uncapped_events_lookup_midway_1') is not null drop table CP2_uncapped_events_lookup_midway_1
	commit --; --^^ to be removed
	
	if object_id('CP2_uncapped_events_lookup_midway_2') is not null drop table CP2_uncapped_events_lookup_midway_2
	commit --; --^^ to be removed

	
	select
			bucket_id
		,	initial_channel_name
		,	X_Adjusted_Event_End_Time
		-- we're going to rank over this random variable to pick our sample:
		,	rand(number() * datepart(ms, now())) as sequencer
		-- We need these guys later to do the ordering that the endpoint selection thing uses;
		-- we can't build the event_id at this stage because the min_row & max_row trick wants
		-- the ranking to be dense,	 because a random element is selected between them.
		,	fake_cb_row_id
		,	adjusted_event_start_time
	into	CP2_uncapped_events_lookup_midway_1
	from	CP2_event_listing
	where	capped_event	=	0
	commit --;-- ^^ originally a commit

	-- CP2_uncapped_events_lookup also gets used to select the end times for capped events
	-- once the index lookup stuff is done, but other than that neither the uncapped nor the
	-- capped table gets used past the loop which populates that CP2_capped_events_with_endpoints table, which
	-- we may indeed keep as a seperate table and build that iteratively as we're going to
	-- need some way to drag the cb_row_id back in from the raw viewing table... maybe that
	-- happens at section D06.

	CREATE INDEX for_ranking on CP2_uncapped_events_lookup_midway_1 (bucket_id, initial_channel_name, sequencer)
	commit --;-- ^^ originally a commit

	-- Second stage: rank by this random number
	select
			* -- yes this is a horrible form, but we've already clipped the source table down to only the things we need.
		,	rank()	over	(
								partition by
										bucket_id
									,	initial_channel_name
								order by	sequencer
							)	as	cull_ranking
	into	CP2_uncapped_events_lookup_midway_2
	from	CP2_uncapped_events_lookup_midway_1
	commit --; --^^ to be removed
	-- We'd have done it in the same query, but Sybase doesn't like trying to put
	-- things which determine on number() inside a rank function, oh well.

	-- Third part: cull all the overpopulated buckets
	delete from CP2_uncapped_events_lookup_midway_2
	where	cull_ranking > @uncapped_sample_pop_max
	commit --;-- ^^ originally a commit

	create index for_ordering on CP2_uncapped_events_lookup_midway_2	(
																				bucket_id
																			,	initial_channel_name
																			,	adjusted_event_start_time
																			,	X_Adjusted_Event_End_Time
																			,	fake_cb_row_id
																		)
	commit --; --^^ to be removed


	if object_id('CP2_uncapped_events_lookup') is not null drop table CP2_uncapped_events_lookup
	commit --; --^^ to be removed

	-- Third stage: build the uncapped table lookup using just the remaiing sample of stuff
	select
			bucket_id
		,	initial_channel_name
		,	rank()	over	(
								order by
										bucket_id
									,	initial_channel_name
									,	X_Adjusted_Event_End_Time
									,	adjusted_event_start_time
									,	fake_cb_row_id
							)		as	event_id
		,	X_Adjusted_Event_End_Time
	into	CP2_uncapped_events_lookup
	from	CP2_uncapped_events_lookup_midway_2
	commit --;-- ^^ originally a commit

	-- Done! Clear out the intermediates.
	if object_id('CP2_uncapped_events_lookup_midway_1') is not null drop table CP2_uncapped_events_lookup_midway_1
	commit --; --^^ to be removed
	if object_id('CP2_uncapped_events_lookup_midway_2') is not null drop table CP2_uncapped_events_lookup_midway_2
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_uncapped_events_lookup
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'D02: Midway 2/3 (Uncapped lookup)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-- (## QA item: check how many buckets ended up with the maximum number of uncapped
	-- events in them? bearing in mind we now have channel + bucket...)

	-- identify capped universe
	if object_id('CP2_capped_events_lookup') is not null drop table CP2_capped_events_lookup
	commit --; --^^ to be removed

	select
			subscriber_id
		,	bucket_id
		,	initial_channel_name
		,	adjusted_event_start_time
		,	X_Adjusted_Event_End_Time
		,	max_dur_mins
	into	CP2_capped_events_lookup
	from	CP2_event_listing
	where	capped_event	=	1
	commit --;-- ^^ originally a commit

	-- create indexes to speed up processing
	create unique index fake_pk on CP2_uncapped_events_lookup (event_id)
	commit --; --^^ to be removed

	create        index idx1    on CP2_uncapped_events_lookup (bucket_id, initial_channel_name, X_Adjusted_Event_End_Time)
	commit --; --^^ to be removed

	create unique index fake_pk on   CP2_capped_events_lookup (bucket_id, initial_channel_name, adjusted_event_start_time, subscriber_id)
	-- bucket_id and cahnel name aren't needed there for completeness, but it really *really* helps on the query

	commit --;-- ^^ originally a commit
	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_capped_events_lookup
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'D02: Complete! (Capped lookup)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-- Okay, so using these buckets for this match will reduce the IO, simplify index use, tidy
	-- up the batching process too, generally a good move all around.

	-- And hey, turns out we can grab this number as a pretty good indicator of batch progress:
	/*create or replace variable*/ declare @number_of_capped_events float
	commit --; --^^ to be removed
	
	set @number_of_capped_events = @QA_catcher
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- D03) INDEX THE DURATION-REPLACEMENT LOOKUP
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : D03) INDEX THE DURATION-REPLACEMENT LOOKUP'
	COMMIT

	-- So we've given this section a pretty thorough overhaul and now it actually runs on a
	-- full day of data. Main points introduced:
	--  a. We're now batching by start hour, initial genre, and live/playback
	--  b. We've got a single numerical key which defines all combinations of the above in 1 integer
	--  c. Columns that never get used are no longer included on the capped and uncapped tables
	--  d. Indices are now directly tuned to support the joins in play
	--  e. We've bounded the uncapped population to a fixed number of items per bucket
	-- That said, this action is still the bottleneck for this entire capping build; it's taking
	-- hours to process even a single day's worth of caps, and this is pre-ramp up. Past ramp up
	-- with 3x as much data returned? We might end up capping one day of data in each overnight
	-- build. Good thing we have a scheduler that's able to check if the build has gone too far
	-- into the morning, leave things for the next day, and just pick up where it left off. And
	-- depending on how we made the prioritisation work... it can still build the regular reports
	-- before continuing with the capping tasks outstanding from the previous builds. Nice that
	-- we built a scheduler as solidly as we did then, eh?

	-- The buckets are simultaneously more agressive batching (smaller batches) and a lot cleaner
	-- to work with that the batches that just took dat & start hour. We don't even need the
	-- batching lookup table any more, we can just run it off the buckets table.
	/*create or replace variable*/ declare @the_bucket     int
	commit --; --^^ to be removed
	/*create or replace variable*/ declare @max_buckets    int
	commit --; --^^ to be removed
	/*create or replace variable*/ declare @bucket_offset  int
	commit --; --^^ to be removed
	/*
	create variable @the_bucket     int;
	create variable @max_buckets    int;
	create variable @bucket_offset  int;
	*/
	-- need somewhere to put the results:
	if object_id('CP2_capped_events_with_endpoints') is not null drop table CP2_capped_events_with_endpoints
	commit --; --^^ to be removed
	
	create table CP2_capped_events_with_endpoints	( -- currently not a temp thing because we want to be able to track how data gets into that table...
															subscriber_id                  integer
														,	Adjusted_Event_Start_Time      datetime
														,	X_Adjusted_Event_End_Time      datetime    -- Uncapped event time: needed for control total purposes
														,	max_dur_mins                   integer
														,	bucket_id                      integer
														,	initial_channel_name           varchar(30)
														,	firstrow                       integer
														,	lastrow                        integer
														-- Variables that get played with later as caps are set:
														,	rand_num                       float       default null
														,	uncap_row_num                  integer     default null
														,	capped_event_end_time          datetime    default null
													)
	-- If we need the start time, initial channel etc we can just go into the bucket lookup
	-- and get that stuff afterwards.

	-- We'll add indices after all the data goes in.

	commit --;-- ^^ originally a commit

	-- Here is the start of the work loop:
	select
		-- Need this min/max thing because the buckets table has an IDENTITY key,
		-- and that doesn't get reset between builds...
			@the_bucket		=	min(bucket_id)
		,	@bucket_offset	=	min(bucket_id)
		,	@max_buckets	=	max(bucket_id)
	from	CP2_capping_buckets
	commit --;-- ^^ originally a commit

	-- Okay, now we can actually assemble the table:
	while	@the_bucket	<=	@max_buckets
	begin

		insert into	CP2_capped_events_with_endpoints	(
																subscriber_id
															,	adjusted_event_start_time
															,	X_Adjusted_Event_End_Time
															,	max_dur_mins
															,	bucket_id
															,	initial_channel_name
															,	firstrow
															,	lastrow
														)
		select
				t1.subscriber_id
			,	t1.adjusted_event_start_time
			,	min(t1.X_Adjusted_Event_End_Time)  -- they're all the same anyways
			,	min(t1.max_dur_mins)
			,	@the_bucket
			,	min(t1.initial_channel_name)       -- also determined by adjusted_event_start_time
			-- identify first and last row id in uncapped events that have same profile as capped event
			,	min(t2.event_id)
			,	max(t2.event_id)
		from
						CP2_capped_events_lookup	as	t1
			left join	CP2_uncapped_events_lookup	as	t2	on	t1.bucket_id              		=	@the_bucket
															and t2.bucket_id              		=	@the_bucket
															and t1.initial_channel_name   		=	t2.initial_channel_name
															and t2.X_Adjusted_Event_End_Time	>	dateadd(second, @min_view_duration_sec, t1.adjusted_event_start_time)     -- Capped event min length restriction (7 seconds)
															and t2.X_Adjusted_Event_End_Time	<=	dateadd(second, 180 * 60, t1.adjusted_event_start_time)                   -- Capped event max length restriction (180 minutes)
															and t2.X_Adjusted_Event_End_Time	<=	t1.X_Adjusted_Event_End_Time
		where	t1.bucket_id	=	@the_bucket
		-- and   t2.bucket_id            = @the_bucket     -- SBE: this condition is removed to retain capped event with no matching uncapped events
		group by
				t1.subscriber_id
			,	t1.adjusted_event_start_time
--        ,t1.X_Adjusted_Event_End_Time -- isn't this entirely determined by the event start time? in fact yes, it is... worse than that, it's never used later in the build...
		commit

		-- Check the control totals every now and then for progress tracking purposes:
		if mod(@the_bucket - @bucket_offset + 1, 40) = 0     -- notify every 40 buckets; the first demo build had ~470 buckets to consider
		begin
		
			set @QA_catcher = -1
			commit

			-- How many items have we resolved thus far?
			select @QA_catcher = count(1)
			from CP2_capped_events_with_endpoints
			commit

			-- Note that the counter we're tracking should ultimately head towards the number of capped
			-- items. hey, let's put that in as a progress meter...

			execute M00_2_output_to_logger 'D03: Processed bucket ' || convert(varchar(10), @the_bucket - @bucket_offset + 1) || ' out of ' || convert(varchar(10), @max_buckets - @bucket_offset + 1) || ' (Events: ' || round(100 * @QA_catcher/@number_of_capped_events,1) || '%) ' || coalesce(@QA_catcher, -1)
			commit
			-- Bear in mind that the later buckets are going to be much smaller than the earlier ones,
			-- because buckets with not very many events are less likely to be discovered earlier in
			-- the DISTINCT pass; progress will probably jump with the first few cycles of buckets,
			-- and then spend a lot of buckets just tidying up little sparsely populated edge cases.

		end

		-- Move on to the next bucket
		set @the_bucket = @the_bucket + 1
		commit
		
	end
	commit --;-- ^^ originally a commit
	-- This guy still takes ages, but it's now a managed/mitigated bottleneck rather
	-- than a showstopper.

	CREATE hg     INDEX idx1    ON CP2_capped_events_with_endpoints (uncap_row_num)
	commit --;-- ^^ originally a commit
	create unique index fake_PK on CP2_capped_events_with_endpoints (subscriber_id, adjusted_event_start_time)
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_capped_events_with_endpoints
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'D03: Complete! (Duration replacement)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- D04) RANDOMLY CHOOSE REPLACEMENT DURATION FOR CAPPED EVENTS
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : D04) RANDOMLY CHOOSE REPLACEMENT DURATION FOR CAPPED EVENTS'
	COMMIT

	--create a pretty random multiplier
	/*create or replace variable*/ declare @multiplier bigint              --has to be a bigint if you are dealing with millions of records.
					--create variable @multiplier bigint
	commit --; --^^ to be removed
	
	SET @multiplier = DATEPART(millisecond,now())+1 -- pretty random number between 1 and 1000
	commit --; --^^ to be removed

	--generate random number for each capped event
	update	CP2_capped_events_with_endpoints
	set	rand_num	=	rand(number(*)*@multiplier)      --the number(*) function just gives a sequential number.
	commit --;-- ^^ originally a commit

	--identify row id in uncapped universe to select
	update	CP2_capped_events_with_endpoints
	set	uncap_row_num	=	case
								when	firstrow > 0	then	round(((lastrow - firstrow) * rand_num + firstrow),0)
								else							null
							end
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select	@QA_catcher = count(1)
	from	CP2_capped_events_with_endpoints
	where	uncap_row_num is not null
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'D04: Complete! (Select replacements)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- D05) ASSIGN NEW END TIMES
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : D05) ASSIGN NEW END TIMES'
	COMMIT

	--assign new event end time to capped events
	update	CP2_capped_events_with_endpoints	t1
	set	capped_event_end_time	=	t2.X_Adjusted_Event_End_Time
	from
					CP2_capped_events_with_endpoints	as	t1
		inner join	CP2_uncapped_events_lookup			as	t2	on	t1.uncap_row_num	=	t2.event_id
	commit --;-- ^^ originally a commit

	-- And that's the last use of the "uncapped" table
	if object_id('CP2_uncapped_events_lookup') is not null drop table CP2_uncapped_events_lookup
	commit --; --^^ to be removed
	if object_id('CP2_capped_events_lookup') is not null drop table CP2_capped_events_lookup
	commit --;-- ^^ originally a commit


	--assign end time of first programme to capped events if no uncapped distribution is available
	update CP2_capped_events_with_endpoints t1
	set	capped_event_end_time	=	case	-- when capped time is still missing and first instance duration > max_dur_mins => max_dur_mins
										when
												t1.capped_event_end_time is null
											and	(datediff(second, t2.adjusted_event_start_time, t2.x_viewing_end_time) > max_dur_mins * 60)		then	dateadd(minute, max_dur_mins, t2.adjusted_event_start_time)
										when
												t1.capped_event_end_time is null																then	t2.x_viewing_end_time
										else																											t1.capped_event_end_time
									end
	from	CP2_First_Programmes_In_Event t2
	where
			t1.subscriber_id				=	t2.subscriber_id
		and t1.adjusted_event_start_time	=	t2.adjusted_event_start_time
		and t1.firstrow						is null
	commit --;-- ^^ originally a commit
	--2501
	-- Joining to CP2_First_Programmes_In_Event again? Should the broadcast end time be already in the record
	-- we're treating, ie, can do with a single table update, no join? -Yes, but what if the event
	-- ends before the first show ends? using the first program table, we dodge that issue. Also: predcting
	-- temp space errors on this guy too, oh well.

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_capped_events_with_endpoints
	where capped_event_end_time is not null
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'D05: Complete! (Assign end times)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	--select now(), 'landmark' ; --^^ just for debug


	-------------------------------------------------------------------------------------------------
	-- D06) PUSH CAPPING BACK ONTO INITIAL VIEWING TABLE
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : D06) PUSH CAPPING BACK ONTO INITIAL VIEWING TABLE'
	COMMIT

	-- Heh, no, we're going to throw all of this viewing into a new dynamically named table which
	-- matches the timestamp of the daily table. Or... well, we'll probably build the thing here
	-- and then do it as one single port, rather than have a whole bunch of dynamically named tables.
	-- But still... at this point we only need a tiny selection of what's on the viewing data tables,
	-- we're pretty close to the end of the process.

	-- Man, I hope I don't have to batch this thing as well.... nah, we seem okay.

	delete from CP2_capped_data_holding_pen
	commit --;-- ^^ originally a commit

	-- First we're only adding the capped data, have to remember to throw in
	-- the uncapped stuff too at a later stage when all the capping processing
	-- is done (but before the generic stuff like BARB minute and scaling
	-- weighting coeficients)
	insert into CP2_capped_data_holding_pen	(
													cb_row_id
												,	subscriber_id
												,	account_number
												,	programme_trans_sk
												,	adjusted_event_start_time
												,	X_Adjusted_Event_End_Time
												,	x_viewing_start_time
												,	x_viewing_end_time
												,	capped_event_end_time
												,	timeshifting
												,	capped_flag
												-- Things we need for control totals:
												,	program_air_date
												,	live
												,	genre
											)
	select
			vr.cb_row_id
		,	vr.subscriber_id
		,	vr.account_number
		,	vr.programme_trans_sk
		,	vr.adjusted_event_start_time
		,	vr.X_Adjusted_Event_End_Time
		,	vr.x_viewing_start_time
		,	vr.x_viewing_end_time
		,	cewe.capped_event_end_time
		,	case
				when	vr.live = 1		then	'LIVE'
				when	vr.live = 0		then	'TIMESHIFT'   -- Will later be replaced with 'VOSDAL' or 'PLAYBACK7' or 'PLAYBACK28'
				else							'FAIL!'                        -- ## QA Check that there aren't any of these
			end
		,	case
				when	cewe.subscriber_id is not null		then	11	-- 11 for things that need capping treatment
				else												0	-- 0 for no capping
			end
		,	vr.program_air_date
		,	vr.live
		,	vr.genre
	from
					Capping2_01_Viewing_Records			as	vr
		left join	CP2_capped_events_with_endpoints	as	cewe	on	cewe.subscriber_id             =	vr.subscriber_id
																	and	cewe.adjusted_event_start_time =	vr.adjusted_event_start_time
	commit --;-- ^^ originally a commit
	-- WAIT! ## we need to get TIMESHIFTING flag in here too. Thouhg we haven't
	-- checked VOSDAL / PLAYBACK7 / PLAYBACK28 yet, but we can flag LIVE stuff.


	/*
	--append fields to table to store additional metrics for capping
	alter table Capping2_01_Viewing_Records
	add (capped_event_end_time datetime
					,capped_x_viewing_start_time datetime
					,capped_x_viewing_end_time datetime
					,capped_x_programme_viewed_duration integer
					,capped_flag integer );

	--update daily view table with revised end times for capped events
	update Capping2_01_Viewing_Records t1
	set capped_event_end_time=t2.capped_event_end_time
	from CP2_capped_events_with_endpoints t2
	where t1.subscriber_id=t2.subscriber_id
	and t1.adjusted_event_start_time=t2.adjusted_event_start_time;

	commit;
	*/

	-- How are we going to get the control totals out if we don't put the capping back onto
	-- the viewing records table? hmmm....

	-- Okay, before we get rid of the holding pen, let's grab the event distributions from it:
	delete from CP2_QA_event_control_distribs
	where build_date = @target_date
	commit --;-- ^^ originally a commit

	-- okey, the way these GROUP BY statements go, this guy could be slow...


	insert into CP2_QA_event_control_distribs	(
														build_date
													,	data_state
													,	duration_interval
													,	viewing_events
												)
	select
			@target_date
		,	convert(varchar(20), '1.) Uncapped')
		,	datediff(minute, Adjusted_Event_Start_Time, X_Adjusted_Event_End_Time) as grouping_minute -- batched into 1m chunks, so 0 means viewing durations between 0s and 1 minute
		,	count(1)
	from	CP2_capped_events_with_endpoints
	group by	grouping_minute
	commit --;-- ^^ originally a commit

	insert into CP2_QA_event_control_distribs	(
														build_date
													,	data_state
													,	duration_interval
													,	viewing_events
												)
	select
			@target_date
		,	convert(varchar(20), '2.) Capped')
		,	datediff(minute, Adjusted_Event_Start_Time,	capped_event_end_time) as grouping_minute -- batched into 1m chunks,	 so 0 means viewing durations between 0s and 1 minute
		,	count(1)
	from	CP2_capped_events_with_endpoints
	group by	grouping_minute
	commit --;-- ^^ originally a commit


	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_capped_data_holding_pen
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'D06: Midway 1/3 (Populate holding pen)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-- More column renamiings: because we're not on the
	--  capped_x_viewing_start_time         => viewing_starts
	--  capped_x_viewing_end_time           => viewing_stops
	--  capped_x_programme_viewed_duration  => viewing_duration
	-- We're also discontinuing use of the Capping2_01_Viewing_Records table, because
	-- we've grabbed everything we want and are now just working in the holding
	-- pen. Actually, we could empty out the vireing records table (wait, have
	-- we imported all the uncapped data yet?)

	--update table to create revised start and end viewing times
	update	CP2_capped_data_holding_pen
	set
			viewing_starts	=	case
									-- if start of viewing_time is beyond capped end time then flag as null
									when	capped_event_end_time <= x_viewing_start_time	then	null
									-- else leave start of viewing time unchanged
									else															x_viewing_start_time
								end
		,	viewing_stops	=	case
									-- if start of viewing_time is beyond capped end time then flag as null
									when	capped_event_end_time <= x_viewing_start_time	then	null
									-- if capped event end time is beyond end time then leave end time unchanged
									when	capped_event_end_time > x_viewing_end_time		then	x_viewing_end_time
									-- if capped event end time is null then leave end time unchanged
									when	capped_event_end_time is null					then	x_viewing_end_time
									-- otherwise set end time to capped event end time
									else															capped_event_end_time
								end
	where	capped_flag	=	11  -- Only bother with the capped events...
	commit --;-- ^^ originally a commit

	-- And now the more basic case where there's no capping;
	update	CP2_capped_data_holding_pen
	set
			viewing_starts	=	x_viewing_start_time
		,	viewing_stops	=	x_viewing_end_time
	where	capped_flag	=	0
	commit --;-- ^^ originally a commit

	--calculate revised programme viewed duration
	update	CP2_capped_data_holding_pen
	set	viewing_duration = datediff(second, viewing_starts, viewing_stops)
	commit --;-- ^^ originally a commit

	--set capped_flag based on nature of capping
	--1 programme view not affected by capping
	--2 if programme view has been shortened by a long duration capping rule
	--3 if programme view has been excluded by a long duration capping rule

	--identify views which need to be capped
	update	CP2_capped_data_holding_pen
	set	capped_flag	=	case
							when	viewing_stops < x_viewing_end_time	then	2
							when	viewing_starts is null				then	3
							else												1
						end
	where	capped_flag	=	11
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_capped_data_holding_pen
	where viewing_duration is not null
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'D06: Midway 2/3 (Calculate view bounds)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit


	-- Now the total viewing should be different... though there's no midpoint, it just *chunk* turns up all at once
	delete from	CP2_QA_viewing_control_totals
	where	(
					data_state	like	'3%'
				or	data_state	like	'4%'
			)
	and	build_date = @target_date
	commit --;-- ^^ originally a commit

	insert into CP2_QA_viewing_control_totals
	select
			@target_date
		,	convert(varchar(20), '3.) Capped') -- aliases are handled in table construction
		,	program_air_date
		,	live
		,	genre
		,	count(1)
		,	round(sum(coalesce(datediff(second, viewing_starts, viewing_stops),0)) / 60.0 / 60 / 24.0, 2)
	from	CP2_capped_data_holding_pen
	group by
			program_air_date
		,	live
		,	genre
	commit --; --^^ to be removed

	-- OK, so that's the total of what's left, but we also want the breakdown by
	-- each capping action, so we can check that they all add up:

	-- First clear out the marks in case we're rerunning this section without starting
	-- from the top of the script:
	delete from CP2_QA_viewing_control_totals
	where data_state like '4%'
	and build_date = @target_date
	commit --;-- ^^ originally a commit

	-- The total time in events that were not capped:
	insert into CP2_QA_viewing_control_totals
	select
			@target_date
		,	convert(varchar(20), '4a.) Uncapped')
		,	program_air_date
		,	live
		,	genre
		,	count(1)
		,	round(sum(coalesce(datediff(second, viewing_starts, viewing_stops),0)) / 60.0 / 60 / 24.0, 2)
	from	CP2_capped_data_holding_pen
	where	capped_flag	=	0
	group by
			program_air_date
		,	live
		,	genre
	commit --; --^^ to be removed

	-- Total time in events that were capped but the viewing for that record wasn't affected:
	insert into	CP2_QA_viewing_control_totals
	select
			@target_date
		,	convert(varchar(20), '4b.) Unaffected')
		,	program_air_date
		,	live
		,	genre
		,	count(1)
		,	round(sum(coalesce(datediff(second, viewing_starts, viewing_stops),0)) / 60.0 / 60 / 24.0, 2)
	from	CP2_capped_data_holding_pen
	where	capped_flag	=	1
	group by
			program_air_date
		,	live
		,	genre
	commit --; --^^ to be removed

	-- Total time in events that were just dropped:
	insert into	CP2_QA_viewing_control_totals
	select
			@target_date
		,	convert(varchar(20), '4c.) Excluded')
		,	program_air_date
		,	live
		,	genre
		,	count(1)
		,	round(sum(coalesce(datediff(second, x_viewing_start_time, x_viewing_end_time),0)) / 60.0 / 60 / 24.0, 2)
	from	CP2_capped_data_holding_pen
	where	capped_flag	=	3
	group by
			program_air_date
		,	live
		,	genre
	commit --;-- ^^ originally a commit

	-- The total time left in events that were capped:
	insert into	CP2_QA_viewing_control_totals
	select
			@target_date
		,	convert(varchar(20), '4d.) Truncated')
		,	program_air_date
		,	live
		,	genre
		,	count(1)
		,	round(sum(coalesce(datediff(second, viewing_starts, viewing_stops),0)) / 60.0 / 60 / 24.0, 2)
	from	CP2_capped_data_holding_pen
	where	capped_flag	=	2
	group by
			program_air_date
		,	live
		,	genre
	commit --; --^^ to be removed

	-- Total time removed from events that were capped
	insert into	CP2_QA_viewing_control_totals
	select
			@target_date
		,	convert(varchar(20), '4e.) T-Margin')
		,	program_air_date
		,	live
		,	genre
		,	count(1)
		,	round	(
							(
									sum(coalesce(datediff(second, x_viewing_start_time, x_viewing_end_time),0))
								-	sum(coalesce(datediff(second, viewing_starts, viewing_stops),0))
							) / 60.0 / 60 / 24.0
						,	2
					)
	from	CP2_capped_data_holding_pen
	where	capped_flag	=	2
	group by
			program_air_date
		,	live
		,	genre
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'D06: Midway 1/2 (Control totals)'
	commit --;-- ^^ originally a commit

	-- Wait, so where's the bit where we delete all the records that were excluded by capping? Maybe we
	-- just don't migrate them into the dynamic table? Update: Nope, here it is:

	delete from  CP2_capped_data_holding_pen
	where capped_flag = 3

	commit --;-- ^^ originally a commit

	-- At the same time, we're also reducing it to viewing records that are strictly contained
	-- within the viewing table we're processing.
	if object_id('capped_viewing_totals') is not null drop table capped_viewing_totals
	commit --; --^^ to be removed

	-- Oh, we should also grab the total average viewing (again)
	select
			subscriber_id
		,	round(sum(viewing_duration) / 60.0, 0)	as	total_box_viewing
	into	capped_viewing_totals
	from	CP2_capped_data_holding_pen
	group by	subscriber_id
	-- Don't need the WHERE filter, we've already removed things not on the daily table
	commit --;-- ^^ originally a commit

	select @QA_catcher = avg(total_box_viewing)
	from capped_viewing_totals
	commit --;-- ^^ originally a commit

	update CP2_QA_daily_average_viewing
	set average_capped_viewing = @QA_catcher
	where build_date = @target_date
	commit --;-- ^^ originally a commit

	if object_id('capped_viewing_totals') is not null drop table capped_viewing_totals
	commit --; --^^ to be removed

	-- Section mostly complete!

	set @QA_catcher = -1
	commit --;-- ^^ originally a commit

	select @QA_catcher = count(1)
	from CP2_capped_data_holding_pen
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'D06: Complete! (Capping on viewing table)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- D07) CHECKING TOTAL VIEWING BEFORE AND AFTER CAPPING
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : D07) CHECKING TOTAL VIEWING BEFORE AND AFTER CAPPING'
	COMMIT

	/* ##QA##EQ##: Pivot the results of this extraction query in Excel I guess:
	select * from CP2_QA_viewing_control_totals
	order by data_state, program_air_date, live, genre
	*/

	/* What we expect:
					*. '1.) Collect' should match '2.) Pre-Cap'
					*. '4a.) Uncapped' + '4b.) Unafected' + '4d.) Truncated' should add up to '3.) Capped',
					*. '4a.) Uncapped' + '4b.) Unafected' + '4c.) Excluded' + '4d.) Truncated' + '4e.) T-Margin' should add up to '1.) Collect'
	They should match pretty much exactly, since we've rounded everything to 2dp in hours.
	*/

	set @QA_catcher = -1
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'D07: NYIP! (Total viewing before / after capping)'
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- D08) LOOKING AT VIEWING DURATION PROFILE BEFORE AND AFTER CAPPING
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : D08) LOOKING AT VIEWING DURATION PROFILE BEFORE AND AFTER CAPPING'
	COMMIT

	-- Okay, but we're going to batch it into 10s histogram thing, because other these tables will be
	-- huge, and we should still be able to get all the detail want from this view even:

	-- Wait, except this is going on viewing records and not on event lengths... neet to build the one
	-- that actually goes on event lengths... though do we have that anywhere? Er... the only place it
	-- currently lives at event level is on the temp table - CP2_capped_events_with_endpoints - maybe
	-- we make that guy permanent so we can interrogate it later? We'll also need the event start time
	-- and end time too so we can properly get all the durations we need directly from it. Okey.

	delete from CP2_QA_viewing_control_distribs
	where build_date = @target_date
	commit --;-- ^^ originally a commit

	insert into CP2_QA_viewing_control_distribs	(
														build_date
													,	data_state
													,	duration_interval
													,	viewing_events
												)
	select
			@target_date
		,	convert(varchar(20), '1.) Uncapped')
		,	floor(x_programme_viewed_duration / 10) * 10 as grouping_guy -- batched into 10s chunks, so 0 means viewing durations between 0s and 10s
		,	count(1)
	from	Capping2_01_Viewing_Records
	where	x_programme_viewed_duration	>	0
	group by	grouping_guy
	commit --;-- ^^ originally a commit

	-- Is this the last time we need Capping2_01_Viewing_Records? From here, everything
	-- should be happening in the holding pen...


	insert into CP2_QA_viewing_control_distribs	(
														build_date
													,	data_state
													,	duration_interval
													,	viewing_events
												)
	select
			@target_date
		,	convert(varchar(20), '2.) Capped')
		,	floor(viewing_duration / 10) * 10 as grouping_guy -- again giving the alias so as to make the grouping more transparent
		,	count(1)
	from	CP2_capped_data_holding_pen
	where	viewing_duration	>	0
	group by	grouping_guy
	commit --;-- ^^ originally a commit

	/* ##QA##EQ##: Extraction query: make a graph in Excel or something
	select * from CP2_QA_viewing_control_distribs
	order by data_state, duration_interval
	*/

	set @QA_catcher = -1
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'D08: Complete! (Viewing duration profile)'
	commit --;-- ^^ originally a commit


	--select now(), 'E start'; --^^ just for debug

	/*
	--------------------------------------------------------------------------------
	-- E - Add Additional Feilds to the Viewing data
	--------------------------------------------------------------------------------

	E01 - Add Playback and Vosdal flags

	--------------------------------------------------------------------------------
	*/


	--------------------------------------------------------------------------------
	-- E01  Add Playback and Vosdal flags to the viewing data
	--------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : E01) Add Playback and Vosdal flags to the viewing data'
	COMMIT

	/*
	vosdal (viewed on same day as live) and playback = within 7 days of air

	viewing_starts - adjusted for playback and capped events, otherwise it is the original viewing time

	*/

	/* Don't need this, we're just playing with the timeshifting flag
	--Add the additional fields to the viewing table
	ALTER TABLE CP2_capped_data_holding_pen
	add (VOSDAL             as integer default 0
	,Playback           as integer default 0
	,playback_date      as date
	,playback_post7     as integer default 0 );
	*/

	-- Update the fields:
	Update	CP2_capped_data_holding_pen
	set	timeshifting	=	case
								when	live = 1																			then	'LIVE'
								when	date(viewing_starts) = program_air_date												then	'VOSDAL'
								when
										date(viewing_starts)	>	program_air_date
									and	viewing_starts			<=	dateadd(hour, 170, cast(program_air_date as datetime))	then	'PLAYBACK7'
								when	viewing_starts > dateadd(hour, 170, cast(program_air_date as datetime))				then	'PLAYBACK28'
							end
	commit --; --^^ to be removed

	/* The old build:
		set  VOSDAL        = (case when viewing_starts <= dateadd(hh, 26,cast( cast( program_air_date as date) as datetime)) and live = 0
																																																																																																																																		then 1 else 0 end)

						,Playback       =       (case when viewing_starts <= (dateadd(hour, 170, program_air_date))
																																				and  viewing_starts > (cast(dateadd(hour,26,program_air_date)  as datetime))and live = 0 then 1 else 0 end)

						,playback_post7 =       (case when viewing_starts > (dateadd(day, 170, program_air_date))and live = 0
																																																																																																																									then 1 else 0 end); -- flag these so identifying mismatchs is easy later
	*/
	--select top 10 * from CP2_capped_data_holding_pen where vosdal = 1
	--select top 10 * from CP2_capped_data_holding_pen where playback = 1
	--select top 10 * from CP2_capped_data_holding_pen where playback_post7 = 1

	-- So at this point all the capping and processing is done, and we can transfer these guys
	-- to the dynamic daily augmentation table.

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_capped_data_holding_pen
	where timeshifting is not null
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'E01: Complete! (Calculate view bounds)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit



	--select now(), 'G start'; --^^ just for debug


	--------------------------------------------------------------------------------
	-- PART G) - DYNAMIC DAILY CAPPED TABLE
	--------------------------------------------------------------------------------
	-- So we've done all this work, and now we need to dynamically put the results
	-- into a table named after the same day as the source daily table. Oh and we have
	-- to leave off the items that were in the previous daily table which we only
	-- included to get the right capping behaviour for early items.

	delete from	CP2_capped_data_holding_pen
	where	date(viewing_starts) <> @target_date
	commit --;-- ^^ originally a commit
	
	
	--------------------------------------------------------------------------------
	-- G01) - ADD SCALING WEIGHTS
	--------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : G01) - ADD SCALING WEIGHTS'
	COMMIT
	
	update	CP2_capped_data_holding_pen		as	a
	set		a.Scaling_Weighting	=	b.adsmart_scaling_weight
	from	CP2_accounts	as	b
	where	a.account_number	=	b.account_number
	and		b.reference_date	=	@target_date
	commit


	--------------------------------------------------------------------------------
	-- G02) - DYNAMIC DAILY AUGMENTATION TABLE: POPULATION
	--------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : G02) - DYNAMIC DAILY AUGMENTATION TABLE: POPULATION'
	COMMIT
	
	-- Specifically no filters here as we're running the QA actions over CP2_capped_data_holding_pen
	-- since that makes it a lot easier to get the totals and checks etc into logger than when
	-- doing everything dynamically of the daily augmented tables.

	insert into Vespa_Daily_Augs	(
											Cb_Row_Id
										,	target_date
										,	iteration_number
										,	Account_Number
										,	Subscriber_Id
										,	Programme_Trans_Sk
										,	Timeshifting
										,	Viewing_Starts
										,	Viewing_Stops
										,	Viewing_Duration
										,	Capped_Flag
										,	Capped_Event_End_Time
										,	Scaling_Segment_Id
										,	Scaling_Weighting
										,	BARB_Minute_Start
										,	BARB_Minute_End
										,	adjusted_event_start_time
										,	X_Adjusted_Event_End_Time
										,	x_viewing_start_time
										,	x_viewing_end_time
										-- Other things we only need to maintain our control totals:
										,	program_air_date
										,	live
										,	genre
									)
	select
			Cb_Row_Id
		,	@target_date
		,	@iteration_number
		,	Account_Number
		,	Subscriber_Id
		,	Programme_Trans_Sk
		,	Timeshifting
		,	Viewing_Starts
		,	Viewing_Stops
		,	Viewing_Duration
		,	Capped_Flag
		,	Capped_Event_End_Time
		,	Scaling_Segment_Id
		,	Scaling_Weighting
		,	BARB_Minute_Start
		,	BARB_Minute_End
		,	adjusted_event_start_time
		,	X_Adjusted_Event_End_Time
		,	x_viewing_start_time
		,	x_viewing_end_time
		-- Other things we only need to maintain our control totals:
		,	program_air_date
		,	live
		,	genre
	from	CP2_capped_data_holding_pen
	commit --;-- ^^ originally a commit


	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_capped_data_holding_pen
	where date(viewing_starts) = @target_date
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'G02: Aug table completed' || coalesce(@QA_catcher, -1)
	commit --; --^^ to be removed


end; -- procedure CP2_build_days_caps_CUSTOM
commit;

--select '5 end', now()
grant execute on V306_CP2_M05_Build_Day_Caps to vespa_group_low_security;
commit;







