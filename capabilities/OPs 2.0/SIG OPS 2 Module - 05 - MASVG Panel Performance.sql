 /*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:							OPS 2.0
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jose Loureda
**Stakeholder:                          Operational Reports / SIG
**Due Date:                             20/09/2013
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:

**Modules:

M05: MASVG Panel Composition
        M05.0 - Initialising environment
        M05.1 - Deriving Metrics for Panel Performance on DP
				-- Num_logs_sent_30d
				-- reporting_quality
				-- Num_logs_sent_7d
				-- Continued_trans_30d
				-- Continued_trans_7d
				-- Reporting_performance
				-- avg_reporting_quality
				-- min_reporting_quality
				-- avg_events_in_logs
				-- total_calls_in_30d
		M05.2 - Deriving Metrics for Panel Performance on AP
				-- Num_logs_sent_30d
				-- reporting_quality
				-- Reporting_performance
				-- avg_reporting_quality
				-- min_reporting_quality
		M05.3 - General Panel Performance KPIs
				-- return_data_7d
				-- return_data_30d
				-- reporting_quality_s
        M05.4 - QAing results
        M05.5 - Returning results

**Stats:

	-- running time: 18 min approx...
	
--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M05.0 - Initialising environment
-----------------------------------

create or replace procedure sig_masvg_m05_panel_performance
as begin


if object_id ('vespa_analysts.M04_t1_panel_sample_stage0') is not null
begin
    
    delete from m05_t1_panel_performance_stage0
    commit
    
	MESSAGE cast(now() as timestamp)||' | Beginig M05.0 - Initialising environment' TO CLIENT
	
	-- VARIABLES
	
	--declare @last_full_date		date
	declare @from_dt			date
	declare	@to_dt				date
	declare @event_from_date 	integer
	declare @event_to_date      integer
	declare @profiling_day		date
	
	--execute vespa_analysts.Regulars_Get_report_end_date @last_full_date output								-- YYYY-MM-DD
	select @profiling_day 	= max(cb_data_date) from sk_prod.cust_single_account_view
	
	set @to_dt 				= @profiling_day 																-- YYYY-MM-DD
	set @from_dt 			= @profiling_day -60															-- YYYY-MM-DD
	set @event_from_date    = convert(integer,dateformat(dateadd(day, -60, @profiling_day),'yyyymmddhh'))	-- YYYYMMDD00
	set @event_to_date      = convert(integer,dateformat(@profiling_day,'yyyymmdd')+'23')	                -- YYYYMMDD23
	
	
	
	-- Getting everyone in the panel into the performance table to measure them...
	insert	into m05_t1_panel_performance_stage0	(
														account_number
														,subscriber_id
													)
	select	distinct
			account_number
			,subscriber_id
	from	M04_t1_panel_sample_stage0
		
	commit
	
	-- Constructing the base to measure...	
	-- Sampling the period of interest to derived the panel performance metrics we need...
	select	subscriber_id
			,convert(date, dateadd(hh, -6, log_received_start_date_time_utc))		as log_received_date
			,min(dateadd(hour,1, EVENT_START_DATE_TIME_UTC))						as first_event_mark
			,max(dateadd(hour,1, EVENT_END_DATE_TIME_UTC))							as last_event_mark
			,count(1)																as num_events_in_logs
			,datepart(hh, min(dateadd(hour,1, LOG_RECEIVED_START_DATE_TIME_UTC)))	as hour_received
	into	#measure_base_stage_1
	from   	sk_prod.VESPA_DP_PROG_VIEWED_CURRENT
	where  	panel_id in (12,11)
	and     dk_event_start_datehour_dim between @event_from_date and @event_to_date
	and     LOG_RECEIVED_START_DATE_TIME_UTC is not null
	and     LOG_START_DATE_TIME_UTC is not null
	and     subscriber_id > 0 -- to avoid nulls and -1...
	group 	by	subscriber_id
				,log_received_start_date_time_utc
	having 	log_received_start_date_time_utc is not null	
	
	commit	
		
	delete from #measure_base_stage_1 where	log_received_date > @profiling_day
	
	commit -- > 12 min up to here... is too much...
	MESSAGE cast(now() as timestamp)||' | @ M05.0: Sampling from the Viewing Tables DONE' TO CLIENT
	
	-- Preparing the data to slice the 30 days stats...
	
	if object_id ('vespa_analysts.measure_base_stage_2') is not null
		drop table measure_base_stage_2
		
	commit
	
	select	subscriber_id
			,log_received_date			as log_date
			,count(1) 					as num_of_calls
			,min(first_event_mark)		as first_event_mark
			,max(last_event_mark)		as last_event_mark
			,sum(num_events_in_logs)	as size_of_logs
			,min(hour_received)			as hour_received
	into	measure_base_stage_2
	from 	#measure_base_stage_1
	group 	by 	subscriber_id
				,log_date

	commit
	create hg index fake_hg1 on measure_base_stage_2(subscriber_id)
	commit
	MESSAGE cast(now() as timestamp)||' | @ M05.0: Preparing data for slicing 30days stats DONE' TO CLIENT
	
	
	-- Preparing the data to slice the 7 days stats...
	
	if object_id ('vespa_analysts.measure_base_stage_3') is not null
		drop table measure_base_stage_3
		
	commit
	
	select	subscriber_id
			,convert(date, log_received_date)	as log_date
			,count(1) 							as num_of_calls
			,min(first_event_mark)				as first_event_mark
			,max(last_event_mark)				as last_event_mark
			,sum(num_events_in_logs)			as size_of_logs
			,min(hour_received)					as hour_received
	into	measure_base_stage_3
	from 	(
				select	*
				from	#measure_base_stage_1
				where	log_received_date+7> @profiling_day
                and     subscriber_id > 0
			)	as stage_3
	group 	by 	subscriber_id
				,log_date
	
	commit --> 12 mins up to here... is too much
	create hg index fake_hg1 on measure_base_stage_3(subscriber_id)
	commit
	MESSAGE cast(now() as timestamp)||' | @ M05.0: Preparing data for slicing 7days stats DONE' TO CLIENT
	
	drop table #measure_base_stage_1
	
	commit
	MESSAGE cast(now() as timestamp)||' | @ M05.0: Initialisation DONE' TO CLIENT
	
	-- [ NFQA ]
		
-------------------------------------------------------
-- M05.1 - Deriving Metrics for Panel Performance on DP
-------------------------------------------------------

	MESSAGE cast(now() as timestamp)||' | Beginig M05.1 - Deriving Metrics for Panel Performance on DP' TO CLIENT

	-- Num_logs_sent_30d
	-- reporting_quality
	-- avg_events_in_logs
	-- total_calls_in_30d

	select	subscriber_id
			,min(log_date) as start_scan
	into	#first_reports 
	from	measure_base_stage_2
	group	by subscriber_id
	
	update	#first_reports
    set 	start_scan = 	case	when start_scan > dateadd(day, -30, @profiling_day) then start_scan
									else dateadd(day, -29, @profiling_day)
							end

	update	#first_reports
    set 	start_scan =	case	when start_scan > enablement_date then start_scan
									else enablement_date
							end
    from 	m04_t1_panel_sample_stage0	as m04
    where	#first_reports.subscriber_id = m04.subscriber_id						
			
	delete from #first_reports where   start_scan > @profiling_day
	commit
	
	-- Since grouping is not supported when updating a table in Sybase, temp table it is...		
	select  base.subscriber_id
			,count(distinct (case when base.log_date > dateadd(day,-30,@profiling_day) then base.log_Date else null end))   as logs_in_30d
			,case when max(panel_sample.panel) in('VESPA','VESPA11','ALT5')	then convert(float, datediff(day, min(start_scan), @profiling_day)+1)         end as full_dividend
			,round((case when max(panel_sample.panel) in ('ALT6','ALT7')    then ((convert(float, datediff(day, min(start_scan), @profiling_day)+1))/2)   end),0) as alte_dividend
			,case   when max(panel_sample.panel) in ('VESPA','VESPA11','ALT5')
						then cast(logs_in_30d as float)/full_dividend
					when max(panel_sample.panel) in ('ALT6','ALT7')
						then cast(logs_in_30d as float)/alte_dividend
			end     as reporting_quality
			,sum(base.num_of_calls)         as total_calls_in_30d --> this is a potential metric we could start using in the future...
			,round(avg(size_of_logs),2) as avg_events_in_logs
	into	#temp_shelf
	from    measure_base_stage_2                    as base
			inner join #first_reports				as fr
			on	base.subscriber_id = fr.subscriber_id
			left join m04_t1_panel_sample_stage0    as panel_sample
			on  base.subscriber_id = panel_sample.subscriber_id
	group   by  base.subscriber_id


	update	m05_t1_panel_performance_stage0
	set		num_logs_sent_30d = shelf.logs_in_30d
			,reporting_quality =	shelf.reporting_quality
			,avg_events_in_logs = shelf.avg_events_in_logs
			,total_calls_in_30d = shelf.total_calls_in_30d
	from	m05_t1_panel_performance_stage0	as m05
			inner join	#temp_shelf	as shelf
			on	m05.subscriber_id = shelf.subscriber_id

	drop table #temp_shelf
	drop table #first_reports

	commit --> 26 secs
	MESSAGE cast(now() as timestamp)||' | @ M05.1: Deriving Logs sent in 30d, Rep Qual, size of logs and total calls DONE' TO CLIENT
	
	
	-- Num_logs_sent_7d

	-- Since grouping is not supported when updating a table in Sybase, temp table it is...		
	select  subscriber_id
			,count(distinct log_date)	as logs_in_7d
			,sum(num_of_calls)         	as total_calls_in_7d --> this is a potential metric we could start using in the future...
			,round(avg(size_of_logs),2) as avg_events_in_logs
	into	#temp_shelf
	from    measure_base_stage_3
	group   by  subscriber_id

	update	m05_t1_panel_performance_stage0
	set		num_logs_sent_7d = shelf.logs_in_7d
	from	m05_t1_panel_performance_stage0	as m05
			inner join	#temp_shelf	as shelf
			on	m05.subscriber_id = shelf.subscriber_id

	drop table #temp_shelf

	commit
	MESSAGE cast(now() as timestamp)||' | @ M05.1: Deriving logs sent in 7d DONE' TO CLIENT
			
	-- Continued_trans_30d

	-- To get the intervales we first want to get at what date did each interval starts and ends so we can do a date diff 
	-- and hence the interval lenght...

	-- getting all starting dates of the intervals...
	select	r.subscriber_id
			,r.log_date
			,rank() over (partition by r.subscriber_id order by r.log_date) as interval_sequencer
	into 	#intervals_starts
	from 	measure_base_stage_2 as l
			right join measure_base_stage_2 as r
			on	l.subscriber_id = r.subscriber_id
			and	l.log_date+1 = r.log_date
	where 	l.subscriber_id is null

	-- getting all ending dates of the intervals...
	select	l.subscriber_id
			,l.log_date
			,rank() over (partition by l.subscriber_id order by l.log_date) as interval_sequencer
	into 	#intervals_ends    
	from 	measure_base_stage_2 as l
			left join measure_base_stage_2 as r
			on	l.subscriber_id = r.subscriber_id
			and l.log_date+1 = r.log_date
	where 	r.subscriber_id is null

	-- per box, what was the max interval it had on last 30 days...
	select  l.subscriber_id
			,max(datediff(day, l.log_date, r.log_date) +1) as interval_length
	into	#temp_shelf
	from 	#intervals_starts as l
			inner join #intervals_ends as r
			on	l.subscriber_id = r.subscriber_id
			and	l.interval_sequencer = r.interval_sequencer
	group   by  l.subscriber_id

	/* -> POTENTIAL FOR A NEW PANEL PERFORMANCE TABLE <- */

	-- Updating output table with max interval per box...
	update	m05_t1_panel_performance_stage0
	set		Continued_trans_30d = shelf.interval_length
	from 	m05_t1_panel_performance_stage0	as m05
			inner join #temp_shelf			as shelf
			on	m05.subscriber_id = shelf.subscriber_id

	commit

	drop table #intervals_starts
	drop table #intervals_ends
	drop table #temp_shelf
	--drop table measure_base_stage_2
	
	commit
	MESSAGE cast(now() as timestamp)||' | @ M05.1: Deriving Continuity Over 30d DONE' TO CLIENT
	
	-- Continued_trans_7d

	select	r.subscriber_id
			,r.log_date
			,rank() over (partition by r.subscriber_id order by r.log_date) as interval_sequencer
	into 	#intervals_starts
	from 	measure_base_stage_3 as l
			right join measure_base_stage_3 as r
			on	l.subscriber_id = r.subscriber_id
	and 	l.log_date+1 = r.log_date
	where 	l.subscriber_id is null


	select	l.subscriber_id
			,l.log_date
			,rank() over (partition by l.subscriber_id order by l.log_date) as interval_sequencer
	into 	#intervals_ends    
	from 	measure_base_stage_3 as l
			left join measure_base_stage_3 as r
			on	l.subscriber_id = r.subscriber_id
	and 	l.log_date+1 = r.log_date
	where 	r.subscriber_id is null


	-- per box, what was the max interval it had on last 7 days...
	select  l.subscriber_id
			,max(datediff(day, l.log_date, r.log_date) +1) as interval_length
	into	#temp_shelf
	from 	#intervals_starts as l
			inner join #intervals_ends as r
			on	l.subscriber_id = r.subscriber_id
			and	l.interval_sequencer = r.interval_sequencer
	group   by  l.subscriber_id


	-- Updating output table with max interval per box...
	update	m05_t1_panel_performance_stage0
	set		Continued_trans_7d = shelf.interval_length
	from 	m05_t1_panel_performance_stage0	as m05
			inner join #temp_shelf			as shelf
			on	m05.subscriber_id = shelf.subscriber_id

	commit

	drop table #intervals_starts
	drop table #intervals_ends
	drop table #temp_shelf
	--drop table measure_base_stage_3

	commit
	MESSAGE cast(now() as timestamp)||' | @ M05.1: Deriving Continuity Over 7d DONE' TO CLIENT


	/*

	-- avg_reporting_quality -- se puede derivar al final cuando este construyendo el output table...
	-- min_reporting_quality -- se puede derivar al final cuando este construyendo el output table...
	-- Reporting_performance

	*/



	-------------------------------------------------------
	-- M05.2 - Deriving Metrics for Panel Performance on AP
	-------------------------------------------------------
	
	MESSAGE cast(now() as timestamp)||' | Beginig M05.2 - Deriving Metrics for Panel Performance on AP' TO CLIENT
	
	-- Num_logs_sent_30d
	-- reporting_quality

	-- Since Sybase doesn't allow to group on Update commands, storing results into a temp shelf...

	select  cast(alt.subscriber_id as integer)      as subscriber_id
			,alt.panel
			,count(distinct (case when alt.dt > dateadd(day,-30,@profiling_day) then alt.dt else null end)) as Num_logs_sent_30d
			,count(distinct (case when alt.dt > dateadd(day,-7,@profiling_day) then alt.dt else null end))	as Num_logs_sent_7d
			,min(alt.dt) as scan1
			,case   when scan1 > dateadd(day,-30,@profiling_day) then scan1
					else dateadd(day,-29,@profiling_day)
			end     as scan2
			,case   when scan2 > min(m04.enablement_date) then scan2
					else min(m04.enablement_date)
			end     scan3
			,case   when scan3 > @profiling_day then @profiling_day
					else scan3
			end     as start_scan
			,case   when alt.panel = 5 then convert(float, datediff(day, start_scan, @profiling_day)+1)
					else ((convert(float, datediff(day, start_scan, @profiling_day)+1))/2)
			end     as dividend
			,cast(Num_logs_sent_30d as float) / dividend as reporting_quality
	into	#temp_shelf
	from    vespa_analysts.panel_data           as alt
			inner join  m04_t1_panel_sample_stage0  as m04
			on  cast(alt.subscriber_id as integer) = m04.subscriber_id
			and m04.panel in ('ALT6','ALT7','ALT5')
			inner join m05_t1_panel_performance_stage0  as m05
			on  m04.subscriber_id = m05.subscriber_id
	where   alt.dt > @from_dt 
	and     alt.dt <= @to_dt
	and     alt.data_received = 1
	group   by  subscriber_id
				,alt.panel

	commit

	-- updating metrics for AP...

	update	m05_t1_panel_performance_stage0
	set		Num_logs_sent_30d	= shelf.Num_logs_sent_30d
			,Num_logs_sent_7d	= shelf.Num_logs_sent_7d
			,reporting_quality 	= shelf.reporting_quality
	from	m05_t1_panel_performance_stage0	as m05
			inner join #temp_shelf			as shelf
			on	m05.subscriber_id = shelf.subscriber_id

	commit

	drop table #temp_shelf

	commit
	MESSAGE cast(now() as timestamp)||' | @ M05.2: Deriving Logs Sent Over 30d, Rep Qual DONE' TO CLIENT

/*

-- avg_reporting_quality -- se puede derivar al final cuando este construyendo el output table...
-- min_reporting_quality -- se puede derivar al final cuando este construyendo el output table...
-- Reporting_performance

*/

	-----------------------------------------
	-- M05.3 - General Panel Performance KPIs
	-----------------------------------------
	
	MESSAGE now() || '| M05.3 - General Panel Performance KPIs' TO CLIENT
	
	-- AT BOX LEVEL
	-- return_data_7d...
	-- return_data_30d...
	
	update	m05_t1_panel_performance_stage0 as m05
	set		return_data_30d	=	case	when m04.panel in ('ALT6','ALT7') and m05.Num_logs_sent_30d >= 15	then 1
										when m05.Num_logs_sent_30d >= 30 									then 1
                                        else 0
								end
			,return_data_7d =	case	when m04.panel in ('ALT6','ALT7') and m05.Num_logs_sent_7d >= 3	then 1
										when m05.Num_logs_sent_7d >= 7 								    then 1
                                        else 0
								end
	from	M04_t1_panel_sample_stage0		as m04
	where	m04.subscriber_id	= m05.subscriber_id
	
	commit
	
	MESSAGE now() || '| @ M05.3: updating return data KPI for both 30/7 days DONE' TO CLIENT
	
	---------------------------------------------------------------
	
	-- num_ac_returned_30d
	-- num_ac_returned_7d
	-- ac_full_returned_30d
	-- ac_full_returned_7d

	declare @todt	date	

	-- calculating the date for last Saturday... which is the end of the week for our time frame...
	-- this is the mark from where we analyse the performance of boxes and accounts back into 30 days...
	select  @todt =	case	when datepart(weekday,today()) = 7 then today()
							else (today() - datepart(weekday,today()))
					end
    
	-- a list of accounts and how many boxes each has...
	select  panel		
			,account_number
			,count(distinct subscriber_id) 	as num_boxes
			,min(enablement_date)			as enablement_date
	into	#acview
	from    m04_t1_panel_sample_stage0
	where	panel is not null
	group   by  panel
				,account_number
	
	commit
	create hg index hg1 on #acview(account_number)
	create lf index lf1 on #acview(panel)
	commit
	
	MESSAGE now() || '| @ M05.3: Constructing ACVIEW DONE' TO CLIENT
	
	-- counting for each day on the past 30 days the number of boxes that dialed
	-- for every single account...
	select  perf.dt
			,boxview.account_number
			,count(distinct perf.subscriber_id) as dialling_b
	into	#panel_data
	from    vespa_analysts.panel_data               as perf
			inner join  m04_t1_panel_sample_stage0	as boxview
			on  perf.subscriber_id = boxview.subscriber_id
			and boxview.panel is not null
			and boxview.status_vespa = 'Enabled'
			and	boxview.panel in ('ALT5','ALT6','ALT7')
	where   perf.panel is not null
	and     perf.data_received = 1
	and     perf.dt between @todt-29 and @todt
	group   by  perf.dt 
				,boxview.account_number
	
	commit
	create date index date1 on #panel_data(dt)
	create hg index hg1 	on #panel_data(account_number)
	commit

	MESSAGE now() || '| @ M05.3: Constructing PANEL DATA Snapshot DONE' TO CLIENT
	
	-- For AP
		
	select  acview.panel
			,acview.account_number
			,count(distinct panel_data.dt)                                                                      as num_ac_returned_30d
			,count(distinct (case when panel_data.dt > dateadd(day,-7,@todt) then panel_data.dt else null end)) as num_ac_returned_7d
			,case	when acview.panel in ('ALT6','ALT7') and num_ac_returned_30d >= 15  then 1
					when num_ac_returned_30d >= 30 									    then 1
					else 0
			end     as ac_full_returned_30d
			,case	when acview.panel in ('ALT6','ALT7') and num_ac_returned_7d >= 3    then 1
					when num_ac_returned_7d >= 7								        then 1
					else 0
			end     as ac_full_returned_7d
	into    #AP_ac_performance
	from    #acview	as acview
			inner join  #panel_data	as panel_Data
			on  acview.account_number   = panel_data.account_number
	where   panel_data.dialling_b >= acview.num_boxes
	group   by  acview.panel
				,acview.account_number

	commit
	create hg index hg1 on #ap_ac_performance(Account_number)
	commit
	
	MESSAGE now() || '| @ M05.3: Measuring AP Performance DONE' TO CLIENT
	
	update	m05_t1_panel_performance_stage0	as m05
	set		num_ac_returned_30d		=	base.num_ac_returned_30d
	        ,num_ac_returned_7d  	=	base.num_ac_returned_7d
	        ,ac_full_returned_30d	=	base.ac_full_returned_30d
	        ,ac_full_returned_7d 	=	base.ac_full_returned_7d
	from	#AP_ac_performance	as base
	where	base.account_number	=	m05.account_number
	
	commit
	drop table #ap_ac_performance
	commit
	
	MESSAGE now() || '| @ M05.3: Commiting AP performance DONE' TO CLIENT
	
	-- For DP
	
	select  account_number
			,count(distinct dt)                                                             as num_ac_returned_30d
			,count(distinct (case when dt > dateadd(day,-7,@todt) then dt else null end))   as num_ac_returned_7d
			,case when num_ac_returned_30d >= 30    then 1 else 0 end                       as ac_full_returned_30d
			,case when num_ac_returned_7d >= 7      then 1 else 0 end                       as ac_full_returned_7d
	into    #viq_data
	from    (
				select  adjusted_event_start_Date_vespa as dt
						,account_number
				from    sk_prod.VIQ_VIEWING_DATA_SCALING
				where   adjusted_event_start_Date_vespa between @todt-29 and @todt
			)   as base
	group   by  account_number
	
	commit
	create hg index hg1 	on #viq_data(account_number)
	commit
	
	MESSAGE now() || '| @ M05.3: Measuring DP performance DONE' TO CLIENT
	
	update	m05_t1_panel_performance_stage0	as m05
	set		num_ac_returned_30d		=	base.num_ac_returned_30d
	        ,num_ac_returned_7d  	=	base.num_ac_returned_7d
	        ,ac_full_returned_30d	=	base.ac_full_returned_30d
	        ,ac_full_returned_7d 	=	base.ac_full_returned_7d
	from	#viq_data	as base
	where	base.account_number	=	m05.account_number
	
	commit
	drop table #viq_data
	commit
	
	MESSAGE now() || '| @ M05.3: Commiting DP performance DONE' TO CLIENT
	
	-- reporting_quality_s
	/*
		As part of a new KPI since 07/10/2014, we now want to check the frequency at which
		an account is a candidate for the scaling sample over the last 30 days 
		This KPI will have the shape of a ratio hence the formula is plain simple as:
		
		X = num of days where an account returned data / (30 if you are in panel 12,11,5 or 15 if you are in 6,7)
	*/
	
	-- treating accounts on the AP
	
	/*
		Since this new KPI is based on Scaling and the AP does not participate in this
		process, we are replicating here the same business rules to select accounts for
		scaling so we can flag the SCALING CANDIDATES in the AP
	*/

    if object_id('vespa_analysts.rq_scaling_final') is not null
        drop table rq_scaling_final

    commit

    select  acview.panel
			,acview.account_number
			,count(distinct dial.dt)    as dials
			,case   when @todt-29 <= min(acview.enablement_date)  then 
																		case    when min(acview.enablement_date)>@todt then @todt
																				else min(acview.enablement_date)
																		end
					else @todt-29
			end     as start_scan
			,cast(dials as float) / cast    (
												(
													case    when acview.panel = 'ALT5' then convert(float, datediff(day, start_scan, @todt)+1)
															else ((convert(float, datediff(day, start_scan, @todt)+1))/2)
													end
												)   as float
											)   as RQ
    into    rq_scaling_final
	from    #panel_data			as dial
			inner join #acview  as acview
			on  dial.account_number = acview.account_number
			and dial.dialling_b >= acview.num_boxes -- This is the condition that flags whether an account returned data or not
	group   by  acview.panel
				,acview.account_number

    commit
    create lf index lf1 on rq_scaling_final(panel)
    create hg index hg1 on rq_scaling_final(account_number)
    commit
	
	MESSAGE now() || '| @ M05.3: Deriving Scaling Candidates in AP + RQ (Accounts returning data definition) DONE' TO CLIENT
	
	-- treating accounts on the DP
	
    insert  into rq_scaling_final
	select  acview.panel
			,acview.account_number
			,count(distinct viq.adjusted_event_start_date_vespa)                        as dials
			,case   when @todt-29 <= min(acview.enablement_date)  then min(acview.enablement_date)
					else @todt-29
			end     as start_scan
			,cast(dials as float) / convert(float, datediff(day, start_scan, @todt)+1)  as RQ
	from    sk_prod.VIQ_VIEWING_DATA_SCALING    as viq
			inner join #acview              as acview
			on  viq.account_number  = acview.account_number
			and acview.panel in ('VESPA','VESPA11')
	where   viq.adjusted_event_start_date_vespa between @todt-29 and @todt
	group   by  acview.panel
				,acview.account_number

    commit
	
	MESSAGE now() || '| @ M05.3: Deriving Scaling Candidates in DP + RQ (Accounts returning data definition) DONE' TO CLIENT
	
	drop table #panel_data 
	drop table #acview
	commit

	update  m05_t1_panel_performance_stage0 as m05
	set     reporting_quality_s = rqs.rq
	from    rq_scaling_final                as rqs
	where   m05.account_number  = rqs.account_number
	
	commit
	drop table rq_scaling_final
	commit
	
	MESSAGE now() || '| @ M05.3: Commiting performance at Account level DONE' TO CLIENT
	
------------------------
-- M05.4 - QAing results
------------------------

----------------------------
-- M05.5 - Returning results
----------------------------

-- m05_t1_panel_performance_stage0...
	MESSAGE cast(now() as timestamp)||' | M05 Finished, table m05_t1_panel_performance_stage0 BUILT' TO CLIENT

end
else
begin
	MESSAGE cast(now() as timestamp)||' | Exiting M05: Required Input not found or empty (M04_t1_panel_sample_stage0)' TO CLIENT
end

end;

commit;
grant execute on sig_masvg_m05_panel_performance to vespa_group_low_security;
commit;