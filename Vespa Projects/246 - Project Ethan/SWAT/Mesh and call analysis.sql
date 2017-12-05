--                                            '#                                 
--                                            '#                                 
--                                            '#                                 
--                                            '#                                 
--                                            '#                                 
--                                            '#                                 
--              '##                           '#                                 
--              ###                           '#                                 
--             .###                           '#                                 
--             .###                           '#                                 
--     .:::.   .###       ::         ..       '#       .                   ,:,   
--   ######### .###     #####       ###.      '#      '##  ########`     ########
--  ########## .###    ######+     ####       '#      '##  #########'   ########'
-- ;#########  .###   +#######     ###;       '#      '##  ###    ###.  ##       
-- ####        .###  '#### ####   '###        '#      '##  ###     ###  ##       
-- '####+.     .### ;####  +###:  ###+        '#      '##  ###      ##  ###`     
--  ########+  .###,####    #### .###         '#      '##  ###      ##. ;#####,  
--  `######### .###`####    `########         '#      '##  ###      ##.  `######`
--     :######`.### +###.    #######          '#      '##  ###      ##      .####
--         ###'.###  ####     ######          '#      '##  ###     ;##         ##
--  `'':..+###:.###  .####    ,####`          '#      '##  ###    `##+         ##
--  ########## .###   ####.    ####           '#      '##  ###   +###   ;,    +##
--  #########, .###    ####    ###:           '#      '##  #########    ########+
--  #######;   .##:     ###+  '###            '#      '##  '######      ;######, 
--                            ###'            '#                                 
--                           ;###             '#                                 
--                           ####             '#                                 
--                          :###              '#                                 
--                                            '#                                 
--                                            '#                                 
--                                            '#                                 
--                                            '#                                 
--                                            '#                                 
--                                            '#                                 
-- ------------------------------------------------------------------------------
-- ------------------------------------------------------------------------------
-- Mesh and call analysis.sql
-- 2016-08-05
--
-- Environment:
-- Olive
-- 
--
-- Function: 
-- SWAT survey analysis
--
-- Source tables/files:
-- 	thompsonja.swat_data_adjusted_jon -- 1 record per account/row_id
--	thompsonja.QSWAT_Mesh_data -- normalised from above to show 1 record per mesh reading
--	/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/tanghoi/2016-08-10_Visit_data.csv
-- 
-- ------------------------------------------------------------------------------



--------------------------------------------------------------------------------
-- Extract unique accounts and installation dates
--------------------------------------------------------------------------------
drop table swat_3k_accounts;
select
		cast(a.row_id as int)					as	row_id
	,	cast(a.account_number as varchar(20))	as	account_number
	,	cast(a.visit_date as date)				as	install_date
	,	b.connectivity_RAG						as	install_RAG
into	swat_3k_accounts
from
				thompsonja.swat_data_adjusted_jon	a
	left join	thompsonja.QSWAT_Mesh_data			b 	on	a.row_id		=	b.row_id
														and	cast(a.visit_date as date)	=	cast(b.mesh_date as date)
where	MESH_ALL_TIME_Matched	=	0	-- filter to accounts which have both call and connectivity data only
group by	-- should be unique already, but just to be sure!
		a.row_id
	,	a.account_number
	,	install_date
	,	install_RAG
order by
		a.row_id
	,	a.account_number
	,	install_date
	,	install_RAG
;
create unique hg index uhg1 on swat_3k_accounts(account_number);
create unique hg index uhg2 on swat_3k_accounts(row_id);
create date index dttm1 on swat_3k_accounts(install_date);
create lf index lf1 on swat_3k_accounts(install_RAG);


--------------------------------------------------------------------------------
-- Normalise call data
--------------------------------------------------------------------------------
drop table QSWAT_Call_data;
select
    	row_id
    ,	account_number
    ,	cast(visit_date as date)			as	visit_date
    ,	card
    ,			CALL_DATA_Call_Time_1		as	call_timestamp
    ,	date(	CALL_DATA_Call_Time_1	)	as	call_date
    ,			CALL_DATA_Last_SCT_Group_1 	as	SCT_group
    ,			CALL_DATA_Last_SCT_1 		as	SCT
into	QSWAT_Call_data
from	thompsonja.swat_data_adjusted_jon
where	call_date between '2016-06-10' and '2016-06-28'
;


create or replace variable @i int = 1;
create or replace variable @sql_ varchar(2048);
while	@i < 20
	begin
		set @i = @i + 1
		commit

		-- select @i, cast(@i as varchar(64))
		-- commit

		set @sql_ = 
			'
			insert into QSWAT_Call_data
			select
			    	row_id
			    ,	account_number
			    ,	cast(visit_date as date)	visit_date
			    ,	card
			    ,	CALL_DATA_Call_Time_' || cast(@i as varchar(64)) || '		as call_timestamp
			    ,	date(CALL_DATA_Call_Time_' || cast(@i as varchar(64)) || ')	as call_date
			    ,	CALL_DATA_Last_SCT_Group_' || cast(@i as varchar(64)) || ' 	as SCT_group
			    ,	CALL_DATA_Last_SCT_' || cast(@i as varchar(64)) || ' 		as SCT
			from
			    thompsonja.swat_data_adjusted_jon
			where
			    call_date between ''' || '2016-06-10' || ''' and ''' || '2016-06-28' || '''
			'
		commit

		execute(@sql_)
		commit

	end
;



--------------------------------------------------------------------------------
-- Summarise number of red MESH days up to 14 days post-install
--------------------------------------------------------------------------------
drop table QSWAT_account_calls;
select	/*top 1000*/
		t0.account_number
	,	t0.row_id
	,	t0.install_date
	,	t0.install_RAG
	,	t0.days_red
	,	t0.red_days_post_install
	,	t1.install_day_calls
	,	t1.post_install_day_calls
into	QSWAT_account_calls
from			(
					select	-- top 100
							a.account_number
						,	a.row_id
						,	a.install_date
						,	a.install_RAG
						,	sum	(
									case
										when	b.row_id is NUll	then	0
										else								1
									end
								)	as	days_red
						,	case
								when	days_red	=	0	then	'No red days'
								when	days_red	=	1	then	'1 red day'
								when	days_red	>	1	then	'>1 red days'
								else								NULL
							end		as	red_days_post_install
					from
									swat_3k_accounts			a
						left join	thompsonja.QSWAT_Mesh_data	b	on	a.row_id			=		cast(b.row_id as int)
																	and	b.connectivity_RAG	=		'R'
																	and	b.mesh_date			between	dateadd(day,1,a.install_date)
																							and		dateadd(day,14,a.install_date)
					group by
							a.account_number
						,	a.row_id
						,	a.install_date
						,	a.install_RAG
					-- order by
					-- 		a.account_number
					-- 	,	a.row_id
					-- 	,	a.install_date
					-- 	,	a.install_RAG
				)	t0
	left join	(
					select	-- top 100
							a.account_number
						,	a.row_id
						,	a.install_date
						,	a.install_RAG
						,	sum	(
									case	b.call_date
										when	a.install_date	then	1
										else							0
									end
								)	as	install_day_calls
						,	sum	(
									case
										when	b.call_date > a.install_date	then	1
										else											0
									end
								)	as	post_install_day_calls
					from
									swat_3k_accounts	a
						left join	QSWAT_Call_data		b	on	a.account_number	=		b.account_number
															and	b.call_date			between	a.install_date
																					and		dateadd(day,14,a.install_date)
															and	SCT_group			in		('Sky Q TV Tech','Sky Q BB&T Tech')
					group by
							a.account_number
						,	a.row_id
						,	a.install_date
						,	a.install_RAG
				)	t1	on	t0.account_number	=	t1.account_number
						and	t0.row_id			=	t1.row_id
						and	t0.install_date		=	t1.install_date
						and	t0.install_RAG		=	t1.install_RAG
order by
		t0.account_number
	,	t0.row_id
	,	t0.install_date
	,	t0.install_RAG
;




--------------------------------------------------------------------------------
-- Calculate connectivity RAG offsets and identify installation RAG
--------------------------------------------------------------------------------
drop table #offset
;

select 	row_num	as	call_date_offset
into	#offset
from	sa_rowgenerator(-2,3)
;

drop table QSWAT_Call_data_with_lags;
select
		c.row_id
	,	c.account_number
	,	a.install_date
	,	c.card
	,	c.call_timestamp
	,	c.call_date
	,	c.SCT_group
	,	c.SCT
	,	a.install_RAG
	,	o.call_date_offset
	,	m1.mesh_date
	,	m1.connectivity_RAG
	,	case
			when	call_date_offset	<=	0 	then	'pre-call'
			when	call_date_offset	>	0 	then	'post-call'
			else										NULL
		end												as 	pre_post
into 	QSWAT_Call_data_with_lags
from
    			QSWAT_Call_data				c
	cross join	#offset						o
    left join	thompsonja.QSWAT_Mesh_data	m1 	on	c.row_id		=	m1.row_id
												and	m1.mesh_date	=	dateadd(day, o.call_date_offset, c.call_date)
    left join	swat_3k_accounts			a 	on	c.account_number	=	a.account_number
where	c.call_date	>	c.visit_date
order by
		c.row_id
	,	c.account_number
	,	a.install_date
	,	c.card
	,	c.call_timestamp
	,	c.call_date
	,	c.SCT_group
	,	c.SCT
	,	a.install_RAG
	,	o.call_date_offset
	,	m1.mesh_date
	,	m1.connectivity_RAG
	,	pre_post
;



--------------------------------------------------------------------------------
-- Summarise to show install/pre-call/post-call RAG status per account/call
--------------------------------------------------------------------------------
drop table QSwat_pre_post_call_MESH;
select
		row_id
	,	account_number
	,	install_date
	,	call_timestamp
	,	call_date
	,	SCT_group
	,	SCT
	,	install_RAG
	,	max	(
				case	pre_post
					when	'pre-call'	then	pre_post_RAG
					else						NULL
				end
			)	as	pre_call_RAG
	,	max	(
				case	pre_post
					when	'post-call'	then	pre_post_RAG
					else						NULL
				end
			)	as	post_call_RAG
into	QSwat_pre_post_call_MESH
from	(
			select
					row_id
				,	account_number
				,	install_date
				,	call_timestamp
				,	call_date
				,	SCT_group
				,	SCT
				,	install_RAG
				,	pre_post
				,	case 	(
								max	(
										case 	connectivity_RAG
											when	'R'		then	3
											when	'A'		then	2
											when	'G'		then	1
											else					NULL
										end
									)
							)
						when	3	then	'R'
						when	2	then	'A'
						when	1	then	'G'
						else				NULL
					end		as 	pre_post_RAG
			from	QSWAT_Call_data_with_lags
			where
					install_RAG is not null
				and	SCT_group	in	('Sky Q TV Tech','Sky Q BB&T Tech')
			group by
					row_id
				,	account_number
				,	install_date
				,	call_timestamp
				,	call_date
				,	SCT_group
				,	SCT
				,	install_RAG
				,	pre_post
			-- order by
			-- 		row_id
			-- 	,	account_number
			-- 	,	call_timestamp
			-- 	,	call_date
			-- 	,	install_RAG
			-- 	,	pre_post
		)	t0
group by
		row_id
	,	account_number
	,	install_date
	,	call_timestamp
	,	call_date
	,	SCT_group
	,	SCT
	,	install_RAG
order by
		cast(row_id as int)
	,	account_number
	,	install_date
	,	call_timestamp
	,	call_date
	,	SCT_group
	,	SCT
	,	install_RAG
;
select * from QSwat_pre_post_call_MESH;


-- High-level aggregate of pre-call RAG accounts
select
		pre_call_RAG
	,	count()							as	calls
	,	count(distinct account_number)	as	accounts
from	QSwat_pre_post_call_MESH
group by	pre_call_RAG
order by	pre_call_RAG
;



/*	-- Load visit data
create or replace variable @input_file_path varchar(256) = '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/tanghoi/2016-08-10_Visit_data.csv';
 
-- Prepare temporary import table
drop table #tmp_undelimited;

create table #tmp_undelimited	(
		job_timestamp	varchar(8000)
	,	tech_number	varchar(8000)
	,	job_number	varchar(8000)
	,	account_number	varchar(20)
	,	installation_type	varchar(8000)
	,	hub_router_type	varchar(8000)
	,	visit_reason	varchar(8000)
	,	chrome_mesh_state_pre_visit	varchar(8000)
	,	resolution_actions	varchar(8000)
	,	Could_the_issue_have_been_resolved_prior_to_the_visit	varchar(8000)
	,	What_specifically_could_have_been_done_to_resolve_the_issues_prior_to_this_visit	varchar(8000)
	,	What_equipment_did_you_use_on_this_Service_visit	varchar(8000)
	,	Were_there_any_aspects_that_you_were_unable_to_resolve	varchar(8000)
	,	chrome_mesh_state_post_visit	varchar(8000)
	,	Is_there_any_other_information_you_feel_is_relevant_to_this_visit	varchar(8000)
								)
;


-- Read the data
create or replace variable @sql_ varchar(8000);
set	@sql_	=	'
load table #tmp_undelimited	(
		job_timestamp	'',''
	,	tech_number	'',''
	,	job_number	'',''
	,	account_number	'',''
	,	installation_type	'',''
	,	hub_router_type	'',''
	,	visit_reason	'',''
	,	chrome_mesh_state_pre_visit	'',''
	,	resolution_actions	'',''
	,	Could_the_issue_have_been_resolved_prior_to_the_visit	'',''
	,	What_specifically_could_have_been_done_to_resolve_the_issues_prior_to_this_visit	'',''
	,	What_equipment_did_you_use_on_this_Service_visit	'',''
	,	Were_there_any_aspects_that_you_were_unable_to_resolve	'',''
	,	chrome_mesh_state_post_visit	'',''
	,	Is_there_any_other_information_you_feel_is_relevant_to_this_visit	''\n''
							)
from ''' || @input_file_path || '''
QUOTES OFF
ESCAPES OFF
SKIP 1
NOTIFY 1000
'
;

execute (@sql_);

-- Convert to Sybase timestamp and write to table
drop table visit_data;
select	--top 100
		cast(
    			left(job_timestamp,10) || ' ' || left	(
																case
																   	when	substr(job_timestamp,13,1) = ':'	and	job_timestamp like '%AM%'	then	'0' || substr(job_timestamp,12)
																   	when	substr(job_timestamp,13,1) = ':'	and	job_timestamp like '%PM%'	then	cast(cast(substr(substr(job_timestamp,12),1,1) as int) + 12 as varchar(20)) || substr(substr(job_timestamp,12),2)
																   	when	substr(job_timestamp,13,1) <> ':'									then	substr(job_timestamp,12)
																   	else	NULL
																end
	        												,	8
	        											)
	            as timestamp
			)	job_dt
	,	*
into	visit_data
from	#tmp_undelimited
;


*/



--------------------------------------------------------------------------------
-- Calculate connectivity RAG offsets and identify installation RAG for visits
--------------------------------------------------------------------------------
drop table #offset
;

select 	row_num
into	#offset
from	sa_rowgenerator(-2,3)
;

drop table QSWAT_visit_data_with_lags;
select	/*top 100*/
    	v.account_number
    ,	a.row_id
    ,	a.install_date
    ,	a.install_RAG
	,	v.job_dt
    ,	v.tech_number
    ,	v.job_number
	,	o.row_num	as	offset_days
	,	m.mesh_date
	,	m.connectivity_RAG
	,	case
			when	offset_days	<=	0 	then	'pre-visit'
			when	offset_days	>	0 	then	'post-visit'
			else								NULL
		end			as 	pre_post
into	QSWAT_visit_data_with_lags
from
				visit_data					v
	cross join	#offset						o
	inner join	swat_3k_accounts			a	on	a.account_number	=	v.account_number
    inner join	thompsonja.QSWAT_Mesh_data	m 	on	a.row_id			=	cast(m.row_id as int)
												and	m.mesh_date			=	dateadd(day, o.row_num, date(v.job_dt))
where	v.job_dt	>	a.install_date
order by
    	v.account_number
    ,	a.row_id
    ,	a.install_date
    ,	a.install_RAG
	,	v.job_dt
    ,	v.tech_number
    ,	v.job_number
	,	offset_days
	,	m.mesh_date
	,	m.connectivity_RAG
	,	pre_post
;




--------------------------------------------------------------------------------
-- Summarise to show pre-/post-visit RAG status per account/visit
--------------------------------------------------------------------------------
drop table QSwat_pre_post_visit_MESH;
select
		t0.account_number
	,	t0.row_id
	,	t0.job_date
	,	t0.tech_number
	,	t0.job_number
	,	t0.install_RAG
	,	max	(
				case	t0.pre_post
					when	'pre-visit'	then	t0.pre_post_RAG
					else						NULL
				end
			)	as	pre_visit_RAG
	,	max	(
				case	t0.pre_post
					when	'post-visit'	then	t0.pre_post_RAG
					else							NULL
				end
			)	as	post_visit_RAG
into	QSwat_pre_post_visit_MESH
from	(
			select
					account_number
				,	row_id
				,	install_RAG
				,	date(job_dt)	job_date
				,	tech_number
				,	job_number
				,	pre_post
				,	case 	(
								max	(
										case 	connectivity_RAG
											when	'R'		then	3
											when	'A'		then	2
											when	'G'		then	1
											else					NULL
										end
									)
							)
						when	3	then	'R'
						when	2	then	'A'
						when	1	then	'G'
						else				NULL
					end		as 	pre_post_RAG
			from	QSWAT_visit_data_with_lags
			group by
					account_number
				,	row_id
				,	install_RAG
				,	job_date
				,	tech_number
				,	job_number
				,	pre_post
			-- order by
			-- 		account_number
			-- 	,	row_id
			-- ,	install_RAG
			-- 	,	job_date
			-- 	,	tech_number
			-- 	,	job_number
			-- 	,	pre_post
		)	t0
group by
		t0.account_number
	,	t0.row_id
	,	t0.job_date
	,	t0.tech_number
	,	t0.job_number
	,	t0.install_RAG
order by
		t0.account_number
	,	t0.row_id
	,	t0.job_date
	,	t0.tech_number
	,	t0.job_number
	,	t0.install_RAG
;





--------------------------------------------------------------------------------
-- Chrome app post-install mesh status vs. mesh diagnostics
--------------------------------------------------------------------------------
select
		acc.*
	,	t0.chrome_install_state
	,	t0.chrome_install_RAG
from
				swat_3k_accounts	acc
	left join	(
					select
							account_number
						,	case
								when	lower(INST_SURVEY_What_state_did_the_Chrome_app_show_at_the_end_of_the_install) like 	'%(red)%' then INST_SURVEY_What_state_did_the_Chrome_app_show_at_the_end_of_the_install
								when	lower(INST_SURVEY_What_state_did_the_Chrome_app_show_at_the_end_of_the_install) like 	'%(amber)%' then INST_SURVEY_What_state_did_the_Chrome_app_show_at_the_end_of_the_install
								when	lower(INST_SURVEY_What_state_did_the_Chrome_app_show_at_the_end_of_the_install) like 	'%(green)%' then INST_SURVEY_What_state_did_the_Chrome_app_show_at_the_end_of_the_install
								else																							'Other'
							end	as chrome_install_state
						,	case
								when	lower(INST_SURVEY_What_state_did_the_Chrome_app_show_at_the_end_of_the_install) like 	'%(red)%' then 'R'
								when	lower(INST_SURVEY_What_state_did_the_Chrome_app_show_at_the_end_of_the_install) like 	'%(amber)%' then 'A'
								when	lower(INST_SURVEY_What_state_did_the_Chrome_app_show_at_the_end_of_the_install) like 	'%(green)%' then 'G'
								else																							'Other'
							end	as chrome_install_RAG
						-- ,	count()
					from    thompsonja.swat_data_adjusted_jon
					-- where	chrome_install_RAG	<>	'Other'
					-- group by
					-- 		chrome_install_state
					-- 	,	chrome_install_RAG
					-- order by
					-- 		chrome_install_state
					-- 	,	chrome_install_RAG
				)					t0	on	acc.account_number	=	t0.account_number
;







--------------------------------------------------------------------------------
-- Second 3k cohort (5th and 6th August installs)
--------------------------------------------------------------------------------
/*
create or replace variable @input_file_path varchar(256) = '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/tanghoi/SWAT_installs_data_2nd_3k.csv';

-- Prepare temporary import table
drop table #tmp_undelimited;
create table #tmp_undelimited	(
		Account_Number	varchar(8000)
	,	Viewing_Card	varchar(8000)
	,	Sales_Date	varchar(8000)
	,	Sales_Time	varchar(8000)
	,	Sales_Agent	varchar(8000)
	,	RTM	varchar(8000)
	,	New_or_Existing	varchar(8000)
	,	Current_TV_Status	varchar(8000)
	,	Current_TV_Package	varchar(8000)
	,	Fms_Job_Reference	varchar(8000)
	,	Visit_Description	varchar(8000)
	,	Original_Visit_Dt	varchar(8000)
	,	Actual_Install_Date	varchar(8000)
	,	Actual_Install_Time	varchar(8000)
	,	Installer	varchar(8000)
	,	Service_Visits_Booked	varchar(8000)
	,	Service_Visit_Job_Ref	varchar(8000)
	,	Service_Booking_Date	varchar(8000)
	,	Service_Booking_Date_Time	varchar(8000)
	,	Service_Booking_Agent	varchar(8000)
	,	Service_Call_Description	varchar(8000)
	,	Service_Visit_Scheduled_Date	varchar(8000)
	,	Service_Visit_Status	varchar(8000)
	,	Call_Id	varchar(8000)
	,	Interaction_Id	varchar(8000)
	,	Call_Date	varchar(8000)
	,	Call_Time	varchar(8000)
	,	Last_Agent	varchar(8000)
	,	Last_SCT	varchar(8000)
	,	Last_SCT_Group	varchar(8000)
	,	Duration	varchar(8000)
	,	Call_Pre_or_Post	varchar(8000)
	,	Call_same_day_as_install	varchar(8000)
	,	Customer_Calls	varchar(8000)
	,	Transfers_or_Consults	varchar(8000)
	)
;


-- Read the data
create or replace variable @sql_ varchar(8000);
set	@sql_	=	'
load table #tmp_undelimited	(
		Account_Number	'',''
	,	Viewing_Card	'',''
	,	Sales_Date	'',''
	,	Sales_Time	'',''
	,	Sales_Agent	'',''
	,	RTM	'',''
	,	New_or_Existing	'',''
	,	Current_TV_Status	'',''
	,	Current_TV_Package	'',''
	,	Fms_Job_Reference	'',''
	,	Visit_Description	'',''
	,	Original_Visit_Dt	'',''
	,	Actual_Install_Date	'',''
	,	Actual_Install_Time	'',''
	,	Installer	'',''
	,	Service_Visits_Booked	'',''
	,	Service_Visit_Job_Ref	'',''
	,	Service_Booking_Date	'',''
	,	Service_Booking_Date_Time	'',''
	,	Service_Booking_Agent	'',''
	,	Service_Call_Description	'',''
	,	Service_Visit_Scheduled_Date	'',''
	,	Service_Visit_Status	'',''
	,	Call_Id	'',''
	,	Interaction_Id	'',''
	,	Call_Date	'',''
	,	Call_Time	'',''
	,	Last_Agent	'',''
	,	Last_SCT	'',''
	,	Last_SCT_Group	'',''
	,	Duration	'',''
	,	Call_Pre_or_Post	'',''
	,	Call_same_day_as_install	'',''
	,	Customer_Calls	'',''
	,	Transfers_or_Consults	''\n''
	)
from ''' || @input_file_path || '''
QUOTES OFF
ESCAPES OFF
SKIP 1
NOTIFY 1000
'
;
execute (@sql_);


-- Convert field types and write to permanent table (Note - one record per call)
drop table visit_data_cohort2;
select
		cast(Account_Number as varchar(20))	Account_Number
	,	cast(Viewing_Card as int)	Viewing_Card
	,	convert(date,left(Sales_Date,10),103)	Sales_Date
	,	cast(convert(date,left(Sales_Time,10),103) || ' ' || cast(right(Sales_Time,8) as time) as timestamp)	Sales_Time
	,	Sales_Agent
	,	RTM
	,	New_or_Existing
	,	Current_TV_Status
	,	Current_TV_Package
	,	Fms_Job_Reference
	,	Visit_Description
	,	convert(date,left(Original_Visit_Dt,10),103)	Original_Visit_Dt
	,	convert(date,left(Actual_Install_Date,10),103)	Actual_Install_Date
	,	cast(convert(date,left(Actual_Install_Time,10),103) || ' ' || cast(right(Actual_Install_Time,8) as time) as timestamp)	Actual_Install_Time
	,	Installer
	,	Service_Visits_Booked
	,	Service_Visit_Job_Ref
	,	convert(date,left(Service_Booking_Date,10),103)	Service_Booking_Date
	,	cast(convert(date,left(Service_Booking_Date_Time,10),103) || ' ' || cast(right(Service_Booking_Date_Time,8) as time) as timestamp)	Service_Booking_Date_Time
	,	Service_Booking_Agent
	,	Service_Call_Description
	,	convert(date,left(Service_Visit_Scheduled_Date,10),103)	Service_Visit_Scheduled_Date
	,	Service_Visit_Status
	,	Call_Id
	,	Interaction_Id
	,	convert(date,left(Call_Date,10),103)	Call_Date
	,	cast(convert(date,left(Call_Time,10),103) || ' ' || cast(right(Call_Time,8) as time) as timestamp)	Call_Time
	,	Last_Agent
	,	Last_SCT
	,	Last_SCT_Group
	,	Duration
	,	Call_Pre_or_Post
	,	Call_same_day_as_install
	,	Customer_Calls
	,	Transfers_or_Consults
into	visit_data_cohort2
from	#tmp_undelimited
;
create hg index idx1 on visit_data_cohort2(Account_Number);
create hg index idx2 on visit_data_cohort2(Viewing_Card);
create date index idx3 on visit_data_cohort2(Actual_Install_Date);
create dttm index idx4 on visit_data_cohort2(Actual_Install_Time);
create dttm index idx5 on visit_data_cohort2(Service_Booking_Date_Time);
create date index idx6 on visit_data_cohort2(Service_Visit_Scheduled_Date);
create date index idx7 on visit_data_cohort2(Call_Date);
create dttm index idx8 on visit_data_cohort2(Call_Time);







-- Mesh data for the second 3k
create or replace variable @input_file_path varchar(256) = '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/tanghoi/SWAT_mesh_data_2nd_3k.csv';


-- Prepare temporary import table
drop table #tmp_undelimited;
create table #tmp_undelimited	(
		Card_Number	varchar(8000)
	,	Status_Date	varchar(8000)
	,	File_Date	varchar(8000)
	,	All_Devices_RAG	varchar(8000)
	,	One_Device_RAG	varchar(8000)
	,	Mesh_RAG	varchar(8000)
	,	Streaming_Conns_Raw	varchar(8000)
	,	Streaming_RAG	varchar(8000)
	,	Streaming_Interrupts_RAW	varchar(8000)
	,	Streaming_Interrupts_RAG	varchar(8000)
	,	Reboots_RAW	varchar(8000)
	,	Reboots_RAG	varchar(8000)
	,	thirD_partY_RAG	varchar(8000)
	)
;

-- Read the data
create or replace variable @sql_ varchar(8000);
set	@sql_	=	'
load table #tmp_undelimited	(
		Card_Number	'',''
	,	Status_Date	'',''
	,	File_Date	'',''
	,	All_Devices_RAG	'',''
	,	One_Device_RAG	'',''
	,	Mesh_RAG	'',''
	,	Streaming_Conns_Raw	'',''
	,	Streaming_RAG	'',''
	,	Streaming_Interrupts_RAW	'',''
	,	Streaming_Interrupts_RAG	'',''
	,	Reboots_RAW	'',''
	,	Reboots_RAG	'',''
	,	thirD_partY_RAG	''\n''
	)
from ''' || @input_file_path || '''
QUOTES OFF
ESCAPES OFF
SKIP 1
NOTIFY 1000
'
;
execute (@sql_);


-- Convert field types and write to permanent table
drop table MESH_data_cohort2;
select
		cast(Card_Number as int)	Card_Number
	,	convert(date,left(Status_Date,10),103)	Status_Date
	,	convert(date,left(File_Date,10),103)	File_Date
	,	cast(All_Devices_RAG as varchar(1))	All_Devices_RAG
	,	cast(One_Device_RAG as varchar(1))	One_Device_RAG
	,	cast(Mesh_RAG as varchar(1))	Mesh_RAG
	,	cast(Streaming_Conns_Raw as int)	Streaming_Conns_Raw
	,	cast(Streaming_RAG as varchar(1))	Streaming_RAG
	,	cast(Streaming_Interrupts_RAW as varchar(1))	Streaming_Interrupts_RAW
	,	cast(Streaming_Interrupts_RAG as varchar(1))	Streaming_Interrupts_RAG
	,	cast(Reboots_RAW as int)	Reboots_RAW
	,	cast(Reboots_RAG as varchar(1))	Reboots_RAG
	,	cast(thirD_partY_RAG as varchar(1))	thirD_partY_RAG
into	MESH_data_cohort2
from	#tmp_undelimited
;
create hg index idx1 on mesh_data_cohort2(Card_Number);
create date index idx2 on mesh_data_cohort2(Status_Date);
create date index idx3 on mesh_data_cohort2(File_Date);


*/



--------------------------------------------------------------------------------
-- Extract unique accounts and installation dates for second cohort
--------------------------------------------------------------------------------
drop table	swat_3k_accounts_cohort2;
select
		account_number
	,	Viewing_Card
	,	max(Actual_Install_Date)	Actual_Install_Date
into	swat_3k_accounts_cohort2
from	visit_data_cohort2
group by
		account_number
	,	Viewing_Card
;
create unique hg index idx1 on swat_3k_accounts_cohort2(account_number);
create unique hg index idx2 on swat_3k_accounts_cohort2(Viewing_Card);
create date index idx3 on swat_3k_accounts_cohort2(Actual_Install_Date);



--------------------------------------------------------------------------------
-- Chrome App connectivity RAG on install day
--------------------------------------------------------------------------------


-- select
-- 		acc.*
-- 	,	t0.chrome_install_RAG
-- 	,	'10-11 June installs'	as install_group
-- from
-- 				swat_3k_accounts	acc
-- 	left join	(
-- 					select
-- 							account_number
-- 						,	case
-- 								when	lower(INST_SURVEY_What_state_did_the_Chrome_app_show_at_the_end_of_the_install) like 	'%(red)%' then 'R'
-- 								when	lower(INST_SURVEY_What_state_did_the_Chrome_app_show_at_the_end_of_the_install) like 	'%(amber)%' then 'A'
-- 								when	lower(INST_SURVEY_What_state_did_the_Chrome_app_show_at_the_end_of_the_install) like 	'%(green)%' then 'G'
-- 								else																							'Other'
-- 							end	as chrome_install_RAG
-- 					from    thompsonja.swat_data_adjusted_jon
-- 				)					t0	on	acc.account_number	=	t0.account_number
-- ;
-- 0	210113084391	2016-06-10	G	G
-- 1	220019187289	2016-06-11	G	Other
-- 3	210076192256	2016-06-11	G	G
-- 4	220008299111	2016-06-11	G	Other
-- 5	200002171367	2016-06-11	G	Other





--------------------------------------------------------------------------------
-- Mesh RAG status n-days post-install
--------------------------------------------------------------------------------
-- 2nd cohort
select 
		acc.account_number
	,	acc.Viewing_Card
	,	acc.Actual_Install_Date
	,	mes.Status_Date
	,	datediff(day,acc.Actual_Install_Date,mes.Status_Date)	as	offset_days
	,		mes.All_Devices_RAG
		||	mes.One_Device_RAG
		||	mes.Mesh_RAG
		||	mes.Streaming_RAG
		||	mes.Streaming_Interrupts_RAG
		||	mes.Reboots_RAG
		||	mes.thirD_partY_RAG		as	concat_RAG
	,	case
			when	concat_RAG	like	'%R%'	then	'R'
			when	concat_RAG	like	'%A%'	then	'A'
			when	concat_RAG	like	'%G%'	then	'G'
			else										NULL
		end							as	connectivity_RAG
from
				swat_3k_accounts_cohort2	acc
	inner join	mesh_data_cohort2			mes	on	acc.Viewing_Card	=	mes.Card_Number
where	offset_days	>=	0
;


-- 1st cohort
select	--top 100
		acc.account_number
	,	acc.install_date
	,	acc.install_RAG
	,	mes.connectivity_RAG
	,	datediff(day,acc.install_date,mes.mesh_date)	offset_days
from
				swat_3k_accounts			acc
	inner join	thompsonja.QSWAT_Mesh_data;	mes	on	acc.row_id	=	cast(mes.row_id as int)
where	offset_days	>=	0
;

