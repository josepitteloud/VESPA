

---------------------
-- Data Understanding
---------------------

-- what is the best point in time to start comparing Voice vs Text searches
/*
	OUTPUT AT:
	G:\RTCI\Sky Projects\Vespa\Products\Analysis - Excel\Sky Q - PA - Search face off (Voice vs Text).XLSX
*/
-- are there voice searches in month X?
-- if so, since when in the month X?

select	gn_lvl2_session
		,count(distinct date_||'-'||dk_serial_number||'-'||gn_lvl2_session_grain) as njourneys
		,count(distinct dk_serial_number)	as nboxes
		,count(distinct date_)	as ndays
--from	z_pa_events_fact_201703
--from	z_pa_events_fact_201704
from	z_pa_events_fact
where	lower(gn_lvl2_session) like '%search%'
and		date_ >= '2017-05-01'
group	by	gn_lvl2_session


-- what is people's choice? looking into their preferences

with	base as	(
					select	extract(month from date_)	as month_
							,date_
							,dk_serial_number
							,gn_lvl2_session
							,count(distinct date_||'-'||gn_lvl2_session_grain)	as nsearches
							,cast(nsearches as float) / cast((sum(nsearches) over (partition by month_ ,dk_serial_number)) as float)	as prop
					--from	z_pa_events_fact_201703
					--from	z_pa_events_fact_201704
					from	z_pa_events_fact
					where	lower(gn_lvl2_session) like '%search%'
					and		date_ >= '2017-05-01'
					group	by	month_
								,date_
								,dk_serial_number
								,gn_lvl2_session
				)
select	month_
		,date_
		,sum(prop)		as nboxes
from	base
group	by	month_
			,date_
			
			

			
-------------------
-- Data Preparation
-------------------			

-- Creating a list for STBs using a remote with voice search capability and reporting persistently (on 90% of dates available)
drop table	z_pa_vvt_persisten_stbs; commit;

with	base as	(
					select	dk_serial_number
							,date_
							,max( case when gn_lvl2_session = 'Voice Search' then 1 else 0 end) as vs_flag
					from	z_pa_events_fact_201703
					where	date_ >= '2017-03-06'
					group	by	1,2
					union
					select	dk_serial_number
							,date_
							,max( case when gn_lvl2_session = 'Voice Search' then 1 else 0 end) as vs_flag
					from	z_pa_events_fact_201704
					group	by	1,2
					union
					select	dk_serial_number
							,date_
							,max( case when gn_lvl2_session = 'Voice Search' then 1 else 0 end) as vs_flag
					from	z_pa_events_fact_201705
					where	date_ >= '2017-05-01'
					group	by	1,2
				)
		,ref as	(
					-- STBs Capable of doing Voice Search...
					select	distinct dk_serial_number as boxid
					from	base
					where	vs_flag = 1
				)
select	dk_serial_number
		,count(distinct date_)	as ndays
into	z_pa_vvt_persisten_stbs
from	base
		-- this is for filtering only STBs capable of doing Voice Search...
		inner join ref
		on	base.dk_serial_number = ref.boxid
group	by	dk_serial_number
having	ndays >= 58; -- Reporting persistently on 90% of the 65 days available...
commit;


-- Creating a list of STBs that hold a Bluetooth remote and therefore capable of doing Voice Searches
drop table	z_pa_vvt_capable_stbs_2; commit;

with	base as	(
					select	distinct 
							dk_serial_number
					from	z_pa_events_fact_201703
					where	date_ >= '2017-03-06'
					and		remote_type = 'BT'
					union	
					select	distinct
							dk_serial_number
					from	z_pa_events_fact_201704
					where	remote_type = 'BT'
					union
					select	distinct
							dk_serial_number
					from	z_pa_events_fact_201705
					where	date_ >= '2017-05-01'
					and		remote_type = 'BT'
				)
-- STBs Capable of doing Voice Search because they hold a Bluetooth remote...
select	distinct dk_serial_number
into	z_pa_vvt_capable_stbs_2
from	base;
commit;


-- Setting ground data for the analysis

--drop table	z_pa_vvt_ground_data; commit;
--truncate table 	z_pa_vvt_ground_data; commit;

--insert	into z_pa_vvt_ground_data
with	etl1 as		(
						select	date_
								,dt
								,timems
								,dk_serial_number
								,gn_lvl2_session
								,gn_lvl2_session_grain
								,dk_action_id
								,dk_previous
								,dk_current
								,dk_referrer_id
								,ss_elapsed_next_action
						from	z_pa_events_fact_201703
						where	lower(gn_lvl2_session) like '%search%'
						and		date_ >= '2017-03-06'
						union
						select	date_
								,dt
								,timems
								,dk_serial_number
								,gn_lvl2_session
								,gn_lvl2_session_grain
								,dk_action_id
								,dk_previous
								,dk_current
								,dk_referrer_id
								,ss_elapsed_next_action
						from	z_pa_events_fact_201704
						where	lower(gn_lvl2_session) like '%search%'
						union
						select	date_
								,dt
								,timems
								,dk_serial_number
								,gn_lvl2_session
								,gn_lvl2_session_grain
								,dk_action_id
								,dk_previous
								,dk_current
								,dk_referrer_id
								,ss_elapsed_next_action
						from	z_pa_events_fact_201705
						where	lower(gn_lvl2_session) like '%search%'
						and		date_ between '2017-05-01' and '2017-05-09'
					)
		,etl2 as	(
						select	date_
								,dt
								,timems
								,dk_serial_number
								,gn_lvl2_session
								,gn_lvl2_session_grain
								,dk_action_id
								,ss_elapsed_next_action
								,dk_previous
								,dk_current
								,dk_referrer_id
								,query
								,error_msg
								,last_value(x ignore nulls)	over	(
																		partition by	date_
																						,dk_serial_number
																						,gn_lvl2_session_grain
																		order by		timems
																		rows between 	50 preceding and current row
																	)	as input
						from	(
									select	etl1.date_
											,etl1.dt
											,etl1.timems
											,etl1.dk_serial_number
											,etl1.gn_lvl2_session
											,etl1.gn_lvl2_session_grain
											,etl1.dk_action_id
											,etl1.ss_elapsed_next_Action
											,etl1.dk_previous
											,etl1.dk_current
											,etl1.dk_referrer_id
											,ref.query
											,ref.error_msg
											,case	when gn_lvl2_Session = 'Voice Search' and dk_action_id = 01605 	then dense_rank() over(partition by etl1.date_,etl1.dk_serial_number,etl1.gn_lvl2_session_grain order by timems)
													when gn_lvl2_Session = 'Search' and dk_previous<>dk_action_id and dk_action_id ='01605' then dense_rank() over(partition by date_,etl1.dk_serial_number,gn_lvl2_session_grain order by etl1.timems) 
													else null 
											end 	as x
									from	etl1
											left join pa_voice_search_events	as ref
											on	etl1.dk_serial_number	= ref.dk_serial_number
											and	etl1.timems				= ref.timems
								)	as final_
					)
select	*
into	z_pa_vvt_ground_data
from	(
			select	extract(month from date_)																		as month_
					,(extract(epoch from date_ - date('2017-03-06'))/7)+1											as week_
					,date_
					,dk_serial_number
					,gn_lvl2_session
					,gn_lvl2_Session_grain
					,input																							as attempt_id
					,min(case when error_msg in ('AS_NO_VOICE_HEARD','EPG_AccidentalPress') then 0 else 1 end)		as valid_
					,max(case when dk_action_id in (02400,03000,00001,02000,02010,02002,02005) then 1 else 0 end)	as conv_flag
					,sum(ss_elapsed_next_action)																	as time_spent
					,sum(time_spent) over (partition by date_,dk_Serial_number,gn_lvl2_session_grain)				as journey_time
			from	etl2
			group	by	month_
						,date_
						,dk_serial_number
						,gn_lvl2_session
						,gn_lvl2_session_grain
						,attempt_id
		)	as final_etl
where	journey_time <=1000; -- 83 % of Searches throughout time frame, capping the tail
commit;
					

-------------------------------
-- Data Analysis (Queries Only)
-------------------------------

/*
	
	All these queries are used in Tableau for Analysis and Display
	
	OUTPUT AT:
	G:\RTCI\Sky Projects\Vespa\Products\Analysis - Tableau\Sky Q - PA - Voice Search vs Text Search.twb
	
*/

-- Checking at a cohort on weekly snapshots (All STBs Voice Searching in April 2017)
with	base as			(
							select	month_
									,week_
									,date_
									,dk_Serial_number
									,gn_lvl2_Session
									,count(Distinct gn_lvl2_Session_grain||'-'||attempt_id)													as nsearches
									,count(distinct (case when conv_flag = 1 then gn_lvl2_Session_Grain||'-'||attempt_id else null end))	as nconv_searches
							from	z_pa_vvt_ground_data
							where	valid_ = 1
							and		attempt_id is not null
							group	by	month_
										,week_
										,date_
										,dk_Serial_number
										,gn_lvl2_session
						)
		,base_size as	(
							select	week_
									,count(distinct dk_serial_number) as nactive_stbs
							from	(
										select	distinct
												(extract(epoch from date_ - date('2017-03-06'))/7)+1 as week_
												,dk_serial_number
										from	z_pa_events_fact_201703
										where	date_ >= '2017-03-06'
										and		remote_type = 'BT'
										union	
										select	distinct
												(extract(epoch from date_ - date('2017-03-06'))/7)+1 as week_
												,dk_serial_number
										from	z_pa_events_fact_201704
										where	remote_type = 'BT'
										union
										select	distinct
												(extract(epoch from date_ - date('2017-03-06'))/7)+1 as week_
												,dk_serial_number
										from	z_pa_events_fact_201705
										where	date_ between '2017-05-01' and '2017-05-07'
										and		remote_type = 'BT'	
									)	as x
							group	by	week_
						)

-- QUERY 1 (Overall Activity)
select	base.week_
		,count(distinct date_)																					as ndays
		,max(base_size.nactive_stbs)																			as nboxes
		,count(distinct (case when nsearches > 0 then base.dk_serial_number else null end))						as nboxes_searching
		,sum(case when gn_lvl2_session = 'Search' then nsearches  else 0 end)									as ntext
		,sum(case when gn_lvl2_session = 'Voice Search' then nsearches  else 0 end)								as nvoice
		,sum(case when gn_lvl2_session = 'Search' then nconv_searches  else 0 end)								as nconv_text
		,sum(case when gn_lvl2_session = 'Voice Search' then nconv_searches else 0 end)							as nconv_voice
		,count(distinct (case when gn_lvl2_session = 'Search' then base.dk_serial_number else null end))		as ntext_reach
		,count(distinct (case when gn_lvl2_session = 'Voice Search' then base.dk_serial_number else null end))	as nvoice_reach
from	base
		inner join base_size
		on	base.week_	= base_size.week_
		--inner join z_pa_vvt_persisten_stbs as ref
		--on	base.dk_serial_number = ref.dk_serial_number
		inner join z_pa_vvt_capable_stbs_2 as ref
		on	base.dk_serial_number = ref.dk_serial_number
group	by	base.week_
having	ndays = 7 -- to only take into consideration complete weeks...


-- QUERY 2 (Preferences between Voice and Text)
select	week_
		,gn_lvl2_session
		,sum(prop)	as pref_nboxes
from	(
			select	week_
					,base.dk_serial_number
					,gn_lvl2_session
					,sum(nsearches)															as nweekly_searches
					,sum(nweekly_searches) over (partition by week_ ,base.dk_serial_number)	as tot_weekly_searches
					,case	when tot_weekly_searches > 0 then cast(nweekly_searches as float) / cast(tot_weekly_searches as float)
							else 0
					end		as prop
			from	base
					--inner join z_pa_vvt_persisten_stbs as ref
					--on	base.dk_serial_number = ref.dk_serial_number
					inner join z_pa_vvt_capable_stbs_2 as ref
					on	base.dk_serial_number = ref.dk_serial_number
			group	by	week_
						,base.dk_serial_number
						,gn_lvl2_session
		)	as final_
group	by	week_
			,gn_lvl2_session


-- QUERY 3 (Search Loyalty)
select	week_
		,case	when nsearch_types > 1 	then 'Both'
				when text_search = 1	then 'Text Searching'
				when voice_search = 1	then 'Voice Searching'
		end		as loyalty
		,count(distinct dk_serial_number)	as nactive_stbs
from	(
			select	week_
					,base.dk_serial_number
					,max(case when gn_lvl2_session = 'Search' then 1 else 0 end)		as text_search
					,max(case when gn_lvl2_session = 'Voice Search' then 1 else 0 end)	as voice_search
					,text_search + voice_search 										as nsearch_types
			from	base
					--inner join z_pa_vvt_persisten_stbs as ref
					--on	base.dk_serial_number = ref.dk_serial_number
					inner join z_pa_vvt_capable_stbs_2 as ref
					on	base.dk_serial_number = ref.dk_serial_number
			where	nsearches > 0
			group	by	week_
						,base.dk_serial_number
		)	as checking
group	by	week_
			,loyalty