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
-- V346_daily_SkyQ_NSS_volumes.sql
-- 2016-08-01
--
-- Environment:
-- SQL to be run on Hadoop Impala
-- http://uphad4j0.bskyb.com:8888/impala/
--
-- Function: 
-- Calculate daily volumes Sky Q STBs and viewing cards experiencing NSS
-- 
-- ------------------------------------------------------------------------------




with	t0	as	(	-- Get unique devices/messages
					select
							device_id
						,	parameter_value	as	X_SKY_COM_SMARTCARD_ID
						,	report_timestamp
						,	cast(concat('20',substr(report_timestamp,7,2),'-',substr(report_timestamp,4,2),'-',substr(report_timestamp,1,2),' ',substr(report_timestamp,10,12)) as timestamp)	dt
						-- ,	count(1)	num_params
					from	pace
					where
							d								>		from_unixtime(unix_timestamp()-(86400*30),'yyyyMMdd')
						and	device_id						like	'32%'
						and substr(report_timestamp,22,1)	=		'Z'
						and	parameter_name					=		'Device.DeviceInfo.X_SKY_COM_SMARTCARD_ID'
						and	parameter_value					<>		'testx_sky_com_smartcard_id'		-- Ignore test devices
					group by
							device_id
						,	parameter_value
						,	report_timestamp
						,	dt
				)
select
		t4.dt_
	,	t4.NSS_day_flag
	,	count(1/*distinct t4.device_id*/)				stb_devices
	,	count(distinct t4.X_SKY_COM_SMARTCARD_ID)		viewing_cards
from	(
			select	-- Derive NSS flag at viewing card-day grain
					t3.device_id
				,	t3.X_SKY_COM_SMARTCARD_ID
				,	t3.dt_
				,	max(t3.NSS_flag)	over	(partition by t3.X_SKY_COM_SMARTCARD_ID, t3.dt_) NSS_day_flag
			from	(
						select	-- Add device-day NSS flag
								a.device_id
							,	a.X_SKY_COM_SMARTCARD_ID
							,	a.dt_
							,	case
									when	nss.device_id 	is NULL 	then	0
									else										1
								end as NSS_flag
						from			(	-- Extract all unique Sky Q devices and viewing cards per day
											select
													device_id
												,	X_SKY_COM_SMARTCARD_ID
												,	to_date(dt)	dt_
											from	t0
											group by
													device_id
												,	X_SKY_COM_SMARTCARD_ID
												,	dt_
										)	a
							left join	(	-- Determine devices and messages that are NNS - collapse onto device-day grain
											select
													device_id
												,	X_SKY_COM_SMARTCARD_ID
												,	dt_
											from	(
														select	-- Calculate time differences between the previous 2 messages
																device_id
															,	X_SKY_COM_SMARTCARD_ID
															,	report_timestamp
															,	dt
															,	dt_
															,	dt1
															,	dt2
															,	unix_timestamp(dt) - unix_timestamp(dt1)	dt_dt1
															,	unix_timestamp(dt1) - unix_timestamp(dt2)	dt1_dt2
														from	(
																	select	-- Calculate previous 2 message timestamps using lag function
																			device_id
																		,	X_SKY_COM_SMARTCARD_ID
																		,	report_timestamp
																		,	dt
																		,	to_date(dt)											dt_
																		,	lag(dt,1) over (partition by device_id order by dt)	dt1
																		,	lag(dt,2) over (partition by device_id order by dt)	dt2
																	from	t0
															)	t1
													)	t2
											where
													(
															dt_dt1 	between	870		and	930
														or 	dt_dt1 	between	3470	and	3630
													)
												and	dt1_dt2	between	870	and	930
											group by
													device_id
												,	X_SKY_COM_SMARTCARD_ID
												,	dt_
											-- order by
												-- 	device_id
												-- ,	X_SKY_COM_SMARTCARD_ID
												-- ,	dt_
										)	nss 	on	a.device_id 				=	nss.device_id
													and a.X_SKY_COM_SMARTCARD_ID	=	nss.X_SKY_COM_SMARTCARD_ID
													and	a.dt_						=	nss.dt_
						-- order by
						-- 		a.device_id
						-- 	,	nss.dt
					)	t3
			-- order by
			-- 		device_id
			-- ,	X_SKY_COM_SMARTCARD_ID
			-- 	,	dt_
		)	t4
group by
		t4.dt_
	,	t4.NSS_day_flag
order by
		t4.dt_
	,	t4.NSS_day_flag
;
