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
-- 
-- denorm X_SKY_COM_DVBS with NSS flags.sql
-- 2016-08-01
-- 
-- Environment:
-- SQL to be run on Hadoop Impala
-- http://uphad4j0.bskyb.com:8888/impala/
-- 
-- Function: 
-- Denormalise LNB and Tuner objects in SkyQ diagnostics data. 
-- Add definition of NSS and apply flag to each message.
-- 
--
-- ------------------------------------------------------------------------------


/*	-- Create output table
create table tmp_tuner_denorm_201608(
		device_id	string
	,	report_timestamp	string
	,	dt	timestamp
	,	NSS_flag	tinyint
	,	X_SKY_COM_SMARTCARD_ID	int
	,	LNB_1_ShortCircuitDetectState	string
	,	LNB_1_State	string
	,	LNB_1_Voltage	string
	,	LNB_2_ShortCircuitDetectState	string
	,	LNB_2_SState	string
	,	LNB_2_SVoltage	string
	,	Locked	string
	,	LTRFEC	string
	,	LTRFrequency	int
	,	LTRInversion	string
	,	LTRMode	string
	,	LTRModulation	string
	,	LTRRollOff	string
	,	LTRSymbolRate	int
	,	RxPowerLevel	int
	,	Scan	string
	,	SNR	int
	,	Status	string
	,	TSBERCount	int
	,	TSBERWindow	int
	,	TSFEC	string
	,	TSFrequency	int
	,	TSInversion	string
	,	TSMode	string
	,	TSModulation	string
	,	TSRollOff	string
	,	TSSignalStatus	string
	,	TSSignalStrength	int
	,	TSSymbolRate	int
	,	TunerState	string
	,	row_num	bigint
)
;

truncate table tmp_tuner_denorm_201608;
*/


insert into	tmp_tuner_denorm_201608
select
		t_.*
	,	row_number() over (order by	t_.device_id,t_.dt)	row_num
from	(
			with t0 as	(	-- Get unique messages
							select
									device_id
								,	report_timestamp
								,	parameter_name
								,	parameter_value
								,	cast(concat('20',substr(report_timestamp,7,2),'-',substr(report_timestamp,4,2),'-',substr(report_timestamp,1,2),' ',substr(report_timestamp,10,12)) as timestamp)	dt
							from	pace
							where
									(
											parameter_name			like	'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.LNB.%'
										or	parameter_name			like	'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.2.%'
										or	(
													parameter_name	=		'Device.DeviceInfo.X_SKY_COM_SMARTCARD_ID'
												and parameter_value	<>		'testx_sky_com_smartcard_id'	-- Ignore test devices
											)
									)
								and	cast(d as int)					between	20160801
																	and		20160831
								and	device_id						like	'32%'
								and	substr(report_timestamp,22,1)	=		'Z'
						)

			select
					a.device_id
				,	a.report_timestamp
				,	a.dt
				,	case
						when	nss.device_id 	is NULL 	then	0
						else										1
					end 																																											as	NSS_flag
				,	cast(max(case a.parameter_name when 'Device.DeviceInfo.X_SKY_COM_SMARTCARD_ID'	then a.parameter_value else NULL end) as int)													as	X_SKY_COM_SMARTCARD_ID
				,	max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.LNB.1.ShortCircuitDetectState'	then a.parameter_value else NULL end)			as	LNB_1_ShortCircuitDetectState
				,	max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.LNB.1.State'	then a.parameter_value else NULL end)								as	LNB_1_State
				,	max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.LNB.1.Voltage'	then a.parameter_value else NULL end)							as	LNB_1_Voltage
				,	max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.LNB.2.ShortCircuitDetectState'	then a.parameter_value else NULL end) 			as	LNB_2_ShortCircuitDetectState
				,	max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.LNB.2.State'	then a.parameter_value else NULL end)	 							as	LNB_2_SState
				,	max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.LNB.2.Voltage'	then a.parameter_value else NULL end) 							as	LNB_2_SVoltage
				,	max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.2.Locked'	then a.parameter_value else NULL end) 							as	Locked
				,	max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.2.LTRFEC'	then a.parameter_value else NULL end) 							as	LTRFEC
				,	cast(max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.2.LTRFrequency'	then a.parameter_value else NULL end) as int) 		as	LTRFrequency
				,	max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.2.LTRInversion'	then a.parameter_value else NULL end) 						as	LTRInversion
				,	max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.2.LTRMode'	then a.parameter_value else NULL end) 							as	LTRMode
				,	max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.2.LTRModulation'	then a.parameter_value else NULL end) 					as	LTRModulation
				,	max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.2.LTRRollOff'	then a.parameter_value else NULL end) 						as	LTRRollOff
				,	cast(max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.2.LTRSymbolRate'	then a.parameter_value else NULL end) as int) 		as	LTRSymbolRate
				,	cast(max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.2.RxPowerLevel'	then a.parameter_value else NULL end) as int) 		as	RxPowerLevel
				,	max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.2.Scan'	then a.parameter_value else NULL end) 								as	Scan
				,	cast(max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.2.SNR'	then a.parameter_value else NULL end) as int) 					as	SNR
				,	max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.2.Status'	then a.parameter_value else NULL end) 							as	Status
				,	cast(max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.2.TSBERCount'	then a.parameter_value else NULL end) as int) 			as	TSBERCount
				,	cast(max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.2.TSBERWindow'	then a.parameter_value else NULL end) as int) 			as	TSBERWindow
				,	max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.2.TSFEC'	then a.parameter_value else NULL end) 							as	TSFEC
				,	cast(max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.2.TSFrequency'	then a.parameter_value else NULL end) as int) 			as	TSFrequency
				,	max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.2.TSInversion'	then a.parameter_value else NULL end) 						as	TSInversion
				,	max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.2.TSMode'	then a.parameter_value else NULL end) 							as	TSMode
				,	max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.2.TSModulation'	then a.parameter_value else NULL end) 						as	TSModulation
				,	max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.2.TSRollOff'	then a.parameter_value else NULL end) 						as	TSRollOff
				,	max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.2.TSSignalStatus'	then a.parameter_value else NULL end) 					as	TSSignalStatus
				,	cast(max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.2.TSSignalStrength'	then a.parameter_value else NULL end) as int)	as	TSSignalStrength
				,	cast(max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.2.TSSymbolRate'	then a.parameter_value else NULL end) as int) 		as	TSSymbolRate
				,	max(case a.parameter_name when 'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.2.TunerState'	then a.parameter_value else NULL end) 						as	TunerState
			from
							t0	a
				left join	(	-- Apply definition here to identify messages generated during NSS
								select	*
								from	(	-- Calculate differences between timestamp lags
											select
													*
												,	unix_timestamp(dt1) - unix_timestamp(dt)	dt_dt1	-- 1st order difference
												,	unix_timestamp(dt2) - unix_timestamp(dt)	dt_dt2	-- 2nd order difference
											from	(	-- Calculate timestamp lags
														select
																device_id
															,	report_timestamp
															,	dt
															,	lead(dt,1) over (partition by device_id order by dt)	dt1
															,	lead(dt,2) over (partition by device_id order by dt)	dt2
														from	(	-- Get all unique messages
																	select
																			device_id
																		,	report_timestamp
																		,	dt
																	from	t0
																	where	parameter_name	=	'Device.DeviceInfo.X_SKY_COM_SMARTCARD_ID'
																	group by
																			device_id
																		,	report_timestamp
																		,	dt
																)	t1
													)	t2
										)	t3
								where
										(
												dt_dt1 	between	870		and	930		-- allow for 60s window centered around 900s (15 mins) 1st order difference in message gap
											or 	dt_dt1 	between	3570	and	3630	-- allow for 60s window centered around 3600s (1 hour) 1st order difference in message gap
											or 	dt_dt1 	between	43170	and	43230	-- allow for 60s window centered around 43200s (12 hours) 1st order difference in message gap
									-- 	)
									-- and
									-- 	(
											or	dt_dt2 	between	870			and	930			-- allow for 60s window centered around 900s (15 mins) 2nd order difference in message gap
											or 	dt_dt2 	between	3570		and	3630		-- allow for 60s window centered around 3600s (1 hour) 2nd order difference in message gap
											or 	dt_dt2 	between	43170		and	43230		-- allow for 60s window centered around 43200s (12 hours) 2nd order difference in message gap
											or	dt_dt2	between	(2*870)		and	(2*930)		-- allow for 60s window centered around (30 mins) 2nd order difference in message gap
											or 	dt_dt2	between	(2*3570)	and	(2*3630)	-- allow for 60s window centered around (2 hours) 2nd order difference in message gap
											or 	dt_dt2	between	(2*43170)	and	(2*43230)	-- allow for 60s window centered around (24 hours) 2nd order difference in message gap
										)
							)		nss		on 	a.device_id			=	nss.device_id
											and	a.report_timestamp	=	nss.report_timestamp
			group by
					a.device_id
				,	a.report_timestamp
				,	a.dt
				,	NSS_flag
			having
					X_SKY_COM_SMARTCARD_ID	is not NULL	-- Remove (after casting to int) any missing viewing cards and test devices which have X_SKY_COM_SMARTCARD_ID = 'testx_sky_com_smartcard_id'
				and	TunerState				is not NULL
			-- order by
			-- 		a.device_id
			-- 	,	a.dt
		)	t_
;



/*
-- Create output table for lags
create table tmp_tuner_denorm_201608_with_lags(
		device_id	string
	,	report_timestamp	string
	,	dt	timestamp
	,	NSS_flag	tinyint
	,	X_SKY_COM_SMARTCARD_ID	int
	,	LNB_1_ShortCircuitDetectState	string
	,	LNB_1_State	string
	,	LNB_1_Voltage	string
	,	LNB_2_ShortCircuitDetectState	string
	,	LNB_2_SState	string
	,	LNB_2_SVoltage	string
	,	Locked	string
	,	LTRFEC	string
	,	LTRFrequency	int
	,	LTRInversion	string
	,	LTRMode	string
	,	LTRModulation	string
	,	LTRRollOff	string
	,	LTRSymbolRate	int
	,	RxPowerLevel	int
	,	Scan	string
	,	SNR	int
	,	Status	string
	,	TSBERCount	int
	,	TSBERWindow	int
	,	TSFEC	string
	,	TSFrequency	int
	,	TSInversion	string
	,	TSMode	string
	,	TSModulation	string
	,	TSRollOff	string
	,	TSSignalStatus	string
	,	TSSignalStrength	int
	,	TSSymbolRate	int
	,	TunerState	string
	,	row_num	bigint
	,	dt1	timestamp
	,	dt2	timestamp
	,	dt3	timestamp
	,	dt4	timestamp
	,	dt5	timestamp

	,	NSS_flag_dt1	tinyint
	,	X_SKY_COM_SMARTCARD_ID_dt1	int
	,	LNB_1_ShortCircuitDetectState_dt1	string
	,	LNB_1_State_dt1	string
	,	LNB_1_Voltage_dt1	string
	,	LNB_2_ShortCircuitDetectState_dt1	string
	,	LNB_2_SState_dt1	string
	,	LNB_2_SVoltage_dt1	string
	,	Locked_dt1	string
	,	LTRFEC_dt1	string
	,	LTRFrequency_dt1	int
	,	LTRInversion_dt1	string
	,	LTRMode_dt1	string
	,	LTRModulation_dt1	string
	,	LTRRollOff_dt1	string
	,	LTRSymbolRate_dt1	int
	,	RxPowerLevel_dt1	int
	,	Scan_dt1	string
	,	SNR_dt1	int
	,	Status_dt1	string
	,	TSBERCount_dt1	int
	,	TSBERWindow_dt1	int
	,	TSFEC_dt1	string
	,	TSFrequency_dt1	int
	,	TSInversion_dt1	string
	,	TSMode_dt1	string
	,	TSModulation_dt1	string
	,	TSRollOff_dt1	string
	,	TSSignalStatus_dt1	string
	,	TSSignalStrength_dt1	int
	,	TSSymbolRate_dt1	int
	,	TunerState_dt1	string

	,	NSS_flag_dt2	tinyint
	,	X_SKY_COM_SMARTCARD_ID_dt2	int
	,	LNB_1_ShortCircuitDetectState_dt2	string
	,	LNB_1_State_dt2	string
	,	LNB_1_Voltage_dt2	string
	,	LNB_2_ShortCircuitDetectState_dt2	string
	,	LNB_2_SState_dt2	string
	,	LNB_2_SVoltage_dt2	string
	,	Locked_dt2	string
	,	LTRFEC_dt2	string
	,	LTRFrequency_dt2	int
	,	LTRInversion_dt2	string
	,	LTRMode_dt2	string
	,	LTRModulation_dt2	string
	,	LTRRollOff_dt2	string
	,	LTRSymbolRate_dt2	int
	,	RxPowerLevel_dt2	int
	,	Scan_dt2	string
	,	SNR_dt2	int
	,	Status_dt2	string
	,	TSBERCount_dt2	int
	,	TSBERWindow_dt2	int
	,	TSFEC_dt2	string
	,	TSFrequency_dt2	int
	,	TSInversion_dt2	string
	,	TSMode_dt2	string
	,	TSModulation_dt2	string
	,	TSRollOff_dt2	string
	,	TSSignalStatus_dt2	string
	,	TSSignalStrength_dt2	int
	,	TSSymbolRate_dt2	int
	,	TunerState_dt2	string

	,	NSS_flag_dt3	tinyint
	,	X_SKY_COM_SMARTCARD_ID_dt3	int
	,	LNB_1_ShortCircuitDetectState_dt3	string
	,	LNB_1_State_dt3	string
	,	LNB_1_Voltage_dt3	string
	,	LNB_2_ShortCircuitDetectState_dt3	string
	,	LNB_2_SState_dt3	string
	,	LNB_2_SVoltage_dt3	string
	,	Locked_dt3	string
	,	LTRFEC_dt3	string
	,	LTRFrequency_dt3	int
	,	LTRInversion_dt3	string
	,	LTRMode_dt3	string
	,	LTRModulation_dt3	string
	,	LTRRollOff_dt3	string
	,	LTRSymbolRate_dt3	int
	,	RxPowerLevel_dt3	int
	,	Scan_dt3	string
	,	SNR_dt3	int
	,	Status_dt3	string
	,	TSBERCount_dt3	int
	,	TSBERWindow_dt3	int
	,	TSFEC_dt3	string
	,	TSFrequency_dt3	int
	,	TSInversion_dt3	string
	,	TSMode_dt3	string
	,	TSModulation_dt3	string
	,	TSRollOff_dt3	string
	,	TSSignalStatus_dt3	string
	,	TSSignalStrength_dt3	int
	,	TSSymbolRate_dt3	int
	,	TunerState_dt3	string

	,	NSS_flag_dt4	tinyint
	,	X_SKY_COM_SMARTCARD_ID_dt4	int
	,	LNB_1_ShortCircuitDetectState_dt4	string
	,	LNB_1_State_dt4	string
	,	LNB_1_Voltage_dt4	string
	,	LNB_2_ShortCircuitDetectState_dt4	string
	,	LNB_2_SState_dt4	string
	,	LNB_2_SVoltage_dt4	string
	,	Locked_dt4	string
	,	LTRFEC_dt4	string
	,	LTRFrequency_dt4	int
	,	LTRInversion_dt4	string
	,	LTRMode_dt4	string
	,	LTRModulation_dt4	string
	,	LTRRollOff_dt4	string
	,	LTRSymbolRate_dt4	int
	,	RxPowerLevel_dt4	int
	,	Scan_dt4	string
	,	SNR_dt4	int
	,	Status_dt4	string
	,	TSBERCount_dt4	int
	,	TSBERWindow_dt4	int
	,	TSFEC_dt4	string
	,	TSFrequency_dt4	int
	,	TSInversion_dt4	string
	,	TSMode_dt4	string
	,	TSModulation_dt4	string
	,	TSRollOff_dt4	string
	,	TSSignalStatus_dt4	string
	,	TSSignalStrength_dt4	int
	,	TSSymbolRate_dt4	int
	,	TunerState_dt4	string

	,	NSS_flag_dt5	tinyint
	,	X_SKY_COM_SMARTCARD_ID_dt5	int
	,	LNB_1_ShortCircuitDetectState_dt5	string
	,	LNB_1_State_dt5	string
	,	LNB_1_Voltage_dt5	string
	,	LNB_2_ShortCircuitDetectState_dt5	string
	,	LNB_2_SState_dt5	string
	,	LNB_2_SVoltage_dt5	string
	,	Locked_dt5	string
	,	LTRFEC_dt5	string
	,	LTRFrequency_dt5	int
	,	LTRInversion_dt5	string
	,	LTRMode_dt5	string
	,	LTRModulation_dt5	string
	,	LTRRollOff_dt5	string
	,	LTRSymbolRate_dt5	int
	,	RxPowerLevel_dt5	int
	,	Scan_dt5	string
	,	SNR_dt5	int
	,	Status_dt5	string
	,	TSBERCount_dt5	int
	,	TSBERWindow_dt5	int
	,	TSFEC_dt5	string
	,	TSFrequency_dt5	int
	,	TSInversion_dt5	string
	,	TSMode_dt5	string
	,	TSModulation_dt5	string
	,	TSRollOff_dt5	string
	,	TSSignalStatus_dt5	string
	,	TSSignalStrength_dt5	int
	,	TSSymbolRate_dt5	int
	,	TunerState_dt5	string

	,	row_num_lag	bigint
)
;

truncate table tmp_tuner_denorm_201608_with_lags;
*/


insert into tmp_tuner_denorm_201608_with_lags
select
		a.device_id
	,	a.report_timestamp
	,	a.dt
	,	a.NSS_flag
	,	a.X_SKY_COM_SMARTCARD_ID
	,	a.LNB_1_ShortCircuitDetectState
	,	a.LNB_1_State
	,	a.LNB_1_Voltage
	,	a.LNB_2_ShortCircuitDetectState
	,	a.LNB_2_SState
	,	a.LNB_2_SVoltage
	,	a.Locked
	,	a.LTRFEC
	,	a.LTRFrequency
	,	a.LTRInversion
	,	a.LTRMode
	,	a.LTRModulation
	,	a.LTRRollOff
	,	a.LTRSymbolRate
	,	a.RxPowerLevel
	,	a.Scan
	,	a.SNR
	,	a.Status
	,	a.TSBERCount
	,	a.TSBERWindow
	,	a.TSFEC
	,	a.TSFrequency
	,	a.TSInversion
	,	a.TSMode
	,	a.TSModulation
	,	a.TSRollOff
	,	a.TSSignalStatus
	,	a.TSSignalStrength
	,	a.TSSymbolRate
	,	a.TunerState
	,	a.row_num
	,	a.dt1
	,	a.dt2
	,	a.dt3
	,	a.dt4
	,	a.dt5

	,	b.NSS_flag	as	NSS_flag_dt1
	,	b.X_SKY_COM_SMARTCARD_ID	as	X_SKY_COM_SMARTCARD_ID_dt1
	,	b.LNB_1_ShortCircuitDetectState	as	LNB_1_ShortCircuitDetectState_dt1
	,	b.LNB_1_State	as	LNB_1_State_dt1
	,	b.LNB_1_Voltage	as	LNB_1_Voltage_dt1
	,	b.LNB_2_ShortCircuitDetectState	as	LNB_2_ShortCircuitDetectState_dt1
	,	b.LNB_2_SState	as	LNB_2_SState_dt1
	,	b.LNB_2_SVoltage	as	LNB_2_SVoltage_dt1
	,	b.Locked	as	Locked_dt1
	,	b.LTRFEC	as	LTRFEC_dt1
	,	b.LTRFrequency	as	LTRFrequency_dt1
	,	b.LTRInversion	as	LTRInversion_dt1
	,	b.LTRMode	as	LTRMode_dt1
	,	b.LTRModulation	as	LTRModulation_dt1
	,	b.LTRRollOff	as	LTRRollOff_dt1
	,	b.LTRSymbolRate	as	LTRSymbolRate_dt1
	,	b.RxPowerLevel	as	RxPowerLevel_dt1
	,	b.Scan	as	Scan_dt1
	,	b.SNR	as	SNR_dt1
	,	b.Status	as	Status_dt1
	,	b.TSBERCount	as	TSBERCount_dt1
	,	b.TSBERWindow	as	TSBERWindow_dt1
	,	b.TSFEC	as	TSFEC_dt1
	,	b.TSFrequency	as	TSFrequency_dt1
	,	b.TSInversion	as	TSInversion_dt1
	,	b.TSMode	as	TSMode_dt1
	,	b.TSModulation	as	TSModulation_dt1
	,	b.TSRollOff	as	TSRollOff_dt1
	,	b.TSSignalStatus	as	TSSignalStatus_dt1
	,	b.TSSignalStrength	as	TSSignalStrength_dt1
	,	b.TSSymbolRate	as	TSSymbolRate_dt1
	,	b.TunerState	as	TunerState_dt1

	,	c.NSS_flag	as	NSS_flag_dt2
	,	c.X_SKY_COM_SMARTCARD_ID	as	X_SKY_COM_SMARTCARD_ID_dt2
	,	c.LNB_1_ShortCircuitDetectState	as	LNB_1_ShortCircuitDetectState_dt2
	,	c.LNB_1_State	as	LNB_1_State_dt2
	,	c.LNB_1_Voltage	as	LNB_1_Voltage_dt2
	,	c.LNB_2_ShortCircuitDetectState	as	LNB_2_ShortCircuitDetectState_dt2
	,	c.LNB_2_SState	as	LNB_2_SState_dt2
	,	c.LNB_2_SVoltage	as	LNB_2_SVoltage_dt2
	,	c.Locked	as	Locked_dt2
	,	c.LTRFEC	as	LTRFEC_dt2
	,	c.LTRFrequency	as	LTRFrequency_dt2
	,	c.LTRInversion	as	LTRInversion_dt2
	,	c.LTRMode	as	LTRMode_dt2
	,	c.LTRModulation	as	LTRModulation_dt2
	,	c.LTRRollOff	as	LTRRollOff_dt2
	,	c.LTRSymbolRate	as	LTRSymbolRate_dt2
	,	c.RxPowerLevel	as	RxPowerLevel_dt2
	,	c.Scan	as	Scan_dt2
	,	c.SNR	as	SNR_dt2
	,	c.Status	as	Status_dt2
	,	c.TSBERCount	as	TSBERCount_dt2
	,	c.TSBERWindow	as	TSBERWindow_dt2
	,	c.TSFEC	as	TSFEC_dt2
	,	c.TSFrequency	as	TSFrequency_dt2
	,	c.TSInversion	as	TSInversion_dt2
	,	c.TSMode	as	TSMode_dt2
	,	c.TSModulation	as	TSModulation_dt2
	,	c.TSRollOff	as	TSRollOff_dt2
	,	c.TSSignalStatus	as	TSSignalStatus_dt2
	,	c.TSSignalStrength	as	TSSignalStrength_dt2
	,	c.TSSymbolRate	as	TSSymbolRate_dt2
	,	c.TunerState	as	TunerState_dt2

	,	d.NSS_flag	as	NSS_flag_dt3
	,	d.X_SKY_COM_SMARTCARD_ID	as	X_SKY_COM_SMARTCARD_ID_dt3
	,	d.LNB_1_ShortCircuitDetectState	as	LNB_1_ShortCircuitDetectState_dt3
	,	d.LNB_1_State	as	LNB_1_State_dt3
	,	d.LNB_1_Voltage	as	LNB_1_Voltage_dt3
	,	d.LNB_2_ShortCircuitDetectState	as	LNB_2_ShortCircuitDetectState_dt3
	,	d.LNB_2_SState	as	LNB_2_SState_dt3
	,	d.LNB_2_SVoltage	as	LNB_2_SVoltage_dt3
	,	d.Locked	as	Locked_dt3
	,	d.LTRFEC	as	LTRFEC_dt3
	,	d.LTRFrequency	as	LTRFrequency_dt3
	,	d.LTRInversion	as	LTRInversion_dt3
	,	d.LTRMode	as	LTRMode_dt3
	,	d.LTRModulation	as	LTRModulation_dt3
	,	d.LTRRollOff	as	LTRRollOff_dt3
	,	d.LTRSymbolRate	as	LTRSymbolRate_dt3
	,	d.RxPowerLevel	as	RxPowerLevel_dt3
	,	d.Scan	as	Scan_dt3
	,	d.SNR	as	SNR_dt3
	,	d.Status	as	Status_dt3
	,	d.TSBERCount	as	TSBERCount_dt3
	,	d.TSBERWindow	as	TSBERWindow_dt3
	,	d.TSFEC	as	TSFEC_dt3
	,	d.TSFrequency	as	TSFrequency_dt3
	,	d.TSInversion	as	TSInversion_dt3
	,	d.TSMode	as	TSMode_dt3
	,	d.TSModulation	as	TSModulation_dt3
	,	d.TSRollOff	as	TSRollOff_dt3
	,	d.TSSignalStatus	as	TSSignalStatus_dt3
	,	d.TSSignalStrength	as	TSSignalStrength_dt3
	,	d.TSSymbolRate	as	TSSymbolRate_dt3
	,	d.TunerState	as	TunerState_dt3

	,	e.NSS_flag	as	NSS_flag_dt4
	,	e.X_SKY_COM_SMARTCARD_ID	as	X_SKY_COM_SMARTCARD_ID_dt4
	,	e.LNB_1_ShortCircuitDetectState	as	LNB_1_ShortCircuitDetectState_dt4
	,	e.LNB_1_State	as	LNB_1_State_dt4
	,	e.LNB_1_Voltage	as	LNB_1_Voltage_dt4
	,	e.LNB_2_ShortCircuitDetectState	as	LNB_2_ShortCircuitDetectState_dt4
	,	e.LNB_2_SState	as	LNB_2_SState_dt4
	,	e.LNB_2_SVoltage	as	LNB_2_SVoltage_dt4
	,	e.Locked	as	Locked_dt4
	,	e.LTRFEC	as	LTRFEC_dt4
	,	e.LTRFrequency	as	LTRFrequency_dt4
	,	e.LTRInversion	as	LTRInversion_dt4
	,	e.LTRMode	as	LTRMode_dt4
	,	e.LTRModulation	as	LTRModulation_dt4
	,	e.LTRRollOff	as	LTRRollOff_dt4
	,	e.LTRSymbolRate	as	LTRSymbolRate_dt4
	,	e.RxPowerLevel	as	RxPowerLevel_dt4
	,	e.Scan	as	Scan_dt4
	,	e.SNR	as	SNR_dt4
	,	e.Status	as	Status_dt4
	,	e.TSBERCount	as	TSBERCount_dt4
	,	e.TSBERWindow	as	TSBERWindow_dt4
	,	e.TSFEC	as	TSFEC_dt4
	,	e.TSFrequency	as	TSFrequency_dt4
	,	e.TSInversion	as	TSInversion_dt4
	,	e.TSMode	as	TSMode_dt4
	,	e.TSModulation	as	TSModulation_dt4
	,	e.TSRollOff	as	TSRollOff_dt4
	,	e.TSSignalStatus	as	TSSignalStatus_dt4
	,	e.TSSignalStrength	as	TSSignalStrength_dt4
	,	e.TSSymbolRate	as	TSSymbolRate_dt4
	,	e.TunerState	as	TunerState_dt4

	,	f.NSS_flag	as	NSS_flag_dt5
	,	f.X_SKY_COM_SMARTCARD_ID	as	X_SKY_COM_SMARTCARD_ID_dt5
	,	f.LNB_1_ShortCircuitDetectState	as	LNB_1_ShortCircuitDetectState_dt5
	,	f.LNB_1_State	as	LNB_1_State_dt5
	,	f.LNB_1_Voltage	as	LNB_1_Voltage_dt5
	,	f.LNB_2_ShortCircuitDetectState	as	LNB_2_ShortCircuitDetectState_dt5
	,	f.LNB_2_SState	as	LNB_2_SState_dt5
	,	f.LNB_2_SVoltage	as	LNB_2_SVoltage_dt5
	,	f.Locked	as	Locked_dt5
	,	f.LTRFEC	as	LTRFEC_dt5
	,	f.LTRFrequency	as	LTRFrequency_dt5
	,	f.LTRInversion	as	LTRInversion_dt5
	,	f.LTRMode	as	LTRMode_dt5
	,	f.LTRModulation	as	LTRModulation_dt5
	,	f.LTRRollOff	as	LTRRollOff_dt5
	,	f.LTRSymbolRate	as	LTRSymbolRate_dt5
	,	f.RxPowerLevel	as	RxPowerLevel_dt5
	,	f.Scan	as	Scan_dt5
	,	f.SNR	as	SNR_dt5
	,	f.Status	as	Status_dt5
	,	f.TSBERCount	as	TSBERCount_dt5
	,	f.TSBERWindow	as	TSBERWindow_dt5
	,	f.TSFEC	as	TSFEC_dt5
	,	f.TSFrequency	as	TSFrequency_dt5
	,	f.TSInversion	as	TSInversion_dt5
	,	f.TSMode	as	TSMode_dt5
	,	f.TSModulation	as	TSModulation_dt5
	,	f.TSRollOff	as	TSRollOff_dt5
	,	f.TSSignalStatus	as	TSSignalStatus_dt5
	,	f.TSSignalStrength	as	TSSignalStrength_dt5
	,	f.TSSymbolRate	as	TSSymbolRate_dt5
	,	f.TunerState	as	TunerState_dt5

	,	row_number() over (order by	a.device_id,a.dt)	row_num_lag

from
				(
					select
							*
						,	lag(dt,1) over (partition by device_id order by dt)	dt1
						,	lag(dt,2) over (partition by device_id order by dt)	dt2
						,	lag(dt,3) over (partition by device_id order by dt)	dt3
						,	lag(dt,4) over (partition by device_id order by dt)	dt4
						,	lag(dt,5) over (partition by device_id order by dt)	dt5
					from	tmp_tuner_denorm_201608
				)						a
	inner join	tmp_tuner_denorm_201608	b	on	a.dt1		=	b.dt
											and	a.device_id	=	b.device_id
	inner join	tmp_tuner_denorm_201608	c	on	a.dt2		=	c.dt
											and	a.device_id	=	c.device_id
	inner join	tmp_tuner_denorm_201608	d	on	a.dt3		=	d.dt
											and	a.device_id	=	d.device_id
	inner join	tmp_tuner_denorm_201608	e	on	a.dt4		=	e.dt
											and	a.device_id	=	e.device_id
	inner join	tmp_tuner_denorm_201608	f	on	a.dt5		=	f.dt
											and	a.device_id	=	f.device_id
where	-- For model development we want to ignore preceding messages that are already suffering from NSS
		b.NSS_flag	=	0
	and	c.NSS_flag	=	0
	and	d.NSS_flag	=	0
	and	e.NSS_flag	=	0
	and	f.NSS_flag	=	0
;







