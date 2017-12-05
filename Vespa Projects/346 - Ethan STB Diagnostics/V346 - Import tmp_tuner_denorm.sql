------------------------------------------------------------------------
-- Import dataset without time lags (all original fields)
------------------------------------------------------------------------

create or replace variable @input_file_path varchar(256) = '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/tanghoi/Diagnostics/tmp_tuner_denorm_201608_1.csv';
 
-- Prepare temporary import table
drop table #tmp_undelimited;

create table #tmp_undelimited	(
		device_id	varchar(8000)
	,	report_timestamp	varchar(8000)
	,	dt	varchar(8000)
	,	NSS_flag	varchar(8000)
	,	X_SKY_COM_SMARTCARD_ID	varchar(8000)
	,	LNB_1_ShortCircuitDetectState	varchar(8000)
	,	LNB_1_State	varchar(8000)
	,	LNB_1_Voltage	varchar(8000)
	,	LNB_2_ShortCircuitDetectState	varchar(8000)
	,	LNB_2_SState	varchar(8000)
	,	LNB_2_SVoltage	varchar(8000)
	,	Locked	varchar(8000)
	,	LTRFEC	varchar(8000)
	,	LTRFrequency	varchar(8000)
	,	LTRInversion	varchar(8000)
	,	LTRMode	varchar(8000)
	,	LTRModulation	varchar(8000)
	,	LTRRollOff	varchar(8000)
	,	LTRSymbolRate	varchar(8000)
	,	RxPowerLevel	varchar(8000)
	,	Scan	varchar(8000)
	,	SNR	varchar(8000)
	,	Status	varchar(8000)
	,	TSBERCount	varchar(8000)
	,	TSBERWindow	varchar(8000)
	,	TSFEC	varchar(8000)
	,	TSFrequency	varchar(8000)
	,	TSInversion	varchar(8000)
	,	TSMode	varchar(8000)
	,	TSModulation	varchar(8000)
	,	TSRollOff	varchar(8000)
	,	TSSignalStatus	varchar(8000)
	,	TSSignalStrength	varchar(8000)
	,	TSSymbolRate	varchar(8000)
	,	TunerState	varchar(8000)
	,	row_num	varchar(8000)
								)
;


-- Read the data
create or replace variable @sql_ varchar(8000);
set	@sql_	=	'
load table #tmp_undelimited	(
		device_id	'',''
	,	report_timestamp	'',''
	,	dt	'',''
	,	NSS_flag	'',''
	,	X_SKY_COM_SMARTCARD_ID	'',''
	,	LNB_1_ShortCircuitDetectState	'',''
	,	LNB_1_State	'',''
	,	LNB_1_Voltage	'',''
	,	LNB_2_ShortCircuitDetectState	'',''
	,	LNB_2_SState	'',''
	,	LNB_2_SVoltage	'',''
	,	Locked	'',''
	,	LTRFEC	'',''
	,	LTRFrequency	'',''
	,	LTRInversion	'',''
	,	LTRMode	'',''
	,	LTRModulation	'',''
	,	LTRRollOff	'',''
	,	LTRSymbolRate	'',''
	,	RxPowerLevel	'',''
	,	Scan	'',''
	,	SNR	'',''
	,	Status	'',''
	,	TSBERCount	'',''
	,	TSBERWindow	'',''
	,	TSFEC	'',''
	,	TSFrequency	'',''
	,	TSInversion	'',''
	,	TSMode	'',''
	,	TSModulation	'',''
	,	TSRollOff	'',''
	,	TSSignalStatus	'',''
	,	TSSignalStrength	'',''
	,	TSSymbolRate	'',''
	,	TunerState	'',''
	,	row_num	''\n''
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
drop table tmp_tuner_denorm_201608;
create table tmp_tuner_denorm_201608(
		device_id varchar(16)
	,	report_timestamp varchar(22)
	,	dt timestamp
	,	NSS_flag bit
	,	X_SKY_COM_SMARTCARD_ID int
	,	LNB_1_ShortCircuitDetectState varchar(256)
	,	LNB_1_State varchar(256)
	,	LNB_1_Voltage	varchar(256)
	,	LNB_2_ShortCircuitDetectState	varchar(256)
	,	LNB_2_SState	varchar(256)
	,	LNB_2_SVoltage	varchar(256)
	,	Locked	varchar(256)
	,	LTRFEC	varchar(256)
	,	LTRFrequency	bigint
	,	LTRInversion	varchar(256)
	,	LTRMode	varchar(256)
	,	LTRModulation	varchar(256)
	,	LTRRollOff	varchar(256)
	,	LTRSymbolRate	bigint
	,	RxPowerLevel	bigint
	,	Scan	varchar(256)
	,	SNR	bigint
	,	Status	varchar(256)
	,	TSBERCount	bigint
	,	TSBERWindow	bigint
	,	TSFEC	varchar(256)
	,	TSFrequency	bigint
	,	TSInversion	varchar(256)
	,	TSMode	varchar(256)
	,	TSModulation	varchar(256)
	,	TSRollOff	varchar(256)
	,	TSSignalStatus	varchar(256)
	,	TSSignalStrength	bigint
	,	TSSymbolRate	bigint
	,	TunerState	varchar(256)
	,	row_num	bigint
)
;


insert into	tmp_tuner_denorm_201608
select -- top 100
		cast(device_id as varchar(16))
	,	cast(report_timestamp as varchar(22))
	,	cast(dt as timestamp)
	,	cast(NSS_flag as bit)
	,	cast(X_SKY_COM_SMARTCARD_ID as int)
	,	cast(LNB_1_ShortCircuitDetectState as varchar(256))
	,	cast(LNB_1_State as varchar(256))
	,	cast(LNB_1_Voltage	as	varchar(256))
	,	cast(LNB_2_ShortCircuitDetectState	as	varchar(256))
	,	cast(LNB_2_SState	as	varchar(256))
	,	cast(LNB_2_SVoltage	as	varchar(256))
	,	cast(Locked	as	varchar(256))
	,	cast(LTRFEC	as	varchar(256))
	,	cast(LTRFrequency	as	bigint)
	,	cast(LTRInversion	as	varchar(256))
	,	cast(LTRMode	as	varchar(256))
	,	cast(LTRModulation	as	varchar(256))
	,	cast(LTRRollOff	as	varchar(256))
	,	cast(LTRSymbolRate	as	bigint)
	,	cast(RxPowerLevel	as	bigint)
	,	cast(Scan	as	varchar(256))
	,	cast(SNR	as	bigint)
	,	cast(Status	as	varchar(256))
	,	cast(TSBERCount	as	bigint)
	,	cast(TSBERWindow	as	bigint)
	,	cast(TSFEC	as	varchar(256))
	,	cast(TSFrequency	as	bigint)
	,	cast(TSInversion	as	varchar(256))
	,	cast(TSMode	as	varchar(256))
	,	cast(TSModulation	as	varchar(256))
	,	cast(TSRollOff	as	varchar(256))
	,	cast(TSSignalStatus	as	varchar(256))
	,	cast(TSSignalStrength	as	bigint)
	,	cast(TSSymbolRate	as	bigint)
	,	cast(TunerState	as	varchar(256))
	,	cast(row_num	as	bigint)
from #tmp_undelimited
;


create hg	index idx_1 on tmp_tuner_denorm_201608(device_id);
create dttm	index idx_2 on tmp_tuner_denorm_201608(dt);
create dttm	index idx_2 on tmp_tuner_denorm_201608(dt);








------------------------------------------------------------------------
-- Import dataset WITH time lags (best predictors only)
------------------------------------------------------------------------

create or replace variable @input_file_path varchar(256) = '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/tanghoi/Diagnostics/tmp_tuner_denorm_201608_with_lags_refined.csv';

drop table #tmp_undelimited;
create table #tmp_undelimited(
		device_id	varchar(8000)
	,	dt	varchar(8000)
	,	nss_flag	varchar(8000)
	,	x_sky_com_smartcard_id	varchar(8000)
	,	dt1	varchar(8000)
	,	dt2	varchar(8000)
	,	dt3	varchar(8000)
	,	dt4	varchar(8000)
	,	dt5	varchar(8000)
	,	snr	varchar(8000)
	,	tsberwindow	varchar(8000)
	,	rxpowerlevel	varchar(8000)
	,	tsbercount	varchar(8000)
	,	tssignalstrength	varchar(8000)
	,	snr_dt1	varchar(8000)
	,	tsberwindow_dt1	varchar(8000)
	,	rxpowerlevel_dt1	varchar(8000)
	,	tsbercount_dt1	varchar(8000)
	,	tssignalstrength_dt1	varchar(8000)
	,	snr_dt2	varchar(8000)
	,	tsberwindow_dt2	varchar(8000)
	,	rxpowerlevel_dt2	varchar(8000)
	,	tsbercount_dt2	varchar(8000)
	,	tssignalstrength_dt2	varchar(8000)
	,	snr_dt3	varchar(8000)
	,	tsberwindow_dt3	varchar(8000)
	,	rxpowerlevel_dt3	varchar(8000)
	,	tsbercount_dt3	varchar(8000)
	,	tssignalstrength_dt3	varchar(8000)
	,	snr_dt4	varchar(8000)
	,	tsberwindow_dt4	varchar(8000)
	,	rxpowerlevel_dt4	varchar(8000)
	,	tsbercount_dt4	varchar(8000)
	,	tssignalstrength_dt4	varchar(8000)
	,	snr_dt5	varchar(8000)
	,	tsberwindow_dt5	varchar(8000)
	,	rxpowerlevel_dt5	varchar(8000)
	,	tsbercount_dt5	varchar(8000)
	,	tssignalstrength_dt5	varchar(8000)
)
;


-- Read the data
create or replace variable @sql_ varchar(8000);
set	@sql_	=	'
load table #tmp_undelimited	(
		device_id	'',''
	,	dt	'',''
	,	nss_flag	'',''
	,	x_sky_com_smartcard_id	'',''
	,	dt1	'',''
	,	dt2	'',''
	,	dt3	'',''
	,	dt4	'',''
	,	dt5	'',''
	,	snr	'',''
	,	tsberwindow	'',''
	,	rxpowerlevel	'',''
	,	tsbercount	'',''
	,	tssignalstrength	'',''
	,	snr_dt1	'',''
	,	tsberwindow_dt1	'',''
	,	rxpowerlevel_dt1	'',''
	,	tsbercount_dt1	'',''
	,	tssignalstrength_dt1	'',''
	,	snr_dt2	'',''
	,	tsberwindow_dt2	'',''
	,	rxpowerlevel_dt2	'',''
	,	tsbercount_dt2	'',''
	,	tssignalstrength_dt2	'',''
	,	snr_dt3	'',''
	,	tsberwindow_dt3	'',''
	,	rxpowerlevel_dt3	'',''
	,	tsbercount_dt3	'',''
	,	tssignalstrength_dt3	'',''
	,	snr_dt4	'',''
	,	tsberwindow_dt4	'',''
	,	rxpowerlevel_dt4	'',''
	,	tsbercount_dt4	'',''
	,	tssignalstrength_dt4	'',''
	,	snr_dt5	'',''
	,	tsberwindow_dt5	'',''
	,	rxpowerlevel_dt5	'',''
	,	tsbercount_dt5	'',''
	,	tssignalstrength_dt5	''\n''
)
from ''' || @input_file_path || '''
QUOTES OFF
ESCAPES OFF
SKIP 1
NOTIFY 1000
'
;

execute (@sql_);


drop table tmp_tuner_denorm_201608_with_lags_refined;
create table tmp_tuner_denorm_201608_with_lags_refined(
		device_id	varchar(16)
	,	dt	timestamp
	,	nss_flag	bit
	,	x_sky_com_smartcard_id	int
	,	dt1	timestamp
	,	dt2	timestamp
	,	dt3	timestamp
	,	dt4	timestamp
	,	dt5	timestamp
	,	snr	bigint
	,	tsberwindow	bigint
	,	rxpowerlevel	bigint
	,	tsbercount	bigint
	,	tssignalstrength	bigint
	,	snr_dt1	bigint
	,	tsberwindow_dt1	bigint
	,	rxpowerlevel_dt1	bigint
	,	tsbercount_dt1	bigint
	,	tssignalstrength_dt1	bigint
	,	snr_dt2	bigint
	,	tsberwindow_dt2	bigint
	,	rxpowerlevel_dt2	bigint
	,	tsbercount_dt2	bigint
	,	tssignalstrength_dt2	bigint
	,	snr_dt3	bigint
	,	tsberwindow_dt3	bigint
	,	rxpowerlevel_dt3	bigint
	,	tsbercount_dt3	bigint
	,	tssignalstrength_dt3	bigint
	,	snr_dt4	bigint
	,	tsberwindow_dt4	bigint
	,	rxpowerlevel_dt4	bigint
	,	tsbercount_dt4	bigint
	,	tssignalstrength_dt4	bigint
	,	snr_dt5	bigint
	,	tsberwindow_dt5	bigint
	,	rxpowerlevel_dt5	bigint
	,	tsbercount_dt5	bigint
	,	tssignalstrength_dt5	bigint
)
;

insert into tmp_tuner_denorm_201608_with_lags_refined
select
		cast(device_id	as	varchar(16))
	,	cast(dt	as	timestamp)
	,	cast(nss_flag	as	bit)
	,	cast(x_sky_com_smartcard_id	as	int)
	,	cast(dt1	as	timestamp)
	,	cast(dt2	as	timestamp)
	,	cast(dt3	as	timestamp)
	,	cast(dt4	as	timestamp)
	,	cast(dt5	as	timestamp)
	,	cast(snr	as	bigint)
	,	cast(tsberwindow	as	bigint)
	,	cast(rxpowerlevel	as	bigint)
	,	cast(tsbercount	as	bigint)
	,	cast(tssignalstrength	as	bigint)
	,	cast(snr_dt1	as	bigint)
	,	cast(tsberwindow_dt1	as	bigint)
	,	cast(rxpowerlevel_dt1	as	bigint)
	,	cast(tsbercount_dt1	as	bigint)
	,	cast(tssignalstrength_dt1	as	bigint)
	,	cast(snr_dt2	as	bigint)
	,	cast(tsberwindow_dt2	as	bigint)
	,	cast(rxpowerlevel_dt2	as	bigint)
	,	cast(tsbercount_dt2	as	bigint)
	,	cast(tssignalstrength_dt2	as	bigint)
	,	cast(snr_dt3	as	bigint)
	,	cast(tsberwindow_dt3	as	bigint)
	,	cast(rxpowerlevel_dt3	as	bigint)
	,	cast(tsbercount_dt3	as	bigint)
	,	cast(tssignalstrength_dt3	as	bigint)
	,	cast(snr_dt4	as	bigint)
	,	cast(tsberwindow_dt4	as	bigint)
	,	cast(rxpowerlevel_dt4	as	bigint)
	,	cast(tsbercount_dt4	as	bigint)
	,	cast(tssignalstrength_dt4	as	bigint)
	,	cast(snr_dt5	as	bigint)
	,	cast(tsberwindow_dt5	as	bigint)
	,	cast(rxpowerlevel_dt5	as	bigint)
	,	cast(tsbercount_dt5	as	bigint)
	,	cast(tssignalstrength_dt5	as	bigint)
from	#tmp_undelimited
;

create hg	index idx_1	on	tmp_tuner_denorm_201608_with_lags_refined(device_id);
create dttm	index idx_2	on	tmp_tuner_denorm_201608_with_lags_refined(dt);
create dttm	index idx_3	on	tmp_tuner_denorm_201608_with_lags_refined(dt1);
create dttm	index idx_4	on	tmp_tuner_denorm_201608_with_lags_refined(dt2);
create dttm	index idx_5	on	tmp_tuner_denorm_201608_with_lags_refined(dt3);
create dttm	index idx_6	on	tmp_tuner_denorm_201608_with_lags_refined(dt4);
create dttm	index idx_7	on	tmp_tuner_denorm_201608_with_lags_refined(dt5);
create hg	index idx_8	on	tmp_tuner_denorm_201608_with_lags_refined(x_sky_com_smartcard_id);






------------------------------------------------------------------------
-- Append weather data
------------------------------------------------------------------------

drop table #tmp;
select --top 20 
		a.device_id
	,	a.dt
	,	a.nss_flag
	,	a.x_sky_com_smartcard_id
	,	a.dt1
	,	a.dt2
	,	a.dt3
	,	a.dt4
	,	a.dt5
	,	a.snr
	,	a.tsberwindow
	,	a.rxpowerlevel
	,	a.tsbercount
	,	a.tssignalstrength
	,	a.snr_dt1
	,	a.tsberwindow_dt1
	,	a.rxpowerlevel_dt1
	,	a.tsbercount_dt1
	,	a.tssignalstrength_dt1
	,	a.snr_dt2
	,	a.tsberwindow_dt2
	,	a.rxpowerlevel_dt2
	,	a.tsbercount_dt2
	,	a.tssignalstrength_dt2
	,	a.snr_dt3
	,	a.tsberwindow_dt3
	,	a.rxpowerlevel_dt3
	,	a.tsbercount_dt3
	,	a.tssignalstrength_dt3
	,	a.snr_dt4
	,	a.tsberwindow_dt4
	,	a.rxpowerlevel_dt4
	,	a.tsbercount_dt4
	,	a.tssignalstrength_dt4
	,	a.snr_dt5
	,	a.tsberwindow_dt5
	,	a.rxpowerlevel_dt5
	,	a.tsbercount_dt5
	,	a.tssignalstrength_dt5
	,	stb.account_number
	,	stb.account_sub_type
	,	stb.account_type
	,	stb.box_installed_dt
	,	stb.box_replaced_dt
	,	stb.service_instance_id
	,	sav.fin_postcode_outcode_paf
	,	wea.date_time
	,	wea.cloud_cover
	,	wea.relative_humidity
	,	wea.temperature
	,	wea.weather_type
	,	wea.weather_type_desc
	,	wea.wind_direction
	,	wea.wind_speed
into	#tmp
from
				tmp_tuner_denorm_201608_with_lags_refined	a
	inner join	cust_set_top_box							stb on	a.device_id			=		stb.decoder_nds_number
																and	account_sub_type	=		'Normal'
																and	account_type		=		'Standard'
																and	a.dt				between	stb.box_installed_dt
																						and		stb.box_replaced_dt
	inner join	cust_single_account_view					sav	on	stb.account_number	=	sav.account_number
	inner join	WEATHER_DATA_HISTORY						wea	on	sav.fin_postcode_outcode_paf	=		wea.district
																and	wea.date_time					between	dateadd(hour,-6,a.dt)
																									and		a.dt
;

create hg	index idx_1	on	#tmp(device_id);
create dttm	index idx_2	on	#tmp(dt);
create dttm	index idx_3	on	#tmp(dt1);
create dttm	index idx_4	on	#tmp(dt2);
create dttm	index idx_5	on	#tmp(dt3);
create dttm	index idx_6	on	#tmp(dt4);
create dttm	index idx_7	on	#tmp(dt5);
create hg	index idx_8	on	#tmp(x_sky_com_smartcard_id);


drop table tmp_tuner_denorm_201608_with_lags_refined_plus_weather;
select
		*
	,	rank()	over	(
							partition by
									device_id
								,	dt
								,	nss_flag
								,	x_sky_com_smartcard_id
								,	account_number
								,	account_sub_type
								,	account_type
								,	box_installed_dt
								,	box_replaced_dt
								,	service_instance_id
								,	fin_postcode_outcode_paf
							order by	date_time	desc
						)	as	rnk
into	tmp_tuner_denorm_201608_with_lags_refined_plus_weather
from	#tmp
;

create hg	index idx_1	on	tmp_tuner_denorm_201608_with_lags_refined_plus_weather(device_id);
create dttm	index idx_2	on	tmp_tuner_denorm_201608_with_lags_refined_plus_weather(dt);
create dttm	index idx_3	on	tmp_tuner_denorm_201608_with_lags_refined_plus_weather(dt1);
create dttm	index idx_4	on	tmp_tuner_denorm_201608_with_lags_refined_plus_weather(dt2);
create dttm	index idx_5	on	tmp_tuner_denorm_201608_with_lags_refined_plus_weather(dt3);
create dttm	index idx_6	on	tmp_tuner_denorm_201608_with_lags_refined_plus_weather(dt4);
create dttm	index idx_7	on	tmp_tuner_denorm_201608_with_lags_refined_plus_weather(dt5);
create hg	index idx_8	on	tmp_tuner_denorm_201608_with_lags_refined_plus_weather(x_sky_com_smartcard_id);
create lf	index idx_9	on	tmp_tuner_denorm_201608_with_lags_refined_plus_weather(rnk);


drop table tmp_tuner_denorm_201608_with_lags_refined_plus_weather_denorm;
select
		a.device_id
	,	a.dt
	,	a.nss_flag
	,	a.x_sky_com_smartcard_id
	,	a.dt1
	,	a.dt2
	,	a.dt3
	,	a.dt4
	,	a.dt5
	,	a.snr
	,	a.tsberwindow
	,	a.rxpowerlevel
	,	a.tsbercount
	,	a.tssignalstrength
	,	a.snr_dt1
	,	a.tsberwindow_dt1
	,	a.rxpowerlevel_dt1
	,	a.tsbercount_dt1
	,	a.tssignalstrength_dt1
	,	a.snr_dt2
	,	a.tsberwindow_dt2
	,	a.rxpowerlevel_dt2
	,	a.tsbercount_dt2
	,	a.tssignalstrength_dt2
	,	a.snr_dt3
	,	a.tsberwindow_dt3
	,	a.rxpowerlevel_dt3
	,	a.tsbercount_dt3
	,	a.tssignalstrength_dt3
	,	a.snr_dt4
	,	a.tsberwindow_dt4
	,	a.rxpowerlevel_dt4
	,	a.tsbercount_dt4
	,	a.tssignalstrength_dt4
	,	a.snr_dt5
	,	a.tsberwindow_dt5
	,	a.rxpowerlevel_dt5
	,	a.tsbercount_dt5
	,	a.tssignalstrength_dt5
	,	a.account_number
	,	a.account_sub_type
	,	a.account_type
	,	a.box_installed_dt
	,	a.box_replaced_dt
	,	a.service_instance_id
	,	a.fin_postcode_outcode_paf

	,	a.date_time		as	date_time_wdt1
	,	a.cloud_cover		as	cloud_cover_wdt1
	,	a.relative_humidity		as	relative_humidity_wdt1
	,	a.temperature		as	temperature_wdt1
	,	a.weather_type		as	weather_type_wdt1
	,	a.weather_type_desc		as	weather_type_desc_wdt1
	,	a.wind_direction		as	wind_direction_wdt1
	,	a.wind_speed		as	wind_speed_wdt1

	,	b.date_time		as	date_time_wdt2
	,	b.cloud_cover		as	cloud_cover_wdt2
	,	b.relative_humidity		as	relative_humidity_wdt2
	,	b.temperature		as	temperature_wdt2
	,	b.weather_type		as	weather_type_wdt2
	,	b.weather_type_desc		as	weather_type_desc_wdt2
	,	b.wind_direction		as	wind_direction_wdt2
	,	b.wind_speed		as	wind_speed_wdt2

	,	c.date_time		as	date_time_wdt3
	,	c.cloud_cover		as	cloud_cover_wdt3
	,	c.relative_humidity		as	relative_humidity_wdt3
	,	c.temperature		as	temperature_wdt3
	,	c.weather_type		as	weather_type_wdt3
	,	c.weather_type_desc		as	weather_type_desc_wdt3
	,	c.wind_direction		as	wind_direction_wdt3
	,	c.wind_speed		as	wind_speed_wdt3

	,	d.date_time		as	date_time_wdt4
	,	d.cloud_cover		as	cloud_cover_wdt4
	,	d.relative_humidity		as	relative_humidity_wdt4
	,	d.temperature		as	temperature_wdt4
	,	d.weather_type		as	weather_type_wdt4
	,	d.weather_type_desc		as	weather_type_desc_wdt4
	,	d.wind_direction		as	wind_direction_wdt4
	,	d.wind_speed		as	wind_speed_wdt4

	,	e.date_time		as	date_time_wdt5
	,	e.cloud_cover		as	cloud_cover_wdt5
	,	e.relative_humidity		as	relative_humidity_wdt5
	,	e.temperature		as	temperature_wdt5
	,	e.weather_type		as	weather_type_wdt5
	,	e.weather_type_desc		as	weather_type_desc_wdt5
	,	e.wind_direction		as	wind_direction_wdt5
	,	e.wind_speed		as	wind_speed_wdt5

	,	f.date_time		as	date_time_wdt6
	,	f.cloud_cover		as	cloud_cover_wdt6
	,	f.relative_humidity		as	relative_humidity_wdt6
	,	f.temperature		as	temperature_wdt6
	,	f.weather_type		as	weather_type_wdt6
	,	f.weather_type_desc		as	weather_type_desc_wdt6
	,	f.wind_direction		as	wind_direction_wdt6
	,	f.wind_speed		as	wind_speed_wdt6

into	tmp_tuner_denorm_201608_with_lags_refined_plus_weather_denorm
from
				tmp_tuner_denorm_201608_with_lags_refined_plus_weather	a
	inner join	tmp_tuner_denorm_201608_with_lags_refined_plus_weather	b	on	a.device_id	=	b.device_id
																			and	a.dt		=	b.dt
																			and	b.rnk		=	2
	inner join	tmp_tuner_denorm_201608_with_lags_refined_plus_weather	c	on	a.device_id	=	c.device_id
																			and	a.dt		=	c.dt
																			and	c.rnk		=	3
	inner join	tmp_tuner_denorm_201608_with_lags_refined_plus_weather	d	on	a.device_id	=	d.device_id
																			and	a.dt		=	d.dt
																			and	d.rnk		=	4
	inner join	tmp_tuner_denorm_201608_with_lags_refined_plus_weather	e	on	a.device_id	=	e.device_id
																			and	a.dt		=	e.dt
																			and	e.rnk		=	5
	inner join	tmp_tuner_denorm_201608_with_lags_refined_plus_weather	f	on	a.device_id	=	f.device_id
																			and	a.dt		=	f.dt
																			and	f.rnk		=	6
where	a.rnk	=	1
;

drop table tmp_tuner_denorm_201608_with_lags_refined_plus_weather;

/*
UNLOAD
select * from tmp_tuner_denorm_201608_with_lags_refined_plus_weather_denorm
TO '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/tanghoi/Diagnostics/tmp_tuner_denorm_201608_with_lags_refined_plus_weather_denorm.csv'
QUOTES OFF
;
*/