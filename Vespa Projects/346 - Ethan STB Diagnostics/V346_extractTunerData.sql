-- Define analysis date range
create or replace variable @dt_start date	=	'2016-03-25';
create or replace variable @dt_end 	date	=	'2016-04-15';





-- Extract only those records associated with a tuner that's locked to the EPG frequency
drop table #tmp;
select
		b.tstamp
	,	b.id
    ,	b.parameter_name
    ,	substring(substring(b.parameter_name,69),charindex('.',substring(b.parameter_name,69))+1)						as	parameter_name_short
    ,	b.parameter_value
    ,   cast(substring(substring(a.parameter_name,69),1,charindex('.',substring(a.parameter_name,69))-1) as tinyint)	as  EPG_tuner_number
into    #tmp
from
				et_technical	a
    inner join	et_technical	b	on	b.tstamp							between	@dt_start
												                            and		@dt_end
    								and	substring(a.parameter_name,1,70)	=		substring(b.parameter_name,1,70)
                                    and	a.id								=		b.id
                                    and	a.tstamp                        	=		b.tstamp
where
		a.parameter_name	like	'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.2.TSFrequency%'
	-- and a.parameter_value	=		'1368000'
	and	a.tstamp			between	@dt_start
                            and		@dt_end
;

create hg index idx1 on #tmp(id);
create dttm index idx2 on #tmp(tstamp);
create lf index idx3 on #tmp(EPG_tuner_number);






-- Denormalise all tuner-related parameters and reported values
drop table #tmp2;
select -- top 100
		tstamp
	,	id
    ,	EPG_tuner_number
    ,	max(case when parameter_name_short = 'Status' 			then	parameter_value	end)	as	Status
    ,	max(case when parameter_name_short = 'TSFrequency' 		then	parameter_value	end)	as	TSFrequency
    ,	max(case when parameter_name_short = 'TSInversion' 		then	parameter_value	end)	as	TSInversion
    ,	max(case when parameter_name_short = 'LTRFEC' 			then	parameter_value	end)	as	LTRFEC
    ,	max(case when parameter_name_short = 'SNR' 				then	parameter_value	end)	as	SNR
    ,	max(case when parameter_name_short = 'TSBERCount' 		then	parameter_value	end)	as	TSBERCount
    ,	max(case when parameter_name_short = 'TSSymbolRate' 	then	parameter_value	end)	as	TSSymbolRate
    ,	max(case when parameter_name_short = 'TSModulation' 	then	parameter_value	end)	as	TSModulation
    ,	max(case when parameter_name_short = 'TSFEC'		 	then	parameter_value	end)	as	TSFEC
    ,	max(case when parameter_name_short = 'TSSignalStrength' then	parameter_value	end)	as	TSSignalStrength
    ,	max(case when parameter_name_short = 'RxPowerLevel' 	then	parameter_value	end)	as	RxPowerLevel
    ,	max(case when parameter_name_short = 'LTRSymbolRate' 	then	parameter_value	end)	as	LTRSymbolRate
    ,	max(case when parameter_name_short = 'TunerState' 		then	parameter_value	end)	as	TunerState
    ,	max(case when parameter_name_short = 'LTRModulation' 	then	parameter_value	end)	as	LTRModulation
    ,	max(case when parameter_name_short = 'LTRRollOff' 		then	parameter_value	end)	as	LTRRollOff
    ,	max(case when parameter_name_short = 'Scan' 			then	parameter_value	end)	as	Scan
    ,	max(case when parameter_name_short = 'TSRollOff' 		then	parameter_value	end)	as	TSRollOff
    ,	max(case when parameter_name_short = 'LTRFrequency' 	then	parameter_value	end)	as	LTRFrequency
    ,	max(case when parameter_name_short = 'TSMode' 			then	parameter_value	end)	as	TSMode
    ,	max(case when parameter_name_short = 'TSBERWindow' 		then	parameter_value	end)	as	TSBERWindow
    ,	max(case when parameter_name_short = 'LTRInversion' 	then	parameter_value	end)	as	LTRInversion
    ,	max(case when parameter_name_short = 'Locked' 			then	parameter_value	end)	as	Locked
    ,	max(case when parameter_name_short = 'TSSignalStatus' 	then	parameter_value	end)	as	TSSignalStatus
    ,	max(case when parameter_name_short = 'LTRMode' 			then	parameter_value	end)	as	LTRMode
into	#tmp2
from	#tmp
group by
		tstamp
	,	id
    ,	EPG_tuner_number
order by
		tstamp
	,	id
    ,	EPG_tuner_number
;

create hg index idx1 on #tmp2(id);
create dttm index idx2 on #tmp2(tstamp);
create lf index idx3 on #tmp2(EPG_tuner_number);









-- Cast reported values into appropriate types
drop table et_tuner_denorm;
select
		tstamp
	,	id
    ,	EPG_tuner_number
	,	Status
	,	cast(TSFrequency as int)	as	TSFrequency
	,	TSInversion
	,	LTRFEC
	,	cast(SNR as int)			as	SNR
	,	cast(TSBERCount as int)		as	TSBERCount
	,	cast(TSSymbolRate as int)	as	TSSymbolRate
	,	TSModulation
	,	TSFEC
	,	cast(TSSignalStrength as int)	as	TSSignalStrength
	,	cast(RxPowerLevel as int)	as	RxPowerLevel
	,	cast(LTRSymbolRate as int)	as	LTRSymbolRate
	,	TunerState
	,	LTRModulation
	,	cast(LTRRollOff as int)		as	LTRRollOff
	,	Scan
	,	cast(TSRollOff as int)		as	TSRollOff
	,	cast(LTRFrequency as int)	as	LTRFrequency
	,	TSMode
	,	cast(TSBERWindow as int)	as	TSBERWindow
	,	LTRInversion
	,	Locked
	,	TSSignalStatus
	,	LTRMode
into	et_tuner_denorm
from	#tmp2
where	TSFrequency	=	1368000
;

create hg index idx1 on et_tuner_denorm(id);
create dttm index idx2 on et_tuner_denorm(tstamp);
create lf index idx3 on et_tuner_denorm(EPG_tuner_number);








-- Join onto cust_set_top_box (ignoring any that have been associated with more than 1 account number)
drop table et_devices;
select
		et.id
	,	count(distinct stb.account_number)	n
into	et_devices
from
				et_tuner_denorm				et
	left join	cust_set_top_box			stb	on	et.id				=	stb.decoder_nds_number
group by
		et.id
having	n	=	1
;

create unique hg index idx1 on et_devices(id);









-- Filter for customer devices/accounts only
drop table et_device_accounts;
select
		et.id
	,	stb.account_number
	,	sav.fin_postcode_outcode_paf
	,	min	(
				case
					when	sav.ACCT_TYPE_code	=	'STD'	then	1
					else											0
				end
			)	as	cust_account_flag
into	et_device_accounts
from
				et_devices					et
	inner join	cust_set_top_box			stb	on	et.id				=	stb.decoder_nds_number
	inner join	CUST_SINGLE_ACCOUNT_VIEW	sav	on	stb.account_number	=	sav.account_number
												and	sav.CUST_ACTIVE_DTV	=	1
group by
		et.id
	,	stb.account_number
	,	sav.fin_postcode_outcode_paf
;

create unique hg index idx1 on et_device_accounts(id);
create hg index idx2 on et_device_accounts(account_number);



/*
-- Add weather data and export
drop table #final;
select
		et.*
	,	acc.account_number
	,	acc.cust_account_flag
    ,	acc.fin_postcode_outcode_paf
	,	wea.cloud_cover
	,	wea.date_time
	,	wea.relative_humidity
	,	wea.sunshine_duration
	,	wea.temperature
	,	wea.weather_type
	,	wea.weather_type_desc
	,	wea.wind_direction
	,	wea.wind_speed
into	#final
from
				et_tuner_denorm			et
	inner join	et_device_accounts		acc		on	et.id																		=	acc.id
	left join	WEATHER_DATA_HISTORY	wea		on	dateadd(hour,datepart(hour,et.tstamp),cast(date(et.tstamp) as datetime))	=	wea.date_time
												and	acc.fin_postcode_outcode_paf												=	wea.district
;
*/


/*	QA...
select
		cust_account_flag
	,	count(distinct id)
    ,	count(distinct account_number)
from	et_device_accounts
group by
		cust_account_flag
order by
		cust_account_flag
;
-- cust_account_flag   count(distinct et_device_accounts.id)   count(distinct et_device_accounts.account_number)
-- 0   7368    7353
-- 1   29886   29884




select
		devices
	,	count(distinct account_number)	accounts
from	(
            select
            		account_number
            	,	cust_account_flag
            	,	count(distinct id)	devices
            from	et_device_accounts
            group by
            		account_number
            	,	cust_account_flag
		)	t0
group by
		devices
order by
		devices
;
-- devices	accounts
-- 1	37220
-- 2	17

-- Since only only GW devices report tuner-specific diagnostics data, we won't see any Q Mini devices here. No need for panic!

*/






--------------------------------------------------------------------------------------------------
--------									ANALYSIS										------
--------------------------------------------------------------------------------------------------

-- Quick sense check on daily volumes
select
		date(tstamp)		dt
	,	count()				messages
	,	count(distinct id)	devices
from	et_tuner_denorm
group by	dt
order by	dt
;
/*
dt	messages	devices
2016-03-25	20226	16425
2016-03-26	20004	16359
2016-03-27	20379	15931
2016-03-28	22601	17988
2016-03-29	21857	17334
2016-03-30	21975	17703
2016-03-31	22277	17889
2016-04-01	22195	18073
2016-04-02	22282	18310
2016-04-03	22613	18847
2016-04-04	23983	18992
2016-04-05	23460	19126
2016-04-06	21562	17129
2016-04-07	24852	20840
2016-04-08	23599	19698
2016-04-09	24151	20338
2016-04-10	24128	20698
2016-04-11	25941	21026
2016-04-12	26666	21492
2016-04-13	25731	21685
2016-04-14	26964	23235
*/




-- Time of day of messages
select
		date(tstamp)			dt
	,	datepart(hour,tstamp)	hh
	,	datepart(minute,tstamp)	mm
	,	count()					messages
	,	count(distinct id)		devices
from	et_tuner_denorm
group by
		dt
	,	hh
	,	mm
order by
		dt
	,	hh
	,	mm
;





-- Diversity of tuners pointing to EPG frequency
select
		date(tstamp)			dt
	,	datepart(hour,tstamp)	hh
	,	datepart(minute,tstamp)	mm
	,	et.EPG_tuner_number
	,	count()					messages
	,	count(distinct et.id)	devices
from
				et_tuner_denorm		et
	inner join	et_device_accounts	acc		on	et.id	=	acc.id
group by
		dt
	,	hh
	,	mm
	,	et.EPG_tuner_number
order by
		dt
	,	hh
	,	mm
	,	et.EPG_tuner_number
;






-- Number of tuners pointing to EPG per device CONCURRENTLY
select
		num_tuners_on_EPG
	,	count(distinct id)	devices
from	(
            select
            		et.id
            	,	et.tstamp
                ,	count(distinct et.EPG_tuner_number)	num_tuners_on_EPG
            from
            				et_tuner_denorm		et
            	inner join	et_device_accounts	acc		on	et.id	=	acc.id
            group by
            		et.id
            	,	et.tstamp
		)	t0
group by	num_tuners_on_EPG
order by	num_tuners_on_EPG
;
/*
num_tuners_on_EPG	devices
1	37253
2	2740
3	87
*/







-- 









/*

select
		date(tstamp)	dt
	,	N
    ,	count()
from	(
            select
            		tstamp
            	,	id
                ,	count(distinct EPG_locked_tuner_number)	N
            from	#tmp
            group by
            		tstamp
            	,	id
		)	t0
group by
		dt
	,	N
order by
		dt
	,	N
;



select
		date(tstamp)		dt
	,	EPG_locked_tuner_number
	,	count(distinct id)
    ,	count(distinct tstamp)
from	#tmp
group by
		dt
	,	EPG_locked_tuner_number
order by
		dt
	,	EPG_locked_tuner_number
;

*/

