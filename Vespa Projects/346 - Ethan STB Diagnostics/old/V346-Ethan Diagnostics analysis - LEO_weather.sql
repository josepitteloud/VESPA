-- Prepare base data - extract only the tuner-related paramters and extend fields ahead of un-normalising the data

create or replace variable @SNR_threshold int;

set @SNR_threshold=330; -- 3.3 db

drop table #tmp;
select
                id
        ,       tstamp
    ,   cast(replace(parameter_name,'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.','') as varchar(255))   as  str
        ,       case    left(str,2)
                        when    '1.'            then    1
                        when    '2.'            then    2
                        when    '3.'            then    3
                        when    '4.'            then    4
                        when    '5.'            then    5
                        when    '6.'            then    6
                        when    '7.'            then    7
                        when    '8.'            then    8
                        when    '9.'            then    9
                        when    '10'            then    10
                        when    '11'            then    11
                        when    '12'            then    12
                        when    '13'            then    13
                        when    '14'            then    14
                        when    '15'            then    15
                        when    '16'            then    16
                        else                                            null
                end                                                                                             as      tuner_number
        ,       substring(str,3)                                                                as      param_name_short
    ,   case    param_name_short
            when    'TSFrequency'       then    cast(parameter_value as int)
            else                                null
        end                                             as  TSFrequency_
    ,   case    param_name_short
            when    'TSSymbolRate'      then    cast(parameter_value as int)
            else                                null
        end                                             as  TSSymbolRate_
    ,   case    param_name_short
            when    'SNR'               then    cast(parameter_value as int)
            else                                null
        end                                             as  SNR_
    ,   case    param_name_short
            when    'RxPowerLevel'      then    cast(parameter_value as int)
            else                                null
        end                                             as  RxPowerLevel_
    ,   case    param_name_short
            when    'Locked'            then    parameter_value
            else                                null
        end                                             as  Locked_
    ,   case    param_name_short
            when    'Status'            then    parameter_value
            else                                null
        end                                             as  Status_
    ,   case    param_name_short
            when    'TSModulation'      then    parameter_value
            else                                null
        end                                             as  TSModulation_
    ,   case    param_name_short
            when    'TSMode'            then    parameter_value
            else                                null
        end                                             as  TSMode_
    ,   case    param_name_short
            when    'TSFEC'             then    parameter_value
            else                                null
        end                                             as  TSFEC_
    ,   case    param_name_short
            when    'LTRFrequency'      then    cast(parameter_value as int)
            else                                null
        end                                             as  LTRFrequency_
    ,   case    param_name_short
            when    'SNR'               then    cast(parameter_value as int)-@SNR_threshold
            else                                null
        end                                             as  Margin_
    , cast (Margin_ as double)/100.0 as MarginDB_
    , cast (NULL as varchar(30)) as account_number_
    , cast (NULL as varchar(30)) as postcode_
    , cast (NULL as varchar(100)) as fin_postcode_outcode_paf_
    , cast (NULL as varchar(30)) as government_region_
    , cast (NULL as varchar(100)) as region_
    , cast (NULL as varchar(30)) as cloud_cover_
    , cast (NULL as varchar(100)) as relative_humidity_
    , cast (NULL as varchar(100)) as sunshine_duration_
    , cast (NULL as varchar(100)) as temperature_
    , cast (NULL as varchar(100)) as weather_type_
    , cast (NULL as varchar(100)) as weather_type_desc_
    , cast (NULL as varchar(100)) as wind_direction_
    , cast (NULL as varchar(100)) as wind_speed_
into    #tmp
from    ripolile.et_technical
where
                parameter_name  like    'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.%'
        and     param_name_short        in      (
                                                                        'TSFrequency'
                                        ,       'TSSymbolRate'
                                        ,       'SNR'
                                    ,   'RxPowerLevel'
                                    ,   'Locked'
                                    ,   'Status'
                                    ,   'TSModulation'
                                    ,   'TSMode'
                                    ,   'TSFEC'
                                    ,   'LTRFrequency'
                                                        )
;

create hg index hg1 on #tmp(id);
create hg index hg2 on #tmp(tstamp);
create dttm index dttm1 on #tmp(tstamp);
create lf index lf1 on #tmp(tuner_number);



-- Remove NULL and collapse onto a single row per id/timestamp/tuner
drop table et_technical_tuner;
select -- top 100
                id
        ,       tstamp
        ,       tuner_number
    ,   max((TSFrequency_ + 10410000) / 1.0e6)   as  TPFrequency_GHz
    ,   max(TSSymbolRate_)                       as      TSSSymbolRate
    ,   max(SNR_)                                as      SNR
    ,   max(RxPowerLevel_)                       as      RxPowerLevel
    ,   max(Locked_)                             as      Locked
    ,   max(Status_)                             as      Status
    ,   max(TSModulation_)                       as  TSModulation
    ,   max(TSMode_)                             as  TSMode
    ,   max(TSFEC_)                              as  TSFEC
    ,   max((LTRFrequency_ + 10410000) / 1.0e6)  as  LTRFrequency_GHz
    ,   max(Margin_)                             as      Margin
    ,   max(MarginDB_)                           as      MarginDB
    ,   max(account_number_)                     as      account_number
    ,   max(postcode_)                           as      postcode
    ,   max(fin_postcode_outcode_paf_)           as      fin_postcode_outcode_paf
    ,   max(government_region_)                  as      government_region
    ,   max(region_)                             as      region
    ,   max(cloud_cover_)                        as      cloud_cover
    ,   max(relative_humidity_)                  as      relative_humidity
    ,   max(sunshine_duration_)                  as      sunshine_duration
    ,   max(temperature_)                        as      temperature
    ,   max(weather_type_)                       as      weather_type
    ,   max(weather_type_desc_)                  as      weather_type_desc
    ,   max(wind_direction_)                     as      wind_direction
    ,   max(wind_speed_)                         as      wind_speed
into    et_technical_tuner
from    #tmp
group by
                id
        ,       tstamp
        ,       tuner_number
having
        TPFrequency_GHz =   11.778 -- EPG frequency
    and (
                Locked         =   'Locked'
            or  (
                        Locked             =   'Not Locked'
                    and LTRFrequency_GHz    =   TPFrequency_GHz
                )
        )
--    and TSModulation_   in  ('QPSK','8PSK')
order by
                id
        ,       tstamp
        ,       tuner_number
;


create hg index hg1 on et_technical_tuner(id);
-- create hg index hg2 on et_technical_tuner(tstamp);
create dttm index dttm1 on et_technical_tuner(tstamp);
create lf index lf1 on et_technical_tuner(tuner_number);


update et_technical_tuner
set account_number=post.account_number
,postcode=cust_postcode_key
,fin_postcode_outcode_paf=post.fin_postcode_outcode_paf
,government_region=post.government_region
,region=post.region
from
et_technical_tuner et
inner join
ripolile.diagnosticsPostcodeCheckTable post
on
et.id=post.nds_nr
;

drop table tmp_weather_data_history

select *
into
tmp_weather_data_history
from
weather_data_history
where
(date_time>='2015-06-28' and  date_time<='2015-07-06')
and
district in (
'TW7'
,'KT16'
,'MK4'
,'SW20'
,'CM14'
,'W4'
,'TW18'
,'TW2'
,'SW15'
,'LS20'
,'KT12'
,'SL4'
,'EH47'
,'TN8'
,'GU8'
,'EN1'
,'TW8'
,'RM6'
,'S60'
,'KT15'
,'SL6'
,'EH4'
,'EH54'
,'UB2')
;

update et_technical_tuner
set
cloud_cover=cast(we.cloud_cover as varchar(100))
,relative_humidity=cast(we.relative_humidity as varchar(100))
,sunshine_duration=cast(we.sunshine_duration as varchar(100))
    ,temperature=cast(we.temperature as varchar(100))
    ,weather_type=cast(we.weather_type as varchar(100))
    ,weather_type_desc=cast(we.weather_type_desc as varchar(100))
    ,wind_direction=cast(we.wind_direction as varchar(100))
    ,wind_speed=cast(we.wind_speed as varchar(100))
from
et_technical_tuner et
inner join
tmp_weather_data_history we
on
we.date_time=cast( cast(tstamp as varchar(14))||'00' as datetime)
and
et.fin_postcode_outcode_paf=we.district
-- where
;

/*

select top 10 we.cloud_cover
,we.relative_humidity
,we.sunshine_duration
,we.temperature
,we.weather_type
,we.weather_type_desc
,we.wind_direction
,we.wind_speed
,cast( cast(tstamp as varchar(14))||'00' as datetime)
from
et_technical_tuner et
inner join
weather_data_history we
on
et.fin_postcode_outcode_paf=we.district
where
we.date_time=cast( cast(tstamp as varchar(14))||'00' as datetime)
;
*/


