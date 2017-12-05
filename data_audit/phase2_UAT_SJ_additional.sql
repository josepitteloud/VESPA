/*

barb_spot_data
barb_promo_data
landmark_data
bss_data
attribution_data
(these are main daily data files)

slotsqa_spot_reporting
slotsqa_service_key
slotsqa_service_key_attributes
slotsqa_service_key_landmark
(these are the lookup tables imported from the UAT spreadsheet)

*/

-- Correct duplication in the spots reporting table
-- Film 24 4319 has been replaced with Sony TV 4319
delete
from sj_slotsqa_spot_reporting
where db2_station = '4319'
and description = 'Film 24 (was Bonanza (was Action! 241 (wasSoundtrack)'

/****** Barb versus Landmark Comparison ******/

select top 100* from barb_spot_data
select top 100* from landmark_data


--Test 3.0

select slot_start_broadcast_date, count(*)
from landmark_data
group by slot_start_broadcast_date
order by slot_start_broadcast_date

select date_of_transmission, count(*)
from barb_spot_data
group by date_of_transmission
order by date_of_transmission

/*
slot_start_broadcast_date
2012-02-01
2012-02-02
2012-02-03
2012-02-04
2012-02-05
2012-05-24
2012-05-25
2012-05-26
2012-05-27
2012-05-28
2012-05-29
2012-05-30
2012-05-31
2012-06-01
2012-06-02
2012-06-03
2012-06-04
2012-06-05

date_of_transmission
2012-05-28
2012-05-29
2012-05-30
2012-05-31
2012-06-01
2012-06-02
2012-06-03
2012-06-04
*/

--Test 3.0

select   slot_start_broadcast_date
        ,count(distinct service_key)
from landmark_data as lan
        inner join sj_slotsqa_service_key_landmark as skl on lan.sare_no = cast(skl.sare_no as int)
--where slot_start_broadcast_date in ('20120528','20120529','20120530','20120531','20120601','20120602','20120603','20120604')
group by slot_start_broadcast_date
order by slot_start_broadcast_date


select date_of_transmission
        ,count(distinct service_key)
from barb_spot_data as bas
       left join sj_slotsqa_service_key as ser on right('00000' || bas.log_station_code_for_spot, 5) || bas.split_transmission_indicator = right('00000' || ser.log_station_code, 5) || ser.sti_code
where date_of_transmission in ('20120528','20120529','20120530','20120531','20120601','20120602','20120603','20120604')
group by date_of_transmission
order by date_of_transmission

-- Test 3.1a

--LANDMARK

select   slot_start_broadcast_date
        ,lan.SLOT_START_BROADCAST_TIME_HOURS || right('0' || lan.SLOT_START_TIME_MINUTES, 2) || right('0' || lan.SLOT_START_TIME_SECONDS, 2) as spot_start_time
        ,skl.service_key
        ,spot_source
        ,BARB_SALES_HOUSE_ID
        ,SLOT_DURATION
into #landmark -- drop table #landmark
from landmark_data as lan
        left join sj_slotsqa_service_key_landmark as skl on lan.sare_no = cast(skl.sare_no as int)
        left join sj_slotsqa_service_key_attributes as att on att.service_key = skl.service_key
--where slot_start_broadcast_date in ('20120528','20120529','20120530','20120531','20120601','20120602','20120603','20120604')
group by slot_start_broadcast_date, spot_start_time, skl.service_key, spot_source,BARB_SALES_HOUSE_ID,SLOT_DURATION
order by slot_start_broadcast_date, spot_start_time, skl.service_key, spot_source,BARB_SALES_HOUSE_ID,SLOT_DURATION

--1178021 Row(s) affected

-- BARB

select date_of_transmission
        ,spot_start_time
        ,ser.service_key
        ,spot_source
        ,sales_house_identifier
        ,SPOT_DURATION as SLOT_DURATION
into #barb -- drop table #barb
from barb_spot_data as bas
       left join sj_slotsqa_service_key as ser on right('00000' || bas.log_station_code_for_spot, 5) || bas.split_transmission_indicator = right('00000' || ser.log_station_code, 5) || ser.sti_code
       left join sj_slotsqa_service_key_attributes as att on att.service_key = ser.service_key
where date_of_transmission in ('20120528','20120529','20120530','20120531','20120601','20120602','20120603','20120604')
group by date_of_transmission, spot_start_time, ser.service_key, spot_source,sales_house_identifier,SLOT_DURATION
order by date_of_transmission, spot_start_time, ser.service_key, spot_source,sales_house_identifier,SLOT_DURATION

--1030552 Row(s) affected


 select distinct(service_key)
        ,cast (0 as int) as barb_28
        ,cast (0 as int) as barb_29
        ,cast (0 as int) as barb_30
        ,cast (0 as int) as barb_31
        ,cast (0 as int) as barb_01
        ,cast (0 as int) as barb_02
        ,cast (0 as int) as barb_03
        ,cast (0 as int) as barb_04
        ,cast (0 as int) as landmark_28
        ,cast (0 as int) as landmark_29
        ,cast (0 as int) as landmark_30
        ,cast (0 as int) as landmark_31
        ,cast (0 as int) as landmark_01
        ,cast (0 as int) as landmark_02
        ,cast (0 as int) as landmark_03
        ,cast (0 as int) as landmark_04
        ,spot_source
    into #keys -- drop table #keys
    from sj_slotsqa_service_key_attributes
;

select service_key
       ,count(case when date_of_transmission = '20120528' then spot_start_time else null end) as barb_28
       ,count(case when date_of_transmission = '20120529' then spot_start_time else null end) as barb_29
       ,count(case when date_of_transmission = '20120530' then spot_start_time else null end) as barb_30
       ,count(case when date_of_transmission = '20120531' then spot_start_time else null end) as barb_31
       ,count(case when date_of_transmission = '20120601' then spot_start_time else null end) as barb_01
       ,count(case when date_of_transmission = '20120602' then spot_start_time else null end) as barb_02
       ,count(case when date_of_transmission = '20120603' then spot_start_time else null end) as barb_03
       ,count(case when date_of_transmission = '20120604' then spot_start_time else null end) as barb_04
into #barb_keys -- drop table  #barb_keys
from #barb
group by service_key;

select service_key
       ,count(case when slot_start_broadcast_date = '20120528' then spot_start_time else null end) as landmark_28
       ,count(case when slot_start_broadcast_date = '20120529' then spot_start_time else null end) as landmark_29
       ,count(case when slot_start_broadcast_date = '20120530' then spot_start_time else null end) as landmark_30
       ,count(case when slot_start_broadcast_date = '20120531' then spot_start_time else null end) as landmark_31
       ,count(case when slot_start_broadcast_date = '20120601' then spot_start_time else null end) as landmark_01
       ,count(case when slot_start_broadcast_date = '20120602' then spot_start_time else null end) as landmark_02
       ,count(case when slot_start_broadcast_date = '20120603' then spot_start_time else null end) as landmark_03
       ,count(case when slot_start_broadcast_date = '20120604' then spot_start_time else null end) as landmark_04
into #landmark_keys -- drop table #landmark_keys
from #landmark
group by service_key
order by service_key;

Update #keys as base
set     base.barb_28 = ba.barb_28
        ,base.barb_29 = ba.barb_29
        ,base.barb_30 = ba.barb_30
        ,base.barb_31 = ba.barb_31
        ,base.barb_01 = ba.barb_01
        ,base.barb_02 = ba.barb_02
        ,base.barb_03 = ba.barb_03
        ,base.barb_04 = ba.barb_04
from #keys as base
        inner join #barb_keys as ba on base.service_key = ba.service_key;


Update #keys as base
set     base.landmark_28 = ba.landmark_28
        ,base.landmark_29 = ba.landmark_29
        ,base.landmark_30 = ba.landmark_30
        ,base.landmark_31 = ba.landmark_31
        ,base.landmark_01 = ba.landmark_01
        ,base.landmark_02 = ba.landmark_02
        ,base.landmark_03 = ba.landmark_03
        ,base.landmark_04 = ba.landmark_04
from #keys as base
        inner join #landmark_keys as ba on base.service_key = ba.service_key;

select * from #keys
order by
(case when spot_source = 'Landmark' then 1
              when spot_source = 'BARB' then 2
              else 3 end)

--Test 3.3


select bas.date_of_transmission
       ,bas.spot_start_time
       ,ser.service_key
       ,bas.spot_duration
       ,bas.clearcast_commercial_number
       ,bas.spot_type
       ,bas.Campaign_Approval_ID
       ,bas.Campaign_Approval_ID_Version_number
       ,cast(sales_house_identifier as int) as sales_house_identifier
       ,cast(0 as int) as landmark
       ,cast(0 as int) as landmark_slot_duration
       ,cast(0 as int) as landmark_CLEARCAST_COMMERCIAL_NO
       ,cast(0 as varchar(10)) as landmark_MEDIA_SPOT_TYPE
       ,cast(0 as int) as landmark_CAMPAIGN_APPROVAL_ID
       ,cast(0 as int) as landmark_CAMPAIGN_APPROVAL_VERSION
       ,cast(0 as int) as landmark_BARB_SALES_HOUSE_ID
into #barb_spots -- drop table #barb_spots
from barb_spot_data as bas
       left join sj_slotsqa_service_key as ser on right('00000' || bas.log_station_code_for_spot, 5) || bas.split_transmission_indicator = right('00000' || ser.log_station_code, 5) || ser.sti_code
       left join sj_slotsqa_service_key_attributes as att on att.service_key = ser.service_key
where spot_source = 'Landmark'
group by bas.date_of_transmission, bas.spot_start_time, ser.service_key,bas.spot_duration,bas.clearcast_commercial_number,bas.spot_type
, bas.Campaign_Approval_ID,bas.Campaign_Approval_ID_Version_number,sales_house_identifier;

select lan.slot_start_broadcast_date
       ,lan.SLOT_START_BROADCAST_TIME_HOURS || right('0' || lan.SLOT_START_TIME_MINUTES, 2) || right('0' || lan.SLOT_START_TIME_SECONDS, 2) as spot_start_time
       ,skl.service_key
       ,slot_duration
       ,cast(lan.CLEARCAST_COMMERCIAL_NO as int)
       ,MEDIA_SPOT_TYPE
       ,CAMPAIGN_APPROVAL_ID
       ,CAMPAIGN_APPROVAL_VERSION
       ,cast(BARB_SALES_HOUSE_ID as int) as BARB_SALES_HOUSE_ID
into #landmark_spots -- drop table #landmark_spots
from landmark_data as lan
        left join sj_slotsqa_service_key_landmark as skl on lan.sare_no = cast(skl.sare_no as int)
        left join sj_slotsqa_service_key_attributes as att on att.service_key = skl.service_key
where spot_source = 'Landmark'
group by lan.slot_start_broadcast_date, spot_start_time, skl.service_key
, slot_duration,CLEARCAST_COMMERCIAL_NO,MEDIA_SPOT_TYPE,CAMPAIGN_APPROVAL_ID,CAMPAIGN_APPROVAL_VERSION,BARB_SALES_HOUSE_ID;


Update #barb_spots
set landmark = 1
    ,bs.landmark_slot_duration = ls.Slot_duration
    --,bs.landmark_CLEARCAST_COMMERCIAL_NO = ls.CLEARCAST_COMMERCIAL_NO
    ,bs.landmark_MEDIA_SPOT_TYPE = ls.MEDIA_SPOT_TYPE
    ,bs.landmark_CAMPAIGN_APPROVAL_ID = ls.CAMPAIGN_APPROVAL_ID
    ,bs.landmark_CAMPAIGN_APPROVAL_VERSION = ls.CAMPAIGN_APPROVAL_VERSION
    ,bs.landmark_BARB_SALES_HOUSE_ID = BARB_SALES_HOUSE_ID
from #barb_spots as bs
        inner join #landmark_spots as ls on bs.date_of_transmission = ls.slot_start_broadcast_date
                                        and bs.spot_start_time = ls.spot_start_time
                                        and bs.service_key = ls.service_key

-- Identifier

drop table #keys
drop table  #barb_keys
drop table #landmark_keys

 select distinct(identifier)
        ,cast (0 as int) as barb_28
        ,cast (0 as int) as barb_29
        ,cast (0 as int) as barb_30
        ,cast (0 as int) as barb_31
        ,cast (0 as int) as barb_01
        ,cast (0 as int) as barb_02
        ,cast (0 as int) as barb_03
        ,cast (0 as int) as barb_04
        ,cast (0 as int) as landmark_28
        ,cast (0 as int) as landmark_29
        ,cast (0 as int) as landmark_30
        ,cast (0 as int) as landmark_31
        ,cast (0 as int) as landmark_01
        ,cast (0 as int) as landmark_02
        ,cast (0 as int) as landmark_03
        ,cast (0 as int) as landmark_04
    into #keys -- drop table #keys
    from (    select cast(BARB_SALES_HOUSE_ID as  int) as identifier from #landmark_spots
        union select cast(sales_house_identifier as int) as identifier from #barb_spots) as h
;

select cast(sales_house_identifier as int) as identifier
       ,count(case when date_of_transmission = '20120528' then spot_start_time else null end) as barb_28
       ,count(case when date_of_transmission = '20120529' then spot_start_time else null end) as barb_29
       ,count(case when date_of_transmission = '20120530' then spot_start_time else null end) as barb_30
       ,count(case when date_of_transmission = '20120531' then spot_start_time else null end) as barb_31
       ,count(case when date_of_transmission = '20120601' then spot_start_time else null end) as barb_01
       ,count(case when date_of_transmission = '20120602' then spot_start_time else null end) as barb_02
       ,count(case when date_of_transmission = '20120603' then spot_start_time else null end) as barb_03
       ,count(case when date_of_transmission = '20120604' then spot_start_time else null end) as barb_04
into #barb_keys -- drop table  #barb_keys
from #barb_spots
where landmark = 1
group by identifier;

select cast(landmark_BARB_SALES_HOUSE_ID as int) as identifier
       ,count(case when date_of_transmission = '20120528' then spot_start_time else null end) as landmark_28
       ,count(case when date_of_transmission = '20120529' then spot_start_time else null end) as landmark_29
       ,count(case when date_of_transmission = '20120530' then spot_start_time else null end) as landmark_30
       ,count(case when date_of_transmission = '20120531' then spot_start_time else null end) as landmark_31
       ,count(case when date_of_transmission = '20120601' then spot_start_time else null end) as landmark_01
       ,count(case when date_of_transmission = '20120602' then spot_start_time else null end) as landmark_02
       ,count(case when date_of_transmission = '20120603' then spot_start_time else null end) as landmark_03
       ,count(case when date_of_transmission = '20120604' then spot_start_time else null end) as landmark_04
into #landmark_keys -- drop table #landmark_keys
from #barb_spots
where landmark = 1
group by identifier
order by identifier;

Update #keys as base
set     base.barb_28 = ba.barb_28
        ,base.barb_29 = ba.barb_29
        ,base.barb_30 = ba.barb_30
        ,base.barb_31 = ba.barb_31
        ,base.barb_01 = ba.barb_01
        ,base.barb_02 = ba.barb_02
        ,base.barb_03 = ba.barb_03
        ,base.barb_04 = ba.barb_04
from #keys as base
        inner join #barb_keys as ba on base.identifier = ba.identifier;


Update #keys as base
set     base.landmark_28 = ba.landmark_28
        ,base.landmark_29 = ba.landmark_29
        ,base.landmark_30 = ba.landmark_30
        ,base.landmark_31 = ba.landmark_31
        ,base.landmark_01 = ba.landmark_01
        ,base.landmark_02 = ba.landmark_02
        ,base.landmark_03 = ba.landmark_03
        ,base.landmark_04 = ba.landmark_04
from #keys as base
        inner join #landmark_keys as ba on base.identifier = ba.identifier;

select * from #keys
order by identifier


--Test 3.4

drop table #keys
drop table  #barb_keys
drop table #landmark_keys

 select distinct(SLOT_DURATION) as  SLOT_DURATION
        ,cast (0 as int) as barb_28
        ,cast (0 as int) as barb_29
        ,cast (0 as int) as barb_30
        ,cast (0 as int) as barb_31
        ,cast (0 as int) as barb_01
        ,cast (0 as int) as barb_02
        ,cast (0 as int) as barb_03
        ,cast (0 as int) as barb_04
        ,cast (0 as int) as landmark_28
        ,cast (0 as int) as landmark_29
        ,cast (0 as int) as landmark_30
        ,cast (0 as int) as landmark_31
        ,cast (0 as int) as landmark_01
        ,cast (0 as int) as landmark_02
        ,cast (0 as int) as landmark_03
        ,cast (0 as int) as landmark_04
    into #keys -- drop table #keys
    from (    select cast(SLOT_DURATION as  int) as SLOT_DURATION from #landmark_spots
        union select cast(SPOT_DURATION as int) as SLOT_DURATION from #barb_spots) as h
;

select cast(SPOT_DURATION as int) as SLOT_DURATION
       ,count(case when date_of_transmission = '20120528' then spot_start_time else null end) as barb_28
       ,count(case when date_of_transmission = '20120529' then spot_start_time else null end) as barb_29
       ,count(case when date_of_transmission = '20120530' then spot_start_time else null end) as barb_30
       ,count(case when date_of_transmission = '20120531' then spot_start_time else null end) as barb_31
       ,count(case when date_of_transmission = '20120601' then spot_start_time else null end) as barb_01
       ,count(case when date_of_transmission = '20120602' then spot_start_time else null end) as barb_02
       ,count(case when date_of_transmission = '20120603' then spot_start_time else null end) as barb_03
       ,count(case when date_of_transmission = '20120604' then spot_start_time else null end) as barb_04
into #barb_keys -- drop table  #barb_keys
from #barb_spots
where  landmark=1
group by SLOT_DURATION;

select cast(landmark_SLOT_DURATION as int) as SLOT_DURATION
       ,count(case when date_of_transmission = '20120528' then spot_start_time else null end) as landmark_28
       ,count(case when date_of_transmission = '20120529' then spot_start_time else null end) as landmark_29
       ,count(case when date_of_transmission = '20120530' then spot_start_time else null end) as landmark_30
       ,count(case when date_of_transmission = '20120531' then spot_start_time else null end) as landmark_31
       ,count(case when date_of_transmission = '20120601' then spot_start_time else null end) as landmark_01
       ,count(case when date_of_transmission = '20120602' then spot_start_time else null end) as landmark_02
       ,count(case when date_of_transmission = '20120603' then spot_start_time else null end) as landmark_03
       ,count(case when date_of_transmission = '20120604' then spot_start_time else null end) as landmark_04
into #landmark_keys -- drop table #landmark_keys
from #barb_spots
where landmark=1
group by SLOT_DURATION
order by SLOT_DURATION;

Update #keys as base
set     base.barb_28 = ba.barb_28
        ,base.barb_29 = ba.barb_29
        ,base.barb_30 = ba.barb_30
        ,base.barb_31 = ba.barb_31
        ,base.barb_01 = ba.barb_01
        ,base.barb_02 = ba.barb_02
        ,base.barb_03 = ba.barb_03
        ,base.barb_04 = ba.barb_04
from #keys as base
        inner join #barb_keys as ba on base.SLOT_DURATION = ba.SLOT_DURATION;


Update #keys as base
set     base.landmark_28 = ba.landmark_28
        ,base.landmark_29 = ba.landmark_29
        ,base.landmark_30 = ba.landmark_30
        ,base.landmark_31 = ba.landmark_31
        ,base.landmark_01 = ba.landmark_01
        ,base.landmark_02 = ba.landmark_02
        ,base.landmark_03 = ba.landmark_03
        ,base.landmark_04 = ba.landmark_04
from #keys as base
        inner join #landmark_keys as ba on base.SLOT_DURATION = ba.SLOT_DURATION;

select spot_duration, landmark_slot_duration, count(*) from #barb_spots
where landmark = 1
group by spot_duration, landmark_slot_duration
order by spot_duration, landmark_slot_duration


-- test 3.3

select landmark_BARB_SALES_HOUSE_ID, sales_house_identifier, count(*)
from #barb_spots
where landmark = 1
group by landmark_BARB_SALES_HOUSE_ID, sales_house_identifier
order by landmark_BARB_SALES_HOUSE_ID, sales_house_identifier

--test 3.4

select landmark_slot_duration, spot_duration, count(*)
from #barb_spots
--where landmark = 1
group by landmark_slot_duration, spot_duration
order by landmark_slot_duration, spot_duration

--test 3.5

select landmark_media_spot_type, spot_type, count(*)
from #barb_spots
where landmark = 1
group by landmark_media_spot_type, spot_type
order by landmark_media_spot_type, spot_type

--test 3.5

select landmark_Campaign_Approval_ID, Campaign_Approval_ID, count(*)
from #barb_spots
where landmark = 1
group by landmark_Campaign_Approval_ID, Campaign_Approval_ID
order by landmark_Campaign_Approval_ID, Campaign_Approval_ID

--test 3.6

select landmark_CAMPAIGN_APPROVAL_VERSION, Campaign_Approval_ID_Version_number, count(*)
from #barb_spots
where landmark = 1
group by landmark_CAMPAIGN_APPROVAL_VERSION, Campaign_Approval_ID_Version_number
order by landmark_CAMPAIGN_APPROVAL_VERSION, Campaign_Approval_ID_Version_number

CAMPAIGN_APPROVAL_VERSION = Campaign_Approval_ID_Version Number


-- test 3.12

drop table #keys
drop table #barb
drop table #landmark

select distinct service_key
into #keys
from sj_slotsqa_service_key_attributes

select distinct service_key, cast(0 as bit) as attributes_table
into #barb
from sj_slotsqa_service_key

Update #barb
set attributes_table = 1
from #barb as ba
        inner join #keys as k on ba.service_key = k.service_key

select count(*) from #barb where attributes_table = 1
select * from #barb where attributes_table <> 1
-- 377
-- 370


select distinct service_key, cast(0 as bit) as attributes_table
into #landmark
from sj_slotsqa_service_key_landmark

Update #landmark
set attributes_table = 1
from #landmark as ba
        inner join #keys as k on ba.service_key = k.service_key

  select * from #landmark where attributes_table <> 1
-- 233
-- 232

select count(*) from #landmark where attributes_table = 1

--- test 5.1 (see 3.1)


-- Sales house

drop table #keys
drop table  #barb_keys
drop table #landmark_keys

select bas.date_of_transmission
       ,bas.spot_start_time
       ,ser.service_key
       ,bas.spot_duration
       ,bas.clearcast_commercial_number
       ,bas.spot_type
       ,bas.Campaign_Approval_ID
       ,bas.Campaign_Approval_ID_Version_number
       ,cast(sales_house_identifier as int) as sales_house_identifier
       ,cast(0 as int) as landmark
       ,cast(0 as int) as landmark_slot_duration
       ,cast(0 as int) as landmark_CLEARCAST_COMMERCIAL_NO
       ,cast(0 as varchar(10)) as landmark_MEDIA_SPOT_TYPE
       ,cast(0 as int) as landmark_CAMPAIGN_APPROVAL_ID
       ,cast(0 as int) as landmark_CAMPAIGN_APPROVAL_VERSION
       ,cast(0 as int) as landmark_BARB_SALES_HOUSE_ID
into #barb_spots -- drop table #barb_spots
from barb_spot_data as bas
       left join sj_slotsqa_service_key as ser on right('00000' || bas.log_station_code_for_spot, 5) || bas.split_transmission_indicator = right('00000' || ser.log_station_code, 5) || ser.sti_code
       left join sj_slotsqa_service_key_attributes as att on att.service_key = ser.service_key
where spot_source = 'BARB'
group by bas.date_of_transmission, bas.spot_start_time, ser.service_key,bas.spot_duration,bas.clearcast_commercial_number,bas.spot_type
, bas.Campaign_Approval_ID,bas.Campaign_Approval_ID_Version_number,sales_house_identifier;


 select distinct(identifier) as identifier
        ,cast (0 as int) as barb_28
        ,cast (0 as int) as barb_29
        ,cast (0 as int) as barb_30
        ,cast (0 as int) as barb_31
        ,cast (0 as int) as barb_01
        ,cast (0 as int) as barb_02
        ,cast (0 as int) as barb_03
        ,cast (0 as int) as barb_04
        ,cast (0 as int) as landmark_28
        ,cast (0 as int) as landmark_29
        ,cast (0 as int) as landmark_30
        ,cast (0 as int) as landmark_31
        ,cast (0 as int) as landmark_01
        ,cast (0 as int) as landmark_02
        ,cast (0 as int) as landmark_03
        ,cast (0 as int) as landmark_04
    into #keys -- drop table #keys
    from (select cast(sales_house_identifier as int) as identifier from #barb_spots) as h
;

select cast(sales_house_identifier as int) as identifier
       ,count(case when date_of_transmission = '20120528' then spot_start_time else null end) as barb_28
       ,count(case when date_of_transmission = '20120529' then spot_start_time else null end) as barb_29
       ,count(case when date_of_transmission = '20120530' then spot_start_time else null end) as barb_30
       ,count(case when date_of_transmission = '20120531' then spot_start_time else null end) as barb_31
       ,count(case when date_of_transmission = '20120601' then spot_start_time else null end) as barb_01
       ,count(case when date_of_transmission = '20120602' then spot_start_time else null end) as barb_02
       ,count(case when date_of_transmission = '20120603' then spot_start_time else null end) as barb_03
       ,count(case when date_of_transmission = '20120604' then spot_start_time else null end) as barb_04
into #barb_keys -- drop table  #barb_keys
from #barb_spots
group by identifier;

select * from #keys
order by identifier




