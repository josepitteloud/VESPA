----------------------
-- ZERO RATED SPOTS --
----------------------

------------------------------------------------------------------------------------
-- We need to add an aggregated identifier to the data - we need to pull spot data again.
------------------------- -----------------------------------------------------------



if object_id('molson_spots') is not null drop table molson_spots

select * into molson_spots
from neighbom.BARB_MASTER_SPOT_DATA            -- Martin Neighbors' table
where clearcast_commercial_no in ('MUMSTCI009040','MUMSTCI014030')
and BARB_DATE_OF_TRANSMISSION between '2012-08-01' and '2012-09-21'

--- Add on Channel Name Details

alter table molson_spots add full_name varchar(255);
alter table molson_spots add vespa_name varchar(255);
alter table molson_spots add channel_name varchar(255);
alter table molson_spots add techedge_name varchar(255);
alter table molson_spots add infosys_name varchar(255);


update molson_spots
set a.full_name=b.full_name
,a.vespa_name=b.vespa_name
,a.channel_name=b.channel_name
,a.techedge_name=b.techedge_name
,a.infosys_name=b.infosys_name
from molson_spots as a
left outer join VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES as b
on a.service_key=b.service_key
where a.local_date_of_transmission between b.effective_from and b.effective_to
;

-- TIDY --
alter table molson_spots add spot_channel_name varchar(255);
update molson_spots
set spot_channel_name = trim(full_name)
from molson_spots;

create  hg index idx1 on molson_spots(service_key);
create  hg index idx2 on molson_spots(utc_spot_start_date_time);
create  hg index idx3 on molson_spots(utc_spot_end_date_time);

-- some checks

select count(*) from molson_spots

---- ENSURE THIS DATA MATCHES WHAT WE HAVE IN TECHEDGE --

-- SPOT VIEWED FLAG --

if object_id('molson_spots2') is not null drop table molson_spots2

select *
       ,flag = 1
       ,sum(flag) over (order by utc_spot_start_date_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as identifier
into molson_spots2
from molson_spots;


--select count(*) from molson_spots2 where identifier is null

-- SECOND IDENTIFIER --

drop table molson_spots3

-- this does not add up to the right number  --
select  log_station_code
        ,sti_code
        ,utc_spot_start_date_time
        ,utc_spot_end_date_time
        ,clearcast_commercial_no
        ,max(flag) as flag -- should be 1
        ,sum(flag) over (order by utc_spot_start_date_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as identifier_agg
into molson_spots3
from molson_spots2
group by log_station_code
        ,sti_code
        ,utc_spot_start_date_time
        ,utc_spot_end_date_time
        ,clearcast_commercial_no;



alter table molson_coors_viewing_data
        add identifier_agg integer;


Update molson_coors_viewing_data
        set vw.identifier_agg = spot.identifier_agg
from molson_coors_viewing_data  vw
join molson_spots3       spot
on   vw.utc_spot_start_date_time    = spot.utc_spot_start_date_time
and  vw.utc_spot_end_date_time = spot.utc_spot_end_date_time
--and  vw.service_key = spot.service_key
--and  vw.full_name =  spot.full_name
and  vw.clearcast_commercial_no =  spot.clearcast_commercial_no;
-- log station code
-- and st_code


alter table molson_spots2
        add identifier_agg integer;



Update molson_spots2
        set vw.identifier_agg = spot.identifier_agg
from molson_spots2  vw
join molson_spots3       spot
on   vw.utc_spot_start_date_time    = spot.utc_spot_start_date_time
and  vw.utc_spot_end_date_time = spot.utc_spot_end_date_time
--and  vw.service_key = spot.service_key
--and  vw.full_name =  spot.full_name
and  vw.clearcast_commercial_no =  spot.clearcast_commercial_no;
-- log station code
-- and st_code



-- check
select count(*) from molson_spots2 where identifier_agg is null;


-- lets get a list of spots which are zero rated from the vespa perspective

alter table molson_spots2
        add spot_viewed integer default 0


Update molson_spots2 vw
        set vw.spot_viewed = 1
from molson_spots2  vw
inner join molson_coors_viewing_data       spot
on   vw.identifier_agg = spot.identifier_agg;


select * from molson_spots2 where spot_viewed <>1;


SELECT identifier
        ,identifier_agg
        , utc_spot_start_date_time
        , utc_spot_end_date_time
        , spot_duration
        , clearcast_commercial_no
        , preceding_programme_name
        , techedge_name as channel_name
        , BARB_DATE_OF_TRANSMISSION
        , log_station_code
        , sti_code
from molson_spots2
where spot_viewed <>1;

