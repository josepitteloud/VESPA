
/*
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- Project Name: Project Ethan
-- Authors: Leonardo Ripoli (leonardo.ripoli@bskyb.com)
-- Insight Collation: V246
-- Date: 16 December 2014


-- Business Brief:
--      To examine viewing through the Sky Go platform

-- Code Summary:
--      take data from comscore view and put it in a more usable format (elimination of multiple rows, unification of events that span midnight etc.

Purpose:

create a table to store viewing events so that we get rid of the midnight-split in the comscore view
plus find the scaling weight for those accounts that are in the overlap VESPA-Comscore population

firstly: we take from the comscore view only one instance per event (some events are split into linear instances)
secondly: we join those records for which a join has been possible (event continuing matches event continues)
thirdly: we try a nice solution for those records that have an abnormal viewing duration

pre-requisites:
create an empty table comscore_view_tmp_Leo_oneInstanceOnly, it will store temporary data



-- table creation is needed beforehand, please follow the instructions below:

-- drop table comscore_view_tmp_Leo_oneInstanceOnly just drop it if you already created it

;
select top 1 account_number
,  cb_key_household
, sam_profileid
, ns_ap_device
,   platform_name
,  platform_version
,stream_context,station_name
,channel_id, service_key
, vod_asset_id
, ad_asset_id
, dk_programme_instance_dim
, viewing_event_start_utc
, viewing_event_end_utc
,viewing_event_start_utc_raw
, viewing_event_end_utc_raw
, viewing_event_start_local
, viewing_event_end_local
, daylight_savings_start_flag
,daylight_savings_end_flag
,server_event_start_utc
,server_event_end_utc
,server_event_start_utc_raw
, server_event_end_utc_raw
, server_start_local_time
, server_end_local_time
, connection_type_start
,  connection_type_end
, genre_description
, sub_genre_description
, ad_flag
, aggr_event_id
, event_count
, erroneous_data_suspected_flag
, view_continuing_flag
, view_continues_next_day_flag
, linear_instance_flag
, content_duration
, programme_name
, data_date_local
, cast(NULL as BIGINT) as matching_aggr_event_id_post_midnight
,cast(NULL as bigint) as event_duration_client
,cast(NULL as bigint) as event_duration_server
,cast(NULL as bigint) as raw_viewing_duration_client
,cast(NULL as int) as wrong_data_flag
,cast(NULL as varchar(100)) as scaling_universe_key
,cast(NULL as numeric(13,6)) as adsmart_scaling_weight
,cast(NULL as bigint) as scaling_segment_key
,cast(NULL as varchar(100)) as scaling_attribute_01
,cast(NULL as varchar(100)) as scaling_attribute_02
,cast(NULL as varchar(100)) as scaling_attribute_03
,cast(NULL as varchar(100)) as scaling_attribute_04
,cast(NULL as varchar(100)) as scaling_attribute_05
,cast(NULL as varchar(100)) as scaling_attribute_06
,cast(NULL as varchar(100)) as scaling_attribute_07
-- also insert 
--, load_date
into comscore_view_tmp_Leo_oneInstanceOnly
from  vespa_shared.VESPA_Comscore_SkyGo_Union_View
;
truncate table comscore_view_tmp_Leo_oneInstanceOnly
*/

/*
create a temporary table with the ranking on program instances
name of table is comscore_view_tmp_Leo
*/

create or replace variable  @query_template varchar(10000)
;

create or replace variable  @query varchar(10000)
;
create or replace variable @cntr_days integer
;
create or replace variable @nr_of_total_days integer
;

set @nr_of_total_days=(select datediff(day,'2014-11-03','2014-12-10'))--(select count(distinct data_date_local) from vespa_shared.VESPA_Comscore_SkyGo_Union_View)
;


drop table tmp_sum_tab
;
drop table tmp_aggrid_tab
;
/*
Create auxiliary temporary tables to be used later
*/
create table tmp_aggrid_tab
(
 aggr_event_id bigint

);

create table tmp_sum_tab
(
 aggr_event_id bigint
 ,raw_event_duration bigint
);


IF OBJECT_ID('comscore_view_tmp_Leo') IS NOT NULL
drop table comscore_view_tmp_Leo
;


SET @query_template='select account_number'
SET @query_template=@query_template || ',  cb_key_household'
SET @query_template=@query_template || ', sam_profileid'
SET @query_template=@query_template || ', ns_ap_device'
SET @query_template=@query_template || ',   platform_name'
SET @query_template=@query_template || ',  platform_version'
SET @query_template=@query_template || ',stream_context,station_name'
SET @query_template=@query_template || ',channel_id, service_key'
SET @query_template=@query_template || ', vod_asset_id'
SET @query_template=@query_template || ', ad_asset_id'
SET @query_template=@query_template || ', dk_programme_instance_dim'
SET @query_template=@query_template || ', viewing_event_start_utc'
SET @query_template=@query_template || ', viewing_event_end_utc'
SET @query_template=@query_template || ',viewing_event_start_utc_raw'
SET @query_template=@query_template || ', viewing_event_end_utc_raw'
SET @query_template=@query_template || ', viewing_event_start_local'
SET @query_template=@query_template || ', viewing_event_end_local'
SET @query_template=@query_template || ', daylight_savings_start_flag'
SET @query_template=@query_template || ',daylight_savings_end_flag'
SET @query_template=@query_template || ',server_event_start_utc'
SET @query_template=@query_template || ',server_event_end_utc'
SET @query_template=@query_template || ',server_event_start_utc_raw'
SET @query_template=@query_template || ', server_event_end_utc_raw'
SET @query_template=@query_template || ', server_start_local_time'
SET @query_template=@query_template || ', server_end_local_time'
SET @query_template=@query_template || ', connection_type_start'
SET @query_template=@query_template || ',  connection_type_end'
SET @query_template=@query_template || ', genre_description'
SET @query_template=@query_template || ', sub_genre_description'
SET @query_template=@query_template || ', ad_flag'
SET @query_template=@query_template || ', aggr_event_id'
SET @query_template=@query_template || ', event_count'
SET @query_template=@query_template || ', erroneous_data_suspected_flag'
SET @query_template=@query_template || ', view_continuing_flag'
SET @query_template=@query_template || ', view_continues_next_day_flag'
SET @query_template=@query_template || ', linear_instance_flag'
SET @query_template=@query_template || ', content_duration'
SET @query_template=@query_template || ', programme_name'
SET @query_template=@query_template || ', data_date_local'
--SET @query_template=@query_template || ', load_date'
SET @query_template=@query_template || ', row_number() over (partition by aggr_event_id order by server_event_start_utc) therank '
SET @query_template=@query_template || ' into comscore_view_tmp_Leo'
SET @query_template=@query_template || ' from  vespa_shared.VESPA_Comscore_SkyGo_Union_View'
;

set @cntr_days=0
;

WHILE @cntr_days < @nr_of_total_days
 BEGIN

SET @query=@query_template || ' where data_date_local=dateadd(dd,' || cast( @cntr_days as varchar(6) ) || ',''2014-10-29'')'
commit

EXECUTE (@query)
commit

insert into comscore_view_tmp_Leo_oneInstanceOnly
(
account_number
,  cb_key_household
, sam_profileid
, ns_ap_device
,   platform_name
,  platform_version
,stream_context,station_name
,channel_id, service_key
, vod_asset_id
, ad_asset_id
, dk_programme_instance_dim
, viewing_event_start_utc
, viewing_event_end_utc
,viewing_event_start_utc_raw
, viewing_event_end_utc_raw
, viewing_event_start_local
, viewing_event_end_local
, daylight_savings_start_flag
,daylight_savings_end_flag
,server_event_start_utc
,server_event_end_utc
,server_event_start_utc_raw
, server_event_end_utc_raw
, server_start_local_time
, server_end_local_time
, connection_type_start
,  connection_type_end
, genre_description
, sub_genre_description
, ad_flag
, aggr_event_id
, event_count
, erroneous_data_suspected_flag
, view_continuing_flag
, view_continues_next_day_flag
, linear_instance_flag
, content_duration
, programme_name
, data_date_local
--,event_duration_client -- currently taken out
--,event_duration_server -- currently taken out
)
select account_number
,  cb_key_household
, sam_profileid
, ns_ap_device
,   platform_name
,  platform_version
,stream_context,station_name
,channel_id, service_key
, vod_asset_id
, ad_asset_id
, dk_programme_instance_dim
, viewing_event_start_utc
, viewing_event_end_utc
,viewing_event_start_utc_raw
, viewing_event_end_utc_raw
, viewing_event_start_local
, viewing_event_end_local
, daylight_savings_start_flag
,daylight_savings_end_flag
,server_event_start_utc
,server_event_end_utc
,server_event_start_utc_raw
, server_event_end_utc_raw
, server_start_local_time
, server_end_local_time
, connection_type_start
,  connection_type_end
, genre_description
, sub_genre_description
, ad_flag
, aggr_event_id
, event_count
, erroneous_data_suspected_flag
, view_continuing_flag
, view_continues_next_day_flag
, linear_instance_flag
, content_duration
, programme_name
, data_date_local
--,datediff(ss,viewing_event_start_local,viewing_event_end_local)  -- currently taken out
--,datediff(ss,server_start_local_time,server_end_local_time)  -- currently taken out
from  comscore_view_tmp_Leo
where therank=1
commit

drop table comscore_view_tmp_Leo
commit
set @cntr_days = @cntr_days +1
commit

end


-- create temp table to hold the joined data
-- probably better to do this in batches of 2 days (it must be at least 2 because of the midnight cross between days)

update comscore_view_tmp_Leo_oneInstanceOnly one
set
one.viewing_event_end_utc=two.viewing_event_end_utc
, one.viewing_event_end_utc_raw=two.viewing_event_end_utc_raw
, one.viewing_event_end_local=two.viewing_event_end_local
, one.server_event_end_utc=two.server_event_end_utc
, one.server_event_end_utc_raw=two.server_event_end_utc_raw
, one.server_end_local_time=two.server_end_local_time
--, one.view_continuing_flag=2 -- signal it has been processed (there was a match between continues and continuing) -- probably useless, because we already have the matching_aggr_event_id_post_midnight that signals a join when not null
, one.matching_aggr_event_id_post_midnight=two.aggr_event_id
from
comscore_view_tmp_Leo_oneInstanceOnly one
inner join
comscore_view_tmp_Leo_oneInstanceOnly two
on
one.account_number=two.account_number
and
one.cb_key_household=two.cb_key_household
and
one.sam_profileid=two.sam_profileid
and
one.server_end_local_time=two.server_start_local_time --server_end_local_time
and
one.ns_ap_device=two.ns_ap_device
and
one.platform_name=two.platform_name
and
one.platform_version=two.platform_version
and
one.stream_context=two.stream_context
and
one.station_name=two.station_name
and
one.view_continues_next_day_flag=1
and
two.view_continues_next_day_flag=0
and
one.view_continuing_flag=0
and
two.view_continuing_flag=1


create hg index aggr_1 on tmp_aggrid_tab(aggr_event_id)
create hg index aggr_2 on tmp_sum_tab(aggr_event_id)
--create hg index aggr_Leo_tmp on comscore_view_tmp_Leo_oneInstanceOnly(aggr_event_id)

set @query_template='insert into tmp_aggrid_tab '
set @query_template=@query_template || 'select distinct aggr_event_id '
set @query_template=@query_template || 'from comscore_view_tmp_Leo_oneInstanceOnly '


set @cntr_days=0

WHILE @cntr_days < @nr_of_total_days
 BEGIN

SET @query=@query_template || ' where data_date_local=dateadd(dd,' || cast( @cntr_days as varchar(6) ) || ',''2014-10-29'') '
commit

commit

EXECUTE (@query)
commit


insert into tmp_sum_tab
select vw.aggr_event_id
,sum(duration_viewed) as raw_event_duration
from vespa_shared.VESPA_Comscore_SkyGo_Union_View vw
inner join
tmp_aggrid_tab tmp
on vw.aggr_event_id=tmp.aggr_event_id
group by vw.aggr_event_id

commit

update comscore_view_tmp_Leo_oneInstanceOnly
set raw_viewing_duration_client=raw_event_duration
from
comscore_view_tmp_Leo_oneInstanceOnly Leo
inner join
tmp_sum_tab tmp
on Leo.aggr_event_id=tmp.aggr_event_id

commit
-- cleanup
truncate table tmp_aggrid_tab
commit
truncate table tmp_sum_tab
commit

set @cntr_days = @cntr_days +1

end

truncate table tmp_aggrid_tab
;
truncate table tmp_sum_tab


;
insert into tmp_aggrid_tab
select distinct matching_aggr_event_id_post_midnight
from comscore_view_tmp_Leo_oneInstanceOnly
;
delete from tmp_aggrid_tab
where aggr_event_id is null
;

insert into tmp_sum_tab
select vw.aggr_event_id
,sum(duration_viewed) as raw_event_duration
from vespa_shared.VESPA_Comscore_SkyGo_Union_View vw
inner join
tmp_aggrid_tab tmp
on vw.aggr_event_id=tmp.aggr_event_id
group by vw.aggr_event_id
;

update comscore_view_tmp_Leo_oneInstanceOnly Leo
set Leo.raw_viewing_duration_client=Leo.raw_viewing_duration_client-cast(coalesce(tmp.raw_event_duration,0) as bigint)
from
comscore_view_tmp_Leo_oneInstanceOnly Leo
inner join
tmp_sum_tab tmp
on Leo.matching_aggr_event_id_post_midnight=tmp.aggr_event_id
;

IF OBJECT_ID('deletedRowsComscoreTable') IS NOT NULL
drop table deletedRowsComscoreTable
;

select *
into deletedRowsComscoreTable
from comscore_view_tmp_Leo_oneInstanceOnly
where aggr_event_id in (select aggr_event_id from tmp_aggrid_tab)

;
delete from comscore_view_tmp_Leo_oneInstanceOnly
where aggr_event_id in (select aggr_event_id from tmp_aggrid_tab)
;



-- here calculate overlap: take scaling weights from the scaling table

create or replace variable  @query varchar(10000)
;
create or replace variable @cntr_days integer
;
create or replace variable @nr_of_total_days integer
;
create or replace variable @start_date date
;
create or replace variable @end_date date
;
set @start_date=date('2014-11-03')
;
set @end_date=date('2014-11-10') -- this is the upper limit, data will be taken up to the day before
;
set @nr_of_total_days=(select datediff(day,@start_date,@end_date))
;
set @cntr_days=0
;

select count() as countStep1 from comscore_view_tmp_Leo_oneInstanceOnly
;

WHILE @cntr_days < @nr_of_total_days
 BEGIN

select 'step2 start', now(),@cntr_days as day_cntr, dateadd(dd,@cntr_days,@start_date) as day_of_interest
commit

SET @query=
'
update comscore_view_tmp_Leo_oneInstanceOnly ves
set
ves.scaling_universe_key=sca.scaling_universe_key
,ves.adsmart_scaling_weight=sca.adsmart_scaling_weight
,ves.scaling_segment_key=sca.scaling_segment_key
,ves.scaling_attribute_01=sca.scaling_attribute_01
,ves.scaling_attribute_02=sca.scaling_attribute_02
,ves.scaling_attribute_03=sca.scaling_attribute_03
,ves.scaling_attribute_04=sca.scaling_attribute_04
,ves.scaling_attribute_05=sca.scaling_attribute_05
,ves.scaling_attribute_06=sca.scaling_attribute_06
,ves.scaling_attribute_07=sca.scaling_attribute_07
from
comscore_view_tmp_Leo_oneInstanceOnly ves
inner join
sk_prod.viq_viewing_data_scaling sca
on
ves.data_date_local=###run_date###
and
ves.account_number=sca.account_number
and
ves.data_date_local=sca.adjusted_event_start_date_vespa
'

commit

execute (replace(@query,'###run_date###', 'dateadd(dd,' || cast( @cntr_days as varchar(6) ) || ','''  || cast( @start_date as varchar(20) ) || ''')'))

commit
select 'step2 end', now(),@cntr_days as day_cntr, dateadd(dd,@cntr_days,@start_date) as day_of_interest,@query,
replace(@query,'###run_date###', 'dateadd(dd,' || cast( @cntr_days as varchar(6) ) || ','''  || cast( @start_date as varchar(20) ) || ''')')

commit
set @cntr_days = @cntr_days +1

commit
end
;
select count() as countStep2 from comscore_view_tmp_Leo_oneInstanceOnly
;



/*
Update the table, set wrong data flag when appropriate


-- both flags set indicate a viewing that lasts more than one day -> suspect wrong
update comscore_view_tmp_Leo_oneInstanceOnly
set wrong_data_flag=1
where view_continuing_flag=1 and view_continues_next_day_flag=1
and datepart(hh,viewing_event_start_local) != 0

-- flag viewing continuing set but the continuing event does not start at midnight
update comscore_view_tmp_Leo_oneInstanceOnly
set wrong_data_flag=2
where view_continuing_flag=1
and datepart(hh,viewing_event_start_local) != 0


-- flag viewing continuing set but the continuing event does not start at midnight
update comscore_view_tmp_Leo_oneInstanceOnly
set wrong_data_flag=3
where view_continuing_flag=1
and data_date_local>'2014-10-29'

-- set better, firstly do a clear of the error
*/

/*
update comscore_view_tmp_Leo_oneInstanceOnly
set wrong_data_flag=0
where view_continues_next_day_flag=1
*/
/*
-- then, to re-set it, use where view_continues_next_day_flag=1 and matching_aggr_event_id_post_midnight is null
update comscore_view_tmp_Leo_oneInstanceOnly
set wrong_data_flag=4
where view_continues_next_day_flag=1
and matching_aggr_event_id_post_midnight is null
and datepart(hh,viewing_event_end_local) != 0
--and data_date_local<'2014-12-12'

update comscore_view_tmp_Leo_oneInstanceOnly
set wrong_data_flag=5
where view_continues_next_day_flag=1
and matching_aggr_event_id_post_midnight is null
and data_date_local<'2014-12-12'

--
*/


/*
drop table #distLeo
drop table #distView
safety checks:
select distinct aggr_event_id,view_continuing_flag
into
#distLeo
  from comscore_view_tmp_Leo_oneInstanceOnly
  where data_date_local='2014-11-06'
  ;
select distinct aggr_event_id,view_continuing_flag
into
#distView
 from vespa_shared.VESPA_Comscore_SkyGo_Union_View
  where data_date_local='2014-11-06'

*/
