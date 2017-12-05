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
--      take data from VESPA and put it in a more usable format (elimination of multiple rows, unification of events that span through multiple instances...)

Purpose:

create a table to store VESPA viewing events so that we get rid of the multiple instances and take one row per event

The output tabe is: VESPA_view_tmp_Leo_oneInstanceOnly

pre-requisites:
create an empty table VESPA_view_tmp_Leo_oneInstanceOnly, it will store temporary data

drop table VESPA_view_tmp_Leo_oneInstanceOnly

;
select top 1
date(event_start_date_time_utc) as data_date_local
,account_number
,subscriber_id
,event_start_date_time_utc
,event_end_date_time_utc
,live_recorded
,capping_end_date_time_utc
,duration
,case when capping_end_date_time_utc is null then null else cast(datediff(ss,event_start_date_time_utc,capping_end_date_time_utc) as bigint) end as duration_capped
,playback_type
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
,cast(NULL as int) as wrong_data_flag
into VESPA_view_tmp_Leo_oneInstanceOnly
from  vespa_dp_prog_viewed_201411
;
truncate table VESPA_view_tmp_Leo_oneInstanceOnly
;

create hg index acc_nr_index on VESPA_view_tmp_Leo_oneInstanceOnly(account_number)
;
create table _mavedi
(
sizescale numeric(13,6)
,lupesc varchar(100)
);


*/

/*
select count() from comscore_view_tmp_Leo
select count() from VESPA_view_tmp_Leo_oneInstanceOnly
select count() from comscore_view_tmp_Leo
where therank>1
select top 10 * from VESPA_view_tmp_Leo_oneInstanceOnly
*/
-- truncate table VESPA_view_tmp_Leo_oneInstanceOnly
-- drop table VESPA_view_tmp_Leo_oneInstanceOnly

create or replace variable  @query_template varchar(10000)
;

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
create or replace variable @input_data_table varchar(200)
;
set @input_data_table='vespa_dp_prog_viewed_201411'
;
set @start_date=date('2014-11-01')
;
set @end_date=date('2014-12-01') -- this is the upper limit, data will be taken up to the day before
;
set @nr_of_total_days=(select datediff(day,@start_date,@end_date))
;
set @cntr_days=0
;

WHILE @cntr_days < @nr_of_total_days
 BEGIN
-- =@query_template || ' where data_date_local=dateadd(dd,' || cast( @cntr_days as varchar(6) ) || ','  || cast( @start_date as varchar(20) ) || ')'

select 'step1 start', now(),@cntr_days as day_cntr, dateadd(dd,@cntr_days,@start_date) as day_of_interest
commit

SET @query=
'
insert into VESPA_view_tmp_Leo_oneInstanceOnly
(
data_date_local
,account_number
,subscriber_id
,event_start_date_time_utc
,event_end_date_time_utc
,live_recorded
,capping_end_date_time_utc
,duration
,duration_capped
,playback_type
)
select
date(event_start_date_time_utc) as data_date_local
,account_number
,subscriber_id
,event_start_date_time_utc
,event_end_date_time_utc
,live_recorded
,capping_end_date_time_utc
,duration
,case when capping_end_date_time_utc is null then null else cast(datediff(ss,event_start_date_time_utc,capping_end_date_time_utc) as bigint) end as duration_capped
,playback_type
from ###input_table###
group by account_number,subscriber_id,event_start_date_time_utc,event_end_date_time_utc,live_recorded,capping_end_date_time_utc,duration,duration_capped,playback_type
having data_date_local=###run_date###
'

/*
and account_number in
(
''210000891231''
,''210000541217''
,''210005061168''
,''200006872051''
,''200004623308''
,''220000545784''
,''220000601579''
,''210019960199''
,''210018743539''
,''210019759526''
)

*/

commit

execute (   replace(
     replace(@query,'###run_date###', 'dateadd(dd,' || cast( @cntr_days as varchar(6) ) || ','''  || cast( @start_date as varchar(20) ) || ''')')
     ,'###input_table###',@input_data_table)   )
commit


select 'step1 end', now(),@cntr_days as day_cntr, dateadd(dd,@cntr_days,@start_date) as day_of_interest,@query,
replace(
     replace(@query,'###run_date###', 'dateadd(dd,' || cast( @cntr_days as varchar(6) ) || ','''  || cast( @start_date as varchar(20) ) || ''')')
     ,'###input_table###',@input_data_table)

commit
set @cntr_days = @cntr_days +1
commit

end

;
select count() as countStep1 from VESPA_view_tmp_Leo_oneInstanceOnly
;

-- update the table with information on the weights

--
;
set @cntr_days=0
;

WHILE @cntr_days < @nr_of_total_days
 BEGIN

select 'step2 start', now(),@cntr_days as day_cntr, dateadd(dd,@cntr_days,@start_date) as day_of_interest
commit

SET @query=
'
update VESPA_view_tmp_Leo_oneInstanceOnly ves
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
VESPA_view_tmp_Leo_oneInstanceOnly ves
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
select 'step1 end', now(),@cntr_days as day_cntr, dateadd(dd,@cntr_days,@start_date) as day_of_interest,@query,
replace(@query,'###run_date###', 'dateadd(dd,' || cast( @cntr_days as varchar(6) ) || ','''  || cast( @start_date as varchar(20) ) || ''')')

commit
set @cntr_days = @cntr_days +1

commit
end

select count() as countStep2 from VESPA_view_tmp_Leo_oneInstanceOnly
;

/*
set @query_template='insert into VESPA_view_tmp_Leo_oneInstanceOnly_safeCopy select * from VESPA_view_tmp_Leo_oneInstanceOnly '

set @cntr_days=0

WHILE @cntr_days < @nr_of_total_days
 BEGIN

SET @query=@query_template || ' where data_date_local=dateadd(dd,' || cast( @cntr_days as varchar(6) ) || ',''2014-11-01'')'
commit

commit

EXECUTE (@query)
commit
set @cntr_days = @cntr_days +1

end
*/
/*
-- then, to re-set it, use where view_continues_next_day_flag=1 and matching_aggr_event_id_post_midnight is null
update VESPA_view_tmp_Leo_oneInstanceOnly
set wrong_data_flag=4
where view_continues_next_day_flag=1
and matching_aggr_event_id_post_midnight is null
and datepart(hh,viewing_event_end_local) != 0
--and data_date_local<'2014-12-12'

update VESPA_view_tmp_Leo_oneInstanceOnly
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
  from VESPA_view_tmp_Leo_oneInstanceOnly
  where data_date_local='2014-11-06'
  ;
select distinct aggr_event_id,view_continuing_flag
into
#distView
 from vespa_dp_prog_viewed_201411
  where data_date_local='2014-11-06'

*/

