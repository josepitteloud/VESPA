
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- Project Name: Project Ethan
-- Authors: Leonardo Ripoli (leonardo.ripoli@bskyb.com)
-- Insight Collation: V246
-- Date: 16 December 2014


-- Business Brief:
--      To examine viewing through the Sky Go platform

-- Code Summary:
--      Compare representation of scaling segments between Sky panel and Comscore population
--      the current script takes the representation either of the panel from the scaling table sk_prod.viq_viewing_data_scaling or from the comscore table,
--      the variable @input_data_table.
--      
--      Every variable in the scaling segment is operated separately, a future update is to use dynamic sql and do in a loop
/*
Basic
Movies
Movies & Sport
Sport

*/

create or replace variable @var_name  varchar(100)
;
set @var_name='scaling_attribute_03' -- for future use in a dynamic query

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
--set @input_data_table='comscore_view_tmp_Leo_oneInstanceOnly' -- 'sk_prod.viq_viewing_data_scaling'
set @input_data_table='sk_prod.viq_viewing_data_scaling'
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


IF OBJECT_ID('tmp_table') IS NOT NULL
DROP table tmp_table

commit

SET @query=
'
select distinct
adjusted_event_start_date_vespa as data_date_local
--data_date_local
,account_number
,adsmart_scaling_weight
,scaling_attribute_03
into tmp_table
from
###input_table###
where
data_date_local=###run_date###
and
scaling_attribute_03=''Basic''
--scaling_attribute_03=''Movies''
--scaling_attribute_03=''Movies & Sport''
--scaling_attribute_03=''Sport''
--scaling_attribute_03=''Sport''
'
commit


commit

execute (   replace(
     replace(@query,'###run_date###', 'dateadd(dd,' || cast( @cntr_days as varchar(6) ) || ','''  || cast( @start_date as varchar(20) ) || ''')')
     ,'###input_table###',@input_data_table)   )
commit


if @cntr_days=0
begin

IF OBJECT_ID('varTable') IS NOT NULL
DROP table varTable

commit

select
data_date_local
,cast(count(account_number) as bigint) as nrOfAccounts
,cast( sum(adsmart_scaling_weight) as double) as nrOfScaledAccounts
into varTable
from
tmp_table
group by data_date_local

commit
end
else
begin
insert into varTable
select
data_date_local
,cast(count(account_number) as bigint) as nrOfAccounts
,cast( sum(adsmart_scaling_weight) as double) as nrOfScaledAccounts
from
tmp_table
group by data_date_local

commit
end

commit
set @cntr_days = @cntr_days +1
commit

end

select * from varTable
;

-- from here it is the repetition of the above, for the variable number 2

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
--set @input_data_table='comscore_view_tmp_Leo_oneInstanceOnly' -- 'sk_prod.viq_viewing_data_scaling'
set @input_data_table='sk_prod.viq_viewing_data_scaling'
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


IF OBJECT_ID('tmp_table') IS NOT NULL
DROP table tmp_table

commit

SET @query=
'
select distinct
adjusted_event_start_date_vespa as data_date_local
--data_date_local
,account_number
,adsmart_scaling_weight
,scaling_attribute_03
into tmp_table
from
###input_table###
where
data_date_local=###run_date###
and
--scaling_attribute_03=''Basic''
scaling_attribute_03=''Movies''
--scaling_attribute_03=''Movies & Sport''
--scaling_attribute_03=''Sport''
--scaling_attribute_03=''Sport''
'
commit


commit

execute (   replace(
     replace(@query,'###run_date###', 'dateadd(dd,' || cast( @cntr_days as varchar(6) ) || ','''  || cast( @start_date as varchar(20) ) || ''')')
     ,'###input_table###',@input_data_table)   )
commit


if @cntr_days=0
begin

IF OBJECT_ID('varTable') IS NOT NULL
DROP table varTable

commit

select
data_date_local
,cast(count(account_number) as bigint) as nrOfAccounts
,cast( sum(adsmart_scaling_weight) as double) as nrOfScaledAccounts
into varTable
from
tmp_table
group by data_date_local

commit
end
else
begin
insert into varTable
select
data_date_local
,cast(count(account_number) as bigint) as nrOfAccounts
,cast( sum(adsmart_scaling_weight) as double) as nrOfScaledAccounts
from
tmp_table
group by data_date_local

commit
end

commit
set @cntr_days = @cntr_days +1
commit

end

select * from varTable
;

-- from here it is the repetition of the above, for the variable number 3

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
--set @input_data_table='comscore_view_tmp_Leo_oneInstanceOnly' -- 'sk_prod.viq_viewing_data_scaling'
set @input_data_table='sk_prod.viq_viewing_data_scaling'
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


IF OBJECT_ID('tmp_table') IS NOT NULL
DROP table tmp_table

commit

SET @query=
'
select distinct
adjusted_event_start_date_vespa as data_date_local
--data_date_local
,account_number
,adsmart_scaling_weight
,scaling_attribute_03
into tmp_table
from
###input_table###
where
data_date_local=###run_date###
and
--scaling_attribute_03=''Basic''
--scaling_attribute_03=''Movies''
scaling_attribute_03=''Movies & Sport''
--scaling_attribute_03=''Sport''
--scaling_attribute_03=''Sport''
'
commit


commit

execute (   replace(
     replace(@query,'###run_date###', 'dateadd(dd,' || cast( @cntr_days as varchar(6) ) || ','''  || cast( @start_date as varchar(20) ) || ''')')
     ,'###input_table###',@input_data_table)   )
commit


if @cntr_days=0
begin

IF OBJECT_ID('varTable') IS NOT NULL
DROP table varTable

commit

select
data_date_local
,cast(count(account_number) as bigint) as nrOfAccounts
,cast( sum(adsmart_scaling_weight) as double) as nrOfScaledAccounts
into varTable
from
tmp_table
group by data_date_local

commit
end
else
begin
insert into varTable
select
data_date_local
,cast(count(account_number) as bigint) as nrOfAccounts
,cast( sum(adsmart_scaling_weight) as double) as nrOfScaledAccounts
from
tmp_table
group by data_date_local

commit
end

commit
set @cntr_days = @cntr_days +1
commit

end

select * from varTable
;

-- from here it is the repetition of the above, for the variable number 4

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
--set @input_data_table='comscore_view_tmp_Leo_oneInstanceOnly' -- 'sk_prod.viq_viewing_data_scaling'
set @input_data_table='sk_prod.viq_viewing_data_scaling'
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


IF OBJECT_ID('tmp_table') IS NOT NULL
DROP table tmp_table

commit

SET @query=
'
select distinct
adjusted_event_start_date_vespa as data_date_local
--data_date_local
,account_number
,adsmart_scaling_weight
,scaling_attribute_03
into tmp_table
from
###input_table###
where
data_date_local=###run_date###
and
--scaling_attribute_03=''Basic''
--scaling_attribute_03=''Movies''
--scaling_attribute_03=''Movies & Sport''
scaling_attribute_03=''Sport''
--scaling_attribute_03=''Sport''
'
commit


commit

execute (   replace(
     replace(@query,'###run_date###', 'dateadd(dd,' || cast( @cntr_days as varchar(6) ) || ','''  || cast( @start_date as varchar(20) ) || ''')')
     ,'###input_table###',@input_data_table)   )
commit


if @cntr_days=0
begin

IF OBJECT_ID('varTable') IS NOT NULL
DROP table varTable

commit

select
data_date_local
,cast(count(account_number) as bigint) as nrOfAccounts
,cast( sum(adsmart_scaling_weight) as double) as nrOfScaledAccounts
into varTable
from
tmp_table
group by data_date_local

commit
end
else
begin
insert into varTable
select
data_date_local
,cast(count(account_number) as bigint) as nrOfAccounts
,cast( sum(adsmart_scaling_weight) as double) as nrOfScaledAccounts
from
tmp_table
group by data_date_local

commit
end

commit
set @cntr_days = @cntr_days +1
commit

end

select * from varTable
;

-- from here it is the repetition of the above, for the variable number 5

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
--set @input_data_table='comscore_view_tmp_Leo_oneInstanceOnly' -- 'sk_prod.viq_viewing_data_scaling'
set @input_data_table='sk_prod.viq_viewing_data_scaling'
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


IF OBJECT_ID('tmp_table') IS NOT NULL
DROP table tmp_table

commit

SET @query=
'
select distinct
adjusted_event_start_date_vespa as data_date_local
--data_date_local
,account_number
,adsmart_scaling_weight
,scaling_attribute_03
into tmp_table
from
###input_table###
where
data_date_local=###run_date###
and
--scaling_attribute_03=''Basic''
--scaling_attribute_03=''Movies''
--scaling_attribute_03=''Movies & Sport''
--scaling_attribute_03=''Sport''
scaling_attribute_06=''Sport''
'
commit


commit

execute (   replace(
     replace(@query,'###run_date###', 'dateadd(dd,' || cast( @cntr_days as varchar(6) ) || ','''  || cast( @start_date as varchar(20) ) || ''')')
     ,'###input_table###',@input_data_table)   )
commit


if @cntr_days=0
begin

IF OBJECT_ID('varTable') IS NOT NULL
DROP table varTable

commit

select
data_date_local
,cast(count(account_number) as bigint) as nrOfAccounts
,cast( sum(adsmart_scaling_weight) as double) as nrOfScaledAccounts
into varTable
from
tmp_table
group by data_date_local

commit
end
else
begin
insert into varTable
select
data_date_local
,cast(count(account_number) as bigint) as nrOfAccounts
,cast( sum(adsmart_scaling_weight) as double) as nrOfScaledAccounts
from
tmp_table
group by data_date_local

commit
end

commit
set @cntr_days = @cntr_days +1
commit

end

select * from varTable
;
