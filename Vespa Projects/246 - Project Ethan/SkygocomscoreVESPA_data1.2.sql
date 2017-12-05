-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- Project Name: Project Ethan
-- Authors: Leonardo Ripoli (leonardo.ripoli@bskyb.com)
-- Insight Collation: V246
-- Date: 16 December 2014


-- Business Brief:
--      To examine viewing through the Sky Go platform

-- Code Summary:
--      Extract data from VESPA and put them in a format suitable for analysis


-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- A4: calculate average/median viewing per ntiles

-- Create table containing viewing duration grouped by sam_profileid,platform_name,stream_context


-- organize per account


-- total viewing duration across days per account (subcategories stream and platform)
-- calculate here avg viewing per day, obtained by total viewing per sam profile id divided by days of viewing

-- uncapped distribution

IF OBJECT_ID('table_accountVESPA_total') IS NOT NULL
DROP table table_accountVESPA_total
;


select
account_number
,cast(sum(duration) as bigint) as viewingDurationAcrossDays -- viewing duration
,cast(sum(coalesce(duration_capped,duration)) as bigint) as cappedViewingDurationAcrossDays -- viewing duration
,cast(sum(duration*coalesce(adsmart_scaling_weight,0)) as double) as scaledViewingDurationAcrossDays -- viewing duration
,cast(sum(coalesce(duration_capped,duration)*coalesce(adsmart_scaling_weight,0)) as double) as scaledCappedViewingDurationAcrossDays -- viewing duration
,count(distinct data_date_local) as nr_of_days_of_viewing
,(case when nr_of_days_of_viewing != 0 then cast(viewingDurationAcrossDays as double)/cast(nr_of_days_of_viewing as double) else 0 end) as avgDurationPerDay
,(case when nr_of_days_of_viewing != 0 then cast(cappedViewingDurationAcrossDays as double)/cast(nr_of_days_of_viewing as double) else 0 end) as cappedAvgDurationPerDay
,(case when nr_of_days_of_viewing != 0 then cast(scaledViewingDurationAcrossDays as double)/cast(nr_of_days_of_viewing as double) else 0 end) as scaledAvgDurationPerDay
,(case when nr_of_days_of_viewing != 0 then cast(scaledCappedViewingDurationAcrossDays as double)/cast(nr_of_days_of_viewing as double) else 0 end) as scaledCappedAvgDurationPerDay
into table_accountVESPA_total
  from VESPA_view_tmp_Leo_oneInstanceOnly
group by account_number

/*
having account_number in
(
'210000891231'
,'210000541217'
,'210005061168'
,'200006872051'
,'200004623308'
,'220000545784'
,'220000601579'
,'210019960199'
,'210018743539'
,'210019759526'
)
*/
;

delete from table_accountVESPA_total
where account_number is null
;
-- calculate the ntiles and put results in a table
IF OBJECT_ID('tableNtiles_accountVESPA_total') IS NOT NULL
DROP table tableNtiles_accountVESPA_total
;

select
account_number
,avgDurationPerDay
,ntile(100) over (order by avgDurationPerDay) as ntile
into tableNtiles_accountVESPA_total
from table_accountVESPA_total
--group by platform_name, stream_context, ntile

;
select
ntile
,avg(avgDurationPerDay) as avgViewing
,median(avgDurationPerDay) as medianViewing
,count(avgDurationPerDay) as nrOfAccounts
from tableNtiles_accountVESPA_total
group by
ntile


-- capped
select count(distinct account_number) from table_accountVESPA_total
;
select count(distinct account_number) from VESPA_view_tmp_Leo_oneInstanceOnly
;
select count(distinct account_number) from vespa_dp_prog_viewed_201411
;
-- calculate the ntiles and put results in a table
IF OBJECT_ID('tableNtiles_accountVESPA_total_capped') IS NOT NULL
DROP table tableNtiles_accountVESPA_total_capped
;

select
account_number
,cappedAvgDurationPerDay
,ntile(100) over (order by cappedAvgDurationPerDay) as ntile
--,count(sam_profileid) over (partition by platform_name, stream_context, ntile) as nrPieces
into tableNtiles_accountVESPA_total_capped
from table_accountVESPA_total
--group by platform_name, stream_context, ntile

;
select
ntile
,avg(cappedAvgDurationPerDay) as cappedAvgViewing
,median(cappedAvgDurationPerDay) as cappedMedianViewing
,count(cappedAvgDurationPerDay) as nrOfAccounts
from tableNtiles_accountVESPA_total_capped
group by
ntile


-- scaled

-- calculate the ntiles and put results in a table
IF OBJECT_ID('tableNtiles_accountVESPA_total_scaled') IS NOT NULL
DROP table tableNtiles_accountVESPA_total_scaled
;

select
account_number
,scaledAvgDurationPerDay
,ntile(100) over (order by scaledAvgDurationPerDay) as ntile
--,count(sam_profileid) over (partition by platform_name, stream_context, ntile) as nrPieces
into tableNtiles_accountVESPA_total_scaled
from table_accountVESPA_total
--group by platform_name, stream_context, ntile

;
select
ntile
,avg(scaledAvgDurationPerDay) as scaledAvgViewing
,median(scaledAvgDurationPerDay) as scaledMedianViewing
,count(scaledAvgDurationPerDay) as nrOfAccounts
from tableNtiles_accountVESPA_total_scaled
group by
ntile



-- scaled and capped

-- calculate the ntiles and put results in a table
IF OBJECT_ID('tableNtiles_accountVESPA_total_scaled_capped') IS NOT NULL
DROP table tableNtiles_accountVESPA_total_scaled_capped
;

select
account_number
,scaledCappedAvgDurationPerDay
,ntile(100) over (order by scaledCappedAvgDurationPerDay) as ntile
--,count(sam_profileid) over (partition by platform_name, stream_context, ntile) as nrPieces
into tableNtiles_accountVESPA_total_scaled_capped
from table_accountVESPA_total
--group by platform_name, stream_context, ntile

;
select
ntile
,avg(scaledCappedAvgDurationPerDay) as scaledCappedAvgViewing
,median(scaledCappedAvgDurationPerDay) as scaledCappedMedianViewing
,count(scaledCappedAvgDurationPerDay) as nrOfAccounts
from tableNtiles_accountVESPA_total_scaled_capped
group by
ntile


-- calcuate sum of daily weights
-- insert in a table the daily sum of weights
IF OBJECT_ID('VESPA_dailyFacts') IS NOT NULL
DROP table VESPA_dailyFacts

select
data_date_local
,cast(NULL as double) as sumOfDailyWeights--cast(sum(adsmart_scaling_weight) as double) as sumOfWeights
,cast(NULL as double) as sumOfDailyWeightsReported--cast(sum(adsmart_scaling_weight) as double) as sumOfWeights
,cast(sum(duration) as bigint) as sumOfDuration
,cast(sum(duration*coalesce(adsmart_scaling_weight,0)) as double) as sumOfDuration_scaled
,cast(sum(coalesce(duration_capped,duration)) as bigint) as sumOfDuration_capped
,cast(sum(coalesce(duration_capped,duration)*coalesce(adsmart_scaling_weight,0)) as double) as sumOfDuration_capped_scaled
,cast(count(distinct account_number) as bigint) as nrOfAccounts
,cast(count(distinct (case when adsmart_scaling_weight is null then null else account_number end)) as bigint) as nrOfScaledAccounts
,cast(sumOfDuration as double)/cast(nrOfAccounts as double) as avgDurationPerDay
,cast(sumOfDuration_capped as double)/cast(nrOfAccounts as double) as capped_avgDurationPerDay
,cast(sumOfDuration_capped_scaled as double)/cast(nrOfScaledAccounts as double) as capped_scaled_avgDurationPerDay
,cast(sumOfDuration_scaled as double)/cast(nrOfScaledAccounts as double) as scaled_avgDurationPerDay
into VESPA_dailyFacts
from
VESPA_view_tmp_Leo_oneInstanceOnly
group by
data_date_local
;

select * from VESPA_dailyFacts
order by
data_date_local


-- day, total weights, number of accounts that have been assigned a weight
select data_date_local, sum(adsmart_scaling_weight) as scalingTabWeights, count(account_number) as scalingAccountts
from
(select distinct
adjusted_event_start_date_vespa as data_date_local
,account_number
,adsmart_scaling_weight
from
sk_prod.viq_viewing_data_scaling
where data_date_local between '2014-11-01' and '2014-11-30'
) sca
group by data_date_local
order by data_date_local
;

-- date, number of accounts reporting viewing on that day
select date(event_start_date_time_utc) as data_date_local, count(distinct account_number) as vesAccountts
from
vespa_dp_prog_viewed_201411
group by data_date_local
having
data_date_local between '2014-11-01' and '2014-11-30'
order by data_date_local


-- avg viewing per day per account
select
data_date_local
,cast(sum(coalesce(duration_capped,duration)) as bigint) as viewingDurationPerDay -- viewing duration
,cast(count(distinct account_number) as bigint) as nr_of_accounts
,cast(case when nr_of_accounts=0 then 0 else cast(viewingDurationPerDay as double)/cast(nr_of_accounts as double) end as double) as avgViewingPerAccount
--into table_account
  from
VESPA_view_tmp_Leo_oneInstanceOnly
group by data_date_local
order by data_date_local
;


-- calculate values for each variable in total population from scaling table

select distinct scaling_attribute_06
from
sk_prod.viq_viewing_data_scaling
where adjusted_event_start_date_vespa='2014-11-01'


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
,scaling_attribute_02
into tmp_table
from
###input_table###
where
data_date_local=###run_date###
and
scaling_attribute_02=''London''
--scaling_attribute_02=''NI, Scotland, & Border''
--scaling_attribute_02=''North England''
--scaling_attribute_02=''South England''
--scaling_attribute_02=''Wales & Midlands''
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

scaling_attribute_02=''London''
--scaling_attribute_02=''NI, Scotland, & Border''
--scaling_attribute_02=''North England''
--scaling_attribute_02=''South England''
--scaling_attribute_02=''Wales & Midlands''

select
data_date_local
,cast(count(account_number) as bigint) as nrOfAccounts
,cast( sum(case when scaling_attribute_02='London' then adsmart_scaling_weight else 0 end) as double) as nrOfScaledAccounts
--,cast( sum(case when scaling_attribute_02='NI, Scotland, & Border' then adsmart_scaling_weight else 0 end) as double) as nrOfScaledAccounts
--,cast( sum(case when scaling_attribute_02='North England' then adsmart_scaling_weight else 0 end) as double) as nrOfScaledAccounts
--,cast( sum(case when scaling_attribute_02='South England' then adsmart_scaling_weight else 0 end) as double) as nrOfScaledAccounts
--,cast( sum(case when scaling_attribute_02='Wales & Midlands' then adsmart_scaling_weight else 0 end) as double) as nrOfScaledAccounts
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
,cast( sum(case when scaling_attribute_02='London' then adsmart_scaling_weight else 0 end) as double) as nrOfScaledAccounts
--,cast( sum(case when scaling_attribute_02='NI, Scotland, & Border' then adsmart_scaling_weight else 0 end) as double) as nrOfScaledAccounts
--,cast( sum(case when scaling_attribute_02='North England' then adsmart_scaling_weight else 0 end) as double) as nrOfScaledAccounts
--,cast( sum(case when scaling_attribute_02='South England' then adsmart_scaling_weight else 0 end) as double) as nrOfScaledAccounts
--,cast( sum(case when scaling_attribute_02='Wales & Midlands' then adsmart_scaling_weight else 0 end) as double) as nrOfScaledAccounts
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




select
data_date_local
,cast(sum(raw_viewing_duration_client) as bigint) as viewingDurationPerDay -- viewing duration
,cast(count(distinct account_number) as bigint) as nr_of_accounts
,cast(case when nr_of_accounts=0 then 0 else cast(viewingDurationPerDay as double)/cast(nr_of_accounts as double) end as double) as avgViewingPerAccount
--into table_account
  from
comscore_view_tmp_Leo_oneInstanceOnly
group by data_date_local
;

