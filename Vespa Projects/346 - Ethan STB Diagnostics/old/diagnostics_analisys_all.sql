
/* number of records/day (watch because each day of data includes data from 23:00 of the day before)*/
select reference_date
,count() as nrOfRecords
from
et_technical
group by reference_date
;

/* nr of records per day model */
select reference_date
, substring(id,1,3) as model
,count() as nrOfRecords
from
et_technical
group by reference_date,model
;

/* number of accounts reporting per day */
select reference_date
,count(distinct id) as nrOfAccounts
from
et_technical
group by reference_date
;

/* number of accounts reporting per day per model */
select reference_date
, substring(id,1,3) as model
,count(distinct id) as nrOfAccounts
from
et_technical
group by reference_date,model
;

/* number of accounts reporting per day per model */
select reference_date
, substring(id,1,3) as model
,count(distinct id) as nrOfAccounts
from
et_technical
group by reference_date,model
;

/* number of records per account */
select reference_date
,count(distinct id) as nrOfAccounts
,count() as NrOfRecords
,(cast(NrOfRecords as double)/cast(nrOfAccounts as double)) as recordsPerAccount
from
et_technical
group by reference_date
;

/* number of records per account per model */
select reference_date
, substring(id,1,3) as model
,count(distinct id) as nrOfAccounts
,count() as NrOfRecords
,(cast(NrOfRecords as double)/cast(nrOfAccounts as double)) as recordsPerAccount
from
et_technical
group by reference_date,model
;

/* parameters number */
select count(distinct parameter_name)
--into
from
et_technical
;

/* parameters list with occurrences(nr of times they have been reported) and number of different values */
select parameter_name
,count() as nrOfOccurrences
,count(distinct parameter_value) as distinctValues
from
et_technical
group by parameter_name
order by nrOfOccurrences desc

/*
nr of params returned per msg and id is not always the same but if we look at the distribution, there is some variability

even in the periodic ones there is a spread

inconsistency in nr of params

select distinct top 100 parameter_name
,parameter_value
from
et_technical
where
parameter_name='Device.Ethernet.Link.xxx.LastChange'
*/

select
reference_date
, id
,count() as NrOfRecords
,row_number() over (partition by reference_date order by NrOfRecords desc) as rn
into #tmp_tab
from
et_technical
group by reference_date, id
order by reference_date, rn
;

select reference_date
,sum(NrOfRecords) as RecordsFromTop10
from #tmp_tab
where rn<=10
group by reference_date

select * from #tmp_tab
where rn<=10

/* nr of distinct parameters reported per day per model per account */
select reference_date
, substring(id,1,3) as model
,count(distinct id) as nrOfAccounts
,count(distinct parameter_name) as distinctParams
,(cast(distinctParams as double)/cast(nrOfAccounts as double)) as parametersPerAccount
from
et_technical
group by reference_date,model
;

/* nr of distinct parameters per day */
select reference_date
--, substring(id,1,3) as model
--,count(distinct id) as nrOfAccounts
,count(distinct parameter_name) as distinctValues
--,(cast(distinctValues as double)/cast(nrOfAccounts as double)) as parametersPerAccount
from
et_technical
group by reference_date
;

select reference_date
, substring(id,1,3) as model
,count(distinct tstamp) as nrOFMessages
,count(distinct id) nrOfDevices
,cast(nrOFMessages as double)/cast(nrOfDevices as double) as messagesPerDevice
from
et_technical
group by reference_date, model
order by reference_date, model
;

/*
select reference_date
, substring(id,1,3) as model
,count(distinct tstamp) as nrOFMessages
,count(distinct id) nrOfDevices
,cast(nrOFMessages as double)/cast(nrOfDevices as double) as messagesPerDevice
from
et_technical
group by reference_date, model, tstamp
order by reference_date, model
;
*/

/* dedup */
--drop table tmp_tabb
;

select
id
,tstamp
,parameter_name
,parameter_value
,count() as nrOfDups
into tmp_tabb
from
et_technical
group by id,tstamp,parameter_name,parameter_value
;

-- 97087416 rows



