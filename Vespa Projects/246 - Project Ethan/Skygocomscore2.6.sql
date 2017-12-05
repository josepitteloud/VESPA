
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- Project Name: Project Ethan
-- Authors: Leonardo Ripoli (leonardo.ripoli@bskyb.com)
-- Insight Collation: V246
-- Date: 16 December 2014


-- Business Brief:
--      To examine viewing through the Sky Go platform

-- Code Summary:
--      Extract data from Comscore views and put them in a format suitable for analysis

-- Modules:
-- A0 - caveats/open points
-- A1 - Facts section: create auxiliary tables and variables and determines the following facts:
-- total number of active accounts in the Sky go registrant and total number of sam profile id
-- total number of accounts and sam profile id that reported viewing
-- different combinations of stream/platform
-- distribution of number of sam profile ids per account
--
-- A2 - division of viewing into subcategories stream type/platform name
--
-- A3 - linear viewing analysis, in particular, number of viewing events that have been matched with EPG
--
-- A4 - ntiles with median and average duration for viewing subdivided into stream context and platform name




-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- A0 - caveats
-- 1- inconsistencies between comscore view and registrant:
-- there are a number of accounts that in the registrant are associated to a sam profile id -1, whereas in the comscore view they are associated with a proper sam profile id
-- it can be seen from the following:
--
-- we select those accounts in comscore view that have -1 associated in the registrant: currently some 6677 accounts have this problem

select distinct account_number, sam_profileid
from vespa_shared.VESPA_Comscore_SkyGo_Union_View
where account_number in (select distinct account_number from SKY_PLAYER_REGISTRANT where sam_profile_id=-1)
group by account_number, sam_profileid

select count(distinct account_number)
from vespa_shared.VESPA_Comscore_SkyGo_Union_View
where account_number in (select distinct account_number from SKY_PLAYER_REGISTRANT where sam_profile_id=-1)
and sam_profileid!=-1

-- 2- there are some sam_profile ids that are in the comscore view but not in the registrant

select count(distinct sam_profileid) from vespa_shared.VESPA_Comscore_SkyGo_Union_View -- currently 2441131

select count(distinct sam_profileid) from vespa_shared.VESPA_Comscore_SkyGo_Union_View
where sam_profileid in (select distinct sam_profile_id from SKY_PLAYER_REGISTRANT) -- currently 2124882

select count(distinct sam_profileid) from vespa_shared.VESPA_Comscore_SkyGo_Union_View
where sam_profileid not in (select distinct sam_profile_id from SKY_PLAYER_REGISTRANT) -- currently 316249

-- 3- there are some sam_profile ids that are in the comscore view but not in the registrant

select count(distinct account_number) from vespa_shared.VESPA_Comscore_SkyGo_Union_View -- currently 1934467

select count(distinct account_number) from vespa_shared.VESPA_Comscore_SkyGo_Union_View
where account_number in (select distinct account_number from SKY_PLAYER_REGISTRANT) -- currently 1827586
-- difference:
select count(distinct account_number) from vespa_shared.VESPA_Comscore_SkyGo_Union_View
where account_number not in (select distinct account_number from SKY_PLAYER_REGISTRANT) -- currently 316249

select distinct account_number from vespa_shared.VESPA_Comscore_SkyGo_Union_View
where account_number not in
-- therefore it looks like that the registrant is not the most up-to-date source of information -- to be done: LOOK FOR A MORE RELIABLE SOURCE

select distinct account_number
into #temp_accnr
from SKY_PLAYER_REGISTRANT


select distinct account_number
into #temp_accnr_view
from vespa_shared.VESPA_Comscore_SkyGo_Union_View

select count(*) as cnt_view
from #temp_accnr_view

select count(*) as cnt_join
from
#temp_accnr_view vw
inner join
#temp_accnr reg
on vw.account_number=reg.account_number

select distinct account_number from vespa_shared.VESPA_Comscore_SkyGo_Union_View
where account_number not in (select * from #temp_accnr) -- currently 316249

select *
from #temp_accnr_view -- vespa_shared.VESPA_Comscore_SkyGo_Union_View
where account_number='912002820559'

select *
from #temp_accnr --SKY_PLAYER_REGISTRANT
where account_number='912002820559'


select * from
 vespa_shared.VESPA_Comscore_SkyGo_Union_View
where vod_asset_id='7558359afd948410VgnVCM1000000b43150a____'
and data_date_local <= date('2014-10-31')
and sam_profileid=11923923
order by server_start_local_time

select ns_ts,ns_utc from COMSCORE_UNION_VIEW
where sam_profileid=38504210
and cb_data_date= '2014-10-29'
and ns_st_ci='3e100774d1957410VgnVCM1000000b43150a____'
order by ns_utc


select * from
 vespa_shared.VESPA_Comscore_SkyGo_Union_View
where
vod_asset_id='3e100774d1957410VgnVCM1000000b43150a____'
and data_date_local = date('2014-10-29')
and sam_profileid=38504210
--order by server_start_local_time


-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- A1 - Facts section: create auxiliary tables and variables and determines the following facts:
-- total number of active accounts in the Sky go registrant and total number of sam profile id
-- total number of accounts and sam profile id that reported viewing
-- different combinations of stream/platform
-- distribution of number of sam profile ids per account

/*
drop table TableDateAndSamIds;
drop table TableComscoreAccNrsAndSamIds;
drop table TableRegistrantAccNrsAndSamIds;
drop table samid_accnr_date_tab;
*/



-- create dummy variable to be used throughout the script
create or replace variable @dummy int;


-- create table containing distinct date and sam_profileid from VESPA_Comscore_SkyGo_Union_View
IF OBJECT_ID('TableDateAndSamIds') IS NULL
begin
select distinct data_date_local, sam_profileid
into TableDateAndSamIds
  from vespa_shared.VESPA_Comscore_SkyGo_Union_View
end
;


-- create table containing distinct sam_profileid, account_number from VESPA_Comscore_SkyGo_Union_View
-- currently this is unused!!!!
IF OBJECT_ID('TableComscoreAccNrsAndSamIds') IS NULL
begin
select distinct sam_profileid, account_number
into TableComscoreAccNrsAndSamIds
from
vespa_shared.VESPA_Comscore_SkyGo_Union_View
end

;

-- create table containing distinct sam_profileid and ACTIVE account_number from SKY_PLAYER_REGISTRANT
-- to be revised, find a more up-to-date source of information
IF OBJECT_ID('TableRegistrantAccNrsAndSamIds') IS NULL
begin
select distinct reg.sam_profile_id as sam_profileid, reg.account_number
into TableRegistrantAccNrsAndSamIds
from
SKY_PLAYER_REGISTRANT reg
inner join
cust_single_account_view cust
on reg.account_number=cust.account_number
and cust.account_status='2) Active'
commit

update TableRegistrantAccNrsAndSamIds upd
set upd.sam_profileid=com.sam_profileid
from TableRegistrantAccNrsAndSamIds upd
inner join TableComscoreAccNrsAndSamIds com
on upd.account_number=com.account_number
where upd.sam_profileid=-1
and com.sam_profileid!=-1
commit
-- there still remain a certain number of sam ids that have the value -1, this can affect the calculation of the number of devices per person

end

;

/*
-- we join in a table date, sam_profileid and account_number with information from comscore view and registrant (active accounts only)
IF OBJECT_ID('samid_accnr_date_tab') IS NULL
BEGIN
select distinct reported.data_date_local, reported.sam_profileid, registrant.account_number
into samid_accnr_date_tab
from
TableDateAndSamIds as reported
left join
TableRegistrantAccNrsAndSamIds as registrant
on registrant.sam_profileid=reported.sam_profileid
END
*/

-- we join in a table date, sam_profileid and account_number with information from comscore view and registrant (active accounts only)
IF OBJECT_ID('samid_accnr_date_tab') IS NULL
BEGIN
select distinct data_date_local, sam_profileid, account_number
into samid_accnr_date_tab
  from vespa_shared.VESPA_Comscore_SkyGo_Union_View
END

;
-- Firstly, caluclate the number of total sam_profile_id and accounts registered for the SkyGo
create or replace variable @nrOfTotalSamIds double;
set @nrOfTotalSamIds=(select count(distinct sam_profileid) from TableRegistrantAccNrsAndSamIds)
;
create or replace variable @nrOfTotalCurrAccounts double;
set @nrOfTotalCurrAccounts=(select count(distinct account_number) from TableRegistrantAccNrsAndSamIds)
;

-- Secondly, caluclate the number of total sam_profile_id and accounts that reported data for the SkyGo
create or replace variable @nrOfTotalSamIdsReported double;
set @nrOfTotalSamIdsReported=(select count(distinct sam_profileid) from samid_accnr_date_tab)
;
create or replace variable @nrOfTotalCurrAccountsReported double;
set @nrOfTotalCurrAccountsReported=(select count(distinct account_number) from samid_accnr_date_tab)
;


-- facts section:

-- number of sam ids that reported, number of accounts that reported, and percentage out of registered active ones
select @nrOfTotalSamIds,@nrOfTotalCurrAccounts,@nrOfTotalSamIdsReported,@nrOfTotalCurrAccountsReported, (@nrOfTotalSamIdsReported/@nrOfTotalSamIds)*100 as percentageSamIdsThatReported, (@nrOfTotalCurrAccountsReported/@nrOfTotalCurrAccounts)*100
;

-- here we find the distincs available combinations:
select distinct
stream_context -- lin, vod, dvod
,platform_name -- ios,android, plugin etc.
  from vespa_shared.VESPA_Comscore_SkyGo_Union_View


-- the following select gives the number of devices per person total (active sky registrant accounts)
select nrOfDevices, count(account_number) as nrOfAccounts, (cast(nrOfAccounts as double)/@nrOfTotalCurrAccounts)*100 as PercentageTotalAccounts
from
(
select account_number, count(distinct sam_profileid) as nrOfDevices
from TableRegistrantAccNrsAndSamIds
group by account_number
) as tmp
group by nrOfDevices
order by nrOfDevices


-- the following select gives the number of devices per person (accounts that reported only)
select nrOfDevices, count(distinct account_number) as nrOfAccounts, (cast(nrOfAccounts as double)/@nrOfTotalCurrAccountsReported)*100 as PercentageOfReportedAccounts
from
(
select account_number, count(distinct sam_profileid) as nrOfDevices
from samid_accnr_date_tab
group by account_number
) as tmp
group by nrOfDevices
order by nrOfDevices


-- the following select gives samids per day, accounts per day and percentage of sam_ids per day that have a null account associated
select
data_date_local
,DATENAME(dw,data_date_local) as dayName
,count(distinct sam_profileid) as samidsPerDay
--,(cast(samidsPerDay as double)/@nrOfTotalSamIds)*100 as percentageOfTotalSamIds
,(cast(samidsPerDay as double)/@nrOfTotalSamIdsReported)*100 as percentageOfTotalSamIdsThatReported
,count(account_number) as accountsPerDay
--, (cast(accountsPerDay as double)/@nrOfTotalCurrAccounts)*100 as percentageAccounts
, (cast(accountsPerDay as double)/@nrOfTotalCurrAccountsReported)*100 as percentageAccountsThatReported
,sum(case when account_number is null then 1 else 0 end) as samIdsWithNullAccountsPerDay
,(cast(samIdsWithNullAccountsPerDay as double)/cast(samidsPerDay as double))*100 as percentaceOfSamIdsWithNullAccountsPerDay
,sum(case when sam_profileid is null then 1 else 0 end) as nullSamidsPerDay
from samid_accnr_date_tab
group by data_date_local
order by data_date_local


-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- A2:  Create table containing viewing divided in subcategories stream and platform

IF OBJECT_ID('TableStreamPlatform') IS NOT NULL
DROP table TableStreamPlatform
;
--
select data_date_local
,DATENAME(dw,data_date_local) as dayName
,stream_context -- lin, vod, dvod
,platform_name -- ios,android, plugin etc.
,count(1) as NrOfEvents_StreamPlatform -- nr of records per stream per platform
,cast(sum(duration_viewed) as bigint) as viewingDuration_StreamPlatform -- viewing duration
,count(distinct sam_profileid) as nrOfSamProfileids_StreamPlatform -- nr of sam ids per stream per platform
,sum(case when account_number is null then 1 else 0 end) as NrOfNullAccount_StreamPlatform -- nr of null account_nr per stream per platform
,sum(case when service_key is null then 1 else 0 end) as NrOfNullSerKeys_StreamPlatform -- nr of null ser_keys per stream (it will only makes sense for linear streams)
,(cast(nrOfSamProfileids_StreamPlatform as double)/@nrOfTotalSamIdsReported)*100 as percentageOfTotSamIdsThatReported_StreamPlatform
into TableStreamPlatform
  from vespa_shared.VESPA_Comscore_SkyGo_Union_View
group by data_date_local, stream_context,platform_name
order by data_date_local,stream_context,platform_name
;

select * from TableStreamPlatform

-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- A3:   linear viewing analysis, in particular, number of viewing events that have been matched with EPG

-- firstly  Create table containing viewing divided in subcategories stream type

IF OBJECT_ID('TableStream') IS NOT NULL
DROP table TableStream
;

select data_date_local
,stream_context -- stream type (lin, vod ....)
,count(1) as NrOfEvents_stream -- nr of events per stream
,cast(sum(duration_viewed) as bigint) as viewingDuration_stream -- viewing duration per stream
,count(distinct sam_profileid) as nrOfSamProfileids_stream -- nr of accounts per stream
,sum(case when account_number is null then 1 else 0 end) as NrOfNullAccount_stream -- nr of nul accounts per stream
,sum(case when service_key is null then 1 else 0 end) as NrOfNullSerKeys_stream -- nr of null ser_keys per stream (it will only makes sense for linear streams)
,cast(nrOfSamProfileids_stream as double)/@nrOfTotalSamIdsReported as percentageOfTotSamIdsThatReported_stream
into TableStream
  from vespa_shared.vespa_comscore_skygo_union_view
group by data_date_local,stream_context

;


-- then create a table characterized for linear streaming, taking only records for which EPG matching has been possible
IF OBJECT_ID('TableLin_plat_linInst') IS NOT NULL
DROP table TableLin_plat_linInst
;
--
select data_date_local
,platform_name
,count(1) as NrOfEvents_lin_plat_linInst
,count(distinct sam_profileid) as nrOfSamProfileids_lin_plat_linInst
,sum(case when account_number is null then 1 else 0 end) as NrOfNullAccount_lin_plat_linInst -- nr of null accounts per linear stream per instance
,sum(case when service_key is null then 1 else 0 end) as NrOfNullSerKeys_lin_plat_linInst -- nr of null ser_keys per linear stream per instance_flag
,cast(nrOfSamProfileids_lin_plat_linInst as double)/@nrOfTotalSamIdsReported as percentageOfTotSamIdsThatReported_lin_plat_linInst
into TableLin_plat_linInst
  from vespa_shared.vespa_comscore_skygo_union_view
where stream_context='lin'
and linear_instance_flag=1
group by data_date_local,platform_name
order by data_date_local,platform_name

;

-- then join the previous 2 to retrieve information on how many linear events have been successfully associated with EPG
select lin.data_date_local
,DATENAME(dw,lin.data_date_local) as dayName
,lin.platform_name
,str_plat.NrOfEvents_StreamPlatform as NrOfLinearViewingEvents
,lin.NrOfEvents_lin_plat_linInst as NrOf_EPG_LinearViewingEvents -- nr of events with EPG match
,(NrOfLinearViewingEvents - NrOf_EPG_LinearViewingEvents) as NrOf_NO_EPG_LinearViewingEvents
,(CAST(NrOf_NO_EPG_LinearViewingEvents AS DOUBLE)/CAST(NrOfLinearViewingEvents AS DOUBLE))*100 as percentageOfNoEPGevents
,str_plat.nrOfSamProfileids_StreamPlatform as NrOfAccounts_LinearViewing
,lin.nrOfSamProfileids_lin_plat_linInst
,str_plat.NrOfNullAccount_StreamPlatform as NrOfNullAccount_LinearViewing
,str_plat.NrOfNullSerKeys_StreamPlatform as NrOfNullSerKeys_LinearViewing
from
TableLin_plat_linInst as lin
inner join
TableStreamPlatform as str_plat
on str_plat.data_date_local=lin.data_date_local
and str_plat.platform_name=lin.platform_name
where str_plat.stream_context='lin'
/*where*/
order by lin.data_date_local

;


-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- A4: calculate average/median viewing per ntiles

-- Create table containing viewing duration grouped by sam_profileid,platform_name,stream_context

-- total viewing duration across days per sam_id (subcategories stream and platform)
-- calculate here avg viewing per day, obtained by total viewing per sam profile id divided by days of viewing
IF OBJECT_ID('table_samprofileid') IS NOT NULL
DROP table table_samprofileid
;

select
sam_profileid
,platform_name
,stream_context
,cast(sum(raw_viewing_duration_client) as bigint) as viewingDurationAcrossDays -- viewing duration
,count(distinct data_date_local) as nr_of_days_of_viewing
,(case when nr_of_days_of_viewing != 0 then cast(viewingDurationAcrossDays as double)/cast(nr_of_days_of_viewing as double) else 0 end) as avgDurationPerDay
into table_samprofileid
  from comscore_view_tmp_Leo_oneInstanceOnly
group by sam_profileid,platform_name,stream_context
;

-- calculate the ntiles and put results in a table
IF OBJECT_ID('tableNtiles') IS NOT NULL
DROP table tableNtiles
;

select
platform_name
,stream_context
,sam_profileid
,avgDurationPerDay
,ntile(100) over (partition by platform_name, stream_context order by avgDurationPerDay) as ntile
--,count(sam_profileid) over (partition by platform_name, stream_context, ntile) as nrPieces
into tableNtiles
from table_samprofileid
--group by platform_name, stream_context, ntile

;
select
stream_context
,platform_name
,ntile
,avg(avgDurationPerDay) as avgViewing
,median(avgDurationPerDay) as medianViewing
,count(avgDurationPerDay) as nrOfSamIds
from tableNtiles
group by
stream_context,platform_name,ntile


-- organize per account


-- total viewing duration across days per sam_id (subcategories stream and platform)
-- calculate here avg viewing per day, obtained by total viewing per sam profile id divided by days of viewing
IF OBJECT_ID('table_account') IS NOT NULL
DROP table table_account
;

select
account_number
,platform_name
,stream_context
,cast(sum(raw_viewing_duration_client) as bigint) as viewingDurationAcrossDays -- viewing duration
,count(distinct data_date_local) as nr_of_days_of_viewing
,(case when nr_of_days_of_viewing != 0 then cast(viewingDurationAcrossDays as double)/cast(nr_of_days_of_viewing as double) else 0 end) as avgDurationPerDay
into table_account
  from
(
 select raw_viewing_duration_client,account_number,platform_name,stream_context,data_date_local
from
comscore_view_tmp_Leo_oneInstanceOnly
--where erroneous_data_suspected_flag=0
) tab
group by account_number,platform_name,stream_context
;

delete from table_account
where account_number is null
;
-- calculate the ntiles and put results in a table
IF OBJECT_ID('tableNtiles_account') IS NOT NULL
DROP table tableNtiles_account
;

select
platform_name
,stream_context
,account_number
,avgDurationPerDay
,ntile(100) over (partition by platform_name,stream_context order by avgDurationPerDay) as ntile
--,count(sam_profileid) over (partition by platform_name, stream_context, ntile) as nrPieces
into tableNtiles_account
from table_account
--group by platform_name, stream_context, ntile

;
select
stream_context
,platform_name
,ntile
,avg(avgDurationPerDay) as avgViewing
,median(avgDurationPerDay) as medianViewing
,count(avgDurationPerDay) as nrOfAccounts
from tableNtiles_account
group by
platform_name,stream_context,ntile

-- total (no stream/platform subdivision)


-- total viewing duration across days per sam_id (subcategories stream and platform)
-- calculate here avg viewing per day, obtained by total viewing per sam profile id divided by days of viewing
IF OBJECT_ID('table_account') IS NOT NULL
DROP table table_account
;

select
account_number
,cast(sum(raw_viewing_duration_client) as bigint) as viewingDurationAcrossDays -- viewing duration
,count(distinct data_date_local) as nr_of_days_of_viewing
,(case when nr_of_days_of_viewing != 0 then cast(viewingDurationAcrossDays as double)/cast(nr_of_days_of_viewing as double) else 0 end) as avgDurationPerDay
into table_account
  from
comscore_view_tmp_Leo_oneInstanceOnly
group by account_number
;

delete from table_account
where account_number is null
;

-- calculate the ntiles and put results in a table
IF OBJECT_ID('tableNtiles_account') IS NOT NULL
DROP table tableNtiles_account
;

select
account_number
,avgDurationPerDay
,ntile(100) over (order by avgDurationPerDay) as ntile
--,count(sam_profileid) over (partition by platform_name, stream_context, ntile) as nrPieces
into tableNtiles_account
from table_account
--group by platform_name, stream_context, ntile

select count(distinct account_number) from table_account
select count(distinct account_number) from comscore_view_tmp_Leo_oneInstanceOnly
;
select
ntile
,avg(avgDurationPerDay) as avgViewing
,median(avgDurationPerDay) as medianViewing
,count(avgDurationPerDay) as nrOfAccounts
from tableNtiles_account
group by
ntile

-- avg viewing per day per account


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

select
data_date_local
,cast(sum(raw_viewing_duration_client) as bigint) as viewingDurationPerDay -- viewing duration
,cast(count(distinct account_number) as bigint) as nr_of_accounts
,cast(case when nr_of_accounts=0 then 0 else cast(viewingDurationPerDay as double)/cast(nr_of_accounts as double) end as double) as avgViewingPerAccount
--into table_account
  from
(
 select raw_viewing_duration_client,account_number,platform_name,stream_context,data_date_local
from
comscore_view_tmp_Leo_oneInstanceOnly
where erroneous_data_suspected_flag=0
) tab
group by data_date_local
;

-- avg viewing per day subdivided per stream/account

select
data_date_local
,platform_name
,stream_context
,cast(sum(raw_viewing_duration_client) as bigint) as viewingDurationPerDay -- viewing duration
,cast(count(distinct account_number) as bigint) as nr_of_accounts
,cast(case when nr_of_accounts=0 then 0 else cast(viewingDurationPerDay as double)/cast(nr_of_accounts as double) end as double) as avgViewingPerAccount
--into table_account
  from
comscore_view_tmp_Leo_oneInstanceOnly
group by data_date_local,platform_name,stream_context
order by data_date_local,platform_name,stream_context
;

select
data_date_local
,platform_name
,stream_context
,cast(sum(raw_viewing_duration_client) as bigint) as viewingDurationPerDay -- viewing duration
,cast(count(distinct account_number) as bigint) as nr_of_accounts
,cast(case when nr_of_accounts=0 then 0 else cast(viewingDurationPerDay as double)/cast(nr_of_accounts as double) end as double) as avgViewingPerAccount
--into table_account
  from
(
 select raw_viewing_duration_client,account_number,platform_name,stream_context,data_date_local
from
comscore_view_tmp_Leo_oneInstanceOnly
where erroneous_data_suspected_flag=0
) tab
group by data_date_local,platform_name,stream_context
order by data_date_local,platform_name,stream_context
;



-- overlap with panel

-- calculate daily sum of weights in the comscore

select data_date_local, sum(adsmart_scaling_weight) as scalingTabWeights, count(account_number) as scalingAccountts
from
(select distinct
data_date_local
,account_number
,adsmart_scaling_weight
from comscore_view_tmp_Leo_oneInstanceOnly
where adsmart_scaling_weight is not null
) sca
group by data_date_local
order by data_date_local
;










































-- firstly, put data into the table
IF OBJECT_ID('VespaTable') IS NOT NULL
DROP table VespaTable
;

select
adjusted_event_start_date_vespa
,account_number
,max(calculated_scaling_weight) as calculated_scaling_weight
into VespaTable
from
sk_prod.viq_viewing_data_scaling
where (adjusted_event_start_date_vespa>'2014-10-22' and adjusted_event_start_date_vespa<'2014-12-13')
group by
adjusted_event_start_date_vespa,account_number
;
delete from VespaTable
where account_number is null


;

IF OBJECT_ID('ComscoreTable') IS NOT NULL
DROP table ComscoreTable
;

select
data_date_local
,account_number
,platform_name
,stream_context
,cast(sum(raw_viewing_duration_client) as bigint) as viewingDurationPerDay -- viewing duration
,count() as viewingEventsPerDay
into ComscoreTable
  from comscore_view_tmp_Leo_oneInstanceOnly
group by data_date_local,account_number,platform_name,stream_context
;

delete from ComscoreTable
where account_number is null
;


IF OBJECT_ID('OverlapTable') IS NOT NULL
DROP table OverlapTable
;

select
data_date_local
,com.account_number
,platform_name
,stream_context
,viewingDurationPerDay -- viewing duration
,ves.calculated_scaling_weight
,cast(viewingDurationPerDay*ves.calculated_scaling_weight as double) as scaledViewingDurationPerDay
,viewingEventsPerDay
,cast(viewingEventsPerDay*ves.calculated_scaling_weight as double ) as scaledViewingEventsPerDay
into OverlapTable
  from
ComscoreTable com
inner join
VespaTable ves
on
com.data_date_local=ves.adjusted_event_start_date_vespa
and
com.account_number=ves.account_number
;


-- calculate the ntile duration
IF OBJECT_ID('table_account_overlap') IS NOT NULL
DROP table table_account_overlap
;

select
account_number
,platform_name
,stream_context
,cast(sum(viewingDurationPerDay) as bigint) as viewingDurationAcrossDays -- viewing duration
,sum(scaledViewingDurationPerDay) as scaledViewingDurationAcrossDays -- viewing duration
,count(distinct data_date_local) as nr_of_days_of_viewing
,(case when nr_of_days_of_viewing != 0 then cast(viewingDurationAcrossDays as double)/cast(nr_of_days_of_viewing as double) else 0 end) as avgDurationPerDay
,(case when nr_of_days_of_viewing != 0 then scaledViewingDurationAcrossDays/cast(nr_of_days_of_viewing as double) else 0 end) as scaledAvgDurationPerDay
,sum(calculated_scaling_weight) as sumOfWeightsAcrossDays
,(case when nr_of_days_of_viewing != 0 then sumOfWeightsAcrossDays/cast(nr_of_days_of_viewing as double) else 0 end) as averagedScalingWeightPerDay
,sum(viewingEventsPerDay) as viewingEventsAcrossDays
,(case when nr_of_days_of_viewing != 0 then viewingEventsAcrossDays/cast(nr_of_days_of_viewing as double) else 0 end) as averagedViewingEventsPerDay
into table_account_overlap
  from OverlapTable
group by account_number,platform_name,stream_context
;


delete from table_account_overlap
where account_number is null
;
-- calculate the ntiles and put results in a table
IF OBJECT_ID('tableNtiles_account_overlap') IS NOT NULL
DROP table tableNtiles_account_overlap
;

select
platform_name
,stream_context
,account_number
,avgDurationPerDay
,ntile(100) over (partition by platform_name, stream_context order by avgDurationPerDay) as ntile
--,count(sam_profileid) over (partition by platform_name, stream_context, ntile) as nrPieces
into tableNtiles_account_overlap
from table_account_overlap
--group by platform_name, stream_context, ntile

;


select
stream_context
,platform_name
,ntile
,avg(avgDurationPerDay) as avgViewing
,median(avgDurationPerDay) as medianViewing
,count(avgDurationPerDay) as nrOfAccounts
from tableNtiles_account_overlap
group by
stream_context,platform_name,ntile
;

-- calculate the ntiles and put results in a table
IF OBJECT_ID('tableNtiles_account_overlap_scaled') IS NOT NULL
DROP table tableNtiles_account_overlap_scaled
;

select
platform_name
,stream_context
,account_number
,scaledAvgDurationPerDay
,ntile(100) over (partition by platform_name, stream_context order by scaledAvgDurationPerDay) as ntile
--,count(sam_profileid) over (partition by platform_name, stream_context, ntile) as nrPieces
into tableNtiles_account_overlap_scaled
from table_account_overlap
--group by platform_name, stream_context, ntile

;


select
stream_context
,platform_name
,ntile
,avg(scaledAvgDurationPerDay) as scaled_avgViewing
,median(scaledAvgDurationPerDay) as scaled_medianViewing
,count(scaledAvgDurationPerDay) as nrOfAccounts
from tableNtiles_account_overlap_scaled
group by
stream_context,platform_name,ntile
;


-- global figures:
drop table tableLeft;
drop table tableRight;
drop table tableLeftStrPlat;
drop table tableRightStrPlat;


select
data_date_local
,DATENAME(dw,data_date_local) as dayName
--, stream_context, platform_name
,count(distinct account_number) as totalAccounts
,sum(viewingDurationPerDay) as sumOfViewingDurations
--into tableLeft
from
ComscoreTable
group by data_date_local --, stream_context, platform_name
--order by data_date_local, stream_context, platform_name
;
select
data_date_local
,DATENAME(dw,data_date_local) as dayName
--, stream_context, platform_name
,count(distinct account_number) as overlapAccounts
,sum(calculated_scaling_weight) as scaledOverlapAccounts
,sum(viewingDurationPerDay) as OverlapViewingDuration
,sum( cast(viewingDurationPerDay*calculated_scaling_weight as double)) as scaledViewingDuration
into tableRight
from
OverlapTable
group by data_date_local --, stream_context, platform_name
--order by data_date_local, stream_context, platform_name
;

select
l.data_date_local
,l.dayName
,l.totalAccounts
,l.sumOfViewingDurations
,r.overlapAccounts
,r.scaledOverlapAccounts
,r.OverlapViewingDuration
,r.scaledViewingDuration
from
tableLeft l
left join
tableRight r
on
l.data_date_local=r.data_date_local
;


-- per stream/platform
select
data_date_local
,DATENAME(dw,data_date_local) as dayName
, stream_context, platform_name
,count(account_number) as totalAccounts
,sum(viewingDurationPerDay) as sumOfViewingDurations
into tableLeftStrPlat
from
ComscoreTable
group by data_date_local, stream_context, platform_name
--order by data_date_local, stream_context, platform_name
;
select
data_date_local
,DATENAME(dw,data_date_local) as dayName
, stream_context, platform_name
,count(account_number) as overlapAccounts
,sum(calculated_scaling_weight) as scaledOverlapAccounts
,sum(viewingDurationPerDay) as OverlapViewingDuration
,sum( cast(viewingDurationPerDay*calculated_scaling_weight as double)) as scaledViewingDuration
into tableRightStrPlat
from
OverlapTable
group by data_date_local, stream_context, platform_name
--order by data_date_local, stream_context, platform_name
;

select
l.data_date_local
,l.dayName
, l.stream_context
,l.platform_name
,l.totalAccounts
,l.sumOfViewingDurations
,r.overlapAccounts
,r.scaledOverlapAccounts
,r.OverlapViewingDuration
,r.scaledViewingDuration

from
tableLeftStrPlat l
left join
tableRightStrPlat r
on
l.data_date_local=r.data_date_local
and
l.stream_context=r.stream_context
and
l.platform_name=r.platform_name
;

---
-- VESPA VIEWING


IF OBJECT_ID('VESPAviewingTable') IS NOT NULL -- ComscoreTable
DROP table VESPAviewingTable
;

select
data_date_local
,account_number
,platform_name
,stream_context
,cast(sum(raw_viewing_duration_client) as bigint) as viewingDurationPerDay -- viewing duration
,count() as viewingEventsPerDay
into VESPAviewingTable
  from vespa_dp_prog_viewed_201411
group by data_date_local,account_number,platform_name,stream_context
;




/*
data_date_local
,com.account_number
,platform_name
,stream_context
,viewingDurationPerDay -- viewing duration
,ves.calculated_scaling_weight
,cast(viewingDurationPerDay*ves.calculated_scaling_weight as double) as scaledViewingDurationPerDay
,viewingEventsPerDay
,cast(viewingEventsPerDay*ves.calculated_scaling_weight as double ) as scaledViewingEventsPerDay

*/

select
distinct data_date_local
,DATENAME(dw,data_date_local) as dayName
from
OverlapTable
order by data_date_local

/*
IF OBJECT_ID('OverlapTablecheck') IS NOT NULL
DROP table OverlapTablecheck
;

select
data_date_local
,adjusted_event_start_date_vespa as vespa_date
,ves.account_number as ves_account
,com.account_number as com_account
,platform_name
,stream_context
,viewingDurationPerDay -- viewing duration
,ves.calculated_scaling_weight
into OverlapTablecheck
  from
ComscoreTable com
inner join
VespaTable ves
on
com.data_date_local=ves.adjusted_event_start_date_vespa
and
com.account_number=ves.account_number


select top 1000 *
from
OverlapTablecheck

*/

select * from
comscore_view_tmp_Leo_oneInstanceOnly
where
raw_viewing_duration_client > 86400
order by data_date_local,aggr_event_id, viewing_event_start_utc

select data_date_local,max(duration_viewed) as maxDuration
from vespa_shared.VESPA_Comscore_SkyGo_Union_View
group by data_date_local
having maxDuration>86400
order by data_date_local


select data_date_local,max(duration_viewed) as maxDuration, cast(sum(duration_viewed) as bigint) as sumOfDurations, count() as nrOfRecords
from
(select * from
vespa_shared.VESPA_Comscore_SkyGo_Union_View
where duration_viewed>86400
) tab
group by data_date_local
order by data_date_local

select

select data_date_local,count(),sum()
from
(select * from
comscore_view_tmp_Leo_oneInstanceOnly
where
raw_viewing_duration_client > 86400
) tab
group by data_date_local
order by data_date_local


select top 10 *
 from vespa_shared.VESPA_Comscore_SkyGo_Union_View
where view_continuing_flag=1
and datepart(hh,viewing_event_start_local) != 0
and data_date_local='2014-10-30'

--


IF OBJECT_ID('comscoreWednesdayEventTable') IS NOT NULL -- ComscoreTable
DROP table comscoreWednesdayEventTable
;

-- choose '2014-11-12', wednesday and '2014-11-23' a Sunday

select
aggr_event_id
,raw_viewing_duration_client
,ntile(100) over (order by raw_viewing_duration_client) as ntile
into comscoreWednesdayEventTable
from
comscore_view_tmp_Leo_oneInstanceOnly
where
data_date_local='2014-11-12'

-- take median for each ntile

select
ntile
,median(raw_viewing_duration_client) as median
,count() as nrOfEvents
from comscoreWednesdayEventTable
group by ntile
order by ntile



IF OBJECT_ID('comscoreWednesdayEventTableStrPlat') IS NOT NULL -- ComscoreTable
DROP table comscoreWednesdayEventTableStrPlat
;
-- subdivision stream/platform
select
aggr_event_id
,stream_context
,platform_name
,raw_viewing_duration_client
,ntile(100) over (partition by platform_name, stream_context order by raw_viewing_duration_client) as ntile
into comscoreWednesdayEventTableStrPlat
from
comscore_view_tmp_Leo_oneInstanceOnly
where
data_date_local='2014-11-12'

-- take median for each ntile

select
platform_name
,stream_context
,ntile
,median(raw_viewing_duration_client) as median
,count() as nrOfEvents
from comscoreWednesdayEventTableStrPlat
group by platform_name, stream_context,ntile
order by platform_name, stream_context,ntile



IF OBJECT_ID('comscoreSundayEventTable') IS NOT NULL -- ComscoreTable
DROP table comscoreSundayEventTable
;

-- choose '2014-11-12', wednesday and '2014-11-23' a Sunday

select
aggr_event_id
,raw_viewing_duration_client
,ntile(100) over (order by raw_viewing_duration_client) as ntile
into comscoreSundayEventTable
from
comscore_view_tmp_Leo_oneInstanceOnly
where
data_date_local='2014-11-23'

-- take median for each ntile

select
ntile
,median(raw_viewing_duration_client) as median
,count() as nrOfEvents
from comscoreSundayEventTable
group by ntile
order by ntile



IF OBJECT_ID('comscoreSundayEventTableStrPlat') IS NOT NULL -- ComscoreTable
DROP table comscoreSundayEventTableStrPlat
;
-- subdivision stream/platform
select
aggr_event_id
,platform_name
,stream_context
,raw_viewing_duration_client
,ntile(100) over (partition by platform_name, stream_context order by raw_viewing_duration_client) as ntile
into comscoreSundayEventTableStrPlat
from
comscore_view_tmp_Leo_oneInstanceOnly
where
data_date_local='2014-11-23'

-- take median for each ntile


select
platform_name
,stream_context
,ntile
,median(raw_viewing_duration_client) as median
,avg(raw_viewing_duration_client) as avg
,min(raw_viewing_duration_client) as min
,max(raw_viewing_duration_client) as max
,count() as nrOfEvents
from comscoreSundayEventTableStrPlat
group by platform_name, stream_context,ntile
order by platform_name, stream_context,ntile


select
count(distinct account_number)
from SKY_PLAYER_REGISTRANT

select data_date_local
,count(distinct account_number)
from
comscore_view_tmp_Leo_oneInstanceOnly
group by
data_date_local
order by
data_date_local


-- distribution of time differences

select top 10 event_duration_client from comscore_view_tmp_Leo_oneInstanceOnly

update comscore_view_tmp_Leo_oneInstanceOnly
set wrong_data_flag=2

update comscore_view_tmp_Leo_oneInstanceOnly
set event_duration_client=datediff(ss,viewing_event_start_utc,viewing_event_end_utc)
, event_duration_server=datediff(ss,server_event_start_utc,server_event_end_utc)

select data_date_local, count()
from
comscore_view_tmp_Leo_oneInstanceOnly
group by data_date_local
order by data_date_local


-- delete first and last day (the boundaries will have midnight match problem so we eliminate them)
delete from comscore_view_tmp_Leo_oneInstanceOnly
where ((data_date_local = '2014-11-02') or (data_date_local = '2014-11-10'))

select
data_date_local
,aggr_event_id
--,matching_aggr_event_id_post_midnight
,erroneous_data_suspected_flag
,datediff(ss,viewing_event_start_utc,server_event_start_utc) as difference_start
,datediff(ss,viewing_event_end_utc,server_event_end_utc) as difference_end
into differenceTable
from comscore_view_tmp_Leo_oneInstanceOnly



select count()
from  vespa_shared.VESPA_Comscore_SkyGo_Union_View
where
((data_date_local > '2014-11-02') and (data_date_local < '2014-11-10'))
and ((datediff(ss,viewing_event_start_utc,server_event_start_utc)>(24*3600)) or (datediff(ss,viewing_event_end_utc,server_event_end_utc)>(24*3600)))
and erroneous_data_suspected_flag=0


select count()
from  vespa_shared.VESPA_Comscore_SkyGo_Union_View
where
((data_date_local > '2014-11-02') and (data_date_local < '2014-11-10'))
and erroneous_data_suspected_flag=1
and ((datediff(ss,viewing_event_start_utc,server_event_start_utc)>(24*3600)) or (datediff(ss,viewing_event_end_utc,server_event_end_utc)>(24*3600)))


select count() from differenceTable
where ((difference_start>(24*3600)) or (difference_end>(24*3600)))
and erroneous_data_suspected_flag=0

select count() from differenceTable
where ((difference_start>(24*3600)) or (difference_end>(24*3600)))
and erroneous_data_suspected_flag=1


create table allEvents
(
 event_difference bigint

);

create table allEventsNoError
(
 event_difference bigint

);

insert into allEvents
select
difference_start
from differenceTable

insert into allEvents
select
difference_end
from differenceTable


insert into allEventsNoError
select
difference_start
from differenceTable
where erroneous_data_suspected_flag=0

insert into allEventsNoError
select
difference_end
from differenceTable
where erroneous_data_suspected_flag=0



select
event_difference
,ntile(100) over (order by event_difference) as ntile
into ntileDiff
from allEvents


select
event_difference
,ntile(100) over (order by event_difference) as ntile
into ntileDiffNoError
from allEventsNoError


select
ntile
,median(event_difference) as median
,avg(event_difference) as avg
,min(event_difference) as min
,max(event_difference) as max
,count() as nrOfEvents
from ntileDiffNoError
group by ntile
order by ntile
;

select
ntile
,median(event_difference) as median
,avg(event_difference) as avg
,min(event_difference) as min
,max(event_difference) as max
,count() as nrOfEvents
from ntileDiff
group by ntile
order by ntile
;

