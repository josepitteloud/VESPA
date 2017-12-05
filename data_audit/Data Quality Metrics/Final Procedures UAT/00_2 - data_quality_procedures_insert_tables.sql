-----------------------------insert basic checks types into the data_quality_check_type table start---------------------------------------------------------
--*************************************************************CHECKEAR
insert into data_quality_check_type
select * from kinnairt.data_quality_check_type
--*************************************************************CHECKEAR
commit

-----------------------------insert basic checks types into the data_quality_check_type table end---------------------------------------------------------

-----------------------------insert vespa_run_group_type into data_quality_run_group table start---------------------------------------------------------


insert into data_quality_run_group
select * from kinnairt.data_quality_run_group

commit

-----------------------------insert vespa_run_group_type into data_quality_run_group table end---------------------------------------------------------

-----------------------------insert columns for basic checks into data_quality_columns table start---------------------------------------------------------

insert into data_quality_columns
select * from kinnairt.data_quality_columns

commit

-----------------------------insert columns for basic checks into data_quality_columns table end---------------------------------------------------------

-----------------------------insert columns for basic checks into data_quality_check_details table start---------------------------------------------------------


insert into data_quality_check_details
select * from kinnairt.data_quality_check_details

commit

-----------------------------insert columns for basic checks into data_quality_check_details table end---------------------------------------------------------

--*************************************************************CHECKEAR
insert into data_quality_vespa_metrics
select * from kinnairt.data_quality_vespa_metrics
--*************************************************************CHECKEAR
commit

-----------------------------insert columns for results into data_quality_results table start---------------------------------------------------------

insert into data_quality_results
(dq_check_detail_id ,dq_run_id  ,result ,RAG_STATUS ,
sql_processed   ,date_period    ,data_total ,logger_id  ,
data_date   ,load_timestamp ,modified_date)
select
dq_check_detail_id  ,dq_run_id  ,result ,RAG_STATUS ,
sql_processed   ,date_period    ,data_total ,logger_id  ,
data_date   ,load_timestamp ,modified_date
from kinnairt.data_quality_results

commit



-----------------------------insert columns for results into data_quality_results table end---------------------------------------------------------


-----------------------------insert columns for results into data_quality_sky_base_upscale table start---------------------------------------------------------

insert into data_quality_sky_base_upscale
select * from kinnairt.data_quality_sky_base_upscale;

commit

-----------------------------insert columns for results into data_quality_vespa_repository table start---------------------------------------------------------

insert into data_quality_vespa_repository
(dq_run_id  ,
viewing_data_date   ,
dq_vm_id ,
metric_result   ,
metric_tolerance_amber  ,
metric_tolerance_red    ,
metric_rag  ,
load_timestamp,
modified_date)
select
dq_run_id   ,
viewing_data_date   ,
dq_vm_id    bigint  ,
metric_result   ,
metric_tolerance_amber  ,
metric_tolerance_red    ,
metric_rag  ,
load_timestamp,
modified_date
from
kinnairt.data_quality_vespa_repository;

commit


-----------------------------insert columns for results into data_quality_vespa_repository table end---------------------------------------------------------


----------------------data_quality columns tables-------------------------------------------------------


insert into data_quality_columns
(creator, column_name, table_name, column_type, column_length, load_timestamp)
select creator, cname, tname, coltype, length, getdate() from sys.syscolumns
where upper(tname) = 'DATA_QUALITY_LINEAR_SLOT_CAMPAIGN_DATA_AUDIT'

commit

insert into data_quality_columns
(creator, column_name, table_name, column_type, column_length, load_timestamp)
select creator, cname, tname, coltype, length, getdate() from sys.syscolumns
where upper(tname) = 'DATA_QUALITY_SLOT_DATA_AUDIT'

commit
