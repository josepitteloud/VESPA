declare @table_name varchar(50) 
declare @creator  varchar(50) 
declare @column_name  varchar(100)
declare @check_type varchar(40) 
declare @run_type varchar(50) 
declare @dq_run_id int
DECLARE @dq_check_type_id INT

set @table_name =  'data_quality_dp_data_audit'
set @creator =  'kinnairt'
--set @check_type =  'COLUMN_TYPE_LENGTH_CHECK'
--set @check_type =  'ISNULL_CHECK'
--set @check_type =  'UNKNOWN_CHECK'
set @check_type =  'PRIMARY_KEY_CHECK'
set @run_type =  'VESPA_DATA_QUALITY'

set @dq_run_id = (select dq_run_id from data_quality_run_GROUP where UPPER(run_type) = UPPER(@run_type))
set @dq_check_type_id = (select dq_check_type_id from data_quality_check_type where UPPER(dq_check_type) = UPPER(@check_type))

insert into data_quality_check_details
(dq_col_id, dq_run_id, dq_check_type_id, unknown_value, load_timestamp)
select dq_col_id, @dq_run_id, @dq_check_type_id, NULL, getdate()
from data_quality_columns
where UPPER(table_name) = UPPER(@table_name)
and UPPER(creator) = UPPER(@creator)
and UPPER(column_name) like 'PK_%'


commit
