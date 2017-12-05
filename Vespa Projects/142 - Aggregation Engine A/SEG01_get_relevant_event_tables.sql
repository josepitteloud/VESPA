

CREATE or replace procedure SEG01_get_relevant_event_tables(
                in @_temporal_library_rule_id   bigint,
                in @_datetime_field             integer,
                out @_table_name                varchar(48)
                ) AS
BEGIN

exec seg01_log 'SEG01_get_relevant_event_tables'
commit

    DECLARE @min_datetime_type_str      varchar(36)
    DECLARE @max_datetime_type_str      varchar(36)
  --  DECLARE @_temporal_library_rule_id   bigint

   --this should be set via the @_datetime_field
    SET @min_datetime_type_str = 'min_viewed_datetime'
    SET @max_datetime_type_str = 'max_viewed_datetime'

--    SET @_temporal_library_rule_id = 5

    SET @_table_name = 'SEG01_event_tbls_'||dateformat(getdate(),'yyyymmddhhnnss')||'_'||@_temporal_library_rule_id||'tmp'


--exec seg01_log ' writing<'||@_table_name||'>'


    execute(' create table '||@_table_name||' ( '||
            ' uniqid bigint not null,           '||
            ' schema_name varchar(36) not null, '||
            ' table_name varchar(64) not null   )')

    commit


--exec seg01_log ' Error Status 0.1<'||@@error||'>'

DECLARE @xsql varchar(15000)

SET @xsql =' INSERT into '||@_table_name||'   '||
   ' select dense_rank() over(order by table_name) uniqid, ev_tbls.schema_name, table_name          '||
  -- '   into '||@_table_name||'                                                                      '||
   '   from SEG01_viewed_dp_event_table_summary_tbl ev_tbls, SEG01_temporal_library_tbl lib         '||
   '  where lib.uniqid = '||@_temporal_library_rule_id||
   '    and (period_start_datetime between '||@min_datetime_type_str||' and '||@max_datetime_type_str||
   '      or period_end_datetime   between '||@min_datetime_type_str||' and '||@max_datetime_type_str||')'

--select @xsql

execute(@xsql)

execute('commit')


END;


------- END Proc






commit;


--test

select top 1 *
from SEG01_event_tbls_20131016114012_8tmp










---- T E S T    -------------------------------->

DECLARE @_temporal_library_rule_id  bigint
DECLARE @_datetime_type_field       integer
DECLARE @_table_name                varchar(48)
DECLARE @current_id_max             bigint
DECLARE @current_id                 bigint
DECLARE @event_table_name           varchar(48)
DECLARE @schema_name                varchar(24)



SET @_temporal_library_rule_id = 8
SET @_datetime_type_field = 1
--SET @_table_name 'SEG01_event_tbl_list_tmp'  --change this depending on proc run

exec SEG01_get_relevant_event_tables @_temporal_library_rule_id,  @_datetime_type_field,  @_table_name

--select @_table_name

--iterate through

execute(' select @current_id_max = max(uniqid) '||
           'from '||@_table_name)

SET @current_id = 1

while @current_id <= @current_id_max

  BEGIN

      --get the event table details
      execute('select @event_table_name = table_name, '||
              '       @schema_name      = schema_name '||
              ' from '||@_table_name                   ||
              ' where uniqid = '||@current_id)

    select @schema_name, @event_table_name

/*
    --EXTRACT information from the table to build aggregate
    SET @sql_insert_str1 =
     ' INSERT INTO '||@insert_table||
     ' select e.pk_viewing_prog_instance_fact, '||@segmentid||' segmentid '||
     --'   from '||@schema_name||'.'||@table_name||
     '   from '||@schema_name||'.'||@event_table_name||' e, SEG01_root_temporal_tbl t '||
     '  where e.'||@col_name||' '||@operator||' '||@condition||
     '    and e.pk_viewing_prog_instance_fact = t.pk_viewing_prog_instance_fact '||
     '    and t.temporal_library_id = '||@_temporal_library_rule_id
*/




    SET @current_id = @current_id + 1

  END

execute(' drop table '||@_table_name)
execute(' commit ')



-----


select *
from SEG01_log_tbl


