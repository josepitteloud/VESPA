create procedure kinnairt.DQ_Basic_Checks_adsmart( 
  @table_name varchar(200),
  @column_name varchar(200),
  @creator varchar(200),
  @check_type varchar(200),
  @dq_run_id integer,
  @target_date date= null,
  @CP2_build_ID bigint= null,
  @dq_check_detail_id integer ) 
as
declare @metric_benchmark decimal(16,3)
declare @metric_tolerance_amber decimal(16,3)
declare @metric_tolerance_red decimal(16,3)
declare @metric_rag varchar(5)
declare @exception_value varchar(255)
declare @notnull_col_checks varchar(1000)
declare @match_check_table_name varchar(200)
declare @notnull_col_checks_loop varchar(8000)
declare @table_sql_where_clause varchar(2000)
declare @var_sql varchar(8000)
declare @count_isnull integer
declare @col_type_check integer
declare @max_length_chk integer
declare @dist_cnt_chk integer
declare @unknown_chk integer
declare @COL_TYPE_CHK integer
declare @fk_dq_col_id integer
declare @pk_result integer
declare @dq_det_id integer
declare @res_final varchar(200)
declare @column_type varchar(30)
declare @unknown_value varchar(20)
declare @table_row_count integer
declare @column_row_count integer
declare @min_value varchar(255)
declare @max_value varchar(255)
declare @var_sql_key varchar(50)
declare @pass_fail varchar(4)
declare @fk_diff_count integer
declare @fk_table_name varchar(200)
declare @fk_column_name varchar(200)
declare @fk_creator varchar(200)
declare @load_date datetime
declare @expected_value varchar(40)
declare @dq_check_detail_id bigint
declare @cb_match_value integer
declare @var_sql_null varchar(2000)
begin
  set @load_date = getdate()
  select unknown_value=dq_chk_det.unknown_value,dq_det_id=dq_chk_det.dq_check_detail_id,
    expected_value=dq_chk_det.expected_value,column_type=dq_col.column_type,dq_run_id=dq_run_type.dq_run_id,
    metric_benchmark=dq_chk_det.metric_benchmark,metric_tolerance_amber=dq_chk_det.metric_tolerance_amber,
    metric_tolerance_red=dq_chk_det.metric_tolerance_red
    into #tmp_dq_values
    from data_quality_check_type as dq_chk_type
      ,data_quality_columns as dq_col
      ,data_quality_check_details as dq_chk_det
      ,data_quality_run_group as dq_run_type
    where dq_chk_det.dq_col_Id = dq_col.dq_col_Id
    and dq_chk_det.dq_check_type_id = dq_chk_type.dq_check_type_id
    and dq_chk_det.dq_sched_run_id = dq_run_type.dq_run_id
    and UPPER(dq_chk_type.dq_check_type) = UPPER(@check_type)
    and UPPER(dq_col.creator) = UPPER(@creator)
    and UPPER(dq_col.table_name) = UPPER(@table_name)
    and UPPER(dq_col.column_name) = UPPER(@column_name)
    and DQ_RUN_TYPE.dq_run_id = @dq_run_id
    and dq_chk_det.dq_check_detail_id = @dq_check_detail_id
  select exception_value=dq_chk_det.exception_value,notnull_col_checks=dq_chk_det.notnull_col_checks,
    sql_where_clause=dq_chk_det.sql_where_clause,fk_dq_col_id=dq_chk_det.fk_dq_col_id
    into #tmp_dq_values_2
    from data_quality_check_type as dq_chk_type
      ,data_quality_columns as dq_col
      ,data_quality_check_details as dq_chk_det
      ,data_quality_run_group as dq_run_type
    where dq_chk_det.dq_col_Id = dq_col.dq_col_Id
    and dq_chk_det.dq_check_type_id = dq_chk_type.dq_check_type_id
    and dq_chk_det.dq_sched_run_id = dq_run_type.dq_run_id
    and UPPER(dq_chk_type.dq_check_type) = UPPER(@check_type)
    and UPPER(dq_col.creator) = UPPER(@creator)
    and UPPER(dq_col.table_name) = UPPER(@table_name)
    and UPPER(dq_col.column_name) = UPPER(@column_name)
    and DQ_RUN_TYPE.dq_run_id = @dq_run_id
    and dq_chk_det.dq_check_detail_id = @dq_check_detail_id
  set @dq_det_id = (select dq_det_id from #tmp_dq_values)
  set @unknown_value = (select unknown_value from #tmp_dq_values)
  set @column_type = (select column_type from #tmp_dq_values)
  set @expected_value = (select expected_value from #tmp_dq_values)
  set @metric_benchmark = (select metric_benchmark from #tmp_dq_values)
  set @metric_tolerance_amber = (select metric_tolerance_amber from #tmp_dq_values)
  set @metric_tolerance_red = (select metric_tolerance_red from #tmp_dq_values)
  set @notnull_col_checks = (select notnull_col_checks from #tmp_dq_values_2)
  set @table_sql_where_clause = (select sql_where_clause from #tmp_dq_values_2)
  set @exception_value = (select exception_value from #tmp_dq_values_2)
  set @fk_dq_col_id = (select fk_dq_col_id from #tmp_dq_values_2)
  if @fk_dq_col_id is not null
    begin
      select creator,table_name,column_name
        into #tmp_foreign_cols
        from data_quality_columns
        where dq_col_id = @fk_dq_col_id
      set @fk_table_name = (select table_name from #tmp_foreign_cols)
      set @fk_creator = (select creator from #tmp_foreign_cols)
      set @fk_column_name = (select column_name from #tmp_foreign_cols)
    end
  if @check_type = 'ISNULL_CHECK'
    begin
      if @table_sql_where_clause is null
        begin
          set @var_sql = 'select count(1) INTO @count_isnull from ' || @creator || '.' || @table_name || ' where ' || @column_name || ' is null'
        end
      if @table_sql_where_clause is not null
        begin
          set @var_sql = 'select count(1) INTO @count_isnull from ' || @creator || '.' || @table_name || ' ' || @table_sql_where_clause || ' and ' || @column_name || ' is null'
        end
      execute(@var_sql)
      commit work
      set @metric_rag = metric_benchmark_check(@count_isnull,@metric_benchmark,@metric_tolerance_amber,@metric_tolerance_red)
      commit work
      execute logger_add_event @CP2_build_ID,3,'DQ ISNULL check for ' || @table_name || '.' || @column_name || ''
      insert into data_quality_results
        ( dq_check_detail_id,dq_run_id,result,rag_status,sql_processed,load_timestamp,logger_id,data_date ) 
        values( @DQ_DET_ID,@dq_run_id,@count_isnull,@metric_rag,@var_sql,@load_date,@CP2_build_ID,@target_date ) 
      commit work
    end
  if @check_type = 'MAX_LENGTH_CHECK'
    begin
      if(UPPER(@column_type) like '%CHAR%' and @table_sql_where_clause is null)
        begin
          set @var_sql = 'select max(' || @column_name || ') INTO @max_length_chk from ' || @creator || '.' || @table_name || ''
          execute(@var_sql)
          commit work
        end
      if(UPPER(@column_type) like '%CHAR%' and @table_sql_where_clause is not null)
        begin
          set @var_sql = 'select max(' || @column_name || ') INTO @max_length_chk from ' || @creator || '.' || @table_name || ' ' || @table_sql_where_clause || ''
          execute(@var_sql)
          commit work
        end
      if isnull(@var_sql,'<NULL>') = '<NULL>'
        begin
          set @var_sql = 'column type not appropriate for processing'
        end
      set @metric_rag = metric_benchmark_check(@max_length_chk,@metric_benchmark,@metric_tolerance_amber,@metric_tolerance_red)
      commit work
      execute logger_add_event @CP2_build_ID,3,'DQ Max Length Chk for ' || @table_name || '.' || @column_name || ''
      insert into data_quality_results
        ( dq_check_detail_id,dq_run_id,result,rag_status,sql_processed,load_timestamp,logger_id,data_date ) 
        values( @DQ_DET_ID,@dq_run_id,@max_length_chk,@metric_rag,@var_sql,@load_date,@CP2_build_ID,@target_date ) 
      commit work
    end
  if @check_type = 'DISTINCT_COUNT_CHECK'
    begin
      if @table_sql_where_clause is null
        begin
          set @var_sql = 'select count (distinct ' || @column_name || ') INTO @dist_cnt_chk from ' || @creator || '.' || @table_name || ''
        end
      if @table_sql_where_clause is not null
        begin
          set @var_sql = 'select count (distinct ' || @column_name || ') INTO @dist_cnt_chk from ' || @creator || '.' || @table_name || ' ' || @table_sql_where_clause || ''
        end
      execute(@var_sql)
      commit work
      set @metric_rag = metric_benchmark_check(@dist_cnt_chk,@metric_benchmark,@metric_tolerance_amber,@metric_tolerance_red)
      commit work
      execute logger_add_event @CP2_build_ID,3,'DQ Distinct Cnt for ' || @table_name || '.' || @column_name || ''
      insert into data_quality_results
        ( dq_check_detail_id,dq_run_id,result,rag_status,sql_processed,load_timestamp,logger_id,data_date ) 
        values( @DQ_DET_ID,@dq_run_id,@dist_cnt_chk,@metric_rag,@var_sql,@load_date,@CP2_build_ID,@target_date ) 
      commit work
    end
  if @check_type = 'UNKNOWN_CHECK'
    begin
      execute logger_add_event @CP2_build_ID,3,'UNKNOWN CHECK for ' || @table_name || '.' || @column_name || '.' || @unknown_value || ''
      if(isnumeric(@unknown_value) = 1 and @table_sql_where_clause is null and lower(@column_type) <> 'varchar')
        begin
          set @var_sql = 'select count (1) INTO @unknown_chk from ' || @creator || '.' || @table_name || ' WHERE ' || @column_name || ' = convert(int, (' || @unknown_value || '))'
          commit work
        end
      if(isnumeric(@unknown_value) = 1 and @table_sql_where_clause is null and lower(@column_type) = 'varchar')
        begin
          set @var_sql = 'select count (1) INTO @unknown_chk from ' || @creator || '.' || @table_name || ' WHERE ' || @column_name || ' = (''' || @unknown_value || ''')'
          commit work
        end
      if(isnumeric(@unknown_value) = 1 and @table_sql_where_clause is not null and lower(@column_type) <> 'varchar')
        begin
          set @var_sql = 'select count (1) INTO @unknown_chk from ' || @creator || '.' || @table_name || ' ' || @table_sql_where_clause || ' and ' || @column_name || ' = convert(int, (' || @unknown_value || '))'
          commit work
        end
      if(isnumeric(@unknown_value) = 1 and @table_sql_where_clause is not null and lower(@column_type) <> 'varchar')
        begin
          set @var_sql = 'select count (1) INTO @unknown_chk from ' || @creator || '.' || @table_name || ' ' || @table_sql_where_clause || ' and ' || @column_name || ' = (''' || @unknown_value || ''')'
          commit work
        end
      if(isnumeric(@unknown_value) = 0 and @table_sql_where_clause is null)
        begin
          set @var_sql = 'select count (1) INTO @unknown_chk from ' || @creator || '.' || @table_name || ' WHERE UPPER(' || @column_name || ') = UPPER(''' || @unknown_value || ''')'
        end
      if(isnumeric(@unknown_value) = 0 and @table_sql_where_clause is not null)
        begin
          set @var_sql = 'select count (1) INTO @unknown_chk from ' || @creator || '.' || @table_name || ' ' || @table_sql_where_clause || ' and UPPER(' || @column_name || ') = UPPER(''' || @unknown_value || ''')'
        end
      execute(@var_sql)
      commit work
      insert into tst_int values( @unknown_chk ) 
      set @metric_rag = metric_benchmark_check(@unknown_chk,@metric_benchmark,@metric_tolerance_amber,@metric_tolerance_red)
      commit work
      insert into data_quality_results
        ( dq_check_detail_id,dq_run_id,result,rag_status,sql_processed,load_timestamp,logger_id,data_date ) 
        values( @DQ_DET_ID,@dq_run_id,@unknown_chk,@metric_rag,@var_sql,@load_date,@CP2_build_ID,@target_date ) 
      commit work
    end
  if @check_type = 'COLUMN_TYPE_LENGTH_CHECK'
    begin
      set @var_sql
         = 'select count (*) INTO @COL_TYPE_CHK from \x0A(select coltype, length from sys.syscolumns where upper(creator) = ''' || @creator
         || '''\x0AAND UPPER(TNAME) = ''' || @table_name || ''' AND UPPER(CNAME) = ''' || @column_name
         || '''\x0A UNION  SELECT COLUMN_TYPE, COLUMN_LENGTH FROM DATA_QUALITY_COLUMNS WHERE UPPER(CREATOR) = ''' || @creator
         || '''\x0A AND UPPER(TABLE_NAME) = ''' || @table_name || ''' AND UPPER(COLUMN_NAME) = ''' || @column_name || ''') T'
      execute(@var_sql)
      commit work
      set @metric_rag = metric_benchmark_check(@COL_TYPE_CHK,@metric_benchmark,@metric_tolerance_amber,@metric_tolerance_red)
      commit work
      execute logger_add_event @CP2_build_ID,3,'DQ Col Type Chk for ' || @table_name || '.' || @column_name || ''
      insert into data_quality_results
        ( dq_check_detail_id,dq_run_id,result,rag_status,sql_processed,load_timestamp,logger_id,data_date ) 
        values( @DQ_DET_ID,@dq_run_id,@COL_TYPE_CHK,@metric_rag,@var_sql,@load_date,@CP2_build_ID,@target_date ) 
      commit work
    end
  if @check_type = 'PRIMARY_KEY_CHECK'
    begin
      execute logger_add_event @CP2_build_ID,3,'DQ Primary Key Chk Start for ' || @table_name || '.' || @column_name || ''
      set @var_sql = 'select count (1) INTO @table_row_count from ' || @creator || '.' || @table_name || ' ' || @table_sql_where_clause || ''
      execute(@var_sql)
      commit work
      set @var_sql = 'select count (distinct ' || @column_name || ') INTO @column_row_count from ' || @creator || '.' || @table_name || ' ' || @table_sql_where_clause || ''
      execute(@var_sql)
      commit work
      set @pk_result = @table_row_count-@column_row_count
      set @metric_rag = metric_benchmark_check(@pk_result,@metric_benchmark,@metric_tolerance_amber,@metric_tolerance_red)
      commit work
      execute logger_add_event @CP2_build_ID,3,'DQ PK Chk for ' || @table_name || '.' || @column_name || ''
      insert into data_quality_results
        ( dq_check_detail_id,dq_run_id,result,rag_status,sql_processed,load_timestamp,logger_id,data_date ) 
        values( @DQ_DET_ID,@dq_run_id,@pk_result,@metric_rag,@var_sql,@load_date,@CP2_build_ID,@target_date ) 
      commit work
    end
  if @check_type = 'TABLE_COUNT_CHECK'
    begin
      set @var_sql = 'select count (1) INTO @table_row_count from ' || @creator || '.' || @table_name || ' ' || @table_sql_where_clause || ''
      execute(@var_sql)
      commit work
      set @metric_rag = metric_benchmark_check(@table_row_count,@metric_benchmark,@metric_tolerance_amber,@metric_tolerance_red)
      commit work
      execute logger_add_event @CP2_build_ID,3,'DQ TABLE_COUNT_CHECK Cnt for ' || @creator || '.' || @table_name || ''
      insert into data_quality_results
        ( dq_check_detail_id,dq_run_id,result,rag_status,sql_processed,load_timestamp,logger_id,data_date ) 
        values( @DQ_DET_ID,@dq_run_id,@table_row_count,@metric_rag,@var_sql,@load_date,@CP2_build_ID,@target_date ) 
      commit work
    end
  if @check_type = 'MIN_VALUE_CHECK'
    begin
      if @exception_value is null
        begin
          set @var_sql = 'select min(' || @column_name || ') INTO @min_value from ' || @creator || '.' || @table_name || ' ' || @table_sql_where_clause || ''
        end
      if @exception_value is not null
        begin
          if @table_sql_where_clause is null
            begin
              set @var_sql = 'select min(' || @column_name || ') INTO @min_value from ' || @creator || '.' || @table_name || ' where ' || @column_name || ' not in (''' || @exception_value || ''')'
            end
          if @table_sql_where_clause is not null
            begin
              set @var_sql = 'select min(' || @column_name || ') INTO @min_value from ' || @creator || '.' || @table_name || ' ' || @table_sql_where_clause || ' and ' || @column_name || ' not in (''' || @exception_value || ''')'
            end
        end
      execute(@var_sql)
      commit work
      commit work
      execute logger_add_event @CP2_build_ID,3,'DQ MIN_VALUE_CHECK Cnt for ' || @creator || '.' || @table_name || '.||@column_name||'
      insert into data_quality_results
        ( dq_check_detail_id,dq_run_id,result_text,rag_status,sql_processed,load_timestamp,logger_id,data_date ) 
        values( @DQ_DET_ID,@dq_run_id,@min_value,null,@var_sql,@load_date,@CP2_build_ID,@target_date ) 
      commit work
    end
  if @check_type = 'MAX_VALUE_CHECK'
    begin
      if @exception_value is null
        begin
          set @var_sql = 'select max(' || @column_name || ') INTO @max_value from ' || @creator || '.' || @table_name || ' ' || @table_sql_where_clause || ''
        end
      if @exception_value is not null
        begin
          if @table_sql_where_clause is null
            begin
              set @var_sql = 'select max(' || @column_name || ') INTO @max_value from ' || @creator || '.' || @table_name || ' where ' || @column_name || ' not in (''' || @exception_value || ''')'
            end
          if @table_sql_where_clause is not null
            begin
              set @var_sql = 'select max(' || @column_name || ') INTO @max_value from ' || @creator || '.' || @table_name || ' ' || @table_sql_where_clause || ' and ' || @column_name || ' not in (''' || @exception_value || ''')'
            end
        end
      execute(@var_sql)
      commit work
      execute logger_add_event @CP2_build_ID,3,'DQ MAX_VALUE_CHECK Cnt for ' || @creator || '.' || @table_name || '.' || @column_name || ''
      insert into data_quality_results
        ( dq_check_detail_id,dq_run_id,result_text,rag_status,sql_processed,load_timestamp,logger_id,data_date ) 
        values( @DQ_DET_ID,@dq_run_id,@max_value,null,@var_sql,@load_date,@CP2_build_ID,@target_date ) 
      commit work
    end
  if @check_type = 'FOREIGN_KEY_CHECK'
    begin
      execute logger_add_event @CP2_build_ID,3,'DQ FOREIGN KEY check Start for ' || @table_name || '.' || @column_name || ' AND ' || @fk_table_name || '.' || @fk_column_name || ''
      if @table_sql_where_clause is null
        begin
          set @var_sql
             = 'select distinct ' || @column_name || ' pk_col,1 col into #tmp_col_chk_1 from ' || @creator || '.' || @table_name || ''
          execute(@var_sql)
          set @var_sql
             = 'select distinct ' || @fk_column_name || ' fk_col,1 col into #tmp_col_chk_2 from ' || @fk_creator || '.' || @fk_table_name || ''
          execute(@var_sql)
          select total=count(1),col=1 into #tmp_col_chk_total
            from #tmp_col_chk_1 as a where not exists(select 1 from #tmp_col_chk_2 as b where a.pk_col = b.fk_col)
          set @fk_diff_count = (select total from #tmp_col_chk_total)
        end
      if @table_sql_where_clause is not null
        begin
          set @var_sql
             = 'select distinct ' || @column_name || ' pk_col,1 col into #tmp_col_chk_1 from ' || @creator || '.' || @table_name || ' ' || @table_sql_where_clause || ''
          execute(@var_sql)
          set @var_sql
             = 'select distinct ' || @fk_column_name || ' fk_col,1 col into #tmp_col_chk_2 from ' || @fk_creator || '.' || @fk_table_name || ' ' || @table_sql_where_clause || ''
          execute(@var_sql)
          select total=count(1),col=1 into #tmp_col_chk_total
            from #tmp_col_chk_1 as a where not exists(select 1 from #tmp_col_chk_2 as b where a.pk_col = b.fk_col)
          set @fk_diff_count = (select total from #tmp_col_chk_total)
        end
      set @metric_rag = metric_benchmark_check(@fk_diff_count,@metric_benchmark,@metric_tolerance_amber,@metric_tolerance_red)
      commit work
      execute logger_add_event @CP2_build_ID,3,'DQ FOREIGN KEY check end for ' || @table_name || '.' || @column_name || ' AND ' || @fk_table_name || '.' || @fk_column_name || ''
      insert into data_quality_results
        ( dq_check_detail_id,dq_run_id,result,rag_status,sql_processed,load_timestamp,logger_id,data_date ) 
        values( @DQ_DET_ID,@dq_run_id,@fk_diff_count,@metric_rag,@var_sql,@load_date,@CP2_build_ID,@target_date ) 
      commit work
    end
  if @check_type = 'MATCH_RATES_SKYBASE_CHECK'
    begin
      if lower(@column_name) = 'cb_key_individual'
        begin
          set @var_sql_key = 'individual_key'
        end
      if lower(@column_name) = 'cb_key_household'
        begin
          set @var_sql_key = 'household_key'
        end
      if lower(@column_name) = 'cb_address_postcode'
        begin
          set @var_sql_key = 'cb_address_postcode'
        end
      if @var_sql_key in( 'individual_key','household_key' ) 
        begin
          if @notnull_col_checks is null
            begin
              set @var_sql = 'select count(distinct e.' || @column_name
                 || ') INTO @cb_match_value from Data_Quality_Match_Rates_Skybase as s \x0A                inner join ' || @creator || '.' || @table_name || ' as e on e.' || @column_name || ' = s.' || @var_sql_key || ''
            end
          if @notnull_col_checks is not null and charindex(',',@notnull_col_checks) = 0
            begin
              set @var_sql_null = 'where e.' || @notnull_col_checks || ' is not null '
              set @var_sql = 'select count(distinct e.' || @column_name
                 || ') INTO @cb_match_value from Data_Quality_Match_Rates_Skybase as s \x0A                inner join ' || @creator || '.' || @table_name || ' as e on e.' || @column_name || ' = s.' || @var_sql_key || ' ' || @var_sql_null || ''
            end
          if @notnull_col_checks is not null and charindex(',',@notnull_col_checks) > 0
            begin
              set @notnull_col_checks_loop = @notnull_col_checks
              set @var_sql_null = ' where '
              commit work
              while charindex(',',@notnull_col_checks_loop) > 0
                begin
                  set @var_sql = 'select count(distinct e.' || @column_name
                     || ') INTO @cb_match_value from Data_Quality_Match_Rates_Skybase as s \x0A                inner join ' || @creator || '.' || @table_name || ' as e on e.' || @column_name || ' = s.' || @var_sql_key || ''
                  set @var_sql_null = @var_sql_null+'e.'+' '+substring(@notnull_col_checks_loop,1,charindex(',',@notnull_col_checks_loop)-1)+' is not null and '
                  set @notnull_col_checks_loop = substring(@notnull_col_checks_loop,charindex(',',@notnull_col_checks_loop)+1)
                end
              set @var_sql_null = @var_sql_null+' e.'+@notnull_col_checks_loop+' is not null'
              set @var_sql = @var_sql+@var_sql_null
            end
          execute(@var_sql)
        end
      if @var_sql_key = 'cb_address_postcode'
        begin
          if @notnull_col_checks is null
            begin
              set @var_sql = 'select count(distinct e.' || @column_name
                 || ') INTO @cb_match_value from Data_Quality_Match_Rates_SkyPostcode as s \x0Ainner join ' || @creator || '.' || @table_name || ' as e on trim(replace(e.' || @column_name || ','' '','''')) = s.' || @var_sql_key || ''
            end
          if @notnull_col_checks is not null and charindex(',',@notnull_col_checks) = 0
            begin
              set @var_sql_null = 'where e.' || @notnull_col_checks || ' is not null '
              set @var_sql = 'select count(distinct e.' || @column_name
                 || ') INTO @cb_match_value from Data_Quality_Match_Rates_SkyPostcode as s \x0A                inner join ' || @creator || '.' || @table_name || ' as e on trim(replace(e.' || @column_name || ','' '','''')) = s.' || @var_sql_key || ' ' || @var_sql_null || ''
            end
          if @notnull_col_checks is not null and charindex(',',@notnull_col_checks) > 0
            begin
              set @notnull_col_checks_loop = @notnull_col_checks
              set @var_sql_null = ' where '
              commit work
              while charindex(',',@notnull_col_checks_loop) > 0
                begin
                  set @var_sql = 'select count(distinct e.' || @column_name
                     || ') INTO @cb_match_value from Data_Quality_Match_Rates_SkyPostcode as s \x0A                inner join ' || @creator || '.' || @table_name || ' as e on trim(replace(e.' || @column_name || ','' '','''')) = s.' || @var_sql_key || ''
                  set @var_sql_null = @var_sql_null+'e.'+' '+substring(@notnull_col_checks_loop,1,charindex(',',@notnull_col_checks_loop)-1)+' is not null and '
                  set @notnull_col_checks_loop = substring(@notnull_col_checks_loop,charindex(',',@notnull_col_checks_loop)+1)
                end
              set @var_sql_null = @var_sql_null+' e.'+@notnull_col_checks_loop+' is not null'
              set @var_sql = @var_sql+@var_sql_null
            end
          execute(@var_sql)
        end
      set @metric_rag = metric_benchmark_check(@cb_match_value,@metric_benchmark,@metric_tolerance_amber,@metric_tolerance_red)
      commit work
      execute logger_add_event @CP2_build_ID,3,'DQ SkyBase Match Check for ' || @table_name || '.' || @column_name || ''
      insert into data_quality_results
        ( dq_check_detail_id,dq_run_id,result,rag_status,sql_processed,load_timestamp,logger_id,data_date ) 
        values( @DQ_DET_ID,@dq_run_id,@cb_match_value,@metric_rag,@var_sql,@load_date,@CP2_build_ID,@target_date ) 
      commit work
    end
  if @check_type = 'MATCH_RATES_VESPA_CHECK'
    begin
      if lower(@column_name) = 'cb_key_individual'
        begin
          set @var_sql_key = 'individual_key'
        end
      if lower(@column_name) = 'cb_key_household'
        begin
          set @var_sql_key = 'household_key'
        end
      if lower(@column_name) = 'cb_address_postcode'
        begin
          set @var_sql_key = 'cb_address_postcode'
        end
      if @var_sql_key in( 'individual_key','household_key' ) 
        begin
          if @notnull_col_checks is null
            begin
              set @var_sql = 'select count(distinct e.' || @column_name
                 || ') INTO @cb_match_value from Data_Quality_Match_Rates_Vespa as s \x0A                inner join ' || @creator || '.' || @table_name || ' as e on e.' || @column_name || ' = s.' || @var_sql_key || ''
            end
          if @notnull_col_checks is not null and charindex(',',@notnull_col_checks) = 0
            begin
              set @var_sql_null = 'where e.' || @notnull_col_checks || ' is not null '
              set @var_sql = 'select count(distinct e.' || @column_name
                 || ') INTO @cb_match_value from Data_Quality_Match_Rates_Vespa as s \x0A                inner join ' || @creator || '.' || @table_name || ' as e on e.' || @column_name || ' = s.' || @var_sql_key || ' ' || @var_sql_null || ''
            end
          if @notnull_col_checks is not null and charindex(',',@notnull_col_checks) > 0
            begin
              set @notnull_col_checks_loop = @notnull_col_checks
              set @var_sql_null = ' where '
              commit work
              while charindex(',',@notnull_col_checks_loop) > 0
                begin
                  set @var_sql = 'select count(distinct e.' || @column_name
                     || ') INTO @cb_match_value from Data_Quality_Match_Rates_Vespa as s \x0A                inner join ' || @creator || '.' || @table_name || ' as e on e.' || @column_name || ' = s.' || @var_sql_key || ''
                  set @var_sql_null = @var_sql_null+'e.'+' '+substring(@notnull_col_checks_loop,1,charindex(',',@notnull_col_checks_loop)-1)+' is not null and '
                  set @notnull_col_checks_loop = substring(@notnull_col_checks_loop,charindex(',',@notnull_col_checks_loop)+1)
                end
              set @var_sql_null = @var_sql_null+' e.'+@notnull_col_checks_loop+' is not null'
              set @var_sql = @var_sql+@var_sql_null
            end
          execute(@var_sql)
        end
      if @var_sql_key = 'cb_address_postcode'
        begin
          if @notnull_col_checks is null
            begin
              set @var_sql = 'select count(distinct e.' || @column_name
                 || ') INTO @cb_match_value from Data_Quality_Match_Rates_VespaPostcode as s \x0A                inner join ' || @creator || '.' || @table_name || ' as e on trim(replace(e.' || @column_name || ','' '','''')) = s.' || @var_sql_key || ''
            end
          if @notnull_col_checks is not null and charindex(',',@notnull_col_checks) = 0
            begin
              set @var_sql_null = 'where e.' || @notnull_col_checks || ' is not null '
              set @var_sql = 'select count(distinct e.' || @column_name
                 || ') INTO @cb_match_value from Data_Quality_Match_Rates_VespaPostcode as s \x0A                inner join ' || @creator || '.' || @table_name || ' as e on trim(replace(e.' || @column_name || ','' '','''')) = s.' || @var_sql_key || ' ' || @var_sql_null || ''
            end
          if @notnull_col_checks is not null and charindex(',',@notnull_col_checks) > 0
            begin
              set @notnull_col_checks_loop = @notnull_col_checks
              set @var_sql_null = ' where '
              commit work
              while charindex(',',@notnull_col_checks_loop) > 0
                begin
                  set @var_sql = 'select count(distinct e.' || @column_name
                     || ') INTO @cb_match_value from Data_Quality_Match_Rates_VespaPostcode as s \x0A                inner join ' || @creator || '.' || @table_name || ' as e on trim(replace(e.' || @column_name || ','' '','''')) = s.' || @var_sql_key || ''
                  set @var_sql_null = @var_sql_null+'e.'+' '+substring(@notnull_col_checks_loop,1,charindex(',',@notnull_col_checks_loop)-1)+' is not null and '
                  set @notnull_col_checks_loop = substring(@notnull_col_checks_loop,charindex(',',@notnull_col_checks_loop)+1)
                end
              set @var_sql_null = @var_sql_null+' e.'+@notnull_col_checks_loop+' is not null'
              set @var_sql = @var_sql+@var_sql_null
            end
          execute(@var_sql)
        end
      set @metric_rag = metric_benchmark_check(@cb_match_value,@metric_benchmark,@metric_tolerance_amber,@metric_tolerance_red)
      commit work
      execute logger_add_event @CP2_build_ID,3,'DQ Vespa Match Check for ' || @table_name || '.' || @column_name || ''
      insert into data_quality_results
        ( dq_check_detail_id,dq_run_id,result,rag_status,sql_processed,load_timestamp,logger_id,data_date ) 
        values( @DQ_DET_ID,@dq_run_id,@cb_match_value,@metric_rag,@var_sql,@load_date,@CP2_build_ID,@target_date ) 
      commit work
    end
  if @check_type = 'TABLE_COLUMNS_COUNT_CHECK'
    begin
      set @var_sql = 'select count (' || @column_name || ') INTO @column_row_count from sys.syscolumns where upper(creator) = upper(''' || @creator || ''') and upper(tname) = upper(''' || @table_name || ''')'
      execute(@var_sql)
      commit work
      set @metric_rag = metric_benchmark_check(@column_row_count,@metric_benchmark,@metric_tolerance_amber,@metric_tolerance_red)
      commit work
      execute logger_add_event @CP2_build_ID,3,'DQ Table COlumn Count Check for ' || @table_name || '.' || @column_name || ''
      insert into data_quality_results
        ( dq_check_detail_id,dq_run_id,result,rag_status,sql_processed,load_timestamp,logger_id,data_date ) 
        values( @DQ_DET_ID,@dq_run_id,@column_row_count,@metric_rag,@var_sql,@load_date,@CP2_build_ID,@target_date ) 
      commit work
    end
end