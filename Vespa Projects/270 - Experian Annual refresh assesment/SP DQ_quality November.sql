ALTER procedure pitteloudj.DQ_November_Checks(

  @run_id 	integer,
  @Owner	varchar(100),
  @Tablec 	varchar(100),
  @ColumnN 	integer,
  @Columnb 	varchar(200)) 
as
begin
  declare 
	  @sql2 varchar(1000),
	  @cont integer,
	  @sql1 varchar(1000),
	  @cont2 integer,
	  @errorck integer,
	  @CountResult integer,
	  @Null_flag bit, 
	  @Label varchar(256)
	  
  set @sql2 = 'INSERT INTO Experian_November_Columns_Results '
  set @sql2 = @sql2 || '(fowner, TableName, ColumnName, records, proc_reg, Content_Flag, Null_Flag, New_column, Deleted_col, Date_proc, run_id, Label) '
  set @sql2 = @sql2 || 'SELECT  '||@Owner||','||@Tablec  ||','||@Columnb ||'   ,count(1) hits    ,1    ,0    ,CASE WHEN hits = 0 THEN 1 ELSE 0 END'
  set @sql2 = @sql2 || ',0    ,0    ,getdate(), '||@run_id
  set @sql2 = @sql2 || ', ''' || @Columnb     || ''' FROM  ' || @Owner || '.' || @Tablec || ' GROUP BY ' || @Columnb     || ')'

  execute(@sql2)
end
commit work


