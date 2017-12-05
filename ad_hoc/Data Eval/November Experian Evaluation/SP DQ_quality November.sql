
create procedure pitteloudj.DQ_November_Checks(

  @run_id 	integer,
  @Owner	varchar(100),
  @Tablec 	varchar(100),
  @ColumnN 	varchar(200) ) 
as
begin
  declare @sql2 varchar(1000),
  @cont integer,
  @sql1 varchar(1000),
  @cont2 integer,
  @errorck integer,
  @CountResult integer,
  @Null_flag bit, 
  @Label varchar(256),
  set @sql2
     = '(INSERT INTO Experian_November_Columns_Results 
(	fowner	, TableName    , ColumnName    , records    , proc_reg    , Content_Flag    , Null_Flag    , New_column    , Deleted_col    , Date_proc    , run_id 	, Label ) 
SELECT 
     @Owner	,@Tablec    ,@ColumnN
    ,count(' || @ColumnN     || ') hits
    ,1    ,0    ,CASE WHEN hits = 0 THEN 1 ELSE 0 END
    ,0    ,0    ,getdate()
    ,@run_id	, ' || @ColumnN     || '
FROM ' || @Owner || '.' || @Tablec || ' 
GROUP BY ' || @ColumnN     || ')'
  execute(@sql2)
end
commit work


