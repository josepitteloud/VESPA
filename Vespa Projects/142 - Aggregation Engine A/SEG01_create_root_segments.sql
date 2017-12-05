
/*********************************************************************
 **
 **  Builds root segmentations according to the input parameter list
 **
 **  As most of these parameters will be related to the viewing data
 **   and change table source from one month to the next, a generic
 **   method for describing viewing data as a source is used
 **
 **  requires the following tables to be built:
 **       1. SEG01_root_temporal_build_schedule_tbl
 **       2. SEG01_root_build_schedule_tbl
 **       3. SEG01_temporal_library_tbl
 **       4. SEG01_Segment_Dictionary_Tag_Types_tbl
 **
 *********************************************************************/

CREATE or replace procedure SEG01_create_root_segments(
               -- in @_parameters                 varchar(48),
               -- in @_temporal_library_rule_id   bigint
                --in @_apply_temporal_rule_on   integer  --use,  1:viewed,  2:broadcast,  3:event
                ) AS
BEGIN


  /* Work through the list of root to build defined by:
     ------------------------------------
        select *
          from SEG01_root_temporal_build_schedule_tbl rtbs,
               SEG01_root_build_schedule_tbl rbs,
               SEG01_temporal_library_tbl tl
         where rtbs.root_schedule_id = rbs.uniqid
           and rtbs.temporal_id = tl.uniqid
  */


  -- **********
  --start process here -->

  exec seg01_log 'SEG01_create_root_segments<'||now()||'>'


  DECLARE @insert_table           varchar(42)
--  DECLARE @insert_table_tmp       varchar(42)
  DECLARE @insert_desc_table      varchar(42)
  --DECLARE @audit_table          varchar(24)
  DECLARE @segmentid              bigint
  DECLARE @tagid                  bigint
  DECLARE @tag_name               varchar(255)
  DECLARE @schema_name            varchar(24)
  DECLARE @table_name             varchar(24)
  DECLARE @col_name               varchar(42)
  DECLARE @operator               varchar(6)
  DECLARE @condition              varchar(12)
  DECLARE @tag_type               varchar(24)
  DECLARE @temporal_id            bigint
  DECLARE @sql_insert_str1        varchar(500)
  DECLARE @sql_insert_str2        varchar(254)
  DECLARE @max_segment_build_number bigint
  DECLARE @current_uniqid         bigint
  DECLARE @current_id             bigint


 -- DECLARE @schema_name              varchar(24)
 -- DECLARE @table_name               varchar(24)
  DECLARE @min_datetime_type_str      varchar(36)
  DECLARE @max_datetime_type_str      varchar(36)
  DECLARE @current_id_max             bigint
  DECLARE @event_table_name           varchar(52)
  --DECLARE @schema_name              varchar(36)


  --for root segmentations use the root table
  SET @insert_table = 'SEG01_root_segment_tbl'
 -- SET @insert_table_tmp = 'SEG01_root_segment_tmp' --will include duplicates

  SET @insert_desc_table = 'SEG01_root_segment_desc_tbl'



--exec seg01_log 1


  --create list of scheduled root segmentations to build
  -- first, build the table to hold the list
  IF object_id('seg01_build_list_tmp') IS NOT NULL
    BEGIN
     DROP TABLE seg01_build_list_tmp
   END

  CREATE TABLE seg01_build_list_tmp (
   uniqid                 BIGINT          NOT NULL identity,
   tag_id                 BIGINT          NOT NULL,
   tag_name               VARCHAR(255)    NOT NULL,
   schema_name            VARCHAR(24)     NOT NULL,
   table_name             VARCHAR(24)     NOT NULL,
   col_name               VARCHAR(42)     NOT NULL,
   operator               VARCHAR(12)     NOT NULL,
   condition              VARCHAR(124)    NOT NULL,
   tag_type               VARCHAR(12)     NOT NULL,
   temporal_id            BIGINT          NOT NULL
  )

--exec seg01_log 2

  --CREATE root-segment build list
  INSERT INTO seg01_build_list_tmp (tag_id, tag_name, schema_name, table_name, col_name, operator, condition, tag_type, temporal_id)
  -- so we need to cycle through this table and make some root segmentations
       select t.uniqid,
              t.tag_name,
              t.schema_name,
              t.table_name,
              t.col_name,
              rbs.operator,
              rbs.condition,
              t.tag_type,
              tl.uniqid
        from SEG01_root_temporal_build_schedule_tbl rtbs,
             SEG01_root_build_schedule_tbl rbs,
             SEG01_temporal_library_tbl tl,
             SEG01_Segment_Dictionary_Tag_Types_tbl t
       where rtbs.root_schedule_id = rbs.uniqid
         and rtbs.temporal_id = tl.uniqid
         and rbs.attribute = t.tag_name
          --,SEG01_Tag_Self_Aware_tbl a

      --could take out the following two lines - just to make it less restrictive for now
      --and t.tag_name *= a.tag_name --outer join on self-aware info
      --and lower(a.aware_type) = 'root'
  commit



  SELECT @max_segment_build_number = max(uniqid)
                                     from seg01_build_list_tmp
  SET @current_uniqid = 1


--exec seg01_log 4


  while @current_uniqid <= @max_segment_build_number
    BEGIN

      -- handle the allocation of segment IDs  ** -->    get a segment_id
      exec SEG01_assign_segmentid @segmentid


      --exec seg01_log '4.1'


      SELECT @tagid = tag_id,
             @tag_name = tag_name,
             @schema_name = schema_name,
             @table_name = table_name,
             @col_name = col_name,
             @operator = operator,
             @condition = condition,
             @tag_type = tag_type,
             @temporal_id = temporal_id
        from seg01_build_list_tmp
       where uniqid = @current_uniqid


      --exec seg01_log '4.2'


      --SET @sql_insert_str2 = 'INSERT INTO '||@insert_desc_table||' (segment_id, schema_name, table_name, col_name, operator, condition) values(@segmentid, @schema_name, @table_name, @col_name, @operator, @condition)'
      SET @sql_insert_str2 = 'INSERT INTO '||@insert_desc_table||' (segment_id, tag_id, tag_name, operator, condition) values(@segmentid, @tagid, @tag_name, @operator, @condition)'

--      SET @sql_insert_str2 = replace(@sql_insert_str2, '@temporal_id'  ,    @temporal_id     )
      SET @sql_insert_str2 = replace(@sql_insert_str2, '@segmentid'  ,      @segmentid       )
      SET @sql_insert_str2 = replace(@sql_insert_str2, '@tagid'      ,      @tagid           )
      SET @sql_insert_str2 = replace(@sql_insert_str2, '@tag_name'   , '"'||@tag_name   ||'"')
      --SET @sql_insert_str2 = replace(@sql_insert_str2, '@schema_name', '"'||@schema_name||'"')
      --SET @sql_insert_str2 = replace(@sql_insert_str2, '@table_name' , '"'||@table_name ||'"')
      --SET @sql_insert_str2 = replace(@sql_insert_str2, '@col_name'   , '"'||@col_name   ||'"')
      SET @sql_insert_str2 = replace(@sql_insert_str2, '@operator'   , '"'||@operator   ||'"')
      SET @sql_insert_str2 = replace(@sql_insert_str2, '@condition'  , '"'||@condition  ||'"')



      --1st step (format the condition string)
      IF lower(@tag_type) like 'varchar%'
        BEGIN
          SET @condition = '"'||@condition||'"'
        END

        --exec seg01_log @condition




        /****************
         *  Handle the temporal aspect to extract the pk_viewing_prog_instance_fact's
         *  relevant to these aggregation tags & period
         ****************/

        --1. Extract/identify pk_viewing_prog_instance_fact's that are from the temporal period
        --2. Identify event tables that are required




        /* Need to fill a table with all the pk_viewing_prog_instance_facts
         *  1. join this query to the temporal pk_viewing_prog_instance_fact's
         *     to restrict aggregation
         *  2. we can iterate through each event table - inserting instance from each one
         *  3. ultimately this bit will have to work out which tables to join
         */

        -- get list of tables we need to iterate through for the period
        --let's assume we are going to use viewed_time for now...


        SET @min_datetime_type_str = 'min_viewed_datetime'
        SET @max_datetime_type_str = 'max_viewed_datetime'


        --exec seg01_log '4.3'


        --work out which events tables we need for the period
        execute(
           ' select dense_rank() over(order by table_name) uniqid, ev_tbls.schema_name, table_name          '||
           '   into #SEG01_event_tables_tmp2                                                                '||
           '   from SEG01_viewed_dp_event_table_summary_tbl ev_tbls, SEG01_temporal_library_tbl lib         '||
           '  where lib.uniqid = '||@temporal_id||
           '    and (period_start_datetime between '||@min_datetime_type_str||' and '||@max_datetime_type_str||
           '      or period_end_datetime   between '||@min_datetime_type_str||' and '||@max_datetime_type_str||')')


        select @current_id_max = max(uniqid)
          from #SEG01_event_tables_tmp2


        --exec seg01_log '4.4'

        --start extraction loop here -->
        SET @current_id = 1

        while @current_id <= @current_id_max
          BEGIN
          --exec seg01_log 5

          --get the NEXT event table details
          select @event_table_name = table_name,
                 @schema_name      = schema_name
            from #SEG01_event_tables_tmp2
           where uniqid = @current_id

          --exec seg01_log 6




          /******************************************************
           *  This 'sql_insert_str1' should be converted
           *  so filtering by account is possible
           ******************************************************/


          --2nd step: Insert records into the root_segment_tbl
          SET @sql_insert_str1 =
                ' INSERT INTO '||@insert_table||
                ' select e.pk_viewing_prog_instance_fact, '||@segmentid||' segmentid '||
                --'   from '||@schema_name||'.'||@table_name||
                '   from '||@schema_name||'.'||@event_table_name||' e, SEG01_root_temporal_tbl t '||
                '  where e.'||@col_name||' '||@operator||' '||@condition||
                '    and e.pk_viewing_prog_instance_fact = t.pk_viewing_prog_instance_fact '||
                '    and t.temporal_library_id = '||@temporal_id||
                ' group by e.pk_viewing_prog_instance_fact, segmentid ' --helps remove dups

          --log SQL
          exec seg01_log substring(@sql_insert_str1, 0, 100)
          exec seg01_log substring(@sql_insert_str1, 100, 100)
          exec seg01_log substring(@sql_insert_str1, 200, 100)
          exec seg01_log substring(@sql_insert_str1, 300, 100)


          EXECUTE(replace(@sql_insert_str1,'"','''')) -- replace the " at this point
          commit





          --scan for duplicates









          ---exec seg01_log ' values('||@segmentid||','||@schema_name||','||@table_name||','||@col_name||','||@operator||','||@condition||')'


          SET @current_id = @current_id + 1

          END

        --execute statements and then commit
        EXECUTE(replace(@sql_insert_str2,'"',''''))

        --exec seg01_log 8
        commit

        SET @current_uniqid = @current_uniqid + 1

    END

    --add an index if it doesn't exist to the table
    --SEG01_root_segment_tbl    pk_viewing_prog_instance_fact

END;



------------------------------proc ends here
----------------------------------------------

---test

/*

--for top 1000 records for the last segment_id created
select top 1000 *
from SEG01_root_segment_tbl
where segment_id = (select max(segment_id) from SEG01_root_segment_tbl)


select *
from SEG01_root_segment_desc_tbl


--both together
select top 1000 *
from SEG01_root_segment_tbl r,
     SEG01_root_segment_desc_tbl d
where r.segment_id = d.segment_id
  and r.segment_id = 100


select *
from seg01_log_tbl


*/

--- end test

---------------------- old code



