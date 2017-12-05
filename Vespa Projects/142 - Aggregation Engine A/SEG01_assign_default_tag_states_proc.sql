


/******************************************************************
  *  Now - the dictionary is for handling categorical data, but
  *  what about continuous data where filtering can be acheived
  *  using <> or 'between' type functions?
  *
  *  Create SEG01_Segment_Filters_Tag_Types_tbl
  *  this allows for a drag & drop type approach to
  *  filtering, say on duration less than 6 secs.
  *****************************************************************/


/***********************************************************************************
 ** the sensible default for this should be varchar = discrete, int = continuous.
  * A sample should be taken of the data to find out how many attributes
  * belong to the variable. If for example the tag_type is an INT, but there are
  * only 2 attributes (0 & 1) then the variable is binary, so should be classified
  * as discrete, rather than continuous.
  **********************************************************************************/


--make sure SEG01_Segment_Dictionary_Tag_States_tbl is created before running... doing this outside a
--procedure, as the proc should update the table rather than completely re-write

CREATE OR REPLACE PROCEDURE SEG01_assign_default_tag_states( ) AS
BEGIN

    --make Global variable declarations
    DECLARE  @tag_uid       BIGINT
    DECLARE  @tag_type      VARCHAR(24)
    DECLARE  @max_rows      BIGINT

    IF object_id('SEG01_tag_type_tmp') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_tag_type_tmp
        END

    create table SEG01_tag_type_tmp(
        uniqid                 BIGINT          NOT NULL identity,
        tag_uid                BIGINT          NOT NULL,
        tag_type               VARCHAR(24)     NOT NULL)
    commit

    INSERT into SEG01_tag_type_tmp(tag_uid, tag_type)
      select uniqid, tag_type
        from SEG01_Segment_Dictionary_Tag_Types_tbl --@_schema_name||'.'||@_table_name
       --only update the records where the default is not yet set
       where uniqid not in (select tag_type_uid from SEG01_Segment_Dictionary_Tag_States_tbl)
    commit

    --get the max number of rows that we need to iterate through to load all the defaults
    SELECT @max_rows = max(uniqid)
      from SEG01_tag_type_tmp

    --possibly should truncate SEG01_Segment_Dictionary_Tag_States_tbl here, in prep for the new load that's about to happen

    --loadTagStatesTable:
    WHILE @max_rows > 0 --LOOP
        BEGIN

            --for each row load the tag_uid and tag_type that we need to allocate the default for
            SELECT @tag_uid = tag_uid,
                   @tag_type = tag_type
              from SEG01_tag_type_tmp
             where uniqid = @max_rows

            IF (1 = case when @tag_type like 'varchar%'   then 1
                         when @tag_type like 'char%'      then 1
                         when @tag_type like 'character%' then 1
                         when @tag_type like 'binary%'    then 1
                         when @tag_type like 'varbinary%' then 1
                         else 0 end) --if a character based field, then probably a discrete series attribute
                BEGIN-- DISCRETE VARIABLE
                    insert into SEG01_Segment_Dictionary_Tag_States_tbl(tag_type_uid, variable_state)
                      values(@tag_uid, 'discrete')
                    commit
                END
            ELSE
                BEGIN -- CONTINUOUS VARIABLE
                    /**
                      *  ** For a future version **
                      *  We should add additional code here to determine if the variable is actually
                      *  continuous or not, just because it is an integer doesn't mean it is.
                      */
                    insert into SEG01_Segment_Dictionary_Tag_States_tbl(tag_type_uid, variable_state)
                      values(@tag_uid, 'continuous')
                    commit
                END

            SET @max_rows = @max_rows-1
        END --END LOOP

    drop table SEG01_tag_type_tmp
    commit

END;
