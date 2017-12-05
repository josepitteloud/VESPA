
/***************************************************************
 **
 **  This proc does two things:
 **    1) extracts the data type of the column into a database table
 **       SEG01_Segment_Dictionary_Tag_Types_tbl
 **    2) saves all the permutations that exist within the column into
 **       a table, so they can be used for possible input selections to
 **       the Segmentation Engine
 **
 **  as input we want:
 **             database, schema, table_name, column_name
 **
 ***************************************************************/
CREATE OR REPLACE PROCEDURE SEG01_define_segment(
                in @_database_name          VARCHAR(24),
                in @_schema_name            VARCHAR(24),
                in @_table_name             VARCHAR(48),
                in @_table_proxy             VARCHAR(48), --new field can be NULL, use '%event%' if an event table
                in @_col_name               VARCHAR(42)
                ) AS
BEGIN
       DECLARE @tag_name             VARCHAR(255)
       DECLARE @tag_uid              BIGINT
       DECLARE @tag_uid_available    BIGINT
       DECLARE @tag_value_available  VARCHAR(150)
       DECLARE @tag_value            VARCHAR(255)
       DECLARE @tag_value_uid        BIGINT
       DECLARE @tag_value_type       VARCHAR(32)
       DECLARE @source_table_name    VARCHAR(64)
       DECLARE @source_schema_name   VARCHAR(64)

exec seg01_log 'SEG01_define_segment<'||now()||'>'


       SET @source_schema_name = @_schema_name
       SET @source_table_name  = @_table_name


       --look to see where we need to get the event data from
       IF @_table_proxy = '%event%'
           BEGIN
              exec SEG01_get_latest_event_table_name_from_proxy @source_schema_name, @source_table_name
           END


       --if proxy is not null then use table_name
       SELECT @tag_name = tag_name
         from SEG01_Segment_Dictionary_Tag_Types_tbl
        where tag_name =  @_database_name||'.'||@_schema_name||'.'||@source_table_name||'.'||@_col_name


       IF @tag_name is null
          BEGIN
                --INSERT NEW @_tag_name

                --GO and get the type of this field from the database
                select @tag_value_type = case when lower(coltype) in ('varchar','char','character','binary','varbinary') then lower(coltype)||'('||length||')'
                                              when lower(coltype) in ('float','decimal','numeric','double') then lower(coltype)||'('||length||','||syslength||')'
                                              when lower(coltype) in ('bigint','timestamp','date','datetime','smalldatetime','time','int','integer','tinyint','smallint','bit') then lower(coltype)
                                              else NULL
                                          END
                  from sys.syscolumns a
                 where creator =  @source_schema_name
                   and lower(tname) = lower(@source_table_name)
                   and lower(cname) = lower(@_col_name)


--exec seg01_log 2
                EXECUTE('INSERT into SEG01_Segment_Dictionary_Tag_Types_tbl(tag_name, database_name, schema_name, table_name, table_proxy, col_name, tag_type) VALUES('''||
                                @_database_name||'.'||@_schema_name||'.'||coalesce(@_table_proxy,@_table_name)||'.'||@_col_name||''','''||
                                @_database_name||''','''||
                                @_schema_name||''','''||
                                @_table_name||''','''||
                                @_table_proxy||''','''||
                                @_col_name||''','''||
                                @tag_value_type||''')')
                commit
--exec seg01_log 3
          END

       --now tag_name has been inserted and given an id
       BEGIN
--exec seg01_log 4
             --GET tag_UID and use to insert @_tag_name into DICTIONARY if not exists
             SELECT @tag_uid = uniqid
               from SEG01_Segment_Dictionary_Tag_Types_tbl
              where tag_name = @_database_name||'.'||@_schema_name||'.'||coalesce(@_table_proxy,@_table_name)||'.'||@_col_name

--exec seg01_log '@tag_uid<'||@tag_uid||'>'

             --is the tag_uid available
             SELECT @tag_uid_available = tag_type_uid
               from SEG01_Segment_Dictionary_tbl
              where tag_type_uid = @tag_uid
--exec seg01_log 6

             --if not available
             IF @tag_uid_available is null
                --need to insert the new tag_uid and tag_value
                 BEGIN
--exec seg01_log 7

                     IF object_id('seg01_load_tmp') IS NOT NULL
                         BEGIN
                             DROP TABLE seg01_load_tmp
                         END

                     create table seg01_load_tmp(
                         uniqid                 BIGINT          NOT NULL identity,
                         col_value               VARCHAR(255)     NOT NULL
                     )


--exec seg01_log '@_table_name<'||@_table_name||'>'
                     --ITERATE THROUGH THE TABLE <@_table_name> here
                     EXECUTE('INSERT into seg01_load_tmp (col_value) SELECT distinct STRING('||@_col_name||') FROM '||@source_schema_name||'.'||@source_table_name
                        ||' WHERE '||@_col_name||' is not null')--only update records
                     commit

--exec seg01_log madetable
                     -- we now have a table containing all the values (possible permutations) to load into this Tag (@tag_uid)
                     --we don't need this once we have converted the bit below to update all affected records... rather than insert 1 record
                     select @tag_value = col_value
                       from seg01_load_tmp
                      where uniqid = 1

--exec seg01_log '@_table_name<'||@_tag_value||'>'


                     --insert the load table, that will contain all the permutations
                     EXECUTE(' INSERT into SEG01_Segment_Dictionary_tbl(tag_type_uid, tag_value_uid, tag_value)'||
                             ' SELECT '||@tag_uid||', uniqid, col_value FROM seg01_load_tmp ')
                     commit

--exec seg01_log droped
                     EXECUTE('DROP table seg01_load_tmp')
                     commit
--exec seg01_log 8
                 END
             ELSE
                 --existing tag, check if tag_value exists, and insert if not
                 BEGIN
--exec seg01_log 9
                     -- AS this could be a multiple list of values... if this tag_value is available change all this to an update statement instead...
                     SELECT @tag_value_available = tag_value
                       from SEG01_Segment_Dictionary_tbl
                      where tag_type_uid = @tag_uid
--exec seg01_log 10

                     IF @tag_value_available is null
                         BEGIN
--exec seg01_log 11
                             EXECUTE('INSERT into SEG01_Segment_Dictionary_tbl(tag_type_uid, tag_value) VALUES('''||@tag_uid||''','''||@tag_value||''')')
                             commit
--exec seg01_log 12
                         END
                 END
        END

END;

