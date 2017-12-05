
/*******************************************************************************************************************
 **  Define tables for being 'self-aware' (classifying tags so that the Engine is aware what they are related to. **
 **  Related to viewing sources, metrics, temporal etc...)                                                        **
 **                                                                                                               **
 *******************************************************************************************************************/


/***************************************************************
 **  create selfaware procedure, as input we want:
 **      in @_database_name          VARCHAR(24),
 **      in @_schema_name            VARCHAR(24),
 **      in @_table_name             VARCHAR(24),
 **      in @_table_proxy            VARCHAR(24),
 **      in @_col_name               VARCHAR(42),
 **      in @_aware_type             VARCHAR(42)
 **
 ***************************************************************/
CREATE OR REPLACE PROCEDURE SEG01_define_selfaware(
                in @_database_name          VARCHAR(24),
                in @_schema_name            VARCHAR(24),
                in @_table_name             VARCHAR(24),
                in @_table_proxy            VARCHAR(24),
                in @_col_name               VARCHAR(42),
                in @_aware_type             VARCHAR(42)
                ) AS
BEGIN
    DECLARE @tag_name VARCHAR(255)
    DECLARE @tag_uid BIGINT

    SELECT @tag_name = tag_name --also checks that the tag exists in the dictionary
      from SEG01_Segment_Dictionary_Tag_Types_tbl
     where tag_name =  @_database_name||'.'||@_schema_name||'.'||coalesce(@_table_proxy, @_table_name)||'.'||@_col_name

    --find out if name already exists
    SELECT @tag_uid = uniqid
      from SEG01_Tag_Self_Aware_tbl
     where tag_name =  @tag_name
       and aware_type = @_aware_type

    IF @tag_uid is null
        BEGIN
            EXECUTE('INSERT into SEG01_Tag_Self_Aware_tbl(tag_name, database_name, schema_name, table_name, table_proxy, col_name, aware_type) VALUES('''||
                        @tag_name||''','''||
                        @_database_name||''','''||
                        @_schema_name||''','''||
                        @_table_name||''','''||
                        @_table_proxy||''','''||
                        @_col_name||''','''||
                        @_aware_type||''')')
            commit
        END
END;

