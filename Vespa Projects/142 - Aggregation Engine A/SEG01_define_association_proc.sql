
/****************************************************************************************
 **  To automatically construct queries we need to know how tables relate to each other.
 **  We need to define the relationships that exist between them.
 **  The following procedure helps to define how table columns relate to each other.
 ****************************************************************************************/


CREATE OR REPLACE PROCEDURE SEG01_define_association(
                in @_database_name          VARCHAR(24),
                in @_schema_name            VARCHAR(24),
                in @_table_name             VARCHAR(24),
                in @_col_name               VARCHAR(42),
                in @_association_name       VARCHAR(42)
                ) AS
BEGIN
    DECLARE @association_uid BIGINT

--exec seg01_log start1

    --find out if name already exists
    SELECT @association_uid = uniqid
      from SEG01_Table_association_tbl
     where database_name =  @_database_name
       and schema_name = @_schema_name
       and table_name = @_table_name
       and col_name = @_col_name
       and association_name = @_association_name

--exec seg01_log 1

    IF @association_uid is null
        BEGIN
--exec seg01_log 2
            EXECUTE('INSERT into SEG01_Table_association_tbl(database_name, schema_name, table_name, col_name, association_name) VALUES('''||
                        @_database_name||''','''||
                        @_schema_name||''','''||
                        @_table_name||''','''||
                        @_col_name||''','''||
                        @_association_name||''')')
            commit
--exec seg01_log 3
        END
END;

