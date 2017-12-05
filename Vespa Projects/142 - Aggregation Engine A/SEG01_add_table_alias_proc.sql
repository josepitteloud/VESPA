

/*********************************************************************
 **  Assigns a segmentation ID. Basically just gets the next integer
 **  available, and records the assigned date against it, and leaves
 **  a date column field free for later when the segmentation is
 **  actually created.
 **
 *********************************************************************/

CREATE or replace procedure SEG01_add_table_alias(
                in  @_table_name       varchar(64),
                in  @_table_num        integer,
                out @_table_with_alias varchar(64),
                out @_table_alias varchar(64)
                ) AS
BEGIN

DECLARE @first bit
SET @first = 1

WHILE @_table_num > 0
    BEGIN
        IF @first = 1
            BEGIN
                SET @_table_alias = ' a'
                SET @_table_with_alias = @_table_name||@_table_alias
                SET @first = 0
            END
        ELSE
            BEGIN
                SET @_table_alias = @_table_alias||'a'
                SET @_table_with_alias = @_table_with_alias||'a'
            END

        SET @_table_num = @_table_num-1
    END
END;



