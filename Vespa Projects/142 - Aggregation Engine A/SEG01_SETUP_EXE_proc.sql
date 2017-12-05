
CREATE or replace procedure SEG01_SETUP_EXE_proc(
                ) AS
BEGIN


execute SEG01_SETUP_TABLES_proc
execute SEG01_setup_create_table_data_proc


END;
