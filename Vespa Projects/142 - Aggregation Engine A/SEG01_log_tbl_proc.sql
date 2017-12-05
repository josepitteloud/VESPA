
IF object_id('SEG01_log_tbl') IS NOT NULL
    BEGIN
        DROP TABLE SEG01_log_tbl
    END

create table SEG01_log_tbl(
    out              VARCHAR(150)    NOT NULL
)



create or replace procedure seg01_log(
        in @str varchar(150)
        )
        as
    BEGIN
        EXECUTE('INSERT into SEG01_log_tbl(out) VALUES('''||@str||''')')
        commit
    END;
