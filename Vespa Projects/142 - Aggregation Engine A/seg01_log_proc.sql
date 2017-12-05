create or replace procedure seg01_log(
        in @str varchar(150)
        )
        as
    BEGIN
        EXECUTE('INSERT into SEG01_log_tbl(out) VALUES('''||@str||''')')
        commit
    END;
