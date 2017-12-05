select distinct tname from sys.syscolumns
where creator = 'sk_prod' and tname like 'VESPA_STB_PROG_EVENTS_%'
order by tname;
-- currently goes from 20110503 through to 20120214


create variable @SQL_daily_kludge       varchar(2000);
create variable @scanning_day           date;

create table Green_buttonings (
    viewing_day                 date
    ,panel_id                   tinyint
    ,live_green_buttons         int
    ,timeshifted_green_buttons  int
    ,primary key (viewing_day, panel_id)
);

-- This guy couldn't be a parameterised query anyway, since we're changing the
-- source table on each loop iteration
set @SQL_daily_kludge = 'insert into Green_buttonings
select
        ''#!£*%!#''
        ,panel_id
        ,convert(date, dateadd(hh, -9, min(document_creation_date)))
        ,sum(case when si_service_id = 5048 then 1 else 0 end)
        ,sum(case when service_key = 2338 then 1 else 0 end)
from sk_prod.VESPA_STB_PROG_EVENTS_#*££*# -- will get replaced by the daily stamp of each table
where service_key = 2338
or si_service_id = 5048
group by panel_id
'

delete from Green_buttonings;

set @scanning_day = '20110503';

while @scanning_day <= '2012-02-14'
begin
    execute(replace(replace(@SQL_daily_kludge, '#!£*%!#', @scanning_day), '#*££*#', dateformat(@scanning_day,'yyyymmdd')))

    commit
    set @scanning_day = dateadd(day, 1, scanning_day)
end;

