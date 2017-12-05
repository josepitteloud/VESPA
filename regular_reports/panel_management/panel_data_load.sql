--   create table panel_data(   -- used to be called alt_panel_data which changed due to daily panel being included. BC 23-09-2013
--                subscriber_id int
--               ,panel         tinyint
--               ,dt            date
--               ,data_received bit default 0
-- );
--create unique hg index idx_sub on alt_panel_data(subscriber_id, dt);
--grant select on alt_panel_data to vespa_group_low_security;

    drop table data_dump;
  create table data_dump(
               subid      varchar(20)
              ,panel      tinyint
              ,panel_date varchar(20)
              ,date1      varchar(20)
              ,calls1     varchar(20)
              ,date2      varchar(20)
              ,calls2     varchar(20)
              ,date3      varchar(20)
              ,calls3     varchar(20)
              ,date4      varchar(20)
              ,calls4     varchar(20)
              ,date5      varchar(20)
              ,calls5     varchar(20)
              ,date6      varchar(20)
              ,calls6     varchar(20)
              ,date7      varchar(20)
              ,calls7     varchar(20)
);


create variable @sql varchar(10000);
create variable @panel tinyint;
create variable @counter tinyint;
set @panel=5;

while @panel <= 12 begin

     set @sql = '
     load table data_dump(
                     subid'','',
                     panel'','',
                     panel_date'','',
                     date1'','',
                     calls1'','',
                     date2'','',
                     calls2'','',
                     date3'','',
                     calls3'','',
                     date4'','',
                     calls4'','',
                     date5'','',
                     calls5'','',
                     date6'','',
                     calls6'','',
                     date7'','',
                     calls7''\n''
     )
     from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Berwyn/panel' || @panel || '.csv''
     QUOTES OFF
     ESCAPES OFF
     NOTIFY 1000
     SKIP 1'

     execute (@sql)

     set @counter=1

     while @counter <= 7 begin

               set @sql = '
                 insert into vespa_analysts.panel_data(subscriber_id
                                           ,panel
                                           ,dt
                                           ,data_received
                                           )
                 select cast(subid as int)
                       ,panel
                       ,''20'' || dateformat(date' || @counter || ',''dd-mm-yy'')
                       ,case when cast(calls' || @counter || ' as int) >= 1 then 1 else 0 end
                   from data_dump
                  where subid <> ''''
                    and panel is not null
                   '

           execute (@sql)
               set @counter = @counter + 1

     end

     truncate table data_dump

     set @panel = @panel + 1

   while @panel not in (5,6,7,11,12) begin
             set @panel = @panel + 1
   end
end
;

--test
/*
select dt
      ,sum(case when panel=5  then data_received else 0 end) as p5
      ,sum(case when panel=6  then data_received else 0 end) as p6
      ,sum(case when panel=7  then data_received else 0 end) as p7
      ,sum(case when panel=11  then data_received else 0 end) as p11
      ,sum(case when panel=12 then data_received else 0 end) as p12
      ,count(1)
  from vespa_analysts.panel_data
group by dt
order by dt desc
*/




