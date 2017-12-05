--   create table alt_panel_data(
--                subscriber_id varchar(30)
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
              ,date8      varchar(20)
              ,calls8     varchar(20)
              ,date9      varchar(20)
              ,calls9     varchar(20)
              ,date10     varchar(20)
              ,calls10    varchar(20)
              ,date11     varchar(20)
              ,calls11    varchar(20)
              ,date12     varchar(20)
              ,calls12    varchar(20)
              ,date13     varchar(20)
              ,calls13    varchar(20)
              ,date14     varchar(20)
              ,calls14    varchar(20)
              ,date15     varchar(20)
              ,calls15    varchar(20)
              ,date16     varchar(20)
              ,calls16    varchar(20)
              ,date17     varchar(20)
              ,calls17    varchar(20)
              ,date18     varchar(20)
              ,calls18    varchar(20)
              ,date19     varchar(20)
              ,calls19    varchar(20)
              ,date20     varchar(20)
              ,calls20    varchar(20)
              ,date21     varchar(20)
              ,calls21    varchar(20)
              ,date22     varchar(20)
              ,calls22    varchar(20)
              ,date23     varchar(20)
              ,calls23    varchar(20)
              ,date24     varchar(20)
              ,calls24    varchar(20)
              ,date25     varchar(20)
              ,calls25    varchar(20)
              ,date26     varchar(20)
              ,calls26    varchar(20)
              ,date27     varchar(20)
              ,calls27    varchar(20)
              ,date28     varchar(20)
              ,calls28    varchar(20)
              ,date29     varchar(20)
              ,calls29    varchar(20)
              ,date30     varchar(20)
              ,calls30    varchar(20)
              ,date31     varchar(20)
              ,calls31    varchar(20)
              ,date32     varchar(20)
              ,calls32    varchar(20)
              ,date33     varchar(20)
              ,calls33    varchar(20)
              ,date34     varchar(20)
              ,calls34    varchar(20)
              ,date35     varchar(20)
              ,calls35    varchar(20)
              ,date36     varchar(20)
              ,calls36    varchar(20)
              ,date37     varchar(20)
              ,calls37    varchar(20)
              ,date38     varchar(20)
              ,calls38    varchar(20)
              ,date39     varchar(20)
              ,calls39    varchar(20)
              ,date40     varchar(20)
              ,calls40    varchar(20)
);


create variable @sql varchar(10000);
create variable @panel tinyint;
create variable @counter tinyint;
set @panel=6;

while @panel <= 7 begin

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
                     calls7'','',
                     date8'','',
                     calls8'','',
                     date9'','',
                     calls9'','',
                     date10'','',
                     calls10'','',
                     date11'','',
                     calls11'','',
                     date12'','',
                     calls12'','',
                     date13'','',
                     calls13'','',
                     date14'','',
                     calls14'','',
                     date15'','',
                     calls15'','',
                     date16'','',
                     calls16'','',
                     date17'','',
                     calls17'','',
                     date18'','',
                     calls18'','',
                     date19'','',
                     calls19'','',
                     date20'','',
                     calls20'','',
                     date21'','',
                     calls21'','',
                     date22'','',
                     calls22'','',
                     date23'','',
                     calls23'','',
                     date24'','',
                     calls24'','',
                     date25'','',
                     calls25'','',
                     date26'','',
                     calls26'','',
                     date27'','',
                     calls27'','',
                     date28'','',
                     calls28'','',
                     date29'','',
                     calls29'','',
                     date30'','',
                     calls30'','',
                     date31'','',
                     calls31'','',
                     date32'','',
                     calls32'','',
                     date33'','',
                     calls33'','',
                     date34'','',
                     calls34'','',
                     date35'','',
                     calls35'','',
                     date36'','',
                     calls36'','',
                     date37'','',
                     calls37'','',
                     date38'','',
                     calls38'','',
                     date39'','',
                     calls39'','',
                     date40'','',
                     calls40''\n''
     )
     from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/panel' || @panel || '.csv''
     QUOTES OFF
     ESCAPES OFF
     NOTIFY 1000
     SKIP 1'

     execute (@sql)

     set @counter=1

     while @counter <= 40 begin

               set @sql = '
                 insert into vespa_analysts.alt_panel_data(subscriber_id
                                           ,panel
                                           ,dt
                                           ,data_received
                                           )
                 select trim(subid)
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

   while @panel not in (6,7,12) begin
             set @panel = @panel + 1
   end
end
;

--test
/*
select dt
      ,sum(case when panel=6  then data_received else 0 end) as p6
      ,sum(case when panel=7  then data_received else 0 end) as p7
      ,sum(case when panel=12 then data_received else 0 end) as p12
      ,count(1)
  from vespa_analysts.alt_panel_data
group by dt
order by dt
*/


--truncate table vespa_analysts.alt_panel_data

