--------------------
-- Initialise tables
--------------------
/*
create table vobb_connection_data(cardid          bigint
                                  ,subid           bigint
                                  ,panel           tinyint
                                  ,call_start_gmt  date
                                  ,bb              varchar(1)
)
;
*/

truncate table vobb_connection_data;

create table #vobb_connection_data_char(cardid          varchar(30)
                                       ,subid           varchar(30)
                                       ,panel           varchar(30)
                                       ,call_start_gmt  varchar(30)
                                       ,bb              varchar(30)
)
;




load table      #vobb_connection_data_char
(               cardid',',
                subid',',
                panel',',
                call_start_gmt',',
                bb'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/connections.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;



-- The Historical log becomes uunmanageable in Excel once we hit ~million rows, so simply include each new daily file (with the header lines removed)...
--2014/2/1 - this entire day was missing from the main connections.csv due to a reporting issue for that day
load table      #vobb_connection_data_char
(               cardid',',
                subid',',
                panel',',
                call_start_gmt',',
                bb'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/broadband_connections_2014-02-01.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;

--2014/02/08 - 2014/02/14
  create variable @counter int;
     set @counter = 8;
   while @counter <= 14
   begin
         execute ('load table #vobb_connection_data_char
                   (cardid'','',
                    subid'','',
                    panel'','',
                    call_start_gmt'','',
                    bb''\n''
               )
               from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/broadband_connections_2014-02-' || right('0' || @counter, 2) || '.csv''
             QUOTES OFF
            ESCAPES OFF
             NOTIFY 1000')
       commit
          set @counter = @counter + 1
     end;

--2014/02/17 - 2014/02/21
     set @counter = 17;
   while @counter <= 21
   begin
         execute ('load table #vobb_connection_data_char
                   (cardid'','',
                    subid'','',
                    panel'','',
                    call_start_gmt'','',
                    bb''\n''
               )
               from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/broadband_connections_2014-02-' || @counter || '.csv''
             QUOTES OFF
            ESCAPES OFF
             NOTIFY 1000')
       commit
          set @counter = @counter + 1
     end;

-- Convert all connection logs so far (the date format changes after here)
  insert into vobb_connection_data(cardid
                                   ,subid
                                   ,panel
                                   ,call_start_gmt
                                   ,bb
)
  select cast(cardid as bigint)
        ,cast(subid as bigint)
        ,cast(panel as tinyint)
        ,cast(substr(call_start_gmt, 7, 4) || '-' || substr(call_start_gmt, 4, 2) || '-' || left(call_start_gmt, 2) as date)
        ,bb
    from #vobb_connection_data_char
;

truncate table #vobb_connection_data_char;

--2014/02/22 - 2014/03/01
    execute ('load table #vobb_connection_data_char
              (cardid'','',
               subid'','',
               panel'','',
               call_start_gmt'','',
               bb''\n''
          )
          from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/broadband_connections_22feb_to_01_mar.csv''
        QUOTES OFF
       ESCAPES OFF
        NOTIFY 1000')
;

--2014/03/02
    execute ('load table #vobb_connection_data_char
              (cardid'','',
               subid'','',
               panel'','',
               call_start_gmt'','',
               bb''\n''
          )
          from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/broadband_connections_2014-03-02.csv''
        QUOTES OFF
       ESCAPES OFF
        NOTIFY 1000')
;

--From 2014/03/04
     set @counter = 4;
   while @counter <= 24
   begin
         execute ('load table #vobb_connection_data_char
                   (cardid'','',
                    subid'','',
                    panel'','',
                    call_start_gmt'','',
                    bb''\n''
               )
               from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/broadband_connections_2014-03-' || right('0' || @counter, 2) || '.csv''
             QUOTES OFF
            ESCAPES OFF
             NOTIFY 1000')
       commit
          set @counter = @counter + 1
     end;

-- Convert all connection logs
  insert into vobb_connection_data(cardid
                                   ,subid
                                   ,panel
                                   ,call_start_gmt
                                   ,bb
)
  select cast(cardid as bigint)
        ,cast(subid as bigint)
        ,cast(panel as tinyint)
        ,cast(substr(call_start_gmt, 8, 4) || '-0' || month(substr(call_start_gmt, 4, 3)) || '-' || left(call_start_gmt, 2) as date)
        ,bb
    from #vobb_connection_data_char
where call_start_gmt not in ('CALL_START_GMT','--------------------')
;



