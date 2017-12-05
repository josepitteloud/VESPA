/* Some pre-requisites if running this script for the first time...
create table     #vobb_link_data_char(Subscriber_ID  varchar(30)
                                     ,Account_Number varchar(30)
                                     ,Model          varchar(30)
                                     ,Location       varchar(30)
                                     ,panel_expected varchar(30)
                                     ,date_added     varchar(30)
                                     ,prev_rq        varchar(30)
);


drop table vobb_link_data_r7_staff;
create table     vobb_link_data_r7_staff(Subscriber_ID  int
                                        ,Account_Number varchar(30)
                                        ,Model          varchar(30)
                                        ,Location       varchar(30)
                                        ,panel_expected tinyint
                                        ,date_added     date
                                        ,prev_rq        real
);

load table      #vobb_link_data_char(
                Subscriber_ID',',
                Account_Number',',
                Model',',
                Location',',
                panel_expected',',
                date_added,
                prev_rq'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/vobb_r7_staff_linkdata.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
skip 1
;

  insert into vobb_link_data_r7_staff(Subscriber_ID
                          ,Account_Number
                          ,Model
                          ,Location
                          ,panel_expected
                          ,date_added
                          ,prev_rq
)
  select cast(Subscriber_ID  as int)
        ,Account_Number
        ,Model
        ,Location
        ,cast(panel_expected as int)
        ,cast(date_added     as date)
        ,case when prev_rq =char(13) then null else cast(prev_rq as real) end
from #vobb_link_data_char
;
*/


--------------------
-- Initialise tables
--------------------
create table     #vobb_Netezza_data_char(dt             varchar(30)
                                        ,panelid        varchar(2)
                                        ,subscriberid   varchar(30)
)
;

create table     #vobb_Netezza_data(     dt             date
                                        ,panelid        tinyint
                                        ,subscriberid   int
)
;

create table    #vobb_all_data(          dt             date
                                        ,panelid        tinyint
                                        ,subscriberid   int
                                        ,account_number varchar(30)
                                        ,model          varchar(30)
                                        ,location       varchar(30)
                                        ,date_added     date
                                        ,data_received  bit
                                        ,prev_rq        real
)
;

create table #vobb_connection_data_char(cardid          varchar(30)
                                       ,subid           varchar(30)
                                       ,panel           varchar(30)
                                       ,call_start_gmt  varchar(30)
                                       ,bb              varchar(30)
)
;

create table #vobb_connection_data(cardid          bigint
                                  ,subid           bigint
                                  ,panel           tinyint
                                  ,call_start_gmt  date
                                  ,bb              varchar(1)
)
;



------------------------------
-- Import Netezza viewing data
------------------------------
load table      #vobb_Netezza_data_char
(               dt',',
                panelid',',
                subscriberid'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/Netezza.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;

  insert into #vobb_netezza_data(dt
                                ,panelid
                                ,subscriberid)
  select cast(left(dt,10) as date)
        ,cast(panelid as int)
        ,cast(subscriberid as int)
    from #vobb_netezza_data_char
group by dt             -- can these group-bys be performed within the Netezza query, hence reducing the amount of data to transfer?
        ,panelid
        ,subscriberid
;



-------------------------------------
-- Import daily panel connection logs
-------------------------------------
-- Historical logs
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
  insert into #vobb_connection_data(cardid
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
   while @counter <= 23
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
  insert into #vobb_connection_data(cardid
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

----------------------
-- Combine and process
----------------------
  insert into #vobb_all_data(dt
                            ,panelid
                            ,subscriberid
                            ,account_number
                            ,model
                            ,location
                            ,date_added
                            ,data_received
                            ,prev_rq
)
  select cal.calendar_date
        ,lnk.panel_expected
        ,lnk.subscriber_id
        ,lnk.account_number
        ,lnk.model
        ,lnk.location
        ,date_added
        ,case when
                (       con.panel = 4
                        and cal.calendar_date >= '2014-01-14'
                        and con.bb = 'Y'
                        and net.subscriberid is not null
                )
                or (    con.panel = 4
                        and (cal.calendar_date <  '2014-01-14' or cal.calendar_date in ('2014-02-15', '2014-02-16'))
                        and net.subscriberid is not null
                   )
                or (    con.panel = 9
                        and con.subid is not null
                   )
                then 1 else 0
          end as data_received
        ,lnk.prev_rq
    from sk_prod.sky_calendar               as cal
         cross join vobb_link_data_r7_staff as lnk
         left join #vobb_netezza_data       as net on lnk.subscriber_id  = net.subscriberid
                                                  and lnk.panel_expected = net.panelid
                                                  and cal.calendar_date  = net.dt
         left join #vobb_connection_data    as con on lnk.subscriber_id  = con.subid
                                                  and lnk.panel_expected = con.panel
                                                  and cal.calendar_date  = con.call_start_gmt
   where cal.calendar_date between date_added and date(now())
;




---------------------------------------------------
-- Calculate the subscriber-level reporting quality
---------------------------------------------------
  select subscriberid
        ,panelid
        ,cal.calendar_date
        ,sum(case when dt between cal.calendar_date - 15 and cal.calendar_date and dt >= date_added then data_received else 0 end) as rq_numerator
        ,sum(case when dt between cal.calendar_date - 15 and cal.calendar_date and dt >= date_added then 1             else 0 end) as rq_denominator
    into #vobb_rq
    from sk_prod.sky_calendar     as cal
         cross join #vobb_all_data as vad
   where cal.calendar_date between date_added and now()
group by subscriberid
        ,panelid
        ,cal.calendar_date
;





---------------------------------
-- Generate summary for reporting
---------------------------------
  select vad.subscriberid
        ,vad.account_number
        ,vad.model
        ,vad.location
        ,vad.panelid
        ,vad.dt
        ,vad.data_received
        ,case when rq_denominator = 0 then 0 else coalesce(1.0 * vrq.rq_numerator / rq_denominator, 0) end as rq
        ,prev_rq
  into #vobb_pivot
    from #vobb_all_data      as vad
         inner join #vobb_rq as vrq on vad.subscriberid = vrq.subscriberid
                                   and vad.panelid      = vrq.panelid
                                   and vad.dt           = vrq.calendar_date
where vad.subscriberid not in (177849
                              ,252739
                              ,252993
                              ,1837124
                              ,10035183
                              ,10696939
                              ,16316169
                              ,17380009
                              ,26081791
                              ,28401486
);
---





--------------------------------------
-- Remove never-returners from summary
--------------------------------------
select subscriberid
into #never_returners
from (
    select
            subscriberid
            , sum(data_received) as dr
    from #vobb_pivot
    group by subscriberid
    having dr = 0
    ) as t
;




--------------------------
-- Summary for pivot table
--------------------------
select *
from
        #vobb_pivot                     as vp
        left join #never_returners      as nr   on nr.subscriberid = vp.subscriberid
where nr.subscriberid is null
;





---




