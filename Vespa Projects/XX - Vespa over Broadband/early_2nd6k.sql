/*
create table     #vobb_link_data_char(Subscriber_ID  varchar(30)
                                     ,Account_Number varchar(30)
                                     ,Model          varchar(30)
                                     ,Location       varchar(30)
                                     ,panel_expected varchar(30)
                                     ,date_added     varchar(30)
                                     ,prev_rq        varchar(30)
);

drop table vobb_link_data_early_2nd6k;

create table     vobb_link_data_early_2nd6k(Subscriber_ID  int
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
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/vobb_link_data_early_2nd6k.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
skip 1
;

  insert into vobb_link_data_early_2nd6k(Subscriber_ID
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
        ,cast(prev_rq as real)
from #vobb_link_data_char
;
*/

create table     #vobb_Netezza_data_char(dt             varchar(30)
                                        ,panelid        varchar(2)
                                        ,subscriberid   varchar(30)
);

create table     #vobb_Netezza_data(     dt             date
                                        ,panelid        tinyint
                                        ,subscriberid   int
);

create table    #vobb_all_data(          dt             date
                                        ,panelid        tinyint
                                        ,subscriberid   int
                                        ,account_number varchar(30)
                                        ,model          varchar(30)
                                        ,location       varchar(30)
                                        ,date_added     date
                                        ,data_received  bit
                                        ,prev_rq        real
);

create table #vobb_connection_data_char(cardid          varchar(30)
                                       ,subid           varchar(30)
                                       ,panel           varchar(30)
                                       ,call_start_gmt  varchar(30)
                                       ,bb              varchar(30)
);

create table #vobb_connection_data(cardid          bigint
                                  ,subid           bigint
                                  ,panel           tinyint
                                  ,call_start_gmt  datetime
                                  ,bb              varchar(1)
);

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
group by dt
        ,panelid
        ,subscriberid
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
        ,'2014-01-10'
        ,case when (cal.calendar_date >= '2014-01-14' and con.subid is not null and net.subscriberid is not null)
                or net.subscriberid is not null then 1 else 0
          end as data_received
        ,lnk.prev_rq
    from sk_prod.sky_calendar               as cal
         cross join vobb_link_data_early_2nd6k  as lnk
         left join #vobb_netezza_data       as net on lnk.subscriber_id  = net.subscriberid
                                                  and lnk.panel_expected = net.panelid
                                                  and cal.calendar_date  = net.dt
         left join #vobb_connection_data    as con on lnk.subscriber_id  = con.subid
                                                  and lnk.panel_expected = con.panel
                                                  and cal.calendar_date  = con.call_start_gmt
   where cal.calendar_date between '2014-01-10' and date(now())
;

  select subscriberid
        ,panelid
        ,cal.calendar_date
        ,sum(case when dt between cal.calendar_date - 15 and cal.calendar_date and dt >= date_added then data_received else 0 end) as rq_numerator
        ,sum(case when dt between cal.calendar_date - 15 and cal.calendar_date and dt >= date_added then 1             else 0 end) as rq_denominator
    into #vobb_rq
    from sk_prod.sky_calendar     as cal
         cross join #vobb_all_data as vad
   where cal.calendar_date between '2014-01-10' and now()
group by subscriberid
        ,panelid
        ,cal.calendar_date
;

  select vad.subscriberid
        ,vad.account_number
        ,vad.model
        ,vad.location
        ,vad.panelid
        ,vad.dt
        ,vad.data_received
        ,case when rq_denominator = 0 then 0 else coalesce(1.0 * vrq.rq_numerator / rq_denominator, 0) end as rq
        ,prev_rq
    from #vobb_all_data      as vad
         inner join #vobb_rq as vrq on vad.subscriberid = vrq.subscriberid
                                   and vad.panelid      = vrq.panelid
                                   and vad.dt           = vrq.calendar_date
;
---


