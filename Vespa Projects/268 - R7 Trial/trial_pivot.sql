/*
create table     #vobb_link_data_char(Subscriber_ID  varchar(30)
                                     ,Account_Number varchar(30)
                                     ,Model          varchar(30)
                                     ,Location       varchar(30)
                                     ,panel_expected varchar(30)
                                     ,date_added     varchar(30)
);

create table     vobb_link_data_r7_staff(Subscriber_ID  int
                                        ,Account_Number varchar(30)
                                        ,Model          varchar(30)
                                        ,Location       varchar(30)
                                        ,panel_expected tinyint
                                        ,date_added     date
);
truncate table vobb_link_data_r7_staff;
load table      #vobb_link_data_char(
                Subscriber_ID',',
                Account_Number',',
                Model',',
                Location',',
                panel_expected',',
                date_added'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/vobb_r7_staff_linkdata.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
skip 1
;

  insert into vobb_link_data(Subscriber_ID  
                          ,Account_Number 
                          ,Model          
                          ,Location       
                          ,panel_expected 
                          ,date_added     
)
  select cast(Subscriber_ID  as int)
        ,Account_Number 
        ,Model          
        ,Location       
        ,cast(panel_expected as int)
        ,cast(date_added     as date)
from #vobb_link_data_char
;
*/

create table     #vobb_Netezza_data_char(dt           varchar(30)
                                        ,panelid      varchar(2)
                                        ,subscriberid varchar(30)
);

create table     #vobb_Netezza_data(     dt           date
                                        ,panelid      tinyint
                                        ,subscriberid int
);

create table    #vobb_all_data(          dt             date
                                        ,panelid        tinyint
                                        ,subscriberid   int
                                        ,account_number varchar(30)
                                        ,model          varchar(30)
                                        ,location       varchar(30)
                                        ,date_added     date
                                        ,data_received  bit
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

  insert into #vobb_all_data(dt
                            ,panelid        
                            ,subscriberid   
                            ,account_number 
                            ,model          
                            ,location       
                            ,date_added
                            ,data_received  
)
  select cal.calendar_date
        ,lnk.panel_expected
        ,lnk.subscriber_id
        ,lnk.account_number
        ,lnk.model
        ,lnk.location
        ,lnk.date_added
        ,case when net.subscriberid is null then 0 else 1 end as data_received
    from sk_prod.sky_calendar         as cal
         cross join vobb_link_data    as lnk
         left join #vobb_netezza_data as net on lnk.subscriber_id  = net.subscriberid
                                            and lnk.panel_expected = net.panelid
                                            and cal.calendar_date  = net.dt                               
   where cal.calendar_date between '2013-08-21' and date(now())
;

/* check future
  insert into #vobb_all_data(dt
                            ,panelid        
                            ,subscriberid   
                            ,account_number 
                            ,model          
                            ,location       
                            ,date_added
                            ,data_received  
)
  select dt+1
                            ,panelid        
                            ,subscriberid   
                            ,account_number 
                            ,model          
                            ,location       
                            ,date_added
                            ,data_received  
    from #vobb_all_data
where dt='2013-10-31'
*/

  select subscriberid
        ,panelid
        ,cal.calendar_date
        ,sum(case when dt between cal.calendar_date - 15 and cal.calendar_date and dt >= date_added then data_received else 0 end) as rq_numerator
        ,sum(case when dt between cal.calendar_date - 15 and cal.calendar_date and dt >= date_added then 1             else 0 end) as rq_denominator
    into #vobb_rq
    from sk_prod.sky_calendar     as cal
         cross join #vobb_all_data as vad
   where cal.calendar_date between '2013-08-21' and now()--+7
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
    from #vobb_all_data      as vad
         inner join #vobb_rq as vrq on vad.subscriberid = vrq.subscriberid
                                   and vad.panelid      = vrq.panelid
                                   and vad.dt           = vrq.calendar_date
where vad.subscriberid not in (8907934
                              ,10656128
                              ,10102096
                              ,28239568
                              ,17244058
                              ,16962683
                              ,24583416
                              ,14180274
                              ,19504873

                              ,25363754
                              ,26767684
                              ,13424350
                              ,13338050
                              ,23805389

--subs removed 9th October
                              ,11840131
                              ,28441368
                              ,16883212
                              ,16475589

--subs still on other panels
                              ,11466989
                              ,128928
                              ,1485894
                              ,18819685
                              ,2061775
                              ,21464645
                              ,23761186
                              ,24535985
                              ,26025038
                              ,9790831
                              ,1837124
                              ,10715734
                              ,177849
                              ,19123490
                              ,22807338
                              ,9159862

--removed 1st Nov
,18891328             
,26767687             
--re4moved 5th November
,15566007

);





---

















