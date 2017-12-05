    drop table ams_report;
    drop table ams_details_temp;
    drop table ams_details;
    drop table ams_netezza_results;
    drop table ams_netezza_results_reformed;

  create table ams_report(
         id int identity
        ,date_time varchar(30)
        ,details   varchar(30)
        ,host      varchar(30)
        )
;

  create table ams_details_temp(
         STB varchar(30)
        ,device_id     varchar(30)
        ,data_received varchar(30)
        );

  create table ams_details(
         STB           varchar(30)
        ,device_id     varchar(30)
        ,data_received bit default 0
        );

  create table ams_netezza_results(
         scms_subscriber_id varchar(30)
        ,device_id          varchar(30)
        ,max_dt             varchar (30)
        );

  create table ams_netezza_results_reformed(
         scms_subscriber_id varchar(30)
        ,device_id          varchar(30)
        ,max_dt             varchar (30)
        );

 execute('    load table ams_report(
                   date_time,
                   details,
                   host''\n''
)
--    from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/ams_report_' || right('0' || datepart(day,today()), 2) || right('0' || datepart(month,today()), 2) || datepart(year,today()) || '.csv''
   from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/ams_report_30092015.csv''
  QUOTES ON
 ESCAPES OFF
  NOTIFY 1000
    SKIP 1
');

    load table ams_netezza_results(
                   scms_subscriber_id,
                   device_id,
                   max_dt'\n'
)
    from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/ams_netezza_results.csv'
  QUOTES ON
 ESCAPES OFF
  NOTIFY 1000
;


  insert into ams_details_temp
        (device_id
        ,STB
        ,data_received
        )
  select details
        ,min(details) over (order by id rows between 1 following and 1 following)
        ,min(details) over (order by id rows between 1 preceding and 1 preceding)
    from ams_report as ams
;

  insert into ams_details
        (STB
        ,device_id
        ,data_received
        )
  select case when STB like '%Report saved%'
                or STB like '%Missing HTTP header%'
                or STB like '%Invalid Logformat%' then null
              else STB end as stb
        ,device_id
        ,case when bas.data_received like '%Report saved%' then 1 else 0 end
    from ams_details_temp as bas
   where bas.device_id like 'STB%'
group by stb
        ,device_id
        ,data_received
;
--/*
      -- DIS version
  insert into ams_netezza_results_reformed(
         scms_subscriber_id
        ,device_id
        ,max_dt
        )
  select scms_subscriber_id
        ,'STB_' || device_id
        ,max_dt
    from ams_netezza_results
;
--*/
/*
      -- ODS version
  insert into ams_netezza_results_reformed(
         scms_subscriber_id
        ,device_id
        ,max_dt
        )
  select scms_subscriber_id
        ,device_id
        ,max_dt
    from ams_netezza_results
;
*/


--results
  select max(STB) as stb
        ,ams.device_id as device_id
        ,max(data_received) as data_recd
        ,max(max_dt) as latest_event
    from ams_details as ams
         left join ams_netezza_results_reformed as net on ams.device_id = net.device_id
group by ams.device_id
;

      -- check
  select case when data_received like '%Report saved%'        then 'Report saved'
              when data_received like '%Missing HTTP header%' then 'Missing HTTP header'
              when data_received like '%Invalid Logformat%'   then 'Invalid Logformat'
              else 'none' end as typ
        ,count(distinct device_id)
    from ams_details_temp
   where device_id like 'STB%'
group by typ
;



/*
select * from ams_details_temp
select * from ams_details
select * from ams_netezza_results_reformed
select * from ams_report
*/



