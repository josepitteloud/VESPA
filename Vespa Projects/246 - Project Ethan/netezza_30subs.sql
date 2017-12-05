select dt,sub from (
  
  select date(log_creation_datetime) as dt
  ,scms_subscriber_id as sub
    from dis_reference..FINAL_DTH_VIEWING_event_history
   where panel_id_reported = 8
and scms_subscriber_id in ('34519415'
,'34519406'
,'34519417'
,'33593316'
,'34519408'
,'34519416'
,'33593314'
,'34519415'
,'33593320'
,'33593318'
,'34519409'
,'33593338'
,'33593334'
,'34519413'
,'34519411'
,'34519412'
,'34519414'
,'34519418'
,'33593336'
,'33593322'
,'34519166'
)
group by dt,sub
) as subq
group by sub,dt

 --detail
select *
    from dis_reference..FINAL_DTH_VIEWING_event_history
--where scms_subscriber_id='34519166'
where scms_subscriber_id = '34307704'

