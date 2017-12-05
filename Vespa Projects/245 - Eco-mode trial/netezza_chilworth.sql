select full_channel_name,event_start_reported_datetime,scms_subscriber_id
from 	dis_reference..FINAL_DTH_VIEWING_event_history
where date(event_start_reported_datetime) >= '2014-02-04'
and SCMS_SUBSCRIBER_ID in (
'26847699'
,'26847841'
,'26893281'
,'26893289'
,'26893393'
,'26893472'
,'26893522'
,'26893585'
,'26894591'
)
order by scms_subscriber_id,event_start_reported_datetime
