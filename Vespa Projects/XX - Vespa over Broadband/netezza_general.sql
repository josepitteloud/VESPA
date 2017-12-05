select date(log_creation_datetime) as dt
,panel_id_reported
,SCMS_SUBSCRIBER_ID as sub
from 	dis_reference..FINAL_DTH_VIEWING_event_history
where dt >= '2014-01-01'
and panel_id_reported in (5,11)
group by sub,dt,panel_id_reported