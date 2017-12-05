select * into viq_008_tab1_tst
from
(select a.pk_viewing_prog_instance_fact,a.dk_event_start_datehour_dim,
 a.dk_barb_min_start_datehour_dim, a.dk_barb_min_start_time_dim,
 a.dk_barb_min_end_datehour_dim, a.dk_barb_min_end_time_dim,
 a.subscriber_id, a.dk_instance_start_datehour_dim, a.dk_instance_start_time_dim
from SK_PROD.VESPA_dP_PROG_VIEWED_201303 a) t

--commit

select a.*,
 LAG(a.dk_barb_min_start_time_dim, 1, 0) OVER (ORDER BY a.subscriber_id, a.DK_INSTANCE_START_DATEHOUR_DIM,a.DK_INSTANCE_START_TIME_DIM asc) AS barb_start_time_prev,
 LAG(a.dk_barb_min_end_time_dim, 1, 0) OVER (ORDER BY a.subscriber_id, a.DK_INSTANCE_START_DATEHOUR_DIM,a.DK_INSTANCE_START_TIME_DIM asc) AS barb_end_time_prev,
 LAG(a.pk_viewing_prog_instance_fact, 1, 0) OVER (ORDER BY a.subscriber_id, a.DK_INSTANCE_START_DATEHOUR_DIM,a.DK_INSTANCE_START_TIME_DIM asc) AS pk_prev
into viq_008_tab2_tst2
from viq_008_tab1_tst a

commit


select pk_viewing_prog_instance_fact, pk_prev from 
(select distinct a.pk_viewing_prog_instance_fact, a.SUBSCRIBER_ID, a.dk_barb_min_start_datehour_dim,A.LIVE_RECORDED,
a.dk_broadcast_start_datehour_dim, a.dk_broadcast_end_datehour_dim, a.dk_event_start_datehour_dim,a.dk_instance_start_datehour_dim,  b.pk_prev,
c.dk_broadcast_start_datehour_dim broadcast_start_prev, c.live_recorded live_prev from 
SK_PROD.VESPA_dP_PROG_VIEWED_201303 a, 
(select * from viq_008_tab2_tst2 a
where subscriber_id is not null
and dk_barb_min_start_time_dim = barb_end_time_prev
and dk_barb_min_start_time_dim > 0) b,
SK_PROD.VESPA_dP_PROG_VIEWED_201303 c
where a.pk_viewing_prog_instance_fact = b.pk_viewing_prog_instance_fact 
and b.pk_prev = c.pk_viewing_prog_instance_fact
and a.live_recorded = 'LIVE'
and substr(cast(a.dk_event_start_datehour_dim as varchar(20)),1,8) = '20130319') A
where A.live_recorded = 'LIVE'
AND LIVE_PREV = 'LIVE'
AND A.DK_BROADCAST_START_DATEHOUR_DIM != 2013031900
AND BROADCAST_START_PREV != 2013031900

