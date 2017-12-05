
--generate service_instance_ids from the boxes

select X_DESCRIPTION, SERVICE_INSTANCE_ID, X_BOX_TYPE, X_MANUFACTURER 
INTO #tmp_service_instance
from sk_prod.cust_set_top_box
where x_active_box_flag_new = 'Y'
AND X_MANUFACTURER = 'Samsung'
and x_pvr_type = 'PVR5'

--join to vespa_single_box_view to get the subscriber ids which you need to get the Netezza results

select a.subscriber_id,a.panel_id_vespa, b.* 
into #tmp_subscriber_id
from vespa_analysts.vespa_single_box_view a,
#tmp_service_instance b
where a.service_instance_id = b.service_instance_id

--get panel 12 records only

select subscriber_id from #tmp_subscriber_id
where panel_id_vespa = 12
