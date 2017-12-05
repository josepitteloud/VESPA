select * into #stb_active
from
(
    select  account_number
            ,service_instance_id
            ,active_box_flag
            ,box_installed_dt
            ,box_replaced_dt
            ,x_pvr_type
            ,x_anytime_enabled
            ,current_product_description
            ,x_anytime_plus_enabled
            ,x_box_type
            ,CASE WHEN x_description like '%HD%2TB%'    THEN 1 ELSE 0 END AS HD2TB
            ,CASE WHEN x_description like '%HD%1TB%'    THEN 1 ELSE 0 END AS HD1TB
            ,CASE WHEN x_description like '%HD%'        THEN 1 ELSE 0 END AS HD
            ,x_manufacturer
            ,x_description
            ,x_model_number
            ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) active_flag
from sk_prod.cust_set_top_box) t
where active_flag = 1


---create a list of DRX 595 active boxes.  To be safe, use the box_replaced_dt to further refine the active boxes

SELECT * INTO #DRX_595_BOXES_LIST
FROM
(select  a.* from #STB_ACTIVE a
where active_flag = 1
and box_replaced_dt = '9999-09-09'
and x_model_number = 'DRX 595') t


---join to Vespa SBV.  This is a view that gets refreshed weekly each Monday night that gives us a view of each box 
---for each active subscriber on any of the panels


/*
select  a.subscriber_id, a.account_number, a.service_instance_id, a.panel_id_vespa,a.status_vespa,b.x_model_number
into #stb_account_analysis
from vespa_analysts.vespa_single_box_view a,
#DRX_595_BOXES_LIST b
where a.account_number = b.account_number
and a.status_vespa in ('Enabled','EnableRequested','EnablePending')
*/

---join to Vespa subscriber_status. not sure if this is 100% accurate but think it allows the code to be run as required.

select  a.subscriber_id, a.account_number, b.service_instance_id, a.panel_id_vespa,a.status_vespa,b.x_model_number
into #stb_account_analysis
from (select card_subscriber_id subscriber_id,account_number, a.panel_no panel_id_vespa,
a.result status_vespa from sk_prod.vespa_subscriber_status a,
where result in ('EnablePending','Enabled')) a,
#DRX_595_BOXES_LIST b
where a.account_number = b.account_number

select panel_id_vespa, count(distinct account_number) from #stb_account_analysis
group by panel_id_vespa
