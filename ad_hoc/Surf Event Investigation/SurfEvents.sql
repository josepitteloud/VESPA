/*
select live_recorded,count(1) from sk_prod.vespa_events_all
where date(event_start_date_time_utc)='2012-11-01'
group by live_recorded

select event_type,count(1) from sk_prod.VESPA_STB_PROG_EVENTS_20121004
group by event_type

select * from sk_prod.VESPA_STB_PROG_EVENTS_20121004
where subscriber_id=3185976
--and event_type='evSurf'
and panel_id=12
--2012-10-04 00:36:01.000000
;
select event_start_date_time_utc,event_end_date_time_utc,* from sk_prod.vespa_events_all
where date(event_start_date_time_utc) = '2012-10-04'
and subscriber_id=3185976
--and dk_previous_channel_dim=-1
order by event_start_date_time_utc
*/


select * into #stb_active from
     (select account_number
            ,x_model_number
            ,service_instance_id
            ,x_box_type
            ,x_pvr_type
            ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) rank
        from sk_prod.cust_Set_top_box) as sub
 where rank = 1
;

  select x_pvr_type
        ,x_model_number
        ,count(1) as cow
    from sk_prod.VESPA_STB_PROG_EVENTS_20121004 as vev
         inner join #stb_active as stb on vev.service_instance_id = stb.service_instance_id
   where panel_id in (4,5,12)
--     and event_type='evSurf'
group by x_pvr_type
        ,x_model_number
order by cow desc
;

  select x_model_number,x_box_type,count(1)
    from #stb_active where x_model_number in ('DRX 890', 'Unknown', 'DRX 780', 'TDS850NB', 'DRX 895')
group by x_model_number,x_box_type

select top 10 * from sk_prod.cust_Set_top_box



sp_iqcontext


select count(distinct account_number) from sk_prod.vespa_events_All where event_start_Date_time_utc between '2012-08-21' and '2012-08-21 23:59:59'
and programme_name = 'EastEnders'
