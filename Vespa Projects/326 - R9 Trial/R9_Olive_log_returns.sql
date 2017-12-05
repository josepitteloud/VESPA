  select dt
--        ,pan.panel
        ,sum(case when box_model like 'DRX 89%'  then data_received else 0 end) as model_89x
        ,sum(case when box_model like 'DRX 595%' then data_received else 0 end) as model_595
    from vespa_analysts.panel_data as pan
         inner join vespa_analysts.sig_single_box_view as sbv on pan.subscriber_id = sbv.subscriber_id
   where dt >= '2015-05-01'
group by dt
--        ,pan.panel


