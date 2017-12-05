  select count()
        ,adjusted_event_start_date_vespa
        ,case when x_model_number like 'DRX 89%' then '89' when x_model_number like 'DRX 595%' then '595' else '0' end as model
    from cust_set_top_box as stb
         inner join viq_viewing_data_scaling as viq on viq.account_number = stb.account_number
   where adjusted_event_start_date_vespa > '2017-03-20'
     and model in ('89','595')
     and active_box_flag = 'Y'
group by adjusted_event_start_date_vespa
        ,model
order by adjusted_event_start_date_vespa
        ,model




