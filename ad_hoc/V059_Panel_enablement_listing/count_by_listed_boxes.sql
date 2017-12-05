  select x_manufacturer
        ,x_model_number
        ,x_pvr_type
    from sk_prod.cust_set_top_box
group by x_manufacturer
        ,x_model_number
        ,x_pvr_type
order by x_manufacturer
        ,x_model_number
        ,x_pvr_type
;

  select panel_no
--        ,full_cow
        ,sub_cow
        ,count(1)
    from (select stb.account_number
                ,max(case when left(decoder_nds_number,4) in ('4F20', '4F21', '4E20', '4E21', '9F20', '9F21', '9F22', '9F23') then 1 else 0 end) as sub_cow
                ,panel_no
            from sk_prod.cust_set_top_box                   as stb
                 inner join sk_prod.vespa_subscriber_status as vss on stb.account_number = vss.account_number
              and result='Enabled'
              and box_replaced_dt = '9999-09-09'
         group by stb.account_number
                 ,panel_no
         ) as sub
group by panel_no
--        ,full_cow
        ,sub_cow
;

--for pivot
  select *
        ,cast(null as varchar(30)) as subscriber_id
   into #stb from
     (select service_instance_id
            ,stb.decoder_nds_number
            ,x_manufacturer
            ,x_model_number
            ,rank () over (partition by stb.service_instance_id order by ph_non_subs_link_sk desc) rank
        from sk_prod.cust_Set_top_box as stb
       where box_replaced_dt = '9999-09-09'
        ) as sub
       where rank = 1
;

create hg index idx1 on #stb(service_instance_id);

  update #stb as stb
     set stb.subscriber_id = csi.si_external_identifier
    from sk_prod.cust_service_instance as csi
   where csi.src_system_id = stb.service_instance_id
     and si_external_identifier is not null
;

          select x_manufacturer
                ,x_model_number
                ,left(decoder_nds_number,4) as code
                ,panel_no
                ,count(1) as cow
            from #stb                                       as stb
                 inner join sk_prod.vespa_subscriber_status as vss on stb.subscriber_id = vss.card_subscriber_id
           where result='Enabled'
        group by code
                ,panel_no
                ,x_manufacturer
                ,x_model_number
;










