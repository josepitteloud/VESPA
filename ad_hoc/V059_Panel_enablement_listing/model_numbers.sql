drop table model_numbers;

select account_number
  into #active
  from sk_prod.cust_single_account_view
 where cust_active_dtv = 1
;

create hg index idx1 on #active(account_number);

  select service_instance_id
        ,stb.account_number
        ,cast(0 as varchar(30))      as subscriber_id
        ,cast(null as varchar(30))   as model_number
        ,cast(null as varchar(30))   as account_type
        ,cast(null as varchar(30))   as box_type
        ,cast(null as varchar(30))   as prefix
        ,cast(0 as bit)              as last_12m
    into model_numbers
    from sk_prod.cust_set_top_box as stb
         inner join #active       as act on stb.account_number = act.account_number
   where box_replaced_dt = '9999-09-09'
group by service_instance_id
        ,stb.account_number
        ,account_sub_type
;

create hg index idx1 on model_numbers(subscriber_id);
create hg index idx2 on model_numbers(service_instance_id);

update model_numbers as bas
   set account_type = prod_ph_subs_account_sub_type
  from sk_prod.cust_single_account_view as sav
 where bas.account_number = sav.account_number
;

select *
      ,null as sub_type
 INTO #stb from (select service_instance_id
                       ,ph_non_subs_link_sk
                       ,x_model_number
                       ,current_product_description
                       ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) as rank
                   from sk_prod.cust_set_top_box) as sub
 where rank = 1
;

create hg index idx3 on #stb(service_instance_id);

update #stb
   set sub_type = subscription_sub_type
  from sk_prod.cust_subs_hist as csh
 where #stb.service_instance_id = csh.service_instance_id
;

update model_numbers as mod
   set mod.model_number = #stb.x_model_number
      ,mod.box_type     = #stb.current_product_description
  from #stb
 where mod.service_instance_id = #stb.service_instance_id
;

update model_numbers as mod
   set mod.subscriber_id = csi.si_external_identifier
  from sk_prod.cust_service_instance as csi
 where mod.service_instance_id = csi.src_system_id
;

  select subscriber_id
        ,max(dt) as latest_date
        ,null as prefix
    into #latest_prefix
    from callback_data
group by subscriber_id
; --15,346,677

  update #latest_prefix as lat
     set lat.prefix = cal.prefix
    from callback_data as cal
   where cal.dt = lat.latest_date
     and cal.subscriber_id = lat.subscriber_id
;

create variable @max_date date;
select @max_date = max(latest_date) from #latest_prefix;

update model_numbers as mod
   set mod.prefix = cast(pfx.prefix as varchar)
  from #latest_prefix as pfx
 where cast(mod.subscriber_id as int) = pfx.subscriber_id
   and mod.subscriber_id is not null
   and mod.subscriber_id <> '0'
;

--check
select count(1) from model_numbers as mod
inner join #latest_prefix as pfx on cast(mod.subscriber_id as int) = pfx.subscriber_id


update model_numbers as mod
   set last_12m = 1
  from #latest_prefix as pfx
 where cast(mod.subscriber_id as int) = pfx.subscriber_id
   and pfx.latest_date >= @max_date - 366
;

  select model_number
        ,account_type
        ,case when account_type in ('Staff','Normal','VIP') then account_type
              when account_type = '?'                       then 'Normal'
              else                                               'Other' end as type_summarised
        ,prefix
        ,last_12m
        ,box_type
        ,case when box_type like '%+%HD%' then 'Sky+HD' else 'Other' end as box_summarised
        ,count(1) as box_count
    from model_numbers
group by model_number
        ,account_type
        ,type_summarised
        ,prefix
        ,last_12m
        ,box_type
        ,box_summarised
;

---




select prefix,count(1) from model_numbers group by prefix
select box_type,count(1) from model_numbers group by box_type
select model_number,count(1) from model_numbers group by model_number
select top 10 * from #stb
select count(1) from model_numbers
13,468,023
select count(1) from #latest_prefix
15,346,677
select count(1) from model_numbers where prefix is null
11,383,866




redo account_sub_type
add ontime,missing cbacks

