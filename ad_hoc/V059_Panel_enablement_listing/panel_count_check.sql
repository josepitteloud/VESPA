  select account_number
        ,cast(card_subscriber_id as int) as subscriber_id
        ,cast(0 as bit) as anytimeplus_enabled
        ,cast(0 as bit) as anytimeplus_active
        ,cast(0 as bit) as last_7days
        ,panel_no
    into #anytimepluscheck
    from sk_prod.VESPA_SUBSCRIBER_STATUS
   where result='Enabled'
; --1,948,320

  select account_number
        ,service_instance_id
        ,cast(0 as int) as subscriber_id
    into #anytimeplus
    from (select account_number, service_instance_id, active_box_flag,x_anytime_plus_enabled
                ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) as active_flag
            from sk_prod.cust_set_top_box) as sub
   where active_flag = 1
    and x_anytime_plus_enabled='Y'
;

commit;
create hg index idx1 on #anytimeplus(service_instance_id);

  update #anytimeplus as bas
     set bas.subscriber_id = coalesce(cast(csi.si_external_identifier as int),0)
    from sk_prod.cust_service_instance as csi
   where bas.service_instance_id = csi.src_system_id
; --9134506 9153437

  update #anytimepluscheck as bas
     set anytimeplus_enabled = 1
    from #anytimeplus as sub
   where bas.subscriber_id = sub.subscriber_id
; --1141042 1142188

--accounts that have A+ activated
SELECT     account_number
into #aplus_accounts
FROM       sk_prod.CUST_SUBS_HIST
WHERE      subscription_sub_type='PDL subscriptions'  --anytime plus subscription
AND        status_code='AC'
AND        first_activation_dt<'9999-09-09'         -- (END)
AND        first_activation_dt>='2010-10-01'        -- (START) Oct 2010 was the soft launch of A+, no one should have it before then
AND        account_number is not null
AND        account_number <> '?'
GROUP BY   account_number
; --3,066,541

  update #anytimepluscheck as bas
     set anytimeplus_active = 1
    from #aplus_accounts as acc
   where acc.account_number = bas.account_number
;

--dialled back in the last 7 days
  select subscriber_id
    into logs_dump
    from sk_prod.VESPA_events_viewed_all
   where convert(date, dateadd(hh, -6, event_start_date_time_utc)) >= convert(date,dateadd(day, -7, now()))
group by subscriber_id
;

  update #anytimepluscheck as bas
     set last_7days = 1
    from logs_dump as dum
   where bas.subscriber_id = dum.subscriber_id
;

--box
  select panel_no
        ,count(1)
        ,sum(anytimeplus_enabled)
        ,sum(case when anytimeplus_enabled = 1 and anytimeplus_active = 1                    then 1 else 0 end) as active
        ,sum(case when anytimeplus_enabled = 1 and anytimeplus_active = 1 and last_7days = 1 then 1 else 0 end) as dialled_back
    from #anytimepluscheck
group by panel_no
;

--account
  select panel_no
        ,count(1)
        ,sum(enabled) as enabled_
        ,sum(case when enabled = 1 and active=1 then 1 else 0 end) as active_
        ,sum(case when enabled = 1 and active = 1 and last_7days = 1 then 1 else 0 end) as dialled_back
   from (select account_number
               ,panel_no
               ,max(anytimeplus_enabled) as enabled
               ,max(anytimeplus_active)  as active
               ,max(last_7days)  as last_7days
           from #anytimepluscheck
       group by account_number
               ,panel_no) as sub
group by panel_no






---



