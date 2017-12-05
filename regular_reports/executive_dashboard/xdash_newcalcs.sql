      -- run panel balancing daily metrics
 execute ('  call V352_M01_PanBal(@max_imb          = 20
                                 ,@min_boxes        = 600000
                                 ,@max_boxes        = 10000000
                                 ,@min_vp1b         = 300000
                                 ,@min_ta_prop      = .25
                                 ,@min_ta_returning = .12
                                 ,@prec             = 500
                                 ,@gen_schema       = 0
                                 ,@run_type         = 0        --default 0 for daily metrics, 1 for full balancing
                                 ,@country_manager  = 0        --0 for UK, 1 for ROI
                                 ,@max_pstn         = 1000000
                                 )')
;

      -- refresh SBV
 execute SBV_refresh_single_box_view;

  create table #results(
         metric varchar(50)
        ,value float
        )
;

      -- personalised services consent
  select account_number
        ,min(case when cust_viewing_data_capture_allowed = 'Y' then 1 else 0 end) as consent
    into #sav
    from cust_single_account_view
   where cust_active_dtv = 1
group by account_number
;

  insert into #results
  select 'personalised services consent'
        ,sum(consent)
    from #sav
;

      -- personalised services consent %
  insert into #results
  select 'personalised services consent %'
        ,sum(consent) * 1.0 / count()
    from #sav
;

  create variable @maxdt date;

  select @maxdt = max(effective_from_dt)
    from cust_subs_hist
;

  select distinct(account_number)
    into #new_custs
    from cust_subs_hist
   where subscription_sub_type = 'DTV Primary Viewing'
     and status_code in ('AC','AB','PC')
     and effective_from_dt >=  dateadd(month,-6,@maxdt)
     and effective_to_dt = '9999-09-09'
;

  commit;
  create unique hg index uhacc on #new_custs(account_number);

      -- personalised services consent % for new customers
  insert into #results
  select 'personalised services consent % for new customers'
        ,sum(consent) * 1.0 / count()
    from #sav as sav
         inner join #new_custs as nec on sav.account_number = nec.account_number
;

      -- viewing panel
      -- accounts enabled
  insert into #results
  select 'viewing panel accounts enabled'
        ,count(distinct account_number)
    from vespa_panel_status_manual
   where panel_no in (10, 11, 12)
;

      -- accounts returning data
  insert into #results
  select 'viewing panel accounts returning data'
        ,count(distinct account_number)
    from stb_connection_fact
   where data_return_reliability_metric > 0
     and panel_id_reported in (10, 11, 12)
;

      -- average RQ of accounts returning data
  select account_number
        ,min(data_return_reliability_metric) as rq
    into #rq
    from stb_connection_fact
   where panel_id_reported in (10, 11, 12)
group by account_number
  having rq > 0
;

  insert into #results
  select 'viewing panel average RQ of accounts returning data'
        ,avg(rq) / 30
    from #rq
;

      -- panel balance
  insert into #results
  select 'panel balance'
        ,max(value)
    from panbal_metrics
   where metric like 'Current / %'
     and metric not in (
         'Current / ESS from VIQ'
        ,'Current / Primary panel accounts'
        ,'Current / Primary panel boxes'
        ,'Current / Combined panels boxes'
        ,'Current / Combined panels accounts'
        )
;

      -- all panels
      -- accounts enabled
  insert into #results
  select 'all panels accounts enabled'
        ,count(distinct account_number)
    from vespa_panel_status_manual
;

      -- accounts returning data
  insert into #results
  select 'all panels accounts returning data'
        ,count(distinct account_number)
    from stb_connection_fact
   where data_return_reliability_metric > 0
;

      -- average RQ of accounts returning data
  select account_number
        ,min(data_return_reliability_metric) as rq
    into #rq_all_panels
    from stb_connection_fact
group by account_number
  having rq > 0
;

  insert into #results
  select 'all panels average RQ of accounts returning data'
        ,avg(rq) / 30
    from #rq_all_panels
;

      -- virtual panel accounts returning data
  insert into #results
  select 'virtual panels accounts returning data'
        ,count(distinct account_number)
    from   (select stb.account_number
                  ,min(data_return_reliability_metric) as rq
              from stb_connection_fact as stb
                   inner join panbal_sav as sav on sav.account_number = stb.account_number
             where vp1 = 1
          group by stb.account_number) as sub
   where rq >= 0
;

      -- results
  select * from #results;


select count(distinct account_number)
              from stb_connection_fact as stb
where data_return_reliability_metric>0

select top 10 *




