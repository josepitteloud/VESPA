  select *
        ,201404 as [Reference]
    into etl_scoring_partial19
    from yarlagaddar.view_cust_Calls_hist
   where event_dt >= '2013-01-01'
     and TypeOfEvent = 'IC'
;

  select account_number
        ,max(event_dt) as Date_of_Last_IC_call
    into etl_scoring_partial21
    from etl_scoring_partial20
   where event_dt > [2_Years_Prior]
     and event_dt <= Snapshot_Date
     and event_dt >= '2013-01-01'
group by account_number
;

  select *
        ,case when dtv_first_act_date >  '2013-06-30' then '<10_Months'
              when dtv_first_act_date <= '2013-06-30' and dtv_first_act_date > '2012-04-30' then '10-24_Months'
              when dtv_first_act_date <= '2012-04-30' and sum_unstable_flags = 0 then '24_Months+'
              else 'Unstable' end as Segment
        ,'2014-04-30' as snapshot_date
    into etl_scoring_partial49
    from etl_scoring_partial48
;

  select *
        ,datediff(month, dtv_first_act_date,  snapshot_date) as Total_Tenure_Months
        ,datediff(month, dtv_latest_act_date, snapshot_date) as Current_Tenure_Months
    into modeling_scoring_partial9
    from modeling_scoring_partial8
;

  select *
        ,datediff(month, Date_of_Last_TA_Call, snapshot_date) as Months_Since_Last_TA_Call
    into modeling_scoring_partial11
    from modeling_scoring_partial10
;

  select *
        ,datediff(month, Date_of_Last_PAT_Call, snapshot_date) as Months_Since_Last_PAT_Call
    into modeling_scoring_partial13
    from modeling_scoring_partial12
;

  select *
        ,datediff(month, Date_of_Last_IC_call, snapshot_date) as Months_Since_Last_IC_Call
    into modeling_scoring_partial15
    from modeling_scoring_partial14
;


      -- checks
  select count(1) from vespa_analysts.SkyBase_TA_scores
  select count(1) from TA_scores_20140407
  select min([$lrp-ta_in_3-6_months_flag]) from TA_scores_20140407
  select min(ta_propensity) from vespa_analysts.SkyBase_TA_scores
  select max([$lrp-ta_in_3-6_months_flag]) from TA_scores_20140407;
  select max(ta_propensity) from vespa_analysts.SkyBase_TA_scores;
  select sum(case when [$lrp-ta_in_3-6_months_flag] between 0.6 and 0.7 then 1 else 0 end) from TA_scores_20140407;
  select sum(case when ta_propensity                between 0.6 and 0.7 then 1 else 0 end) from vespa_analysts.SkyBase_TA_scores;

  select n.account_number,ta_propensity as o,[$lrp-ta_in_3-6_months_flag] as n
    into #results
    from vespa_analysts.SkyBase_TA_scores as o
         inner join TA_scores_20140407 as n on o.account_number =n.account_number

      -- absolute change
  select cast(((n-o) * 100) as int) as diff
        ,count(1)
    from #results
group by diff

      -- % change
  select cast(((n-o)/o) * 100 as int) as diff
        ,count(1)
    from #results
group by diff

      -- new
  select segment
        ,account_number
        ,[$lrp-ta_in_3-6_months_flag] as ta
        ,cast(0 as bit) as panel
        ,cast(0 as bit) as high_rq
    into #temp
    from TA_scores_20140407
;

  update #temp as bas
     set panel = 1
    from vespa_analysts.vespa_single_box_view as sbv
   where bas.account_number = sbv.account_number
     and panel_id_vespa in (5, 6, 7, 11, 12)
     and status_vespa = 'Enabled'
;

  update #temp as bas
     set high_rq = 1
    from vespa_analysts.panbal_sav as sav
   where bas.account_number = sav.account_number
     and viq_rq >= .5
     and bas.panel = 1
;

  select segment
        ,count(1) as accounts
        ,sum(ta) as ta_tot
        ,ta_tot/ accounts * 1.0 as pc
        ,sum(panel) as panel_acc
        ,sum(panel * ta) as panel_ta
        ,panel_ta/ta_tot * 1.0 as pane_pc
        ,sum(high_rq * ta) as hirq
        ,hirq/ta_tot * 1.0 as hirq_pc
    from #temp
group by segment
;

      -- old
  select segment
        ,account_number
        ,[ta_propensity] as ta
        ,cast(0 as bit) as panel
        ,cast(0 as bit) as high_rq
        ,cast(0 as bit) as current_cust
        ,cast(0 as bit) as cancel_attempt
    into #tempo
    from vespa_analysts.SkyBase_TA_scores
;

  update #tempo as bas
     set panel = 1
    from vespa_analysts.vespa_single_box_view as sbv
   where bas.account_number = sbv.account_number
     and panel_id_vespa in (5, 6, 7, 11, 12)
     and status_vespa = 'Enabled'
;

  update #tempo as bas
     set high_rq = 1
    from vespa_analysts.panbal_sav as sav
   where bas.account_number = sav.account_number
     and viq_rq >= .5
     and bas.panel = 1
;

  update #tempo as bas
     set current_cust = 1
    from cust_single_account_view as sav
   where bas.account_number = sav.account_number
     and cust_active_dtv = 1
;

  update #tempo as bas
     set cancel_attempt = 1
    from cust_change_attempt as cca
   where bas.account_number = cca.account_number
    and change_attempt_type = 'CANCELLATION ATTEMPT'
    and attempt_date >= '2014-05-01'
;

  select segment
        ,count(1) as accounts
        ,sum(ta) as ta_tot
        ,ta_tot/ accounts * 1.0 as pc
        ,sum(panel) as panel_acc
        ,sum(panel * ta) as panel_ta
        ,panel_ta/ta_tot * 1.0 as pane_pc
        ,sum(high_rq * ta) as hirq
        ,hirq/ta_tot * 1.0 as hirq_pc
    from #tempo
group by segment
;

  select cast(ta * 100 as int) as ta
        ,count(1)
        ,sum(current_cust)
        ,sum(cancel_attempt)
    from #tempo
group by ta
;

select count(1) from vespa_analysts.SkyBase_TA_scores_hist

--When happy with the results, and the file is not being used, run the following to update the table:

/*
  insert into vespa_analysts.SkyBase_TA_scores_hist(account_number
                                                   ,segment
                                                   ,ta_propensity
                                                   ,replaced_dt)
  select account_number
        ,segment
        ,ta_propensity
        ,today() as replaced_dt
    from vespa_analysts.SkyBase_TA_scores
;

  delete from vespa_analysts.SkyBase_TA_scores;

  insert into vespa_analysts.SkyBase_TA_scores(account_number
                                              ,segment
                                              ,ta_propensity)
  select account_number
        ,segment
        ,[$lrp-ta_in_3-6_months_flag]
    from TA_scores_20140407
;

commit;
*/






