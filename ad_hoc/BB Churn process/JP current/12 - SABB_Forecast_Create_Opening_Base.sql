text
create procedure pitteloudj.SABB_Forecast_Create_Opening_Base( @Forecast_Start_Wk integer,@sample_pct real ) 
as
begin
  message convert(timestamp,now()) || ' | Forecast_Create_Opening_Base - Begining - Initialising Environment' to client
  if not exists(select tname from syscatalog where creator = user_name() and tabletype = 'TABLE' and upper(tname) = UPPER('FORECAST_Base_Sample'))
    begin
      create table pitteloudj.FORECAST_Base_Sample(
        account_number varchar(20) null default null,
        end_date datetime null default null,
        subs_year integer null default null,
        subs_week_and_year integer null default null,
        subs_week_of_year tinyint null default null,
        weekid bigint null default null,
        BB_Status_Code varchar(4) null default null,
        BB_Segment varchar(30) null default null,
        country varchar(3) null default null,
        BB_package varchar(50) null default null,
        churn_type varchar(10) null default null,
        BB_offer_rem_and_end_raw integer null default-9999,
        BB_offer_rem_and_end integer null default-9999,
        BB_tenure_raw integer null default 0,
        BB_tenure integer null default 0,
        my_sky_login_3m_raw integer null default 0,
        my_sky_login_3m integer null default 0,
        talk_type varchar(30) null default 'NONE',
        home_owner_status varchar(20) null default 'UNKNOWN',
        BB_all_calls_1m_raw integer null default 0,
        BB_all_calls_1m integer null default 0,
        Simple_Segments varchar(13) null default 'UNKNOWN',
        node_SA tinyint null default 0,
        segment_SA varchar(20) null default 'UNKNOWN',
        PL_Future_Sub_Effective_Dt datetime null default null,
        DTV_Activation_Type varchar(100) null default null,
        Curr_Offer_start_Date_BB datetime null default null,
        Curr_offer_end_date_Intended_BB datetime null default null,
        Prev_offer_end_date_BB datetime null default null,
        Future_offer_Start_dt datetime null default null,
        Future_end_Start_dt datetime null default null,
        BB_latest_act_dt datetime null default null,
        BB_first_act_dt datetime null default null,
        rand_sample real null default null,
        sample varchar(10) null default null,
        SABB_flag bit not null default 0,
        )
      message convert(timestamp,now()) || ' | Forecast_Create_Opening_Base - FORECAST_Base_Sample' to client
    end
  declare @base_date date
  declare @true_sample_rate real
  declare @multiplier bigint
  set @multiplier = DATEPART(millisecond,now())+738
  set temporary option Query_Temp_Space_Limit = 0
  select * into #Sky_Calendar from Subs_Calendar(@Forecast_Start_Wk/100-1,@Forecast_Start_Wk/100)
  set @base_date = (select max(calendar_date-7) from #sky_calendar where subs_week_and_year = @Forecast_Start_Wk)
  set @multiplier = DATEPART(millisecond,now())+1
  message convert(timestamp,now()) || ' | Forecast_Create_Opening_Base - @base_date: ' || @base_date to client
  -- drop table if exists #base_sample
  delete from FORECAST_Base_Sample
  message convert(timestamp,now()) || ' | Forecast_Create_Opening_Base - Cleaning FORECAST_Base_Sample ' to client
  insert into FORECAST_Base_Sample
    select account_number, --
      end_date, --
      subs_year,
      'subs_week_and_year'=convert(integer,subs_week_and_year), --
      subs_week_of_year, --
      'weekid'=(subs_year-2010)*52+subs_week_of_year,
      BB_Status_Code, --- ??? we want this to hold the status at the time, so may be held in dtv_status_code at the moment - confirm
      'BB_Segment'=case when BB_Active > 0 then 'BB' else 'Non BB' end,
      'country'=case when ROI > 0 then 'ROI' else 'UK' end, --
      BB_package,
      'Churn_type'=case when BB_Enter_SysCan+BB_Enter_CusCan+BB_Enter_HM+BB_Enter_3rd_Party > 1 then 'MULTI' --- UPDATED next
      when BB_Enter_SysCan > 0 then 'SysCan'
      when BB_Enter_CusCan > 0 then 'CusCan'
      when BB_Enter_HM > 0 then 'HM'
      when BB_Enter_3rd_Party > 0 then '3rd Party'
      else null
      end, --- ========================================================================= --??? add in here the variables required to build the segments
      BB_offer_rem_and_end_raw, ----??? hold raw - not bucket
      'BB_offer_rem_and_end'=convert(integer,null),
      BB_tenure_raw, ----??? hold raw - not bucket
      'BB_tenure'=convert(integer,null),
      my_sky_login_3m_raw, ---??? doesn't this need to hold value for that week rather than the overall value?  We will generate the flag on th efly!
      'my_sky_login_3m'=convert(integer,null),
      talk_type,
      home_owner_status,
      BB_all_calls_1m_raw, ----??? hold raw - not bucket
      'BB_all_calls_1m'=convert(integer,null),
      'Simple_Segments'=case when trim(simple_segment) in( '1 Secure' ) then '1 Secure'
      when trim(simple_segment) in( '2 Start' ) then '2 Start'
      when trim(simple_segment) in( '3 Stimulate','2 Stimulate' ) then '3 Stimulate'
      when trim(simple_segment) in( '4 Support','3 Support' ) then '4 Support'
      when trim(simple_segment) in( '5 Stabilise','4 Stabilise' ) then '5 Stabilise'
      when trim(simple_segment) in( '6 Suspense','5 Suspense' ) then '6 Suspense'
      else 'Other/Unknown' -- ??? check the simple segment coding here that cleans this up, but generally looks ok
      end,'node_SA'=convert(tinyint,0),
      'segment_SA'=convert(varchar(20),null),
      'PL_Future_Sub_Effective_Dt'=convert(date,null),
      'DTV_Activation_Type'=convert(varchar(100),null), ---??? this does what?
      --- ??? we will need something that allows the offer ends times to be manipulated
      Curr_Offer_start_Date_BB, ---??? dont we need something like this?
      curr_offer_end_date_Intended_BB, ---??? dont we need something like this?
      Prev_offer_end_date_BB, ---??? dont we need something like this?
      'Future_offer_Start_dt'=convert(date,null),
      'Future_end_Start_dt'=convert(date,null),
      BB_latest_act_dt, --##### BB_TENURE RAW ???? ---??? this does what?	
      BB_first_act_dt,
      'rand_sample'=rand(number()*@multiplier),
      'sample'=convert(varchar(10),null),
      'SABB_flag'=case when bb_active = 1 and dtv_active = 0 then 1 else 0 end
      from pitteloudj.cust_fcast_weekly_base_2
      where end_date = @base_date
      and bb_active = 1 and dtv_active = 0 --??? do we need a sabb flag?
      and BB_latest_act_dt is not null --??? do we have this, or a bb_act_date?
  --???? changes to the where clause here?
  message convert(timestamp,now()) || ' | Forecast_Create_Opening_Base - Insert Into FORECAST_Base_Sample completed: ' || @@rowcount to client
  commit work
  select a.account_number,
    a.end_date,
    a.subs_year,
    a.subs_week_of_year,
    'Churn_type'=case when b.Enter_SysCan > 0 then 'SysCan'
    when b.Enter_CusCan > 0 then 'CusCan'
    when b.Enter_HM > 0 then 'HM'
    when b.Enter_3rd_Party > 0 then '3rd Party'
    else null
    end,'week_rnk'=RANK() over(partition by a.account_number,a.end_date order by b.event_dt desc)
    into #t1
    from FORECAST_Base_Sample as a
      join CITEAM.Broadband_Comms_Pipeline as b on a.account_number = b.account_number
      and a.subs_year = b.subs_year
      and a.subs_week_of_year = b.subs_week_of_year
    where a.Churn_type = 'MULTI'
  commit work
  delete from #t1 where week_rnk > 1
  create hg index IO1 on #t1(account_number)
  create dttm index IO2 on #t1(end_date)
  commit work
  update FORECAST_Base_Sample as a
    set a.Churn_type = b.Churn_type from
    FORECAST_Base_Sample as a
    join #t1 as b on a.account_number = b.account_number
    and a.end_date = b.end_date
  drop table #t1
  commit work
  update FORECAST_Base_Sample as a
    set a.BB_offer_rem_and_end = b.BB_offer_rem_and_end,
    a.BB_tenure = b.BB_tenure,
    a.my_sky_login_3m = b.my_sky_login_3m,
    a.BB_all_calls_1m = b.BB_all_calls_1m,
    a.node_SA = b.node_SA,
    a.segment_SA = b.segment_SA from
    FORECAST_Base_Sample as a
    join pitteloudj.DTV_FCAST_WEEKLY_BASE_2 as b on a.account_number = b.account_number
    and a.end_date = b.end_date
  message convert(timestamp,now()) || ' | Forecast_Create_Opening_Base - First Update FORECAST_Base_Sample completed: ' || @@rowcount to client
  ---????update this?
  update FORECAST_Base_Sample as sample
    set PL_Future_Sub_Effective_Dt = MoR.PC_Future_Sub_Effective_Dt from
    FORECAST_Base_Sample as sample
    join CITeam.Broadband_Comms_Pipeline as MoR on MoR.account_number = sample.account_number
    and MoR.PC_Future_Sub_Effective_Dt > sample.end_date
    and MoR.event_dt <= sample.end_date
    and(MoR.PC_effective_to_dt > sample.end_date or MoR.PC_effective_to_dt is null)
    where sample.BB_Status_Code = 'PC'
  ---????update this?
  update FORECAST_Base_Sample as sample
    set BB_Status_Code = 'AC'
    where BB_Status_Code = 'PC' and PL_Future_Sub_Effective_Dt is null ----??? looks like data cleanup for bad data
  ---????update this?
  ------------==================================++++++++++++++++++++++++++++++++==========================================---------------
  ------------==================================++++++++++++++++++++++++++++++++==========================================---------------
  ------------==================================++++++++++++++++++++++++++++++++==========================================---------------
  message convert(timestamp,now()) || ' | SABB_Forecast_Create_Opening_Base - UPDATE AB_Future_Sub_Effective_Dt Begin' to client
  select a.account_number,
    b.event_dt,
    a.BB_status_code,
    a.end_date,
    'rankk'=RANK() over(partition by a.account_number order by b.event_dt desc)
    into #AB_BCRQ_2
    from FORECAST_Base_Sample as a
      join CITeam.Broadband_Comms_Pipeline as b on a.account_number = b.account_number
      and b.event_dt <= a.end_date
    where a.BB_status_code in( 'AB','BCRQ' ) and PL_Future_Sub_Effective_Dt is null
    and b.enter_syscan = 1
  delete from #AB_BCRQ_2 where rankk > 1 ---- LATEST PL
  select a.account_number,
    a.event_dt,
    a.end_date,
    a.BB_status_code,
    'next_cancel_status'=b.status_code,
    'next_cancel_dt'=b.effective_from_dt,
    'rankk'=RANK() over(partition by a.account_number order by b.effective_from_dt asc,b.cb_row_id asc)
    into #AB_BCRQ_3
    from #AB_BCRQ_2 as a
      join cust_subs_hist as b on a.account_number = b.account_number and a.event_dt <= b.effective_from_dt
    where b.subscription_sub_type = 'Broadband DSL Line'
    and b.status_code_changed = 'Y'
    and b.effective_from_dt <> b.effective_to_dt
    and b.prev_status_code in( 'BCRQ' ) 
    and b.status_code in( 'CN','SC','PO' ) 
  delete from #AB_BCRQ_3 where rankk > 1 -- First cancellation after event_dt
  create hg index id1 on #AB_BCRQ_3(account_number)
  update FORECAST_Base_Sample as a
    set PL_Future_Sub_Effective_Dt = next_cancel_dt from
    FORECAST_Base_Sample as a
    join #AB_BCRQ_3 as b on a.account_number = b.account_number and a.end_date = b.end_date
  drop table #AB_BCRQ_3
  message convert(timestamp,now()) || ' | SABB_Forecast_Create_Opening_Base -  UPDATE AB_Future_Sub_Effective_Dt checkpoint 1/2' to client
  ----------------------------------------------------------------
  ------------- Accounts in the pipeline 
  ----------------------------------------------------------------
  select a.account_number,
    a.end_date,
    b.event_dt,
    'randx'=RAND(convert(integer,RIGHT(a.account_number,6))*DATEPART(ms,GETDATE())),
    'rankk'=RANK() over(partition by a.account_number order by b.event_dt desc)
    into #AB_2
    from FORECAST_Base_Sample as a
      join CITeam.Broadband_Comms_Pipeline as b on a.account_number = b.account_number
      and b.event_dt <= a.end_date
    where PL_Future_Sub_Effective_Dt is null and a.BB_status_code in( 'AB' ) 
    and b.enter_syscan = 1
  delete from #AB_2 where rankk > 1
  select *,
    'next_cancel_dt'=case when randx <= .25 then DATEADD(day,15,event_dt)
    when randx between .25 and .79 then DATEADD(day,60,event_dt)
    when randx >= .79 then DATEADD(day,65,event_dt)
    else event_dt
    end into #AB_3
    from #AB_2
  update FORECAST_Base_Sample as a
    set PL_Future_Sub_Effective_Dt = next_cancel_dt from
    FORECAST_Base_Sample as a
    join #AB_3 as b on a.account_number = b.account_number and a.end_date = b.end_date
  message convert(timestamp,now()) || ' | SABB_Forecast_Create_Opening_Base -  UPDATE AB_Future_Sub_Effective_Dt checkpoint 2/2' to client
  ------------==================================++++++++++++++++++++++++++++++++==========================================---------------
  ------------==================================++++++++++++++++++++++++++++++++==========================================---------------
  ------------==================================++++++++++++++++++++++++++++++++==========================================---------------
  ------------==================================++++++++++++++++++++++++++++++++==========================================---------------
  ----------------------------------------
  update FORECAST_Base_Sample as sample
    set PL_Future_Sub_Effective_Dt = MoR.BCRQ_Future_Sub_Effective_Dt from
    FORECAST_Base_Sample as sample
    join CITeam.Broadband_Comms_Pipeline as MoR on MoR.account_number = sample.account_number
    and MoR.AB_Future_Sub_Effective_Dt > sample.end_date
    and MoR.event_dt <= sample.end_date
    and(MoR.AB_effective_to_dt > sample.end_date or MoR.AB_effective_to_dt is null)
    where sample.BB_Status_Code = 'BCRQ'
  update FORECAST_Base_Sample as sample
    set BB_Status_Code = 'AC'
    where BB_Status_Code = 'BCRQ' and PL_Future_Sub_Effective_Dt is null
  --sample to speed up processing
  update FORECAST_Base_Sample
    set sample = case when rand_sample < @sample_pct then 'A' else 'B' end
-- Select subs_week_and_year, count(*) as n, count(distinct account_number) as d, n-d as dups from Forecast_Loop_Table group by subs_week_and_year
-- set @true_sample_rate = (select sum(case when sample='A' then cast(1 as float) else 0 end)/count(*) from #base_sample)
end
-- Grant execute rights to the members of CITeam
grant execute on pitteloudj.SABB_Forecast_Create_Opening_Base to CITeam,vespa_group_low_security
/*
-- Test it
Select top 1000 * from CITeam.Forecast_Create_Opening_Base(201601,0.25)

*/