text
create procedure pitteloudj.AB_Status_Movement_Probabilities( in @ForeCAST_Start_Week integer ) 
result( 
  Churn_type varchar(10),
  Status_Code varchar(4),
  Wks_To_Intended_Churn varchar(20),
  Status_Code_EoW varchar(4),
  Status_Code_EoW_Rnk integer,
  AB_ReAC_Offer_Applied tinyint,
  AB_s integer,
  Cum_Total_Cohort_ABs integer,
  Total_Cohort_ABs integer,
  AB_Percentile_Lower_Bound real,
  AB_Percentile_Upper_Bound real ) 
begin
  message cast(now() as timestamp) || ' | AB_Status_Movement_Probabilities - Initialization Begin' to client;
  select * into #Sky_Calendar from CITeam.Subs_Calendar(@ForeCAST_Start_Week/100-1,@ForeCAST_Start_Week/100);
  select mor.account_number,
    mor.status_code,
    mor.event_dt,
    mor.AB_Future_Sub_Effective_Dt,
    cast(mor.AB_Future_Sub_Effective_Dt-datepart(weekday,mor.AB_Future_Sub_Effective_Dt+2)+7 as date) as AB_Future_Sub_Effective_Dt_End_Dt,
    mor.AB_Effective_To_Dt,
    mor.AB_Next_status_code as Next_status_code,
    case when oua.offer_id is not null then 1 else 0 end as AB_ReAC_Offer_Applied,
    case when mor.Enter_SysCan > 0 then 'SysCan'
    when mor.Enter_CusCan > 0 then 'CusCan'
    when mor.Enter_HM > 0 then 'HM'
    when mor.Enter_3rd_Party > 0 then '3rd Party'
    else null
    end as Churn_type into #AB_Intended_Churn
    from citeam.Broadband_Comms_Pipeline as mor
      left outer join offer_usage_all as oua on oua.account_number = mor.account_number
      and oua.offer_Start_Dt_Actual = MoR.AB_Effective_To_Dt
      and MoR.AB_Next_Status_Code = 'AC'
      and oua.offer_Start_Dt_Actual = oua.Whole_offer_Start_Dt_Actual
      and lower(oua.offer_dim_description) not like '%price protection%'
      and oua.subs_type = 'Broadband DSL Line'
    where AB_Future_Sub_Effective_Dt between(select min(calendar_date-6*7) from #sky_calendar where subs_week_and_year = @ForeCAST_Start_Week) -- Last 6 Wk PC conversions
    and(select min(calendar_date-1) from #sky_calendar where subs_week_and_year = @ForeCAST_Start_Week)
    and AB_Future_Sub_Effective_Dt is not null
    and AB_Next_status_code is not null
    and AB_Effective_To_Dt <= AB_Future_Sub_Effective_Dt
    and(status_code = 'AB' or(status_code = 'BCRQ' and Churn_type = 'SysCan'));
  ---------------------------------------------------------------------------------------------------------------
  ----------		UPDATE to flag BCRQ to CN accounts
  ---------------------------------------------------------------------------------------------------------------
  message cast(now() as timestamp) || ' | AB_Status_Movement_Probabilities - UPDATE to flag BCRQ to CN accounts Begin' to client;
  select a.account_number,
    a.event_dt,
    b.status_code as next_cancel_status,
    b.effective_from_dt as next_cancel_dt,
    RANK() over(partition by a.account_number order by b.effective_from_dt asc,b.cb_row_id asc) as rankk
    into #AB_BCRQ
    from #AB_Intended_Churn as a
      join cust_subs_hist as b on a.account_number = b.account_number and a.AB_Effective_To_Dt <= b.effective_from_dt
    where b.subscription_sub_type = 'Broadband DSL Line'
    and b.status_code_changed = 'Y'
    and b.status_code in( 'PO','SC','CN' ) 
    and a.Next_status_code in( 'BCRQ' ) 
    and b.effective_from_dt <> b.effective_to_dt
    and b.prev_status_code in( 'BCRQ' ) ;
  message cast(now() as timestamp) || ' | AB_Status_Movement_Probabilities - UPDATE to flag BCRQ to CN accounts checkpoint 1/2' to client;
  delete from #AB_BCRQ where rankk > 1;
  update #AB_Intended_Churn as a
    set AB_Future_Sub_Effective_Dt = next_cancel_dt,
    Next_status_code = next_cancel_status from
    #AB_Intended_Churn as a
    join #AB_BCRQ as b on a.account_number = b.account_number and a.event_dt = b.event_dt and a.status_code = 'AB';
  drop table #AB_BCRQ;
  update #AB_Intended_Churn
    set Next_status_code = 'CN'
    where Next_status_code = 'BCRQ';
  message cast(now() as timestamp) || ' | AB_Status_Movement_Probabilities - UPDATE to flag BCRQ to CN accounts checkpoint 2/2' to client;
  ---------------------------------------------------------------------------------------------------------------
  ---------------------------------------------------------------------------------------------------------------				
  select AB_s.*,
    case when(cast(AB_Future_Sub_Effective_Dt as integer)-cast(End_Date as integer))/7 = 0 then 'Churn in next 1 wks'
    when(cast(AB_Future_Sub_Effective_Dt as integer)-cast(End_Date as integer))/7 = 1 then 'Churn in next 2 wks'
    when(cast(AB_Future_Sub_Effective_Dt as integer)-cast(End_Date as integer))/7 = 2 then 'Churn in next 3 wks'
    when(cast(AB_Future_Sub_Effective_Dt as integer)-cast(End_Date as integer))/7 = 3 then 'Churn in next 4 wks'
    when(cast(AB_Future_Sub_Effective_Dt as integer)-cast(End_Date as integer))/7 = 4 then 'Churn in next 5 wks'
    when(cast(AB_Future_Sub_Effective_Dt as integer)-cast(End_Date as integer))/7 = 5 then 'Churn in next 6 wks'
    when(cast(AB_Future_Sub_Effective_Dt as integer)-cast(End_Date as integer))/7 = 6 then 'Churn in next 7 wks'
    when(cast(AB_Future_Sub_Effective_Dt as integer)-cast(End_Date as integer))/7 = 7 then 'Churn in next 8 wks'
    when(cast(AB_Future_Sub_Effective_Dt as integer)-cast(End_Date as integer))/7 = 8 then 'Churn in next 9 wks'
    when(cast(AB_Future_Sub_Effective_Dt as integer)-cast(End_Date as integer))/7 >= 9 then 'Churn in next 10+ wks' end as Wks_To_Intended_Churn,
    sc.Calendar_date as End_date,
    case when sc.calendar_date+7 between event_dt and AB_Effective_To_Dt then 'AB'
    when sc.calendar_date+7 between AB_Effective_To_Dt and AB_Future_Sub_Effective_Dt_End_Dt then Next_Status_Code end as Status_Code_EoW,
    case when sc.calendar_date+7 = AB_Effective_To_Dt-datepart(weekday,AB_Effective_To_Dt+2)+7
    and Status_Code_EoW = 'AC' then AB_s.AB_ReAC_Offer_Applied
    else 0
    end as AB_ReAC_Offer_Applied_EoW,(case when Status_Code_EoW = 'AC' and AB_ReAC_Offer_Applied = 0 then 1
    when Status_Code_EoW = 'AC' and AB_ReAC_Offer_Applied = 1 then 2
    when Status_Code_EoW = 'CN' then 3
    when Status_Code_EoW = 'BCRQ' then 4
    when Status_Code_EoW = 'PC' then 5
    when Status_Code_EoW = 'PO' then 6
    when Status_Code_EoW = 'SC' then 7
    else 0
    end) as Status_Code_EoW_Rnk into #AB_PL_Status
    from #AB_Intended_Churn as AB_s
      join #sky_calendar as sc on sc.calendar_date between AB_s.event_dt and AB_s.AB_Effective_To_Dt-1
      and sc.subs_last_day_of_week = 'Y';
  select Wks_To_Intended_Churn,
    Status_Code_EoW,
    Status_Code_EoW_Rnk,
    AB_ReAC_Offer_Applied_EoW,
    count() as AB_s,
    Sum(AB_s) over(partition by Wks_To_Intended_Churn,Churn_type order by Status_Code_EoW_Rnk asc) as Cum_Total_Cohort_ABs,
    Sum(AB_s) over(partition by Wks_To_Intended_Churn,Churn_type) as Total_Cohort_ABs,
    cast(null as real) as AB_Percentile_Lower_Bound,
    cast(Cum_Total_Cohort_ABs as real)/Total_Cohort_ABs as AB_Percentile_Upper_Bound,
    Row_Number() over(partition by Wks_To_Intended_Churn,Churn_type order by Status_Code_EoW_Rnk asc) as Row_ID,
    Churn_type,
    status_code
    into #AB_Percentiles
    from #AB_PL_Status
    group by Wks_To_Intended_Churn,
    Status_Code_EoW_Rnk,
    Status_Code_EoW,
    AB_ReAC_Offer_Applied_EoW,
    Churn_type,
    status_code
    order by status_code asc,
    Churn_type asc,
    Wks_To_Intended_Churn asc,
    Status_Code_EoW_Rnk asc,
    Status_Code_EoW asc,
    AB_ReAC_Offer_Applied_EoW asc;
  message cast(now() as timestamp) || ' | AB_Status_Movement_Probabilities - AB_Percentiles populated: ' || @@rowcount to client;
  update #AB_Percentiles as pcp
    set AB_Percentile_Lower_Bound = cast(Coalesce(pcp2.AB_Percentile_Upper_Bound,0) as real) from
    #AB_Percentiles as pcp
    left outer join #AB_Percentiles as pcp2 on pcp2.Wks_To_Intended_Churn = pcp.Wks_To_Intended_Churn and pcp2.Row_ID = pcp.Row_ID-1 and pcp.Churn_type = pcp2.Churn_type;
  select Churn_type,
    status_code,
    Wks_To_Intended_Churn,
    Status_Code_EoW,
    Status_Code_EoW_Rnk,
    AB_ReAC_Offer_Applied_EoW,
    AB_s,
    Cum_Total_Cohort_ABs,
    Total_Cohort_ABs,
    AB_Percentile_Lower_Bound,
    AB_Percentile_Upper_Bound
    from #AB_Percentiles;
  message cast(now() as timestamp) || ' | AB_Status_Movement_Probabilities - Completed' to client
end