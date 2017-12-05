text
create procedure ------------------------------------------------------------------------------------------------------------------------------
-- Procedure to calc rates for customers moving from PC to another status ----------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------
pitteloudj.PC_Status_Movement_Probabilities( in ForeCAST_Start_Week integer ) 
result( 
  churn_type varchar(10),
  Initial_status_code varchar(10),
  Wks_To_Intended_Churn varchar(20),
  Status_Code_EoW varchar(4),
  Status_Code_EoW_Rnk integer,
  PC_ReAC_Offer_Applied tinyint,
  PCs integer,
  Cum_Total_Cohort_PCs integer,
  Total_Cohort_PCs integer,
  PC_Percentile_Lower_Bound real,
  PC_Percentile_Upper_Bound real ) 
begin
  message cast(now() as timestamp) || ' | PC_Status_Movement_Probabilities - Initialising Environment' to client;
  select * into #Sky_Calendar from CITeam.Subs_Calendar(ForeCAST_Start_Week/100-1,ForeCAST_Start_Week/100);
  select MoR.account_number,
    MoR.status_code,
    MoR.event_dt,
    MoR.PC_Future_Sub_Effective_Dt,
    cast(MoR.PC_Future_Sub_Effective_Dt-datepart(weekday,MoR.PC_Future_Sub_Effective_Dt+2)+7 as date) as PC_Future_Sub_Effective_Dt_End_Dt,
    MoR.PC_Effective_To_Dt,
    MoR.PC_Next_status_code as Next_status_code,
    case when oua.offer_id is not null then 1 else 0 end as PC_ReAC_Offer_Applied,
    case when MoR.Enter_SysCan > 0 then 'SysCan'
    when MoR.Enter_CusCan > 0 then 'CusCan'
    when MoR.Enter_HM > 0 then 'HM'
    when MoR.Enter_3rd_Party > 0 then '3rd Party'
    else null
    end as Churn_type
    into #PC_Intended_Churn
    from CITEAM.Broadband_Comms_Pipeline as mor
      left outer join offer_usage_all as oua on oua.account_number = mor.account_number
      and oua.offer_Start_Dt_Actual = MoR.PC_Effective_To_Dt
      and MoR.PC_Next_Status_Code = 'AC'
      and oua.offer_Start_Dt_Actual = oua.Whole_offer_Start_Dt_Actual
      and lower(oua.offer_dim_description) not like '%price protection%'
      and oua.subs_type = 'Broadband DSL Line'
    where MoR.PC_Future_Sub_Effective_Dt between(select min(calendar_date-6*7) from #sky_calendar where subs_week_and_year = ForeCAST_Start_Week) -- Last 6 Wk PC conversions
    and(select min(calendar_date-1) from #sky_calendar where subs_week_and_year = ForeCAST_Start_Week)
    and(MoR.status_code in( 'PC' ) or(MoR.status_code in( 'BCRQ' ) and churn_type in( 'CusCan','3rd Party','HM' ) ))
    and MoR.PC_Future_Sub_Effective_Dt is not null
    and Next_status_code is not null
    and MoR.PC_Effective_To_Dt <= MoR.PC_Future_Sub_Effective_Dt;
  select PCs.*,
    case when(cast(PC_Future_Sub_Effective_Dt as integer)-cast(End_Date as integer))/7 = 0 then 'Churn in next 1 wks'
    when(cast(PC_Future_Sub_Effective_Dt as integer)-cast(End_Date as integer))/7 = 1 then 'Churn in next 2 wks'
    when(cast(PC_Future_Sub_Effective_Dt as integer)-cast(End_Date as integer))/7 = 2 then 'Churn in next 3 wks'
    when(cast(PC_Future_Sub_Effective_Dt as integer)-cast(End_Date as integer))/7 = 3 then 'Churn in next 4 wks'
    when(cast(PC_Future_Sub_Effective_Dt as integer)-cast(End_Date as integer))/7 = 4 then 'Churn in next 5 wks'
    when(cast(PC_Future_Sub_Effective_Dt as integer)-cast(End_Date as integer))/7 >= 5 then 'Churn in next 6+ wks' end as Wks_To_Intended_Churn,
    sc.Calendar_date as End_date,
    case when sc.calendar_date+7 between event_dt and PC_Effective_To_Dt then 'PC'
    when sc.calendar_date+7 between PC_Effective_To_Dt and PC_Future_Sub_Effective_Dt_End_Dt then Next_Status_Code end as Status_Code_EoW,
    case when sc.calendar_date+7 between PC_Effective_To_Dt and PC_Future_Sub_Effective_Dt_End_Dt
    and Status_Code_EoW = 'AC' then PCs.PC_ReAC_Offer_Applied else 0 end as PC_ReAC_Offer_Applied_EoW,
    (case Status_Code_EoW when 'AC' then 1
    when 'CN' then 2
    when 'BCRQ' then 3
    when 'PO' then 4
    when 'AB' then 5
    when 'SC' then 6 end)-PC_ReAC_Offer_Applied_EoW as Status_Code_EoW_Rnk
    into #PC_PL_Status
    from #PC_Intended_Churn as PCs
      join #sky_calendar as sc on sc.calendar_date between PCs.event_dt and PCs.PC_Effective_To_Dt-1 and sc.subs_last_day_of_week = 'Y';
  select churn_type,
    status_code,
    Wks_To_Intended_Churn,
    Status_Code_EoW,
    Status_Code_EoW_Rnk,
    PC_ReAC_Offer_Applied_EoW,
    count() as PCs,
    SUM(PCs) over(partition by Wks_To_Intended_Churn order by Status_Code_EoW_Rnk asc) as Cum_Total_Cohort_PCs,
    SUM(PCs) over(partition by Wks_To_Intended_Churn) as Total_Cohort_PCs,
    cast(null as real) as PC_Percentile_Lower_Bound,
    cast(Cum_Total_Cohort_PCs as real)/Total_Cohort_PCs as PC_Percentile_Upper_Bound
    into #PC_Percentiles
    from #PC_PL_Status
    group by status_code,
    Wks_To_Intended_Churn,
    Status_Code_EoW_Rnk,
    Status_Code_EoW,
    PC_ReAC_Offer_Applied_EoW,
    churn_type
    order by Wks_To_Intended_Churn asc,
    Status_Code_EoW_Rnk asc,
    Status_Code_EoW asc,
    PC_ReAC_Offer_Applied_EoW asc,
    churn_type asc;
  message cast(now() as timestamp) || ' | PC_Status_Movement_Probabilities - PC_Percentiles Populated: ' || @@rowcount to client;
  update #PC_Percentiles as pcp
    set PC_Percentile_Lower_Bound = cast(Coalesce(pcp2.PC_Percentile_Upper_Bound,0) as real) from
    #PC_Percentiles as pcp
    left outer join #PC_Percentiles as pcp2 on pcp2.Wks_To_Intended_Churn = pcp.Wks_To_Intended_Churn and pcp2.Status_Code_EoW_Rnk = pcp.Status_Code_EoW_Rnk-1;
  message cast(now() as timestamp) || ' | PC_Status_Movement_Probabilities - Initialising Completed' to client;
  select * from #PC_Percentiles
end