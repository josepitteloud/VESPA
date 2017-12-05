text
create procedure pitteloudj.Intraweek_PCs_Dist( in ForeCAST_Start_Week integer ) 
result( 
  Churn_type varchar(10),
  Status_code varchar(4),
  Next_Status_Code varchar(4),
  PC_ReAC_Offer_Applied tinyint,
  PCs integer,
  IntaWk_PC_Lower_Pctl real,
  IntaWk_PC_Upper_Pctl real ) 
begin
  message cast(now() as timestamp) || ' | Intraweek_PCs_Dist - Initialization begin ' to client;
  select * into #Sky_Calendar from Citeam.subs_calendar(ForeCAST_Start_Week/100-1,ForeCAST_Start_Week/100);
  select mor.subs_week_and_year,
    mor.event_dt,
    mor.event_dt-datepart(weekday,mor.event_dt+2) as PC_Event_End_Dt,
    mor.PC_Effective_To_Dt,
    mor.PC_Effective_To_Dt-datepart(weekday,mor.PC_Effective_To_Dt+2) as PC_Effective_To_End_Dt,
    mor.account_number,
    -- ,csh.status_code Next_Status_Code1
    MoR.PC_Next_Status_Code as Next_Status_Code,
    case when oua.offer_id is not null then 1 else 0 end as PC_ReAC_Offer_Applied,
    case when Enter_SysCan > 0 then 'SysCan'
    when Enter_CusCan > 0 then 'CusCan'
    when Enter_HM > 0 then 'HM'
    when Enter_3rd_Party > 0 then '3rd Party'
    else null
    end as Churn_type into #Acc_PC_Events_Same_Week
    from citeam.Broadband_Comms_Pipeline as MoR
      left outer join offer_usage_all as oua on oua.account_number = mor.account_number
      and oua.offer_Start_Dt_Actual = MoR.PC_Effective_To_Dt
      and MoR.PC_Next_Status_Code = 'AC'
      and oua.offer_Start_Dt_Actual = oua.Whole_offer_Start_Dt_Actual
      and lower(oua.offer_dim_description) not like '%price protection%'
      and oua.subs_type = 'Broadband DSL Line'
    where mor.event_dt between(select max(calendar_date-6-5*7) from #sky_calendar where subs_week_and_year = ForeCAST_Start_Week) -- Last 6 Wk PC conversions
    and(select max(calendar_date) from #sky_calendar where subs_week_and_year = ForeCAST_Start_Week)
    and mor.status_code = 'PC';
  --AND (Same_Day_Cancels > 0 OR PC_Pending_Cancellations > 0 OR Same_Day_PC_Reactivations > 0);
  select Coalesce(case when PC_Effective_To_End_Dt = PC_Event_End_Dt then MoR.Next_Status_Code else null end,'PC') as Next_Status_Code,
    cast(case Next_Status_Code when 'AC' then 1
    when 'CN' then 2
    when 'BCRQ' then 3
    when 'AB' then 4
    when 'SC' then 5
    when 'PO' then 5
    else 0
    end as integer) as Next_Status_Code_Rnk,cast(case when PC_Effective_To_End_Dt = PC_Event_End_Dt then MoR.PC_ReAC_Offer_Applied else 0 end as integer) as PC_ReAC_Offer_Applied,
    Row_number() over(partition by churn_type order by Next_Status_Code_Rnk asc,PC_ReAC_Offer_Applied asc) as Row_ID,
    churn_type,
    COUNT() as PCs
    into #PC_Events_Same_Week
    from #Acc_PC_Events_Same_Week as MoR
    group by Next_Status_Code,
    PC_ReAC_Offer_Applied,
    churn_type;
  select Row_ID,
    Next_Status_Code,
    PC_ReAC_Offer_Applied,
    PCs,
    churn_type,
    SUM(PCs) over(partition by churn_type order by Row_ID asc) as Cum_PCs,
    SUM(PCs) over(partition by churn_type) as Total_PCs,
    cast(Cum_PCs as real)/Total_PCs as IntaWk_PC_Upper_Pctl
    into #PC_Events
    from #PC_Events_Same_Week as pc1
    group by Row_ID,
    Next_Status_Code,
    PC_ReAC_Offer_Applied,
    PCs,
    churn_type;
  select pc1.churn_type,
    'PC' as Status_code,
    pc1.Next_Status_Code,
    pc1.PC_ReAC_Offer_Applied,
    pc1.PCs,
    Coalesce(pc2.IntaWk_PC_Upper_Pctl,0) as IntaWk_PC_Lower_Pctl,
    pc1.IntaWk_PC_Upper_Pctl
    from #PC_Events as pc1
      left outer join #PC_Events as pc2 on pc2.row_id = pc1.row_id-1 and pc1.churn_type = pc2.churn_type;
  message cast(now() as timestamp) || ' | Intraweek_PCs_Dist - COMPLETED' to client
end