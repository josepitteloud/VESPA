text
create procedure spencerc2.Intraweek_BCRQ_Dist( in ForeCAST_Start_Week integer ) 
result( 
  Churn_type varchar(10),
  Status_Code varchar(4),
  Next_Status_Code varchar(4),
  BCRQ_ReAC_Offer_Applied tinyint,
  BCRQ integer,
  IntaWk_BCRQ_Lower_Pctl real,
  IntaWk_BCRQ_Upper_Pctl real ) 
begin
  select * into #Sky_Calendar from Citeam.subs_calendar(ForeCAST_Start_Week/100-1,ForeCAST_Start_Week/100);
  select mor.subs_week_and_year,
    mor.event_dt,
    mor.event_dt-datepart(weekday,event_dt+2) as BCRQ_Event_End_Dt,
    mor.BCRQ_Effective_To_Dt,
    mor.BCRQ_Effective_To_Dt-datepart(weekday,mor.BCRQ_Effective_To_Dt+2) as BCRQ_Effective_To_End_Dt,
    mor.account_number,
    MoR.BCRQ_Next_Status_Code as Next_Status_Code,
    case when oua.offer_id is not null then 1 else 0 end as BCRQ_ReAC_Offer_Applied,
    case when Enter_SysCan > 0 then 'SysCan'
    when Enter_CusCan > 0 then 'CusCan'
    when Enter_HM > 0 then 'HM'
    when Enter_3rd_Party > 0 then '3rd Party'
    else null
    end as Churn_type into #Acc_BCRQ_Events_Same_Week
    from citeam.Broadband_Comms_Pipeline as MoR
      left outer join offer_usage_all as oua on oua.account_number = mor.account_number
      and oua.offer_Start_Dt_Actual = MoR.PC_Effective_To_Dt
      and MoR.PC_Next_Status_Code = 'AC'
      and oua.offer_Start_Dt_Actual = oua.Whole_offer_Start_Dt_Actual
      and lower(oua.offer_dim_description) not like '%price protection%'
      and oua.subs_type = 'Broadband DSL Line'
    where mor.event_dt between(select max(calendar_date-6-5*7) from #sky_calendar where subs_week_and_year = ForeCAST_Start_Week)
    and(select max(calendar_date) from #sky_calendar where subs_week_and_year = ForeCAST_Start_Week)
    and mor.status_code = 'BCRQ';
  select Coalesce(case when BCRQ_Effective_To_End_Dt = BCRQ_Event_End_Dt then MoR.Next_Status_Code else null end,'BCRQ') as Next_Status_Code,
    cast(case Next_Status_Code when 'AC' then 1
    when 'CN' then 2
    when 'BCRQ' then 3
    when 'AB' then 4
    when 'SC' then 5
    when 'PO' then 5
    else 0
    end as integer) as Next_Status_Code_Rnk,cast(case when BCRQ_Effective_To_End_Dt = BCRQ_Event_End_Dt then MoR.BCRQ_ReAC_Offer_Applied else 0 end as integer) as BCRQ_ReAC_Offer_Applied,
    Row_number() over(partition by churn_type order by Next_Status_Code_Rnk asc,BCRQ_ReAC_Offer_Applied asc) as Row_ID,
    churn_type,
    COUNT() as BCRQs
    into #BCRQ_Events_Same_Week
    from #Acc_BCRQ_Events_Same_Week as MoR
    group by Next_Status_Code,
    BCRQ_ReAC_Offer_Applied,
    churn_type;
  select Row_ID,
    Next_Status_Code,
    BCRQ_ReAC_Offer_Applied,
    BCRQs,
    churn_type,
    SUM(BCRQs) over(partition by churn_type order by Row_ID asc) as Cum_BCRQs,
    SUM(BCRQs) over(partition by churn_type) as Total_BCRQs,
    cast(Cum_BCRQs as real)/Total_BCRQs as IntaWk_BCRQ_Upper_Pctl
    into #BCRQ_Events
    from #BCRQ_Events_Same_Week as pc1
    group by Row_ID,
    Next_Status_Code,
    BCRQ_ReAC_Offer_Applied,
    BCRQs,
    churn_type;
  select pc1.churn_type,
    'BCRQ' as Status_code,
    pc1.Next_Status_Code,
    pc1.BCRQ_ReAC_Offer_Applied,
    pc1.BCRQs,
    Coalesce(pc2.IntaWk_BCRQ_Upper_Pctl,0) as IntaWk_BCRQ_Lower_Pctl,
    pc1.IntaWk_BCRQ_Upper_Pctl
    from #BCRQ_Events as pc1
      left outer join #BCRQ_Events as pc2 on pc2.row_id = pc1.row_id-1 and pc1.churn_type = pc2.churn_type
end