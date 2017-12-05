create procedure Intraweek_ABs_Dist( 
  in ForeCAST_Start_Week integer ) 
result( 
  Churn_type varchar(10),
  Status_Code varchar(4),
  Next_Status_Code varchar(4),
  AB_ReAC_Offer_Applied tinyint,
  ABs integer,
  IntaWk_AB_Lower_Pctl real,IntaWk_AB_Upper_Pctl real ) 
begin
  message cast(now() as timestamp) || ' | Intraweek_ABs_Dist - Initialization begin ' to client;
  select * into #Sky_Calendar from Citeam.subs_calendar(ForeCAST_Start_Week/100-1,ForeCAST_Start_Week/100);
  select mor.subs_week_and_year,
    mor.event_dt,
    mor.event_dt-datepart(weekday,event_dt+2) as AB_Event_End_Dt,
    mor.AB_Effective_To_Dt,
    mor.AB_Effective_To_Dt-datepart(weekday,mor.AB_Effective_To_Dt+2) as AB_Effective_To_End_Dt,
    mor.account_number,
    MoR.AB_Next_Status_Code as Next_Status_Code,
    case when oua.offer_id is not null then 1 else 0 end as AB_ReAC_Offer_Applied,
    case when Enter_SysCan > 0 then 'SysCan'
    when Enter_CusCan > 0 then 'CusCan'
    when Enter_HM > 0 then 'HM'
    when Enter_3rd_Party > 0 then '3rd Party'
    else null
    end as Churn_type into #Acc_AB_Events_Same_Week
    from citeam.Broadband_Comms_Pipeline as MoR
      left outer join offer_usage_all as oua on oua.account_number = mor.account_number
      and oua.offer_Start_Dt_Actual = MoR.AB_Effective_To_Dt
      and MoR.AB_Next_Status_Code = 'AC'
      and oua.offer_Start_Dt_Actual = oua.Whole_offer_Start_Dt_Actual
      and lower(oua.offer_dim_description) not like '%price protection%'
      and oua.subs_type = 'Broadband DSL Line'
    where mor.event_dt between(select max(calendar_date-6-5*7) from #sky_calendar where subs_week_and_year = ForeCAST_Start_Week)
    and(select max(calendar_date) from #sky_calendar where subs_week_and_year = ForeCAST_Start_Week)
    and Mor.status_code = 'AB';
  select Churn_type,
    Coalesce(case when AB_Effective_To_End_Dt = AB_Event_End_Dt then MoR.Next_Status_Code else null end,'AB') as Next_Status_Code,
    cast(case when Next_Status_Code = 'AC' and AB_ReAC_Offer_Applied = 0 then 1
    when Next_Status_Code = 'AC' and AB_ReAC_Offer_Applied = 1 then 2
    when Next_Status_Code = 'CN' then 3
    when Next_Status_Code = 'BCRQ' then 4
    when Next_Status_Code = 'PC' then 5
    when Next_Status_Code = 'PO' then 6
    when Next_Status_Code = 'SC' then 7
    else 0
    end as integer) as Next_Status_Code_Rnk,cast(case when AB_Effective_To_End_Dt = AB_Event_End_Dt then MoR.AB_ReAC_Offer_Applied else 0 end as integer) as AB_ReAC_Offer_Applied,
    Row_number() over(partition by Churn_type order by Next_Status_Code_Rnk asc) as Row_ID,
    count() as ABs
    into #AB_Events_Same_Week
    from #Acc_AB_Events_Same_Week as MoR
    group by Next_Status_Code,
    AB_ReAC_Offer_Applied,
    Churn_type;
  drop table #Acc_AB_Events_Same_Week;
  select Row_ID,
    Churn_type,
    Next_Status_Code,
    AB_ReAC_Offer_Applied,
    ABs,
    sum(ABs) over(partition by Churn_type order by Row_ID asc) as Cum_ABs,
    sum(ABs) over(partition by Churn_type) as Total_ABs,
    cast(Cum_ABs as real)/Total_ABs as IntaWk_PC_Upper_Pctl
    into #AB_Events
    from #AB_Events_Same_Week as pc1
    group by Row_ID,
    Next_Status_Code,
    AB_ReAC_Offer_Applied,
    ABs,
    Churn_type;
  drop table #AB_Events_Same_Week;
  select pc1.Churn_type,
    'AB' as Status_code,
    pc1.Next_Status_Code,
    pc1.AB_ReAC_Offer_Applied,
    pc1.ABs,
    Coalesce(pc2.IntaWk_PC_Upper_Pctl,0) as IntaWk_PC_Lower_Pctl,
    pc1.IntaWk_PC_Upper_Pctl
    from #AB_Events as pc1
      left outer join #AB_Events as pc2 on pc2.row_id = pc1.row_id-1 and pc1.Churn_type = pc2.Churn_type;
  message cast(now() as timestamp) || ' | Intraweek_ABs_Dist - Completed' to client
end