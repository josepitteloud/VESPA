text
create procedure spencerc2.PC_Duration_Dist( in ForeCAST_Start_Week integer ) 
result( 
  churn_type varchar(10),
  Days_To_churn integer,
  PCs integer,
  Total_PCs integer,
  PC_Days_Lower_Prcntl real,
  PC_Days_Upper_Prcntl real ) 
begin
  message cast(now() as timestamp) || ' | PC_Duration_Dist - BEGIN ' to client;
  select * into #Sky_Calendar from Citeam.subs_calendar(ForeCAST_Start_Week/100-1,ForeCAST_Start_Week/100);
  select case when status_code in( 'PC' ) then 1
    when status_code in( 'BCRQ' ) and churn_type in( 'CusCan','3rd Party','HM' ) then 2
    else 0
    end as source,event_dt-datepart(weekday,event_dt+2) as PC_Event_End_Dt,
    case when source = 1 then PC_Effective_To_Dt-datepart(weekday,PC_Effective_To_Dt+2)
    when source = 2 then PC_Effective_To_Dt-datepart(weekday,PC_Effective_To_Dt+2)
    else null
    end as PC_Effective_To_End_Dt,case when source = 1 then PC_Future_Sub_Effective_Dt-datepart(weekday,PC_Future_Sub_Effective_Dt+2)
    when source = 2 then PC_Future_Sub_Effective_Dt-datepart(weekday,PC_Future_Sub_Effective_Dt+2)
    else null
    end as PC_Future_Sub_End_Dt,PC_Future_Sub_Effective_Dt-PC_Event_End_Dt as Days_To_churn,
    case when Enter_SysCan > 0 then 'SysCan'
    when Enter_CusCan > 0 then 'CusCan'
    when Enter_HM > 0 then 'HM'
    when Enter_3rd_Party > 0 then '3rd Party'
    else null
    end as Churn_type into #PC_Events_Days_To_Intended_Churn
    from citeam.Broadband_Comms_Pipeline
    where event_dt between(select max(calendar_date-6*7+1) from #sky_calendar where subs_week_and_year = ForeCAST_Start_Week)
    and(select max(calendar_date) from #sky_calendar where subs_week_and_year = ForeCAST_Start_Week)
    and(status_code in( 'PC' ) or(status_code in( 'BCRQ' ) and churn_type in( 'CusCan','3rd Party','HM' ) ))
    and Days_To_churn > 0;
  select churn_type,
    Days_To_churn,
    Row_number() over(partition by churn_type order by Days_To_churn asc) as Row_ID,
    count() as PCs,
    SUM(PCs) over(partition by churn_type) as Total_PCs,
    SUM(PCs) over(partition by churn_type order by Days_To_churn asc) as Cum_PCs,
    cast(PCs as real)/Total_PCs as Pct_PCs,
    cast(null as real) as PC_Days_Lower_Prcntl,
    cast(Cum_PCs as real)/Total_PCs as PC_Days_Upper_Prcntl
    into #PC_Days_Prcntl
    from #PC_Events_Days_To_Intended_Churn
    group by Days_To_churn,churn_type
    order by churn_type asc,Days_To_churn asc;
  update #PC_Days_Prcntl as pc1
    set pc1.PC_Days_Lower_Prcntl = Coalesce(pc2.PC_Days_Upper_Prcntl,0) from
    #PC_Days_Prcntl as pc1
    left outer join #PC_Days_Prcntl as pc2 on pc2.Row_ID = pc1.Row_ID-1;
  select churn_type,
    Days_To_churn,
    PCs,
    Total_PCs,
    PC_Days_Lower_Prcntl,
    PC_Days_Upper_Prcntl
    from #PC_Days_Prcntl;
  message cast(now() as timestamp) || ' | PC_Duration_Dist - BEGIN ' to client
end