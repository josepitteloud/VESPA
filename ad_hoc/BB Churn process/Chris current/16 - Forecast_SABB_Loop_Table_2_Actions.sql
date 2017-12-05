text
create procedure spencerc2.Forecast_SABB_Loop_Table_2_Actions( 
  in Counter integer,
  in Rate_Multiplier real ) 
sql security invoker
begin
  declare multiplier bigint;
  declare multiplier_2 bigint;
  message cast(now() as timestamp) || ' | Forecast_SABB_Loop_Table_2_Actions - Initializing' to client;
  set multiplier = DATEPART(millisecond,now())+1;
  set multiplier_2 = DATEPART(millisecond,now())+2;
  drop table if exists spencerc2.intraweek_movements;
  drop table if exists weekly_movements;
  ---??? temporary update of churn type so that we can check movements when this fully popoulated
  /*	UPDATE Forecast_Loop_Table_2 AS a
SET churn_type = 'SysCan'

FROM Forecast_Loop_Table_2 AS a
where churn_type is null;
*/
  ---??? end of temp code
  --------------------------------------------------------------------------------------------------------------
  -- Predicted rates -------------------------------------------------------------------------------------------
  --------------------------------------------------------------------------------------------------------------
  --- rates ----
  update Forecast_Loop_Table_2 as a
    set pred_bb_enter_SysCan_rate = Coalesce(b.pred_SysCan_rate,0),
    pred_bb_enter_CusCan_rate = Coalesce(b.pred_CusCan_rate,0),
    pred_bb_enter_HM_rate = Coalesce(b.pred_HM_rate,0),
    pred_bb_enter_3rd_party_rate = Coalesce(b.pred_3rd_party_rate,0),
    pred_BB_Offer_Applied_rate = Coalesce(b.pred_BB_Offer_Applied_rate,0) from
    Forecast_Loop_Table_2 as a
    left outer join SABB_predicted_values as b on(a.subs_week_of_year = b.subs_week or(a.subs_week_of_year = 53 and b.subs_week = 52)) and a.sabb_forecast_segment = b.sabb_forecast_segment;
  ------ SysCan trend ------
  update Forecast_Loop_Table_2 as a
    set pred_bb_enter_SysCan_YoY_Trend = Coalesce(d.Grad_Coeff*4*(cast(counter-1 as real)/53),0) from
    Forecast_Loop_Table_2 as a
    left outer join Fcast_Regr_Coeffs as d on a.sabb_forecast_segment = d.fcast_segment
    --         and d.LV = Forecast_Start_Wk
    and d.Metric = 'SysCan Entry';
  ------ SysCan cum ----
  update Forecast_Loop_Table_2 as a
    set cum_bb_enter_SysCan_rate = case when pred_bb_enter_SysCan_rate+pred_bb_enter_SysCan_YoY_Trend <= 0 then 0 else pred_bb_enter_SysCan_rate+pred_bb_enter_SysCan_YoY_Trend end;
  ------ CusCan trend ------
  update Forecast_Loop_Table_2 as a
    set pred_bb_enter_CusCan_YoY_Trend = Coalesce(d.Grad_Coeff*4*(cast(counter-1 as real)/53),0) from
    Forecast_Loop_Table_2 as a
    left outer join Fcast_Regr_Coeffs as d on a.sabb_forecast_segment = d.fcast_segment
    --         and d.LV = Forecast_Start_Wk
    and d.Metric = 'CusCan Entry';
  ------ CusCan cum ----
  update Forecast_Loop_Table_2 as a
    set cum_bb_enter_CusCan_rate = case when pred_bb_enter_CusCan_rate+pred_bb_enter_CusCan_YoY_Trend <= 0 then 0 else pred_bb_enter_CusCan_rate+pred_bb_enter_CusCan_YoY_Trend end;
  ------ HM trend ------
  update Forecast_Loop_Table_2 as a
    set pred_bb_enter_HM_YoY_Trend = Coalesce(d.Grad_Coeff*4*(cast(counter-1 as real)/53),0) from
    Forecast_Loop_Table_2 as a
    left outer join Fcast_Regr_Coeffs as d on a.sabb_forecast_segment = d.fcast_segment
    --         and d.LV = Forecast_Start_Wk
    and d.Metric = 'HM Entry';
  ------ HM cum ----
  update Forecast_Loop_Table_2 as a
    set cum_bb_enter_HM_rate = case when pred_bb_enter_HM_rate+pred_bb_enter_HM_YoY_Trend <= 0 then 0 else pred_bb_enter_HM_rate+pred_bb_enter_HM_YoY_Trend end;
  ------ 3rd party trend ------
  update Forecast_Loop_Table_2 as a
    set pred_bb_enter_3rd_party_YoY_Trend = Coalesce(d.Grad_Coeff*4*(cast(counter-1 as real)/53),0) from
    Forecast_Loop_Table_2 as a
    left outer join Fcast_Regr_Coeffs as d on a.sabb_forecast_segment = d.fcast_segment
    --         and d.LV = Forecast_Start_Wk
    and d.Metric = '3rd Party Entry';
  ------ 3rd party cum ----
  update Forecast_Loop_Table_2 as a
    set cum_bb_enter_3rd_party_rate = case when pred_bb_enter_3rd_party_rate+pred_bb_enter_3rd_party_YoY_Trend <= 0 then 0 else pred_bb_enter_3rd_party_rate+pred_bb_enter_3rd_party_YoY_Trend end;
  ------ BB offer applied trend ------
  update Forecast_Loop_Table_2 as a
    set pred_BB_Offer_Applied_YoY_Trend = Coalesce(d.Grad_Coeff*4*(cast(counter-1 as real)/53),0) from
    Forecast_Loop_Table_2 as a
    left outer join Fcast_Regr_Coeffs as d on a.sabb_forecast_segment = d.fcast_segment
    --         and d.LV = Forecast_Start_Wk
    and d.Metric = 'BB Offer Applied';
  ------ BB offer applied cum ----
  update Forecast_Loop_Table_2 as a
    set cum_BB_Offer_Applied_rate
     = case when pred_BB_Offer_Applied_rate+pred_BB_Offer_Applied_YoY_Trend <= 0 then 0
    else pred_BB_Offer_Applied_rate+pred_BB_Offer_Applied_YoY_Trend
    end;
  message cast(now() as timestamp) || ' | Forecast_SABB_Loop_Table_2_Actions - Checkpoint 1 ' to client;
  ----???? it is possible that the trend value is not appropriate in these circumstances and that we should actually be using 
  --- a non trend value like this for the movements into pipeline:
  --- set cum_bb_enter_SysCan_rate = pred_bb_enter_SysCan_rate
  --- we will review at an appropriate time!
  --- don't think we need this section:
  --------------------------------------------------------------------------------------------------------------
  -- TA/WC Volumes, Saves & Offers Applied  --------------------------------------------------------------------
  --------------------------------------------------------------------------------------------------------------
  --??? have deleted this - look at previous code to restore
  --------------------------------------------------------------------------------------------------------------
  -- Pending Cancels -------------------------------------------------------------------------------------------
  --------------------------------------------------------------------------------------------------------------
  --- pred DTV_PC ----
  ---??? I don't think we need the conversion rate because we have used the calling rate functionality to drive the conversion to PC so far.  Therefore we use the rates thath come out of there (probably the cum rates)
  ---- ??? therefore commenting this out for now
  /*
Update Forecast_Loop_Table_2 as a
Set pred_TA_DTV_PC_rate = b.TA_DTV_PC_Conv_Rate
,pred_TA_Sky_Plus_Save_rate = b.TA_SkyPlus_Save_Rate
,pred_WC_DTV_PC_rate = b.WC_DTV_PC_Conv_Rate
,pred_WC_Sky_Plus_Save_rate = b.WC_SkyPlus_Save_Rate
,pred_Other_DTV_PC_rate = Coalesce(b.Other_DTV_PC_Conv_Rate,0)
from Forecast_Loop_Table_2 as a
inner join
TA_DTV_PC_Vol as b
on a.cuscan_forecast_segment = b.cuscan_forecast_segment;
*/
  -- we already have
  -- cum_bb_enter_SysCan_rate	 
  -- cum_bb_enter_CusCan_rate	 
  -- cum_bb_enter_HM_rate	 
  -- cum_bb_enter_3rd_party_rate	 
  -- we will use these going forward ....
  ---??? still need to model SkyPlus Saves (wa sin original model next to TA, but is TA now out of scope?)
  ---?? BB_Offer_Applied needs work
  update Forecast_Loop_Table_2
    set rand_action_Pipeline = case when BB_status_code in( 'AB','BCRQ','PC' ) then 1 ----??? change this CASE to be whatever the scope of the deniminator was when creating the rates
    else null ---??? i'm not sure this correct, but let's get this running through first
    end;
  update Forecast_Loop_Table_2
    set rand_action_Pipeline = rand(number()*multiplier+4)
    where rand_action_Pipeline is null;
  drop table if exists
    #Pipeline_Rank;
  select account_number,
    rand_action_Pipeline,
    count() over(partition by SABB_Forecast_segment) as Total_Cust_In_SABB_Segment,
    cast(rank() over(partition by SABB_Forecast_segment order by rand_action_Pipeline asc) as real) as SABB_Group_rank,
    case when rand_action_Pipeline <= cum_bb_enter_SysCan_rate then 1 else 0 end as BB_SysCan,
    case when rand_action_Pipeline > cum_bb_enter_SysCan_rate and rand_action_Pipeline <= (cum_bb_enter_SysCan_rate+cum_bb_enter_CusCan_rate) then 1 else 0 end as BB_CusCan,
    case when rand_action_Pipeline > (cum_bb_enter_SysCan_rate+cum_bb_enter_CusCan_rate) and rand_action_Pipeline <= (cum_bb_enter_SysCan_rate+cum_bb_enter_CusCan_rate+cum_bb_enter_HM_rate) then 1 else 0 end as BB_HM,
    case when rand_action_Pipeline > (cum_bb_enter_SysCan_rate+cum_bb_enter_CusCan_rate+cum_bb_enter_HM_rate) and rand_action_Pipeline <= (cum_bb_enter_SysCan_rate+cum_bb_enter_CusCan_rate+cum_bb_enter_HM_rate+cum_bb_enter_3rd_party_rate) then 1 else 0 end as BB_3rd_Party
    into #Pipeline_Rank
    from Forecast_Loop_Table_2;
  commit work;
  message cast(now() as timestamp) || ' | Forecast_SABB_Loop_Table_2_Actions - Checkpoint 2 ' to client;
  create hg index idx_1 on #Pipeline_Rank(account_number);
  update Forecast_Loop_Table_2 as a
    set BB_SysCan = 1,
    churn_type = 'SysCan' from
    Forecast_Loop_Table_2 as a
    join #Pipeline_Rank as b on b.account_number = a.account_number and b.BB_SysCan = 1;
  update Forecast_Loop_Table_2 as a
    set BB_CusCan = 1,
    churn_type = 'CusCan' from
    Forecast_Loop_Table_2 as a
    join #Pipeline_Rank as b on b.account_number = a.account_number and b.BB_CusCan = 1;
  update Forecast_Loop_Table_2 as a
    set BB_HM = 1,
    churn_type = 'HM' from
    Forecast_Loop_Table_2 as a
    join #Pipeline_Rank as b on b.account_number = a.account_number and b.BB_HM = 1;
  update Forecast_Loop_Table_2 as a
    set BB_3rd_Party = 1,
    churn_type = '3rd Party' from
    Forecast_Loop_Table_2 as a
    join #Pipeline_Rank as b on b.account_number = a.account_number and b.BB_3rd_Party = 1;
  ---??? do the above clauses need to be restricted to active statuses?
  message cast(now() as timestamp) || ' | Forecast_SABB_Loop_Table_2_Actions - Checkpoint 3 ' to client;
  update Forecast_Loop_Table_2 as a
    set BB_Offer_Applied = 1
    where bb_status_code = 'AC' and bb_syscan = 0 and BB_CusCan = 0 and BB_HM = 0 and BB_3rd_party = 0
    and rand_BB_Offer_Applied <= pred_BB_Offer_Applied_rate+pred_BB_Offer_Applied_YoY_Trend
    and end_date between curr_offer_start_date_bb and curr_offer_end_date_intended_bb;
  ---??? I believe this should hold the offer BB_Offer_Applied_rate for statuses that stay at AC - therefore need to alter the definition in the pipeline rate proc to capture the BB_Offer_Applied only for these
  ---???? the following code will need to be tidied up and productionised so it works for every date and uses data in the right schema
  --- but this will give the intraweek percentiles in a format we can use:
  ---??? code starts here:
  select churn_type,
    case when status_code in( 'AB','BCRQ','PC' ) then 'PL' else status_code end as pseudo_status,
    case when next_status_code in( 'AB','PC','BCRQ' ) then 'PL' else next_status_code end as next_pseudo_status,
    AB_ReAC_offer_applied,
    sum(ABs) as Cnt,
    Row_number() over(partition by Churn_type order by pseudo_status asc) as Row_ID
    into #im
    from(select * from Intrawk_AB_Pct where next_status_code not in( 'AP' ) union
      select * from Intrawk_PC_Pct where next_status_code not in( 'AP' ) union
      select * from Intrawk_BCRQ_Pct where next_status_code not in( 'AP' ) ) as x
    group by churn_type,
    pseudo_status,
    next_pseudo_status,
    AB_ReAC_offer_applied
    order by churn_type asc,
    pseudo_status asc,
    next_pseudo_status asc,
    AB_ReAC_offer_applied asc;
  select Row_ID,
    churn_type,
    pseudo_status,
    next_pseudo_status,
    AB_ReAC_offer_applied,
    cnt,
    SUM(cnt) over(partition by churn_type order by Row_ID asc) as acum_abs,
    SUM(cnt) over(partition by churn_type) as acum_abs1,
    cast(acum_abs as real)/acum_abs1 as prob
    into #t1
    from #im;
  select t1.churn_type,
    t1.pseudo_status,
    t1.next_pseudo_status,
    t1.AB_ReAC_offer_applied as ReAC_Offer_Applied,
    t1.cnt,
    t1.acum_abs,
    t1.acum_abs1,
    COALESCE(t2.prob,0) as Lower,
    t1.prob as UPPER
    into intraweek_movements
    from #t1 as t1
      left outer join #t1 as t2 on t1.row_id = t2.row_id+1 and t1.Churn_type = t2.Churn_type;
  message cast(now() as timestamp) || ' | Forecast_SABB_Loop_Table_2_Actions - Checkpoint 4 ' to client;
  ---- This code takes the new entries to the pipeline on this loop from the four types and models further status movements within that same week;
  update Forecast_Loop_Table_2 as base
    set BB_Status_Code_EoW = AB.Next_pseudo_status,
    BB_Offer_Applied = COALESCE(AB.ReAC_Offer_Applied,0) from
    Forecast_Loop_Table_2 as base
    join(select * from intraweek_movements where churn_type = 'SysCan') as AB
    on base.rand_Intrawk_BB_SysCan between AB.lower and AB.upper
    where BB_SysCan > 0;
  update Forecast_Loop_Table_2 as base
    set BB_Status_Code_EoW = AB.Next_pseudo_status,
    BB_Offer_Applied = COALESCE(AB.ReAC_Offer_Applied,0) from
    Forecast_Loop_Table_2 as base
    join(select * from intraweek_movements where churn_type = 'CusCan') as AB
    on base.rand_Intrawk_BB_SysCan between AB.lower and AB.upper
    where BB_CusCan > 0;
  update Forecast_Loop_Table_2 as base
    set BB_Status_Code_EoW = AB.Next_pseudo_status,
    BB_Offer_Applied = COALESCE(AB.ReAC_Offer_Applied,0) from
    Forecast_Loop_Table_2 as base
    join(select * from intraweek_movements where churn_type = 'HM') as AB on base.rand_Intrawk_BB_SysCan between AB.lower and AB.upper
    where BB_HM > 0;
  update Forecast_Loop_Table_2 as base
    set BB_Status_Code_EoW = AB.Next_pseudo_status,
    BB_Offer_Applied = COALESCE(AB.ReAC_Offer_Applied,0) from
    Forecast_Loop_Table_2 as base
    join(select * from intraweek_movements where churn_type = '3rd Party') as AB on base.rand_Intrawk_BB_SysCan between AB.lower and AB.upper
    where BB_3rd_party > 0;
  --?? final update is that if eow status code is set to PL then set the eow status to a real pipeline status (doesn't really matter which one ) - signifying that they are still in the pipeline
  update Forecast_Loop_Table_2 as base
    set BB_Status_Code_EoW = case when BB_SysCan = 1 then 'AB'
    else 'PC'
    end from Forecast_Loop_Table_2 as base
    where base.BB_status_code_EoW = 'PL';
  message cast(now() as timestamp) || ' | Forecast_SABB_Loop_Table_2_Actions - Checkpoint 5 ' to client;
  ---???? the following code will need to be tidied up and productionised so it works for every date and uses data in the right schema
  --- but this will give the churn week status movement percentiles in a format we can use:
  ---??? code starts here:
  select churn_type,
    Wks_to_intended_churn,
    case when Status_Code_EoW in( 'AB','PC','BCRQ' ) then 'PL' else Status_Code_EoW end as next_pseudo_status_EoW,
    ReAC_offer_applied,
    sum(Cnt) as Cnt,
    Row_number() over(partition by Churn_type,Wks_to_intended_churn order by next_pseudo_status_EoW asc,ReAC_offer_applied asc) as Row_ID
    into #wm
    from(select * from PC_PL_Status_Change_Dist union
      select * from AB_PL_Status_Change_Dist) as x
    --??? extend this bad status filter?
    where(status_code_eow not in( 'PA' ) or(Wks_to_intended_churn = 'Churn in next 1 wks' and status_code_eow not in( 'AB','PC','BCRQ' ) ))
    group by churn_type,
    next_pseudo_status_EoW,
    ReAC_offer_applied,
    Wks_to_intended_churn
    order by churn_type asc,
    Wks_to_intended_churn asc,
    next_pseudo_status_EoW asc,
    ReAC_offer_applied asc;
  select Row_ID,
    churn_type,
    Wks_to_intended_churn,
    next_pseudo_status_EoW,
    ReAC_offer_applied,
    cnt,
    SUM(cnt) over(partition by churn_type,Wks_to_intended_churn order by Row_ID asc) as acum_abs,
    SUM(cnt) over(partition by churn_type,Wks_to_intended_churn) as acum_abs1,
    cast(acum_abs as real)/acum_abs1 as prob
    into #t2
    from #wm;
  select t1.churn_type,
    t1.Wks_to_intended_churn,
    t1.next_pseudo_status_EoW,
    t1.ReAC_offer_applied,
    t1.cnt,
    t1.acum_abs,
    t1.acum_abs1,
    COALESCE(t2.prob,0) as Lower_,
    t1.prob as UPPER_
    into weekly_movements
    from #t2 as t1
      left outer join #t2 as t2 on t1.row_id = t2.row_id+1 and t1.Churn_type = t2.Churn_type and t1.Wks_to_intended_churn = t2.Wks_to_intended_churn;
  ---??? code ends here
  message cast(now() as timestamp) || ' | Forecast_SABB_Loop_Table_2_Actions - Checkpoint 6 ' to client;
  update Forecast_Loop_Table_2 as base
    set BB_Status_Code_EoW = PC.next_pseudo_status_EoW,
    BB_Offer_Applied = ReAC_offer_applied from
    Forecast_Loop_Table_2 as base
    join weekly_movements as PC
    on base.rand_BB_pipeline_Status_Change between PC.lower_ and PC.upper_
    and trim(base.churn_type) = pc.churn_type
    and case when datediff(day,base.End_Date,base.PL_Future_Sub_Effective_Dt)/7 = 0 then 'Churn in next 1 wks'
    when datediff(day,base.End_Date,base.PL_Future_Sub_Effective_Dt)/7 = 1 then 'Churn in next 2 wks'
    when datediff(day,base.End_Date,base.PL_Future_Sub_Effective_Dt)/7 = 2 then 'Churn in next 3 wks'
    when datediff(day,base.End_Date,base.PL_Future_Sub_Effective_Dt)/7 = 3 then 'Churn in next 4 wks'
    when datediff(day,base.End_Date,base.PL_Future_Sub_Effective_Dt)/7 = 4 then 'Churn in next 5 wks'
    when datediff(day,base.End_Date,base.PL_Future_Sub_Effective_Dt)/7 >= 5 then 'Churn in next 6+ wks' end
    --           ??? what are we going to call DTV_PC_Future_Sub_Effective_Dt?
     = PC.Wks_To_Intended_Churn
    where BB_Status_Code in( 'PC','BCRQ','AB' )  ---??? not very happy at referring to PC, AB and BCRQ here!
    and bb_syscan = 0 and BB_CusCan = 0 and BB_HM = 0 and BB_3rd_party = 0
    and trim(base.churn_type) in( 'CusCan','HM','3rd Party' ) ;
  -- ??? does this make sense to change like this?  -- is this correct?
  ---??? need a join to make the cuscan, syscan, 3rd party, HM all join to the right sections.  If we do ths we can get rid of the section below
  ---??? check all the names in the code above!
  update Forecast_Loop_Table_2 as base
    set BB_Status_Code_EoW = AB.next_pseudo_status_EoW,
    BB_Offer_Applied = AB.ReAC_offer_applied from
    Forecast_Loop_Table_2 as base
    join weekly_movements as AB ---??? obviously need this table
    on base.rand_BB_pipeline_Status_Change between AB.lower_ and AB.upper_
    and trim(base.churn_type) = ab.churn_type
    and case when datediff(day,base.End_Date,base.PL_Future_Sub_Effective_Dt)/7 = 0 then 'Churn in next 1 wks'
    when datediff(day,base.End_Date,base.PL_Future_Sub_Effective_Dt)/7 = 1 then 'Churn in next 2 wks'
    when datediff(day,base.End_Date,base.PL_Future_Sub_Effective_Dt)/7 = 2 then 'Churn in next 3 wks'
    when datediff(day,base.End_Date,base.PL_Future_Sub_Effective_Dt)/7 = 3 then 'Churn in next 4 wks'
    when datediff(day,base.End_Date,base.PL_Future_Sub_Effective_Dt)/7 = 4 then 'Churn in next 4 wks'
    when datediff(day,base.End_Date,base.PL_Future_Sub_Effective_Dt)/7 = 5 then 'Churn in next 6 wks'
    when datediff(day,base.End_Date,base.PL_Future_Sub_Effective_Dt)/7 = 6 then 'Churn in next 7 wks'
    when datediff(day,base.End_Date,base.PL_Future_Sub_Effective_Dt)/7 = 7 then 'Churn in next 8 wks'
    when datediff(day,base.End_Date,base.PL_Future_Sub_Effective_Dt)/7 = 8 then 'Churn in next 9 wks'
    when datediff(day,base.End_Date,base.PL_Future_Sub_Effective_Dt)/7 >= 9 then 'Churn in next 10+ wks' end
     = AB.Wks_To_Intended_Churn
    where BB_Status_Code in( 'PC','BCRQ','AB' ) and trim(base.churn_type) in( 'SysCan' ) 
    and bb_syscan = 0 and BB_CusCan = 0 and BB_HM = 0 and BB_3rd_party = 0;
  --?? final update is that if eow status code is set to PL then set the eow status to the current status (i.e. nothing has changed)
  update Forecast_Loop_Table_2 as base
    set BB_Status_Code_EoW = case when BB_SysCan = 1 then 'AB'
    when BB_CusCan = 1 or BB_HM = 1 or BB_3rd_Party = 1 then 'PC'
    else BB_Status_Code
    end from Forecast_Loop_Table_2 as base
    where base.BB_status_code_EoW = 'PL';
  message cast(now() as timestamp) || ' | Forecast_SABB_Loop_Table_2_Actions - Checkpoint 7 ' to client;
  ---??? check all the names in the code above!
  update Forecast_Loop_Table_2 as base
    set CusCan = 1
    where BB_Status_Code_EoW = 'CN'
    and trim(churn_type) = 'CusCan';
  --	AND BB_CusCan > 0;
  update Forecast_Loop_Table_2 as base
    set SysCan = 1
    where BB_Status_Code_EoW = 'CN'
    and trim(churn_type) = 'SysCan';
  --	AND BB_SysCan > 0;
  update Forecast_Loop_Table_2 as base
    set HM = 1
    where BB_Status_Code_EoW = 'CN'
    and trim(churn_type) = 'HM';
  --AND BB_HM > 0;
  update Forecast_Loop_Table_2 as base
    set _3rd_Party = 1
    where BB_Status_Code_EoW = 'CN'
    and trim(churn_type) = '3rd Party'
--	AND BB_3rd_party > 0;
/*xx*/
end