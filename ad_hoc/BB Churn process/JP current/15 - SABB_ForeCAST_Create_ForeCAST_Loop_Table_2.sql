text
create procedure pitteloudj.SABB_ForeCAST_Create_ForeCAST_Loop_Table_2( 
  @ForeCAST_Start_Wk integer,
  @ForeCAST_End_Wk integer,
  @true_sample_rate real ) 
as
begin
  message convert(timestamp,now()) || ' | SABB_ForeCAST_Create_ForeCAST_Loop_Table_2 - Initializaing' to client
  set temporary option Query_Temp_Space_Limit = 0
  declare @multiplier bigint
  drop table if exists #Loop_Sky_Calendar
  drop table if exists pitteloudj.Pred_Rates
  drop table if exists pitteloudj.ForeCAST_Loop_Table_2
  set @multiplier = DATEPART(millisecond,now())+1
  message convert(timestamp,now()) || ' | SABB_ForeCAST_Create_ForeCAST_Loop_Table_2 - Initializaing DONE' to client
  -- update the dates first
  message convert(timestamp,now()) || ' | SABB_ForeCAST_Create_ForeCAST_Loop_Table_2 - Generating Sub-Structures' to client
  select * into #Loop_Sky_Calendar from CITeam.Subs_Calendar(@ForeCAST_Start_Wk/100,@ForeCAST_End_Wk/100)
  update ForeCAST_Loop_Table as a
    set subs_week_and_year = sc.subs_week_and_year,
    subs_week_of_year = sc.subs_week_of_year from
    ForeCAST_Loop_Table as a
    join #Loop_Sky_Calendar as sc on sc.calendar_date = a.end_date+7
  -- update the segments
  update ForeCAST_Loop_Table
    set SABB_forecast_segment = case when BB_status_code in( 'AB','PC','BCRQ' ) then BB_status_code
    else SABB_forecast_segment
    end,segment_SA = case when BB_status_code in( 'AB','PC','BCRQ' ) then BB_status_code
    else segment_SA
    end
  update ForeCAST_Loop_Table
    set rand_action_Pipeline = rand(number()*@multiplier+1),
    rand_BB_Offer_Applied = rand(number()*@multiplier+2),
    rand_Intrawk_BB_NotSysCan = rand(number()*@multiplier+3),
    rand_Intrawk_BB_SysCan = rand(number()*@multiplier+4),
    rand_BB_Pipeline_Status_Change = rand(number()*@multiplier+5),
    rand_New_Off_Dur = rand(number()*@multiplier+6),
    rand_BB_NotSysCan_Duration = rand(number()*@multiplier+7)
  -- 3.02 Add Random Number and Segment Size for random event allocations later --
  /* --============================= Missing fields from the master of retention/ bb pipeline/ cust_fcast table: PO_Pipeline_Cancellations; Same_Day_Cancels; SC_Gross_Terminations -===========*/
  select a.*
    into Pred_Rates
    from ForeCAST_Loop_Table as a
  -- this can removed once cuscan and syscan are foreCASTed - ----
  /*
LEFT JOIN (SELECT Cuscan_foreCAST_segment 
, sum(PO_Pipeline_Cancellations) + sum(Same_Day_Cancels) AS Cuscan
FROM citeam.DTV_FCAST_Weekly_Base
WHERE Subs_year = 2015 AND subs_week = (SELECT max(subs_week_of_year) FROM ForeCAST_Loop_Table) GROUP BY Cuscan_foreCAST_segment ) AS b ON a.Cuscan_foreCAST_segment = b.Cuscan_foreCAST_segment
LEFT JOIN (SELECT Syscan_foreCAST_segment 
, sum(SC_Gross_Terminations) AS Syscan 
FROM citeam.DTV_FCAST_Weekly_Base WHERE Subs_year = 2015 AND subs_week = (SELECT max(subs_week_of_year) FROM ForeCAST_Loop_Table) GROUP BY Syscan_foreCAST_segment ) AS c ON a.syscan_foreCAST_segment = c.syscan_foreCAST_segment
*/
  message convert(timestamp,now()) || ' | SABB_ForeCAST_Create_ForeCAST_Loop_Table_2 - Generating Structures DONE ' to client
  -- 3.04 Calculate Proportions for random event allocation and bring in event rates --
  -- we have calculated above the distributions for TA_Calls and WC_Calls
  --     we need to treat somehow the overlapping customers - that go in PC and AB
  -- we calculate first the cuscan and then we exclude the cuscan in order to caluclate the syscan
  -- we set syscan rank as null
  message convert(timestamp,now()) || ' | SABB_ForeCAST_Create_ForeCAST_Loop_Table_2 - Generating ForeCAST_Loop_Table_2' to client
  select a.*,
    'SABB_forecast_segment_count'=COUNT() over(partition by a.SABB_forecast_segment),
    'SABB_Group_rank'=convert(real,row_number() over(partition by a.SABB_forecast_segment order by rand_action_Pipeline asc)),
    'pct_SABB_COUNT'=SABB_Group_rank/SABB_forecast_segment_count,
    'SABB_Churn'=convert(tinyint,0),
    -- cuscan
    'pred_bb_enter_SysCan_rate'=convert(real,0),
    'pred_bb_enter_SysCan_YoY_Trend'=convert(real,0),
    'cum_bb_enter_SysCan_rate'=convert(real,0),
    'pred_bb_enter_CusCan_rate'=convert(real,0),
    'pred_bb_enter_CusCan_YoY_Trend'=convert(real,0),
    'cum_bb_enter_CusCan_rate'=convert(real,0),
    'pred_bb_enter_HM_rate'=convert(real,0),
    'pred_bb_enter_HM_YoY_Trend'=convert(real,0),
    'cum_bb_enter_HM_rate'=convert(real,0),
    'pred_bb_enter_3rd_party_rate'=convert(real,0),
    'pred_bb_enter_3rd_party_YoY_Trend'=convert(real,0),
    'cum_bb_enter_3rd_party_rate'=convert(real,0),
    'pred_BB_Offer_Applied_rate'=convert(real,0),
    'pred_BB_Offer_Applied_YoY_Trend'=convert(real,0),
    'cum_BB_Offer_Applied_rate'=convert(real,0),
    'DTV_AB'=convert(tinyint,0), ----??? we need equivalent of this for our four froms of pipeline entry?
    'BB_Offer_Applied'=convert(tinyint,0)
    into ForeCAST_Loop_Table_2
    from Pred_Rates as a
  message convert(timestamp,now()) || ' | ForeCAST_Create_ForeCAST_Loop_Table_2 - Generating ForeCAST_Loop_Table_2: ' || @@rowcount to client
  commit work
  create hg index id1 on pitteloudj.ForeCAST_Loop_Table_2(account_number)
  create lf index id2 on pitteloudj.ForeCAST_Loop_Table_2(churn_type)
  create lf index id3 on pitteloudj.ForeCAST_Loop_Table_2(SABB_forecast_segment)
  create lf index id4 on pitteloudj.ForeCAST_Loop_Table_2(BB_Status_Code)
  create lf index id5 on pitteloudj.ForeCAST_Loop_Table_2(subs_week_and_year)
  create lf index id6 on pitteloudj.ForeCAST_Loop_Table_2(weekid)
  commit work
  message convert(timestamp,now()) || ' | SABB_ForeCAST_Create_ForeCAST_Loop_Table_2 - THE END ! ' to client
end
-- Grant execute rights to the members of CITeam
grant execute on pitteloudj.SABB_ForeCAST_Create_ForeCAST_Loop_Table_2 to CITeam,vespa_group_low_security