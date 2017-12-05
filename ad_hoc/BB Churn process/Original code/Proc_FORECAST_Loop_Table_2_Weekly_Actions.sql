Create variable Counter integer; Set Counter = 1;
Create variable  multiplier  bigint;
Create variable  multiplier_2  bigint;

-- First you need to impersonate CITeam
Setuser CITeam;

-- Drop procedure if exists CITeam.Forecast_Loop_Table_2_Actions;

Create procedure CITeam.Forecast_Loop_Table_2_Actions(IN Counter integer,IN Rate_Multiplier float)
SQL Security INVOKER
BEGIN
Declare multiplier  bigint;
Declare multiplier_2  bigint;
Set multiplier = DATEPART(millisecond,now())+1;
Set multiplier_2 = DATEPART(millisecond,now())+2;

--------------------------------------------------------------------------------------------------------------
-- Predicted rates -------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------

--- cuscan rates ----
update Forecast_Loop_Table_2 as a
set  pred_TA_Call_Cust_rate      = Coalesce(b.pred_TA_Call_Cust_rate,0)
    ,pred_Web_Chat_TA_Cust_rate = Coalesce(b.pred_Web_Chat_TA_Cust_rate,0)
    ,pred_TA_DTV_Offer_Applied_rate = Coalesce(b.pred_TA_DTV_Offer_Applied_rate,0)
    ,pred_NonTA_DTV_Offer_Applied_rate = Coalesce(b.pred_NonTA_DTV_Offer_Applied_rate,0)
from Forecast_Loop_Table_2 as a
     left join
     cuscan_predicted_values as b
        on (a.subs_week_of_year       = b.subs_week or (a.subs_week_of_year = 53 and b.subs_week = 52))
        and replace(a.cuscan_forecast_segment,'_SkyQ','_Original') = b.cuscan_forecast_segment;

------ TA trend ------
update Forecast_Loop_Table_2 as a
set pred_TA_Call_Cust_YoY_Trend  = Coalesce(d.Grad_Coeff * 4 * (Cast(counter-1 as integer)/52+1),0)
from Forecast_Loop_Table_2 as a
     left join
     Fcast_Regr_Coeffs as d
        on replace(a.cuscan_forecast_segment,'_SkyQ','_Original')  = d.fcast_segment
--         and d.LV = Forecast_Start_Wk
        and d.Metric = 'TA_Call_Customers';

------ TA cum ----
update Forecast_Loop_Table_2 as a
Set cum_TA_Call_Cust_rate = pred_TA_Call_Cust_rate + pred_TA_Call_Cust_YoY_Trend;

------ WC cum-----
update Forecast_Loop_Table_2 as a
Set cum_Web_Chat_TA_Cust_rate  = cum_TA_Call_Cust_rate + pred_Web_Chat_TA_Cust_rate;

------ WC trend ------
update Forecast_Loop_Table_2 as a
set pred_Web_Chat_TA_Cust_YoY_Trend  = Coalesce(d.Grad_Coeff * 4 * (Cast(counter-1 as integer)/52+1),0)
from Forecast_Loop_Table_2 as a
     left join
     Fcast_Regr_Coeffs as d
        on replace(a.cuscan_forecast_segment,'_SkyQ','_Original')  = d.fcast_segment
--         and d.LV = Forecast_Start_Wk
        and d.Metric = 'Web_Chat_TA_Customers';
-------WC cum ------
update Forecast_Loop_Table_2 as a
Set cum_Web_Chat_TA_Cust_Trend_rate = cum_Web_Chat_TA_Cust_rate + pred_Web_Chat_TA_Cust_YoY_Trend ;

------ DTV Offer trend ------
update Forecast_Loop_Table_2 as a
set pred_TA_DTV_Offer_Applied_YoY_Trend  = Coalesce(d.Grad_Coeff * 4 * (Cast(counter-1 as integer)/52+1),0)
from Forecast_Loop_Table_2 as a
     left join
     Fcast_Regr_Coeffs as d
        on replace(a.cuscan_forecast_segment,'_SkyQ','_Original')  = d.fcast_segment
--         and d.LV = Forecast_Start_Wk
        and d.Metric = 'TA_DTV_Offer_Applied';

------ DTV Offer trend ------
update Forecast_Loop_Table_2 as a
set pred_NonTA_DTV_Offer_Applied_YoY_Trend  = Coalesce(d.Grad_Coeff * 4 * (Cast(counter-1 as integer)/52+1),0)
from Forecast_Loop_Table_2 as a
     left join
     Fcast_Regr_Coeffs as d
        on a.Cuscan_forecast_segment  = d.fcast_segment
--         and d.LV = Forecast_Start_Wk
        and d.Metric = 'NonTA_DTV_Offer_Applied';


-- syscan rates -----
update Forecast_Loop_Table_2 as a
set pred_DTV_AB_rate  = Coalesce(c.pred_DTV_AB_rate,0)
from Forecast_Loop_Table_2 as a
     left join
     syscan_predicted_values as c
     on (a.subs_week_of_year       = c.subs_week or (a.subs_week_of_year = 53 and c.subs_week = 52))
        and a.syscan_forecast_segment              = c.syscan_forecast_segment;

------ AB_DTV trend ------
update Forecast_Loop_Table_2 as a
set pred_DTV_YoY_Trend  = Coalesce(d.Grad_Coeff * 4 * (Cast(counter-1 as integer)/52+1),0)
from Forecast_Loop_Table_2 as a
     left join
     Fcast_Regr_Coeffs as d
        on a.syscan_forecast_segment  = d.fcast_segment
--         and d.LV = Forecast_Start_Wk
        and d.Metric = 'DTV_AB';

---- AB cum ------
update Forecast_Loop_Table_2 as a
set cum_DTV_AB_rate  = pred_DTV_AB_rate  ;

update Forecast_Loop_Table_2 as a
set cum_DTV_AB_trend_rate = cum_DTV_AB_rate + pred_dtv_YoY_Trend;





--------------------------------------------------------------------------------------------------------------
-- TA/WC Volumes, Saves & Offers Applied  --------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------

-- 3.06 Allocate customers randomly based on rates --
update Forecast_Loop_Table_2 as a
set
 TA_Call_Cust   = case when rand_action_Cuscan /*pct_cuscan_count*/ <= cum_TA_Call_Cust_rate * Rate_Multiplier
                       then 1
                       else 0
                  end
,WC_Call_Cust   = case when rand_action_Cuscan /*pct_cuscan_count*/ > cum_TA_Call_Cust_rate * Rate_Multiplier
                            and rand_action_Cuscan /*pct_cuscan_count*/ <= cum_Web_Chat_TA_Cust_rate * Rate_Multiplier
                       then 1
                       else 0
                  end
;

-- TA
update Forecast_Loop_Table_2 as a
set  TA_Call_Count  = b.total_calls
    ,TA_Saves      = b.TA_Saved
    ,DTV_Offer_Applied = b.TA_DTV_Offer_Applied
--     ,TA_Non_Saves   = case when pct_cuscan_count <= pred_TA_Not_Saved_rate        then 1 else 0 end
from Forecast_Loop_Table_2 as a
     inner join
     TA_Call_Dist as b
     on a.CusCan_Forecast_Segment = b.CusCan_Forecast_Segment
        and a.rand_TA_Vol between b.TA_Lower_Pctl and b.TA_Upper_Pctl
where TA_Call_Cust > 0;


-- WebChat

update Forecast_Loop_Table_2 as a
set
     WC_Call_Count  = b.total_WCs
    ,WC_Saves       = b.Webchat_TA_Saved
-- select count(*)
from Forecast_Loop_Table_2 as a
     inner join
     WC_Dist as b
     on b.Subs_week = a.subs_week_of_year
        and Prev_WC_Vol_Percentile <= rand_WC_Vol
        and rand_WC_Vol <= WC_Vol_Percentile
        and Prev_WC_Save_Vol_Percentile <= rand_WC_Save_Vol
        and rand_WC_Save_Vol <= WC_Save_Vol_Percentile
where WC_Call_Cust > 0;























--------------------------------------------------------------------------------------------------------------
-- Pending Cancels -------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
--- pred DTV_PC ----
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


--- cum DTV_PC ----
update Forecast_Loop_Table_2 as a
set cum_TA_DTV_PC_rate = pred_TA_DTV_PC_rate + pred_TA_Sky_Plus_Save_rate
   ,cum_WC_DTV_PC_rate = pred_WC_DTV_PC_rate + pred_WC_Sky_Plus_Save_rate
  ;

---- DTV_PC counts
Drop table if exists #TA_PC_Pctl;
Select cuscan_forecast_segment,account_number,TA_Call_Cust,WC_Call_Cust,TA_Saves,WC_Saves,DTV_Offer_Applied,
Row_number() over(partition by cuscan_forecast_segment order by TA_Saves,DTV_Offer_Applied, WC_Saves) Segment_Rnk,
Count(*) over(partition by cuscan_forecast_segment) Total_Accs,
Cast(Segment_Rnk as float)/Total_Accs as CusCan_Segment_Pctl
into #TA_PC_Pctl
from Forecast_Loop_Table_2
where TA_Call_Cust > 0 and DTV_Status_Code not in ('AB','PC')
;

update Forecast_Loop_Table_2 as a
set
TA_DTV_PC = case when CusCan_Segment_Pctl <= pred_TA_DTV_PC_rate and a.TA_Call_Cust > 0
                then 1
                else 0
            end
,TA_Sky_Plus_Save = case when CusCan_Segment_Pctl > pred_TA_DTV_PC_rate
                              and CusCan_Segment_Pctl < cum_TA_DTV_PC_rate
                              and a.TA_Call_Cust >0
                then 1
                else 0
            end
from Forecast_Loop_Table_2 as a
     inner join
     #TA_PC_Pctl pc
     on pc.account_number = a.account_number;


Drop table if exists #TA_PC_Pctl;
Select cuscan_forecast_segment,account_number,TA_Call_Cust,WC_Call_Cust,TA_Saves,WC_Saves,DTV_Offer_Applied,
Row_number() over(partition by cuscan_forecast_segment order by TA_Saves,WC_Saves,DTV_Offer_Applied) Segment_Rnk,
Count(*) over(partition by cuscan_forecast_segment) Total_Accs,
Cast(Segment_Rnk as float)/Total_Accs as CusCan_Segment_Pctl
into #WC_PC_Pctl
from Forecast_Loop_Table_2
where WC_Call_Cust > 0 and DTV_Status_Code not in ('AB','PC')
;

update Forecast_Loop_Table_2 as a
set WC_DTV_PC = case when rand_WC_DTV_PC_Vol <= pred_WC_DTV_PC_rate and a.WC_Call_Cust > 0
                then 1
                else 0
            end
    ,WC_Sky_Plus_Save = case when rand_WC_DTV_PC_Vol > pred_WC_DTV_PC_rate
                              and rand_WC_DTV_PC_Vol < cum_WC_DTV_PC_rate
                              and a.WC_Call_Cust >0
                then 1
                else 0
            end
from Forecast_Loop_Table_2 as a
     inner join
     #WC_PC_Pctl pc
     on pc.account_number = a.account_number;


update Forecast_Loop_Table_2 as a
set Other_DTV_PC = 1
where a.TA_Call_Cust = 0 and a.WC_Call_Cust = 0 and a.DTV_Offer_Applied = 0
        and rand_Other_DTV_PC_Vol <= pred_Other_DTV_PC_rate
;




Update Forecast_Loop_Table_2
Set rand_action_Syscan = Case when TA_Call_Cust + WC_Call_Cust > 0 then 1
                              else null
                         end;

Update Forecast_Loop_Table_2
Set rand_action_Syscan = rand(number(*)*multiplier+4)
where rand_action_Syscan is null;

-- the low ranking customers will be the ones with no TA / WC
Drop table if exists #SysCan_Rank;
Select
account_number
,rand_action_Syscan
,sum(TA_Call_Cust+WC_Call_Cust) over(partition by Syscan_Forecast_segment) SysCan_Seg_CusCan_Actions
,count(*) over(partition by Syscan_Forecast_segment) Total_Cust_In_SysCan_Segment
,cast(rank() over(partition by Syscan_Forecast_segment  order by rand_action_Syscan) as float)                               as SysCan_Group_rank
,cast(rank() over(partition by Syscan_Forecast_segment order by rand_action_Syscan) as float)/cast(Syscan_segment_count as float) as pct_syscan_count
,case when TA_Call_Cust+WC_Call_Cust = 0
           and rand_action_Syscan <= pred_dtv_AB_rate*Total_Cust_In_SysCan_Segment/(Total_Cust_In_SysCan_Segment-SysCan_Seg_CusCan_Actions)
      then 1
      else 0
 end as DTV_AB
into #SysCan_Rank
from Forecast_Loop_Table_2;

commit;
create hg index idx_1 on #SysCan_Rank(account_number);

-- Update Forecast_Loop_Table_2
-- Set SysCan_Group_rank = b.SysCan_Group_rank,
--     pct_syscan_count = b.pct_syscan_count
-- from Forecast_Loop_Table_2 a
--      inner join
--      #SysCan_Rank b
--      on a.account_number = b.account_number;


update Forecast_Loop_Table_2 as a
set DTV_AB         = 1
from Forecast_Loop_Table_2 as a
     inner join
     #SysCan_Rank b
     on b.account_number = a.account_number
        and b.DTV_AB = 1
;












Update Forecast_Loop_Table_2 as a
Set DTV_Offer_Applied = 1
where --TA_Call_Cust = 1 and rand_TA_DTV_Offer_Applied <= pred_TA_DTV_Offer_Applied_rate + pred_TA_DTV_Offer_Applied_YoY_Trend
      --or
      TA_Call_Cust = 0 and rand_NonTA_DTV_Offer_Applied <= pred_NonTA_DTV_Offer_Applied_rate + pred_NonTA_DTV_Offer_Applied_YoY_Trend
      ;




Update Forecast_Loop_Table_2
Set DTV_Status_Code_EoW = AB.Next_Status_Code,
    DTV_Offer_Applied = AB.AB_ReAC_Offer_Applied
from Forecast_Loop_Table_2 base
     inner join
     IntraWk_AB_Pct AB
     on base.rand_Intrawk_DTV_AB between AB.IntaWk_AB_Lower_Pctl and AB.IntaWk_AB_Upper_Pctl
where DTV_AB > 0;

Update Forecast_Loop_Table_2 base
Set DTV_Status_Code_EoW = PC.Next_Status_Code,
    DTV_Offer_Applied = PC.PC_ReAC_Offer_Applied
from Forecast_Loop_Table_2 base
     inner join
     IntraWk_PC_Pct PC
     on base.rand_Intrawk_DTV_PC between PC.IntaWk_PC_Lower_Pctl and PC.IntaWk_PC_Upper_Pctl
where TA_DTV_PC > 0
      or
      WC_DTV_PC > 0
      or
      TA_Sky_Plus_Save > 0
      or
      WC_Sky_Plus_Save > 0
      or
      Other_DTV_PC > 0;



Update Forecast_Loop_Table_2 base
Set DTV_Status_Code_EoW = PC.Status_Code_EoW,
    DTV_Offer_Applied = PC.PC_ReAC_Offer_Applied
from Forecast_Loop_Table_2 base
     inner join
     PC_PL_Status_Change_Dist PC
     on base.rand_DTV_PC_Status_Change between PC.PC_Percentile_Lower_Bound and PC.PC_Percentile_Upper_Bound
        and Case when (cast(base.DTV_PC_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=0 then 'Churn in next 1 wks'
                 when (cast(base.DTV_PC_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=1 then 'Churn in next 2 wks'
                 when (cast(base.DTV_PC_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=2 then 'Churn in next 3 wks'
                 when (cast(base.DTV_PC_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=3 then 'Churn in next 4 wks'
                 when (cast(base.DTV_PC_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=4 then 'Churn in next 5 wks'
                 when (cast(base.DTV_PC_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7>=5 then 'Churn in next 6+ wks'
--           when (cast(PC_Future_Sub_Effective_Dt as integer) - cast(End_Date as integer))/7>5 then '6+_Wks_To_Churn'
            end = PC.Wks_To_Intended_Churn
where DTV_Status_Code = 'PC'
        and
        (TA_DTV_PC = 0
      and
      WC_DTV_PC = 0
      and
      TA_Sky_Plus_Save = 0
      and
      WC_Sky_Plus_Save = 0
      and
      Other_DTV_PC = 0)
;


Update Forecast_Loop_Table_2 base
Set DTV_Status_Code_EoW = AB.Status_Code_EoW,
    DTV_Offer_Applied = AB.AB_ReAC_Offer_Applied
from Forecast_Loop_Table_2 base
     inner join
     AB_PL_Status_Change_Dist AB
     on base.rand_DTV_PC_Status_Change between AB.AB_Percentile_Lower_Bound and AB.AB_Percentile_Upper_Bound
        and Case when (cast(base.DTV_AB_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=0 then 'Churn in next 1 wks'
                 when (cast(base.DTV_AB_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=1 then 'Churn in next 2 wks'
                 when (cast(base.DTV_AB_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=2 then 'Churn in next 3 wks'
                 when (cast(base.DTV_AB_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=3 then 'Churn in next 4 wks'
                 when (cast(base.DTV_AB_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=4 then 'Churn in next 4 wks'
                 when (cast(base.DTV_AB_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=5 then 'Churn in next 6 wks'
                 when (cast(base.DTV_AB_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=6 then 'Churn in next 7 wks'
                 when (cast(base.DTV_AB_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=7 then 'Churn in next 8 wks'
                 when (cast(base.DTV_AB_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=8 then 'Churn in next 9 wks'
                 when (cast(base.DTV_AB_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7>=9 then 'Churn in next 10+ wks'
--           when (cast(PC_Future_Sub_Effective_Dt as integer) - cast(End_Date as integer))/7>5 then '6+_Wks_To_Churn'
            end = AB.Wks_To_Intended_Churn
where DTV_Status_Code = 'AB' and DTV_AB = 0;

Update Forecast_Loop_Table_2 base
Set CusCan = 1
where DTV_Status_Code_EoW = 'PO';

Update Forecast_Loop_Table_2 base
Set SysCan = 1
where DTV_Status_Code_EoW = 'SC';






END;


-- Grant execute rights to the members of CITeam
grant execute on CITeam.Forecast_Loop_Table_2_Actions to CITeam;

-- Change back to your account
Setuser;

-- Test it
Call CITeam.Forecast_Loop_Table_2_Actions(10);


