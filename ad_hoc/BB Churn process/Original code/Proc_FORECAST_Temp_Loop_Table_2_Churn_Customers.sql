-- First you need to impersonate CITeam
/*
Setuser CITeam;
*/
 Drop procedure if exists Forecast_Temp_Loop_Table_2_Churn_Custs;

Create procedure Forecast_Temp_Loop_Table_2_Churn_Custs()
SQL Security INVOKER
BEGIN

Declare multiplier  bigint;
-- Declare multiplier_2  bigint;
Set multiplier = DATEPART(millisecond,now())+1;
-- Set multiplier_2 = DATEPART(millisecond,now())+2;


---------------------------------------------------------
/* ---- Temporary code to remove churned customers ----*/
---------------------------------------------------------
Drop table if exists #Churn_Custs;
Select account_number,DTV_Activation_Type,Time_Since_Last_TA_Call,CusCan_Forecast_Segment,SysCan_Forecast_Segment,CusCan_Churn,SysCan_Churn,
rand(number(*)*multiplier+7) as rand_Cuscan_churn,
rand(number(*)*multiplier+8) as rand_Syscan_churn
into #Churn_Custs
from Forecast_Loop_Table_2
where DTV_activation_type is null;

Drop table if exists #Churn_Custs_2;
Select *,
Case Time_Since_Last_TA_Call
when 'No Prev TA Calls' then 99999
when '0 Wks since last TA Call' then 2
when '48-52 Wks since last TA Call' then 6
when '53-60 Wks since last TA Call' then 7
when '61+ Wks since last TA Call' then 8
when '06-35 Wks since last TA Call' then 4
when '36-46 Wks since last TA Call' then 5
when '02-05 Wks since last TA Call' then 1
when '01 Wks since last TA Call' then 3
when '47 Wks since last TA Call' then 9
end Time_Since_Last_TA_Call_Rnk
,row_number() over(partition by CusCan_Forecast_Segment order by Time_Since_Last_TA_Call_Rnk, rand_Cuscan_churn) as CusCan_Churn_Rnk
,row_number() over(partition by SysCan_Forecast_Segment order by rand_Cuscan_churn desc,rand_Syscan_churn) as SysCan_Churn_Rnk
into #Churn_Custs_2
from #Churn_Custs;

commit;

-- Select *
-- from #Churn_Custs_2
-- where CusCan_Churn_Rnk <= 1000
-- order by CusCan_Forecast_Segment,DTV_Activation_Type desc,Time_Since_Last_TA_Call_Rnk, rand_Cuscan_churn;

-- Update Forecast_Loop_Table_2
-- Set CusCan = 1
-- from Forecast_Loop_Table_2 a
--      inner join
--      #Churn_Custs_2 b
--      on a.account_number = b.account_number
-- where b.CusCan_Churn_Rnk < b.CusCan_Churn;


Update Forecast_Loop_Table_2
Set SysCan = 1
from Forecast_Loop_Table_2 a
     inner join
     #Churn_Custs_2 b
     on a.account_number = b.account_number
where b.SysCan_Churn_Rnk < b.SysCan_Churn;


END;


-- Grant execute rights to the members of CITeam
grant execute on Forecast_Temp_Loop_Table_2_Churn_Custs to CITeam;
/*
-- Change back to your account
Setuser;

-- Test it
Call CITeam.Forecast_Temp_Loop_Table_2_Churn_Custs;
*/

