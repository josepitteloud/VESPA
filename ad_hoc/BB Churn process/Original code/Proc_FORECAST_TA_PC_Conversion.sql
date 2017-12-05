/*Create variable Y3W52 integer;

Set Y3W52 = 201624;

-- First you need to impersonate CITeam
Setuser CITeam;
*/
Drop procedure if exists Forecast_PC_Conversion_Rates;

Create procedure Forecast_PC_Conversion_Rates(In Y3W52 integer)
Result(CusCan_Forecast_Segment varchar(50)
,Total_TA_DTV_PC integer,Total_TA_SkyPlus_Save integer
,Total_WC_DTV_PC integer,Total_WC_SkyPlus_Save integer
,Total_Other_PC integer
,Total_TA_Cust integer,Total_WC_Cust integer,Total_Other_Cust integer
,TA_DTV_PC_Conv_rate float,TA_SkyPlus_Save_rate float
,WC_DTV_PC_Conv_rate float,WC_SkyPlus_Save_rate float
,Other_DTV_PC_Conv_rate float
)
BEGIN

SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;

Select *
into #Sky_Calendar
from Subs_Calendar(Y3W52/100 -1 ,Y3W52/100);

Drop table if exists #TA_DTV_PC_Vol;
select Cuscan_forecast_segment
,sum(TA_DTV_PC)                                    as Total_TA_DTV_PC
,sum(TA_Sky_Plus_Save)                             as Total_TA_SkyPlus_Save
,sum(WC_DTV_PC)                                    as Total_WC_DTV_PC
,sum(WC_Sky_Plus_Save)                             as Total_WC_SkyPlus_Save
,sum(Other_PC )                                    as Total_Other_PC

,sum(Unique_TA_Caller)                            as Total_TA_Cust
,sum(Web_Chat_TA_Customers)                        as Total_WC_Cust
,Count(*) - Total_TA_Cust - Total_WC_Cust            as Total_Other_Cust

,case when Total_TA_Cust!=0    then cast(Total_TA_DTV_PC as float) / Total_TA_Cust    else 0 end  as TA_DTV_PC_Conv_Rate
,case when Total_TA_Cust!=0    then cast(Total_TA_SkyPlus_Save as float) / Total_TA_Cust    else 0 end  as TA_SkyPlus_Save_Rate

,case when Total_WC_Cust!=0    then cast(Total_WC_DTV_PC as float) / Total_WC_Cust    else 0 end  as WC_DTV_PC_Conv_Rate
,case when Total_WC_Cust!=0    then cast(Total_WC_SkyPlus_Save as float) / Total_WC_Cust    else 0 end  as WC_SkyPlus_Save_Rate

,case when Total_Other_Cust!=0 then cast(Total_Other_PC  as float) / Total_Other_Cust else 0 end  as Other_DTV_PC_Conv_Rate

into #TA_DTV_PC_Vol
from DTV_Fcast_Weekly_Base -- Replace with DTV_FCast_Weekly_Base
where  end_date between (select max(calendar_date - 7 - 6*7) from #sky_calendar where subs_week_and_year = Y3W52) -- Last 6 Wk PC conversions
        and (select max(calendar_date - 7) from #sky_calendar where subs_week_and_year = Y3W52)
        and Downgrade_View = 'Actuals'
group by
cuscan_forecast_segment
;

Select * from #TA_DTV_PC_Vol;

END;




-- Grant execute rights to the members of CITeam
grant execute on Forecast_PC_Conversion_Rates to CITeam;
/*
-- Change back to your account
Setuser;

-- Test it
Select top 10000 * from CITeam.Forecast_PC_Conversion_Rates(201552);

Select * into TA_DTV_PC_Vol from CITeam.Forecast_PC_Conversion_Rates(Y3W52);
Select sum(Total_TA_DTV_PC) ,sum(Total_TA_Cust),Cast(sum(Total_TA_DTV_PC) as float)/sum(Total_TA_Cust)  from CITeam.Forecast_PC_Conversion_Rates(201624);
*/