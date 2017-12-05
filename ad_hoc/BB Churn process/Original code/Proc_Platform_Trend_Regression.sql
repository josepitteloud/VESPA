-- Drop procedure if exists CITeam.Regression_Coefficient;
/*
Setuser CITeam;

Create variable LV integer; Set LV = 201614;
Create variable Regression_Yrs smallint; Set Regression_Yrs = 2;

Create variable  Y3W52 integer;
Create variable  Y1W01 integer;
*/
Drop procedure if exists Regression_Coefficient;

CREATE PROCEDURE Regression_Coefficient(IN LV integer,IN Regression_Yrs tinyint)
RESULT(LV integer,Metric varchar(30),Fcast_Segment varchar(100),Grad_Coeff float,Intercept_Coeff float)
BEGIN

Declare Dynamic_SQL varchar(1000);
Declare Y3W52 integer;
Declare Y1W01 integer;

-- Create variable Dynamic_SQL varchar(1000);

-- Create variable LV integer; Set LV = 201601;

-----------------------------------------------------------------------------------------------------------
-- Create aggregates table from which to calc trends ------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
Select * into #Sky_Calendar from CITeam.Subs_Calendar(LV/100-Regression_Yrs-1,LV/100);

Drop table if exists #Regr_Wks;
Select Cast(sc.subs_week_and_year as integer) Subs_week_and_year
       ,Row_Number() over(order by Subs_week_and_year desc) Wk_Rnk
into #Regr_Wks
from #sky_calendar sc
where Cast(sc.subs_Week_and_year as integer) < LV
    and Subs_Week_of_year != 53
group by Subs_week_and_year;

Delete from #Regr_Wks where Wk_Rnk > Regression_Yrs *52 + 13;

Set Y1W01 = (Select min(Subs_week_and_year) from #Regr_Wks);

-- Case when ((Cast(LV as float)/100) % 1)*100 = 53
--                       then (LV/100-2)*100 + 1
--                  else LV - 300
--             end;
Set Y3W52 = (Select max(Subs_week_and_year) from #Regr_Wks);
-- Case when ((Cast(LV as float)/10) % 1)*10 = 1
--                       then (LV/100 - 1)*100 +  52
--                  else LV - 1
--             end;


drop table if exists #cuscan_weekly_agg;
select
  subs_year
,subs_week
,subs_week_and_year
,cuscan_forecast_segment

--,offer_length_dtv
-- ,dtv_offer_applied
--,new_customer
--,activations_acquisitions
--,activations_reinstates

,Count(*) as n
,Cast(sum(TA_Event_Count) as float) as TA_Call_cnt
,Cast(sum(Unique_TA_Caller) as float) as TA_Call_Customers
,Cast(sum(TA_Non_Save_Count) as float) as TA_Not_Saved
,Cast(sum(TA_Save_Count) as float) as TA_Saved
,Cast(sum(Web_Chat_TA_Cnt) as float) as Web_Chat_TA_Cnt
,Cast(sum(Web_Chat_TA_Customers) as float) as Web_Chat_TA_Customers
,Cast(sum(Web_Chat_TA_Not_Saved) as float) as Web_Chat_TA_Not_Saved
,Cast(sum(Web_Chat_TA_Saved) as float) as Web_Chat_TA_Saved
,Cast(sum(Offer_Applied_DTV) as float) as DTV_Offer_Applied
,Cast(sum(TA_DTV_Offer_Applied) as float) as TA_DTV_Offer_Applied
,Cast(DTV_Offer_Applied - TA_DTV_Offer_Applied        as float) as NonTA_DTV_Offer_Applied

,Cast(sum(TA_DTV_PC)                as float) as TA_DTV_PC
,Cast(sum(WC_DTV_PC)                as float) as WC_DTV_PC
,Cast(sum(Accessibility_DTV_PC) + sum(Min_Term_PC) + sum(Other_PC) as float) as Other_PC

,row_number() over(partition by cuscan_forecast_segment order by subs_week_and_year desc) as week_id
,case
  when week_id between  1 and  52 then 'Curr'
  when week_id between 53 and 104 then 'Prev'
  else null end as week_position
,case when week_id between 1 and 13 then 'Y' else 'N' end as last_quarter
,((week_id-1)/13)+1 as quarter_id
into #cuscan_weekly_agg
from CITeam.DTV_Fcast_Weekly_Base agg
where subs_week_and_year between Y1W01 and Y3W52
        and subs_week != 53
        and Downgrade_View = 'Actuals'
group by subs_year
,subs_week
,subs_week_and_year
,cuscan_forecast_segment
;


-----------------------------------------------------------------------------------------------------------
----------  Turnaround and Webchat Events -----------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
Drop table if exists #Regr_inputs;
select   quarter_id
        ,agg.cuscan_forecast_segment
        ,row_number() over(partition by agg.cuscan_forecast_segment order by quarter_id desc)     as x
        ,sum(cast(TA_Call_Customers     as float))/sum(n)        as TA_cust
        ,sum(cast(Web_Chat_TA_Customers as float))/sum(n)        as WC_cust
        ,Case when sum(TA_Call_Customers) = 0 then 0 else sum(cast(TA_DTV_Offer_Applied as float))/sum(TA_Call_Customers) end       as TA_DTV_Offer_Applied_cust
        ,sum(cast(NonTA_DTV_Offer_Applied as float))/(sum(n)-sum(TA_Call_Customers))       as NonTA_DTV_Offer_Applied_cust

        ,case when sum(TA_Call_Customers) = 0 then 0 else sum(cast(TA_DTV_PC as float))/sum(TA_Call_Customers)  end   as TA_DTV_PC_Cust
        ,case when sum(Web_Chat_TA_Customers) = 0 then 0 else sum(cast(WC_DTV_PC as float))/sum(Web_Chat_TA_Customers) end as WC_DTV_PC_Cust
        ,sum(cast(Other_PC as float))/(sum(n)-sum(TA_Call_Customers)-sum(Web_Chat_TA_Customers)) as Other_PC_Cust

        ,x*x                                                    as xx
        ,x*TA_cust                                              as x_TA_cust
        ,x*WC_cust                                              as x_WC_cust
        ,x*TA_DTV_Offer_Applied_cust                            as x_TA_DTV_Offer_Applied_cust
        ,x*NonTA_DTV_Offer_Applied_cust                         as x_NonTA_DTV_Offer_Applied_cust
        ,x*TA_DTV_PC_Cust                                       as x_TA_DTV_PC_Cust
        ,x*WC_DTV_PC_Cust                                       as x_WC_DTV_PC_Cust
        ,x*Other_PC_Cust                                        as x_Other_PC_Cust


        ,Sum(n)                                                as cell_n
        ,cast(null as float)                                    as TA_regression
        ,cast(null as float)                                    as WC_regression
        ,cast(null as float)                                    as TA_DTV_Offer_Applied_regression
        ,cast(null as float)                                    as NonTA_DTV_Offer_Applied_regression
        ,cast(null as float)                                    as TA_DTV_PC_regression
        ,cast(null as float)                                    as WC_DTV_PC_regression
        ,cast(null as float)                                    as Other_PC_regression

into #Regr_inputs
from #Cuscan_weekly_agg as agg
group by  quarter_id
         ,agg.cuscan_forecast_segment
;


-- Select * from #Regr_inputs;

--weighted univariate regression (weeks is the independent variable, each weekly data point has cell_n observations)
Drop table if exists #Regr_coeff;
select cuscan_forecast_segment
        ,sum(cell_n)             as n
        ,sum(Cast(cell_n as bigint)*x)          as sum_x
        ,sum(Cast(cell_n as bigint)*xx)         as sum_xx

        ,sum(cell_n*TA_cust)       as sum_TA_cust
        ,sum(cell_n*WC_cust)       as sum_WC_cust
        ,sum(cell_n*TA_DTV_Offer_Applied_cust)       as sum_TA_DTV_Offer_Applied_cust
        ,sum(cell_n*NonTA_DTV_Offer_Applied_cust)    as sum_NonTA_DTV_Offer_Applied_cust
        ,sum(cell_n*TA_DTV_PC_Cust)   as sum_TA_DTV_PC_Cust
        ,sum(cell_n*WC_DTV_PC_Cust)   as sum_WC_DTV_PC_Cust
        ,sum(cell_n*Other_PC_Cust)    as sum_Other_PC_Cust

        ,sum(cell_n*x_TA_cust)     as sum_x_TA_cust
        ,sum(cell_n*x_WC_cust)     as sum_x_WC_cust
        ,sum(cell_n*x_TA_DTV_Offer_Applied_cust)     as sum_x_TA_DTV_Offer_Applied_cust
        ,sum(cell_n*x_NonTA_DTV_Offer_Applied_cust)  as sum_x_NonTA_DTV_Offer_Applied_cust
        ,sum(cell_n*x_TA_DTV_PC_Cust)  as sum_x_TA_DTV_PC_Cust
        ,sum(cell_n*x_WC_DTV_PC_Cust)  as sum_x_WC_DTV_PC_Cust
        ,sum(cell_n*x_Other_PC_Cust)   as sum_x_Other_PC_Cust

        ,cast(null as float)     as b0_TA_cust
        ,cast(null as float)     as b0_WC_cust
        ,cast(null as float)     as b0_TA_DTV_Offer_Applied_cust
        ,cast(null as float)     as b0_NonTA_DTV_Offer_Applied_cust
        ,cast(null as float)     as b0_TA_DTV_PC_Cust
        ,cast(null as float)     as b0_WC_DTV_PC_Cust
        ,cast(null as float)     as b0_Other_PC_Cust

        ,cast(null as float)     as b1_TA_cust
        ,cast(null as float)     as b1_WC_cust
        ,cast(null as float)     as b1_TA_DTV_Offer_Applied_cust
        ,cast(null as float)     as b1_NonTA_DTV_Offer_Applied_cust
        ,cast(null as float)     as b1_TA_DTV_PC_Cust
        ,cast(null as float)     as b1_WC_DTV_PC_Cust
        ,cast(null as float)     as b1_Other_PC_Cust


into #Regr_coeff
from #Regr_inputs
group by cuscan_forecast_segment
having n > 1000
;



update #Regr_coeff set b1_TA_cust        = (sum_x_TA_cust       - (sum_TA_cust       *sum_x)/n)/(sum_xx -(sum_x*sum_x)/n);
update #Regr_coeff set b1_WC_cust        = (sum_x_WC_cust       - (sum_WC_cust       *sum_x)/n)/(sum_xx -(sum_x*sum_x)/n);
update #Regr_coeff set b1_TA_DTV_Offer_Applied_cust        = (sum_x_TA_DTV_Offer_Applied_cust       - (sum_TA_DTV_Offer_Applied_cust       *sum_x)/n)/(sum_xx -(sum_x*sum_x)/n);
update #Regr_coeff set b1_NonTA_DTV_Offer_Applied_cust     = (sum_x_NonTA_DTV_Offer_Applied_cust    - (sum_NonTA_DTV_Offer_Applied_cust    *sum_x)/n)/(sum_xx -(sum_x*sum_x)/n);

update #Regr_coeff set b1_TA_DTV_PC_Cust    = case when sum_TA_cust=0 then 0 else (sum_x_TA_DTV_PC_Cust   - (sum_TA_DTV_PC_Cust   *sum_x)/sum_TA_cust)/(sum_xx -(sum_x*sum_x)/sum_TA_cust) end;
update #Regr_coeff set b1_WC_DTV_PC_Cust    = case when sum_WC_cust=0 then 0 else (sum_x_WC_DTV_PC_Cust   - (sum_WC_DTV_PC_Cust   *sum_x)/sum_WC_cust)/(sum_xx -(sum_x*sum_x)/sum_WC_cust) end;
update #Regr_coeff set b1_Other_PC_Cust     = (sum_x_Other_PC_Cust    - (sum_Other_PC_Cust    *sum_x)/(n-sum_TA_cust-sum_WC_cust))/(sum_xx -(sum_x*sum_x)/(n-sum_TA_cust-sum_WC_cust));

update #Regr_coeff set b0_TA_cust        = sum_TA_cust       /n      - b1_TA_cust        *sum_x/n;
update #Regr_coeff set b0_WC_cust        = sum_WC_cust       /n      - b1_WC_cust        *sum_x/n;
update #Regr_coeff set b0_TA_DTV_Offer_Applied_cust        = sum_TA_DTV_Offer_Applied_cust       /n      - b1_TA_DTV_Offer_Applied_cust        *sum_x/n;
update #Regr_coeff set b0_NonTA_DTV_Offer_Applied_cust     = sum_NonTA_DTV_Offer_Applied_cust    /n      - b1_NonTA_DTV_Offer_Applied_cust     *sum_x/n;

update #Regr_coeff set b0_TA_DTV_PC_Cust   = case when sum_TA_cust=0 then 0 else sum_TA_DTV_PC_Cust   /sum_TA_cust      - b1_TA_DTV_PC_Cust    *sum_x/sum_TA_cust end;
update #Regr_coeff set b0_WC_DTV_PC_Cust   = case when sum_WC_cust=0 then 0 else sum_WC_DTV_PC_Cust   /sum_WC_cust      - b1_WC_DTV_PC_Cust    *sum_x/sum_WC_cust end;
update #Regr_coeff set b0_Other_PC_Cust    = sum_Other_PC_Cust    /(n-sum_TA_cust-sum_WC_cust)      - b1_Other_PC_Cust     *sum_x/(n-sum_TA_cust-sum_WC_cust);


-- Select * from Regr_coeff




-----------------------------------------------------------------------------------------------------------
----------  Active Blocks ---------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
drop table if exists #Syscan_weekly_agg;
select
  subs_year
,subs_week
,subs_week_and_year
,Syscan_forecast_segment

,Count(*) as n
,Cast(sum(DTV_AB) as float) as DTV_AB


,row_number() over(partition by Syscan_forecast_segment order by subs_week_and_year desc) as week_id
,case
  when week_id between  1 and  52 then 'Curr'
  when week_id between 53 and 104 then 'Prev'
  else null end as week_position
,case when week_id between 1 and 13 then 'Y' else 'N' end as last_quarter
,((week_id-1)/13)+1 as quarter_id
into #Syscan_weekly_agg
from CITeam.DTV_Fcast_Weekly_Base agg
where subs_week_and_year between Y1W01 and Y3W52
        and subs_week != 53
        and Downgrade_View = 'Actuals'
group by subs_year
,subs_week
,subs_week_and_year
,Syscan_forecast_segment
;


--regression inputs
Drop table if exists #Regr_inputs_2;
select quarter_id
        ,agg.Syscan_forecast_segment
        ,row_number() over(partition by agg.Syscan_forecast_segment  order by quarter_id)     as x
        ,sum(cast(DTV_AB     as float))/sum(n)                  as DTV_AB

        ,x*x                                                    as xx
        ,x*DTV_AB                                               as x_DTV_AB

        ,sum(n)                                                 as cell_n
        ,cast(null as float)                                    as DTV_AB_regression

into #Regr_inputs_2
from #syscan_weekly_agg as agg
group by quarter_id
        ,agg.Syscan_forecast_segment
;

--weighted univariate regression (weeks is the independent variable, each weekly data point has cell_n observations)
Drop table if exists #Regr_coeff_2;
select Syscan_forecast_segment
        ,sum(cell_n)             as n
        ,sum(Cast(cell_n as bigint)*x)          as sum_x
        ,sum(Cast(cell_n as bigint)*xx)         as sum_xx

        ,sum(cell_n*DTV_AB)       as sum_DTV_AB
        ,sum(cell_n*x_DTV_AB)     as sum_x_DTV_AB

        ,cast(null as float)     as b0_DTV_AB
        ,cast(null as float)     as b1_DTV_AB

into #Regr_coeff_2
from #Regr_inputs_2
group by Syscan_forecast_segment
having n > 1000
;


update #Regr_coeff_2 set b1_DTV_AB        = (sum_x_DTV_AB        - (sum_DTV_AB        *sum_x)/n)/(sum_xx -(sum_x*sum_x)/n);
update #Regr_coeff_2 set b0_DTV_AB        = sum_DTV_AB       /n      - b1_DTV_AB         *sum_x/n;




---------------------------------------------------------------------------------------------------
-- Set proc outputs -------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
Select LV,'TA_Call_Customers' as Metric,cuscan_forecast_segment as forecast_segment,b1_TA_cust,b0_TA_cust
from #Regr_coeff
union all
Select LV,'Web_Chat_TA_Customers'  as Metric,cuscan_forecast_segment as forecast_segment,b1_WC_cust,b0_WC_cust
from #Regr_coeff
union all
Select LV,'TA_DTV_Offer_Applied' as Metric,cuscan_forecast_segment as forecast_segment,b1_TA_DTV_Offer_Applied_cust,b0_TA_DTV_Offer_Applied_cust
from #Regr_coeff
union all
Select LV,'NonTA_DTV_Offer_Applied' as Metric,cuscan_forecast_segment as forecast_segment,b1_NonTA_DTV_Offer_Applied_cust,b0_NonTA_DTV_Offer_Applied_cust
from #Regr_coeff
union all
Select LV,'DTV_AB' as Metric,Syscan_forecast_segment as forecast_segment,b1_DTV_AB,b0_DTV_AB
from #Regr_coeff_2
union all
Select LV,'TA_DTV_PC' as Metric,cuscan_forecast_segment as forecast_segment,b1_TA_DTV_PC_Cust,b0_TA_DTV_PC_Cust
from #Regr_coeff
union all
Select LV,'WC_DTV_PC' as Metric,cuscan_forecast_segment as forecast_segment,b1_WC_DTV_PC_Cust,b0_WC_DTV_PC_Cust
from #Regr_coeff
union all
Select LV,'Other_PC' as Metric,cuscan_forecast_segment as forecast_segment,b1_Other_PC_Cust,b0_Other_PC_Cust
from #Regr_coeff
;


END;

Grant Execute on Regression_Coefficient to CITeam;

RETURN;
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------

/*
Call Regression_Coefficient(201601);

Select *
-- into #Regression_Coefficient_Test
from citeam.Regression_Coefficient(201601,2);

Select * from #Regression_Coefficient_Test
*/









