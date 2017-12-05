-- Drop procedure if exists CITeam.Regression_Coefficient;
/*
Setuser CITeam;

Create variable LV integer; Set LV = 201614;
Create variable Regression_Yrs smallint; Set Regression_Yrs = 2;

Create variable  Y3W52 integer;
Create variable  Y1W01 integer;
*/
Drop procedure if exists SABB_Regression_Coefficient;

CREATE OR REPLACE  PROCEDURE SABB_Regression_Coefficient(IN LV integer,IN Regression_Yrs tinyint)
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




drop table if exists #SABB_weekly_agg;
select
  subs_year
,subs_week_of_year as subs_week
,cast(subs_week_and_year as integer) as subs_week_and_year
,node_sa as SABB_forecast_segment
,Count(*) as n

,Cast(sum(case when  bb_offer_rem_and_end = -9999 then 0 else 1 end) as float) as BB_Offer_Applied   ---??? need to change this to the right field when we have it
,Cast(sum(bb_enter_SysCan)                as float) as bb_enter_SysCan
,Cast(sum(bb_enter_CusCan)                as float) as bb_enter_CusCan
,Cast(sum(bb_enter_HM)                		as float) as bb_enter_HM
,Cast(sum(bb_enter_3rd_party)                as float) as bb_enter_3rd_party

,dense_rank() over(order by subs_week_and_year desc) as week_id
,case
  when week_id between  1 and  52 then 'Curr'
  when week_id between 53 and 104 then 'Prev'
  else null
 end as week_position
,case when week_id between 1 and 13 then 'Y' else 'N' end as last_quarter
,(week_id/13)+1 as quarter_id
,Max(Subs_Week) over(partition by Subs_Year) Max_Subs_Week
into #SABB_weekly_agg
--from CITeam.DTV_Fcast_Weekly_Base agg
from pitteloudj.DTV_Fcast_Weekly_Base_2 agg  ---??? update this source later
where subs_week_and_year between Y1W01 and Y3W52
        and subs_week != 53
 --   and Downgrade_View = 'Actuals'    --??? add this back in when we have donwgrade_view re-added to the data
group by subs_year
,subs_week
,subs_week_and_year
,node_sa  -- segment
;

--Update #SABB_weekly_agg Set subs_week = subs_week - 1 where Max_Subs_Week = 53;
--Update #SABB_weekly_agg Set Subs_Week_And_Year = Subs_Year*100 + subs_week;
--Delete from #SABB_weekly_agg where subs_week = 0;

--select top 10 * from #SABB_weekly_agg;

-----------------------------------------------------------------------------------------------------------
----------  Pipeline entry events -----------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
Drop table if exists #Regr_inputs;
select   quarter_id
        ,agg.SABB_forecast_segment
        ,row_number() over(partition by agg.SABB_forecast_segment order by quarter_id desc)     as x
        ,sum(cast(BB_enter_CusCan     as float))/sum(n)        as BB_enter_CusCan
        ,sum(cast(BB_enter_SysCan     as float))/sum(n)        as BB_enter_SysCan
        ,sum(cast(BB_enter_HM     as float))/sum(n)        as BB_enter_HM
        ,sum(cast(BB_enter_3rd_party     as float))/sum(n)        as BB_enter_3rd_party
        ,sum(cast(BB_Offer_Applied     as float))/sum(n)        as BB_Offer_Applied

        ,x*x                                                    as xx
		,x*BB_enter_CusCan										as x_BB_enter_CusCan
		,x*BB_enter_SysCan										as x_BB_enter_SysCan
		,x*BB_enter_HM											as x_BB_enter_HM		
		,x*BB_enter_3rd_party									as x_BB_enter_3rd_party
		,x*BB_Offer_Applied										as x_BB_Offer_Applied		



        ,Sum(n)                                                as cell_n
		
        ,cast(null as float)                                    as BB_enter_CusCan_regression		
        ,cast(null as float)                                    as BB_enter_SysCan_regression			
        ,cast(null as float)                                    as BB_enter_HM_regression			
        ,cast(null as float)                                    as BB_enter_3rd_party_regression	
        ,cast(null as float)                                    as BB_Offer_Applied_regression			
		


into #Regr_inputs
from #SABB_weekly_agg as agg
group by  quarter_id
         ,agg.SABB_forecast_segment
;

 --Select * from #Regr_inputs;

--weighted univariate regression (weeks is the independent variable, each weekly data point has cell_n observations)
Drop table if exists #Regr_coeff;
select SABB_forecast_segment
        ,sum(cell_n)             as n
        ,sum(Cast(cell_n as bigint)*x)          as sum_x
        ,sum(Cast(cell_n as bigint)*xx)         as sum_xx

        ,sum(cell_n*BB_enter_CusCan)       as sum_BB_enter_CusCan
        ,sum(cell_n*BB_enter_SysCan)       as sum_BB_enter_SysCan
        ,sum(cell_n*BB_enter_HM)       		as sum_BB_enter_HM
        ,sum(cell_n*BB_enter_3rd_party)       as sum_BB_enter_3rd_party
        ,sum(cell_n*BB_Offer_Applied)       as sum_BB_Offer_Applied

        ,sum(cell_n*x_BB_enter_CusCan)       as sum_x_BB_enter_CusCan
        ,sum(cell_n*x_BB_enter_SysCan)       as sum_x_BB_enter_SysCan
        ,sum(cell_n*x_BB_enter_HM)       		as sum_x_BB_enter_HM
        ,sum(cell_n*x_BB_Offer_Applied)       as sum_x_BB_enter_3rd_party
        ,sum(cell_n*x_BB_Offer_Applied)       as sum_x_BB_Offer_Applied

        ,cast(null as float)     as b0_BB_enter_CusCan
		,cast(null as float)     as b0_BB_enter_SysCan
		,cast(null as float)     as b0_BB_enter_HM
		,cast(null as float)     as b0_BB_enter_3rd_party
		,cast(null as float)     as b0_BB_Offer_Applied
		
        ,cast(null as float)     as b1_BB_enter_CusCan
		,cast(null as float)     as b1_BB_enter_SysCan
		,cast(null as float)     as b1_BB_enter_HM
		,cast(null as float)     as b1_BB_enter_3rd_party
		,cast(null as float)     as b1_BB_Offer_Applied
		

into #Regr_coeff
from #Regr_inputs
group by SABB_forecast_segment
having n > 1000
;



update #Regr_coeff set b1_BB_enter_CusCan        = (sum_x_BB_enter_CusCan       - (sum_BB_enter_CusCan       *sum_x)/n)/(sum_xx -(sum_x*sum_x)/n);
update #Regr_coeff set b1_BB_enter_SysCan        = (sum_x_BB_enter_SysCan       - (sum_BB_enter_SysCan       *sum_x)/n)/(sum_xx -(sum_x*sum_x)/n);
update #Regr_coeff set b1_BB_enter_HM        = (sum_x_BB_enter_HM       - (sum_BB_enter_HM       *sum_x)/n)/(sum_xx -(sum_x*sum_x)/n);
update #Regr_coeff set b1_BB_enter_3rd_party        = (sum_x_BB_enter_3rd_party       - (sum_BB_enter_3rd_party       *sum_x)/n)/(sum_xx -(sum_x*sum_x)/n);
update #Regr_coeff set b1_BB_Offer_Applied        = (sum_x_BB_Offer_Applied       - (sum_BB_Offer_Applied       *sum_x)/n)/(sum_xx -(sum_x*sum_x)/n);



update #Regr_coeff set b0_BB_enter_CusCan        = sum_BB_enter_CusCan       /n      - b1_BB_enter_CusCan        *sum_x/n;
update #Regr_coeff set b0_BB_enter_SysCan        = sum_BB_enter_SysCan       /n      - b1_BB_enter_SysCan        *sum_x/n;
update #Regr_coeff set b0_BB_enter_HM       	 = sum_BB_enter_HM       /n      - b1_BB_enter_HM        *sum_x/n;
update #Regr_coeff set b0_BB_enter_3rd_party      = sum_BB_enter_3rd_party       /n      - b1_BB_enter_3rd_party        *sum_x/n;
update #Regr_coeff set b0_BB_Offer_Applied      = sum_BB_Offer_Applied       /n      - b1_BB_Offer_Applied        *sum_x/n;


-----------------------------------------------------------------------------------------------------------
----------  Active Blocks ---------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------

/*
drop table if exists #SABB_weekly_agg;
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

*/


---------------------------------------------------------------------------------------------------
-- Set proc outputs -------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
Select LV,'CusCan Entry' as Metric,SABB_forecast_segment as forecast_segment,b1_BB_enter_CusCan,b0_BB_enter_CusCan
from #Regr_coeff
union all
Select LV,'SysCan Entry'  as Metric,SABB_forecast_segment as forecast_segment,b1_BB_enter_SysCan,b0_BB_enter_SysCan
from #Regr_coeff
union all
Select LV,'HM Entry' as Metric,SABB_forecast_segment as forecast_segment,b1_BB_enter_HM,b0_BB_enter_HM
from #Regr_coeff
union all
Select LV,'3rd Party Entry' as Metric,SABB_forecast_segment as forecast_segment,b1_BB_enter_3rd_party,b0_BB_enter_3rd_party
from #Regr_coeff
union all

Select LV,'BB Offer Applied' as Metric,SABB_forecast_segment as forecast_segment,b1_BB_Offer_Applied,b0_BB_Offer_Applied
from #Regr_coeff
;


END;

Grant Execute on SABB_Regression_Coefficient to CITeam;

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


/*
Call Regression_Coefficient(201601,2);

Select *
-- into #Regression_Coefficient_Test
from Regression_Coefficient(201601,2);

Select * from #Regression_Coefficient_Test
*/







