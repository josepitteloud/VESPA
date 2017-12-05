text
create procedure spencerc2.SABB_Regression_Coefficient( in LV integer,in Regression_Yrs tinyint ) 
result( LV integer,Metric varchar(30),Fcast_Segment varchar(100),Grad_Coeff real,Intercept_Coeff real ) 
begin
  declare Dynamic_SQL varchar(1000);
  declare Y3W52 integer;
  declare Y1W01 integer;
  select * into #Sky_Calendar from CITeam.Subs_Calendar(LV/100-Regression_Yrs-1,LV/100);
  drop table if exists #Regr_Wks;
  select cast(sc.subs_week_and_year as integer) as Subs_week_and_year,
    Row_Number() over(order by Subs_week_and_year desc) as Wk_Rnk
    into #Regr_Wks
    from #sky_calendar as sc
    where cast(sc.subs_Week_and_year as integer) < LV
    and Subs_Week_of_year <> 53
    group by Subs_week_and_year;
  delete from #Regr_Wks where Wk_Rnk > Regression_Yrs*52+13;
  set Y1W01 = (select min(Subs_week_and_year) from #Regr_Wks);
  set Y3W52 = (select max(Subs_week_and_year) from #Regr_Wks);
  drop table if exists #SABB_weekly_agg;
  select subs_year,
    subs_week_of_year as subs_week,
    cast(subs_week_and_year as integer) as subs_week_and_year,
    node_sa as SABB_forecast_segment,
    Count() as n,
    cast(sum(case when bb_offer_rem_and_end = -9999 then 0 else 1 end) as real) as BB_Offer_Applied,
    cast(sum(bb_enter_SysCan) as real) as bb_enter_SysCan,
    cast(sum(bb_enter_CusCan) as real) as bb_enter_CusCan,
    cast(sum(bb_enter_HM) as real) as bb_enter_HM,
    cast(sum(bb_enter_3rd_party) as real) as bb_enter_3rd_party,
    dense_rank() over(order by subs_week_and_year desc) as week_id,
    case when week_id between 1 and 52 then 'Curr'
    when week_id between 53 and 104 then 'Prev'
    else null
    end as week_position,
    case when week_id between 1 and 13 then 'Y' else 'N' end as last_quarter,
    (week_id/13)+1 as quarter_id,
    Max(Subs_Week) over(partition by Subs_Year) as Max_Subs_Week
    into #SABB_weekly_agg
    from pitteloudj.DTV_Fcast_Weekly_Base_2 as agg
    where subs_week_and_year between Y1W01 and Y3W52
    and subs_week <> 53
    group by subs_year,
    subs_week,
    subs_week_and_year,
    node_sa;
  drop table if exists #Regr_inputs;
  select quarter_id,
    agg.SABB_forecast_segment,
    row_number() over(partition by agg.SABB_forecast_segment order by quarter_id desc) as x,
    sum(cast(BB_enter_CusCan as real))/sum(n) as BB_enter_CusCan,
    sum(cast(BB_enter_SysCan as real))/sum(n) as BB_enter_SysCan,
    sum(cast(BB_enter_HM as real))/sum(n) as BB_enter_HM,
    sum(cast(BB_enter_3rd_party as real))/sum(n) as BB_enter_3rd_party,
    sum(cast(BB_Offer_Applied as real))/sum(n) as BB_Offer_Applied,
    x*x as xx,
    x*BB_enter_CusCan as x_BB_enter_CusCan,
    x*BB_enter_SysCan as x_BB_enter_SysCan,
    x*BB_enter_HM as x_BB_enter_HM,
    x*BB_enter_3rd_party as x_BB_enter_3rd_party,
    x*BB_Offer_Applied as x_BB_Offer_Applied,
    Sum(n) as cell_n,
    cast(null as real) as BB_enter_CusCan_regression,
    cast(null as real) as BB_enter_SysCan_regression,
    cast(null as real) as BB_enter_HM_regression,
    cast(null as real) as BB_enter_3rd_party_regression,
    cast(null as real) as BB_Offer_Applied_regression
    into #Regr_inputs
    from #SABB_weekly_agg as agg
    group by quarter_id,
    agg.SABB_forecast_segment;
  drop table if exists #Regr_coeff;
  select SABB_forecast_segment,
    sum(cell_n) as n,
    sum(cast(cell_n as bigint)*x) as sum_x,
    sum(cast(cell_n as bigint)*xx) as sum_xx,
    sum(cell_n*BB_enter_CusCan) as sum_BB_enter_CusCan,
    sum(cell_n*BB_enter_SysCan) as sum_BB_enter_SysCan,
    sum(cell_n*BB_enter_HM) as sum_BB_enter_HM,
    sum(cell_n*BB_enter_3rd_party) as sum_BB_enter_3rd_party,
    sum(cell_n*BB_Offer_Applied) as sum_BB_Offer_Applied,
    sum(cell_n*x_BB_enter_CusCan) as sum_x_BB_enter_CusCan,
    sum(cell_n*x_BB_enter_SysCan) as sum_x_BB_enter_SysCan,
    sum(cell_n*x_BB_enter_HM) as sum_x_BB_enter_HM,
    sum(cell_n*x_BB_enter_3rd_party) as sum_x_BB_enter_3rd_party,
    sum(cell_n*x_BB_Offer_Applied) as sum_x_BB_Offer_Applied,
    cast(null as real) as b0_BB_enter_CusCan,
    cast(null as real) as b0_BB_enter_SysCan,
    cast(null as real) as b0_BB_enter_HM,
    cast(null as real) as b0_BB_enter_3rd_party,
    cast(null as real) as b0_BB_Offer_Applied,
    cast(null as real) as b1_BB_enter_CusCan,
    cast(null as real) as b1_BB_enter_SysCan,
    cast(null as real) as b1_BB_enter_HM,
    cast(null as real) as b1_BB_enter_3rd_party,
    cast(null as real) as b1_BB_Offer_Applied
    into #Regr_coeff
    from #Regr_inputs
    group by SABB_forecast_segment
    having n > 1000;
  update #Regr_coeff set b1_BB_enter_CusCan = (sum_x_BB_enter_CusCan-(sum_BB_enter_CusCan*sum_x)/n)/(sum_xx-(sum_x*sum_x)/n);
  update #Regr_coeff set b1_BB_enter_SysCan = (sum_x_BB_enter_SysCan-(sum_BB_enter_SysCan*sum_x)/n)/(sum_xx-(sum_x*sum_x)/n);
  update #Regr_coeff set b1_BB_enter_HM = (sum_x_BB_enter_HM-(sum_BB_enter_HM*sum_x)/n)/(sum_xx-(sum_x*sum_x)/n);
  update #Regr_coeff set b1_BB_enter_3rd_party = (sum_x_BB_enter_3rd_party-(sum_BB_enter_3rd_party*sum_x)/n)/(sum_xx-(sum_x*sum_x)/n);
  update #Regr_coeff set b1_BB_Offer_Applied = (sum_x_BB_Offer_Applied-(sum_BB_Offer_Applied*sum_x)/n)/(sum_xx-(sum_x*sum_x)/n);
  update #Regr_coeff set b0_BB_enter_CusCan = sum_BB_enter_CusCan/n-b1_BB_enter_CusCan*sum_x/n;
  update #Regr_coeff set b0_BB_enter_SysCan = sum_BB_enter_SysCan/n-b1_BB_enter_SysCan*sum_x/n;
  update #Regr_coeff set b0_BB_enter_HM = sum_BB_enter_HM/n-b1_BB_enter_HM*sum_x/n;
  update #Regr_coeff set b0_BB_enter_3rd_party = sum_BB_enter_3rd_party/n-b1_BB_enter_3rd_party*sum_x/n;
  update #Regr_coeff set b0_BB_Offer_Applied = sum_BB_Offer_Applied/n-b1_BB_Offer_Applied*sum_x/n;
  select LV,'CusCan Entry' as Metric,SABB_forecast_segment as forecast_segment,b1_BB_enter_CusCan,b0_BB_enter_CusCan
    from #Regr_coeff union all
  select LV,'SysCan Entry' as Metric,SABB_forecast_segment as forecast_segment,b1_BB_enter_SysCan,b0_BB_enter_SysCan
    from #Regr_coeff union all
  select LV,'HM Entry' as Metric,SABB_forecast_segment as forecast_segment,b1_BB_enter_HM,b0_BB_enter_HM
    from #Regr_coeff union all
  select LV,'3rd Party Entry' as Metric,SABB_forecast_segment as forecast_segment,b1_BB_enter_3rd_party,b0_BB_enter_3rd_party
    from #Regr_coeff union all
  select LV,'BB Offer Applied' as Metric,SABB_forecast_segment as forecast_segment,b1_BB_Offer_Applied,b0_BB_Offer_Applied
    from #Regr_coeff
end