text
create procedure pitteloudj.SABB_Offer_Applied_Duration_Dist( in Forecast_Start_Wk integer,in Num_Wks integer ) 
result( 
  Offer_segment varchar(30),
  Total_Offer_Duration_Mth integer,
  Weekly_Avg_New_Offers integer,
  Total_New_Offers integer,
  Cum_New_Offers integer,
  Dur_Pctl_Lower_Bound real,
  Dur_Pctl_Upper_Bound real ) 
begin
  message cast(now() as timestamp) || ' | SABB_Offer_Applied_Duration_Dist - Initialization begin ' to client;
  select *
    into #Sky_Calendar
    from subs_calendar(Forecast_Start_Wk/100-1,Forecast_Start_Wk/100);
  select case overall_offer_segment
    when '2.(BB)A1.Acquisition/Upgrade' then 'Activations'
    when '2.(BB)B1.TA' then 'TA'
    when '2.(BB)B2.CoE' then 'Other'
    when '2.(BB)B3.PAT' then 'Other'
    when '2.(BB)B4.Pipeline ReInstate' then 'Reactivations'
    when '2.(BB)B5.Other Retention' then 'Reactivations'
    when '2.(BB)C1.BB Package Movement' then 'Other'
    when '2.(BB)C2.Offer On Call' then 'Other'
    when '2.(BB)C4.Other' then 'Other' end as overall_offer_segment,
    Total_Offer_Duration_Mth,
    COUNT()/Num_Wks as Weekly_Avg_New_Offers,
    Sum(Weekly_Avg_New_Offers) over(partition by overall_offer_segment) as Total_New_Offers,
    Sum(Weekly_Avg_New_Offers) over(partition by overall_offer_segment order by Total_Offer_Duration_Mth asc) as Cum_New_Offers,
    cast(Cum_New_Offers as real)/Total_New_Offers as Pctl_New_Offers,
    Row_Number() over(partition by overall_offer_segment order by Total_Offer_Duration_Mth asc) as Dur_Rnk
    into #Offer_Dur
    from citeam.offer_usage_all as oua
    where offer_start_dt_Actual between(select max(calendar_date-7-Num_Wks*7+1) from #sky_calendar where subs_week_and_year = Forecast_Start_Wk)
    and(select max(calendar_date-7) from #sky_calendar where subs_week_and_year = Forecast_Start_Wk)
    and Total_Offer_Duration_Mth <= 36
    and offer_start_dt_Actual = Whole_Offer_Start_Dt_Actual
    and Subs_Type = 'Broadband DSL Line'
    and lower(offer_dim_description) not like '%price protection%'
    and oua.overall_offer_segment_grouped_1 <> 'Price Protection'
    group by overall_offer_segment,
    Total_Offer_Duration_Mth;
  message cast(now() as timestamp) || ' | SABB_Offer_Applied_Duration_Dist - Offer_Dur table completed: ' || @@rowcount to client;
  select dur1.overall_offer_segment,
    dur1.Total_Offer_Duration_Mth,
    dur1.Weekly_Avg_New_Offers,
    dur1.Total_New_Offers,
    dur1.Cum_New_Offers,
    Coalesce(dur2.Pctl_New_Offers,0) as Dur_Pctl_Lower_Bound,
    dur1.Pctl_New_Offers as Dur_Pctl_Upper_Bound
    from #Offer_Dur as dur1
      left outer join #Offer_Dur as dur2 on dur2.overall_offer_segment = dur1.overall_offer_segment and dur2.Dur_Rnk = dur1.Dur_Rnk-1;
  message cast(now() as timestamp) || ' | SABB_Offer_Applied_Duration_Dist - COMPLETED' to client
end