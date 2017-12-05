text
create procedure pitteloudj.SABB_Forecast_Activation_Vols( in Y2W01 integer,in Y3W52 integer ) 
result( 
  Subs_Week_Of_Year smallint,
  Reinstates integer,
  Acquisitions integer,
  New_Customers integer ) 
begin
  set temporary option Query_Temp_Space_Limit = 0;
  message cast(now() as timestamp) || ' | Forecast_Activation_Vols - Initialization begin ' to client;
  drop table if exists #Sky_Calendar;
  create table #Sky_Calendar(
    calendar_date date null,
    subs_week_of_year integer null,);
  create lf index idx_1 on #Sky_Calendar(calendar_date);
  insert into #Sky_Calendar
    select calendar_date,
      subs_week_of_year
      from CITeam.Subs_Calendar(Y2W01/100,Y3W52/100)
      where subs_week_and_year between Y2W01 and Y3W52 and subs_last_day_of_week = 'Y' and subs_week_of_year <> 53;
  message cast(now() as timestamp) || ' | Forecast_Activation_Vols - Calendar setup' to client;
  select end_date,
    cast(null as integer) as Subs_Week_Of_Year,
    sum(case when BB_latest_act_dt between(end_date-6) and end_date and(BB_first_act_dt < BB_latest_act_dt) then 1 else 0 end) as Reinstates,
    sum(case when BB_latest_act_dt between(end_date-6) and end_date and(BB_first_act_dt = BB_latest_act_dt) then 1 else 0 end) as Acquisitions,
    Reinstates+Acquisitions as New_Customers
    into #Activation_Vols
    from pitteloudj.cust_fcast_weekly_Base_2 as base
    where base.end_date = any(select calendar_date from #Sky_Calendar)
    group by end_date;
  message cast(now() as timestamp) || ' | Forecast_Activation_Vols - Activations table DONE:' || @@rowcount to client;
  update #Activation_Vols as av
    set subs_week_of_year = sc.subs_week_of_year from
    #Activation_Vols as av
    join #Sky_Calendar as sc on sc.calendar_date = av.end_date;
  select Subs_Week_Of_Year,
    Avg(Coalesce(av.Reinstates,0)) as Reinstates,
    Avg(Coalesce(av.Acquisitions,0)) as Acquisitions,
    Avg(Coalesce(av.New_Customers,0)) as New_Customers
    from #Activation_Vols as av
    group by Subs_Week_Of_Year;
  message cast(now() as timestamp) || ' | Forecast_Activation_Vols - Proc completed :' to client
end