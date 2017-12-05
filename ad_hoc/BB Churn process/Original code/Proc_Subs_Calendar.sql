/*
Create variable Start_Year integer; Set Start_Year = 2015;
Create variable End_Year integer; Set End_Year = 2017;

Create variable cal_date date;
Create variable min_cal_date date;
Create variable max_cal_date date;

-- First you need to impersonate CITeam
Setuser CITeam;
*/
 Drop procedure if exists Subs_Calendar;

Create procedure Subs_Calendar(In Start_Year integer,In End_Year integer)
Result( Calendar_date date,subs_year integer, subs_week_of_year integer, Subs_Week_And_Year integer,Subs_quarter_of_year integer,Subs_Last_Day_Of_Week char(1))
-- SQL Security Invoker
BEGIN

Declare cal_date date;
Declare min_cal_date date;
Declare max_cal_date date;


-- Calculate start of Start_Year
Set min_cal_date = Cast(Start_Year || '-07-01' as date);
Set min_cal_date = min_cal_date -  datepart(weekday,min_cal_date + 2) + 1;
-- select min_cal_date;

Set max_cal_date = Cast(End_Year + 1 || '-07-01' as date);
Set max_cal_date = max_cal_date -  datepart(weekday,max_cal_date + 2);

CREATE TABLE #Cal_Dates(
                        Row_ID numeric(5) IDENTITY,
                        Calendar_Date date default null,
                        New_Subs_Year bit default 0,
                        New_Subs_Week bit default 0
                        );

Insert into #Cal_Dates(Calendar_Date)
Select top 10000 Cast(null as date) as Calendar_Date
from CITeam.Cust_Fcast_Weekly_Base;

Update #Cal_Dates
Set Calendar_Date = Cast(min_cal_date + Row_ID - 1 as date);

Delete from #Cal_Dates where Calendar_Date > max_cal_date;

Update #Cal_Dates
Set New_Subs_Year = Case when (
                                (month(calendar_date) = 7 and day(calendar_date) = 1)
                                or
                                (month(calendar_date) = 6 and day(calendar_date) between 25 and 30)
                               )
                         then 1
                         else 0
                    end
    ,New_Subs_Week = 1
where datepart(weekday,Calendar_Date+2) = 1
       ;

Select *,sum(New_Subs_Year) over(order by Cast(Calendar_Date as integer)) Subs_Year_ID
into #Cal_Dates_1
from #Cal_Dates;

Select *
,Start_Year + Subs_Year_ID - 1 as Subs_Year
,sum(New_Subs_Week) over(partition by Subs_Year_ID order by Cast(Calendar_Date as integer)) Subs_Week_Of_Year
,Case when datepart(weekday,Calendar_Date + 2) = 7 then 'Y' else 'N' end as Subs_Last_Day_Of_Week
into #Cal_Dates_2
from #Cal_Dates_1;

Select
Calendar_date,
Subs_Year,
Subs_Week_Of_Year,
Subs_Year*100 + Subs_Week_Of_Year as Subs_Week_And_Year,
Case when Subs_Week_Of_Year between 1 and 13 then 1
            when Subs_Week_Of_Year between 14 and 26 then 2
            when Subs_Week_Of_Year between 27 and 39 then 3
            when Subs_Week_Of_Year between 40 and 53 then 4
end Subs_quarter_of_year,
Subs_Last_Day_Of_Week
from #Cal_Dates_2;

END;

-- Grant execute rights to the members of CITeam
grant execute on SUBS_CALENDAR to CITeam;
/*
-- Change back to your account
Setuser;

-- Test it
Select * from  CITeam.SUBS_CALENDAR(2015,2016);

*/







/* Test proc replicates subs calendar
Select *
from CITeam.Subs_Calendar(2007,2016) a
     full outer join
     sky_calendar b
     on a.calendar_date = b.calendar_date
        and a.subs_year = b.subs_year
        and a.subs_week_of_year = b.subs_week_of_year
        and a.Subs_Last_Day_of_Week = b.Subs_Last_Day_of_Week
where coalesce(a.subs_year,b.subs_year) between 2010 and 2015
*/

