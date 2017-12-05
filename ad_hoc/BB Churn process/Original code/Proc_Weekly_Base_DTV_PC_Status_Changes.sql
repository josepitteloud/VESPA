/*Create variable Refresh_Dt date; Set Refresh_Dt = '2016-11-01';

drop variable if exists Dynamic_SQL; Create variable Dynamic_SQL long varchar;
drop variable if exists Weekly_Base_Table; Create variable Weekly_Base_Table varchar(100) default null;
drop variable if exists schema_Name; Create variable schema_Name varchar(100) default null;
drop variable if exists Weekly_Base_Loop_Num; Create variable Weekly_Base_Loop_Num integer default null;

Setuser CITeam;
*/
Drop procedure if exists Weekly_Base_PC_Status_Change_Update;

Create procedure Weekly_Base_PC_Status_Change_Update(IN Refresh_Dt date)
SQL Security DEFINER
BEGIN

Declare Dynamic_SQL long varchar;
Declare Weekly_Base_Table varchar(100);
Declare schema_Name varchar(100) default null;
Declare Weekly_Base_Loop_Num integer;

SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;
Commit;


Select distinct end_date into #End_Dates from citeam.cust_fcast_weekly_base;
Select * into #sky_calendar from CITeam.subs_calendar(2012,2020);

Select subs_year,subs_quarter_of_year,
       '' || subs_year || 'Q' || subs_quarter_of_year Subs_Qtr,
        Row_Number() over(order by subs_year,subs_quarter_of_year) Row_ID
into #Base_Quarters
from #End_Dates ed
     inner join
     #sky_calendar sc
     on sc.calendar_date = ed.end_date
group by subs_year,subs_quarter_of_year;

-- Select * from #Base_Quarters;
-----------------------------------------------------------------------------------------
-- Create table of DTV_PC_Next_Status_Code , DTV_PC_Effective_To_Dt -----------------
-----------------------------------------------------------------------------------------
Drop table if exists #PC_Next_Status_Entries;
select Subs_year,Subs_Week_of_Year,account_number,
       Cast(event_dt - datepart(weekday,event_dt+2) as date) as end_date,
       MoR.PC_Next_Status_Code DTV_PC_Next_Status_Code,
       MoR.PC_Effective_To_Dt DTV_PC_Effective_To_Dt,
       Row_number() over(partition by account_number,end_date order by event_dt,PC_Future_Sub_Effective_Dt desc) Status_change_rnk
into #PC_Next_Status_Entries
from CITeam.Master_Of_Retention MoR
where PC_Effective_To_Dt >= Refresh_dt
        and (MoR.Same_Day_Cancels > 0 or PC_Pending_Cancellations > 0 or Same_Day_PC_Reactivations > 0)
;

Delete from #PC_Next_Status_Entries where Status_change_rnk > 1;

Set schema_Name='CITeam.';

Set Weekly_Base_Loop_Num = 1;

While Weekly_Base_Loop_Num <= (Select max(Row_ID) from #Base_Quarters) Loop

Set Weekly_Base_Table = 'Cust_Fcast_Weekly_Base_'
                        || (Select max(Subs_Qtr) from #Base_Quarters where Row_ID = Weekly_Base_Loop_Num);



            -----------------------------------------------------------------------------------------
            -- Create table of DTV_PC_Next_Status_Code , DTV_PC_Effective_To_Dt -----------------
            -----------------------------------------------------------------------------------------

            Set Dynamic_SQL = ''
            || 'Update ' ||  schema_name ||  Weekly_Base_Table ||' as base ' || Char(13)
            || 'Set base.DTV_PC_Next_Status_Code = PNS.DTV_PC_Next_Status_Code, '
            || '  base.DTV_PC_Effective_To_Dt = PNS.DTV_PC_Effective_To_Dt '
            || '  from '
            ||  schema_name ||  Weekly_Base_Table ||' base '
            || '   inner join '
            || '   #PC_Next_Status_Entries PNS '
            || '   on base.account_number= PNS.account_number '
            || '      and base.end_date = PNS.end_date; '
            || Char(13)
            ;

--             select Dynamic_SQL;
            Execute(Dynamic_SQL);

Set Weekly_Base_Loop_Num = Weekly_Base_Loop_Num + 1;

end Loop;

END;

Grant execute on Weekly_Base_PC_Status_Change_Update to CITeam;
/*
Setuser;

call CITeam.Weekly_Base_PC_Status_Change_Update('1900-01-01');

Select top 1000 account_number,end_date,DTV_PC_PL_Entry_Dt,DTV_PC_Future_Sub_Effective_Dt,DTV_PC_Effective_To_Dt,DTV_PC_Next_Status_Code
from CITeam.Cust_Fcast_Weekly_Base
where end_date >= '2015-06-30' and DTV_PC_PL_Entry_Dt is null and DTV_PC_Effective_To_Dt is not null

Select DTV_PC_Effective_To_Dt,count(*)
from citeam.cust_fcast_weekly_base
where DTV_active = 1
group by DTV_PC_Effective_To_Dt
*/
