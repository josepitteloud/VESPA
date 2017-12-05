---Calculate Accounts and Number of Days Data Returned per day---


select account_number 
,count (distinct cast(instance_start_date_time_utc as date)) as distinct_viewing_days
into #days_viewing_by_account_201308
from sk_prod.vespa_dp_prog_viewed_201308
where dateformat(instance_start_date_time_utc,'HH') not in ('00','01','02','03','04')
group by account_number
;


select top 100 * from  sk_prod.vespa_dp_prog_viewed_201212
commit;

select cast(instance_start_date_time_utc as date) as instance_dt
,count(*) from  sk_prod.vespa_dp_prog_viewed_201309
group by instance_dt
order by instance_dt;
commit;

select count(*) from vespa_analysts.VESPA_DAILY_AUGS_20130106

select top 100 * from vespa_analysts.VESPA_DAILY_AUGS_20130106 where capped_flag=2


commit;






commit;

if object_id('Project_161_viewing_table') is not null drop table Project_161_viewing_table;
create table Project_161_viewing_table (
Viewing_date                    date

);

commit;
-- Build string with placeholder for changing daily table reference
SET @var_sql = '
insert into Project_161_viewing_table(
Viewing_date

)
select
    cast(da.viewing_starts as date),cast(prog.Tx_Start_Datetime_UTC as date),vw.cb_row_ID,vw.Account_Number,vw.Subscriber_Id
    ,vw.Cb_Key_Household,vw.Cb_Key_Family,vw.Cb_Key_Individual
    ,vw.Event_Type,vw.X_Type_Of_Viewing_Event
    ,vw.Adjusted_Event_Start_Time
    ,da.capped_event_end_time,prog.Tx_Start_Datetime_UTC,prog.Tx_End_Datetime_UTC
    ,da.viewing_starts,da.viewing_stops,da.viewing_duration
    ,vw.Recorded_Time_UTC
    ,da.timeshifting
    ,prog.programme_duration, vw.X_Viewing_Time_Of_Day,vw.Programme_Trans_Sk
    ,prog.Channel_Name,prog.Epg_Title,prog.Genre_Description,prog.Sub_Genre_Description
    ,da.capped_flag

from vespa_analysts.ph1_VESPA_DAILY_AUGS_##^^*^*## as da
inner join sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*## as vw
    on da.cb_row_ID = vw.cb_row_ID
inner join VESPA_Programmes_project_161 as prog
    on vw.programme_trans_sk = prog.programme_trans_sk
';
-- Filter for viewing events is applied on the daily augs table already.
-- Loop over the days in the period, extracting all the data.
commit;

SET @scanning_day = @var_prog_period_start;
--delete from Project_161_viewing_table;
commit;
while @scanning_day <= dateadd(dd,0,@var_prog_period_end)
begin
    EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(@scanning_day, 'yyyymmdd')))
--    commit

    set @scanning_day = dateadd(day, 1, @scanning_day)
end;
commit;

grant select on Project_161_viewing_table to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh
, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg,vespa_analysts;
commit;


--select count(*) , count(distinct account_number) from Project_161_viewing_table;

commit;
create  hg index idx1 on Project_161_viewing_table (account_number);
create  hg index idx2 on Project_161_viewing_table (Viewing_date);

commit;




















