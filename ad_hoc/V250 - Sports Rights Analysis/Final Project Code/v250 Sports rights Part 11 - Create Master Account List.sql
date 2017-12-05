/*-----------------------------------------------------------------------------------------------------------------
        Project:V250 - Sports right Analysis Profiling
        Part - Part 11 Create Master Account List
        
        Analyst: Dan Barnett
        SK Prod: 5
        Create Table of 1 record per account number for use for weights/profiling

*/------------------------------------------------------------------------------------------------------------------

----Create Master Account Table based on Total Days Viewing and Total Sports/Overall Viewing---
--dbarnett.v250_days_viewed_by_account
--dbarnett.v250_daily_viewing_duration

-- from V250 - Genre Level Sports Analysis (Total Viewing).sql
--select top 500 * from dbarnett.v250_unannualised_right_activity_by_live_non_live;
--select top 500 * from dbarnett.v250_daily_viewing_duration;

select account_number
,sum(b.viewing_duration) as total_viewing_duration
into #total_viewing_duration
from dbarnett.v250_daily_viewing_duration as b
group by account_number
;
commit;

CREATE HG INDEX idx1 ON #total_viewing_duration (account_number);
commit;

select account_number
,sum(c.viewing_duration) as total_viewing_duration
into #total_viewing_duration_sports
from dbarnett.v250_all_sports_programmes_viewed as c
group by account_number
;
commit;

CREATE HG INDEX idx1 ON #total_viewing_duration_sports (account_number);
commit;
--select count(*) from dbarnett.v250_master_account_list;
--drop table dbarnett.v250_master_account_list;
select a.account_number
,a.total_days_with_viewing
,case when b.total_viewing_duration is null then 0 else b.total_viewing_duration end as total_viewing_duration_all
,case when c.total_viewing_duration is null then 0 else c.total_viewing_duration end as total_viewing_duration_sports
into dbarnett.v250_master_account_list
from dbarnett.v250_days_viewed_by_account as a
left outer join #total_viewing_duration as b
on a.account_number = b.account_number
left outer join #total_viewing_duration_sports as c
on a.account_number = c.account_number
;
commit;

grant all on dbarnett.v250_master_account_list to public;
------



