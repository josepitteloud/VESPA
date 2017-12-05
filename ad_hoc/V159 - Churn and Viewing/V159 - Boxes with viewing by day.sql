
---Box Returning data by day

---V159 Daily Viewing Summary---

--select top 100 * from vespa_analysts.VESPA_DAILY_AUGS_20130210;
--select top 100 * from sk_prod.VESPA_STB_PROG_EVENTS_20120518;

commit;
--------------------------------------------------------------------------------
--PART A: Viewing Data (For Customers active in the snapshot period)
--------------------------------------------------------------------------------

---Get details of Programmes Watched 3+ Minutes of---
CREATE VARIABLE @snapshot_start_dt              datetime;
CREATE VARIABLE @snapshot_end_dt                datetime;
CREATE VARIABLE @viewing_var_sql                varchar(5000);
CREATE VARIABLE @viewing_scanning_day           datetime;
CREATE VARIABLE @playback_snapshot_start_dt            datetime;
--CREATE VARIABLE @viewing_var_num_days           smallint;


-- Date range of programmes to capture
SET @snapshot_start_dt  = '2013-01-01';  --Had to restart loop half way through
--SET @snapshot_start_dt  = '2012-09-01';  --Original
SET @snapshot_end_dt    = '2013-01-28';



IF object_ID ('V159_Daily_viewing_summary_all_box_split') IS NOT NULL THEN
            DROP TABLE  V159_Daily_viewing_summary_all_box_split
END IF;

CREATE TABLE  V159_Daily_viewing_summary_all_box_split
    ( cb_row_ID                                         bigint       not null --primary key
            ,Account_Number                             varchar(20)  not null
            ,Subscriber_Id                              bigint
            ,programme_trans_sk                         bigint
            ,timeshifting                               varchar(15)
            ,viewing_starts                             datetime
            ,viewing_stops                              datetime
            ,viewing_Duration                           decimal(10,0)
            ,capped_flag                                tinyint
            ,capped_event_end_time                      datetime
            ,service_key                                int
            ,Channel_Name                               varchar(30)
            ,epg_title                                  varchar(50)
            ,duration                                   int
            ,Genre_Description                          varchar(30)
            ,Sub_Genre_Description                      varchar(30)
            ,epg_group_Name                             varchar(30)
            ,network_indicator                          varchar(50)
            ,tx_date_utc                                date
            ,x_broadcast_Time_Of_Day                    varchar(15)
            ,pay_free_indicator                         varchar(50)
)
;
commit;

IF object_ID ('V159_Daily_viewing_summary_box_split') IS NOT NULL THEN
            DROP TABLE  V159_Daily_viewing_summary_box_split
END IF;

CREATE TABLE  V159_Daily_viewing_summary_box_split
    ( 
            Account_Number                             varchar(20)  not null
            ,Subscriber_Id                              bigint
            ,viewing_day                      date
            ,viewing_post_6am                      tinyint
            ,viewing_Duration                     bigint
           
)
;

--select top 100 * from vespa_analysts.VESPA_DAILY_AUGS_20121001
--select top 100 * from V159_Tenure_10_16mth_Viewing
-- Build string with placeholder for changing daily table reference
SET @viewing_var_sql = '
        insert into V159_Daily_viewing_summary_all_box_split(
                cb_row_ID
                ,Account_Number
                ,Subscriber_Id
                ,programme_trans_sk
                ,timeshifting
                ,viewing_starts
                ,viewing_stops
                ,viewing_Duration
                ,capped_flag
               ,capped_event_end_time
)
        select
                a.cb_row_ID
                ,a.Account_Number
                ,a.Subscriber_Id
                ,a.programme_trans_sk
                ,a.timeshifting
                ,a.viewing_starts
                ,a.viewing_stops
                ,a.viewing_Duration
                ,a.capped_flag
                ,a.capped_event_end_time
from vespa_analysts.VESPA_DAILY_AUGS_##^^*^*## as a

insert into V159_Daily_viewing_summary_box_split
select account_number
,Subscriber_Id
,min(cast(viewing_starts as date)) as viewing_date
,max(case when dateformat(viewing_starts,''HH'') in (''00'',''01'',''01'',''03'',''04'',''05'') then 0 else 1 end) as viewing_post_6am
,sum(viewing_duration) as total_duration

from V159_Daily_viewing_summary_all_box_split as a
group by account_number
,Subscriber_Id

delete from V159_Daily_viewing_summary_all_box_split where account_number is null or account_number is not null
'
;
--select max (viewing_starts) from vespa_analysts.VESPA_DAILY_AUGS_20121104;
--select genre_description , count(*) from V159_epg_data group by genre_description
--select top 100 * from V159_Tenure_10_16mth_Viewing;
--select top 100 * from  vespa_analysts.VESPA_DAILY_AUGS_20130201;

-- Filter for viewing events is applied on the daily augs table already.
-- Loop over the days in the period, extracting all the data.


SET @viewing_scanning_day = @snapshot_start_dt;

while @viewing_scanning_day <= @snapshot_end_dt
begin
    EXECUTE(replace(@viewing_var_sql,'##^^*^*##',dateformat(@viewing_scanning_day, 'yyyymmdd')))
    commit

    set @viewing_scanning_day = dateadd(day, 1, @viewing_scanning_day)
end


--select viewing_day , count(*) from V159_Daily_viewing_summary group by viewing_day order by viewing_day desc;
--select top 5000 * into V159_Daily_viewing_summary_test from V159_Daily_viewing_summary;commit;
commit;
--select dateformat(viewing_starts,'HH') as hourss , count(*) from vespa_analysts.VESPA_DAILY_AUGS_20130126 group by hourss
--select top 5000 * from V159_Daily_viewing_summary
--select count(*) from V159_Daily_viewing_summary

--select * from V159_Daily_viewing_summary;format
commit;


select top 100 * from V159_Daily_viewing_summary_box_split;


select account_number
,viewing_day
,count(distinct subscriber_id) as subscriber_ids
into #by_day
from V159_Daily_viewing_summary_box_split
where viewing_post_6am=1
group by account_number
,viewing_day
;

select subscriber_ids
,count(*) as records
from #by_day
group by subscriber_ids
order by subscriber_ids

commit;







