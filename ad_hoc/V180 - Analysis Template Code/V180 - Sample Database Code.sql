


--V180 - Create a 0.1% sample of daily viewing into a single table (for Ad-Hoc Analysis)

---Create Full Daily Viewing Details for a 0.1% sample (Phase II Onwards)---
CREATE TABLE  sample_database_full_data
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
            ,tx_date_utc                                date
            ,x_broadcast_Time_Of_Day                    varchar(15)
            ,pay_free_indicator                         varchar(50)
            ,channel_name_inc_hd                        varchar(30)
            ,pay_channel                                tinyint
            ,scaling_weighting                          float
)
;
commit;
CREATE VARIABLE @snapshot_start_dt              datetime;
CREATE VARIABLE @snapshot_end_dt                datetime;
CREATE VARIABLE @viewing_var_sql                varchar(5000);
CREATE VARIABLE @viewing_scanning_day           datetime;
CREATE VARIABLE @playback_snapshot_start_dt            datetime;

SET @viewing_var_sql = '
insert into sample_database_full_data (
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
                ,b.service_key                                
                ,Channel_Name                              
                ,epg_title                                 
                ,duration                                   
                ,tx_date_utc                                
                ,x_broadcast_Time_Of_Day                   
                ,pay_free_indicator 
                ,channel_name_inc_hd
                ,pay_channel
                ,scaling_weighting
from vespa_analysts.VESPA_DAILY_AUGS_##^^*^*## as a
left outer join epg_data_phase_2 as b 
on a.programme_trans_sk=b.programme_trans_sk
where  right(a.account_number,3) in (''438'')
)
'
;
SET @snapshot_start_dt  = '2013-04-01'; --Rerrun from here (phase II starts 14th Aug 2012--
--SET @snapshot_start_dt  = '2012-08-14';  --Original
SET @snapshot_end_dt    = '2013-05-09';  -- Current Date of EPG Data

SET @viewing_scanning_day = @snapshot_start_dt;

while @viewing_scanning_day <= @snapshot_end_dt
begin
    EXECUTE(replace(@viewing_var_sql,'##^^*^*##',dateformat(@viewing_scanning_day, 'yyyymmdd')))
    commit

    set @viewing_scanning_day = dateadd(day, 1, @viewing_scanning_day)
end

commit;

--select count(*),sum(scaling_weighting),sum(case when scaling_weighting>0 then 1 else 0 end) from sample_database_full_data;

