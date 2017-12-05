
  -- Set up parameters
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2011-08-11';
SET @var_prog_period_end    = '2011-09-01';

select
      programme_trans_sk
      ,Channel_Name
      ,Epg_Title
      ,Genre_Description
      ,Sub_Genre_Description
      ,Tx_Start_Datetime_UTC
      ,Tx_End_Datetime_UTC
  into TEST_VESPA_Programmes_20110811 -- drop table VESPA_Programmes_20110811
  from sk_prod.VESPA_EPG_DIM
 where programme_trans_sk in (
201108120000014061
,201108120000002465)
;
--select * from VESPA_Programmes_20110811 where epg_title = 'Trollied';
--select programme_trans_sk from VESPA_Programmes_20110811 where epg_title = 'Trollied';


create unique hg index idx1 on TEST_VESPA_Programmes_20110811(programme_trans_sk);

SET @var_cntr = 0;
SET @var_num_days = 21;       -- Get events up to 30 days of the programme broadcast time (only 20 in this case due to Vespa Suspension at end August

-- To store all the viewing records:
create table TEST_VESPA_tmp_all_viewing_records_trollied_20110811 ( -- drop table TEST_VESPA_tmp_all_viewing_records_trollied_20110811
    cb_row_ID                       bigint      not null primary key
    ,Account_Number                 varchar(20) not null
    ,Subscriber_Id                  decimal(8,0) not null
    ,Cb_Key_Household               bigint
    ,Cb_Key_Family                  bigint
    ,Cb_Key_Individual              bigint
    ,Event_Type                     varchar(20) not null
    ,X_Type_Of_Viewing_Event        varchar(40) not null
    ,Adjusted_Event_Start_Time      datetime
    ,X_Adjusted_Event_End_Time      datetime
    ,X_Viewing_Start_Time           datetime
    ,X_Viewing_End_Time             datetime
    ,Tx_Start_Datetime_UTC          datetime
    ,Tx_End_Datetime_UTC            datetime
    ,Recorded_Time_UTC              datetime
    ,Play_Back_Speed                decimal(4,0)
    ,X_Event_Duration               decimal(10,0)
    ,X_Programme_Duration           decimal(10,0)
    ,X_Programme_Viewed_Duration    decimal(10,0)
    ,X_Programme_Percentage_Viewed  decimal(3,0)
    ,X_Viewing_Time_Of_Day          varchar(15)
    ,Programme_Trans_Sk             bigint      not null
    ,Channel_Name                   varchar(30)
    ,Epg_Title                      varchar(50)
    ,Genre_Description              varchar(30)
    ,Sub_Genre_Description          varchar(30)
);
--exec gen_Create_table 'sk_prod.VESPA_STB_PROG_EVENTS_20110811'
-- Build string with placeholder for changing daily table reference
SET @var_sql = '
    insert into TEST_VESPA_tmp_all_viewing_records_trollied_20110811
    select
        vw.cb_row_ID,vw.Account_Number,vw.Subscriber_Id,vw.Cb_Key_Household,vw.Cb_Key_Family
        ,vw.Cb_Key_Individual,vw.Event_Type,vw.X_Type_Of_Viewing_Event,vw.Adjusted_Event_Start_Time
        ,vw.X_Adjusted_Event_End_Time,vw.X_Viewing_Start_Time,vw.X_Viewing_End_Time
        ,prog.Tx_Start_Datetime_UTC,prog.Tx_End_Datetime_UTC,vw.Recorded_Time_UTC,vw.Play_Back_Speed
        ,vw.X_Event_Duration,vw.X_Programme_Duration,vw.X_Programme_Viewed_Duration
        ,vw.X_Programme_Percentage_Viewed,vw.X_Viewing_Time_Of_Day,vw.Programme_Trans_Sk
        ,prog.Channel_Name,prog.Epg_Title,prog.Genre_Description,prog.Sub_Genre_Description
     from sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*## as vw
          inner join TEST_VESPA_Programmes_20110811 as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
        -- Filter for viewing events during extraction
     where 
--(play_back_speed is null or play_back_speed = 2) and 
        
-- x_programme_viewed_duration > 0
--        and 
Panel_id in (4,5)
--        and x_type_of_viewing_event <> ''Non viewing event'''
      ;


  -- ####### Loop through to populate table: Sybase Interactive style (not entirely tested) ######
--FLT_1: LOOP

    --EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(dateadd(day, @var_cntr, @var_prog_period_start), 'yyyymmdd'));

    --SET @var_cntr = @var_cntr + 1;
    --IF @var_cntr > @var_num_days THEN LEAVE FLT_1;
    --END IF ;

--END LOOP FLT_1;
  -- ####### End of loop (this loop structure not tested yet) ######

  -- ####### Alternate Loop: WinSQL style (tested, good) ######
while @var_cntr < @var_num_days
begin
    EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(dateadd(day, @var_cntr, @var_prog_period_start), 'yyyymmdd')))

    commit

    set @var_cntr = @var_cntr + 1
end;

commit;


--select count(*) from TEST_VESPA_tmp_all_viewing_records_trollied_20110811;

--select * from TEST_VESPA_tmp_all_viewing_records_trollied_20110811 where subscriber_id = 24144928 order by adjusted_event_start_time; 




 select
       document_creation_date , stb_log_creation_date, vw.Account_Number,vw.Subscriber_Id,vw.Event_Type,vw.X_Type_Of_Viewing_Event,vw.Adjusted_Event_Start_Time
        ,vw.X_Adjusted_Event_End_Time,vw.X_Viewing_Start_Time,vw.X_Viewing_End_Time
        ,prog.Tx_Start_Datetime_UTC,prog.Tx_End_Datetime_UTC,vw.Recorded_Time_UTC,vw.Play_Back_Speed
        ,vw.X_Event_Duration,vw.X_Programme_Duration,vw.X_Programme_Viewed_Duration
        ,vw.X_Programme_Percentage_Viewed,vw.X_Viewing_Time_Of_Day,vw.Programme_Trans_Sk
        ,prog.Channel_Name,prog.Epg_Title
     from sk_prod.VESPA_STB_PROG_EVENTS_20110811 as vw
         left outer join sk_prod.VESPA_EPG_DIM as prog
         on vw.programme_trans_sk = prog.programme_trans_sk
        -- Filter for viewing events during extraction
     where subscriber_id = 24144928
--(play_back_speed is null or play_back_speed = 2) and 
        
-- x_programme_viewed_duration > 0
--        and Panel_id in (4,5)
--        and x_type_of_viewing_event <> ''Non viewing event'''
  order by adjusted_event_start_time    ;

output to 'C:\Users\barnetd\Documents\Project 002 - Ad Analysis Trollied\sub_24144928_example.xls' format excel; 

commit;


select count(*) from sk_prod.VESPA_STB_PROG_EVENTS_20111201
select count(distinct subscriber_id) from sk_prod.VESPA_STB_PROG_EVENTS_20111202

