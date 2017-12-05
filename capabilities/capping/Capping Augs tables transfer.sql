--*********************************************
--***** Created by Berwyn Cort 28/11/2013 *****
--*********************************************

/* -- to check status of run
EXECUTE logger_get_latest_job_events 'Capping2.x CUSTOM';
*/

/* -- to check which tables are in which schema
SELECT  tname
FROM    sys.syscatalog
WHERE   UPPER(tname) LIKE 'VESPA_DAILY_AUG%'
AND     creator = 'cortb' -- change schema accordingly 
*/

-- Some admin
-- drop table Vespa_Daily_Augs_20140331									-- to delete tables with no data that ran over the month
-- select * from vespa_analysts.Vespa_daily_augs_20140329				-- to see if there's data in a table
-- call dba.sp_drop_table('vespa_analysts','Vespa_daily_augs_20140335')	-- to drop tables in VA if needed

-- Transferring the Tables
declare @day_counter varchar(10000), @counter integer, @sql varchar(10000), @sql2 varchar(10000), @sql3 varchar(10000)

set @day_counter = '20140329' -- set this to the first day of augs table in format YYYYMMDD - in own schema - note: this cannot over-run the end of the month
set @counter =1

while @counter <=7		-- change number according to how many days you want to transfer

    Begin

        set @sql = -- Creates daily tables
            'call dba.sp_create_table (''vespa_analysts'',''Vespa_daily_augs_' || @day_counter || ''','' cb_row_id                   bigint              primary key
                ,account_number             varchar(20)         not null
                ,subscriber_id              bigint              not null
                ,programme_trans_sk         bigint
                ,timeshifting               varchar(10)
                ,viewing_starts             datetime
                ,viewing_stops              datetime
                ,viewing_duration           bigint
                ,capped_flag                tinyint
                ,capped_event_end_time      datetime
                ,scaling_segment_id         bigint
                ,scaling_weighting          float
                ,BARB_minute_start          datetime
                ,BARB_minute_end            datetime
            '')'
        execute (@sql)

        set @sql2 =
            'create index for_MBM            on vespa_analysts.Vespa_daily_augs_' || @day_counter || ' (scaling_segment_id, viewing_starts, viewing_stops)
            create index for_barb_MBM       on vespa_analysts.Vespa_daily_augs_' || @day_counter || ' (scaling_segment_id, BARB_minute_start, BARB_minute_end)
            create index subscriber_id      on vespa_analysts.Vespa_daily_augs_' || @day_counter || ' (subscriber_id)
            create index account_number     on vespa_analysts.Vespa_daily_augs_' || @day_counter || ' (account_number)'
        execute (@sql2)

        set @sql3 =
            'insert into vespa_analysts.Vespa_daily_augs_' || @day_counter || ' (cb_row_id
                ,account_number
                ,subscriber_id
                ,programme_trans_sk
                ,timeshifting
                ,viewing_starts
                ,viewing_stops
                ,viewing_duration
                ,capped_flag
                ,capped_event_end_time
                ,scaling_segment_id
                ,scaling_weighting
                ,BARB_minute_start
                ,BARB_minute_end
                )
            select * from Vespa_daily_augs_' || @day_counter || ''
        execute (@sql3)

        set @counter = @counter + 1
        set @day_counter = @day_counter + 1

    End

commit;

/* -- clear tables from my schema - this is done after transfer

declare @day_counter varchar(10000), @counter integer, @sql varchar(10000)

set @day_counter = '20140328'

set @counter = 1

while @counter <=2

    Begin
            set @sql =
            'drop table Vespa_Daily_Augs_' || @day_counter || ''

            execute (@sql)

        set @counter = @counter + 1
        set @day_counter = @day_counter + 1

    End

commit;

*/







