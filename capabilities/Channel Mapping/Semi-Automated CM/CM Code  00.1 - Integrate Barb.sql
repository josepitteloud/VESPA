
create or replace procedure CM_Process_Barb_Tables

as begin

    message '[' || now() || '] START Barb preprocess' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] START Barb preprocess'



           -- Get dates to loop through
           declare @varLastCMRunDate    date     
           declare @varCurrentCMRunDate date     = today()
           declare @qa int         


           select @varLastCMRunDate = max(AMEND_DATE) + 1 from vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES
           commit



/*###############################################################################
# Barb
#
# 
#
###############################################################################*/

/*#******************************************
** Create Tables: move this to CM code 00 - structures when done
**
********************************************/

--- Working copy of CHANNEL_MAP_PROD_SERVICE_KEY_BARB
IF NOT EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'CM_25_CHANNEL_MAP_PROD_SERVICE_KEY_BARB')
begin
    create table CM_25_CHANNEL_MAP_PROD_SERVICE_KEY_BARB (
        service_key             int
        ,log_station_code       int
        ,sti_code               int
        ,panel_code             int
        ,promo_panel_code       int
        ,effective_from         timestamp
        ,effective_to           timestamp
        ,amend_date             timestamp
        ,version                int
        ,dummy_barb_code        int
       )

create hg index i1 on CM_25_CHANNEL_MAP_PROD_SERVICE_KEY_BARB(service_key)
create hg index i2 on CM_25_CHANNEL_MAP_PROD_SERVICE_KEY_BARB(log_station_code)
commit
end

--- Missing Log stations codes according to Barb data
IF NOT EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'CM_26_NEW_BARB_LOG_STATION_CODES')
begin
    create table CM_26_NEW_BARB_LOG_STATION_CODES (
        log_station_code            int
        ,log_station_name           varchar(60)
        ,primary_rep_panel_code     int
        ,reporting_start_date       date
        ,reporting_end_date         date
        ,sales_house_1              int
        ,sales_house_2              int
        ,sales_house_3              int
        ,sales_house_4              int
        ,sales_house_5              int
        ,sales_house_6              int
       )

create lf index i1 on CM_26_NEW_BARB_LOG_STATION_CODES(log_station_code)
commit
end


--- Log stations codes now longer active according to Barb data
IF NOT EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'CM_27_TERMINATE_BARB_LOG_STATION_CODES')
begin
    create table CM_27_TERMINATE_BARB_LOG_STATION_CODES (
        service_key             int
        ,log_station_code       int
        ,sti_code               int
        ,panel_code             int
        ,promo_panel_code       int
        ,effective_from         timestamp
        ,effective_to           timestamp
        ,amend_date             timestamp
        ,version                int
        ,dummy_barb_code        int
        ,barb_effective_start       date
        ,barb_effective_end         date
       )

create lf index i1 on CM_27_TERMINATE_BARB_LOG_STATION_CODES(log_station_code)
commit
end



--- Manual mappings to use for CM Barb
IF NOT EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'CM_28_NEW_BARB_MANUAL_MAPPINGS')
begin
    create table CM_28_NEW_BARB_MANUAL_MAPPINGS (
        service_key             int
        ,log_station_code       int
        ,sti_code               int
        ,panel_code             int
        ,promo_panel_code       int
        ,effective_from         timestamp
        ,effective_to           timestamp
        ,amend_date             timestamp
        ,version                int
        ,dummy_barb_code        int
       )

create lf index i1 on CM_28_NEW_BARB_MANUAL_MAPPINGS(log_station_code)
commit
end



/*#******************************************
** Populate working Barb table with current Barb CM data
**
********************************************/

truncate table CM_25_CHANNEL_MAP_PROD_SERVICE_KEY_BARB
commit

insert into CM_25_CHANNEL_MAP_PROD_SERVICE_KEY_BARB
select * from vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_BARB
commit


select @qa = count(1) from CM_25_CHANNEL_MAP_PROD_SERVICE_KEY_BARB
commit

 message '[' || now() || '] CM_25_CHANNEL_MAP_PROD_SERVICE_KEY_BARB (' || @qa || ' rows)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] CM_25_CHANNEL_MAP_PROD_SERVICE_KEY_BARB (' || @qa || ' rows)'



/*#******************************************
** Load manual mappings
**
********************************************/

declare @varSQL               text
commit

truncate table CM_28_NEW_BARB_MANUAL_MAPPINGS
commit

    set @varSQL = '
                    load table CM_28_NEW_BARB_MANUAL_MAPPINGS (
                        service_key,
                        log_station_code,
                        sti_code,
                        panel_code,
                        promo_panel_code,
                        effective_from,
                        effective_to,
                        dummy_barb_code           
                        ''\n''
                    )
                    FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Channel_Mapping_Process/_Process_inputs_/CM_feed_##^1^##_MAN_BARB.csv''
                    SKIP 1
                    QUOTES ON
                    ESCAPES OFF
                    DELIMITED BY '',''
                    ON FILE ERROR CONTINUE 
                  '
    execute( replace(@varSQL, '##^1^##', @varCurrentCMRunDate) )
    commit


select @qa = count(1) from CM_28_NEW_BARB_MANUAL_MAPPINGS
commit


 message '[' || now() || '] CM_28_NEW_BARB_MANUAL_MAPPINGS (' || @qa || ' rows)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] CM_28_NEW_BARB_MANUAL_MAPPINGS (' || @qa || ' rows)'


/*#******************************************
** Identify new and terminated Barb channels since last update
**
********************************************/

declare @max_date date
select @max_date = max(file_creation_date) from BARB_LOG_STATIONS_REP
commit

--- New Barb Channels
truncate table CM_26_NEW_BARB_LOG_STATION_CODES
commit

insert into CM_26_NEW_BARB_LOG_STATION_CODES
select 
    barb.log_station_code
    ,max(barb.log_station_name)
    ,max(barb.primary_rep_panel_code)
    ,max(barb.reporting_start_date)
    ,max(barb.reporting_end_date)
    ,max(barb.sales_house_1)
    ,max(barb.sales_house_2)
    ,max(barb.sales_house_3)
    ,max(barb.sales_house_4)
    ,max(barb.sales_house_5)
    ,max(barb.sales_house_6)   
from 
    BARB_LOG_STATIONS_REP barb
    left join CM_25_CHANNEL_MAP_PROD_SERVICE_KEY_BARB cm
        on barb.log_station_code = cm.log_station_code
where
    barb.file_creation_date = @max_date -- look at latest version of the Barb data only
    and cm.log_station_code is null -- channels in Barb but not CM
    and barb.reporting_start_date <= @varCurrentCMRunDate -- filters to new since last update
    and (case when barb.reporting_end_date is null then date('2999-01-01') else barb.reporting_end_date end)    
                                                    >= @varLastCMRunDate      -- so ignores anything missing in history
group by 
    barb.log_station_code
commit


select @qa = count(1) from CM_26_NEW_BARB_LOG_STATION_CODES
commit


 message '[' || now() || '] CM_26_NEW_BARB_LOG_STATION_CODES (' || @qa || ' rows)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] CM_26_NEW_BARB_LOG_STATION_CODES (' || @qa || ' rows)'



--- Terminated Barb Channels
truncate table CM_27_TERMINATE_BARB_LOG_STATION_CODES
commit


-- Only want to look as the most recent record of a log station code if there are more than one
-- Otherwise could end up terminating channels shouldn't
select
    log_station_code
    ,max(reporting_start_date) as max_start
into
    #most_recent_log_station
from
     BARB_LOG_STATIONS_REP
where
     file_creation_date = @max_date -- look at latest version of the Barb data only
group by 
    log_station_code
commit

create unique hg index i1 on  #most_recent_log_station(log_station_code)
commit


insert into CM_27_TERMINATE_BARB_LOG_STATION_CODES
select
        cm.service_key             
        ,cm.log_station_code       
        ,cm.sti_code               
        ,cm.panel_code             
        ,cm.promo_panel_code       
        ,cm.effective_from         
        ,cm.effective_to           
        ,cm.amend_date             
        ,cm.version                
        ,cm.dummy_barb_code
        ,barb.reporting_start_date
        ,(case when barb.reporting_end_date is null then date('2999-01-01') else barb.reporting_end_date end)        
from
        CM_25_CHANNEL_MAP_PROD_SERVICE_KEY_BARB cm
        inner join BARB_LOG_STATIONS_REP barb                     
            on cm.log_station_code = barb.log_station_code
        inner join #most_recent_log_station r
            on barb.log_station_code = r.log_station_code
where
    barb.file_creation_date = @max_date -- look at latest version of the Barb data only
    and barb.reporting_start_date = r.max_start
    and (case when barb.reporting_end_date is null then date('2999-01-01') else barb.reporting_end_date end)  between @varLastCMRunDate and @varCurrentCMRunDate
commit                                                                                                                -- only those that have terminated since last update



select @qa = count(1) from CM_27_TERMINATE_BARB_LOG_STATION_CODES
commit


 message '[' || now() || '] CM_27_TERMINATE_BARB_LOG_STATION_CODES (' || @qa || ' rows)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] CM_27_TERMINATE_BARB_LOG_STATION_CODES (' || @qa || ' rows)'

    

/*#******************************************
** Update CM Barb with new channels using manual mappings
**
********************************************/

insert into CM_25_CHANNEL_MAP_PROD_SERVICE_KEY_BARB
select
    m.service_key
    ,n.log_station_code
    ,m.sti_code
    ,case when m.panel_code is not null then m.panel_code else n.primary_rep_panel_code end
    ,case when m.promo_panel_code is not null then m.promo_panel_code else n.primary_rep_panel_code end
    ,case when m.effective_from is not null then m.effective_from else n.reporting_start_date end
    ,case when m.effective_to is not null then m.effective_to else 
            case when n.reporting_end_date is null then date('2999-12-31') else n.reporting_end_date end end
    ,@varCurrentCMRunDate
    ,0 -- version
    ,m.dummy_barb_code    
from
   CM_26_NEW_BARB_LOG_STATION_CODES n
   inner join CM_28_NEW_BARB_MANUAL_MAPPINGS m
        on n.log_station_code = m.log_station_code

commit


select @qa = count(1) from  CM_26_NEW_BARB_LOG_STATION_CODES n
                        inner join CM_28_NEW_BARB_MANUAL_MAPPINGS m
                        on n.log_station_code = m.log_station_code
commit



 message '[' || now() || '] Inserted new Barb channels (' || @qa || ' rows)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] Inserted new Barb channels (' || @qa || ' rows)'



/*#******************************************
** Terminate channels
**
********************************************/
                                                                       
update CM_25_CHANNEL_MAP_PROD_SERVICE_KEY_BARB a
set
        effective_to          = case when max_barb_effective_end < @varLastCMRunDate then @varLastCMRunDate else max_barb_effective_end end
        ,amend_date            = @varCurrentCMRunDate
from
    (select log_station_code, max(barb_effective_end) as max_barb_effective_end from CM_27_TERMINATE_BARB_LOG_STATION_CODES group by log_station_code) b
where
    a.log_station_code = b.log_station_code

commit                                                                       
                                                                       
                                                                       

select @qa = count(1) from  CM_25_CHANNEL_MAP_PROD_SERVICE_KEY_BARB a
                        inner join (select log_station_code, max(barb_effective_end) as max_barb_effective_end from CM_27_TERMINATE_BARB_LOG_STATION_CODES group by log_station_code) b
                        on a.log_station_code = b.log_station_code
commit



 message '[' || now() || '] Terminated Barb channels (' || @qa || ' rows)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] Terminated Barb channels (' || @qa || ' rows)'
                                                                       
                                                                       
/*#******************************************
** Update version and effective from/to times
**
********************************************/                     
                                                                       
declare @version_number int                                                                       
commit

select @version_number = max(VERSION) + 1 from vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_BARB																	   
commit

update 	CM_25_CHANNEL_MAP_PROD_SERVICE_KEY_BARB a
set 
	version = @version_number
	,effective_from = dateadd(second, @version_number, cast( date(Effective_From) || ' 06:00:00' as datetime))
	,effective_to = case
                        when date(Effective_To) = '2999-12-31' then cast('2999-12-31 00:00:00' as datetime)
                        else dateadd(second, @version_number - 1, cast( date(Effective_To) || ' 06:00:00' as datetime))
                    end
commit			








																	   
																	   
end -- CM_Process_Barb_And_Landmark_Tables
commit                                                                       

        