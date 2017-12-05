
create or replace procedure CM_Process_Landmark_Tables

as begin

    message '[' || now() || '] START Landmark preprocess' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] START Landmark preprocess'



           -- Get dates to loop through
           declare @varLastCMRunDate    date     
           declare @varCurrentCMRunDate date     = today()
           declare @qa int         


           select @varLastCMRunDate = max(AMEND_DATE) + 1 from vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES
           commit



/*###############################################################################
# LANDMARK
#
# 
#
###############################################################################*/

/*#******************************************
** Create Tables: move this to CM code 00 - structures when done
**
********************************************/

--- Landmark data (table extract from Netezza: SMI_ACCESS..V_MEDIA_SALES_AREA_DIM)
--- NOTE that this is already in the existing code to populate: CM_06_Landmark_Feed


--- Working copy of CHANNEL_MAP_PROD_SERVICE_KEY_LANDMARK
IF NOT EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'CM_30_CHANNEL_MAP_PROD_SERVICE_KEY_LANDMARK')
begin
    create table CM_30_CHANNEL_MAP_PROD_SERVICE_KEY_LANDMARK (
        service_key             int
        ,sare_no			    int
        ,effective_from         timestamp
        ,effective_to           timestamp
        ,amend_date             timestamp
        ,version                int
       )

create hg index i1 on CM_30_CHANNEL_MAP_PROD_SERVICE_KEY_LANDMARK(service_key)
create hg index i2 on CM_30_CHANNEL_MAP_PROD_SERVICE_KEY_LANDMARK(sare_no)
commit
end

--- Missing SAREs according to Landmark data 
IF NOT EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'CM_31_NEW_LANDMARK_SARE_NOS')
begin
    create table CM_31_NEW_LANDMARK_SARE_NOs (
    --    MEDIA_SALES_AREA_PK         	bitint
		MEDIA_SALES_AREA_NUMBER		int
	--	,CURRENCY_CODE					varchar(10)
	--	,DEFAULT_MINUTAGE				int
	--	,BASE_DEMO_NO					int
	--	,SECONDARY_BASE_DEMO_NO			int
	--	,RATING_SUPPLIER_CODE			varchar(10)
	--	,BASE_LENGTH					int
	--	,STATION_PRICE					varchar(10)
	--	,MEDIA_SALES_AREA_CODE			varchar(10)
		,MEDIA_SALES_AREA_NAME			varchar(50)
	--	,MEDIA_SALES_AREA_SHORT_NAME	varchar(50)
	--	,MEDIA_SALES_AREA_DESCRIPTION	varchar(50)
	--	,TX_START_DATE					bigint
	--	,TX_END_DATE					bigint
	--	,IA_START_DATE					bigint
	--	,IA_END_DATE					bigint
	--	,MEDIA_TARGET_SALES_AREA_CODE	bigint
	--	,MEDIA_TARGET_SALES_AREA_NAME	varchar(50)
	--	,MEDIA_TARGET_SALES_AREA_SHORT_NAME	varchar(50)
	--	,MEDIA_TARGET_SALES_AREA_SHORT_CODE	varchar(10)
		,EFFECTIVE_FROM					timestamp
		,EFFECTIVE_TO					timestamp
		,CURRENT_DIM					int
		,CREATE_DATE					timestamp
		,UPDATE_DATE					timestamp
	--	,LOAD_ID						varchar(50)
	)
		
create lf index i1 on CM_31_NEW_LANDMARK_SARE_NOs(MEDIA_SALES_AREA_NUMBER)
commit
end


--- SARE NOs no longer active according to Landmark data
IF NOT EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'CM_32_TERMINATE_LANDMARK_SARENO')
begin
    create table CM_32_TERMINATE_LANDMARK_SARENO (
        service_key             int
        ,sare_no			    int
        ,effective_from         timestamp
        ,effective_to           timestamp
        ,amend_date             timestamp
        ,version                int
		,landmark_effective_start       timestamp
        ,landmark_effective_end         timestamp
       )

create lf index i1 on CM_32_TERMINATE_LANDMARK_SARENO(sare_no)
commit
end



--- Manual mappings to use for CM Landmark
IF NOT EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'CM_33_NEW_LANDMARK_MANUAL_MAPPINGS')
begin
    create table CM_33_NEW_LANDMARK_MANUAL_MAPPINGS (
        service_key             int
        ,sare_no			    int
   --     ,effective_from         timestamp
   --     ,effective_to           timestamp
   --     ,amend_date             timestamp
   --     ,version                int
       )

create lf index i1 on CM_33_NEW_LANDMARK_MANUAL_MAPPINGS(sare_no)
commit
end



/*#******************************************
** Populate working Landmark table with current Landmark CM data
**
********************************************/

truncate table CM_30_CHANNEL_MAP_PROD_SERVICE_KEY_LANDMARK
commit

insert into CM_30_CHANNEL_MAP_PROD_SERVICE_KEY_LANDMARK
select * from vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_LANDMARK
commit


select @qa = count(1) from CM_30_CHANNEL_MAP_PROD_SERVICE_KEY_LANDMARK
commit

 message '[' || now() || '] CM_30_CHANNEL_MAP_PROD_SERVICE_KEY_LANDMARK (' || @qa || ' rows)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] CM_30_CHANNEL_MAP_PROD_SERVICE_KEY_LANDMARK (' || @qa || ' rows)'


/*#******************************************
** Load Landmark Extract [this is already in Main code and will need work out best way to schedule modules: note some changes made]
**
********************************************/
				  
declare @varSQL               text
commit				  

    truncate table CM_06_Landmark_Feed
	commit

    set @varSQL = '
                    load table CM_06_Landmark_Feed (
                        MEDIA_SALES_AREA_PK,
                        MEDIA_SALES_AREA_NUMBER,
                        CURRENCY_CODE,
                        DEFAULT_MINUTAGE,
                        BASE_DEMO_NO,
                        SECONDARY_BASE_DEMO_NO,
                        RATING_SUPPLIER_CODE,
                        BASE_LENGTH,
                        STATION_PRICE,
                        MEDIA_SALES_AREA_CODE,
                        MEDIA_SALES_AREA_NAME,
                        MEDIA_SALES_AREA_SHORT_NAME,
                        MEDIA_SALES_AREA_DESCRIPTION,
                        TX_START_DATE,
                        TX_END_DATE,
                        IA_START_DATE,
                        IA_END_DATE,
                        MEDIA_TARGET_SALES_AREA_CODE,
                        MEDIA_TARGET_SALES_AREA_NAME,
                        MEDIA_TARGET_SALES_AREA_SHORT_NAME,
                        MEDIA_TARGET_SALES_AREA_SHORT_CODE,
                        EFFECTIVE_FROM,
                        EFFECTIVE_TO,
                        CURRENT_DIM,
                        CREATE_DATE,
                        UPDATE_DATE,
                        LOAD_ID --,
                     --   Active,
                     --   Sky,
                     --   SK
                        ''\n''
                    )
                    FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Channel_Mapping_Process/_Process_inputs_/CM_feed_##^1^##_Landmark.csv''
                    SKIP 1
                    QUOTES ON
                    ESCAPES OFF
                    NOTIFY 1000
                    DELIMITED BY '',''
                  '
    execute( replace(@varSQL, '##^1^##', @varCurrentCMRunDate) )
    commit
				  
select @qa = count(1) from CM_06_Landmark_Feed
commit

 message '[' || now() || '] CM_06_Landmark_Feed (' || @qa || ' rows)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] CM_06_Landmark_Feed (' || @qa || ' rows)'		
				  
				  
/*#******************************************
** Load manual mappings
**
********************************************/

truncate table CM_33_NEW_LANDMARK_MANUAL_MAPPINGS
commit

    set @varSQL = '
                    load table CM_33_NEW_LANDMARK_MANUAL_MAPPINGS (
                        service_key 		         
						,sare_no					    
                        ''\n''
                    )
                    FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Channel_Mapping_Process/_Process_inputs_/CM_feed_##^1^##_MAN_Landmark.csv''
                    SKIP 1
                    QUOTES ON
                    ESCAPES OFF
                    DELIMITED BY '',''
                  '

    execute( replace(@varSQL, '##^1^##', @varCurrentCMRunDate) )
    commit


select @qa = count(1) from CM_33_NEW_LANDMARK_MANUAL_MAPPINGS
commit


 message '[' || now() || '] CM_33_NEW_LANDMARK_MANUAL_MAPPINGS (' || @qa || ' rows)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] CM_33_NEW_LANDMARK_MANUAL_MAPPINGS (' || @qa || ' rows)'


/*#******************************************
** Identify new and terminated Landmark channels since last update
**
********************************************/


--- New Landmark channels
truncate table CM_31_NEW_LANDMARK_SARE_NOs
commit

insert into CM_31_NEW_LANDMARK_SARE_NOs
select 
    land.MEDIA_SALES_AREA_NUMBER
    ,max(land.MEDIA_SALES_AREA_NAME)
	,max(land.EFFECTIVE_FROM)					
	,max(land.EFFECTIVE_TO)
	,max(land.CURRENT_DIM)
	,max(land.CREATE_DATE)
	,max(land.UPDATE_DATE)
from 
    CM_06_Landmark_Feed land
    left join CM_30_CHANNEL_MAP_PROD_SERVICE_KEY_LANDMARK cm
        on land.MEDIA_SALES_AREA_NUMBER = cm.sare_no
where
    cm.sare_no is null -- channels in Landmark but not CM
    and land.EFFECTIVE_FROM <= @varCurrentCMRunDate -- filters to new since last update...
	and land.EFFECTIVE_TO >= @varLastCMRunDate     -- ...so ignores anything missing in history
group by 
    land.MEDIA_SALES_AREA_NUMBER
commit


select @qa = count(1) from CM_31_NEW_LANDMARK_SARE_NOs
commit


 message '[' || now() || '] CM_31_NEW_LANDMARK_SARE_NOs (' || @qa || ' rows)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] CM_31_NEW_LANDMARK_SARE_NOs (' || @qa || ' rows)'


--- Terminate Landmark channels
truncate table CM_32_TERMINATE_LANDMARK_SARENO
commit


-- Only want to look as the most recent record of a sare number if there are more than one
-- Otherwise could end up terminating channels shouldn't
select
    media_sales_area_number
    ,max(effective_from) as max_start
into
    #most_recent_landmark
from
     CM_06_Landmark_Feed
group by 
    media_sales_area_number
commit

create unique hg index i1 on #most_recent_landmark(media_sales_area_number)
commit


insert into CM_32_TERMINATE_LANDMARK_SARENO
select
    cm.service_key             
    ,cm.sare_no			    
    ,cm.effective_from         
    ,cm.effective_to           
    ,cm.amend_date             
    ,cm.version  
	,land.EFFECTIVE_FROM
	,land.EFFECTIVE_TO   
from
        CM_30_CHANNEL_MAP_PROD_SERVICE_KEY_LANDMARK cm
        inner join CM_06_Landmark_Feed land                     
            on cm.sare_no = land.MEDIA_SALES_AREA_NUMBER
        inner join #most_recent_landmark r
            on land.MEDIA_SALES_AREA_NUMBER = r.media_sales_area_number            
where
    land.EFFECTIVE_TO between @varLastCMRunDate and @varCurrentCMRunDate -- only those that have terminated since last update
    and land.effective_from = r.max_start
commit                                                                                                                

select @qa = count(1) from CM_32_TERMINATE_LANDMARK_SARENO
commit


 message '[' || now() || '] CM_32_TERMINATE_LANDMARK_SARENO (' || @qa || ' rows)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] CM_32_TERMINATE_LANDMARK_SARENO (' || @qa || ' rows)'

    

/*#******************************************
** Update CM Landmark with new channels using manual mappings
**
********************************************/

insert into CM_30_CHANNEL_MAP_PROD_SERVICE_KEY_LANDMARK
select
	m.service_key             
	,n.MEDIA_SALES_AREA_NUMBER	
	,n.effective_from  --case when m.effective_from is not null then m.effective_from else n.effective_from end
	,n.effective_to    --case when m.effective_to is not null then m.effective_to else n.effective_to end
    ,@varCurrentCMRunDate           
	,0 --version                
from
   CM_31_NEW_LANDMARK_SARE_NOs n
   inner join CM_33_NEW_LANDMARK_MANUAL_MAPPINGS m
        on n.MEDIA_SALES_AREA_NUMBER = m.sare_no

commit

 select @qa = count(1) from  CM_31_NEW_LANDMARK_SARE_NOs n
                        inner join CM_33_NEW_LANDMARK_MANUAL_MAPPINGS m
                        on n.MEDIA_SALES_AREA_NUMBER = m.sare_no
commit



 message '[' || now() || '] Inserted new Landmark channels (' || @qa || ' rows)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] Inserted new Landmark channels (' || @qa || ' rows)'



/*#******************************************
** Terminate channels
**
********************************************/
                                                                       
update CM_30_CHANNEL_MAP_PROD_SERVICE_KEY_LANDMARK a
set
        effective_to          = case when max_land_effective_end < @varLastCMRunDate then @varLastCMRunDate else max_land_effective_end end
        ,amend_date            = @varCurrentCMRunDate
from
    (select sare_no, max(landmark_effective_end) as max_land_effective_end from CM_32_TERMINATE_LANDMARK_SARENO group by sare_no) b
where
    a.sare_no = b.sare_no

commit                                                                       
                                                                       
  				   
select @qa = count(1) from  CM_30_CHANNEL_MAP_PROD_SERVICE_KEY_LANDMARK a
                        inner join (select sare_no, max(landmark_effective_end) as max_land_effective_end from CM_32_TERMINATE_LANDMARK_SARENO group by sare_no) b
                        on a.sare_no = b.sare_no
commit



 message '[' || now() || '] Terminated Landmark channels (' || @qa || ' rows)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] Terminated Landmark channels (' || @qa || ' rows)'
                                                                       
                                                                       
/*#******************************************
** Update version and effective from/to times
**
********************************************/                     
                                                                       
declare @version_number int                                                                       
commit

select @version_number = max(VERSION) + 1 from vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_LANDMARK																   
commit

update 	CM_30_CHANNEL_MAP_PROD_SERVICE_KEY_LANDMARK a
set 
	version = @version_number
	,effective_from = dateadd(second, @version_number, cast( date(Effective_From) || ' 06:00:00' as datetime))
	,effective_to = case
                        when date(Effective_To) = '2999-12-31' then cast('2999-12-31 00:00:00' as datetime)
                        else dateadd(second, @version_number - 1, cast( date(Effective_To) || ' 06:00:00' as datetime))
                    end
commit			


															   
																	   
end -- CM_Process_Landmark_Tables
commit                                                                       

        