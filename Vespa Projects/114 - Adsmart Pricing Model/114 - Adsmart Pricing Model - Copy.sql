


---Part A - Create a List of relevant Spots---
if object_id('BARB_SPOT_DATA_PROJECT_114') is not null drop table BARB_SPOT_DATA_PROJECT_114;
select * into BARB_SPOT_DATA_PROJECT_114  from Neighbom.LANDMARK_MASTER_SPOT_DATA
where spot_type = 'CS'
;
--select * from BARB_SPOT_DATA_PROJECT_114;
commit;

---Part B - Add on Channel Name Details

alter table BARB_SPOT_DATA_PROJECT_114 add full_name varchar(255);
alter table BARB_SPOT_DATA_PROJECT_114 add vespa_name varchar(255);
alter table BARB_SPOT_DATA_PROJECT_114 add channel_name varchar(255);
alter table BARB_SPOT_DATA_PROJECT_114 add techedge_name varchar(255);
alter table BARB_SPOT_DATA_PROJECT_114 add infosys_name varchar(255);


update BARB_SPOT_DATA_PROJECT_114 
set a.full_name=b.full_name
,a.vespa_name=b.vespa_name
,a.channel_name=b.channel_name
,a.techedge_name=b.techedge_name
,a.infosys_name=b.infosys_name
from BARB_SPOT_DATA_PROJECT_114 as a
left outer join VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES as b
on a.service_key=b.service_key
where a.local_date_of_transmission between b.effective_from and b.effective_to
;
commit; 

---Remeove trailing spaces from Full_Name field to crete a field to match to lookup to be used to match to EPG data
alter table BARB_SPOT_DATA_PROJECT_114 add spot_channel_name varchar(255);
update BARB_SPOT_DATA_PROJECT_114 
set spot_channel_name = trim(full_name)
from BARB_SPOT_DATA_PROJECT_114 
;
commit;
create  hg index idx1 on BARB_SPOT_DATA_PROJECT_114(service_key);
create  hg index idx2 on BARB_SPOT_DATA_PROJECT_114(utc_spot_start_date_time);
create  hg index idx3 on BARB_SPOT_DATA_PROJECT_114(utc_spot_end_date_time);

----Link Spot Data to Viewing Data

--------------------------------------------------------------------------------
-- PART C SETUP - Extract Viewing data
--------------------------------------------------------------------------------

/*
PART C   - Extract Viewing data
     C01 - Viewing table for period
     C03 - Clean data
     
*/

CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(3000);
CREATE VARIABLE @scanning_day           datetime;
CREATE VARIABLE @var_num_days           smallint;


-- Date range of programmes to capture
SET @var_prog_period_start  = '2012-08-14';
SET @var_prog_period_end    = '2012-08-28';
-- How many days (after end of broadcast period) to check for timeshifted viewing

--select @var_num_days;
if object_id('VESPA_Programmes_project_114') is not null drop table VESPA_Programmes_project_114;
select
      programme_trans_sk
      ,Channel_Name
      ,Epg_Title
      ,synopsis
      ,Genre_Description
      ,Sub_Genre_Description
      ,Tx_Start_Datetime_UTC
      ,Tx_End_Datetime_UTC
      ,tx_date_utc
       ,service_key
      ,datediff(mi,Tx_Start_Datetime_UTC,Tx_End_Datetime_UTC) as programme_duration
  into VESPA_Programmes_project_114 -- drop table  VESPA_Programmes
  from sk_prod.VESPA_EPG_DIM
 where tx_date_time_utc <= dateadd(day, 1, @var_prog_period_end) -- because @var_prog_period_end is a date and defaults to 00:00:00 when compared to datetimes
-- Add further filters to programmes here if required, eg, lower(channel_name) like '%bbc%'
   ;
--select top 500 * from VESPA_Programmes_project_114 where upper(channel_name) like '%ATLANTIC%';
commit;
create unique hg index idx1 on VESPA_Programmes_project_114(programme_trans_sk);
create  hg index idx2 on VESPA_Programmes_project_114(tx_date_utc);
create  hg index idx3 on VESPA_Programmes_project_114(service_key);
commit;
------ C01 - Viewing table for period
-- C01 - Viewing table for period
commit;

if object_id('Project_114_viewing_table_dump') is not null drop table Project_114_viewing_table_dump;

select vw.*
    ,spot.utc_spot_start_date_time
    ,spot.utc_spot_end_date_time
    ,spot.utc_break_start_date_time
    ,spot.utc_break_end_date_time
,spot.full_name
,spot.vespa_name
,spot.techedge_name 
,spot.infosys_name 
,spot.service_key
,spot.spot_position_in_break
,spot.no_spots_in_break 
,spot.spot_duration
,cast(prog.Tx_Start_Datetime_UTC as date) as tx_date
,prog.Tx_Start_Datetime_UTC
,prog.Tx_End_Datetime_UTC    
,prog.programme_duration
,prog.Channel_Name
,prog.Epg_Title
,prog.Genre_Description
,prog.Sub_Genre_Description,vw.weightings
into Project_114_viewing_table_dump
from BARB_SPOT_DATA_PROJECT_114 as spot
    inner join VESPA_Programmes_project_114 as prog
        on spot.service_key=prog.service_key
    inner join Rombaoad.V98_Tot_mins_cap_raw as vw
        on  prog.programme_trans_sk=vw.pk_viewing_prog_instance_fact
where  
    (           viewing_starts between utc_spot_start_date_time and utc_spot_end_date_time 
        or      viewing_stops between utc_spot_start_date_time and utc_spot_end_date_time 
        or      viewing_starts < utc_spot_start_date_time and viewing_stops> utc_spot_end_date_time 
    )
;



-- Filter for viewing events is applied on the daily augs table already.
-- Loop over the days in the period, extracting all the data.
commit;

SET @scanning_day = @var_prog_period_start;
--delete from Project_114_viewing_table_dump;
commit;
while @scanning_day <= dateadd(dd,0,@var_prog_period_end)
begin
    EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(@scanning_day, 'yyyymmdd')))
    commit

    set @scanning_day = dateadd(day, 1, @scanning_day)
end;
commit;
















/*







select top 100 * from Neighbom.LANDMARK_MASTER_SPOT_DATA;

select local_date_of_transmission , count(*) as records  from Neighbom.LANDMARK_MASTER_SPOT_DATA 
group by local_date_of_transmission 
order by local_date_of_transmission;

commit;

select count(*) 
from Neighbom.LANDMARK_MASTER_SPOT_DATA
where spot_type = 'CS'


commit;
select top 500 * from Rombaoad.V98_Tot_mins_cap_raw;

select top 100 *  from sk_prod.VESPA_EPG_DIM;



into project_

select top 100 * from  mustaphs.AdSmart_20121016;


vespa_analysts.SC2_weightings




SELECT base.account_number
--      ,x_pvr_type
--      ,x_manufacturer
      ,CASE  WHEN x_pvr_type ='PVR6'                                THEN 1
             WHEN x_pvr_type ='PVR5'                                THEN 1
             WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Samsung' THEN 1
             WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Pace'    THEN 1
--             WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Thomson' THEN 1
                                                                    ELSE 0
       END AS Adsmartable
      ,SUM(Adsmartable) AS T_AdSm_box
INTO #SetTop
FROM   sk_prod.CUST_SET_TOP_BOX  AS SetTop
        inner join AdSmart as Base
         on SetTop.account_number = Base.account_number
         where box_replaced_dt = '9999-09-09'
         GROUP BY base.account_number
                ,x_pvr_type
                ,x_manufacturer
                ,box_replaced_dt;

select top 100 * from sk_prod.CUST_SET_TOP_BOX;

commit;

SELECT account_number
,service_instance_id
,x_pvr_type 
,x_manufacturer
,max(dw_created_dt) as max_dw_created_dt
  INTO #boxes -- drop table #boxes
  FROM sk_prod.CUST_SET_TOP_BOX  
where box_replaced_dt = '9999-09-09'

 --(box_installed_dt <= cast('2012-01-15'  as date) 
 --  AND box_replaced_dt   > cast('2012-01-15'  as date)) or box_installed_dt is null
group by account_number
,service_instance_id
,x_pvr_type 
,x_manufacturer
 ;
select count(distinct account_number) from sk_prod.CUST_SET_TOP_BOX  

select * from sk_prod.vespa_epg_dim where programme_trans_sk=10405396439
*/




