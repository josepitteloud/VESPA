
exec STBCAP_import_netezza_thresholds '20141006 to 20141012 Capping Thresholds.csv'

---------------------------------------------
--- Load Historical Capping threhold data from Netezza
-- This uses WinSCP
-- Author: Jason Thompson
-- Date: 17/4/2014
---------------------------------------------

IF object_id('STBCAP_import_netezza_thresholds') IS NOT NULL THEN DROP PROCEDURE STBCAP_import_netezza_thresholds END IF;

create procedure STBCAP_import_netezza_thresholds
        @import_file_name varchar(200)
as
begin


-- Create table to hold the data
-- I have also added a unique row_id

/* Netezza query:
select
        CAPPED_THRESHOLD_DATE
        ,extract(dow from CAPPED_THRESHOLD_DATE) as day_of_week
        ,CAPPED_THRESHOLD_HOUR
        ,CAPPED_THRESHOLD_CHANNEL_PACK
        ,CAPPED_THRESHOLD_BOX
        ,CAPPED_THRESHOLD_GENRE
        ,CAPPED_THRESHOLD_EVENT_DURATION
from
        DIS_PREPARE..CAPPED_THRESHOLD_DIM_PREPARE
where
        CAPPED_THRESHOLD_TYPE = 1 -- Live events
        and (CAPPED_THRESHOLD_DATE >= date('2014-01-27') and CAPPED_THRESHOLD_DATE <= date('2014-04-13'))
*/


/*
CREATE TABLE STBCAP_Netezza_thresholds_live (
row_id bigint
,the_date date
,day_of_week integer
,CAPPED_THRESHOLD_HOUR integer
,CAPPED_THRESHOLD_CHANNEL_PACK varchar(50)
,CAPPED_THRESHOLD_BOX varchar(50)
,CAPPED_THRESHOLD_GENRE varchar(50)
,CAPPED_THRESHOLD_EVENT_DURATION integer
)

create lf index ind_day on STBCAP_Netezza_thresholds_live(day_of_week)
create lf index ind_hour on STBCAP_Netezza_thresholds_live(CAPPED_THRESHOLD_HOUR)
create lf index ind_pack on STBCAP_Netezza_thresholds_live(CAPPED_THRESHOLD_CHANNEL_PACK)
create lf index ind_box on STBCAP_Netezza_thresholds_live(CAPPED_THRESHOLD_BOX)
create lf index ind_genre on STBCAP_Netezza_thresholds_live(CAPPED_THRESHOLD_GENRE)
create hg index ind_duration on STBCAP_Netezza_thresholds_live(CAPPED_THRESHOLD_EVENT_DURATION)
*/

-- I have a table called PI_BARB_import set to hold each row as a long text string
-- First use WinSCP to load the data text file
-- This code then loads this text file into PI_BARB_import
delete from PI_BARB_import
Execute ( 'LOAD TABLE PI_BARB_import (imported_text  ''\n'' )  FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/JasonT/'
        || @import_file_name || ''' QUOTES OFF ESCAPES OFF NOTIFY 1000' --************************************************* CHANGE ****************************
)



-- Need to format the text string for each row
-- Set up another table to hold the postion of each tab which is the deliminator
if object_id('PI_process_import_data') IS NOT NULL THEN DROP TABLE PI_process_import_data END IF;
commit

Create table PI_process_import_data (
        id_row bigint primary key identity
        ,imported_text varchar(10000)
        ,c1 int, c2 int, c3 int, c4 int, c5 int, c6 int
)


delete from PI_process_import_data
-- Now find the postion of each tab and populate the table
insert into PI_process_import_data(imported_text, c1)
select
        imported_text
        ,locate(imported_text, '|', 1)
from
        PI_BARB_import

update PI_process_import_data
        set c2 = locate(imported_text, '|', c1+1)
update PI_process_import_data
        set c3 = locate(imported_text, '|', c2+1)
update PI_process_import_data
        set c4 = locate(imported_text, '|', c3+1)
update PI_process_import_data
        set c5 = locate(imported_text, '|', c4+1)
update PI_process_import_data
        set c6 = locate(imported_text, '|', c5+1)
commit


-- Finally using the postion of the tabs we can break down the long text string
-- into columns and import into our final output table

insert into STBCAP_Netezza_thresholds_live
select
        id_row
        ,date(substring(imported_text, 1, c1-1)) -- date
        ,cast(substring(imported_text, c1+1, c2-c1-1) as integer) -- day of week
        ,cast(substring(imported_text, c2+1, c3-c2-1) as integer) -- hour
        ,substring(imported_text, c3+1, c4-c3-1) -- pack
        ,substring(imported_text, c4+1, c5-c4-1) -- box
        ,substring(imported_text, c5+1, c6-c5-1) -- genre
        ,cast(substring(imported_text, c6+1, len(imported_text) - c6) as integer) -- duration
from
        PI_process_import_data
where id_row <> 1
commit

END
