
exec STBCAP_import_netezza_slots '20141013 to 20141019 Adsmart Slots.csv'


select top 100 * from PI_process_import_data_3

--------------------------

IF object_id('STBCAP_import_netezza_slots') IS NOT NULL THEN DROP PROCEDURE STBCAP_import_netezza_slots END IF;

create procedure STBCAP_import_netezza_slots
        @import_file_name varchar(200)
as
begin



delete from STBCAP_Netezza_Adsmart_Slots

/*
CREATE TABLE STBCAP_Netezza_Adsmart_Slots (
row_id bigint
,DTH_VIEWING_EVENT_ID float
,SCMS_SUBSCRIBER_ID bigint
,EVENT_START_DATETIME timestamp
,EVENT_END_DATETIME timestamp
,EVENT_ACTION varchar(50)
)

create hg index ind_1 on STBCAP_Netezza_Adsmart_Slots(DTH_VIEWING_EVENT_ID)
create hg index ind_2 on STBCAP_Netezza_Adsmart_Slots(SCMS_SUBSCRIBER_ID)
create hg index ind_3 on STBCAP_Netezza_Adsmart_Slots(EVENT_START_DATETIME)
create hg index ind_4 on STBCAP_Netezza_Adsmart_Slots(EVENT_END_DATETIME)
create lf index ind_5 on STBCAP_Netezza_Adsmart_Slots(EVENT_ACTION)

*/


-- I have a table called PI_BARB_import set to hold each row as a long text string
-- First use WinSCP to load the data text file
-- This code then loads this text file into PI_BARB_import
delete from PI_BARB_import
Execute ( 'LOAD TABLE PI_BARB_import (imported_text  ''\n'' )  FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/JasonT/'
        || @import_file_name || ''' QUOTES OFF ESCAPES OFF NOTIFY 1000'
)



-- Need to format the text string for each row
-- Set up another table to hold the postion of each tab which is the deliminator
/*
drop table PI_process_import_data_3
Create table PI_process_import_data_3 (
        id_row bigint primary key identity
        ,imported_text varchar(10000)
        ,c1 int, c2 int, c3 int, c4 int, c5 int
)
*/

delete from PI_process_import_data_3
-- Now find the postion of each tab and populate the table
insert into PI_process_import_data_3(imported_text, c1)
select
        imported_text
        ,locate(imported_text, '|', 1)
from
        PI_BARB_import

update PI_process_import_data_3
        set c2 = locate(imported_text, '|', c1+1)
update PI_process_import_data_3
        set c3 = locate(imported_text, '|', c2+1)
update PI_process_import_data_3
        set c4 = locate(imported_text, '|', c3+1)
update PI_process_import_data_3
        set c5 = locate(imported_text, '|', c4+1)



-- Finally using the postion of the tabs we can break down the long text string
-- into columns and import into our final output table

insert into STBCAP_Netezza_Adsmart_Slots
select
        id_row
        ,cast(substring(imported_text, 1, c1-1) as float)
        ,cast(substring(imported_text, c1+1, c2-c1-1) as integer)
        ,cast(substring(imported_text, c2+1, c3-c2-1) as timestamp)
        ,cast(substring(imported_text, c3+1, c4-c3-1) as timestamp)
        ,substring(imported_text, c4+1, len(imported_text) - c4)
from
        PI_process_import_data_3
where id_row > 1


delete from PI_process_import_data_3


END
