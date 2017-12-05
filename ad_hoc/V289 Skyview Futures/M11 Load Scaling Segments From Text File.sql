
--------------------------------------------------------------------------------------------------------
----- This loads the Scaling Segments from a text file
--------------------------------------------------------------------------------------------------------

-- This populates the table SC3I_Segments_Lookup_v1_1 with the scaling segments
-- Once the table is populated it doesn't need to be populated again unless we change the scaling segments in some way


delete from PI_BARB_import
commit

Execute ( 'LOAD TABLE PI_BARB_import (imported_text  ''\n'' )  FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/JasonT/'
        || 'Skyview Scaling Segments.txt' || ''' QUOTES OFF ESCAPES OFF NOTIFY 1000'
)
commit


drop table PI_process_import_data
commit
-- Need to format the text string for each row
-- Set up another table to hold the postion of each tab which is the deliminator
if object_id('PI_process_import_data') is not null drop table PI_process_import_data

Create table PI_process_import_data (
        id_row bigint primary key identity
        ,imported_text varchar(10000)
        ,c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int)
commit



-- Now find the postion of each tab and populate the table
insert into PI_process_import_data(imported_text, c1)
select
        imported_text
        ,locate(imported_text, char(9), 1)
from
        PI_BARB_import

update PI_process_import_data
        set c2 = locate(imported_text, char(9), c1+1)
update PI_process_import_data
        set c3 = locate(imported_text, char(9), c2+1)
update PI_process_import_data
        set c4 = locate(imported_text, char(9), c3+1)
update PI_process_import_data
        set c5 = locate(imported_text, char(9), c4+1)
update PI_process_import_data
        set c6 = locate(imported_text, char(9), c5+1)
update PI_process_import_data
        set c7 = locate(imported_text, char(9), c6+1)
update PI_process_import_data
        set c8 = locate(imported_text, char(9), c7+1)
commit

-- Finally using the postion of the tabs we can break down the long text string
-- into columns and import into our final output table
delete from SC3I_Segments_Lookup_v1_1
commit

insert into SC3I_Segments_Lookup_v1_1
select
        cast(substring(imported_text, 1, c1-1) as int)
        ,substring(imported_text, c1+1, c2-c1-1)
        ,substring(imported_text, c2+1, c3-c2-1)
        ,substring(imported_text, c3+1, c4-c3-1)
        ,substring(imported_text, c4+1, c5-c4-1)
        ,substring(imported_text, c5+1, c6-c5-1)
        ,substring(imported_text, c6+1, c7-c6-1)
        ,substring(imported_text, c7+1, c8-c7-1)
from
        PI_process_import_data
commit
