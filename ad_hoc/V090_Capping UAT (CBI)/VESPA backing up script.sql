/*
As every time we run the capping SP control tables get deleted...

This scripts allows to save the main tables from which the Capping UAT analysis from vespa side bases for getting the figures out 
as per the QA checklist...

NOTE: the convention name for the tables is "bkp<date>_<tableName>" IE:  bkp10_Capping2_01_Viewing_Records

NOTE: further tables should have the full date and not just only the day number...
*/

if object_id('bkp10_Capping2_01_Viewing_Records') is not null
    drop table bkp10_Capping2_01_Viewing_Records;
commit;

select  *
into    bkp10_Capping2_01_Viewing_Records
from    Capping2_01_Viewing_Records;
commit;


if object_id('bkp10_CP2_event_listing') is not null
    drop table bkp10_CP2_event_listing;
commit;

select  *
into    bkp10_CP2_event_listing
from    CP2_event_listing;
commit;


if object_id('bkp10_CP2_capped_data_holding_pen') is not null
    drop table bkp10_CP2_capped_data_holding_pen;
commit;

select  *
into    bkp10_CP2_capped_data_holding_pen
from    CP2_capped_data_holding_pen;
commit;


if object_id('bkp10_CP2_capped_events_with_endpoints') is not null
    drop table bkp10_CP2_capped_events_with_endpoints;
commit;

select  *
into    bkp10_CP2_capped_events_with_endpoints
from    CP2_capped_events_with_endpoints;
commit;


if object_id('bkp10_CP2_ntiles_week') is not null
    drop table bkp10_CP2_ntiles_week;
commit;

select  *
into    bkp10_CP2_ntiles_week
from    CP2_ntiles_week;
commit;



if object_id('bkp10_CP2_h23_3') is not null
    drop table bkp10_CP2_h23_3;
commit;

select  *
into    bkp10_CP2_h23_3
from    CP2_h23_3;
commit;



if object_id('bkp10_CP2_h20_22') is not null
    drop table bkp10_CP2_h20_22;
commit;

select  *
into    bkp10_CP2_h20_22
from    CP2_h20_22;
commit;



if object_id('bkp10_CP2_h15_19') is not null
    drop table bkp10_CP2_h15_19;
commit;

select  *
into    bkp10_CP2_h15_19
from    CP2_h15_19;
commit;


if object_id('bkp10_CP2_h4_14') is not null
    drop table bkp10_CP2_h4_14;
commit;

select  *
into    bkp10_CP2_h4_14
from    CP2_h4_14;
commit;