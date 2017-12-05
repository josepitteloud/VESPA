

/* Hoousehold characteristics */
select top 1000 * from igonorp.BARB_PANEL_VIEWING_FILE_HOME_CHARACTERISTICS_2012_06_01_07

select date_valid_for,count(*)
from igonorp.BARB_PANEL_VIEWING_FILE_HOME_CHARACTERISTICS_2012_06_01_07
group by date_valid_for
order by date_valid_for
/*
20120601,5803
20120602,5816
20120603,5818
20120604,5818
20120605,5816
20120606,5792
20120607,5794
*/

/* Panel members */
select top 1000 * from igonorp.BARB_VIEWING_FILE_INDIVIDUAL_PANEL_MEMBER_DETAILS_2012_06_01_07

select date_valid_for_DB1,count(*)
from igonorp.BARB_VIEWING_FILE_INDIVIDUAL_PANEL_MEMBER_DETAILS_2012_06_01_07
group by date_valid_for_DB1
order by date_valid_for_DB1
/*
2012-06-01,13893
2012-06-02,13919
2012-06-03,13923
2012-06-04,13922
2012-06-05,13921
2012-06-06,13859
2012-06-07,13856
*/

/*  TV set characteristics */
select top 1000 * from igonorp.BARB_PANEL_VIEWING_FILE_TV_SET_CHARACTERISTICS_2012_06_01_07

select date_valid_for_DB1,count(*)
from igonorp.BARB_PANEL_VIEWING_FILE_TV_SET_CHARACTERISTICS_2012_06_01_07
group by date_valid_for_DB1
order by date_valid_for_DB1
/*
2012-06-01,9795
2012-06-02,9816
2012-06-03,9821
2012-06-04,9821
2012-06-05,9818
2012-06-06,9772
2012-06-07,9770
*/

/* Viewing events for poanel members */
select top 1000 * from igonorp.BARB_VIEWING_RECORD_PANEL_MEMBERS_2012_06_01_07

select date_of_activity_DB1,count(*)
from igonorp.BARB_VIEWING_RECORD_PANEL_MEMBERS_2012_06_01_07
group by date_of_activity_DB1
order by date_of_activity_DB1
/*
2012-05-28,7
2012-05-29,3
2012-05-30,6
2012-05-31,152
2012-06-01,105719
2012-06-02,115758
2012-06-03,125776
2012-06-04,116804
2012-06-05,134789
2012-06-06,111851
2012-06-07,119392
*/

select filename,count(*)
from igonorp.BARB_VIEWING_RECORD_PANEL_MEMBERS_2012_06_01_07
group by filename
order by filename
/*
B20120601.PVF,105673
B20120602.PVF,115789
B20120603.PVF,125659
B20120604.PVF,116801
B20120605.PVF,134795
B20120606.PVF,111789
B20120607.PVF,119751
*/

/* Viewing events from guests */
select count(*) from igonorp.BARB_VIEWING_RECORD_PANEL_GUESTS_2012_06_01_07 -- 
select top 1000 * from igonorp.BARB_VIEWING_RECORD_PANEL_GUESTS_2012_06_01_07

select date_of_activity_DB1,count(*)
from igonorp.BARB_VIEWING_RECORD_PANEL_GUESTS_2012_06_01_07
group by date_of_activity_DB1
order by date_of_activity_DB1
/*
2012-05-31,2
2012-06-01,5947
2012-06-02,8124
2012-06-03,10524
2012-06-04,7909
2012-06-05,8453
2012-06-06,6425
2012-06-07,6804
*/

/* Weights for panel members */
select top 1000 * from igonorp.BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07

select date_of_activity_DB1,count(*)
from igonorp.BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07
group by date_of_activity_DB1
order by date_of_activity_DB1
/*
2012-05-26,8
2012-05-27,16
2012-05-28,26
2012-05-29,30
2012-05-30,50
2012-05-31,89
2012-06-01,122537
2012-06-02,122424
2012-06-03,122330
2012-06-04,121740
2012-06-05,121776
2012-06-06,121167
2012-06-07,121640
*/


-- checks for MN

select datediff(day,date_of_recording_db1,date_of_activity_db1) as delta,count(*)
from igonorp.BARB_VIEWING_RECORD_PANEL_MEMBERS_2012_06_01_07
where date_of_recording_db1 is not null
group by datediff(day,date_of_recording_db1,date_of_activity_db1)
order by datediff(day,date_of_recording_db1,date_of_activity_db1)

select case 
        when date_of_recording_db1 is null then 'Live' 
        when datediff(day,date_of_recording_db1,date_of_activity_db1) = 0 then 'VOSDAL'
        when datediff(day,date_of_recording_db1,date_of_activity_db1) between 1 and 7 then '1-7 days'
        when datediff(day,date_of_recording_db1,date_of_activity_db1) > 7 then '>7 days'
        end as Tx_View_Difference
        ,count(*)
from igonorp.BARB_VIEWING_RECORD_PANEL_MEMBERS_2012_06_01_07
group by Tx_View_Difference
order by datediff(day,date_of_recording_db1,date_of_activity_db1)

/************************************************/
/*    Processing of viewing data for guests     */
/************************************************/

select count(*) from igonorp.New_BARB_Log_Station_Relationship_to_DB1_Station_Record -- 1,059
select top 1000 * from igonorp.New_BARB_Log_Station_Relationship_to_DB1_Station_Record

select count(*) from igonorp.BARB_VIEWING_RECORD_PANEL_GUESTS_VESPA_METADATA_2012_06_01_07_Dedups -- 54,188
select top 1000 * from igonorp.BARB_VIEWING_RECORD_PANEL_GUESTS_VESPA_METADATA_2012_06_01_07_Dedups

-- check events after midnight
select * from igonorp.BARB_VIEWING_RECORD_PANEL_GUESTS_VESPA_METADATA_2012_06_01_07_Dedups
where date_of_recording_db1 = '2012-05-31'
and start_time_of_recording >= 2400

select channel_name,count(*)
from igonorp.BARB_VIEWING_RECORD_PANEL_GUESTS_VESPA_METADATA_2012_06_01_07_Dedups
group by channel_name
order by 2 desc




