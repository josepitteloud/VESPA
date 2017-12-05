
-----------------------------------------------------------------------------------START

                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES


        VESPA V_98 SkyBet - A Profiling Exercise
        --------------------------------
        Author  : Don Rombaoa
        Date    : 22 October 2012

		
SECTIONS
----------------
             
code_location
        --      code_location_01        Viewing Table
        --      code_location_02        Capped Tables
        --      code_location_03        Capped Tables Fix
        --      code_location_04        Segments Table
        --      code_location_05        Segment Viewing
		--		code_location_06        Total Account Viewing
		--		code_location_07 		Master Tables
		--		code_location_08 		Scaling Tables
		--		code_location_09		Share of Viewing Tables 
		--		code_location_10		Deciles
		--		code_location_11		SkyBet Final Output
		--		code_location_12		Grants 		
		--		code_location_13		Added Statistics for SkyBET Project
		--		code_location_14 		Pen Portraits 
		*/

----------------
--Code 01 - Viewing Table
-----------------
SELECT account_number, pk_viewing_prog_instance_fact,  genre_description, sub_genre_description, channel_name, subscriber_id, broadcast_start_date_time_utc, broadcast_end_date_time_utc, instance_start_date_time_utc, instance_end_date_time_utc, bss_name, dk_programme_instance_dim, duration, event_start_date_time_utc,  event_end_date_time_utc, programme_name, programme_instance_name, programme_instance_duration, product_code, pay_free_indicator, reported_playback_speed, broadcast_time_of_day, panel_id, cb_key_individual, cb_key_family, cb_key_household 
INTO V98_Viewing_Table_SkyBetDates_Final
FROM sk_prod.VESPA_EVENTS_VIEWED_ALL 
WHERE subscriber_id is NOT NULL
AND broadcast_start_date_time_utc between '2012-08-13 00:00:00.000' and '2012-08-27 05:59:00.000'
AND duration > 6  
AND panel_id=12
AND account_number is NOT NULL
AND reported_playback_speed is NULL
COMMIT;
--325390657 row(s) affected
Execution time: 9014.921 seconds

--Check to find count for number of account numbers
--select COUNT (distinct (account_number))
--FROM V98_Viewing_Table_SkyBetDates_Final
--615,478

--Check if primary key - 'pk_viewing_prog_instance_fact' - is unique
--select COUNT (*), COUNT (distinct (pk_viewing_prog_instance_fact))
--from V98_Viewing_Table_SkyBetDates_Final
--COUNT(),COUNT(distinct(V98_Viewing_Table_SkyBetDates_Final.pk_viewing_prog_instance_fact))
--325390657,325390657

--Create Indexes
CREATE   INDEX  SBDf_idx_evtSdt_utc ON V98_Viewing_Table_SkyBetDates_Final  (event_start_date_time_utc);
CREATE   INDEX  SBDf_idx_evtEdt_utc ON V98_Viewing_Table_SkyBetDates_Final  (event_end_date_time_utc);
CREATE   INDEX  SBDf_idx_genre ON V98_Viewing_Table_SkyBetDates_Final  (genre_description);
CREATE   INDEX  SBDf_idx_sub_genre ON V98_Viewing_Table_SkyBetDates_Final  (sub_genre_description);
CREATE   INDEX  SBDf_idx_chnl ON V98_Viewing_Table_SkyBetDates_Final  (channel_name);
CREATE   INDEX  SBDf_idx_subID ON V98_Viewing_Table_SkyBetDates_Final  (subscriber_id);
CREATE   INDEX  SBDf_idx_bcsSdt_utc ON V98_Viewing_Table_SkyBetDates_Final  (broadcast_start_date_time_utc);
CREATE   INDEX  SBDf_idx_bcstEdt_utc ON V98_Viewing_Table_SkyBetDates_Final  (broadcast_end_date_time_utc);
CREATE   INDEX  SBDf_idx_instSdt_utc ON V98_Viewing_Table_SkyBetDates_Final  (instance_start_date_time_utc);
CREATE   INDEX  SBDf_idx_instEdt_utc ON V98_Viewing_Table_SkyBetDates_Final  (instance_end_date_time_utc);
CREATE   INDEX  SBDf_idx_dk ON V98_Viewing_Table_SkyBetDates_Final  (dk_programme_instance_dim);
CREATE   INDEX  SBDf_idx_duration ON V98_Viewing_Table_SkyBetDates_Final  (duration);
CREATE   INDEX  SBDf_idx_proginstduration ON V98_Viewing_Table_SkyBetDates_Final  (programme_instance_duration);
CREATE   INDEX  SBDf_idx_cbkyhh ON V98_Viewing_Table_SkyBetDates_Final  (cb_key_household);
CREATE   INDEX  SBDf_idx_panel ON V98_Viewing_Table_SkyBetDates_Final  (panel_id);
CREATE   INDEX  SBDf_idx_acct ON V98_Viewing_Table_SkyBetDates_Final  (account_number);
CREATE   INDEX  SBDf_idx_progname ON V98_Viewing_Table_SkyBetDates_Final  (programme_name );
CREATE   INDEX  SBDf_idx_proginstname ON V98_Viewing_Table_SkyBetDates_Final  (programme_instance_name);
CREATE   INDEX  SBDf_idx_cbindiv ON V98_Viewing_Table_SkyBetDates_Final  (cb_key_individual);
CREATE   INDEX  SBDf_idx_cbfamily ON V98_Viewing_Table_SkyBetDates_Final  (cb_key_family);
Commit;

--------------
-Code 02 - Capped Tables
--------------
--Capped Tables require aggregation as it is daily
SELECT augs.cb_row_id, augs.timeshifting, augs.viewing_starts, augs.viewing_stops, augs.viewing_duration, augs.capped_flag,
cast('2012-08-13' as date) augs_table_dt
INTO  V98_CappedViewingTK
FROM vespa_analysts.Vespa_daily_augs_20120813 AS augs INNER JOIN V98_Viewing_Table_SkyBetDates_Final as vw
ON augs.cb_row_id = vw.pk_viewing_prog_instance_fact;
Commit;
INSERT INTO V98_CappedViewingTK
SELECT augs.cb_row_id, augs.timeshifting, augs.viewing_starts, augs.viewing_stops, augs.viewing_duration, augs.capped_flag,
cast('2012-08-14' as date) augs_table_dt
INTO  V98_CappedViewingTK
FROM vespa_analysts.Vespa_daily_augs_20120814 AS augs INNER JOIN V98_Viewing_Table_SkyBetDates_Final AS vw
ON augs.cb_row_id = vw.pk_viewing_prog_instance_fact;
Commit;
INSERT INTO V98_CappedViewingTK
SELECT augs.cb_row_id, augs.timeshifting, augs.viewing_starts, augs.viewing_stops, augs.viewing_duration, augs.capped_flag,
cast('2012-08-15' as date) augs_table_dt
INTO  V98_CappedViewingTK
FROM vespa_analysts.Vespa_daily_augs_20120815 AS augs INNER JOIN V98_Viewing_Table_SkyBetDates_Final AS vw
ON augs.cb_row_id = vw.pk_viewing_prog_instance_fact;
Commit;
INSERT INTO V98_CappedViewingTK
SELECT augs.cb_row_id, augs.timeshifting, augs.viewing_starts, augs.viewing_stops, augs.viewing_duration, augs.capped_flag,
cast('2012-08-16' as date) augs_table_dt
INTO  V98_CappedViewingTK
FROM vespa_analysts.Vespa_daily_augs_20120816 AS augs INNER JOIN V98_Viewing_Table_SkyBetDates_Final AS vw
ON augs.cb_row_id = vw.pk_viewing_prog_instance_fact;
Commit;
INSERT INTO V98_CappedViewingTK
SELECT augs.cb_row_id, augs.timeshifting, augs.viewing_starts, augs.viewing_stops, augs.viewing_duration, augs.capped_flag,
cast('2012-08-17' as date) augs_table_dt
INTO  V98_CappedViewingTK
FROM vespa_analysts.Vespa_daily_augs_20120817 AS augs INNER JOIN V98_Viewing_Table_SkyBetDates_Final AS vw
ON augs.cb_row_id = vw.pk_viewing_prog_instance_fact;
Commit;
INSERT INTO V98_CappedViewingTK
SELECT augs.cb_row_id, augs.timeshifting, augs.viewing_starts, augs.viewing_stops, augs.viewing_duration, augs.capped_flag,
cast('2012-08-18' as date) augs_table_dt
INTO  V98_CappedViewingTK
FROM vespa_analysts.Vespa_daily_augs_20120818 AS augs INNER JOIN V98_Viewing_Table_SkyBetDates_Final AS vw
ON augs.cb_row_id = vw.pk_viewing_prog_instance_fact;
Commit;
INSERT INTO V98_CappedViewingTK
SELECT augs.cb_row_id, augs.timeshifting, augs.viewing_starts, augs.viewing_stops, augs.viewing_duration, augs.capped_flag,
cast('2012-08-19' as date) augs_table_dt
INTO  V98_CappedViewingTK
FROM vespa_analysts.Vespa_daily_augs_20120819 AS augs INNER JOIN V98_Viewing_Table_SkyBetDates_Final AS vw
ON augs.cb_row_id = vw.pk_viewing_prog_instance_fact;
commit;
INSERT INTO V98_CappedViewingTK
SELECT augs.cb_row_id, augs.timeshifting, augs.viewing_starts, augs.viewing_stops, augs.viewing_duration, augs.capped_flag,
cast('2012-08-20' as date) augs_table_dt
INTO  V98_CappedViewingTK
FROM vespa_analysts.Vespa_daily_augs_20120820 AS augs INNER JOIN V98_Viewing_Table_SkyBetDates_Final AS vw
ON augs.cb_row_id = vw.pk_viewing_prog_instance_fact;
Commit;
INSERT INTO V98_CappedViewingTK
SELECT augs.cb_row_id, augs.timeshifting, augs.viewing_starts, augs.viewing_stops, augs.viewing_duration, augs.capped_flag,
cast('2012-08-21' as date) augs_table_dt
INTO  V98_CappedViewingTK
FROM vespa_analysts.Vespa_daily_augs_20120821 AS augs INNER JOIN V98_Viewing_Table_SkyBetDates_Final AS vw
ON augs.cb_row_id = vw.pk_viewing_prog_instance_fact; 
Commit;
INSERT INTO V98_CappedViewingTK
SELECT augs.cb_row_id, augs.timeshifting, augs.viewing_starts, augs.viewing_stops, augs.viewing_duration, augs.capped_flag,
cast('2012-08-22' as date) augs_table_dt
INTO  V98_CappedViewingTK
FROM vespa_analysts.Vespa_daily_augs_20120822 AS augs INNER JOIN V98_Viewing_Table_SkyBetDates_Final AS vw
ON augs.cb_row_id = vw.pk_viewing_prog_instance_fact;
Commit;
INSERT INTO V98_CappedViewingTK
SELECT augs.cb_row_id, augs.timeshifting, augs.viewing_starts, augs.viewing_stops, augs.viewing_duration, augs.capped_flag,
cast('2012-08-23' as date) augs_table_dt
INTO  V98_CappedViewingTK
FROM vespa_analysts.Vespa_daily_augs_20120823 AS augs INNER JOIN V98_Viewing_Table_SkyBetDates_Final AS vw
ON augs.cb_row_id = vw.pk_viewing_prog_instance_fact;
Commit;
INSERT INTO V98_CappedViewingTK
SELECT augs.cb_row_id, augs.timeshifting, augs.viewing_starts, augs.viewing_stops, augs.viewing_duration, augs.capped_flag,
cast('2012-08-24' as date) augs_table_dt
INTO  V98_CappedViewingTK
FROM vespa_analysts.Vespa_daily_augs_20120824 AS augs INNER JOIN V98_Viewing_Table_SkyBetDates_Final AS vw
ON augs.cb_row_id = vw.pk_viewing_prog_instance_fact;
Commit;
INSERT INTO V98_CappedViewingTK
SELECT augs.cb_row_id, augs.timeshifting, augs.viewing_starts, augs.viewing_stops, augs.viewing_duration, augs.capped_flag,
cast('2012-08-25' as date) augs_table_dt
INTO  V98_CappedViewingTK
FROM vespa_analysts.Vespa_daily_augs_20120825 AS augs INNER JOIN V98_Viewing_Table_SkyBetDates_Final AS vw
ON augs.cb_row_id = vw.pk_viewing_prog_instance_fact;
Commit;
INSERT INTO V98_CappedViewingTK
SELECT augs.cb_row_id, augs.timeshifting, augs.viewing_starts, augs.viewing_stops, augs.viewing_duration, augs.capped_flag,
cast('2012-08-26' as date) augs_table_dt
INTO  V98_CappedViewingTK
FROM vespa_analysts.Vespa_daily_augs_20120826 AS augs INNER JOIN V98_Viewing_Table_SkyBetDates_Final AS vw
ON augs.cb_row_id = vw.pk_viewing_prog_instance_fact;
Commit;
--17207644 row(s) affected



--INDEXING
CREATE   INDEX  CappedTK_idx_cbrowid ON V98_CappedViewingTK  (cb_row_id);
CREATE   INDEX  CappedTK_idx_vwstrt ON V98_CappedViewingTK  (viewing_starts);
CREATE   INDEX  CappedTK_idx_vwsecs ON V98_CappedViewingTK  (viewing_duration);
CREATE   INDEX  CappedTK_idx_dt ON V98_CappedViewingTK  (augs_table_dt);
Commit;

--------------
-Code 03 - Capped Tables Fix
--------------
--Tony Kinnaird fix required by redoing the capped tables. Problem was that CB_ROW_ID showed over 1 million rows with duplicate cb_row_id's. This process below will delete the dupes so that only the first instance will be captured and the second will be deleted. 
CREATE   INDEX  cpdvwtk_idx_cb ON V98_CappedViewingTK (cb_row_id);
Create table viewing_capped_dupes
(pk_viewing_prog_instance_fact bigint
,daily_table_date date
,rank int);
Commit;
insert into viewing_capped_dupes
select * from 
(select cb_row_id, augs_table_dt,
rank () over (partition by cb_row_id order by augs_table_dt) rank
from V98_CappedViewingTK) t
where rank > 1;
commit;
create hg index idx1_viewing_capped_dupes on viewing_capped_dupes(pk_viewing_prog_instance_fact);
create lf index idx2_viewing_capped_dupes on viewing_capped_dupes(daily_table_date);
Commit;
create hg index idx1_v98_cappedviewingtk on v98_cappedviewingtk(cb_row_id);
create lf index idx2_v98_cappedviewing on v98_cappedviewingtk(augs_table_dt);
Commit;
delete from v98_cappedviewingtk
from v98_cappedviewingtk a, viewing_capped_dupes b
where a.cb_row_id = b.pk_viewing_prog_instance_fact
and a.augs_table_dt = b.daily_table_date;
commit;

--Checks on cb_row _id dupes
--select count(1) from v98_cappedviewingtk
--union all
--select count(distinct cb_row_id) from v98_cappedviewingtk
--count(1)
--236625685
--236625685

--select COUNT (*), COUNT (distinct (cb_row_id))
--from V98_CappedViewingTK
--COUNT(),COUNT(distinct(V98_CappedViewingTK.cb_row_id))
--236625685,236625685


--------------
-Code 04 - Segments Table
--------------
-- We are creating vewing segments based on genre, genre-subgenre, and channels. There are 38 segments in all.

CREATE TABLE V98_Segment_Program_table2 (segment_id varchar (35) NOT NULL, dk_programme_instance_dim int, genre_description varchar (35) NOT NULL, sub_genre_description varchar (35) NOT NULL);
Commit;

INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT 'channel_attheraces',  dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE channel_name='At The Races';
Commit; 
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT 'channel_racinguk', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE channel_name='Racing UK';
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'channel_SkyPokercom', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE channel_name='SkyPoker.com';
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'channel_SuperCasino', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE channel_name='SuperCasino';
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'channel_SmartLive', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE channel_name='SmartLive';
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_sports', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Sports';
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_sports_football', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Sports' 
AND sub_genre_description IN ('Football');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_sports_racing', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Sports' 
AND sub_genre_description IN ('Racing');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_sports_American Football', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Sports' 
AND sub_genre_description IN ('American Football');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_sports_Athletics', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Sports' 
AND sub_genre_description IN ('Athletics');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_sports_Baseball', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Sports' 
AND sub_genre_description IN ('Baseball');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_sports_Basketball', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Sports' 
AND sub_genre_description IN ('Basketball');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_sports_Boxing', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Sports' 
AND sub_genre_description IN ('Boxing');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_sports_Cricket', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Sports' 
AND sub_genre_description IN ('Cricket');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_sports_Darts', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Sports' 
AND sub_genre_description IN ('Darts');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_sports_Equestrian', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Sports' 
AND sub_genre_description IN ('Equestrian');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_sports_Extreme', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Sports' 
AND sub_genre_description IN ('Extreme');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_sports_Fishing', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Sports' 
AND sub_genre_description IN ('Fishing');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_sports_undefined', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Sports' 
AND sub_genre_description IN ('Undefined');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_sports_Golf', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Sports' 
AND sub_genre_description IN ('Golf');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_sports_Ice Hockey', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Sports' 
AND sub_genre_description IN ('Ice Hockey');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_sports_Motor Sport', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Sports' 
AND sub_genre_description IN ('Motor Sport');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_sports_Rugby', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Sports' 
AND sub_genre_description IN ('Rugby');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_sports_Snooker/Pool', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Sports' 
AND sub_genre_description IN ('Snooker/Pool');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_sports_Tennis', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Sports' 
AND sub_genre_description IN ('Tennis');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_sports_Watersports', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Sports' 
AND sub_genre_description IN ('Watersports');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_sports_Wintersports', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Sports' 
AND sub_genre_description IN ('Wintersports');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_sports_Wrestling', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Sports' 
AND sub_genre_description IN ('Wrestling');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_sports_other', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Sports' 
AND sub_genre_description NOT IN ('Racing','Football');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_specialist_Gaming', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Specialist' 
AND sub_genre_description IN ('Gaming');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_entertainment', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Entertainment' ;
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_entertainment_comedy', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Entertainment' 
AND sub_genre_description IN ('Comedy');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_entertainment_Detective', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Entertainment' 
AND sub_genre_description IN ('Detective');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_entertainment_Drama', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Entertainment' 
AND sub_genre_description IN ('Drama');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_entertainment_gameshows', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Entertainment' 
AND sub_genre_description IN ('Game Shows');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_entertainment_chatshow', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Entertainment' 
AND sub_genre_description IN ('Chat Show');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_entertainment_Motors', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Entertainment' 
AND sub_genre_description IN ('Motors');
Commit;
INSERT INTO V98_Segment_Program_table2
SELECT DISTINCT  'genre_entertainment_Soaps', dk_programme_instance_dim, genre_description, sub_genre_description
from V98_Viewing_Table_SkyBetDates_Final
WHERE genre_description='Entertainment' 
AND sub_genre_description IN ('Soaps');
Commit;
CREATE   INDEX  epg2_idx_dk ON V98_Segment_Program_table2 (dk_programme_instance_dim);
CREATE   INDEX  epg2_idx_genre ON V98_Segment_Program_table2 (genre_description);
CREATE   INDEX  epg2_idx_subgenre ON V98_Segment_Program_table2 (sub_genre_description);
Commit;
--check
--SELECT Distinct segment_id--, COUNT (segment_id)
--FROM V98_Segment_Program_table2
--GROUP by segment_id
--ORDER by segment_id

--------------
-Code 05 - Segment Viewing
--------------
--Create Main Raw Table called 'V98_SkyBet_Main_Raw_Table'
--This table houses all of our variables needed to calculate viewing for the segments 
SELECT b.account_number, a.segment_id, a.dk_programme_instance_dim, c.viewing_starts, CONVERT(date, b.broadcast_start_date_time_utc) as broadcast_date_utc, c.viewing_duration, b.programme_instance_duration
Into V98_SkyBet_Main_Raw_Table
FROM V98_Segment_Program_table2 AS a 
INNER JOIN V98_Viewing_Table_SkyBetDates_Final AS b
ON a.dk_programme_instance_dim=b.dk_programme_instance_dim
INNER JOIN V98_CappedViewingtk AS c
ON c.cb_row_id=b.pk_viewing_prog_instance_fact
Commit;

--Delete viewing of less than 7 seconds 
DELETE from V98_SkyBet_Main_Raw_Table 
WHERE viewing_duration<7;

CREATE   INDEX RwTbl_Idx_Acct  ON V98_SkyBet_Main_Raw_Table  (account_number);
CREATE   INDEX RwTbl_Idx_viewst  ON V98_SkyBet_Main_Raw_Table (viewing_starts);
CREATE   INDEX RwTbl_Idx_segid  ON V98_SkyBet_Main_Raw_Table (segment_id);
CREATE   INDEX RawTbl_Idx_proginstduration  ON V98_SkyBet_Main_Raw_Table (programme_instance_duration);
CREATE   INDEX RawTbl_Idx_viewingduration  ON V98_SkyBet_Main_Raw_Table (viewing_duration);
CREATE   INDEX RawTbl_Idx_brdcstdate  ON V98_SkyBet_Main_Raw_Table (broadcast_date_utc);
CREATE   INDEX RawTbl_Idx_dkproginsdim  ON V98_SkyBet_Main_Raw_Table (dk_programme_instance_dim);
Commit;
--244196292 row(s) affected

--------------
-Code 06 - Total Account Viewing
--------------
--These group of tables is trying to calculate the total viewing spent for an account during the  two weeks limited by this study
SELECT b.account_number, b.dk_programme_instance_dim, c.viewing_starts, CONVERT(date, b.broadcast_start_date_time_utc) as broadcast_date_utc, c.viewing_duration, b.programme_instance_duration, b.genre_description, b.sub_genre_description, b.pk_viewing_prog_instance_fact
Into V98_Tot_Mins_Cap_Raw
FROM V98_Viewing_Table_SkyBetDates_Final AS b
INNER JOIN V98_CappedViewingtk AS c
ON c.cb_row_id=b.pk_viewing_prog_instance_fact;
Commit;
--236625685 row(s) affected

--SMO asked if we can add some more fields (not necessary for the Sky BET Project but I just wanted the information to be out there that this table has 3 more variables now after SkyBet Project) 
ALTER TABLE V98_Tot_Mins_Cap_Raw ADD viewing_stops varchar(23) null;
ALTER TABLE V98_Tot_Mins_Cap_Raw ADD subscriber_id decimal(8,0) null
ALTER TABLE V98_Tot_Mins_Cap_Raw ADD cb_key_household bigint null
commit;

--We need to create another table 'V98_Tot_Mins_Cap_Raw3' which is the same as V98_Tot_Mins_Cap_Raw but with an additional 3 variables. This will be capped as it goes through the same process. (Updated 22/10/2012)
SELECT b.account_number, b.dk_programme_instance_dim, c.viewing_starts, c.viewing_stops, CONVERT(date, b.broadcast_start_date_time_utc) as broadcast_date_utc, c.viewing_duration, b.programme_instance_duration, b.genre_description, b.sub_genre_description, b.pk_viewing_prog_instance_fact, b.subscriber_id, b.cb_key_household 
Into V98_Tot_Mins_Cap_Raw3
FROM V98_Viewing_Table_SkyBetDates_Final AS b
INNER JOIN V98_CappedViewingtk AS c
ON c.cb_row_id=b.pk_viewing_prog_instance_fact;
Commit;

--Delete the original but we will recreate again with the new variables added -- see next step    
Drop Table V98_Tot_Mins_Cap_Raw

--Select Into V98_Tot_Mins_Cap_Raw (which is the same name as the original in order to make it easier for me).
Select * INTO V98_Tot_Mins_Cap_Raw
FROM V98_Tot_Mins_Cap_Raw3;
Commit; 

--Delete viewing of less than 7 seconds 
DELETE from V98_Tot_Mins_Cap_Raw  
WHERE viewing_duration<7;

CREATE   INDEX TotRwTbl_Idx_Acct  ON V98_Tot_Mins_Cap_Raw  (account_number);
CREATE   INDEX TotRwTbl_Idx_viewst  ON V98_Tot_Mins_Cap_Raw (viewing_starts);
CREATE   INDEX TotRwTbl_Idx_pk  ON V98_Tot_Mins_Cap_Raw (pk_viewing_prog_instance_fact);
CREATE   INDEX TotRawTbl_Idx_proginstduration  ON V98_Tot_Mins_Cap_Raw (programme_instance_duration);
CREATE   INDEX TotRawTbl_Idx_viewingduration  ON V98_Tot_Mins_Cap_Raw (viewing_duration);
CREATE   INDEX TotRawTbl_Idx_brdcstdate  ON V98_Tot_Mins_Cap_Raw (broadcast_date_utc);
CREATE   INDEX TotRawTbl_Idx_dkproginsdim  ON V98_Tot_Mins_Cap_Raw (dk_programme_instance_dim);
CREATE   INDEX TotRawTbl_Idx_subgenre  ON V98_Tot_Mins_Cap_Raw (sub_genre_description);
CREATE   INDEX TotRawTbl_Idx_genre  ON V98_Tot_Mins_Cap_Raw (genre_description);
Commit; 

--------------
-Code 07 - Master Tables
--------------
--There two Master Tables. ONe which is trying to calculate Total Viewing for the accounts called  "V98_Tot_Mins_Master" and one that is trying to calculate the viewing time for the segments/accounts called 'V98_SkyBet_Master_Table'. 
--ONce we aggregate the viewing times we will join them back to get Share of Viewing 

--Let's create 'V98_Tot_Mins_Master'. This is total viewing - need to sum the viewing durations of an accounts total viewing per day 
SELECT account_number, broadcast_date_utc, SUM (CASE WHEN viewing_duration < programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Tot_Viewing_Trumped_Sum, COUNT(distinct dk_programme_instance_dim) as Total_Programs_Watched 
Into V98_Tot_Mins_Master
FROM V98_Tot_Mins_Cap_Raw  
GROUP BY account_number, broadcast_date_utc
ORDER BY account_number, broadcast_date_utc
Commit;
--7140054 row(s) affected

--Let's create the 'V98_SkyBet_Master_Table' -- gives us the total viewing per account and segment
SELECT account_number, segment_id, broadcast_date_utc, SUM (CASE WHEN viewing_duration < programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Viewing_Trumped_Sum, COUNT(distinct dk_programme_instance_dim) as Segment_Programs_Watched
Into V98_SkyBet_Master_Table
FROM V98_SkyBet_Main_Raw_Table  
GROUP BY account_number, broadcast_date_utc, segment_id
ORDER BY account_number, broadcast_date_utc, segment_id
Commit;
--39232982 row(s) affected

--select count (distinct account_number)
--FROM V98_SkyBet_Master_Table
--603,336 08/10/2012

--select count (distinct account_number)
--FROM V98_Tot_Mins_Master
--607,844 08/10/2012

CREATE   INDEX SBM_Idx_Acct  ON V98_SkyBet_Master_Table  (account_number);
CREATE   INDEX SBM_Idx_brdcstdate  ON V98_SkyBet_Master_Table (broadcast_date_utc);
CREATE   INDEX TotMinsMstr_Idx_Acct  ON V98_Tot_Mins_Master  (account_number);
CREATE   INDEX TotMinsMstr_Idx_brdcstdate  ON V98_Tot_Mins_Master (broadcast_date_utc);
CREATE INDEX RawTab2_idx_prog ON V98_SkyBet_Main_Raw_Table2 (programme_name);

--------------
--Code 08 - Scaling Tables - NO LONGER NECESSARY as the study is trying to find prospects within the VESPA PANEL and NOT the Sky Base. May be useful in the future and Tom has asked me. 
--But we added scaling to our overall viewing table as Dan Barnett needed it. See active script further below.
--------------

--Create Scaling Table
--select event.*, w.scaling_day, w.vespa_accounts, w.sky_base_accounts, w.weighting
--  into V098_scaling_tbl
--  from (select  e.account_number,
--                date(broadcast_start_date_time_utc) event_date,
--                pk_viewing_prog_instance_fact,
--                s.scaling_segment_id
--           from V98_Viewing_Table_SkyBetDates_Final e,
--                vespa_analysts.SC2_intervals s
--          where e.account_number = s.account_number
--            and date(broadcast_start_date_time_utc) between date(s.reporting_starts) and date(s.reporting_ends)
--       ) event,
	   --       vespa_analysts.SC2_weightings w-
-- where event.event_date = w.scaling_day
--   and event.scaling_segment_id = w.scaling_segment_id;
--commit;
--309106708 row(s) affected

--create indexes
--drop index V098_scaling_ac_idx;
--create index V098_scaling_ac_idx on V098_scaling_tbl(account_number);
--create index V098_scaling_eventdate_idx on V098_scaling_tbl(event_date);
--create index V098_scaling_pk_idx on V098_scaling_tbl(pk_viewing_prog_instance_fact);
--create index V098_scaling_segment_idx on V098_scaling_tbl(scaling_segment_id);
--create index V098_scaling_scaleday_idx on V098_scaling_tbl(scaling_day);
--commit;

--Create Distinct Scaling -- so that we don't have a billion rows.
--SELECT DISTINCT account_number, event_date, weighting
--INTO V098_distinct_scaling_tbl
--FROM V098_scaling_tbl;
--Commit;
--7117521 row(s) affected
--Execution time: 132.917 seconds
--Execution time: 0.117 seconds

--create index V098_distinctscaling_ac_idx on V098_distinct_scaling_tbl(account_number);
--create index V098_distinctscaling_eventdate_idx on V098_distinct_scaling_tbl(event_date);
--create index V098_distinctscaling_wt_idx on V098_distinct_scaling_tbl(weighting);
--commit;

--grant select on V098_scaling_tbl to vespa_group_low_security;


-----Create additional weighting variables in your viewing table to help with Dan Barnett's project.--------
ALTER TABLE V98_Tot_Mins_Cap_Raw ADD scaling_segment_ID    int;
ALTER TABLE V98_Tot_Mins_Cap_Raw ADD weightings            float default 0; Commit;
create index TotMinsRaw_idx_wts on V98_Tot_Mins_Cap_Raw (broadcast_date_utc);

update V98_Tot_Mins_Cap_Raw
set scaling_segment_ID = l.scaling_segment_ID
from V98_Tot_Mins_Cap_Raw as b
inner join vespa_analysts.SC2_intervals as l
on b.account_number = l.account_number
and b.broadcast_date_utc between l.reporting_starts and l.reporting_ends;
commit;
-- 233221283 row(s) updated, Execution time: 1007.356 seconds

-- Find out the weight for that segment on that day
update V98_Tot_Mins_Cap_Raw
set weightings = s.weighting
from V98_Tot_Mins_Cap_Raw as b
inner join vespa_analysts.SC2_weightings as s
on b.broadcast_date_utc = s.scaling_day
and b.scaling_segment_ID = s.scaling_segment_ID;
commit;
--233220585 row(s) affected, Execution time: 708.877 seconds
-- That's it! Now you've got the weightings.

--------------
--Code 09 - Share of Viewing Tables 
--------------
--Main Table SOV Finally! This is going to be our working table and will help us to create the final output.
SELECT e1.account_number, e1.segment_id, e1.broadcast_date_utc, e1.Segment_Programs_Watched, d1.Total_Programs_Watched, CAST (e1.Segment_Programs_Watched AS FLOAT)/ CAST(Total_Programs_Watched AS FLOAT) AS sov_progs, e1.Viewing_Trumped_Sum, d1.Tot_Viewing_Trumped_Sum, e1.Viewing_Trumped_Sum/d1.Tot_Viewing_Trumped_Sum AS SOV_raw
INTO V98_MainTable_SOV_Final
FROM V98_Tot_Mins_Master AS d1
INNER JOIN V98_SkyBet_Master_Table as e1
ON d1.account_number=e1.account_number
AND d1.broadcast_date_utc=e1.broadcast_date_utc
Commit;
CREATE   INDEX MainSOVF_Idx_brdcstdate  ON V98_MainTable_SOV_Final (broadcast_date_utc);
CREATE   INDEX MainSOVF_Idx_Acct  ON V98_MainTable_SOV_Final  (account_number);

--Add new columns to our V98_MainTable_SOV_Final
ALTER TABLE V98_MainTable_SOV_Final ADD AcctSegmentSecs_raw_AllDates float null; 
ALTER TABLE V98_MainTable_SOV_Final ADD AcctTotalSecs_raw_AllDates float null;
ALTER TABLE V98_MainTable_SOV_Final ADD AcctSegmentSOV_raw_AllDates float null;
ALTER TABLE V98_MainTable_SOV_Final ADD ReportingDays float null;
ALTER TABLE V98_MainTable_SOV_Final ADD AcctSegmentsAvgSecs_raw_AllDates float null;
ALTER TABLE V98_MainTable_SOV_Final ADD AcctAvgSecs_raw_AllDates float null;
commit;

--Let's Get the Total Seconds viewing for each account across the two weeks
SELECT account_number, SUM(Tot_Viewing_Trumped_Sum) AcctTotalSecs
into AcctSecs_raw_aggregates_AllDates
FROM V98_Tot_Mins_Master 
GROUP BY account_number;
COmmit;
--607844 row(s) affected

--Let's update the main table
UPDATE V98_MainTable_SOV_Final
SET f.AcctTotalSecs_raw_AllDates=g.AcctTotalSecs
FROM V98_MainTable_SOV_Final f
INNER JOIN AcctSecs_raw_aggregates_AllDates as g
ON f.account_number=g.account_number;
commit;
--39232982 row(s) updated

--Let's get Total Account-Segment Viewing for the two weeks. So this means that we should have a total viewing for each SEGMENT for EACH ACCOUNT!
SELECT account_number, segment_id, SUM(Viewing_Trumped_Sum) AcctSegmentSecs  
into AcctSegmentSecs_raw_aggregates_AllDates
  FROM V98_MainTable_SOV_Final 
GROUP BY account_number, segment_id ; Commit;
--7922612 row(s) affected

----Let's update the main table
UPDATE V98_MainTable_SOV_Final
SET f.AcctSegmentSecs_raw_AllDates=h.AcctSegmentSecs
FROM V98_MainTable_SOV_Final f
INNER JOIN AcctSegmentSecs_raw_aggregates_AllDates as h
ON h.account_number=f.account_number
AND h.segment_id=f.segment_id; commit;
--39232982 row(s) updated

--Let's get the Share of Viewing (SOV). We want the viewing for EACH account-segment in relation to the account's TOTAL VIEWING.
UPDATE V98_MainTable_SOV_Final 
SET AcctSegmentSOV_raw_AllDates=cast(AcctSegmentSecs_raw_AllDates as double)/cast(AcctTotalSecs_raw_AllDates as double); Commit;
--39232982 row(s) updated

--Need to add reporting days so we can get the average viewing (based on number of days reporting)
UPDATE V98_MainTable_SOV_Final
SET f.ReportingDays=report.reporting_days
FROM V98_MainTable_SOV_Final as f
INNER JOIN barbera.V098_box_reporting_days_tbl as report
ON f.account_number=report.account_number;
Commit;
--39232982 row(s) updated
--Found over 60k accounts that 0 days reporting. This means it division function to get Average Seconds per account based on the number of days reporting will not work unless we convert 0's into nulls
UPDATE V98_MainTable_SOV_Final
SET AcctAvgSecs_raw_AllDates=CASE WHEN ReportingDays=0 THEN NULL
ELSE AcctTotalSecs_raw_AllDates/ReportingDays END;
COMMIT;


--------------
--Code 10 - Deciles
--------------
--We would like to decile the share of viewing of Segments associated with Accounts and we would also like to decile average seconds viewing (bsed on boxes reporting) across the two weeks of this study.
--(added 15/10/2012) There was a problem with the deciling. It was leading to skewed distribution towards the lower deciles. IN addition, there there were over 8k accounts that were missed by the 10th decile and over 30k accounts that were missed by Decile 1. 
--We have looked at this and it looks like the problem was that we needed to summarise the V98_MainTable_SOV_Final as we have multiple AcctSegmentSOV_raw_AllDates entries (multiplied by the number of reporting days)
--So we need to de-dupe, so that there is only one entry for each segment_id, and account_number.
--Then we can work out what the deciles are meant to be… so they won’t be affected by the reporting days

--Original deciling sent to Sky Bet - led to skewing towards the lower deciles due to the V98_MainTable_SOV_Final not being aggregated first. This will not be used in the final output anymore for future Sky Projects. We have an updated further below. 22/10/2012
--SELECT  --an option for day/time maybe required here,
     --   segment_id,
    --    PERCENTILE_CONT(0.001) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_100,
    --    PERCENTILE_CONT(0.1) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_90,
    --    PERCENTILE_CONT(0.2) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_80,
    --    PERCENTILE_CONT(0.3) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_70,
    --    PERCENTILE_CONT(0.4) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_60,
    --    PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_50,
    --    PERCENTILE_CONT(0.6) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_40,
    --    PERCENTILE_CONT(0.7) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_30,
    --    PERCENTILE_CONT(0.8) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_20,
    --    PERCENTILE_CONT(0.9) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_10,
    --    PERCENTILE_CONT(0.999) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_0
  --INTO  V098_AcctAvgSecs_raw_AllDates_pc_tbl
  --FROM   V98_MainTable_SOV_Final
  --GROUP BY --an option for day/time maybe required here,
  --      segment_id;
--select segment_id, 10 'decile', pc_90 pc_start, pc_100 pc_end
  --into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
  --from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--commit;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 9 'decile', pc_80 pc_start, pc_90 pc_end
  --from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 8 'decile', pc_70 pc_start, pc_80 pc_end
  --from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 7 'decile', pc_60 pc_start, pc_70 pc_end
  --from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 6 'decile', pc_50 pc_start, pc_60 pc_end
  --from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 5 'decile', pc_40 pc_start, pc_50 pc_end
  --from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 4 'decile', pc_30 pc_start, pc_40 pc_end
 -- from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 3 'decile', pc_20 pc_start, pc_30 pc_end
  --from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 2 'decile', pc_10 pc_start, pc_20 pc_end
  --from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 1 'decile', pc_0 pc_start, pc_10 pc_end
  --from V098_AcctAvgSecs_raw_AllDates_pc_tbl;


--Need to add new columns to placed account deciles in (added 15/10/2012)
--ALTER TABLE V98_MainTable_SOV_Final add SOV_OLD integer null
--Need to add new columns to placed account deciles in
--ALTER TABLE V98_MainTable_SOV_Final add SOV_Decile float null;
--ALTER TABLE V98_MainTable_SOV_Final ADD Viewed_Decile INTEGER null;
--ALTER TABLE V98_MainTable_SOV_Final  ADD Segment_Viewed_Decile integer null
--Commit;

--Update our MainTable to add the account deciles (added 15/10/2012)
--UPDATE V98_MainTable_SOV_Final
--SET  SOV_OLD=sov.decile
--FROM V98_MainTable_SOV_Final f
--INNER JOIN V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl sov
--ON f.segment_id = sov.segment_id
--AND f.AcctSegmentSOV_raw_AllDates BETWEEN pc_start and pc_end;
--Commit;
--38,448,444 row(s) updated 08/10/2012

--Fix from above includes dropping lower decile bound to '0' and upper decile bound to '10' (updated 15/10/2012) 
--this did not fix the skewing problem though despite the fix capturing almost all of the 38k missing accounts (updated 22/10/2012). Aggregation was required and fix is further below (updated 22/10/2012) 
--SELECT  --an option for day/time maybe required here,
  --      segment_id,
   --     PERCENTILE_CONT(0) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_100,
     --   PERCENTILE_CONT(0.1) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_90,
       -- PERCENTILE_CONT(0.2) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_80,
        --PERCENTILE_CONT(0.3) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_70,
        --PERCENTILE_CONT(0.4) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_60,
        --PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_50,
        --PERCENTILE_CONT(0.6) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_40,
        --PERCENTILE_CONT(0.7) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_30,
        --PERCENTILE_CONT(0.8) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_20,
        --PERCENTILE_CONT(0.9) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_10,
        --PERCENTILE_CONT(1) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_0
  --INTO  V098_AcctSegmentSOV_raw_AllDates_pc_tbl2
  --FROM   rombaoad.V98_MainTable_SOV_Final
  --GROUP BY --an option for day/time maybe required here,
    --    segment_id;
--commit;

--select segment_id, 10 'decile', pc_90 pc_start, pc_100 pc_end
  --into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
  --from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
--commit;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
--select segment_id, 9 'decile', pc_80 pc_start, pc_90 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
--select segment_id, 8 'decile', pc_70 pc_start, pc_80 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
--select segment_id, 7 'decile', pc_60 pc_start, pc_70 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
--select segment_id, 6 'decile', pc_50 pc_start, pc_60 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
--select segment_id, 5 'decile', pc_40 pc_start, pc_50 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
--select segment_id, 4 'decile', pc_30 pc_start, pc_40 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
--select segment_id, 3 'decile', pc_20 pc_start, pc_30 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
--select segment_id, 2 'decile', pc_10 pc_start, pc_20 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
--select segment_id, 1 'decile', pc_0 pc_start, pc_10 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
--commit; -- updated 15/10/2012

--Update the SOV_decile column with the information above
--UPDATE V98_MainTable_SOV_Final
--SET  SOV_Decile=fixedsov.decile
--FROM V98_MainTable_SOV_Final f
--INNER JOIN V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2 AS fixedsov
--ON f.segment_id = fixedsov.segment_id
--AND f.AcctSegmentSOV_raw_AllDates BETWEEN pc_start and pc_end;
--Commit;
--39232982 row(s) updated 15/10/2012

--Decile calculation of the accounts average seconds consumed across the two weeks which takes into consideration the reporting days of the boxes.
--SELECT  --an option for day/time maybe required here,
        --segment_id,
        --PERCENTILE_CONT(0) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_100,
        --PERCENTILE_CONT(0.1) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_90,
        --PERCENTILE_CONT(0.2) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_80,
        --PERCENTILE_CONT(0.3) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_70,
        --PERCENTILE_CONT(0.4) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_60,
        --PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_50,
        --PERCENTILE_CONT(0.6) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_40,
        --PERCENTILE_CONT(0.7) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_30,
        --PERCENTILE_CONT(0.8) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_20,
        --PERCENTILE_CONT(0.9) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_10,
        --PERCENTILE_CONT(1) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_0
  --INTO  V098_AcctAvgSecs_raw_AllDates_pc_tbl
  --FROM   V98_MainTable_SOV_Final
  --GROUP BY --an option for day/time maybe required here,
        --segment_id;
--commit;;
--select segment_id, 10 'decile', pc_90 pc_start, pc_100 pc_end
--  into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--  from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--commit;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 9 'decile', pc_80 pc_start, pc_90 pc_end
--  from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 8 'decile', pc_70 pc_start, pc_80 pc_end
--  from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 7 'decile', pc_60 pc_start, pc_70 pc_end
--  from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 6 'decile', pc_50 pc_start, pc_60 pc_end
--  from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 5 'decile', pc_40 pc_start, pc_50 pc_end
--  from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 4 'decile', pc_30 pc_start, pc_40 pc_end
--  from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 3 'decile', pc_20 pc_start, pc_30 pc_end
---  from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 2 'decile', pc_10 pc_start, pc_20 pc_end
--  from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 1 'decile', pc_0 pc_start, pc_10 pc_end
--  from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--commit;
--;

--Update the account average seconds consumed decile column with the information above
--UPDATE V98_MainTable_SOV_Final
--SET  Viewed_Decile=viewed.decile
--FROM V98_MainTable_SOV_Final f
--INNER JOIN V098_AcctAvgSecs_raw_AllDates_pc_query_tbl as viewed
--ON f.segment_id = viewed.segment_id
--AND f.AcctAvgSecs_raw_AllDates BETWEEN pc_start and pc_end;
--Commit; --39171075 row(s) updated, 15/10/2012

--Decile calculation of the accounts' individual segment average seconds consumed across the two weeks which takes into consideration the reporting days of the boxes.
--SELECT  --an option for day/time maybe required here,
--        segment_id,
--        PERCENTILE_CONT(0) WITHIN GROUP ( ORDER BY AcctSegmentsAvgSecs_raw_AllDates DESC ) pc_100,
--        PERCENTILE_CONT(0.1) WITHIN GROUP ( ORDER BY AcctSegmentsAvgSecs_raw_AllDates DESC ) pc_90,
--        PERCENTILE_CONT(0.2) WITHIN GROUP ( ORDER BY AcctSegmentsAvgSecs_raw_AllDates DESC ) pc_80,
--        PERCENTILE_CONT(0.3) WITHIN GROUP ( ORDER BY AcctSegmentsAvgSecs_raw_AllDates DESC ) pc_70,
--        PERCENTILE_CONT(0.4) WITHIN GROUP ( ORDER BY AcctSegmentsAvgSecs_raw_AllDates DESC ) pc_60,
--        PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY AcctSegmentsAvgSecs_raw_AllDates DESC ) pc_50,
--        PERCENTILE_CONT(0.6) WITHIN GROUP ( ORDER BY AcctSegmentsAvgSecs_raw_AllDates DESC ) pc_40,
--        PERCENTILE_CONT(0.7) WITHIN GROUP ( ORDER BY AcctSegmentsAvgSecs_raw_AllDates DESC ) pc_30,
--        PERCENTILE_CONT(0.8) WITHIN GROUP ( ORDER BY AcctSegmentsAvgSecs_raw_AllDates DESC ) pc_20,
--        PERCENTILE_CONT(0.9) WITHIN GROUP ( ORDER BY AcctSegmentsAvgSecs_raw_AllDates DESC ) pc_10,
--        PERCENTILE_CONT(1) WITHIN GROUP ( ORDER BY AcctSegmentsAvgSecs_raw_AllDates DESC ) pc_0
--  INTO V098_AcctSegmentsAvgSecs_raw_AllDates_pc_tbl
--  FROM V98_MainTable_SOV_Final
--  GROUP BY --an option for day/time maybe required here,
--        segment_id;
--commit;
--
--select segment_id, 10 'decile', pc_90 pc_start, pc_100 pc_end
--  into V098_AcctSegmentsAvgSecs_raw_AllDates_pc_query_tbl
--  from V098_AcctSegmentsAvgSecs_raw_AllDates_pc_tbl;
--commit;
--insert into V098_AcctSegmentsAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 9 'decile', pc_80 pc_start, pc_90 pc_end
--  from V098_AcctSegmentsAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctSegmentsAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 8 'decile', pc_70 pc_start, pc_80 pc_end
--  from V098_AcctSegmentsAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctSegmentsAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 7 'decile', pc_60 pc_start, pc_70 pc_end
--  from V098_AcctSegmentsAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctSegmentsAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 6 'decile', pc_50 pc_start, pc_60 pc_end
--  from V098_AcctSegmentsAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctSegmentsAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 5 'decile', pc_40 pc_start, pc_50 pc_end
--  from V098_AcctSegmentsAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctSegmentsAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 4 'decile', pc_30 pc_start, pc_40 pc_end
--  from V098_AcctSegmentsAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctSegmentsAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 3 'decile', pc_20 pc_start, pc_30 pc_end
--  from V098_AcctSegmentsAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctSegmentsAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 2 'decile', pc_10 pc_start, pc_20 pc_end
--  from V098_AcctSegmentsAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctSegmentsAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 1 'decile', pc_0 pc_start, pc_10 pc_end
--  from V098_AcctSegmentsAvgSecs_raw_AllDates_pc_tbl;
--commit;

--Update the account's individual segment average seconds consumed decile column with the information above
--select * From V98_MainTable_SOV_Final 
--UPDATE V98_MainTable_SOV_Final
--SET  Segment_Viewed_Decile=segmentvw.decile
--FROM V98_MainTable_SOV_Final f
--INNER JOIN V098_AcctSegmentsAvgSecs_raw_AllDates_pc_query_tbl as segmentvw
--ON f.segment_id = segmentvw.segment_id
--AND f.AcctSegmentsAvgSecs_raw_AllDates BETWEEN pc_start and pc_end;
--Commit;  --39,171,075 row(s) updated

---------------------------------------------------------------------Final Fix for Deciling----------------------------------------
--This process resulted in equal bands rather than the previous deciling attempts leading to skewed deciles (towards the lower deciles) as a result of not aggreagating the segments per account for the whole 14 days of the study.
------------------SOV Decile Fix (Updated 22/10/2012)
select distinct segment_id, account_number, AcctSegmentSOV_raw_AllDates
INTO sov_fix_barbera
From V98_MainTable_SOV_Final; Commit;

--Now using N-Tiles for deciling SOV and placing them into a new table
SELECT  segment_id, account_number,
        NTILE(10) OVER (partition by segment_id ORDER BY AcctSegmentSOV_raw_AllDates ASC) decile
into ntilesov3  
FROM  rombaoad.sov_fix_barbera; Commit;

--Update SOV_Ntile column with the information from above
UPDATE V98_MainTable_SOV_Final 
SET  SOV_Decile=decile from V98_MainTable_SOV_Final as f 
INNER JOIN ntilesov3 as t 
ON f.segment_id = t.segment_id
AND f.account_number = t.account_number; commit;
--39232982 row(s) updated, Execution time: 109.561 seconds

--------Viewed Decile Fix 19/10/2012
select distinct segment_id, account_number, AcctAvgSecs_raw_AllDates
INTO vieweddecile_fix_barbera
From V98_MainTable_SOV_Final; Commit;
--7922612 row(s) affected, Execution time: 48.296 seconds

--Now using N-Tiles for deciling the average seconds viewed by account and placing them into a new table. We did not add 'partition by segment_id' in the deciling operation as we are interested in deciling for total seconds viewed and not by segments in this case.
SELECT  segment_id, account_number,
        NTILE(10) OVER (/*	partition by segment_id */  ORDER BY AcctAvgSecs_raw_AllDates ASC) decile
into ntileviewedavgsecs3  
FROM  rombaoad.vieweddecile_fix_barbera; Commit;
--7922612 row(s) affected, Execution time: 26.531 seconds

--Update Viewed_Ntile column with the information from above
UPDATE V98_MainTable_SOV_Final 
SET  Viewed_Decile=decile from V98_MainTable_SOV_Final as f 
INNER JOIN ntileviewedavgsecs3 as avgsecs 
ON f.segment_id = avgsecs.segment_id
AND f.account_number = avgsecs.account_number; commit;
--39232982 row(s) affected, Execution time: 65.889 seconds

--------Segment Viewed Decile Fix 19/10/2012
select distinct segment_id, account_number, AcctSegmentsAvgSecs_raw_AllDates
INTO segmentvieweddecile_fix_barbera
From V98_MainTable_SOV_Final; Commit;
--7922612 row(s) affected, Execution time: 55.014 seconds

--Now using N-Tiles for deciling the average seconds viewed by each distinct segment for each account and placing them into a new table
SELECT  segment_id, account_number,
        NTILE(10) OVER (partition by segment_id ORDER BY AcctSegmentsAvgSecs_raw_AllDates ASC) decile
into ntilesegmentviewedavgsecs3  
FROM  segmentvieweddecile_fix_barbera; Commit;
--7922612 row(s) affected, Execution time: 25.827 seconds

--Update Segment_Viewed_Ntile column with the information from above
UPDATE V98_MainTable_SOV_Final 
SET  Segment_Viewed_Decile=decile from V98_MainTable_SOV_Final as f 
INNER JOIN ntilesegmentviewedavgsecs3 as avgsecsseg 
ON f.segment_id = avgsecsseg.segment_id
AND f.account_number = avgsecsseg.account_number; commit;
--39232982 row(s) affected, Execution time: 92.155 seconds

--------------
--Code 11 - SkyBet Final Output
--------------
--CREATE FINAL OUTPUT TABLE -- This table was sent to Sky Bet but will be revised for the future as the deciling operations did not work and therefore SkyBet referred to these as segments or groups rather than deciles due to the skew. There is an update or fix further below. (Updated 22/10/2012)
--select account_number
--, max(CASE when segment_id = 'channel_racinguk' THEN sov_decile_sum ELSE 0 END) as channel_racinguk_sov	, max(CASE when segment_id = 'channel_racinguk' THEN secs_decile_sum ELSE 0 END) as channel_racinguk_total_secs	, max(CASE when segment_id = 'channel_racinguk' THEN segment_viewed_decile ELSE 0 END) as channel_racinguk_segment_secs
--, max(CASE when segment_id = 'genre_sports_Tennis' THEN sov_decile_sum ELSE 0 END) as genre_sports_Tennis_sov	, max(CASE when segment_id = 'genre_sports_Tennis' THEN secs_decile_sum ELSE 0 END) as genre_sports_Tennis_total_secs	, max(CASE when segment_id = 'genre_sports_Tennis' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Tennis_segment_secs
--, max(CASE when segment_id = 'genre_entertainment_Soaps' THEN sov_decile_sum ELSE 0 END) as genre_entertainment_Soaps_sov	, max(CASE when segment_id = 'genre_entertainment_Soaps' THEN secs_decile_sum ELSE 0 END) as genre_entertainment_Soaps_total_secs	, max(CASE when segment_id = 'genre_entertainment_Soaps' THEN segment_viewed_decile ELSE 0 END) as genre_entertainment_Soaps_segment_secs
--, max(CASE when segment_id = 'genre_sports_Boxing' THEN sov_decile_sum ELSE 0 END) as genre_sports_Boxing_sov	, max(CASE when segment_id = 'genre_sports_Boxing' THEN secs_decile_sum ELSE 0 END) as genre_sports_Boxing_total_secs	, max(CASE when segment_id = 'genre_sports_Boxing' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Boxing_segment_secs
--, max(CASE when segment_id = 'genre_entertainment' THEN sov_decile_sum ELSE 0 END) as genre_entertainment_sov	, max(CASE when segment_id = 'genre_entertainment' THEN secs_decile_sum ELSE 0 END) as genre_entertainment_total_secs	, max(CASE when segment_id = 'genre_entertainment' THEN segment_viewed_decile ELSE 0 END) as genre_entertainment_segment_secs
--, max(CASE when segment_id = 'channel_SmartLive' THEN sov_decile_sum ELSE 0 END) as channel_SmartLive_sov	, max(CASE when segment_id = 'channel_SmartLive' THEN secs_decile_sum ELSE 0 END) as channel_SmartLive_total_secs	, max(CASE when segment_id = 'channel_SmartLive' THEN segment_viewed_decile ELSE 0 END) as channel_SmartLive_segment_secs
--, max(CASE when segment_id = 'genre_entertainment_chatshow' THEN sov_decile_sum ELSE 0 END) as genre_entertainment_chatshow_sov	, max(CASE when segment_id = 'genre_entertainment_chatshow' THEN secs_decile_sum ELSE 0 END) as genre_entertainment_chatshow_total_secs	, max(CASE when segment_id = 'genre_entertainment_chatshow' THEN segment_viewed_decile ELSE 0 END) as genre_entertainment_chatshow_segment_secs
--, max(CASE when segment_id = 'genre_sports_Athletics' THEN sov_decile_sum ELSE 0 END) as genre_sports_Athletics_sov	, max(CASE when segment_id = 'genre_sports_Athletics' THEN secs_decile_sum ELSE 0 END) as genre_sports_Athletics_total_secs	, max(CASE when segment_id = 'genre_sports_Athletics' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Athletics_segment_secs
--, max(CASE when segment_id = 'genre_entertainment_gameshows' THEN sov_decile_sum ELSE 0 END) as genre_entertainment_gameshows_sov	, max(CASE when segment_id = 'genre_entertainment_gameshows' THEN secs_decile_sum ELSE 0 END) as genre_entertainment_gameshows_total_secs	, max(CASE when segment_id = 'genre_entertainment_gameshows' THEN segment_viewed_decile ELSE 0 END) as genre_entertainment_gameshows_segment_secs
--, max(CASE when segment_id = 'genre_sports_Ice Hockey' THEN sov_decile_sum ELSE 0 END) as genre_sports_Ice_Hockey_sov	, max(CASE when segment_id = 'genre_sports_Ice Hockey' THEN secs_decile_sum ELSE 0 END) as genre_sports_Ice_Hockey_total_secs	, max(CASE when segment_id = 'genre_sports_Ice Hockey' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Ice_Hockey_segment_secs
--, max(CASE when segment_id = 'genre_sports_racing' THEN sov_decile_sum ELSE 0 END) as genre_sports_racing_sov	, max(CASE when segment_id = 'genre_sports_racing' THEN secs_decile_sum ELSE 0 END) as genre_sports_racing_total_secs	, max(CASE when segment_id = 'genre_sports_racing' THEN segment_viewed_decile ELSE 0 END) as genre_sports_racing_segment_secs
--, max(CASE when segment_id = 'channel_SkyPokercom' THEN sov_decile_sum ELSE 0 END) as channel_SkyPokercom_sov	, max(CASE when segment_id = 'channel_SkyPokercom' THEN secs_decile_sum ELSE 0 END) as channel_SkyPokercom_total_secs	, max(CASE when segment_id = 'channel_SkyPokercom' THEN segment_viewed_decile ELSE 0 END) as channel_SkyPokercom_segment_secs
--, max(CASE when segment_id = 'genre_entertainment_Motors' THEN sov_decile_sum ELSE 0 END) as genre_entertainment_Motors_sov	, max(CASE when segment_id = 'genre_entertainment_Motors' THEN secs_decile_sum ELSE 0 END) as genre_entertainment_Motors_total_secs	, max(CASE when segment_id = 'genre_entertainment_Motors' THEN segment_viewed_decile ELSE 0 END) as genre_entertainment_Motors_segment_secs
--, max(CASE when segment_id = 'genre_sports_Motor Sport' THEN sov_decile_sum ELSE 0 END) as genre_sports_Motor_Sport_sov	, max(CASE when segment_id = 'genre_sports_Motor Sport' THEN secs_decile_sum ELSE 0 END) as genre_sports_Motor_Sport_total_secs	, max(CASE when segment_id = 'genre_sports_Motor Sport' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Motor_Sport_segment_secs
--, max(CASE when segment_id = 'genre_sports_Golf' THEN sov_decile_sum ELSE 0 END) as genre_sports_Golf_sov	, max(CASE when segment_id = 'genre_sports_Golf' THEN secs_decile_sum ELSE 0 END) as genre_sports_Golf_total_secs	, max(CASE when segment_id = 'genre_sports_Golf' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Golf_segment_secs
--, max(CASE when segment_id = 'genre_sports_football' THEN sov_decile_sum ELSE 0 END) as genre_sports_football_sov	, max(CASE when segment_id = 'genre_sports_football' THEN secs_decile_sum ELSE 0 END) as genre_sports_football_total_secs	, max(CASE when segment_id = 'genre_sports_football' THEN segment_viewed_decile ELSE 0 END) as genre_sports_football_segment_secs
--, max(CASE when segment_id = 'genre_sports_Cricket' THEN sov_decile_sum ELSE 0 END) as genre_sports_Cricket_sov	, max(CASE when segment_id = 'genre_sports_Cricket' THEN secs_decile_sum ELSE 0 END) as genre_sports_Cricket_total_secs	, max(CASE when segment_id = 'genre_sports_Cricket' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Cricket_segment_secs
--, max(CASE when segment_id = 'genre_sports_Snooker/Pool' THEN sov_decile_sum ELSE 0 END) as genre_sports_Snooker/Pool_sov	, max(CASE when segment_id = 'genre_sports_Snooker/Pool' THEN secs_decile_sum ELSE 0 END) as genre_sports_Snooker/Pool_total_secs	, max(CASE when segment_id = 'genre_sports_Snooker/Pool' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Snooker/Pool_segment_secs
--, max(CASE when segment_id = 'genre_entertainment_Drama' THEN sov_decile_sum ELSE 0 END) as genre_entertainment_Drama_sov	, max(CASE when segment_id = 'genre_entertainment_Drama' THEN secs_decile_sum ELSE 0 END) as genre_entertainment_Drama_total_secs	, max(CASE when segment_id = 'genre_entertainment_Drama' THEN segment_viewed_decile ELSE 0 END) as genre_entertainment_Drama_segment_secs
--, max(CASE when segment_id = 'genre_sports_other' THEN sov_decile_sum ELSE 0 END) as genre_sports_other_sov	, max(CASE when segment_id = 'genre_sports_other' THEN secs_decile_sum ELSE 0 END) as genre_sports_other_total_secs	, max(CASE when segment_id = 'genre_sports_other' THEN segment_viewed_decile ELSE 0 END) as genre_sports_other_segment_secs
--, max(CASE when segment_id = 'genre_sports' THEN sov_decile_sum ELSE 0 END) as genre_sports_sov	, max(CASE when segment_id = 'genre_sports' THEN secs_decile_sum ELSE 0 END) as genre_sports_total_secs	, max(CASE when segment_id = 'genre_sports' THEN segment_viewed_decile ELSE 0 END) as genre_sports_segment_secs
--, max(CASE when segment_id = 'genre_sports_Extreme' THEN sov_decile_sum ELSE 0 END) as genre_sports_Extreme_sov	, max(CASE when segment_id = 'genre_sports_Extreme' THEN secs_decile_sum ELSE 0 END) as genre_sports_Extreme_total_secs	, max(CASE when segment_id = 'genre_sports_Extreme' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Extreme_segment_secs
--, max(CASE when segment_id = 'genre_sports_Rugby' THEN sov_decile_sum ELSE 0 END) as genre_sports_Rugby_sov	, max(CASE when segment_id = 'genre_sports_Rugby' THEN secs_decile_sum ELSE 0 END) as genre_sports_Rugby_total_secs	, max(CASE when segment_id = 'genre_sports_Rugby' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Rugby_segment_secs
--, max(CASE when segment_id = 'genre_sports_Darts' THEN sov_decile_sum ELSE 0 END) as genre_sports_Darts_sov	, max(CASE when segment_id = 'genre_sports_Darts' THEN secs_decile_sum ELSE 0 END) as genre_sports_Darts_total_secs	, max(CASE when segment_id = 'genre_sports_Darts' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Darts_segment_secs
--, max(CASE when segment_id = 'genre_sports_Watersports' THEN sov_decile_sum ELSE 0 END) as genre_sports_Watersports_sov	, max(CASE when segment_id = 'genre_sports_Watersports' THEN secs_decile_sum ELSE 0 END) as genre_sports_Watersports_total_secs	, max(CASE when segment_id = 'genre_sports_Watersports' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Watersports_segment_secs
--, max(CASE when segment_id = 'genre_entertainment_comedy' THEN sov_decile_sum ELSE 0 END) as genre_entertainment_comedy_sov	, max(CASE when segment_id = 'genre_entertainment_comedy' THEN secs_decile_sum ELSE 0 END) as genre_entertainment_comedy_total_secs	, max(CASE when segment_id = 'genre_entertainment_comedy' THEN segment_viewed_decile ELSE 0 END) as genre_entertainment_comedy_segment_secs
--, max(CASE when segment_id = 'channel_attheraces' THEN sov_decile_sum ELSE 0 END) as channel_attheraces_sov	, max(CASE when segment_id = 'channel_attheraces' THEN secs_decile_sum ELSE 0 END) as channel_attheraces_total_secs	, max(CASE when segment_id = 'channel_attheraces' THEN segment_viewed_decile ELSE 0 END) as channel_attheraces_segment_secs
--, max(CASE when segment_id = 'genre_specialist_Gaming' THEN sov_decile_sum ELSE 0 END) as genre_specialist_Gaming_sov	, max(CASE when segment_id = 'genre_specialist_Gaming' THEN secs_decile_sum ELSE 0 END) as genre_specialist_Gaming_total_secs	, max(CASE when segment_id = 'genre_specialist_Gaming' THEN segment_viewed_decile ELSE 0 END) as genre_specialist_Gaming_segment_secs
--, max(CASE when segment_id = 'channel_SuperCasino' THEN sov_decile_sum ELSE 0 END) as channel_SuperCasino_sov	, max(CASE when segment_id = 'channel_SuperCasino' THEN secs_decile_sum ELSE 0 END) as channel_SuperCasino_total_secs	, max(CASE when segment_id = 'channel_SuperCasino' THEN segment_viewed_decile ELSE 0 END) as channel_SuperCasino_segment_secs
--, max(CASE when segment_id = 'genre_sports_Fishing' THEN sov_decile_sum ELSE 0 END) as genre_sports_Fishing_sov	, max(CASE when segment_id = 'genre_sports_Fishing' THEN secs_decile_sum ELSE 0 END) as genre_sports_Fishing_total_secs	, max(CASE when segment_id = 'genre_sports_Fishing' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Fishing_segment_secs
--, max(CASE when segment_id = 'genre_sports_undefined' THEN sov_decile_sum ELSE 0 END) as genre_sports_undefined_sov	, max(CASE when segment_id = 'genre_sports_undefined' THEN secs_decile_sum ELSE 0 END) as genre_sports_undefined_total_secs	, max(CASE when segment_id = 'genre_sports_undefined' THEN segment_viewed_decile ELSE 0 END) as genre_sports_undefined_segment_secs
--, max(CASE when segment_id = 'genre_entertainment_Detective' THEN sov_decile_sum ELSE 0 END) as genre_entertainment_Detective_sov	, max(CASE when segment_id = 'genre_entertainment_Detective' THEN secs_decile_sum ELSE 0 END) as genre_entertainment_Detective_total_secs	, max(CASE when segment_id = 'genre_entertainment_Detective' THEN segment_viewed_decile ELSE 0 END) as genre_entertainment_Detective_segment_secs
--, max(CASE when segment_id = 'genre_sports_Baseball' THEN sov_decile_sum ELSE 0 END) as genre_sports_Baseball_sov	, max(CASE when segment_id = 'genre_sports_Baseball' THEN secs_decile_sum ELSE 0 END) as genre_sports_Baseball_total_secs	, max(CASE when segment_id = 'genre_sports_Baseball' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Baseball_segment_secs
--, max(CASE when segment_id = 'genre_sports_Basketball' THEN sov_decile_sum ELSE 0 END) as genre_sports_Basketball_sov	, max(CASE when segment_id = 'genre_sports_Basketball' THEN secs_decile_sum ELSE 0 END) as genre_sports_Basketball_total_secs	, max(CASE when segment_id = 'genre_sports_Basketball' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Basketball_segment_secs
--, max(CASE when segment_id = 'genre_sports_American Football' THEN sov_decile_sum ELSE 0 END) as genre_sports_American_Football_sov	, max(CASE when segment_id = 'genre_sports_American Football' THEN secs_decile_sum ELSE 0 END) as genre_sports_American_Football_total_secs	, max(CASE when segment_id = 'genre_sports_American Football' THEN segment_viewed_decile ELSE 0 END) as genre_sports_American_Football_segment_secs
--, max(CASE when segment_id = 'genre_sports_Wrestling' THEN sov_decile_sum ELSE 0 END) as genre_sports_Wrestling_sov	, max(CASE when segment_id = 'genre_sports_Wrestling' THEN secs_decile_sum ELSE 0 END) as genre_sports_Wrestling_total_secs	, max(CASE when segment_id = 'genre_sports_Wrestling' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Wrestling_segment_secs
--, max(CASE when segment_id = 'genre_sports_Wintersports' THEN sov_decile_sum ELSE 0 END) as genre_sports_Wintersports_sov	, max(CASE when segment_id = 'genre_sports_Wintersports' THEN secs_decile_sum ELSE 0 END) as genre_sports_Wintersports_total_secs	, max(CASE when segment_id = 'genre_sports_Wintersports' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Wintersports_segment_secs
--, max(CASE when segment_id = 'genre_sports_Equestrian' THEN sov_decile_sum ELSE 0 END) as genre_sports_Equestrian_sov	, max(CASE when segment_id = 'genre_sports_Equestrian' THEN secs_decile_sum ELSE 0 END) as genre_sports_Equestrian_total_secs	, max(CASE when segment_id = 'genre_sports_Equestrian' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Equestrian_segment_secs
--into V98_SkyBet_Final_Deciles2
--from V98_MainTable_SOV_Final
--group by account_number;
--COmmit;
--GRANT SELECT on V98_SkyBet_Final_Deciles2 to barbera, slaterc;
--GRANT SELECT on V98_MainTable_SOV_Final to barbera, slaterc;
--COmmit;

--------------------------------------------------------------Final Output Fix(Updated 22/10/2012)------------------------------------------------------------
--SET UP Deciling Grid or Deciling Net for all the three variables we developed - Updated 22/10/2012 
select account_number, max(CASE when segment_id = 'channel_racinguk' THEN sov_decile ELSE 0 END) as channel_racinguk_sov	, max(CASE when segment_id = 'channel_racinguk' THEN viewed_decile ELSE 0 END) as channel_racinguk_total_secs	, max(CASE when segment_id = 'channel_racinguk' THEN segment_viewed_decile ELSE 0 END) as channel_racinguk_segment_secs
, max(CASE when segment_id = 'genre_sports_Tennis' THEN sov_decile ELSE 0 END) as genre_sports_Tennis_sov	, max(CASE when segment_id = 'genre_sports_Tennis' THEN viewed_decile ELSE 0 END) as genre_sports_Tennis_total_secs	, max(CASE when segment_id = 'genre_sports_Tennis' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Tennis_segment_secs
, max(CASE when segment_id = 'genre_entertainment_Soaps' THEN sov_decile ELSE 0 END) as genre_entertainment_Soaps_sov	, max(CASE when segment_id = 'genre_entertainment_Soaps' THEN viewed_decile ELSE 0 END) as genre_entertainment_Soaps_total_secs	, max(CASE when segment_id = 'genre_entertainment_Soaps' THEN segment_viewed_decile ELSE 0 END) as genre_entertainment_Soaps_segment_secs
, max(CASE when segment_id = 'genre_sports_Boxing' THEN sov_decile ELSE 0 END) as genre_sports_Boxing_sov	, max(CASE when segment_id = 'genre_sports_Boxing' THEN viewed_decile ELSE 0 END) as genre_sports_Boxing_total_secs	, max(CASE when segment_id = 'genre_sports_Boxing' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Boxing_segment_secs
, max(CASE when segment_id = 'genre_entertainment' THEN sov_decile ELSE 0 END) as genre_entertainment_sov	, max(CASE when segment_id = 'genre_entertainment' THEN viewed_decile ELSE 0 END) as genre_entertainment_total_secs	, max(CASE when segment_id = 'genre_entertainment' THEN segment_viewed_decile ELSE 0 END) as genre_entertainment_segment_secs
, max(CASE when segment_id = 'channel_SmartLive' THEN sov_decile ELSE 0 END) as channel_SmartLive_sov	, max(CASE when segment_id = 'channel_SmartLive' THEN viewed_decile ELSE 0 END) as channel_SmartLive_total_secs	, max(CASE when segment_id = 'channel_SmartLive' THEN segment_viewed_decile ELSE 0 END) as channel_SmartLive_segment_secs
, max(CASE when segment_id = 'genre_entertainment_chatshow' THEN sov_decile ELSE 0 END) as genre_entertainment_chatshow_sov	, max(CASE when segment_id = 'genre_entertainment_chatshow' THEN viewed_decile ELSE 0 END) as genre_entertainment_chatshow_total_secs	, max(CASE when segment_id = 'genre_entertainment_chatshow' THEN segment_viewed_decile ELSE 0 END) as genre_entertainment_chatshow_segment_secs
, max(CASE when segment_id = 'genre_sports_Athletics' THEN sov_decile ELSE 0 END) as genre_sports_Athletics_sov	, max(CASE when segment_id = 'genre_sports_Athletics' THEN viewed_decile ELSE 0 END) as genre_sports_Athletics_total_secs	, max(CASE when segment_id = 'genre_sports_Athletics' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Athletics_segment_secs
, max(CASE when segment_id = 'genre_entertainment_gameshows' THEN sov_decile ELSE 0 END) as genre_entertainment_gameshows_sov	, max(CASE when segment_id = 'genre_entertainment_gameshows' THEN viewed_decile ELSE 0 END) as genre_entertainment_gameshows_total_secs	, max(CASE when segment_id = 'genre_entertainment_gameshows' THEN segment_viewed_decile ELSE 0 END) as genre_entertainment_gameshows_segment_secs
, max(CASE when segment_id = 'genre_sports_Ice Hockey' THEN sov_decile ELSE 0 END) as genre_sports_Ice_Hockey_sov	, max(CASE when segment_id = 'genre_sports_Ice Hockey' THEN viewed_decile ELSE 0 END) as genre_sports_Ice_Hockey_total_secs	, max(CASE when segment_id = 'genre_sports_Ice Hockey' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Ice_Hockey_segment_secs
, max(CASE when segment_id = 'genre_sports_racing' THEN sov_decile ELSE 0 END) as genre_sports_racing_sov	, max(CASE when segment_id = 'genre_sports_racing' THEN viewed_decile ELSE 0 END) as genre_sports_racing_total_secs	, max(CASE when segment_id = 'genre_sports_racing' THEN segment_viewed_decile ELSE 0 END) as genre_sports_racing_segment_secs
, max(CASE when segment_id = 'channel_SkyPokercom' THEN sov_decile ELSE 0 END) as channel_SkyPokercom_sov	, max(CASE when segment_id = 'channel_SkyPokercom' THEN viewed_decile ELSE 0 END) as channel_SkyPokercom_total_secs	, max(CASE when segment_id = 'channel_SkyPokercom' THEN segment_viewed_decile ELSE 0 END) as channel_SkyPokercom_segment_secs
, max(CASE when segment_id = 'genre_entertainment_Motors' THEN sov_decile ELSE 0 END) as genre_entertainment_Motors_sov	, max(CASE when segment_id = 'genre_entertainment_Motors' THEN viewed_decile ELSE 0 END) as genre_entertainment_Motors_total_secs	, max(CASE when segment_id = 'genre_entertainment_Motors' THEN segment_viewed_decile ELSE 0 END) as genre_entertainment_Motors_segment_secs
, max(CASE when segment_id = 'genre_sports_Motor Sport' THEN sov_decile ELSE 0 END) as genre_sports_Motor_Sport_sov	, max(CASE when segment_id = 'genre_sports_Motor Sport' THEN viewed_decile ELSE 0 END) as genre_sports_Motor_Sport_total_secs	, max(CASE when segment_id = 'genre_sports_Motor Sport' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Motor_Sport_segment_secs
, max(CASE when segment_id = 'genre_sports_Golf' THEN sov_decile ELSE 0 END) as genre_sports_Golf_sov	, max(CASE when segment_id = 'genre_sports_Golf' THEN viewed_decile ELSE 0 END) as genre_sports_Golf_total_secs	, max(CASE when segment_id = 'genre_sports_Golf' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Golf_segment_secs
, max(CASE when segment_id = 'genre_sports_football' THEN sov_decile ELSE 0 END) as genre_sports_football_sov	, max(CASE when segment_id = 'genre_sports_football' THEN viewed_decile ELSE 0 END) as genre_sports_football_total_secs	, max(CASE when segment_id = 'genre_sports_football' THEN segment_viewed_decile ELSE 0 END) as genre_sports_football_segment_secs
, max(CASE when segment_id = 'genre_sports_Cricket' THEN sov_decile ELSE 0 END) as genre_sports_Cricket_sov	, max(CASE when segment_id = 'genre_sports_Cricket' THEN viewed_decile ELSE 0 END) as genre_sports_Cricket_total_secs	, max(CASE when segment_id = 'genre_sports_Cricket' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Cricket_segment_secs
, max(CASE when segment_id = 'genre_sports_Snooker/Pool' THEN sov_decile ELSE 0 END) as genre_sports_Snooker_Pool_sov	, max(CASE when segment_id = 'genre_sports_Snooker/Pool' THEN viewed_decile ELSE 0 END) as genre_sports_Snooker_Pool_total_secs	, max(CASE when segment_id = 'genre_sports_Snooker/Pool' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Snooker_Pool_segment_secs
, max(CASE when segment_id = 'genre_entertainment_Drama' THEN sov_decile ELSE 0 END) as genre_entertainment_Drama_sov	, max(CASE when segment_id = 'genre_entertainment_Drama' THEN viewed_decile ELSE 0 END) as genre_entertainment_Drama_total_secs	, max(CASE when segment_id = 'genre_entertainment_Drama' THEN segment_viewed_decile ELSE 0 END) as genre_entertainment_Drama_segment_secs
, max(CASE when segment_id = 'genre_sports_other' THEN sov_decile ELSE 0 END) as genre_sports_other_sov	, max(CASE when segment_id = 'genre_sports_other' THEN viewed_decile ELSE 0 END) as genre_sports_other_total_secs	, max(CASE when segment_id = 'genre_sports_other' THEN segment_viewed_decile ELSE 0 END) as genre_sports_other_segment_secs
, max(CASE when segment_id = 'genre_sports' THEN sov_decile ELSE 0 END) as genre_sports_sov	, max(CASE when segment_id = 'genre_sports' THEN viewed_decile ELSE 0 END) as genre_sports_total_secs	, max(CASE when segment_id = 'genre_sports' THEN segment_viewed_decile ELSE 0 END) as genre_sports_segment_secs
, max(CASE when segment_id = 'genre_sports_Extreme' THEN sov_decile ELSE 0 END) as genre_sports_Extreme_sov	, max(CASE when segment_id = 'genre_sports_Extreme' THEN viewed_decile ELSE 0 END) as genre_sports_Extreme_total_secs	, max(CASE when segment_id = 'genre_sports_Extreme' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Extreme_segment_secs
, max(CASE when segment_id = 'genre_sports_Rugby' THEN sov_decile ELSE 0 END) as genre_sports_Rugby_sov	, max(CASE when segment_id = 'genre_sports_Rugby' THEN viewed_decile ELSE 0 END) as genre_sports_Rugby_total_secs	, max(CASE when segment_id = 'genre_sports_Rugby' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Rugby_segment_secs
, max(CASE when segment_id = 'genre_sports_Darts' THEN sov_decile ELSE 0 END) as genre_sports_Darts_sov	, max(CASE when segment_id = 'genre_sports_Darts' THEN viewed_decile ELSE 0 END) as genre_sports_Darts_total_secs	, max(CASE when segment_id = 'genre_sports_Darts' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Darts_segment_secs
, max(CASE when segment_id = 'genre_sports_Watersports' THEN sov_decile ELSE 0 END) as genre_sports_Watersports_sov	, max(CASE when segment_id = 'genre_sports_Watersports' THEN viewed_decile ELSE 0 END) as genre_sports_Watersports_total_secs	, max(CASE when segment_id = 'genre_sports_Watersports' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Watersports_segment_secs
, max(CASE when segment_id = 'genre_entertainment_comedy' THEN sov_decile ELSE 0 END) as genre_entertainment_comedy_sov	, max(CASE when segment_id = 'genre_entertainment_comedy' THEN viewed_decile ELSE 0 END) as genre_entertainment_comedy_total_secs	, max(CASE when segment_id = 'genre_entertainment_comedy' THEN segment_viewed_decile ELSE 0 END) as genre_entertainment_comedy_segment_secs
, max(CASE when segment_id = 'channel_attheraces' THEN sov_decile ELSE 0 END) as channel_attheraces_sov	, max(CASE when segment_id = 'channel_attheraces' THEN viewed_decile ELSE 0 END) as channel_attheraces_total_secs	, max(CASE when segment_id = 'channel_attheraces' THEN segment_viewed_decile ELSE 0 END) as channel_attheraces_segment_secs
, max(CASE when segment_id = 'genre_specialist_Gaming' THEN sov_decile ELSE 0 END) as genre_specialist_Gaming_sov	, max(CASE when segment_id = 'genre_specialist_Gaming' THEN viewed_decile ELSE 0 END) as genre_specialist_Gaming_total_secs	, max(CASE when segment_id = 'genre_specialist_Gaming' THEN segment_viewed_decile ELSE 0 END) as genre_specialist_Gaming_segment_secs
, max(CASE when segment_id = 'channel_SuperCasino' THEN sov_decile ELSE 0 END) as channel_SuperCasino_sov	, max(CASE when segment_id = 'channel_SuperCasino' THEN viewed_decile ELSE 0 END) as channel_SuperCasino_total_secs	, max(CASE when segment_id = 'channel_SuperCasino' THEN segment_viewed_decile ELSE 0 END) as channel_SuperCasino_segment_secs
, max(CASE when segment_id = 'genre_sports_Fishing' THEN sov_decile ELSE 0 END) as genre_sports_Fishing_sov	, max(CASE when segment_id = 'genre_sports_Fishing' THEN viewed_decile ELSE 0 END) as genre_sports_Fishing_total_secs	, max(CASE when segment_id = 'genre_sports_Fishing' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Fishing_segment_secs
, max(CASE when segment_id = 'genre_sports_undefined' THEN sov_decile ELSE 0 END) as genre_sports_undefined_sov	, max(CASE when segment_id = 'genre_sports_undefined' THEN viewed_decile ELSE 0 END) as genre_sports_undefined_total_secs	, max(CASE when segment_id = 'genre_sports_undefined' THEN segment_viewed_decile ELSE 0 END) as genre_sports_undefined_segment_secs
, max(CASE when segment_id = 'genre_entertainment_Detective' THEN sov_decile ELSE 0 END) as genre_entertainment_Detective_sov	, max(CASE when segment_id = 'genre_entertainment_Detective' THEN viewed_decile ELSE 0 END) as genre_entertainment_Detective_total_secs	, max(CASE when segment_id = 'genre_entertainment_Detective' THEN segment_viewed_decile ELSE 0 END) as genre_entertainment_Detective_segment_secs
, max(CASE when segment_id = 'genre_sports_Baseball' THEN sov_decile ELSE 0 END) as genre_sports_Baseball_sov	, max(CASE when segment_id = 'genre_sports_Baseball' THEN viewed_decile ELSE 0 END) as genre_sports_Baseball_total_secs	, max(CASE when segment_id = 'genre_sports_Baseball' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Baseball_segment_secs
, max(CASE when segment_id = 'genre_sports_Basketball' THEN sov_decile ELSE 0 END) as genre_sports_Basketball_sov	, max(CASE when segment_id = 'genre_sports_Basketball' THEN viewed_decile ELSE 0 END) as genre_sports_Basketball_total_secs	, max(CASE when segment_id = 'genre_sports_Basketball' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Basketball_segment_secs
, max(CASE when segment_id = 'genre_sports_American Football' THEN sov_decile ELSE 0 END) as genre_sports_American_Football_sov	, max(CASE when segment_id = 'genre_sports_American Football' THEN viewed_decile ELSE 0 END) as genre_sports_American_Football_total_secs	, max(CASE when segment_id = 'genre_sports_American Football' THEN segment_viewed_decile ELSE 0 END) as genre_sports_American_Football_segment_secs
, max(CASE when segment_id = 'genre_sports_Wrestling' THEN sov_decile ELSE 0 END) as genre_sports_Wrestling_sov	, max(CASE when segment_id = 'genre_sports_Wrestling' THEN viewed_decile ELSE 0 END) as genre_sports_Wrestling_total_secs	, max(CASE when segment_id = 'genre_sports_Wrestling' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Wrestling_segment_secs
, max(CASE when segment_id = 'genre_sports_Wintersports' THEN sov_decile ELSE 0 END) as genre_sports_Wintersports_sov	, max(CASE when segment_id = 'genre_sports_Wintersports' THEN viewed_decile ELSE 0 END) as genre_sports_Wintersports_total_secs	, max(CASE when segment_id = 'genre_sports_Wintersports' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Wintersports_segment_secs
, max(CASE when segment_id = 'genre_sports_Equestrian' THEN sov_decile ELSE 0 END) as genre_sports_Equestrian_sov	, max(CASE when segment_id = 'genre_sports_Equestrian' THEN viewed_decile ELSE 0 END) as genre_sports_Equestrian_total_secs	, max(CASE when segment_id = 'genre_sports_Equestrian' THEN segment_viewed_decile ELSE 0 END) as genre_sports_Equestrian_segment_secs
INTO V98_SkyBet_Final_Deciles
FROM V98_MainTable_SOV_Final
group by account_number;
COmmit;
--603336 row(s) affected, Execution time: 1787.435 seconds updated 22/10/2012

--------------
--Code 12 - Grants
--------------
-----------------------------------------------------------------
grant select on V98_CappedViewingtk to vespa_group_low_security; -- executed 04-10-2012
grant select on V98_Viewing_Table_SkyBetDates_Final to vespa_group_low_security; -- see ** for build
grant select on V98_Segment_Program_table2  to vespa_group_low_security; -- See *** for build
grant select on V98_SkyBet_Main_Raw_Table to vespa_group_low_security; --executed 04-10-2012
grant select on V98_Tot_Mins_Cap_Raw to vespa_group_low_security; --executed 04-10-2012

grant select on V098_AcctAvgSecs_raw_AllDates_pc_query_tbl to vespa_group_low_security; --executed 04-10-2012
grant select on V98_MainTable_SOV_Final to vespa_group_low_security; --executed 04-10-2012
GRANT SELECT on V98_SkyBet_Final_Deciles to vespa_group_low_security, barbera, slaterc;
GRANT SELECT on V98_MainTable_SOV_Final to vespa_group_low_security, barbera, slaterc;
GRANT SELECT on V098_AcctAvgSecs_raw_AllDates_pc_tbl to vespa_group_low_security;
GRANT SELECT on V098_AcctSegmentSOV_raw_AllDates_pc_tbl to vespa_group_low_security;
GRANT SELECT on V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl to vespa_group_low_security;
GRANT SELECT on AcctSecs_raw_aggregates_AllDates to vespa_group_low_security;
GRANT SELECT on AcctSegmentSecs_raw_aggregates_AllDates to vespa_group_low_security;
Commit;
GRANT SELECT on V98_SkyBet_Final_Deciles to barbera, slaterc;Commit;



---------
--Code 13 - Added Descriptive Statistics for SkyBET Project
---------
--SkyBet has requested the following according to Sarah Moore's email:
--1. Get average viewing seconds for the top 3 deciles dedicated to the genre-subgenre associated with that segment.
--2. Get the top channels watched by the top 3 deciles of a segment that is dedicated to the genre_sub_genre associated with that segment.
--3. Get the top programmes watched by the top 3 deciles of a segment that is dedicated to the genre_subgenre associated with that segment.

--In order to do sections 2 and 3, we need to create a new table V98_SkyBet_Main_Raw_Table2 (which is similar to V98_SkyBet_Main_Raw_Table except that it has added variables Programme_Name and Channel_Name).
SELECT b.account_number, a.segment_id, a.dk_programme_instance_dim, b.channel_name, b.programme_name, b.programme_instance_name, c.viewing_starts, CONVERT(date, b.broadcast_start_date_time_utc) as broadcast_date_utc, c.viewing_duration, b.programme_instance_duration, b.genre_description, b.sub_genre_description, b.pk_viewing_prog_instance_fact
Into V98_SkyBet_Main_Raw_Table2
FROM V98_Segment_Program_table2 AS a 
INNER JOIN V98_Viewing_Table_SkyBetDates_Final AS b
ON a.dk_programme_instance_dim=b.dk_programme_instance_dim
INNER JOIN V98_CappedViewingtk AS c
ON c.cb_row_id=b.pk_viewing_prog_instance_fact;
Commit; --244196292 row(s) affected, --Execution time: 2093.084 seconds

--Delete viewing of less than 7 seconds 
DELETE from V98_SkyBet_Main_Raw_Table2  
WHERE viewing_duration<7; Commit; --1978106 row(s) affected, Execution time: 48.14 seconds

CREATE INDEX RawTab2_idx_acct ON V98_SkyBet_Main_Raw_Table2 (account_number)

--Replace old table with new one but using the old name 
drop table V98_SkyBet_Main_Raw_Table;
SELECT * INTO V98_SkyBet_Main_Raw_Table
from V98_SkyBet_Main_Raw_Table2; commit;
--We ran checks on this and we verified equivalent number of rows, account numbers, and spot checks on specific accounts to make sure that thinkg like the keys and the duration line up with the accounts and the segments.

--Recreate the indexes
CREATE   INDEX RwTbl_Idx_Acct  ON V98_SkyBet_Main_Raw_Table  (account_number);
CREATE   INDEX RwTbl_Idx_viewst  ON V98_SkyBet_Main_Raw_Table (viewing_starts);
CREATE   INDEX RawTbl_Idx_brdcstdate  ON V98_SkyBet_Main_Raw_Table (broadcast_date_utc);
CREATE   INDEX RawTbl_Idx_dkproginsdim  ON V98_SkyBet_Main_Raw_Table (dk_programme_instance_dim);
CREATE   INDEX RawTbl_Idx_pk  ON V98_SkyBet_Main_Raw_Table(pk_viewing_prog_instance_fact); commit;

--We now need to create a list of account numbers for the top 3 deciles associated with the target segments. 
--(Update 05/11/2012 - these numbers has been updated. to the active script further below. NOte though that this was provided to the Sky BEt team and was the basis of the descriptives.)
--We will join this with the new V98_SkyBet_Main_Raw_Table to get the list of the top viewing by this segment for programmes and channels associated with the genre/subgenre
--Football Fans
--SELECT DISTINCT account_number
--INTO rombaoad.sports_footballtop3deciles
--FROM V98_MainTable_SOV_Final 
--where segment_id='genre_sports_football'
--AND SOV_Decile IN (8,9,10)
----93,058 accounts
--
----Gaming Fans
--SELECT DISTINCT account_number
--INTO rombaoad.specialist_gamingtop3deciles
--FROM V98_MainTable_SOV_Final 
--where segment_id='genre_specialist_Gaming'
--AND SOV_Decile IN (8,9,10)
----27,674 accounts
--
----Sports Other Fans
--SELECT DISTINCT account_number
--INTO rombaoad.sports_othertop3deciles
--FROM V98_MainTable_SOV_Final 
--where segment_id='genre_sports_other'
--AND SOV_Decile IN (8,9,10)
----88528 accounts
--
-----Racing
--SELECT DISTINCT account_number
--INTO rombaoad.sports_racingtop3deciles
--FROM V98_MainTable_SOV_Final 
--where segment_id='genre_sports_racing'
--AND SOV_Decile IN (8,9,10)
----11,662 accounts
--
----Now let's do a merge and create the descriptive statistics associated with football fans (top 3 deciles who watch football content) 
----Top Channels for football content
--SELECT b.channel_name
--        ,SUM(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Total_Viewing_Seconds
--        --,avg(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Avg_Viewing_Seconds (average was calculated using SUM divided by number of accounts in the top 3 deciles instead)
--        --,MIN(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Min_Viewing_Seconds
--    	,MAX(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Max_Viewing_Seconds
--FROM V98_SkyBet_Main_Raw_Table AS b
--INNER JOIN rombaoad.sports_footballtop3deciles AS a
--ON b.account_number=a.account_number
--WHERE segment_id='genre_sports_football'
--GROUP BY b.channel_name
--ORDER by Total_Viewing_Seconds desc;
--
----Top Programmes for football content
--SELECT b.programme_name
--        ,SUM(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Total_Viewing_Seconds
--        --,avg(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Avg_Viewing_Seconds (average was calculated using SUM divided by number of accounts in the top 3 deciles instead)
--        --,MIN(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Min_Viewing_Seconds
--    	,MAX(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Max_Viewing_Seconds
--FROM V98_SkyBet_Main_Raw_Table AS b
--INNER JOIN rombaoad.sports_footballtop3deciles AS a
--ON b.account_number=a.account_number
--WHERE segment_id='genre_sports_football'
--GROUP BY b.programme_name
--ORDER by Total_Viewing_Seconds desc;
--
----create the descriptive statistics associated with gaming fans (top 3 deciles who watch Gaming content)
----Top Channels for gaming content
--SELECT b.channel_name
--        ,SUM(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Total_Viewing_Seconds
--        --,avg(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Avg_Viewing_Seconds (average was calculated using SUM divided by number of accounts in the top 3 deciles instead)
--        --,MIN(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Min_Viewing_Seconds
--    	,MAX(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Max_Viewing_Seconds
--FROM V98_SkyBet_Main_Raw_Table AS b
--INNER JOIN rombaoad.specialist_gamingtop3deciles AS a
--ON b.account_number=a.account_number
--WHERE segment_id='genre_specialist_Gaming'
--GROUP BY b.channel_name
--ORDER by Total_Viewing_Seconds desc;
--
----Top Programmes for gaming content
--SELECT b.programme_name
--        ,SUM(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Total_Viewing_Seconds
--        --,avg(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Avg_Viewing_Seconds (average was calculated using SUM divided by number of accounts in the top 3 deciles instead)
--        --,MIN(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Min_Viewing_Seconds
--    	,MAX(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Max_Viewing_Seconds
--FROM V98_SkyBet_Main_Raw_Table AS b
--INNER JOIN rombaoad.specialist_gamingtop3deciles AS a
--ON b.account_number=a.account_number
--WHERE segment_id='genre_specialist_Gaming'
--GROUP BY b.programme_name
--ORDER by Total_Viewing_Seconds desc;
--
--
----Racing
------Top Channels for racing content
--SELECT b.channel_name
--        ,SUM(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Total_Viewing_Seconds
--        --,avg(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Avg_Viewing_Seconds (average was calculated using SUM divided by number of accounts in the top 3 deciles instead)
--        --,MIN(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Min_Viewing_Seconds
--    	,MAX(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Max_Viewing_Seconds
--FROM V98_SkyBet_Main_Raw_Table AS b
--INNER JOIN rombaoad.sports_racingtop3deciles AS a
--ON b.account_number=a.account_number
--WHERE segment_id='genre_sports_racing'
--GROUP BY b.channel_name
--ORDER by Total_Viewing_Seconds desc;
--
----Top Programmes for racing content
--SELECT b.programme_name
--        ,SUM(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Total_Viewing_Seconds
--        --,avg(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Avg_Viewing_Seconds (average was calculated using SUM divided by number of accounts in the top 3 deciles instead)
--        --,MIN(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Min_Viewing_Seconds
--    	,MAX(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Max_Viewing_Seconds
--FROM V98_SkyBet_Main_Raw_Table AS b
--INNER JOIN rombaoad.sports_racingtop3deciles AS a
--ON b.account_number=a.account_number
--WHERE segment_id='genre_sports_racing'
--GROUP BY b.programme_name
--ORDER by Total_Viewing_Seconds desc;
--
----Sports_Other
------Top Channels for Sports Other content
--SELECT b.channel_name
--        ,SUM(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Total_Viewing_Seconds
--        --,avg(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Avg_Viewing_Seconds (average was calculated using SUM divided by number of accounts in the top 3 deciles instead)
--        --,MIN(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Min_Viewing_Seconds
--    	,MAX(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Max_Viewing_Seconds
--FROM V98_SkyBet_Main_Raw_Table AS b
--INNER JOIN rombaoad.sports_othertop3deciles AS a
--ON b.account_number=a.account_number
--WHERE segment_id='genre_sports_other'
--GROUP BY b.channel_name
--ORDER by Total_Viewing_Seconds desc;
--
----Top Programmes for Sports Other content
--SELECT b.programme_name
--        ,SUM(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Total_Viewing_Seconds
--        --,avg(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Avg_Viewing_Seconds (average was calculated using SUM divided by number of accounts in the top 3 deciles instead)
--        --,MIN(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Min_Viewing_Seconds
--    	,MAX(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Max_Viewing_Seconds
--FROM V98_SkyBet_Main_Raw_Table AS b
--INNER JOIN rombaoad.sports_othertop3deciles AS a
--ON b.account_number=a.account_number
--WHERE segment_id='genre_sports_other'
--GROUP BY b.programme_name
--ORDER by Total_Viewing_Seconds desc;
--
----We now need to create the averages for the TOP 3 Deciles overall.
----Football-Content Average Viewing for the Top 3 Deciles of the Football Segment
--SELECT avg(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Avg_Viewing_Seconds
--FROM V98_SkyBet_Main_Raw_Table AS b
--INNER JOIN rombaoad.sports_footballtop3deciles AS a
--ON b.account_number=a.account_number
--WHERE segment_id='genre_sports_football'
----Avg_Viewing_Seconds
----437.4099464755604180
--
----Gaming-Content Average Viewing for the Top 3 Deciles of the Gaming Segment
--SELECT avg(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Avg_Viewing_Seconds
--FROM V98_SkyBet_Main_Raw_Table AS b
--INNER JOIN rombaoad.specialist_gamingtop3deciles AS a
--ON b.account_number=a.account_number
--WHERE segment_id='genre_specialist_Gaming'
----Avg_Viewing_Seconds
----1220.8461715167078243
--
----Racing-Content Average Viewing for the Top 3 Deciles of the Racing Segment
--SELECT avg(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Avg_Viewing_Seconds
--FROM V98_SkyBet_Main_Raw_Table AS b
--INNER JOIN rombaoad.sports_racingtop3deciles AS a
--ON b.account_number=a.account_number
--WHERE segment_id='genre_sports_racing'
----Avg_Viewing_Seconds
----398.2666176623408378
--
----Sports_Other-Content Average Viewing for the Top 3 Deciles of the Sports Other Segment
--SELECT avg(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Avg_Viewing_Seconds
--FROM V98_SkyBet_Main_Raw_Table AS b
--INNER JOIN rombaoad.sports_othertop3deciles AS a
--ON b.account_number=a.account_number
--WHERE segment_id='genre_sports_other'
----Avg_Viewing_Seconds
----346.5681425414476926

----- List of Account Numbers who are in the Top 3 Deciles associated with watching any given segment
SELECT DISTINCT account_number
INTO rombaoad.sports_racingtop3deciles
FROM V98_MainTable_SOV_Final 
where segment_id='genre_sports_racing'
AND SOV_Decile IN (8,9,10); commit;
--37929 row(s) affected

-----Racing
--SELECT DISTINCT account_number
--INTO rombaoad.sports_racingtop3deciles
--FROM V98_MainTable_SOV_Final 
--where segment_id='genre_sports_racing'
--AND SOV_Decile IN (8,9,10)
----11,662 accounts

SELECT DISTINCT account_number
INTO rombaoad.specialist_gamingtop3deciles
FROM V98_MainTable_SOV_Final 
where segment_id='genre_specialist_Gaming'
AND SOV_Decile IN (8,9,10); commit;
--37545 row(s) affected

----Gaming Fans
--SELECT DISTINCT account_number
--INTO rombaoad.specialist_gamingtop3deciles
--FROM V98_MainTable_SOV_Final 
--where segment_id='genre_specialist_Gaming'
--AND SOV_Decile IN (8,9,10)
----27,674 accounts

SELECT DISTINCT account_number
INTO rombaoad.sports_footballtop3deciles
FROM V98_MainTable_SOV_Final 
where segment_id='genre_sports_football'
AND SOV_Decile IN (8,9,10); commit;
--151347 row(s) affected

--Football Fans
--SELECT DISTINCT account_number
--INTO rombaoad.sports_footballtop3deciles
--FROM V98_MainTable_SOV_Final 
--where segment_id='genre_sports_football'
--AND SOV_Decile IN (8,9,10)
----93,058 accounts

--Sports Other Fans
SELECT DISTINCT account_number
INTO rombaoad.sports_othertop3deciles
FROM V98_MainTable_SOV_Final 
where segment_id='genre_sports_other'
AND SOV_Decile IN (8,9,10); commit;
--153167 row(s) affected

----Sports Other Fans
--SELECT DISTINCT account_number
--INTO rombaoad.sports_othertop3deciles
--FROM V98_MainTable_SOV_Final 
--where segment_id='genre_sports_other'
--AND SOV_Decile IN (8,9,10)
----88528 accounts

------List of account numbers who watched any give segment
SELECT DISTINCT account_number
INTO rombaoad.sports_footballalldeciles
FROM V98_MainTable_SOV_Final 
where segment_id='genre_sports_football'
AND SOV_Decile BETWEEN 1 and 10; commit;
--504493 row(s) affected

SELECT DISTINCT account_number
INTO rombaoad.sports_otheralldeciles
FROM V98_MainTable_SOV_Final 
where segment_id='genre_sports_other'
AND SOV_Decile BETWEEN 1 and 10; commit;
--510559 row(s) affected

SELECT DISTINCT account_number
INTO rombaoad.sports_racingalldeciles
FROM V98_MainTable_SOV_Final 
where segment_id='genre_sports_racing'
AND SOV_Decile BETWEEN 1 and 10; commit;
--126437 row(s) affected

SELECT DISTINCT account_number
INTO rombaoad.specialist_gamingalldeciles
FROM V98_MainTable_SOV_Final 
where segment_id='genre_specialist_Gaming'
AND SOV_Decile BETWEEN 1 and 10; commit;
--125153 row(s) affected

-------------------Top Channels for the Top 3 Deciles in any given segment
--Top Channels for football content
SELECT TOP 25 b.channel_name
        ,SUM(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Total_Viewing_Seconds
        --,avg(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Avg_Viewing_Seconds (average was calculated using SUM divided by number of accounts in the top 3 deciles instead)
        --,MIN(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Min_Viewing_Seconds
    	,MAX(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Max_Viewing_Seconds
FROM V98_SkyBet_Main_Raw_Table AS b
INNER JOIN rombaoad.sports_footballtop3deciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_sports_football'
GROUP BY b.channel_name
ORDER by Total_Viewing_Seconds desc;

--Top Channels for gaming content
SELECT top 50 b.channel_name
        ,SUM(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Total_Viewing_Seconds
        --,avg(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Avg_Viewing_Seconds (average was calculated using SUM divided by number of accounts in the top 3 deciles instead)
        --,MIN(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Min_Viewing_Seconds
    	,MAX(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Max_Viewing_Seconds
FROM V98_SkyBet_Main_Raw_Table AS b
INNER JOIN rombaoad.specialist_gamingtop3deciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_specialist_Gaming'
GROUP BY b.channel_name
ORDER by Total_Viewing_Seconds desc;

----Top Channels for racing content
SELECT top 25 b.channel_name
        ,SUM(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Total_Viewing_Seconds
        --,avg(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Avg_Viewing_Seconds (average was calculated using SUM divided by number of accounts in the top 3 deciles instead)
        --,MIN(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Min_Viewing_Seconds
    	,MAX(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Max_Viewing_Seconds
FROM V98_SkyBet_Main_Raw_Table AS b
INNER JOIN rombaoad.sports_racingtop3deciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_sports_racing'
GROUP BY b.channel_name
ORDER by Total_Viewing_Seconds desc;

----Top Channels for Sports Other content
SELECT top 25 b.channel_name
        ,SUM(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Total_Viewing_Seconds
        --,avg(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Avg_Viewing_Seconds (average was calculated using SUM divided by number of accounts in the top 3 deciles instead)
        --,MIN(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Min_Viewing_Seconds
    	,MAX(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Max_Viewing_Seconds
FROM V98_SkyBet_Main_Raw_Table AS b
INNER JOIN rombaoad.sports_othertop3deciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_sports_other'
GROUP BY b.channel_name
ORDER by Total_Viewing_Seconds desc;


-------------------Top Programmes for the Top 3 Deciles in any given segment

--Top Programmes for football content
SELECT TOP 25 b.programme_name
        ,SUM(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Total_Viewing_Seconds
        --,avg(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Avg_Viewing_Seconds (average was calculated using SUM divided by number of accounts in the top 3 deciles instead)
        --,MIN(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Min_Viewing_Seconds
    	,MAX(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Max_Viewing_Seconds
FROM V98_SkyBet_Main_Raw_Table AS b
INNER JOIN rombaoad.sports_footballtop3deciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_sports_football'
GROUP BY b.programme_name
ORDER by Total_Viewing_Seconds desc;

--Top Programmes for gaming content
SELECT top 50 b.programme_name
        ,SUM(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Total_Viewing_Seconds
        --,avg(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Avg_Viewing_Seconds (average was calculated using SUM divided by number of accounts in the top 3 deciles instead)
        --,MIN(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Min_Viewing_Seconds
    	,MAX(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Max_Viewing_Seconds
FROM V98_SkyBet_Main_Raw_Table AS b
INNER JOIN rombaoad.specialist_gamingtop3deciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_specialist_Gaming'
GROUP BY b.programme_name
ORDER by Total_Viewing_Seconds desc;

--Top Programmes for racing content
SELECT top 25 b.programme_name
        ,SUM(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Total_Viewing_Seconds
        --,avg(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Avg_Viewing_Seconds (average was calculated using SUM divided by number of accounts in the top 3 deciles instead)
        --,MIN(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Min_Viewing_Seconds
    	,MAX(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Max_Viewing_Seconds
FROM V98_SkyBet_Main_Raw_Table AS b
INNER JOIN rombaoad.sports_racingtop3deciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_sports_racing'
GROUP BY b.programme_name
ORDER by Total_Viewing_Seconds desc;

--Top Programmes for Sports Other content
SELECT top 25 b.programme_name
        ,SUM(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Total_Viewing_Seconds
        --,avg(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Avg_Viewing_Seconds (average was calculated using SUM divided by number of accounts in the top 3 deciles instead)
        --,MIN(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Min_Viewing_Seconds
    	,MAX(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Max_Viewing_Seconds
FROM V98_SkyBet_Main_Raw_Table AS b
INNER JOIN rombaoad.sports_othertop3deciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_sports_other'
GROUP BY b.programme_name
ORDER by Total_Viewing_Seconds desc;


-------------------Descriptives for Top 3 Deciles in any given segment

--More descriptives - football
    SELECT SUM(b.viewing_duration)as Total_Viewing_Seconds
		,avg (b.viewing_duration)/60 -- average viewing for everytime that segment is watched 
		,Total_Viewing_Seconds/60 AS Total_Viewing_Minutes
        ,count (distinct b.account_number) AS distinct_acct_numbers
        ,Total_Viewing_Minutes/distinct_acct_numbers AS AvgViewingMinutesAllDates
        ,AvgViewingMinutesAllDates/14 AS AvgViewingMinutesPerDay -- average viewing for each account per day
,COUNT (DISTINCT channel_name)
,COUNT(distinct dk_programme_instance_dim)
    FROM V98_SkyBet_Main_Raw_Table AS b
         INNER JOIN rombaoad.sports_footballtop3deciles AS a ON b.account_number=a.account_number
   WHERE b.segment_id='genre_sports_football';
   
--More descriptives - sports other  
       SELECT SUM(b.viewing_duration)as Total_Viewing_Seconds
		,avg (b.viewing_duration)/60 -- average viewing for everytime that segment is watched 
		,Total_Viewing_Seconds/60 AS Total_Viewing_Minutes
        ,count (distinct b.account_number) AS distinct_acct_numbers
        ,Total_Viewing_Minutes/distinct_acct_numbers AS AvgViewingMinutesAllDates
        ,AvgViewingMinutesAllDates/14 AS AvgViewingMinutesPerDay -- average viewing for each account per day
,COUNT (DISTINCT channel_name)
,COUNT(distinct dk_programme_instance_dim)
    FROM V98_SkyBet_Main_Raw_Table AS b
         INNER JOIN rombaoad.sports_othertop3deciles  AS a ON b.account_number=a.account_number
   WHERE b.segment_id='genre_sports_other';
   
--More descriptives - sports racing  
       SELECT SUM(b.viewing_duration)as Total_Viewing_Seconds
		,avg (b.viewing_duration)/60 -- average viewing for everytime that segment is watched 
		,Total_Viewing_Seconds/60 AS Total_Viewing_Minutes
        ,count (distinct b.account_number) AS distinct_acct_numbers
        ,Total_Viewing_Minutes/distinct_acct_numbers AS AvgViewingMinutesAllDates
        ,AvgViewingMinutesAllDates/14 AS AvgViewingMinutesPerDay -- average viewing for each account per day
,COUNT (DISTINCT channel_name)
,COUNT(distinct dk_programme_instance_dim)
    FROM V98_SkyBet_Main_Raw_Table AS b
         INNER JOIN rombaoad.sports_racingtop3deciles AS a ON b.account_number=a.account_number
   WHERE b.segment_id='genre_sports_racing';

--More descriptives - gaming
       SELECT SUM(b.viewing_duration)as Total_Viewing_Seconds
		,avg (b.viewing_duration)/60 -- average viewing for everytime that segment is watched 
		,Total_Viewing_Seconds/60 AS Total_Viewing_Minutes
        ,count (distinct b.account_number) AS distinct_acct_numbers
        ,Total_Viewing_Minutes/distinct_acct_numbers AS AvgViewingMinutesAllDates
        ,AvgViewingMinutesAllDates/14 AS AvgViewingMinutesPerDay -- average viewing for each account per day
,COUNT (DISTINCT channel_name)
,COUNT(distinct dk_programme_instance_dim)
    FROM V98_SkyBet_Main_Raw_Table AS b
         INNER JOIN rombaoad.specialist_gamingtop3deciles AS a ON b.account_number=a.account_number
   WHERE b.segment_id='genre_specialist_Gaming';

-------------------Descriptives for All Deciles in any given segment

--More descriptives - sports other  
       SELECT SUM(b.viewing_duration)as Total_Viewing_Seconds
		,avg (b.viewing_duration)/60 -- average viewing for everytime that segment is watched 
		,Total_Viewing_Seconds/60 AS Total_Viewing_Minutes
        ,count (distinct b.account_number) AS distinct_acct_numbers
        ,Total_Viewing_Minutes/distinct_acct_numbers AS AvgViewingMinutesAllDates
        ,AvgViewingMinutesAllDates/14 AS AvgViewingMinutesPerDay -- average viewing for each account per day
,COUNT (DISTINCT channel_name)
,COUNT(distinct dk_programme_instance_dim)
    FROM V98_SkyBet_Main_Raw_Table AS b
         INNER JOIN rombaoad.sports_otheralldeciles  AS a ON b.account_number=a.account_number
   WHERE b.segment_id='genre_sports_other';
   
--More descriptives - sports racing  
       SELECT SUM(b.viewing_duration)as Total_Viewing_Seconds
		,avg (b.viewing_duration)/60 -- average viewing for everytime that segment is watched 
		,Total_Viewing_Seconds/60 AS Total_Viewing_Minutes
        ,count (distinct b.account_number) AS distinct_acct_numbers
        ,Total_Viewing_Minutes/distinct_acct_numbers AS AvgViewingMinutesAllDates
        ,AvgViewingMinutesAllDates/14 AS AvgViewingMinutesPerDay -- average viewing for each account per day
,COUNT (DISTINCT channel_name)
,COUNT(distinct dk_programme_instance_dim)
    FROM V98_SkyBet_Main_Raw_Table AS b
         INNER JOIN rombaoad.sports_racingalldeciles AS a ON b.account_number=a.account_number
   WHERE b.segment_id='genre_sports_racing';

--More descriptives - gaming
       SELECT SUM(b.viewing_duration)as Total_Viewing_Seconds
		,avg (b.viewing_duration)/60 -- average viewing for everytime that segment is watched 
		,Total_Viewing_Seconds/60 AS Total_Viewing_Minutes
        ,count (distinct b.account_number) AS distinct_acct_numbers
        ,Total_Viewing_Minutes/distinct_acct_numbers AS AvgViewingMinutesAllDates
        ,AvgViewingMinutesAllDates/14 AS AvgViewingMinutesPerDay -- average viewing for each account per day
,COUNT (DISTINCT channel_name)
,COUNT(distinct dk_programme_instance_dim)
    FROM V98_SkyBet_Main_Raw_Table AS b
         INNER JOIN rombaoad.specialist_gamingalldeciles AS a ON b.account_number=a.account_number
   WHERE b.segment_id='genre_specialist_Gaming';

--More descriptives - football
    SELECT SUM(b.viewing_duration)as Total_Viewing_Seconds
		,avg (b.viewing_duration)/60 -- average viewing for everytime that segment is watched 
		,Total_Viewing_Seconds/60 AS Total_Viewing_Minutes
        ,count (distinct b.account_number) AS distinct_acct_numbers
        ,Total_Viewing_Minutes/distinct_acct_numbers AS AvgViewingMinutesAllDates
        ,AvgViewingMinutesAllDates/14 AS AvgViewingMinutesPerDay -- average viewing for each account per day
,COUNT (DISTINCT channel_name)
,COUNT(distinct dk_programme_instance_dim)
    FROM V98_SkyBet_Main_Raw_Table AS b
         INNER JOIN rombaoad.sports_footballalldeciles AS a ON b.account_number=a.account_number
   WHERE b.segment_id='genre_sports_football';

----------------------------------------
   
   
---------
--Code 14 - Pen Portraits 
---------
--We need to get an idea about what are the boundaries for each segment and each decile within the segment
SELECT t.segment_id, t.SOV_Decile, min (t.AcctSegmentSOV_raw_AllDates) AS Min_SOV, max (t.AcctSegmentSOV_raw_AllDates) AS Max_SOV,  sum(t.AcctSegmentSOV_raw_AllDates) Sum_SOV,  count (distinct t.account_number) AS distinct_acct_numbers, Sum_SOV/distinct_acct_numbers AS Average_SOV 
FROM (select distinct segment_id, account_number, SOV_Decile, AcctSegmentSOV_raw_AllDates 
from V98_MainTable_SOV_Final) as t
GROUP BY t.segment_id, t.SOV_Decile 
order by t.segment_id, t.SOV_Decile desc

--Getting the average SOV for all deciles requires us to create another temporary table.
--That will house each account number's consumption of a segment FOR THE ENTIRE 2-week period
--As the V98_MainTable_SOV_Final is still a daily table, and that we have already aggregated the sums within that table, to make it easier on us and to free ourselves from any confusion and duplication errors, we will create the table below:
SELECT DISTINCT account_number, segment_id, AcctSegmentSOV_raw_AllDates, SOV_Decile
into #dr
From V98_MainTable_SOV_Final

--and now we can run the averages appropriately without any duplication 
select segment_id, avg(AcctSegmentSOV_raw_AllDates)
FROM #dr
GROUP BY segment_id
ORDER BY segment_id

---------------------Daily Viewing Hours Consumed
-----Total Viewing Time for the Top 3 Deciles who watched any given segment broken down by day. 
--Football Top 3 Deciles Viewing Time
select broadcast_date_utc, sum (viewing_duration)/3600 AS Total_Viewing_Hrs
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.sports_footballtop3deciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_sports_football'
GROUP by broadcast_date_utc 
ORDER by broadcast_date_utc;

--Sports Other Top 3 Deciles Viewing Time
select broadcast_date_utc, sum (viewing_duration)/3600 AS Total_Viewing_Hrs
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.sports_othertop3deciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_sports_other'
GROUP by broadcast_date_utc 
ORDER by broadcast_date_utc;

--Gaming Top 3 Deciles Viewing Time
select broadcast_date_utc, sum (viewing_duration)/3600 AS Total_Viewing_Hrs
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.specialist_gamingtop3deciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_specialist_Gaming'
GROUP by broadcast_date_utc 
ORDER by broadcast_date_utc;

--Racing Top 3 Deciles Viewing Time
select broadcast_date_utc, sum (viewing_duration)/3600 AS Total_Viewing_Hrs
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.sports_racingtop3deciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_sports_racing'
GROUP by broadcast_date_utc 
ORDER by broadcast_date_utc;
commit;

-----Total Viewing Time for all people who watched any given segment broken down by day.
--This will help us capture the relative consuming power of that Top 3 Deciles group because we can do share of consumption or indexing.
--All Football Viewing Hours 
select broadcast_date_utc, sum (viewing_duration)/3600 AS Total_Viewing_Hrs
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.sports_footballalldeciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_sports_football'
GROUP by broadcast_date_utc 
ORDER by broadcast_date_utc;

--All Sports Other Viewing Hours 
select broadcast_date_utc, sum (viewing_duration)/3600 AS Total_Viewing_Hrs
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.sports_otheralldeciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_sports_other'
GROUP by broadcast_date_utc 
ORDER by broadcast_date_utc;

--All Gaming Viewing Hours 
select broadcast_date_utc, sum (viewing_duration)/3600 AS Total_Viewing_Hrs
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.specialist_gamingalldeciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_specialist_Gaming'
GROUP by broadcast_date_utc 
ORDER by broadcast_date_utc;

--All Racing Viewing Hours 
select broadcast_date_utc, sum (viewing_duration)/3600 AS Total_Viewing_Hrs
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.sports_racingalldeciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_sports_racing'
GROUP by broadcast_date_utc 
ORDER by broadcast_date_utc;

-------------------Daily Unique Viewers
-----We are interested in capturing the number of fans (top 3 deciles) that watch any given segment on a daily basis
--Racing Top 3 Deciles Unique Viewers
select b.broadcast_date_utc, COUNT (DISTINCT a.account_number) AS Unique_Viewers
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.sports_racingtop3deciles AS a
ON b.account_number=a.account_number
WHERE b.segment_id='genre_sports_racing'
GROUP by b.broadcast_date_utc 
ORDER by b.broadcast_date_utc;
commit;

--Football Top 3 Deciles Unique Viewers
select b.broadcast_date_utc, COUNT (DISTINCT a.account_number) AS Unique_Viewers
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.sports_footballtop3deciles AS a
ON b.account_number=a.account_number
WHERE b.segment_id='genre_sports_football'
GROUP by b.broadcast_date_utc 
ORDER by b.broadcast_date_utc;
commit;

--Sports Other Top 3 Deciles Unique Viewers
select b.broadcast_date_utc, COUNT (DISTINCT a.account_number) AS Unique_Viewers
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.sports_othertop3deciles AS a
ON b.account_number=a.account_number
WHERE b.segment_id='genre_sports_other'
GROUP by b.broadcast_date_utc 
ORDER by b.broadcast_date_utc;

--Gaming Top 3 Deciles Unique Viewers
select b.broadcast_date_utc, COUNT (DISTINCT a.account_number) AS Unique_Viewers
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.specialist_gamingtop3deciles AS a
ON b.account_number=a.account_number
WHERE b.segment_id='genre_specialist_Gaming'
GROUP by b.broadcast_date_utc 
ORDER by b.broadcast_date_utc;
commit;

------Total Unique Viewers
--We are interested in capturing the number of people that watch any given segment on a daily basis. 
--This will help us capture the relative consuming power of that Top 3 Deciles group because we can do share of consumption or indexing.

--Total Racing Unique Viewers
select b.broadcast_date_utc, COUNT (DISTINCT a.account_number) AS Unique_Viewers
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.sports_racingalldeciles AS a
ON b.account_number=a.account_number
WHERE b.segment_id='genre_sports_racing'
GROUP by b.broadcast_date_utc 
ORDER by b.broadcast_date_utc;

--Total Football Unique Viewers
select b.broadcast_date_utc, COUNT (DISTINCT a.account_number) AS Unique_Viewers
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.sports_footballalldeciles AS a
ON b.account_number=a.account_number
WHERE b.segment_id='genre_sports_football'
GROUP by b.broadcast_date_utc 
ORDER by b.broadcast_date_utc;

--Total Gaming Unique Viewers
select b.broadcast_date_utc, COUNT (DISTINCT a.account_number) AS Unique_Viewers
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.specialist_gamingalldeciles AS a
ON b.account_number=a.account_number
WHERE b.segment_id='genre_specialist_Gaming'
GROUP by b.broadcast_date_utc 
ORDER by b.broadcast_date_utc;

--Total Sports Other Unique Viewers
select b.broadcast_date_utc, COUNT (DISTINCT a.account_number) AS Unique_Viewers
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.sports_otheralldeciles AS a
ON b.account_number=a.account_number
WHERE b.segment_id='genre_sports_other'
GROUP by b.broadcast_date_utc 
ORDER by b.broadcast_date_utc;


--------------------Daily Distinct Programme Consumption
-------These are the number of specific programs consumed by the top 3 deciles
--Football Top 3 Deciles Programs Watched
select broadcast_date_utc, count (DISTINCT dk_programme_instance_dim) AS Programs_Watched
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.sports_footballtop3deciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_sports_football'
GROUP by broadcast_date_utc 
ORDER by broadcast_date_utc;

--Sports Other Top 3 Programs Watched
select broadcast_date_utc, count (DISTINCT dk_programme_instance_dim) AS Programs_Watched
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.sports_othertop3deciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_sports_other'
GROUP by broadcast_date_utc 
ORDER by broadcast_date_utc;

--Gaming Top 3 Programs Watched
select broadcast_date_utc, count (DISTINCT dk_programme_instance_dim) AS Programs_Watched
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.specialist_gamingtop3deciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_specialist_Gaming'
GROUP by broadcast_date_utc 
ORDER by broadcast_date_utc;

--Racing Top 3 Programs Watched
select broadcast_date_utc, count (DISTINCT dk_programme_instance_dim) AS Programs_Watched
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.sports_racingtop3deciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_sports_racing'
GROUP by broadcast_date_utc 
ORDER by broadcast_date_utc;

-----Total Distinct Programme Consumption for all people who watched any given segment broken down by day.
--This will help us capture the relative consuming power of that Top 3 Deciles group because we can do share of consumption or indexing.
----Football Distinct Programs Watched
select broadcast_date_utc, count (DISTINCT dk_programme_instance_dim) AS Programs_Watched
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.sports_footballalldeciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_sports_football'
GROUP by broadcast_date_utc 
ORDER by broadcast_date_utc;

--Sports Other Distinct Programs Watched
select broadcast_date_utc, count (DISTINCT dk_programme_instance_dim) AS Programs_Watched
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.sports_otheralldeciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_sports_other'
GROUP by broadcast_date_utc 
ORDER by broadcast_date_utc;

--Gaming Distinct Programs Watched
select broadcast_date_utc, count (DISTINCT dk_programme_instance_dim) AS Programs_Watched
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.specialist_gamingalldeciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_specialist_Gaming'
GROUP by broadcast_date_utc 
ORDER by broadcast_date_utc;

--Racing Distinct Programs Watched
select broadcast_date_utc, count (DISTINCT dk_programme_instance_dim) AS Programs_Watched
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.sports_racingalldeciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_sports_racing'
GROUP by broadcast_date_utc 
ORDER by broadcast_date_utc;

--------------------Daily Total Programme Consumption
--This relates to the number of visits to any programme watched in any given segment. 
--Unlike the Distinct Programme Consumption, if an account returns to the same program three times, each instance was counted as overall exposure to the program.
--This first section looks at the Top 3 Deciles consumption for programmes.

--Football Programmes watched
select broadcast_date_utc, count (dk_programme_instance_dim) AS Programs_Watched
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.sports_footballtop3deciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_sports_football'
GROUP by broadcast_date_utc 
ORDER by broadcast_date_utc;

--Sports Other Programmes watched
select broadcast_date_utc, count (dk_programme_instance_dim) AS Programs_Watched
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.sports_othertop3deciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_sports_other'
GROUP by broadcast_date_utc 
ORDER by broadcast_date_utc;

--Gaming Programmes watched
select broadcast_date_utc, count (dk_programme_instance_dim) AS Programs_Watched
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.specialist_gamingtop3deciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_specialist_Gaming'
GROUP by broadcast_date_utc 
ORDER by broadcast_date_utc;

--Racing Programmes watched
select broadcast_date_utc, count (dk_programme_instance_dim) AS Programs_Watched
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.sports_racingtop3deciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_sports_racing'
GROUP by broadcast_date_utc 
ORDER by broadcast_date_utc;
commit;

------Total programmes consumed by the entire segment. 
--This will help us capture the relative consuming power of that Top 3 Deciles group because we can do share of consumption or indexing.

--Football Total Programs Watched
select broadcast_date_utc, count (dk_programme_instance_dim) AS Programs_Watched
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.sports_footballalldeciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_sports_football'
GROUP by broadcast_date_utc 
ORDER by broadcast_date_utc;

--Other Deciles Total Programs Watched
select broadcast_date_utc, count (dk_programme_instance_dim) AS Programs_Watched
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.sports_otheralldeciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_sports_other'
GROUP by broadcast_date_utc 
ORDER by broadcast_date_utc;

--Gaming Total Programs Watched
select broadcast_date_utc, count (dk_programme_instance_dim) AS Programs_Watched
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.specialist_gamingalldeciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_specialist_Gaming'
GROUP by broadcast_date_utc 
ORDER by broadcast_date_utc;

--Racing Total Programs Watched
select broadcast_date_utc, count (dk_programme_instance_dim) AS Programs_Watched
FROM V98_SkyBet_Main_Raw_Table as b
INNER JOIN rombaoad.sports_racingalldeciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_sports_racing'
GROUP by broadcast_date_utc 
ORDER by broadcast_date_utc;


ALTER TABLE V98_MainTable_SOV_Final add SOV_OLD integer null
SELECT  --an option for day/time maybe required here,
        segment_id,
        PERCENTILE_CONT(0) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_100,
        PERCENTILE_CONT(0.1) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_90,
        PERCENTILE_CONT(0.2) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_80,
        PERCENTILE_CONT(0.3) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_70,
        PERCENTILE_CONT(0.4) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_60,
        PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_50,
        PERCENTILE_CONT(0.6) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_40,
        PERCENTILE_CONT(0.7) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_30,
        PERCENTILE_CONT(0.8) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_20,
        PERCENTILE_CONT(0.9) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_10,
        PERCENTILE_CONT(1) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_0
  INTO  select * from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2
  FROM   rombaoad.V98_MainTable_SOV_Final
  GROUP BY --an option for day/time maybe required here,
        segment_id;
commit;

select segment_id, 10 'decile', pc_90 pc_start, pc_100 pc_end
  into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
commit;
insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
select segment_id, 9 'decile', pc_80 pc_start, pc_90 pc_end
  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
select segment_id, 8 'decile', pc_70 pc_start, pc_80 pc_end
  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
select segment_id, 7 'decile', pc_60 pc_start, pc_70 pc_end
  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
select segment_id, 6 'decile', pc_50 pc_start, pc_60 pc_end
  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
select segment_id, 5 'decile', pc_40 pc_start, pc_50 pc_end
  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
select segment_id, 4 'decile', pc_30 pc_start, pc_40 pc_end
  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
select segment_id, 3 'decile', pc_20 pc_start, pc_30 pc_end
  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
select segment_id, 2 'decile', pc_10 pc_start, pc_20 pc_end
  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
insert into select * from  V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
select segment_id, 1 'decile', pc_0 pc_start, pc_10 pc_end
  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
commit;

UPDATE select * from V98_MainTable_SOV_Final order by 1, 2
SET  f.SOV_OLD=sov.decile
FROM V98_MainTable_SOV_Final f
INNER JOIN V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2 sov
ON f.segment_id = sov.segment_id
AND f.AcctSegmentSOV_raw_AllDates BETWEEN pc_start and pc_end;
Commit;
-----------------------------------------------------------

--Create temp of football top 3 deciles in the SOV Old
SELECT DISTINCT account_number
INTO #tempsports_footballtop3deciles
FROM V98_MainTable_SOV_Final 
where segment_id='genre_sports_football'
AND SOV_OLD IN (8,9,10)
--93,058

--Create temp of racing top 3 deciles in the SOV Old
SELECT DISTINCT account_number
INTO #tempsports_racingtop3deciles
FROM V98_MainTable_SOV_Final 
where segment_id='genre_sports_racing'
AND SOV_OLD IN (8,9,10)
--11,663

--Create temp of sports other top 3 deciles in the SOV Old
SELECT DISTINCT account_number
INTO #tempsports_othertop3deciles
FROM V98_MainTable_SOV_Final 
where segment_id='genre_sports_other'
AND SOV_OLD IN (8,9,10)
--88528 accounts

----Create temp of gaming top 3 deciles in the SOV Old
SELECT DISTINCT account_number
INTO #tempspecialist_gamingtop3deciles
FROM V98_MainTable_SOV_Final 
where segment_id='genre_specialist_Gaming'
AND SOV_OLD IN (8,9,10)
--27,674 accounts



------------------------------------------------------Impact of Old_SOV_Decile and New SOV_Decile--------------------------------
----- List of Account Numbers who are in the Top 3 Deciles associated with watching any given segment
SELECT DISTINCT account_number
INTO rombaoad.sports_racingtop3deciles
FROM V98_MainTable_SOV_Final 
where segment_id='genre_sports_racing'
AND SOV_Decile IN (8,9,10); commit;
--37929 row(s) affected

-----Racing
--SELECT DISTINCT account_number
--INTO rombaoad.sports_racingtop3deciles
--FROM V98_MainTable_SOV_Final 
--where segment_id='genre_sports_racing'
--AND SOV_Decile IN (8,9,10)
----11,662 accounts

SELECT DISTINCT account_number
INTO rombaoad.specialist_gamingtop3deciles
FROM V98_MainTable_SOV_Final 
where segment_id='genre_specialist_Gaming'
AND SOV_Decile IN (8,9,10); commit;
--37545 row(s) affected

----Gaming Fans
--SELECT DISTINCT account_number
--INTO rombaoad.specialist_gamingtop3deciles
--FROM V98_MainTable_SOV_Final 
--where segment_id='genre_specialist_Gaming'
--AND SOV_Decile IN (8,9,10)
----27,674 accounts

SELECT DISTINCT account_number
INTO rombaoad.sports_footballtop3deciles
FROM V98_MainTable_SOV_Final 
where segment_id='genre_sports_football'
AND SOV_Decile IN (8,9,10); commit;
--151347 row(s) affected

--Football Fans
--SELECT DISTINCT account_number
--INTO rombaoad.sports_footballtop3deciles
--FROM V98_MainTable_SOV_Final 
--where segment_id='genre_sports_football'
--AND SOV_Decile IN (8,9,10)
----93,058 accounts

--Sports Other Fans
SELECT DISTINCT account_number
INTO rombaoad.sports_othertop3deciles
FROM V98_MainTable_SOV_Final 
where segment_id='genre_sports_other'
AND SOV_Decile IN (8,9,10); commit;
--153167 row(s) affected

----Sports Other Fans
--SELECT DISTINCT account_number
--INTO rombaoad.sports_othertop3deciles
--FROM V98_MainTable_SOV_Final 
--where segment_id='genre_sports_other'
--AND SOV_Decile IN (8,9,10)
----88528 accounts

------List of account numbers who watched any give segment
SELECT DISTINCT account_number
INTO rombaoad.sports_footballalldeciles
FROM V98_MainTable_SOV_Final 
where segment_id='genre_sports_football'
AND SOV_Decile BETWEEN 1 and 10; commit;
--504493 row(s) affected

SELECT DISTINCT account_number
INTO rombaoad.sports_otheralldeciles
FROM V98_MainTable_SOV_Final 
where segment_id='genre_sports_other'
AND SOV_Decile BETWEEN 1 and 10; commit;
--510559 row(s) affected

SELECT DISTINCT account_number
INTO rombaoad.sports_racingalldeciles
FROM V98_MainTable_SOV_Final 
where segment_id='genre_sports_racing'
AND SOV_Decile BETWEEN 1 and 10; commit;
--126437 row(s) affected

SELECT DISTINCT account_number
INTO rombaoad.specialist_gamingalldeciles
FROM V98_MainTable_SOV_Final 
where segment_id='genre_specialist_Gaming'
AND SOV_Decile BETWEEN 1 and 10; commit;
--125153 row(s) affected

----------------------------------------IMPACt - decile movements of top 3 deciles

select DISTINCT F.account_number, segment_id, SOV_Decile, SOV_OLD, SOV_Decile-SOV_OLD as SOVFootballDiff 
FROM V98_MainTable_SOV_Final as F 
INNER JOIN #tempsports_footballtop3deciles as temp
ON temp.account_number=F.account_number
WHERE SOV_OLD in (8,9,10)
AND segment_id='genre_sports_football'
ORDER BY 5 desc
--this shows that there is not much change in the accounts in the top 3 deciles for football. differential is max 1 (e.g SOV_OLD is 8 and SOV_Decile 9) and min 0 


--IMPACT
select count (DISTINCT F.account_number)
FROM V98_MainTable_SOV_Final as F 
INNER JOIN #tempsports_footballtop3deciles as temp
ON temp.account_number=F.account_number
WHERE SOV_OLD in (8,9,10)
AND segment_id='genre_sports_football'
AND SOV_Decile-SOV_OLD>0
--there are 48,648 accounts that have been promoted in SOV_Decile (max of 1 promotions for any account)

--racing
select DISTINCT F.account_number, segment_id, SOV_Decile, SOV_OLD, SOV_Decile-SOV_OLD as SOVFootballDiff 
FROM V98_MainTable_SOV_Final as F 
INNER JOIN #tempsports_racingtop3deciles as temp
ON temp.account_number=F.account_number
WHERE SOV_OLD in (8,9,10)
AND segment_id='genre_sports_racing'
ORDER BY 5 desc
--this shows that there is a bit of change in the TOP 3 deciles of racing. differential is max 2 (e.g SOV_OLD is 8 and SOV_Decile 10) and min 0 

--IMpact
select count (DISTINCT F.account_number)
FROM V98_MainTable_SOV_Final as F 
INNER JOIN #tempsports_racingtop3deciles as temp
ON temp.account_number=F.account_number
WHERE SOV_OLD in (8,9,10)
AND segment_id='genre_sports_racing'
AND SOV_Decile-SOV_OLD>0
--there are 8,430 accounts that have been promoted in SOV_Decile (max of 2 promotions for any account)

--sports other
select DISTINCT F.account_number, segment_id, SOV_Decile, SOV_OLD, SOV_Decile-SOV_OLD as SOVFootballDiff 
FROM V98_MainTable_SOV_Final as F 
INNER JOIN  #tempsports_othertop3deciles as temp
ON temp.account_number=F.account_number
WHERE SOV_OLD in (8,9,10)
AND segment_id='genre_sports_other'
ORDER BY 5 desc
--this shows that there is some change in the TOP 3 deciles of sports other. differential is max 1 (e.g SOV_OLD is 8 and SOV_Decile 9) and min 0 

--IMpact
select count (DISTINCT F.account_number)
FROM V98_MainTable_SOV_Final as F 
INNER JOIN #tempsports_othertop3deciles as temp
ON temp.account_number=F.account_number
WHERE SOV_OLD in (8,9,10)
AND segment_id='genre_sports_other'
AND SOV_Decile-SOV_OLD>0
--there are 50,979 accounts that have been promoted in SOV_Decile (max of 1 promotions for any account)


--specialist gaming
select DISTINCT F.account_number, segment_id, SOV_Decile, SOV_OLD, SOV_Decile-SOV_OLD as SOVFootballDiff 
FROM V98_MainTable_SOV_Final as F 
INNER JOIN  #tempspecialist_gamingtop3deciles as temp
ON temp.account_number=F.account_number
WHERE SOV_OLD in (8,9,10)
AND segment_id='genre_specialist_Gaming'
ORDER BY 5 desc
--this shows that there is a bit of change in the TOP 3 deciles of gaming. differential is max 1 (e.g SOV_OLD is 8 and SOV_Decile 9) and min 0 

--IMpact
select count (DISTINCT F.account_number)
FROM V98_MainTable_SOV_Final as F 
INNER JOIN #tempspecialist_gamingtop3deciles as temp
ON temp.account_number=F.account_number
WHERE SOV_OLD in (8,9,10)
AND segment_id='genre_specialist_Gaming'
AND SOV_Decile-SOV_OLD>0
--there are 12,527 accounts that have been promoted in SOV_Decile (max of 1 promotions for any account)


-------------------------------------------

--From above we can create a +/- from new SOV decile to old so we can see what is the impact 
ALTER TABLE V98_MainTable_SOV_Final ADD SOV_FootballDiff integer null; commit;

UPDATE V98_MainTable_SOV_Final 
SET  SOV_FootballDiff=SOV_Decile-SOV_OLD
FROM V98_MainTable_SOV_Final; 
Commit; 
--39232982 row(s) updated

--Check data 
SELECT DISTINCT account_number, SOV_Decile, SOV_OLD, SOV_FootballDiff from V98_MainTable_SOV_Final 
ORDER BY 4 desc
--lots of accounts that went from SOV_OLD 3 to SOV_Decile 8. test account number 210016673159. Next script will do a count

SELECT COUNT (DISTINCT account_number) from V98_MainTable_SOV_Final WHERE SOV_FootballDiff=5
--73 accounts

SELECT COUNT (DISTINCT account_number) from V98_MainTable_SOV_Final WHERE SOV_FootballDiff=4
--2,226 accounts

SELECT COUNT (DISTINCT account_number) from V98_MainTable_SOV_Final WHERE SOV_FootballDiff=3
--234,268

SELECT COUNT (DISTINCT account_number) from V98_MainTable_SOV_Final WHERE SOV_FootballDiff=2
--557,218

SELECT COUNT (DISTINCT account_number) from V98_MainTable_SOV_Final WHERE SOV_FootballDiff=1
--591,574

SELECT COUNT (DISTINCT account_number) from V98_MainTable_SOV_Final WHERE SOV_FootballDiff=0
--591,721

SELECT COUNT (DISTINCT account_number) from V98_MainTable_SOV_Final WHERE SOV_FootballDiff=-1
--1,701

SELECT COUNT (DISTINCT account_number) from V98_MainTable_SOV_Final WHERE SOV_FootballDiff<-1
--that's it there is no more SOV demotion from OLD to NEW only in the -1 area

SELECT distinct account_number, SOV_Decile, SOV_OLD, SOV_FootballDiff from V98_MainTable_SOV_Final 
WHERE SOV_FootballDiff=-1

SELECT distinct account_number, SOV_Decile, SOV_OLD, SOV_FootballDiff from V98_MainTable_SOV_Final 
WHERE SOV_OLD in (8,9,10)
ORDER by 4 desc

--SOV_OLD TOP 3 Deciles that appear in SOV_Decile 
SELECT count (distinct account_number)
FROM V98_MainTable_SOV_Final
WHERE SOV_Decile in (8,9,10)
AND account_number IN (SELECT distinct account_number from V98_MainTable_SOV_Final 
WHERE SOV_OLD in (8,9,10))
--526,917 accounts that were in the old SOV and survived in the NEW deciles

--What's the accounts total in Top 3 deciles
SELECT count (distinct account_number) from V98_MainTable_SOV_Final 
WHERE SOV_OLD between 1 AND 10
--603,336


--------------------------Questions----------------------
--Total Super Casino Viewers
select broadcast_date_utc, COUNT (DISTINCT account_number) AS Unique_Viewers
FROM V98_SkyBet_Main_Raw_Table 
WHERE segment_id='channel_SuperCasino'
GROUP by broadcast_date_utc 
ORDER by broadcast_date_utc;

--Total Super Casino Viewers for week 1
select COUNT (DISTINCT account_number) AS Unique_Viewers, SUM (viewing_duration)/60 as Viewing_Mins
FROM V98_SkyBet_Main_Raw_Table 
WHERE segment_id='channel_SuperCasino'
AND broadcast_date_utc between '2012-08-13' AND '2012-08-19'  

--Total Super Casino Viewers for week 2
select COUNT (DISTINCT account_number) AS Unique_Viewers, SUM (viewing_duration)/60 as Viewing_Mins
FROM V98_SkyBet_Main_Raw_Table 
WHERE segment_id='channel_SuperCasino'
AND broadcast_date_utc between '2012-08-20' AND '2012-08-26'  



--Total channel_SkyPokercom Unique Viewers
select broadcast_date_utc, COUNT (DISTINCT account_number) AS Unique_Viewers
FROM V98_SkyBet_Main_Raw_Table 
WHERE segment_id='channel_SkyPokercom'
GROUP by broadcast_date_utc 
ORDER by broadcast_date_utc;

--Total Super Casino Viewers for week 1
select COUNT (DISTINCT account_number) AS Unique_Viewers, SUM (viewing_duration)/60 as Viewing_Mins
FROM V98_SkyBet_Main_Raw_Table 
WHERE segment_id='channel_SkyPokercom'
AND broadcast_date_utc between '2012-08-13' AND '2012-08-19'  

--Total Super Casino Viewers for week 2
select COUNT (DISTINCT account_number) AS Unique_Viewers, SUM (viewing_duration)/60 as Viewing_Mins
FROM V98_SkyBet_Main_Raw_Table 
WHERE segment_id='channel_SkyPokercom'
AND broadcast_date_utc between '2012-08-20' AND '2012-08-26'  

---------------Real Madrid v Barcelona
--need to create a list of account numbers and their TOTAL viewing minutes for the real madrid game. 
--This list is anyone who watched that game for at least 7 seconds as is the minimum viewing limit of the study.
SELECT account_number, SUM(viewing_duration) as Total_ViewingSecs, Total_ViewingSecs/60 as Total_ViewingMins 
INTO RealMadridvBarcelona
FROM V98_SkyBet_Main_Raw_Table
where programme_name='Barcelona v Real Madrid- Live'
GROUP BY account_number
--52656 

--Average viewing minutes (for the whole program instead of consecutive minutes) for those 52,626 account numbers who watched the Real Madrid/Barcelona game
select avg (Total_ViewingMins )
FROM RealMadridvBarcelona
WHERE Total_ViewingSecs>0

--Descriptives are currently in the format of consecutive viewing.
SELECT TOP 25 b.programme_name
        ,SUM (b.viewing_duration)/60 as Total_Viewing_Mins
        ,avg(b.viewing_duration)/60 as Avg_Viewing_Mins-- (average was calculated using SUM divided by number of accounts in the top 3 deciles instead)
        --,MIN(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Min_Viewing_Seconds
    	,MAX(b.viewing_duration)/60 as Max_Viewing_Mins
FROM V98_SkyBet_Main_Raw_Table AS b
INNER JOIN rombaoad.sports_footballtop3deciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_sports_football'
GROUP BY b.programme_name
ORDER by Total_Viewing_Mins desc;

SELECT TOP 25 b.programme_name
        --,MIN(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Min_Viewing_Seconds
    	,MAX(b.viewing_duration)/60 as Max_Viewing_Mins
FROM V98_SkyBet_Main_Raw_Table AS b
INNER JOIN rombaoad.sports_footballtop3deciles AS a
ON b.account_number=a.account_number
WHERE segment_id='genre_sports_football'
GROUP BY b.programme_name, account_number

commit;
================================================END==========================================================END======================================================END===================================================


-------------------------------- CHECKS
--Max and min to show that we have gotten rid of records less than 7 seconds on both Master Tables
--select max(Tot_Viewing_Trumped_Sum), MIn(Tot_Viewing_Trumped_Sum)
--from V98_Tot_Mins_Master
--ORDER BY 1, 2
--Result is 2 days of watching across 2 weeks
--max(V98_Tot_Mins_Master.Tot_Viewing_Trumped_Sum),MIn(V98_Tot_Mins_Master.Tot_Viewing_Trumped_Sum)
--176400,7

--select max(Viewing_Trumped_Sum), MIn(Viewing_Trumped_Sum)
--from V98_SkyBet_Master_Table
--ORDER BY 1, 2
--Result less than 2 days of watching 
--max(V98_SkyBet_Master_Table.Viewing_Trumped_Sum),MIn(V98_SkyBet_Master_Table.Viewing_Trumped_Sum)
--133072,7

--on account#'s
--SELECT COUNT (DISTINCT account_number)
--FROM V98_Tot_Mins_Master
--607844

--SELECT COUNT (DISTINCT account_number)
--FROM V98_SkyBet_Master_Table
--603336
------------------------------Checks END


--------------
-Code 08 - Scaling Tables 
--------------
----De-commissioned as unnecessary as Sky BEt is targeting only the VESPA Panel.
----Create Scaling Table
--select event.*, w.scaling_day, w.vespa_accounts, w.sky_base_accounts, w.weighting
--  into V098_scaling_tbl
--  from (select  e.account_number,
--                date(broadcast_start_date_time_utc) event_date,
--                pk_viewing_prog_instance_fact,
--                s.scaling_segment_id
--           from V98_Viewing_Table_SkyBetDates_Final e,
--                vespa_analysts.SC2_intervals s
--          where e.account_number = s.account_number
--            and date(broadcast_start_date_time_utc) between date(s.reporting_starts) and date(s.reporting_ends)
--       ) event,
--       vespa_analysts.SC2_weightings w
-- where event.event_date = w.scaling_day
--   and event.scaling_segment_id = w.scaling_segment_id;
--commit;
----309106708 row(s) affected
--Execution time: 1210.516 seconds
--Execution time: 0.214 seconds
--
----create indexes
--drop index V098_scaling_ac_idx;
--create index V098_scaling_ac_idx on V098_scaling_tbl(account_number);
--create index V098_scaling_eventdate_idx on V098_scaling_tbl(event_date);
--create index V098_scaling_pk_idx on V098_scaling_tbl(pk_viewing_prog_instance_fact);
--create index V098_scaling_segment_idx on V098_scaling_tbl(scaling_segment_id);
--create index V098_scaling_scaleday_idx on V098_scaling_tbl(scaling_day);
--commit;
--
----Create Distinct Scaling -- so that we don't have a billion rows.
--SELECT DISTINCT account_number, event_date, weighting
--INTO V098_distinct_scaling_tbl
--FROM V098_scaling_tbl;
--Commit;
----7117521 row(s) affected
--Execution time: 132.917 seconds
--Execution time: 0.117 seconds
--
--create index V098_distinctscaling_ac_idx on V098_distinct_scaling_tbl(account_number);
--create index V098_distinctscaling_eventdate_idx on V098_distinct_scaling_tbl(event_date);
--create index V098_distinctscaling_wt_idx on V098_distinct_scaling_tbl(weighting);
--commit;

--grant select on V098_scaling_tbl to vespa_group_low_security;

--------------ChECK-----
--select top 1000 *
--from V098_scaling_tbl
--order by account_number, event_date;
------------CHECK END

--Account Numbers for Scaling
--select COUNT (distinct account_number)
--from V098_distinctscaling_tbl
--388127 distinct accounts
-----------Check END------------

--------------

--------------------Deciling FIX-----------------

--ok so this does work for SOV
--try this with percentile_cont
--drop table V098_segment_scaled_sov_pc_tbl;

--SELECT  --an option for day/time maybe required here,
--        segment_id,
--        PERCENTILE_CONT(0.01) WITHIN GROUP ( ORDER BY Scaled_SOV DESC ) sov_pc_100,
--        PERCENTILE_CONT(0.1) WITHIN GROUP ( ORDER BY Scaled_SOV DESC ) sov_pc_90,
--        PERCENTILE_CONT(0.2) WITHIN GROUP ( ORDER BY Scaled_SOV DESC ) sov_pc_80,
--        PERCENTILE_CONT(0.3) WITHIN GROUP ( ORDER BY Scaled_SOV DESC ) sov_pc_70,
--        PERCENTILE_CONT(0.4) WITHIN GROUP ( ORDER BY Scaled_SOV DESC ) sov_pc_60,
--        PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY Scaled_SOV DESC ) sov_pc_50,
--        PERCENTILE_CONT(0.6) WITHIN GROUP ( ORDER BY Scaled_SOV DESC ) sov_pc_40,
--        PERCENTILE_CONT(0.7) WITHIN GROUP ( ORDER BY Scaled_SOV DESC ) sov_pc_30,
--        PERCENTILE_CONT(0.8) WITHIN GROUP ( ORDER BY Scaled_SOV DESC ) sov_pc_20,
--        PERCENTILE_CONT(0.9) WITHIN GROUP ( ORDER BY Scaled_SOV DESC ) sov_pc_10,
--        PERCENTILE_CONT(0.99) WITHIN GROUP ( ORDER BY Scaled_SOV DESC ) sov_pc_0
--  INTO  V098_segment_scaled_sov_pc_tbl
--  FROM   V98_MainTable_SOV_Final_s2
--  GROUP BY --an option for day/time maybe required here,
--        segment_id;
--commit;
--
--
----put this into a table that can be used to query on properly
--select segment_id, 10 'decile', sov_pc_90 pc_start, sov_pc_100 pc_end
--  into V098_segment_scaled_sov_pc_query_tbl
--  from V098_segment_scaled_sov_pc_tbl;
--commit;
--insert into V098_segment_scaled_sov_pc_query_tbl
--select segment_id, 9 'decile', sov_pc_80 pc_start, sov_pc_90 pc_end
--  from V098_segment_scaled_sov_pc_tbl;
--insert into V098_segment_scaled_sov_pc_query_tbl
--select segment_id, 8 'decile', sov_pc_70 pc_start, sov_pc_80 pc_end
--  from V098_segment_scaled_sov_pc_tbl;
--insert into V098_segment_scaled_sov_pc_query_tbl
--select segment_id, 7 'decile', sov_pc_60 pc_start, sov_pc_70 pc_end
--  from V098_segment_scaled_sov_pc_tbl;
--insert into V098_segment_scaled_sov_pc_query_tbl
--select segment_id, 6 'decile', sov_pc_50 pc_start, sov_pc_60 pc_end
--  from V098_segment_scaled_sov_pc_tbl;
--insert into V098_segment_scaled_sov_pc_query_tbl
--select segment_id, 5 'decile', sov_pc_40 pc_start, sov_pc_50 pc_end
--  from V098_segment_scaled_sov_pc_tbl;
--insert into V098_segment_scaled_sov_pc_query_tbl
--select segment_id, 4 'decile', sov_pc_30 pc_start, sov_pc_40 pc_end
--  from V098_segment_scaled_sov_pc_tbl;
--insert into V098_segment_scaled_sov_pc_query_tbl
--select segment_id, 3 'decile', sov_pc_20 pc_start, sov_pc_30 pc_end
--  from V098_segment_scaled_sov_pc_tbl;
--insert into V098_segment_scaled_sov_pc_query_tbl
--select segment_id, 2 'decile', sov_pc_10 pc_start, sov_pc_20 pc_end
--  from V098_segment_scaled_sov_pc_tbl;
--insert into V098_segment_scaled_sov_pc_query_tbl
--select segment_id, 1 'decile', sov_pc_0 pc_start, sov_pc_10 pc_end
--  from V098_segment_scaled_sov_pc_tbl;
--commit;
----38 row(s) affected
--
----------------------------------------
--
--
--SELECT  --an option for day/time maybe required here,
--        segment_id,
--        PERCENTILE_CONT(0.01) WITHIN GROUP ( ORDER BY ScaledSecs DESC ) sclsecs_pc_100,
--        PERCENTILE_CONT(0.1) WITHIN GROUP ( ORDER BY ScaledSecs DESC ) sclsecs_pc_90,
--        PERCENTILE_CONT(0.2) WITHIN GROUP ( ORDER BY ScaledSecs DESC ) sclsecs_pc_80,
--        PERCENTILE_CONT(0.3) WITHIN GROUP ( ORDER BY ScaledSecs DESC ) sclsecs_pc_70,
--        PERCENTILE_CONT(0.4) WITHIN GROUP ( ORDER BY ScaledSecs DESC ) sclsecs_pc_60,
--        PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY ScaledSecs DESC ) sclsecs_pc_50,
--        PERCENTILE_CONT(0.6) WITHIN GROUP ( ORDER BY ScaledSecs DESC ) sclsecs_pc_40,
--        PERCENTILE_CONT(0.7) WITHIN GROUP ( ORDER BY ScaledSecs DESC ) sclsecs_pc_30,
--        PERCENTILE_CONT(0.8) WITHIN GROUP ( ORDER BY ScaledSecs DESC ) sclsecs_pc_20,
--        PERCENTILE_CONT(0.9) WITHIN GROUP ( ORDER BY ScaledSecs DESC ) sclsecs_pc_10,
--        PERCENTILE_CONT(0.99) WITHIN GROUP ( ORDER BY ScaledSecs DESC ) sclsecs_pc_0
--  INTO  V098_segment_scaled_secs_pc_tbl
--  FROM   V98_MainTable_SOV_Final_s2
--  GROUP BY --an option for day/time maybe required here,
--        segment_id;
--commit;
----38 row(s) affected
--
--
--
----put this into a table that can be used to query on properly
--select segment_id, 10 'decile', sclsecs_pc_90 pc_start, sclsecs_pc_100 pc_end
--  into V098_segment_scaled_secs_pc_query_tbl
--  from V098_segment_scaled_secs_pc_tbl;
--commit;
--insert into V098_segment_scaled_secs_pc_query_tbl
--select segment_id, 9 'decile', sclsecs_pc_80 pc_start, sclsecs_pc_90 pc_end
--  from V098_segment_scaled_secs_pc_tbl;
--insert into V098_segment_scaled_secs_pc_query_tbl
--select segment_id, 8 'decile', sclsecs_pc_70 pc_start, sclsecs_pc_80 pc_end
--  from V098_segment_scaled_secs_pc_tbl;
--insert into V098_segment_scaled_secs_pc_query_tbl
--select segment_id, 7 'decile', sclsecs_pc_60 pc_start, sclsecs_pc_70 pc_end
--  from V098_segment_scaled_secs_pc_tbl;
--insert into V098_segment_scaled_secs_pc_query_tbl
--select segment_id, 6 'decile', sclsecs_pc_50 pc_start, sclsecs_pc_60 pc_end
--  from V098_segment_scaled_secs_pc_tbl;
--insert into V098_segment_scaled_secs_pc_query_tbl
--select segment_id, 5 'decile', sclsecs_pc_40 pc_start, sclsecs_pc_50 pc_end
--  from V098_segment_scaled_secs_pc_tbl;
--insert into V098_segment_scaled_secs_pc_query_tbl
--select segment_id, 4 'decile', sclsecs_pc_30 pc_start, sclsecs_pc_40 pc_end
--  from V098_segment_scaled_secs_pc_tbl;
--insert into V098_segment_scaled_secs_pc_query_tbl
--select segment_id, 3 'decile', sclsecs_pc_20 pc_start, sclsecs_pc_30 pc_end
--  from V098_segment_scaled_secs_pc_tbl;
--insert into V098_segment_scaled_secs_pc_query_tbl
--select segment_id, 2 'decile', sclsecs_pc_10 pc_start, sclsecs_pc_20 pc_end
--  from V098_segment_scaled_secs_pc_tbl;
--insert into V098_segment_scaled_secs_pc_query_tbl
--select segment_id, 1 'decile', sclsecs_pc_0 pc_start, sclsecs_pc_10 pc_end
--  from V098_segment_scaled_secs_pc_tbl;
--commit;
----38 row(s) affected
--
--
--
-------------------------------------CHekcs can be deleteed
----SELECT COUNT (DISTINCT account_number) FROM V98_Tot_Mins_Cap_Raw
----COUNT of distinct account numbers 607,850 (Problem!! not same as the viewing segments raw count of accts - small amount though)
--
-----Problem!! there are viewing of less than 7 seconds - will delete!
----SELECT COUNT (*) 
----FROM V98_Tot_Mins_Cap_Raw
----WHERE viewing_duration<7;
----1 918,071 viewing duration less than 7 seconds for totals raw
--
----SELECT COUNT (*) 
----FROM V98_SkyBet_Main_Raw_Table
----WHERE viewing_duration<7;
----1,978,106
--
----Date range check - good for both sides
----SELECT min(broadcast_date_utc), max(broadcast_date_utc) 
----FROM V98_Tot_Mins_Cap_Raw
----min(V98_Tot_Mins_Cap_Raw.broadcast_date_utc),max(V98_Tot_Mins_Cap_Raw.broadcast_date_utc)
--'2012-08-13','2012-08-27'
--
----SELECT min(broadcast_date_utc), max(broadcast_date_utc) 
----FROM V98_SkyBet_Main_Raw_Table
----min(V98_SkyBet_Main_Raw_Table.broadcast_date_utc),max(V98_SkyBet_Main_Raw_Table.broadcast_date_utc)
--'2012-08-13','2012-08-27'
--
----After fix and deletion of duplicates. Shows from sky bet dates that there are noduplicates in primary key
----select COUNT (*), COUNT (distinct (pk_viewing_prog_instance_fact))
----from V98_Viewing_Table_SkyBetDates_Final
----COUNT(),COUNT(distinct(V98_Viewing_Table_SkyBetDates.pk_viewing_prog_instance_fact))
--288801550,288801550
-- --Commit;
--
----select COUNT (*), COUNT (distinct (cb_row_id))
----from V98_CappedViewingTK
----COUNT(),COUNT(distinct(V98_CappedViewingTK.cb_row_id))
--236625685,236625685
--
---------------------------------- CHECKS
----Max and min to show that we have gotten rid of records less than 7 seconds on both Master Tables
----select max(Tot_Viewing_Trumped_Sum), MIn(Tot_Viewing_Trumped_Sum)
----from V98_Tot_Mins_Master
----ORDER BY 1, 2
----Result is 2 days of watching across 2 weeks
----max(V98_Tot_Mins_Master.Tot_Viewing_Trumped_Sum),MIn(V98_Tot_Mins_Master.Tot_Viewing_Trumped_Sum)
----176400,7
--
----select max(Viewing_Trumped_Sum), MIn(Viewing_Trumped_Sum)
----from V98_SkyBet_Master_Table
----ORDER BY 1, 2
----Result less than 2 days of watching 
----max(V98_SkyBet_Master_Table.Viewing_Trumped_Sum),MIn(V98_SkyBet_Master_Table.Viewing_Trumped_Sum)
----133072,7
--
----on account#'s
----SELECT COUNT (DISTINCT account_number)
----FROM V98_Tot_Mins_Master
----607844
--
----SELECT COUNT (DISTINCT account_number)
----FROM V98_SkyBet_Master_Table
----603336
--
----SELECT COUNT (DISTINCT account_number) FROM V98_Tot_Mins_Cap_Raw
----COUNT of distinct account numbers 607,850 (Problem!! not same as the viewing segments raw count of accts - small amount though)
--
-----Problem!! there are viewing of less than 7 seconds - will delete!
----SELECT COUNT (*) 
----FROM V98_Tot_Mins_Cap_Raw
----WHERE viewing_duration<7;
----1 918,071 viewing duration less than 7 seconds for totals raw
--
----SELECT COUNT (*) 
----FROM V98_SkyBet_Main_Raw_Table
----WHERE viewing_duration<7;
----1,978,106
--
----Date range check - good for both sides
----SELECT min(broadcast_date_utc), max(broadcast_date_utc) 
----FROM V98_Tot_Mins_Cap_Raw
----min(V98_Tot_Mins_Cap_Raw.broadcast_date_utc),max(V98_Tot_Mins_Cap_Raw.broadcast_date_utc)
--'2012-08-13','2012-08-27'
--
----SELECT min(broadcast_date_utc), max(broadcast_date_utc) 
----FROM V98_SkyBet_Main_Raw_Table
----min(V98_SkyBet_Main_Raw_Table.broadcast_date_utc),max(V98_SkyBet_Main_Raw_Table.broadcast_date_utc)
----'2012-08-13','2012-08-27'
--
----After fix and deletion of duplicates. Shows from sky bet dates that there are noduplicates in primary key
----select COUNT (*), COUNT (distinct (pk_viewing_prog_instance_fact))
----from V98_Viewing_Table_SkyBetDates_Final
----COUNT(),COUNT(distinct(V98_Viewing_Table_SkyBetDates.pk_viewing_prog_instance_fact))
----288801550,288801550
----Commit;
--
----select COUNT (*), COUNT (distinct (cb_row_id))
----from V98_CappedViewingTK
----COUNT(),COUNT(distinct(V98_CappedViewingTK.cb_row_id))
----236625685,236625685
--
-----Checks END
--
-----------------------------------Checks end
--
---------------------------------------------------------------------------------------------VARIABLEs ------------------------------------------------------------------------
------------
--NULLS
-------------
--select count(*) from V98_MainTable_SOV_Final where Viewed_Decile is null
----61,907
--
--select count(*) from V98_MainTable_SOV_Final where Segment_Viewed_Decile is null
----39,232,982
--
------------------------------------FIX requested by SMO
--SELECT  --an option for day/time maybe required here,
--        segment_id,
--        PERCENTILE_CONT(0) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_100,
--        PERCENTILE_CONT(0.1) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_90,
--        PERCENTILE_CONT(0.2) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_80,
--        PERCENTILE_CONT(0.3) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_70,
--        PERCENTILE_CONT(0.4) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_60,
--        PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_50,
--        PERCENTILE_CONT(0.6) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_40,
--        PERCENTILE_CONT(0.7) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_30,
--        PERCENTILE_CONT(0.8) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_20,
--        PERCENTILE_CONT(0.9) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_10,
--        PERCENTILE_CONT(1) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_0
--  INTO  V098_AcctSegmentSOV_raw_AllDates_pc_tbl2
--  FROM   rombaoad.V98_MainTable_SOV_Final
--  GROUP BY --an option for day/time maybe required here,
--        segment_id;
--commit;
--
--select segment_id, 10 'decile', pc_90 pc_start, pc_100 pc_end
--  into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
--commit;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
--select segment_id, 9 'decile', pc_80 pc_start, pc_90 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
--select segment_id, 8 'decile', pc_70 pc_start, pc_80 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
--select segment_id, 7 'decile', pc_60 pc_start, pc_70 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
--select segment_id, 6 'decile', pc_50 pc_start, pc_60 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
--select segment_id, 5 'decile', pc_40 pc_start, pc_50 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
--select segment_id, 4 'decile', pc_30 pc_start, pc_40 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
--select segment_id, 3 'decile', pc_20 pc_start, pc_30 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
--select segment_id, 2 'decile', pc_10 pc_start, pc_20 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
--select segment_id, 1 'decile', pc_0 pc_start, pc_10 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2;
--commit;
--
--
--
--select count (*) 
--from top3decileaccounts_sports_football
----88,183
--
--SELECT account_number, segment_id, broadcast_date_utc, SUM (CASE WHEN viewing_duration < programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Viewing_Trumped_Sum, COUNT(distinct dk_programme_instance_dim) as Segment_Programs_Watched
--Into V98_SkyBet_Master_Table2
--FROM V98_SkyBet_Main_Raw_Table2  
--GROUP BY account_number, broadcast_date_utc, segment_id
--ORDER BY account_number, broadcast_date_utc, segment_id
--Commit; --39232982 row(s) affected, Execution time: 422.789 seconds
--
--select count (distinct account_number) FROM V98_SkyBet_Master_Table2 -- 603,336 (same as before)
--
--
--
------Seconds watched in total based on programme_name
--
--SELECT b.programme_name, SUM(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Viewing_Trumped_Sum
--FROM V98_SkyBet_Main_Raw_Table2 AS b
--INNER JOIN top3decileaccounts_sports_football AS a
--ON b.account_number=a.account_number
--GROUP BY b.programme_name
--ORDER by Viewing_Trumped_Sum desc
--
--select * from
--
--
--UPDATE V98_MainTable_SOV_Final
--SET  SOV_Decile=sov.decile
--FROM V98_MainTable_SOV_Final f
--INNER JOIN V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2 sov
--ON f.segment_id = sov.segment_id
--AND f.AcctSegmentSOV_raw_AllDates BETWEEN pc_start and pc_end;
--Commit;
----38,448,444 row(s) updated 08/10/2012
--
--
--ALTER TABLE V98_MainTable_SOV_Final ADD Viewed_Decile INTEGER null;
--
--select *
--from  V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2
--select * from V98_MainTable_SOV_Final
--WHERE SOV_Decile>Viewed_Decile OR SOV_Decile<Viewed_Decile or Viewed_Decile is null
--
----38448444
----null 784538
----39232982
--
----
--drop table V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--drop table V098_AcctAvgSecs_raw_AllDates_pc_query_tbl;
--SELECT  --an option for day/time maybe required here,
--        segment_id,
--        PERCENTILE_CONT(0) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_100,
--        PERCENTILE_CONT(0.1) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_90,
--        PERCENTILE_CONT(0.2) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_80,
--        PERCENTILE_CONT(0.3) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_70,
--        PERCENTILE_CONT(0.4) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_60,
--        PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_50,
--        PERCENTILE_CONT(0.6) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_40,
--        PERCENTILE_CONT(0.7) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_30,
--        PERCENTILE_CONT(0.8) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_20,
--        PERCENTILE_CONT(0.9) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_10,
--        PERCENTILE_CONT(1) WITHIN GROUP ( ORDER BY AcctAvgSecs_raw_AllDates DESC ) pc_0
--  INTO  V098_AcctAvgSecs_raw_AllDates_pc_tbl
--  FROM   V98_MainTable_SOV_Final
--  GROUP BY --an option for day/time maybe required here,
--        segment_id;
--commit;
----
--select segment_id, 10 'decile', pc_90 pc_start, pc_100 pc_end
--  into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--  from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--commit;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 9 'decile', pc_80 pc_start, pc_90 pc_end
--  from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 8 'decile', pc_70 pc_start, pc_80 pc_end
--  from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 7 'decile', pc_60 pc_start, pc_70 pc_end
--  from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 6 'decile', pc_50 pc_start, pc_60 pc_end
--  from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 5 'decile', pc_40 pc_start, pc_50 pc_end
--  from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 4 'decile', pc_30 pc_start, pc_40 pc_end
--  from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 3 'decile', pc_20 pc_start, pc_30 pc_end
--  from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 2 'decile', pc_10 pc_start, pc_20 pc_end
--  from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--insert into V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--select segment_id, 1 'decile', pc_0 pc_start, pc_10 pc_end
--  from V098_AcctAvgSecs_raw_AllDates_pc_tbl;
--commit;
--
--UPDATE V98_MainTable_SOV_Final
--SET  Viewed_Decile=viewed.decile
--FROM V98_MainTable_SOV_Final f
--INNER JOIN V098_AcctAvgSecs_raw_AllDates_pc_query_tbl as viewed
--ON f.segment_id = viewed.segment_id
--AND f.AcctAvgSecs_raw_AllDates BETWEEN pc_start and pc_end;
--Commit; --39171075 row(s) updated, Execution time: 218.293 seconds
--
--
--select * from  V098_AcctAvgSecs_raw_AllDates_pc_query_tbl
--SELECT * FROM V98_MainTable_SOV_Final where AcctAvgSecs_raw_AllDates<0.7142857313156128 and segment_id='genre_sports_football'
--select distinct account_number from V98_MainTable_SOV_Final where Segment_Viewed_Decile is null
----29,561
--
--select distinct a.account_number
--FROM V98_MainTable_SOV_Final 
--where a.Segment_Viewed_Decile is null and a.SOV_Decile=10
----3860
--
--select distinct account_number
--from V98_MainTable_SOV_Final where Segment_Viewed_Decile is null and SOV_Decile=1;
------24251
--
--SELECT count(*) from V98_MainTable_SOV_Final
--where segment_id='genre_sports_football'
--AND Segment_Viewed_Decile is null
----8,598
--
--SELECT count(*) from V98_MainTable_SOV_Final
--where segment_id='genre_sports_football'
--AND Segment_Viewed_Decile is null and SOV_Decile=10
----2077
--
--select * from V98_SkyBet_Final_Deciles where genre_sports_football is null
--
--
--ALTER TABLE 
--select count(distinct account_number) from V98_MainTable_SOV_Final
--where Segment_Viewed_Decile is NULL;
--
--SELECT  --an option for day/time maybe required here,
--        segment_id,
--        PERCENTILE_CONT(0.01) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_100,
--        PERCENTILE_CONT(0.1) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_90,
--        PERCENTILE_CONT(0.2) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_80,
--        PERCENTILE_CONT(0.3) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_70,
--        PERCENTILE_CONT(0.4) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_60,
--        PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_50,
--        PERCENTILE_CONT(0.6) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_40,
--        PERCENTILE_CONT(0.7) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_30,
--        PERCENTILE_CONT(0.8) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_20,
--        PERCENTILE_CONT(0.9) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_10,
--        PERCENTILE_CONT(0.99) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_0
--  INTO  V098_AcctSegmentSOV_raw_AllDates_pc_tbl
--  FROM   V98_MainTable_SOV_Final
--  GROUP BY --an option for day/time maybe required here,
--        segment_id;
--commit;
----38 row(s) affected
--
--select segment_id, 10 'decile', pc_90 pc_start, pc_100 pc_end
--  into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl;
--commit;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl
--select segment_id, 9 'decile', pc_80 pc_start, pc_90 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl
--select segment_id, 8 'decile', pc_70 pc_start, pc_80 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl
--select segment_id, 7 'decile', pc_60 pc_start, pc_70 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl
--select segment_id, 6 'decile', pc_50 pc_start, pc_60 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl
--select segment_id, 5 'decile', pc_40 pc_start, pc_50 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl
--select segment_id, 4 'decile', pc_30 pc_start, pc_40 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl
--select segment_id, 3 'decile', pc_20 pc_start, pc_30 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl
--select segment_id, 2 'decile', pc_10 pc_start, pc_20 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl
--select segment_id, 1 'decile', pc_0 pc_start, pc_10 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl;
--commit;
--38 row(s) affected
-------------------------------------------from jon
--SELECT  --an option for day/time maybe required here,
--        segment_id,
--        PERCENTILE_CONT(0.001) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_100,
--        PERCENTILE_CONT(0.1) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_90,
--        PERCENTILE_CONT(0.2) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_80,
--        PERCENTILE_CONT(0.3) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_70,
--        PERCENTILE_CONT(0.4) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_60,
--        PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_50,
--        PERCENTILE_CONT(0.6) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_40,
--        PERCENTILE_CONT(0.7) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_30,
--        PERCENTILE_CONT(0.8) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_20,
--        PERCENTILE_CONT(0.9) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_10,
--        PERCENTILE_CONT(0.999) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_0
--  INTO  V098_AcctSegmentSOV_raw_AllDates_pc_tbl
--  FROM   V98_MainTable_SOV_Final
--  GROUP BY --an option for day/time maybe required here,
--        segment_id;
--commit;
----38 row(s) affected
--
--select segment_id, 10 'decile', pc_90 pc_start, pc_100 pc_end
--  into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl;
--commit;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl
--select segment_id, 9 'decile', pc_80 pc_start, pc_90 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl
--select segment_id, 8 'decile', pc_70 pc_start, pc_80 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl
--select segment_id, 7 'decile', pc_60 pc_start, pc_70 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl
--select segment_id, 6 'decile', pc_50 pc_start, pc_60 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl
--select segment_id, 5 'decile', pc_40 pc_start, pc_50 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl
--select segment_id, 4 'decile', pc_30 pc_start, pc_40 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl
--select segment_id, 3 'decile', pc_20 pc_start, pc_30 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl
--select segment_id, 2 'decile', pc_10 pc_start, pc_20 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl;
--insert into V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl
--select segment_id, 1 'decile', pc_0 pc_start, pc_10 pc_end
--  from V098_AcctSegmentSOV_raw_AllDates_pc_tbl;
--commit;
--
--ALTER TABLE V98_MainTable_SOV_Final add SOV_OLD integer null
--ALTER TABLE delete 
--
--UPDATE V98_MainTable_SOV_Final
--SET  SOV_OLD=sov.decile
--FROM V98_MainTable_SOV_Final f
--INNER JOIN V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl sov
--ON f.segment_id = sov.segment_id
--AND f.AcctSegmentSOV_raw_AllDates BETWEEN pc_start and pc_end;
--Commit;
----38,448,444 row(s) updated 08/10/2012
--DROP TABLE V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl sov
--DROP TABLE V098_AcctSegmentSOV_raw_AllDates_pc_tbl
--SELECT count(DISTINCT account_number) V98_MainTable_SOV_Final where SOV_OLD is NULL
----
--
--SELECT DISTINCT account_number FROM V98_MainTable_SOV_Final where SOV_OLD is NULL and SOV_Decile=10
----8089
--
--SELECT DISTINCT account_number select * FROM V98_MainTable_SOV_Final where SOV_OLD is NULL and SOV_Decile=1
--
--select * from V98_SkyBet_Final_Deciles
----30398
--
-----Create segment list of account numbers for football
--SELECT DISTINCT account_number
--INTO rombaoad.sports_racingtop3deciles
--FROM V98_MainTable_SOV_Final 
--where segment_id='genre_sports_racing'
--AND SOV_Decile IN (8,9,10)
--GROUP by account_number
----11,662 row(s) affected
--
--SELECT b.programme_name, SUM(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Viewing_Trumped_Sum
--FROM V98_SkyBet_Main_Raw_Table2 AS b
--INNER JOIN sports_footballtop3deciles AS t3
--ON b.account_number=t3.account_number
--GROUP BY b.programme_name
--ORDER by Viewing_Trumped_Sum desc
--
--CREATE INDEX t3_idx_acct ON sports_footballtop3deciles (account_number);
--CREATE INDEX RawTab2_idx_acct ON V98_SkyBet_Main_Raw_Table2 (account_number);
--
--Commit;
--
--select * From V98_SkyBet_Final_Deciles;
--UPDATE V98_MainTable_SOV_Final
--SET  Segment_Viewed_Decile=segmentvw.decile
--FROM V98_MainTable_SOV_Final f
--INNER JOIN V098_AcctSegmentsAvgSecs_raw_AllDates_pc_query_tbl as segmentvw
--ON f.segment_id = segmentvw.segment_id
--AND f.AcctSegmentsAvgSecs_raw_AllDates BETWEEN pc_start and pc_end;
--Commit;  --39,171,075 row(s) updated
--
--
--select * from V98_SkyBet_Main_Raw_Table2
--where segment_id='genre_sports_racing'
--
--SELECT b.programme_name, SUM(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Viewing_Trumped_Sum
--FROM V98_SkyBet_Main_Raw_Table2 AS b
--INNER JOIN rombaoad.sports_footballtop3deciles AS a
--ON b.account_number=a.account_number
--GROUP BY b.programme_name
--ORDER by Viewing_Trumped_Sum desc;
--
-----football
--SELECT b.channel_name
--        ,SUM(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Total_Viewing_Seconds
--        ,avg(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Min_Viewing_Seconds
--        ,MIN(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Min_Viewing_Seconds
--    	,MAX(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Max_Viewing_Seconds
--FROM V98_SkyBet_Main_Raw_Table2 AS b
--INNER JOIN rombaoad.sports_footballtop3deciles AS a
--ON b.account_number=a.account_number
--WHERE segment_id='genre_sports_football'
--GROUP BY b.channel_name
--ORDER by Total_Viewing_Seconds desc;
--
----Football
--SELECT avg(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Avg_Viewing_Seconds
--FROM V98_SkyBet_Main_Raw_Table2 AS b
--INNER JOIN rombaoad.sports_footballtop3deciles AS a
--ON b.account_number=a.account_number
--WHERE segment_id='genre_sports_football'
----Avg_Viewing_Seconds
----437.4099464755604180
--
----Gaming
--SELECT avg(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Avg_Viewing_Seconds
--FROM V98_SkyBet_Main_Raw_Table2 AS b
--INNER JOIN rombaoad.specialist_gamingtop3deciles AS a
--ON b.account_number=a.account_number
--WHERE segment_id='genre_specialist_Gaming'
----Avg_Viewing_Seconds
----1220.8461715167078243
--
----Racing
--SELECT avg(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Avg_Viewing_Seconds
--FROM V98_SkyBet_Main_Raw_Table2 AS b
--INNER JOIN rombaoad.sports_racingtop3deciles AS a
--ON b.account_number=a.account_number
--WHERE segment_id='genre_sports_racing'
----Avg_Viewing_Seconds
----398.2666176623408378
--
----Sports_Other
--SELECT avg(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Avg_Viewing_Seconds
--FROM V98_SkyBet_Main_Raw_Table2 AS b
--INNER JOIN rombaoad.sports_othertop3deciles AS a
--ON b.account_number=a.account_number
--WHERE segment_id='genre_sports_other'
----Avg_Viewing_Seconds
----346.5681425414476926
--
--
--
--SELECT b.programme_name
--        ,SUM(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Total_Viewing_Seconds
--        ,avg(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Avg_Viewing_Seconds
--        ,MIN(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Min_Viewing_Seconds
--    	,MAX(CASE WHEN b.viewing_duration < b.programme_instance_duration THEN viewing_duration  ELSE programme_instance_duration END) as Max_Viewing_Seconds
--FROM V98_SkyBet_Main_Raw_Table2 AS b
--INNER JOIN rombaoad.sports_othertop3deciles AS a
--ON b.account_number=a.account_number
--WHERE segment_id='genre_sports_other'
--GROUP BY b.programme_name
--ORDER by Total_Viewing_Seconds desc;
--
---------------------------------
--
--
--
--
--GRANT SELECT ON V098_AcctSegmentSOV_raw_AllDates_pc_query_tbl2 to barbera, commit;
--
--
--select MIN(AcctSegmentSOV_raw_AllDates), AVG (AcctSegmentSOV_raw_AllDates), MAX (AcctSegmentSOV_raw_AllDates) 
--from V98_MainTable_SOV_Final
--WHERE segment_id='genre_sports'
--and SOV_Decile=1
----MIN(V98_MainTable_SOV_Final.AcctSegmentSOV_raw_AllDates),AVG(V98_MainTable_SOV_Final.AcctSegmentSOV_raw_AllDates),MAX(V98_MainTable_SOV_Final.AcctSegmentSOV_raw_AllDates)
--1.6327826E-5,0.007766884192073137,0.01823569
--
---------------------
--
--SELECT  segment_id,
--        account_number,
--        NTILE(10) OVER (partition by segment_id ORDER BY AcctSegmentSOV_raw_AllDates ASC) decile
--INTO ntilesov
--FROM  rombaoad.V98_MainTable_SOV_Final
----39232982 row(s) affected, Execution time: 70.889 seconds
--
--UPDATE V98_MainTable_SOV_Final 
--SET  SOV_Ntile=decile from V98_MainTable_SOV_Final as f INNER JOIN ntilesov as t ON  f. account_number=t.account_number
--AND f.segment_id = t.segment_id
--
--
--SELECT * 
--from V98_MainTable_SOV_Final
--WHERE segment_id='genre_sports'
--and SOV_Decile=1
--order by 1 asc
--
--channel_SkyPokercom
--channel_SmartLive
--channel_SuperCasino
--channel_attheraces
--channel_racinguk
--genre_entertainment
--genre_entertainment_Detective
--genre_entertainment_Drama
--genre_entertainment_Motors
--genre_entertainment_Soaps
--genre_entertainment_chatshow
--genre_entertainment_comedy
--genre_entertainment_gameshows
--genre_specialist_Gaming
--genre_sports
--genre_sports_American Football
--genre_sports_Athletics
--genre_sports_Baseball
--genre_sports_Basketball
--genre_sports_Boxing
--genre_sports_Cricket
--genre_sports_Darts
--genre_sports_Equestrian
--genre_sports_Extreme
--genre_sports_Fishing
--genre_sports_Golf
--genre_sports_Ice Hockey
--genre_sports_Motor Sport
--genre_sports_Rugby
--genre_sports_Snooker/Pool
--genre_sports_Tennis
--genre_sports_Watersports
--genre_sports_Wintersports
--genre_sports_Wrestling
--genre_sports_football
--genre_sports_other
--genre_sports_racing
--genre_sports_undefined
--
--

--
--Select cb_row_id, viewing_stops
--into #cappedviewingsmo
--FROM describe V98_CappedViewingtk; commit;
----236625685 row(s) affected, Execution time: 141.853 seconds
--
--UPDATE V98_Tot_Mins_Cap_Raw
--SET d.viewing_stops=smo.viewing_stops
--FROM V98_Tot_Mins_Cap_Raw as d
--INNER JOIN #cappedviewingsmo as smo
--ON d.pk_viewing_prog_instance_fact=smo.cb_row_id; commit;
--
--Update V98_Tot_Mins_Cap_Raw
--SET d.cb_key_household=a.cb_key_household
--,d.subscriber_id=a.d.subscriber_id
--FROM V98_Tot_Mins_Cap_Raw as d
--INNER JOIN V98_Viewing_Table_SkyBetDates_Final as a  
--on d.account_number=a.account_number; COMMIT;
--
--SELECT p.FirstName, p.LastName
--    ,NTILE(AcctSegmentSOV_raw_AllDates) OVER(PARTITION BY PostalCode ORDER BY SalesYTD DESC) AS Decile
--INTO V098_AcctSegmentSOV_raw_AllDates_pc_tbl
--FROM rombaoad.V98_MainTable_SOV_Final
--
------------
--select segment_id, NTILE(10) OVER(PARTITION BY AcctSegmentSOV_raw_AllDates ORDER BY AcctSegmentSOV_raw_AllDates DESC) AS Decile
--FROM  V098_AcctSegmentSOV_raw_AllDates_pc_tbl3
--
--
--  , NTILE(100) OVER(PARTITION BY ss.EPG_Genre ORDER BY Seconds_ratio DESC) AS Seconds_cent
--V98_Tot_Mins_Cap_Raw
--
--SELECT  --an option for day/time maybe required here,
--        segment_id,
--        PERCENTILE_CONT(0) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_100,
--        PERCENTILE_CONT(0.1) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_90,
--        PERCENTILE_CONT(0.2) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_80,
--        PERCENTILE_CONT(0.3) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_70,
--        PERCENTILE_CONT(0.4) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_60,
--        PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_50,
--        PERCENTILE_CONT(0.6) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_40,
--        PERCENTILE_CONT(0.7) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_30,
--        PERCENTILE_CONT(0.8) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_20,
--        PERCENTILE_CONT(0.9) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_10,
--        PERCENTILE_CONT(1) WITHIN GROUP ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) pc_0
--  INTO  grant select ON V098_AcctSegmentSOV_raw_AllDates_pc_tbl2 to barbera2; Commit;
--  FROM   rombaoad.V98_MainTable_SOV_Final
--  GROUP BY --an option for day/time maybe required here,
--        segment_id;
--commit;------------------------------
--select * from V098_AcctSegmentSOV_raw_AllDates_pc_tbl4 
--select * from V098_AcctSegmentSOV_raw_AllDates_pc_tbl2
--
--
--select on V098_AcctSegmentSOV_raw_AllDates_pc_tbl4 
--select decile, count(1) sample_count
--from (
--SELECT  account_number, segment_id,
--        NTILE(10) OVER ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) decile
--FROM rombaoad.V98_MainTable_SOV_Final) d
--group by decile;
--select * from V98_MainTable_SOV_Final
--ALTER TABLE V98_MainTable_SOV_Final add SOV_Ntile INTEGER;
--ALTER TABLE V98_MainTable_SOV_Final add Viewed_Ntile INTEGER;
--ALTER TABLE V98_MainTable_SOV_Final add Segment_Viewed_Ntile INTEGER; commit;
--
--UPDATE V98_MainTable_SOV_Final as f
--SET  SOV_Ntile=decile
--FROM (SELECT  account_number, segment_id
--        NTILE(10) OVER ( ORDER BY AcctSegmentSOV_raw_AllDates DESC ) decile
--FROM rombaoad.V98_MainTable_SOV_Final) as t
--where f. account_number=t.account_number 
--AND f.segment_id = t.segment_id;
--
--UPDATE V98_MainTable_SOV_Final as f
--SET  SOV_Ntile=decile
--FROM (SELECT  segment_id, account_number,
--        NTILE(10) OVER (partition by segment_id ORDER BY AcctSegmentSOV_raw_AllDates DESC ) decile
--FROM rombaoad.V98_MainTable_SOV_Final) as t
--where f. account_number=t.account_number 
--AND f.segment_id = t.segment_id
--
--select genre_sports_sov, COUNT (*) 
--FROM V98_SkyBet_Final_Deciles2
--GROUP BY genre_sports_sov 
--ORDER by genre_sports_sov desc
--
--
--SELECT MIN (viewing_duration), MAX (viewing duration), average
--WHERE segment_id='genre_sports'
--
--
--
--describe V98_SkyBet_Final_Deciles2 
--
--SEELC
--
--UPDATE V98_MainTable_SOV_Final 
--SET  SOV_Ntile=decile from V98_MainTable_SOV_Final as f INNER JOIN ntilesov as t ON  f. account_number=t.account_number
--AND f.segment_id = t.segment_id; commit;
--create index V078_ntilesov_ac_idx on ntilesov (account_number asc)
--
--select * from V98_MainTable_SOV_Final 
--
----this is the distribution to find what was wrong. There were many of the values on the smaller deciles.
--select segment_id, sov_decile, min(AcctSegmentSOV_raw_AllDates), max(AcctSegmentSOV_raw_AllDates), avg(AcctSegmentSOV_raw_AllDates), stddev(AcctSegmentSOV_raw_AllDates), count (DISTINCT account_number)
--from rombaoad.V98_MainTable_SOV_Final
--group by  segment_id, sov_decile
--order by  segment_id, sov_decile;
--
--alter table 
-- 
--
--select distinct segment_id, account_number, AcctSegmentSOV_raw_AllDates
--INTO sov_fix_barbera
--From V98_MainTable_SOV_Final; Commit;
--
--SELECT  segment_id, account_number,
--        NTILE(10) OVER (partition by segment_id ORDER BY AcctSegmentSOV_raw_AllDates ASC) decile
--into ntilesov3  
--FROM  rombaoad.sov_fix_barbera; Commit;
--
--UPDATE V98_MainTable_SOV_Final 
--SET  Viewed_Ntile=decile from V98_MainTable_SOV_Final as f 
--INNER JOIN ntilesov3 as t 
--ON f.segment_id = t.segment_id
--AND f.account_number = t.account_number; commit;
--
----Check on the distribution of the accounts - it's all good as they have equal account numbers
--SElECT segment_id, Viewed_Ntile, COUNT (distinct account_number)
--FROM  V98_MainTable_SOV_Final 
--GROUP BY segment_id,  Viewed_Ntile 
--ORDER BY 1, 2 desc 

--example account number that works 620050500862
--
--SELECT  segment_id, account_number,
--        NTILE(10) OVER (partition by segment_id ORDER BY AcctSegmentSOV_raw_AllDates ASC) decile, min (AcctSegmentSOV_raw_AllDates), max (AcctSegmentSOV_raw_AllDates)
--into tempsov  
--FROM  rombaoad.sov_fix_barbera
--GROUP BY segment_id, account_number;
--
--select segment_id, SOV_Decile, min (AcctSegmentSOV_raw_AllDates) AS Min_SOV, max (AcctSegmentSOV_raw_AllDates) AS Max_SOV, avg (AcctSegmentSOV_raw_AllDates) AS Avg_SOV, count(1) AS Number_of_Accounts, count (distinct account_number) AS distinct_acct_numbers, Number_of_Accounts/distinct_acct_numbers
--FROM V98_MainTable_SOV_Final 
--GROUP BY segment_id, SOV_Decile 
--order by segment_id, SOV_Decile desc
--
--SELECT decile, count (*) from ntilesov3
--GROUP BY decile
--  
--
--SELECT segment_id, broadcast_date_utc, avg (CASE WHEN SOV_Decile>7 then SOV_raw END) as AvgTop3DecilesSOV, avg (SOV_raw) as AvgAllDecilesSOV, AvgTop3DecilesSOV-AvgAllDecilesSOV as diff, (diff/AvgAllDecilesSOV)*100 as SOV_Index, avg (CASE WHEN SOV_Decile>7 then sov_progs END) as AvgTop3DecilesSOVProgs, avg (sov_progs) AvgAllDecilesProgs   
--FROM V98_MainTable_SOV_Final
--GROUP BY segment_id, broadcast_date_utc
--ORDER by segment_id, broadcast_date_utc 
--
--select * from ntileviewedavgsecs3 
--where account_number='210043217251'





