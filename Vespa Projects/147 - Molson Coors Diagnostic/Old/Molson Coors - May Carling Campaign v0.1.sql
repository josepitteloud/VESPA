------------------------------------------------------------------------
--                                                                    --
--        Project: Molson Coors - May Carling Campaign                --
--        Description: Collates Viewing data for selected spots       --
--        Version: v1.0                                               --
--        Created: 20/04/2012                                         --
--        Creaded by: Martin Neighbours                               --
--        Lead:                                                       --
--        Analyst: Hannah Starmer                                     --
--        SK Prod: 5                                                  --
--                                                                    --
--                                                                    --
--        PART A.                                                     --
--        PART B.                                                     --
--        PART C.                                                     --
--        PART D.                                                     --
--        PART E.                                                     --
--        PART F.                                                     --
--        PART G.                                                     --
--                                                                    --
--                                                                    --
--                                                                    --
--                                                                    --
--                                                                    --
--                                                                    --
------------------------------------------------------------------------


-- NB. End Table deliberately created as a thin table with joins enabled to other tables through keys --


--------------------------------------------------------------------------------------------------
--                              Known issues                                                    --
--------------------------------------------------------------------------------------------------
-- (1) Known duplicates in vespa_events_all - this issue only impacts timeshift viewing         --
--    ,where the events all table is referenced                                                 --
-- (2) Known "duplicates" in vespa_epg_schedule where there are two dk_programme_instance_dim   --
--     records for the same service key and same start and end time                             --
-- (3) Large number of records in augs do not have an associated scaling weight                 --
--     -these are removed below.  This is because the capping and scaling processes are         --
--     run independently.                                                                       --
-- (4) Phase 1 data is adjusted to ensure a consistent approach with phase 2                    --
--------------------------------------------------------------------------------------------------

------------
-- SET UP --
------------

CREATE VARIABLE @var_period_start               datetime;
CREATE VARIABLE @var_period_end                 datetime;
CREATE VARIABLE @barb_spot_period_start         date;
CREATE VARIABLE @barb_spot_period_end           date;
CREATE VARIABLE @var_sql_spot_list              varchar(4000);
CREATE VARIABLE @var_sql_live                   varchar(4000);
CREATE VARIABLE @var_sql_timeshift              varchar(4000);
CREATE VARIABLE @var_sql_live_ph1               varchar(4000);
CREATE VARIABLE @var_sql_timeshift_ph1          varchar(4000);

CREATE VARIABLE @scanning_day                   datetime;
CREATE VARIABLE @var_num_days                   smallint;


if object_id('#temp_film_no_list') is not null drop table #temp_film_no_list;

CREATE TABLE #temp_film_no_list (
clearcast_commercial_no varchar(15) default null);

-------------------------------
-- THINGS YOU NEED TO CHANGE --
-------------------------------

-- SET @var_period_start  = '2012-07-31';
SET @var_period_start  = '2012-05-01';
SET @var_period_end    = '2012-06-07';
SET @barb_spot_period_start = '2012-05-01';
SET @barb_spot_period_end = '2012-05-31';

-- CHANGE TO FILM CODES YOU ARE INTERESTED IN --

INSERT INTO #temp_film_no_list
SELECT 'VCCMCCA014030'
UNION
SELECT 'VCCMCCA015010'
UNION
SELECT 'VCCMCCA018010';

-----------------------------------
-- SETUP TABLE TEMPLATES WE NEED --
-----------------------------------

-- TEMPLATE FOR MAING OUTPUT TABLE --

if object_id('spot_customer_viewing_capped_thin') is not null drop table spot_customer_viewing_capped_thin;

create table spot_customer_viewing_capped_thin
        (pk_spot_id                             int default null,
        service_key                             int default null,
        pk_viewing_prog_instance_fact           bigint default null,
        dk_programme_instance_dim               bigint default null,
        account_number                          varchar(20) default null,
        subscriber_id                           bigint default null,
        broadcast_spot_viewing_starts           timestamp default null,
        broadcast_spot_viewing_stops            timestamp default null,
        spot_viewing_starts                     timestamp default null,
        spot_viewing_stops                      timestamp default null,
        scaling_weighting                       real default null,
        spot_viewed_duration                    int default null);

-- TABLE TO BE USED TO ADJUST TIMINGS FOR TIMESHIFTED VIEWING --

if object_id('spot_customer_viewing_capped_thin_timeshift_temp') is not null drop table spot_customer_viewing_capped_thin_timeshift_temp;

create table spot_customer_viewing_capped_thin_timeshift_temp (
        pk_spot_id                              int default null,
        service_key                             int default null,
        pk_viewing_prog_instance_fact           bigint default null,
        dk_programme_instance_dim               bigint default null,
        account_number                          varchar(20) default null,
        subscriber_id                           bigint default null,
        broadcast_spot_viewing_starts           timestamp default null,
        broadcast_spot_viewing_stops            timestamp default null,
        spot_viewing_starts                     timestamp default null,
        spot_viewing_stops                      timestamp default null,
        scaling_weighting                       real default null,
        spot_viewed_duration                    int default null);

-- SPOT DATA --

if object_id('spot_data') is not null drop table spot_data;

CREATE TABLE spot_data (
        clearcast_commercial_no                 varchar(15) DEFAULT NULL,
        service_key                             int DEFAULT NULL,
        utc_spot_start_date_time                timestamp DEFAULT NULL,
        utc_start_date                          date DEFAULT NULL,
        utc_end_date                            date DEFAULT NULL,
        utc_spot_end_date_time                  timestamp DEFAULT NULL,
        utc_break_start_date_time               timestamp DEFAULT NULL,
        utc_break_end_date_time                 timestamp DEFAULT NULL,
        spot_position_in_break                  int DEFAULT NULL,
        no_spots_in_break                       int DEFAULT NULL,
        spot_duration                           int DEFAULT NULL,
        barb_date_of_transmission               date DEFAULT NULL,
        barb_spot_start_time                    varchar(6) DEFAULT NULL,
        Full_Name                               varchar(255) DEFAULT NULL,
        Vespa_Name                              varchar(255) DEFAULT NULL,
        channel_name                            varchar(255) DEFAULT NULL,
        techedge_name                           varchar(255) DEFAULT NULL,
        infosys_name                            varchar(255) DEFAULT NULL,
        log_station_code                        int DEFAULT NULL,
        sti_code                                int DEFAULT NULL,
        spot_channel_name                       varchar(255) DEFAULT NULL,
        sold_spot_id                            int NOT NULL DEFAULT NULL,
        pk_spot_id                              int NOT NULL DEFAULT NULL,
        spot_channel_map_version                int DEFAULT NULL,
        channel_map_version                     int DEFAULT NULL);

-- PROGRAMMME AND SPOT DATA FOR PHASE 2 DATA --

if object_id('spot_data_prog_instance') is not null drop table spot_data_prog_instance;

CREATE TABLE spot_data_prog_instance (
        pk_spot_id                              int NOT NULL DEFAULT NULL,
        service_key                             int DEFAULT NULL,
        utc_spot_start_date_time                timestamp DEFAULT NULL,
        utc_spot_end_date_time                  timestamp DEFAULT NULL,
        dk_programme_instance_dim               bigint DEFAULT NULL,
        program_air_datetime                    timestamp DEFAULT NULL,
        program_air_end_datetime                timestamp DEFAULT NULL,
        program_air_date                        date DEFAULT NULL,
        programme_duration                      int DEFAULT NULL,
        broadcast_daypart                       varchar(20) DEFAULT NULL,
        genre_description                       varchar(20) DEFAULT NULL,
        sub_genre_description                   varchar(20) DEFAULT NULL,
        channel_name                            varchar(40) DEFAULT NULL,
        programme_name                          varchar(255) DEFAULT NULL,
        utc_break_start_date_time               timestamp DEFAULT NULL,
        utc_break_end_date_time                 timestamp DEFAULT NULL,
        full_name                               varchar(255) DEFAULT NULL,
        spot_position_in_break                  int DEFAULT NULL,
        no_spots_in_break                       int DEFAULT NULL,
        spot_duration                           int DEFAULT NULL,
        clearcast_commercial_no                 varchar(15) DEFAULT NULL);


create hg index idx1 on spot_data_prog_instance(dk_programme_instance_dim);

-- PROGRAMME AND SPOT DATA FOR PHASE 1 DATA --

if object_id('spot_data_prog_instance_ph1') is not null drop table spot_data_prog_instance_ph1;

CREATE TABLE spot_data_prog_instance_ph1 (
        pk_spot_id                              int NOT NULL DEFAULT NULL,
        service_key                             int DEFAULT NULL,
        utc_spot_start_date_time                timestamp DEFAULT NULL,
        utc_spot_end_date_time                  timestamp DEFAULT NULL,
        dk_programme_instance_dim               bigint DEFAULT NULL,
        program_air_datetime                    timestamp DEFAULT NULL,
        program_air_end_datetime                timestamp DEFAULT NULL,
        program_air_date                        date DEFAULT NULL,
        programme_duration                      int DEFAULT NULL,
        broadcast_daypart                       varchar(20) DEFAULT NULL,
        genre_description                       varchar(20) DEFAULT NULL,
        sub_genre_description                   varchar(20) DEFAULT NULL,
        channel_name                            varchar(40) DEFAULT NULL,
        programme_name                          varchar(255) DEFAULT NULL,
        utc_break_start_date_time               timestamp DEFAULT NULL,
        utc_break_end_date_time                 timestamp DEFAULT NULL,
        full_name                               varchar(255) DEFAULT NULL,
        spot_position_in_break                  int DEFAULT NULL,
        no_spots_in_break                       int DEFAULT NULL,
        spot_duration                           int DEFAULT NULL,
        clearcast_commercial_no                 varchar(15) DEFAULT NULL);

create hg index idx1 on spot_data_prog_instance_ph1(dk_programme_instance_dim);

---------------------
-- POPULATE TABLES --
---------------------

-- SPOT DATA --

--------------------------------------------------------------------------------------------------
--                                              NB.                                             --
--------------------------------------------------------------------------------------------------
-- CREATE UNIQUE SPOT IDENTIFIER AND SOLD SPOT IDENTIFIER THAT CAN BE USED TO RECONCILE TO BARB --
-- SOLD SPOTS CAN BE MAPPED TO MORE THAN 1 CHANNEL                                              --
--------------------------------------------------------------------------------------------------

DELETE FROM spot_data;

INSERT INTO spot_data (
        clearcast_commercial_no
        ,service_key
        ,utc_spot_start_date_time
        ,utc_start_date
        ,utc_end_date
        ,utc_spot_end_date_time
        ,utc_break_start_date_time
        ,utc_break_end_date_time
        ,spot_position_in_break
        ,no_spots_in_break
        ,spot_duration
        ,barb_date_of_transmission
        ,barb_spot_start_time
        ,Full_Name
        ,Vespa_Name
        ,channel_name
        ,techedge_name
        ,infosys_name
        ,log_station_code
        ,sti_code
        ,spot_channel_name
        ,sold_spot_id
        ,pk_spot_id
        ,spot_channel_map_version
        ,channel_map_version)
SELECT   A.clearcast_commercial_no
        ,A.service_key
        ,A.utc_spot_start_date_time
        ,cast(A.utc_spot_start_date_time as date) as utc_start_date
        ,cast(A.utc_spot_end_date_time as date) as utc_end_date
        ,A.utc_spot_end_date_time
        ,A.utc_break_start_date_time
        ,A.utc_break_end_date_time
        ,A.spot_position_in_break
        ,A.no_spots_in_break
        ,A.spot_duration
        ,A.barb_date_of_transmission
        ,A.barb_spot_start_time
        ,B.Full_Name
        ,B.Vespa_Name
        ,B.channel_name
        ,B.techedge_name
        ,B.infosys_name
        ,a.log_station_code
        ,a.sti_code
        ,TRIM(B.Full_Name) AS spot_channel_name
        ,a.sold_spot_id
        ,a.pk_spot_id
        ,a.channel_map_version as spot_channel_map_version
        ,b.version as channel_map_version
FROM     neighbom.BARB_MASTER_SPOT_DATA                         A
JOIN     #temp_film_no_list                                     C
ON       a.clearcast_commercial_no = c.clearcast_commercial_no
LEFT OUTER JOIN
         VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES  B
ON       A.service_key=B.service_key
AND      A.local_date_of_transmission between B.effective_from and B.effective_to
WHERE    barb_date_of_transmission between  @barb_spot_period_start and  @barb_spot_period_end;


-- PROGRAMME DATA --

--------------------------------------------------------------------------------------------------------------
--                                                NB.                                                       --
--------------------------------------------------------------------------------------------------------------
-- (1)RESTRICT PROGRAMME INSTANCES TO THOSE THAT APPEAR IN AUGS TABLE AND ARE ASSOCIATED WITH SPOTS         --
--    THIS WILL MANE NOT ALL SPOTS WILL HAVE AN ASSOCIATED PROGRAMME INSTANCE                               --
-- (2)GETS AROUND ISSUE WITH DUPLICATE PROGRAMME INSTANCES                                                  --
-- (3)SPOT NEED TO START OR END WITHIN A PROGRAMME TO BE PICKED UP                                          --
-- (4)EXPECT THIS LIST TO HAVE MORE RECORDS THAN SPOT LIST AS A SPOT CAN START AND END OF ONE PROGRAMME AND --
--    THE START OF THE NEXT                                                                                 --
--------------------------------------------------------------------------------------------------------------

-- PHASE 2 DATA --

DELETE FROM spot_data_prog_instance;
INSERT INTO spot_data_prog_instance (
        pk_spot_id,
        service_key,
        utc_spot_start_date_time,
        utc_spot_end_date_time,
        dk_programme_instance_dim,
        program_air_datetime,
        program_air_end_datetime,
        program_air_date,
        programme_duration,
        broadcast_daypart,
        genre_description,
        sub_genre_description,
        channel_name,
        programme_name,
        utc_break_start_date_time,
        utc_break_end_date_time,
        full_name,
        spot_position_in_break,
        no_spots_in_break,
        spot_duration,
        clearcast_commercial_no)
SELECT  scd.pk_spot_id,
        scd.service_key,
        scd.utc_spot_start_date_time,
        scd.utc_spot_end_date_time,
        vps.dk_programme_instance_dim,
        vps.broadcast_start_date_time_utc as program_air_datetime,
        vps.broadcast_end_date_time_utc as program_air_end_datetime,
        cast(vps.broadcast_start_date_time_utc as date) program_air_date,
        datediff(mi,vps.broadcast_start_date_time_utc,vps.broadcast_end_date_time_utc) as programme_duration,
        vps.broadcast_daypart,
        vps.genre_description,
        vps.sub_genre_description,
        vps.channel_name,
        vps.programme_name,
        scd.utc_break_start_date_time,
        scd.utc_break_end_date_time,
        scd.full_name,
        scd.spot_position_in_break,
        scd.no_spots_in_break,
        scd.spot_duration,
        scd.clearcast_commercial_no
FROM    sk_prod.vespa_programme_schedule vps
JOIN    spot_data scd
ON      scd.service_key = vps.service_key AND (scd.utc_spot_start_date_time between vps.broadcast_start_date_time_utc and vps.broadcast_end_date_time_utc
                                               OR scd.utc_spot_end_date_time between vps.broadcast_start_date_time_utc and vps.broadcast_end_date_time_utc);

-- PHASE 1 DATA --

DELETE FROM spot_data_prog_instance_ph1;
INSERT INTO spot_data_prog_instance_ph1 (
        pk_spot_id,
        service_key,
        utc_spot_start_date_time,
        utc_spot_end_date_time,
        dk_programme_instance_dim,
        program_air_datetime,
        program_air_end_datetime,
        program_air_date,
        programme_duration,
        broadcast_daypart,
        genre_description,
        sub_genre_description,
        channel_name,
        programme_name,
        utc_break_start_date_time,
        utc_break_end_date_time,
        full_name,
        spot_position_in_break,
        no_spots_in_break,
        spot_duration,
        clearcast_commercial_no)
SELECT  scd.pk_spot_id,
        scd.service_key,
        scd.utc_spot_start_date_time,
        scd.utc_spot_end_date_time,
        vps.programme_trans_sk,
        vps.tx_start_datetime_utc as program_air_datetime,
        vps.tx_end_datetime_utc as program_air_end_datetime,
        tx_date_utc as program_air_date,
        datediff(mi,vps.tx_start_datetime_utc,vps.tx_end_datetime_utc) as programme_duration,
        vps.x_broadcast_time_of_day,
        vps.genre_description,
        vps.sub_genre_description,
        vps.channel_name,
        vps.epg_title,
        scd.utc_break_start_date_time,
        scd.utc_break_end_date_time,
        scd.full_name,
        scd.spot_position_in_break,
        scd.no_spots_in_break,
        scd.spot_duration,
        scd.clearcast_commercial_no
FROM    sk_prod.vespa_epg_dim vps
JOIN    spot_data scd
ON      scd.service_key = vps.service_key AND (scd.utc_spot_start_date_time between vps.tx_start_datetime_utc and vps.tx_end_datetime_utc
                                                OR scd.utc_spot_end_date_time between vps.tx_start_datetime_utc and vps.tx_end_datetime_utc);

---------------------------------------------------------------
-- SET UP VARIABLES FOR LOOPING THROUGH THE DAILY AUG TABLES --
---------------------------------------------------------------

-- TIMESHIFT ONLY --

----------------------------------------------------------------------------------
--                               NB.                                            --
----------------------------------------------------------------------------------
-- NEED TO REFERENCE VIEWING_EVENTS_ALL TO GET TIME)IN_SECONDS_SINCE_RECORDING --
----------------------------------------------------------------------------------

-- PHASE 2 TIMESHIFT DATA --

SET @var_sql_timeshift = '
INSERT INTO spot_customer_viewing_capped_thin_timeshift_temp
        (pk_spot_id,
        service_key,
        pk_viewing_prog_instance_fact,
        dk_programme_instance_dim,
        account_number,
        subscriber_id,
        broadcast_spot_viewing_starts,
        broadcast_spot_viewing_stops,
        spot_viewing_starts,
        spot_viewing_stops,
        scaling_weighting)
SELECT  prog.pk_spot_id,
        prog.service_key,
        cap.cb_row_id,
        prog.dk_programme_instance_dim,
        cap.account_number,
        cap.subscriber_id,
        CASE WHEN dateadd(second,tme.time_in_seconds_since_recording*-1,cap.viewing_starts) > prog.utc_spot_start_date_time
             then dateadd(second,tme.time_in_seconds_since_recording*-1,cap.viewing_starts) else prog.utc_spot_start_date_time end,
        CASE WHEN dateadd(second,tme.time_in_seconds_since_recording*-1,cap.viewing_stops) < prog.utc_spot_end_date_time
             then dateadd(second,tme.time_in_seconds_since_recording*-1,cap.viewing_stops) else prog.utc_spot_end_date_time end,
        CASE WHEN cap.viewing_starts > dateadd(second,tme.time_in_seconds_since_recording,prog.utc_spot_start_date_time)
             then cap.viewing_starts else dateadd(second,tme.time_in_seconds_since_recording,prog.utc_spot_start_date_time) end,
        CASE WHEN cap.viewing_stops <  dateadd(second,tme.time_in_seconds_since_recording,prog.utc_spot_end_date_time)
             then cap.viewing_stops else dateadd(second,tme.time_in_seconds_since_recording,prog.utc_spot_end_date_time) end,
        ta.scaling_weighting
FROM    vespa_analysts.VESPA_DAILY_AUGS_##^^*^*## as cap
JOIN    #temp_account_list ta
ON      cap.account_number = ta.account_number
INNER JOIN
        spot_data_prog_instance as prog
ON      cap.Programme_Trans_Sk = prog.dk_programme_instance_dim and cap.timeshifting  in (''VOSDAL'',''PLAYBACK7'')'
INNER JOIN
        Sk_prod.Vespa_Events_All as tme
ON      cap.cb_row_id=tme.pk_viewing_prog_instance_fact
where   tme.panel_id in (12)
and     (dateadd(second,tme.time_in_seconds_since_recording*-1,viewing_starts) between prog.utc_spot_start_date_time and prog.utc_spot_end_date_time
        or dateadd(second,tme.time_in_seconds_since_recording*-1,viewing_stops)  between prog.utc_spot_start_date_time and prog.utc_spot_end_date_time
        or (dateadd(second,tme.time_in_seconds_since_recording*-1,viewing_starts) < prog.utc_spot_start_date_time
        and dateadd(second,tme.time_in_seconds_since_recording*-1,viewing_stops)> prog.utc_spot_end_date_time))';

-- PHASE 1 TIMESHIFT DATA --

SET @var_sql_timeshift_ph1 = '
INSERT INTO spot_customer_viewing_capped_thin_timeshift_temp
        (pk_spot_id,
        service_key,
        pk_viewing_prog_instance_fact,
        dk_programme_instance_dim,
        account_number,
        subscriber_id,
        broadcast_spot_viewing_starts,
        broadcast_spot_viewing_stops,
        spot_viewing_starts,
        spot_viewing_stops,
        scaling_weighting)
SELECT  prog.pk_spot_id,
        prog.service_key,
        cap.cb_row_id,
        prog.dk_programme_instance_dim,
        cap.account_number,
        cap.subscriber_id,
        CASE WHEN dateadd(second,tme.x_time_in_seconds_since_recording*-1,cap.viewing_starts) > prog.utc_spot_start_date_time then dateadd(second,tme.x_time_in_seconds_since_recording*-1,cap.viewing_starts) else prog.utc_spot_start_date_time end,
        CASE WHEN dateadd(second,tme.x_time_in_seconds_since_recording*-1,cap.viewing_stops) < prog.utc_spot_end_date_time then dateadd(second,tme.x_time_in_seconds_since_recording*-1,cap.viewing_stops) else prog.utc_spot_end_date_time end,
        CASE WHEN cap.viewing_starts > dateadd(second,tme.x_time_in_seconds_since_recording,prog.utc_spot_start_date_time) then cap.viewing_starts else dateadd(second,tme.x_time_in_seconds_since_recording,prog.utc_spot_start_date_time) end,
        CASE WHEN cap.viewing_stops <  dateadd(second,tme.x_time_in_seconds_since_recording,prog.utc_spot_end_date_time) then cap.viewing_stops else dateadd(second,tme.x_time_in_seconds_since_recording,prog.utc_spot_end_date_time) end,
        ta.scaling_weighting
FROM    vespa_analysts.ph1_Vespa_daily_augs_##^^*^*## as cap
JOIN    #temp_account_list ta
ON      cap.account_number = ta.account_number
INNER JOIN
        spot_data_prog_instance_ph1 as prog
ON      cap.Programme_Trans_Sk = prog.dk_programme_instance_dim and cap.timeshifting in (''VOSDAL'',''PLAYBACK7'')
INNER JOIN
        Sk_prod.sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*## as tme
ON      cap.cb_row_id=tme.cb_row_id
where   dateadd(second,tme.x_time_in_seconds_since_recording*-1,viewing_starts) between prog.utc_spot_start_date_time and prog.utc_spot_end_date_time
        or dateadd(second,tme.x_time_in_seconds_since_recording*-1,viewing_stops)  between prog.utc_spot_start_date_time and prog.utc_spot_end_date_time
        or (dateadd(second,tme.x_time_in_seconds_since_recording*-1,viewing_starts) < prog.utc_spot_start_date_time
        and  dateadd(second,tme.x_time_in_seconds_since_recording*-1,viewing_stops)> prog.utc_spot_end_date_time)';

-- LIVE DATA --

-- PHASE 1 LIVE DATA --

SET @var_sql_live_ph1 = '
INSERT INTO spot_customer_viewing_capped_thin
        (pk_spot_id,
        service_key,
        pk_viewing_prog_instance_fact,
        dk_programme_instance_dim,
        account_number,
        subscriber_id,
        broadcast_spot_viewing_starts,
        broadcast_spot_viewing_stops,
        spot_viewing_starts,
        spot_viewing_stops,
        scaling_weighting)
SELECT  prog.pk_spot_id,
        prog.service_key,
        cap.cb_row_id,
        prog.dk_programme_instance_dim,
        cap.account_number,
        cap.subscriber_id,
        CASE WHEN cap.viewing_starts > prog.utc_spot_start_date_time then cap.viewing_starts else prog.utc_spot_start_date_time end,
        CASE WHEN cap.viewing_stops < prog.utc_spot_end_date_time then cap.viewing_stops else prog.utc_spot_end_date_time end,
        CASE WHEN cap.viewing_starts > prog.utc_spot_start_date_time then cap.viewing_starts else prog.utc_spot_start_date_time end,
        CASE WHEN cap.viewing_stops <  prog.utc_spot_end_date_time then cap.viewing_stops else prog.utc_spot_end_date_time end,
        ta.scaling_weighting
FROM    vespa_analysts.ph1_Vespa_daily_augs_##^^*^*## as cap
JOIN    #temp_account_list ta
ON      cap.account_number = ta.account_number
INNER JOIN
        spot_data_prog_instance_ph1 as prog
ON      cap.timeshifting = ''LIVE''
AND     cap.Programme_Trans_Sk = prog.dk_programme_instance_dim
AND     (cap.viewing_starts between prog.utc_spot_start_date_time and prog.utc_spot_end_date_time
OR       cap.viewing_stops between prog.utc_spot_start_date_time and prog.utc_spot_end_date_time
OR      (cap.viewing_starts < prog.utc_spot_start_date_time and cap.viewing_stops > prog.utc_spot_end_date_time))';


-- PHASE 2 LIVE DATA --

SET @var_sql_live = '
INSERT INTO spot_customer_viewing_capped_thin
        (pk_spot_id,
        service_key,
        pk_viewing_prog_instance_fact,
        dk_programme_instance_dim,
        account_number,
        subscriber_id,
        broadcast_spot_viewing_starts,
        broadcast_spot_viewing_stops,
        spot_viewing_starts,
        spot_viewing_stops,
        scaling_weighting)
SELECT  prog.pk_spot_id,
        prog.service_key,
        cap.cb_row_id,
        prog.dk_programme_instance_dim,
        cap.account_number,
        cap.subscriber_id,
        CASE WHEN cap.viewing_starts > prog.utc_spot_start_date_time then cap.viewing_starts else prog.utc_spot_start_date_time end,
        CASE WHEN cap.viewing_stops < prog.utc_spot_end_date_time then cap.viewing_stops else prog.utc_spot_end_date_time end,
        CASE WHEN cap.viewing_starts > prog.utc_spot_start_date_time then cap.viewing_starts else prog.utc_spot_start_date_time end,
        CASE WHEN cap.viewing_stops <  prog.utc_spot_end_date_time then cap.viewing_stops else prog.utc_spot_end_date_time end,
        ta.scaling_weighting
FROM    vespa_analysts.VESPA_DAILY_AUGS_##^^*^*## as cap
JOIN    #temp_account_list ta
ON      cap.account_number = ta.account_number
INNER JOIN
        spot_data_prog_instance as prog
ON      cap.timeshifting = ''LIVE''
AND     cap.Programme_Trans_Sk = prog.dk_programme_instance_dim
AND     (cap.viewing_starts between prog.utc_spot_start_date_time and prog.utc_spot_end_date_time
OR       cap.viewing_stops between prog.utc_spot_start_date_time and prog.utc_spot_end_date_time
OR      (cap.viewing_starts < prog.utc_spot_start_date_time and cap.viewing_stops > prog.utc_spot_end_date_time))';

---------------------------------------------------------
-- FINAL SETUP BEFORE LOOPING THROUGH DAILY AUG TABLES --
---------------------------------------------------------

-- CREATE SOME TEMP TABLES TO SPEED THINGS UP --

IF object_id('#temp_account_list') IS NOT NULL DROP TABLE #temp_account_list;

CREATE TABLE #temp_account_list (
account_number varchar(20) default null,
scaling_weighting real default null);

-- CREATE AN AUDIT LOG SO CAN MONITOR RUN TIMES --


IF object_id('#audit_log') IS NOT NULL DROP TABLE #audit_log;

Create table #audit_log
(audit_action varchar(255) default null,
audit_timestamp timestamp default null,
audit_records int default null);


-- CLEAR OUT ANY OLD DATA --
DELETE FROM spot_customer_viewing_capped_thin;
DELETE FROM spot_customer_viewing_capped_thin_timeshift_temp;

-- FINAL VARIABLE SETUP --

SET @var_period_start  = '2012-05-01';
SET @var_period_end    = '2012-06-07';
SET @scanning_day = @var_period_start;
DELETE FROM #audit_log;

-----------------------------------------------------------------------------
--                              NB.                                        --
-----------------------------------------------------------------------------
-- NEED TO RUN FROM HERE TO LINE 720 IN ONE GO                             --
-- THE FOLLOWING CHUNK OF CODE WILL LOOP THROUGH ALL THE DAILY AUG TABLES  --
-- WITHIN THE TIME PERIOD SPECIFIED IN LINES 611 AND 612                   --
-----------------------------------------------------------------------------

INSERT into #audit_log
SELECT 'Process starts',now(),0

while @scanning_day <= dateadd(dd,0,@var_period_end)
begin

-- CREATE LIST OF ACCOUNTS THAT HAVE VALID SCALING WEIGHTS
    DELETE FROM #temp_account_list

-- THIS NEXT BIT OF CODE ACCOUNTS FOR DIFFERENT SCALING WEIGHTS USED FOR DIFFERENT PERIODS
    IF @scanning_day between '2012-08-01' and '2012-08-31'
    BEGIN
            INSERT INTO #temp_account_list
            SELECT l.account_number,
                   s.weighting
            FROM bednaszs.v_SC2_intervals as l
            JOIN bednaszs.v_SC2_weightings as s
                ON l.scaling_segment_ID = s.scaling_segment_ID and @scanning_day between l.reporting_starts and l.reporting_ends
                and s.scaling_day = @scanning_day
    END
    ELSE
    BEGIN
            INSERT INTO #temp_account_list
            SELECT l.account_number,
                   s.weighting
            FROM vespa_analysts.SC2_intervals as l
            JOIN vespa_analysts.SC2_weightings as s
                ON l.scaling_segment_ID = s.scaling_segment_ID and @scanning_day between l.reporting_starts and l.reporting_ends
                and s.scaling_day = @scanning_day
    END

    IF @scanning_day >= '2012-08-01'
        EXECUTE(replace(@var_sql_live,'##^^*^*##',dateformat(@scanning_day, 'yyyymmdd')))
    ELSE
        EXECUTE(replace(@var_sql_live_ph1,'##^^*^*##',dateformat(@scanning_day, 'yyyymmdd')))
    commit

        INSERT into #audit_log
        SELECT @scanning_day || ' Live completed',now(),(SELECT count(1) from spot_customer_viewing_capped_thin)

-- CLEAR OUT TEMP FILE
    DELETE FROM spot_customer_viewing_capped_thin_timeshift_temp
    IF @scanning_day >= '2012-08-01'
        EXECUTE(replace(@var_sql_timeshift,'##^^*^*##',dateformat(@scanning_day, 'yyyymmdd')))
    ELSE
        EXECUTE(replace(@var_sql_timeshift_ph1,'##^^*^*##',dateformat(@scanning_day, 'yyyymmdd')))
    commit

-- DE-DUP TIMESHIFT DATA
   INSERT INTO spot_customer_viewing_capped_thin
        (pk_spot_id,
        service_key,
        pk_viewing_prog_instance_fact,
        dk_programme_instance_dim,
        account_number,
        subscriber_id,
        broadcast_spot_viewing_starts,
        broadcast_spot_viewing_stops,
        spot_viewing_starts,
        spot_viewing_stops,
        scaling_weighting)
   SELECT       pk_spot_id,
                service_key,
                pk_viewing_prog_instance_fact,
                dk_programme_instance_dim,
                account_number,
                subscriber_id,
                broadcast_spot_viewing_starts,
                broadcast_spot_viewing_stops,
                spot_viewing_starts,
                spot_viewing_stops,
                scaling_weighting
   FROM (
           SELECT       pk_spot_id,
                        service_key,
                        pk_viewing_prog_instance_fact,
                        dk_programme_instance_dim,
                        account_number,
                        subscriber_id,
                        broadcast_spot_viewing_starts,
                        broadcast_spot_viewing_stops,
                        spot_viewing_starts,
                        spot_viewing_stops,
                        scaling_weighting,
                        flag =1,
                        sum(flag) over (partition by pk_viewing_prog_instance_fact,pk_spot_id order by pk_spot_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as dupe_rank
           FROM spot_customer_viewing_capped_thin_timeshift_temp
           ) a
   WHERE dupe_rank = 1

        INSERT into #audit_log
        SELECT @scanning_day || ' Timeshift completed',now(),(SELECT count(1) from spot_customer_viewing_capped_thin)

        set @scanning_day = dateadd(day, 1, @scanning_day)
end;

SELECT * FROM #audit_log;


-- ADD ON FIELD SHOWING THE PROPORTION OF THE SPOT THAT WAS VIEWED --
-- NB. THIS WILL NOT WORK FOR SPOTS THAT BRIDGE TWO PROGRAMMES     --

UPDATE spot_customer_viewing_capped_thin sv
SET    spot_viewed_duration = datediff(second,broadcast_spot_viewing_starts,broadcast_spot_viewing_stops)
FROM   spot_customer_viewing_capped_thin sv
JOIN   spot_data sd
on     sv.pk_spot_id = sd.pk_spot_id;


--------------------
-- QA DIAGNOSTICS --
--------------------

-- CHECK FOR DUPLICATES --

select count(1)
from (
SELECT distinct pk_viewing_prog_instance_fact, pk_spot_id
from spot_customer_viewing_capped_thin) a;

-- SUMMARY BY BARB DATE FOR QA RECONCILIATION --

select     barb_date_of_transmission
          ,clearcast_commercial_no
          ,sum(scaling_weighting) as hh_impacts_u_w
from       spot_customer_viewing_capped_thin st
join       spot_data sd
on         st.pk_spot_id = sd.pk_spot_id
where      st.spot_viewed_duration = sd.spot_duration
group by   barb_date_of_transmission
          ,clearcast_commercial_no
order by   barb_date_of_transmission
          ,clearcast_commercial_no;

select top 10 * from spot_customer_viewing_capped_thin;


-- CREATE SOLD SPOT SUMMARY --


SELECT  a.te_channel,
        a.te_log_station_code,
        a.te_sti,
        a.barb_date_of_transmission,
        left(a.barb_spot_start_time,2) || ':' || substr(a.barb_spot_start_time,3,2) || ':' || right(a.barb_spot_start_time,2) as barb_spot_start_time,
        a.log_station_code,
        a.sti_code,
        a.clearcast_commercial_no,
        CASE when b.hh_impacts_u_w is null then 0 else b.hh_impacts_u_w end as hh_impacts_u_w
FROM (
        SELECT  distinct lsp.te_channel,
                lsp.te_log_station_code,
                lsp.te_sti,
                sd.barb_date_of_transmission,
                sd.barb_spot_start_time,
                sd.log_station_code,
                sd.sti_code,
                sd.clearcast_commercial_no
        FROM spot_data sd
        LEFT JOIN vespa_analysts.channel_map_dev_log_station_panel lsp
                ON sd.log_station_code = lsp.log_station_code and sd.sti_code = lsp.sti_code
        ) a
LEFT JOIN
        (
        SELECT  sd.log_station_code,
                sd.sti_code,
                sd.clearcast_commercial_no,
                sd.barb_date_of_transmission,
                sd.barb_spot_start_time,
                sum(scaling_weighting) as hh_impacts_u_w
        FROM spot_customer_viewing_capped_thin st
        JOIN spot_data sd
             ON st.pk_spot_id = sd.pk_spot_id
        WHERE st.spot_viewed_duration = sd.spot_duration
        GROUP BY        sd.log_station_code,
                        sd.sti_code,
                        sd.clearcast_commercial_no,
                        sd.barb_date_of_transmission,
                        sd.barb_spot_start_time
        ) b
ON      a.log_station_code = b.log_station_code
        and a.sti_code = b.sti_code
        and a.barb_date_of_transmission = b.barb_date_of_transmission
        and a.barb_spot_start_time = b.barb_spot_start_time
order by   a.barb_date_of_transmission
          ,barb_spot_start_time
          ,te_channel;






