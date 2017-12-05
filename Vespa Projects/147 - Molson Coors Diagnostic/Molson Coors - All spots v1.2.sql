------------------------------------------------------------------------
--                                                                    --
--        Project: Molson Coors - May Carling Campaign                --
--        Description: Collates Viewing data for all spots            --
--        Version: v1.0                                               --
--        Created: 20/04/2012                                         --
--        Creaded by: Martin Neighbours                               --
--        Lead:                                                       --
--        Analyst: Hannah Starmer                                     --
--        SK Prod: 5                                                  --
--                                                                    --
--                                                                    --
--        PART A.  Viewing Data                                       --
--        PART B.  Impacts Table                                      --
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


--------------------------
-- PART A. VIEWING DATA --
--------------------------


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


-------------------------------
-- THINGS YOU NEED TO CHANGE --
-------------------------------

SET @var_period_start       = '2012-05-01';
SET @var_period_end         = '2012-06-07';
SET @barb_spot_period_start = '2012-05-01';
SET @barb_spot_period_end   = '2012-05-31';



-----------------------------------
-- SETUP TABLE TEMPLATES WE NEED --
-----------------------------------

-- TEMPLATE FOR MAIN OUTPUT TABLE --

if object_id('all_spot_customer_viewing_capped_thin') is not null drop table all_spot_customer_viewing_capped_thin;

create table all_spot_customer_viewing_capped_thin
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

if object_id('all_spot_customer_viewing_capped_thin_timeshift_temp') is not null drop table all_spot_customer_viewing_capped_thin_timeshift_temp;

create table all_spot_customer_viewing_capped_thin_timeshift_temp (
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



if object_id('all_spot_data') is not null drop table all_spot_data;

CREATE TABLE all_spot_data (
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
        mid_break_spot                          int DEFAULT NULL,
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

if object_id('all_spot_data_prog_instance') is not null drop table all_spot_data_prog_instance;

CREATE TABLE all_spot_data_prog_instance (
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


create hg index idx1 on all_spot_data_prog_instance(dk_programme_instance_dim);

-- PROGRAMME AND SPOT DATA FOR PHASE 1 DATA --

if object_id('all_spot_data_prog_instance_ph1') is not null drop table all_spot_data_prog_instance_ph1;

CREATE TABLE all_spot_data_prog_instance_ph1 (
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

create hg index idx1 on all_spot_data_prog_instance_ph1(dk_programme_instance_dim);

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

DELETE FROM all_spot_data;

INSERT INTO all_spot_data (
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
        ,mid_break_spot
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
        ,round((a.no_spots_in_break/2),0) as mid_break_spot
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
LEFT OUTER JOIN
         VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES  B
ON       A.service_key=B.service_key
AND      A.local_date_of_transmission between B.effective_from and B.effective_to
WHERE    barb_date_of_transmission between  @barb_spot_period_start and  @barb_spot_period_end;

select count(*) from all_spot_data;

--1862787
delete from all_spot_data where mid_break_spot<>spot_position_in_break;

--1602334 Row(s) affected

----------------------------------------------------------------------
-- UPDATE TO PUT IN RATING FACTOR                                   --
-- GOING FORWARDS THIS CAN BE TAKEN DIRECTLY FROM MASTER SPOT TABLE --
----------------------------------------------------------------------

alter table all_spot_data
 add ratecard_weighting decimal(7,3);

update  all_spot_data
set     a.ratecard_weighting=b.ratecard_weighting
from    all_spot_data                           A
left outer join
        neighbom.barb_master_spot_data      B
on      a.pk_spot_id = b.pk_spot_id;


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

DELETE FROM all_spot_data_prog_instance;
INSERT INTO all_spot_data_prog_instance (
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
JOIN    all_spot_data scd
ON      scd.service_key = vps.service_key AND (scd.utc_spot_start_date_time between vps.broadcast_start_date_time_utc and vps.broadcast_end_date_time_utc
                                               OR scd.utc_spot_end_date_time between vps.broadcast_start_date_time_utc and vps.broadcast_end_date_time_utc);

-- PHASE 1 DATA --

DELETE FROM all_spot_data_prog_instance_ph1;
INSERT INTO all_spot_data_prog_instance_ph1 (
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
JOIN    all_spot_data scd
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
INSERT INTO all_spot_customer_viewing_capped_thin_timeshift_temp
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
        all_spot_data_prog_instance as prog
ON      cap.Programme_Trans_Sk = prog.dk_programme_instance_dim and cap.timeshifting  in (''VOSDAL'',''PLAYBACK7'')
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
INSERT INTO all_spot_customer_viewing_capped_thin_timeshift_temp
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
        all_spot_data_prog_instance_ph1 as prog
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
INSERT INTO all_spot_customer_viewing_capped_thin
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
        all_spot_data_prog_instance_ph1 as prog
ON      cap.timeshifting = ''LIVE''
AND     cap.Programme_Trans_Sk = prog.dk_programme_instance_dim
AND     (cap.viewing_starts between prog.utc_spot_start_date_time and prog.utc_spot_end_date_time
OR       cap.viewing_stops between prog.utc_spot_start_date_time and prog.utc_spot_end_date_time
OR      (cap.viewing_starts < prog.utc_spot_start_date_time and cap.viewing_stops > prog.utc_spot_end_date_time))';


-- PHASE 2 LIVE DATA --

SET @var_sql_live = '
INSERT INTO all_spot_customer_viewing_capped_thin
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
        all_spot_data_prog_instance as prog
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
DELETE FROM all_spot_customer_viewing_capped_thin;
DELETE FROM all_spot_customer_viewing_capped_thin_timeshift_temp;

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
    IF @scanning_day between '2011-08-01' and '2012-08-31'

    BEGIN
            INSERT INTO #temp_account_list
            SELECT l.account_number,
                   s.weighting
            FROM vespa_analysts.SC2_intervals as l
            JOIN vespa_analysts.SC2_weightings as s
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
        SELECT @scanning_day || ' Live completed',now(),(SELECT count(1) from all_spot_customer_viewing_capped_thin)

-- CLEAR OUT TEMP FILE
    DELETE FROM all_spot_customer_viewing_capped_thin_timeshift_temp
    IF @scanning_day >= '2012-08-01'
        EXECUTE(replace(@var_sql_timeshift,'##^^*^*##',dateformat(@scanning_day, 'yyyymmdd')))
    ELSE
        EXECUTE(replace(@var_sql_timeshift_ph1,'##^^*^*##',dateformat(@scanning_day, 'yyyymmdd')))
    commit

-- DE-DUP TIMESHIFT DATA
   INSERT INTO all_spot_customer_viewing_capped_thin
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
           FROM all_spot_customer_viewing_capped_thin_timeshift_temp
           ) a
   WHERE dupe_rank = 1

        INSERT into #audit_log
        SELECT @scanning_day || ' Timeshift completed',now(),(SELECT count(1) from spot_customer_viewing_capped_thin)

        set @scanning_day = dateadd(day, 1, @scanning_day)
end;

SELECT * FROM #audit_log


-- ADD ON FIELD SHOWING THE PROPORTION OF THE SPOT THAT WAS VIEWED --

---------------------------------------------------------------------
--                              NB.                                --
---------------------------------------------------------------------
-- (1) THIS WILL NOT WORK FOR SPOTS THAT BRIDGE TWO PROGRAMMES     --
-- (2) VIEWING TABLE WILL INCLUDE PARTIALLY VIEWED SPOTS           --
---------------------------------------------------------------------

UPDATE all_spot_customer_viewing_capped_thin sv
SET    spot_viewed_duration = datediff(second,broadcast_spot_viewing_starts,broadcast_spot_viewing_stops)
FROM   all_spot_customer_viewing_capped_thin sv
JOIN   all_spot_data sd
on     sv.pk_spot_id = sd.pk_spot_id;


--------------------
-- QA DIAGNOSTICS --
--------------------

-- CHECK FOR DUPLICATES --

select count(1)
from (
SELECT distinct pk_viewing_prog_instance_fact, pk_spot_id
from all_spot_customer_viewing_capped_thin) a;

-- SUMMARY BY BARB DATE FOR QA RECONCILIATION --

select     barb_date_of_transmission
          ,clearcast_commercial_no
          ,sum(scaling_weighting) as hh_impacts_u_w
from       all_spot_customer_viewing_capped_thin st
join       all_spot_data sd
on         st.pk_spot_id = sd.pk_spot_id
where      st.spot_viewed_duration = sd.spot_duration
group by   barb_date_of_transmission
          ,clearcast_commercial_no
order by   barb_date_of_transmission
          ,clearcast_commercial_no;


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
        FROM all_spot_data sd
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
        FROM all_spot_customer_viewing_capped_thin st
        JOIN all_spot_data sd
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


---------------------------
-- PART B. IMPACTS TABLE --
---------------------------

--------------------------------------------------------
--         PART B: CREATE CLIENT SPOTS FILE           --
--------------------------------------------------------

-- PULL DATA TOGETHER TO CREATE AN IMPACTS TABLE --

drop table allspots_may_viewing_data;
select      a.*
           ,c.barb_date_of_transmission
           ,left(c.barb_spot_start_time,2) || ':' || substr(c.barb_spot_start_time,3,2) || ':' || right(c.barb_spot_start_time,2) as barb_spot_start_time
           ,c.barb_spot_start_time as barb_spot_start_unformated
           ,case when cast(c.barb_spot_start_time as integer)
                between 60000 and 085959 then 'Breakfast Time'
                when cast(c.barb_spot_start_time as integer)
                between 090000 and 172959 then 'Daytime'
                when cast(c.barb_spot_start_time as integer)
                between 173000 and 195959 then 'Early Peak'
                when cast(c.barb_spot_start_time as integer)
                between 200000 and 225999 then 'Late Peak'
                when cast(c.barb_spot_start_time as integer)
                between 230000 and 235959 then 'Post Peak'
                when cast(c.barb_spot_start_time as integer)
                between 240000 and 242959 then 'Post Peak'
                when cast(c.barb_spot_start_time as integer)
                between 243000 and 295959 then 'Night Time'
            else 'Unknown'
            end as daypart
           ,c.clearcast_commercial_no
           ,case when c.ratecard_weighting is null then cast(c.spot_duration as decimal (7,3))/cast(30 as decimal (7,3))
                 else c.ratecard_weighting end as ratecard_weighting
           ,(cast((scaling_weighting*ratecard_weighting) as float)) as adjusted_scaling
           ,b.genre_description
           ,b.sub_genre_description
           ,b.programme_name
into       allspots_may_viewing_data
from       all_spot_customer_viewing_capped_thin    A
left join
           all_spot_data                            C
on         a.pk_spot_id=c.pk_spot_id
left join
           all_spot_data_prog_instance_ph1          B
on         a.pk_spot_id=b.pk_spot_id
and        a.dk_programme_instance_dim=b.dk_programme_instance_dim
where      a.spot_viewed_duration=c.spot_duration;


-- QA THAT WE ARE GETTING SAME TOTAL IMPACTS AS AT END OF VIEWING CODE --

select    barb_date_of_transmission
         ,clearcast_commercial_no
         ,sum(scaling_weighting)
from      allspots_may_viewing_data
group by  barb_date_of_transmission
         ,clearcast_commercial_no;


------------------------------------
-- APPEND A FEW ADDITIONAL FIELDS --
------------------------------------


alter table allspots_may_viewing_data
 add      (cb_key_household            bigint
          ,channel_new                 varchar(50)
          ,media_pack                  varchar(25)
          ,sales_house                 varchar(25)
          ,hh_contain_18_to_24         integer
          ,hh_contain_16_to_34         integer
          ,hh_contain_over_18          integer
          ,hh_contain_male             integer
          ,brought_audience            integer
          ,aspirational_audience       integer
          ,buckle_segments             varchar(25)
          ,brought_audience_hh         integer
          ,aspirational_audience_hh    integer
          ,social_explorers_hh         integer
          ,enthusiastic_influencers_hh integer
          ,content_routiners_hh        integer
          ,safe_and_savy_hh            integer
          ,considered_balancers_hh     integer
          ,do_not_targets_hh           integer);


-- CB_KEY_HOUSEHOLD FIELD --

drop table acct_hh;
select  distinct account_number
       ,cb_key_household
into    acct_hh
from    sk_prod.cust_single_account_view
where   account_number in (select distinct account_number from allspots_may_viewing_data);

update      allspots_may_viewing_data
set         a.cb_key_household=b.cb_key_household
from        allspots_may_viewing_data     A
left join
            acct_hh                  B
on          a.account_number=b.account_number;


-- CHANNEL NAME --

-- CREATE CHANNEL LOOKUP --
/*
select
      distinct lsp.te_channel,
      sd.*
into #spot_additions
FROM      all_spot_data sd
LEFT JOIN vespa_analysts.channel_map_dev_log_station_panel lsp
ON        sd.log_station_code = lsp.log_station_code
and       sd.sti_code = lsp.sti_code;

select service_key,te_channel,count(*)
into #service
from #spot_additions
group by service_key,te_channel;


--drop table te_channel_lkup;
select a.service_key
      ,a.te_channel
      ,case when a.service_key in (2402) then 'Animal Planet'
            when a.service_key in (1873) then 'Bio'
            when a.service_key in (3619) then 'Bio +1'
            when a.service_key in (1621,1622,1624) then 'CH4'
            when a.service_key in (1671,1673) then 'CH4+1'
            when a.service_key in (1800,1801,1828,1829) then 'Channel 5'
            when a.service_key in (1448) then 'CI Network '
            when a.service_key in (2510) then 'Comedy Central'
            when a.service_key in (2306) then 'Dave'
            when a.service_key in (2401) then 'Discovery'
            when a.service_key in (1627) then 'Film4'
            when a.service_key in (1628) then 'E4'
            when a.service_key in (2302) then 'Eden'
            when a.service_key in (3141) then 'ESPN'
            when a.service_key in (1305) then 'FOX'
            when a.service_key in (2308) then 'Good Food'
            when a.service_key in (1875) then 'History'
            when a.service_key in (6240) then 'ITV2'
            when a.service_key in (6015,6130,6140,6141,6142,6143,6160,6180,6089,6011,6390,6210,6220) then 'ITV1'
            when a.service_key in (6125,6128) then 'ITV1 +1'
            when a.service_key in (6260,6260,6533) then 'ITV3'
            when a.service_key in (6261)  then 'ITV3+1'
            when a.service_key in (6272) then 'ITV4'
            when a.service_key in (3340) then 'More4 '
            when a.service_key in (1806) then 'National Geographic'
            when a.service_key in (1847) then 'National Geographic Wild'
            when a.service_key in (1402) then 'Sky 1'
            when a.service_key in (1752) then 'Sky Arts 1'
            when a.service_key in (1753) then 'Sky Arts 2'
            when a.service_key in (1412) then 'Sky Atlantic'
            when a.service_key in (2201) then 'Sky Living'
            when a.service_key in (1404) then 'Sky News'
            when a.service_key in (1301,4002) then 'Sky Sports 1'
            when a.service_key in (1302,4081) then 'Sky Sports 2'
            when a.service_key in (1333,4022) then 'Sky Sports 3'
            when a.service_key in (1322,4026) then 'Sky Sports 4'
            when a.service_key in (1471) then 'Sky Sports Active 1'
            when a.service_key in (1472) then 'Sky Sports Active 2'
            when a.service_key in (1473) then 'Sky Sports Active 3'
            when a.service_key in (1474) then 'Sky Sports Active 4'
            when a.service_key in (1475) then 'Sky Sports Active 5'
            when a.service_key in (1306,3835) then 'Sky Sports F1'
            when a.service_key in (1314,4049) then 'Sky Sports News'
            when a.service_key in (1842) then 'Universal'
            when a.service_key in (2617) then 'Watch'
            when a.service_key in (4077) then 'Cartoon Network'
            when a.service_key in (1843) then 'Disney XD'
            when a.service_key in (2501) then 'MTV'
            when a.service_key in (3508,4006) then 'MTV Live'
            when a.service_key in (1846) then 'Nickelodeon uk'
            when a.service_key in (3630) then 'Sahara One'
            when a.service_key in (1814) then 'Sky Movies Showcase'
            when a.service_key in (1001) then 'Sky Movies Action & Adventure'
            when a.service_key in (1812) then 'Sky Movies Classics'
            when a.service_key in (1002) then 'Sky Movies Comedy'
            when a.service_key in (1818) then 'Sky Movies Crime & Thriller'
            when a.service_key in (1816) then 'Sky Movies Drama & Romance'
            when a.service_key in (1808) then 'Sky Movies Family'
            when a.service_key in (1811) then 'Sky Movies Indie'
            when a.service_key in (1815,4015) then 'Sky Movies Modern Greats'
            when a.service_key in (1409) then 'Sky Movies Premiere'
            when a.service_key in (1807) then 'Sky Movies SciFi/Horror'
            when a.service_key in (1771) then 'Star Plus'
            when a.service_key in (2505) then 'Syfy'
            when a.service_key in (1371)  then 'Cartoonito'
            when a.service_key in (1372)  then 'TCM 2'
            when a.service_key in (1771)  then 'STAR Plus'
            when a.service_key in (1808,4018)  then 'Sky Family'
            when a.service_key in (1842)  then 'Universal'
            when a.service_key in (1843,4070)  then 'Disney XD'
            when a.service_key in (1844)  then 'Disney XD+1'
            when a.service_key in (1845)  then 'Nick Replay'
            when a.service_key in (1846,4069)  then 'Nickelodeon'
            when a.service_key in (1849)  then 'Nicktoons'
            when a.service_key in (1857)  then 'Nick Jr.'
            when a.service_key in (1872)  then 'Community Channel'
            when a.service_key in (2606)  then 'Zing'
            when a.service_key in (2619)  then 'HiTV'
            when a.service_key in (3001)  then 'Horse & Country'
            when a.service_key in (3104)  then 'UMP Stars'
            when a.service_key in (3108)  then 'Vox Africa'
            when a.service_key in (3207)  then 'Universal+1'
            when a.service_key in (3220)  then 'Men & Movies  +1'
            when a.service_key in (3251)  then 'UMP Movies'
            when a.service_key in (3408)  then 'Sunrise TV'
            when a.service_key in (3531)  then 'Vintage TV'
            when a.service_key in (3541)  then 'Men & Movies'
            when a.service_key in (3608)  then 'Star Life OK'
            when a.service_key in (3613)  then 'STAR Gold'
            when a.service_key in (3630)  then 'Sahara One'
            when a.service_key in (3631)  then 'SONY SAB'
            when a.service_key in (3643)  then 'True Movies 1'
            when a.service_key in (3708)  then 'movies4men'
            when a.service_key in (3721)  then 'Movies4Men +1'
            when a.service_key in (3731)  then 'My Channel'
            when a.service_key in (3732)  then 'LiverpoolFCTV'
            when a.service_key in (3735)  then 'Motors TV'
            when a.service_key in (3750)  then 'POP'
            when a.service_key in (3751)  then 'True Movies 2'
            when a.service_key in (3780)  then 'Tiny Pop'
            when a.service_key in (3806)  then 'ARY World'
            when a.service_key in (5601,4077)  then 'Cartoon Network'
            when a.service_key in (3028) then '5* +1'
            when a.service_key in (2413) then 'Investigation'
            else te_channel
       end as channel
into te_channel_lkup
from #service  a;

select * from te_channel_lkup;
*/



update            allspots_may_viewing_data
set               cub.channel_new= tmp.channel
from              allspots_may_viewing_data as cub
left outer join   te_channel_lkup as tmp
on                tmp.service_key = cub.service_key;


-- MEDIA PACK AND SALES HOUSE --
/*
select   ska.service_key as service_key
        ,ska.full_name
        ,PACK.NAME
        ,cgroup.primary_sales_house
        ,(case when pack.name is null then cgroup.channel_group
                else pack.name end) as channel_category
into #packs
from vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES ska
left join
        (select a.service_key
               ,b.name
         from   vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK a
         join   neighbom.CHANNEL_MAP_DEV_LANDMARK_CHANNEL_PACK_LOOKUP b
         on     a.sare_no between b.sare_no and b.sare_no + 999
         where  a.service_key <> 0) pack
     on ska.service_key = pack.service_key
left join
        (select distinct a.service_key
               ,b.primary_sales_house
               ,b.channel_group
         from   vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_BARB a
         join   neighbom.CHANNEL_MAP_DEV_BARB_CHANNEL_GROUP b
         on a.log_station_code = b.log_station_code
         and a.sti_code = b.sti_code
         where service_key <>0) cgroup
    on   ska.service_key = cgroup.service_key
where    cgroup.primary_sales_house is not null
order by cgroup.primary_sales_house
        ,channel_category;

--if object_id('LkUpPack') is not null drop table LkUpPack;
SELECT    primary_sales_house
         ,service_key
         ,full_name
         ,(case
                when service_key = 3777 OR service_key = 6756 then 'LIFESTYLE & CULTURE'
                when service_key = 4040 then 'SPORTS'
                when service_key = 1845 OR service_key = 4069 OR service_key = 1859 then 'KIDS'
                when service_key = 4006 then 'MUSIC'
                when service_key = 3621 OR service_key = 4080 then 'ENTERTAINMENT'
                when service_key = 3760 then 'DOCUMENTARIES'
                when service_key = 1757 then 'MISCELLANEOUS'
                when service_key = 3639 OR service_key = 4057 then 'Media Partners'
          else channel_category END) AS channel_category
INTO      LkUpPack
FROM     #packs
order by  primary_sales_house
         ,channel_category;
*/

update            allspots_may_viewing_data
set               cub.media_pack = tmp.channel_category
                  ,cub.sales_house = tmp.primary_sales_house
from              allspots_may_viewing_data as cub
left outer join   LkUpPack as tmp
on                tmp.service_key = cub.service_key;

-- HOUSEHOLD FLAGS --


update        allspots_may_viewing_data
set           a.hh_contain_18_to_24 = b.hh_contain_18_to_24
             ,a.hh_contain_16_to_34 = b.hh_contain_16_to_34
             ,a.hh_contain_over_18 = b.hh_contain_over_18
             ,a.hh_contain_male = b.hh_contain_male
             ,a.brought_audience = b.brought_audience
             ,a.aspirational_audience = b.aspirational_audience
             ,a.buckle_segments = b.buckle_segments
from          allspots_may_viewing_data      A
left join
              carling_hh_aggregate      B
on            a.cb_key_household=b.cb_key_household;



update         allspots_may_viewing_data
set            a.brought_audience_hh = (case when a.brought_audience=1 then a.adjusted_scaling else 0 end)
              ,a.aspirational_audience_hh  = (case when a.aspirational_audience=1 then a.adjusted_scaling else 0 end)
              ,a.social_explorers_hh  = (case when a.buckle_segments in ('Social Explorers') then a.adjusted_scaling else 0 end)
              ,a.enthusiastic_influencers_hh  = (case when a.buckle_segments in ('Enthusiastic Influencers') then a.adjusted_scaling else 0 end)
              ,a.content_routiners_hh  = (case when a.buckle_segments in ('Content Routiners') then a.adjusted_scaling else 0 end)
              ,a.safe_and_savy_hh  = (case when a.buckle_segments in ('Safe & Savvy') then a.adjusted_scaling else 0 end)
              ,a.considered_balancers_hh  = (case when a.buckle_segments in ('Considered Balancers') then a.adjusted_scaling else 0 end)
              ,a.do_not_targets_hh  = (case when a.buckle_segments in ('Do Not Target') then a.adjusted_scaling else 0 end)
from           allspots_may_viewing_data        A;

-- QA --
/*
select   barb_date_of_transmission,clearcast_commercial_no,sum(scaling_weighting)
from     carling_viewing_data
group by barb_date_of_transmission,clearcast_commercial_no;
*/

-- IMPACT PIVOT TABLE --


if object_id('allspots_may_impacts') is not null drop table allspots_may_impacts;

select     barb_date_of_transmission
          ,barb_spot_start_time
          ,daypart
          ,clearcast_commercial_no
          ,sales_house
          ,media_pack
          ,channel_new
          ,genre_description
          ,sub_genre_description
          ,programme_name
          ,min(spot_viewed_duration) as spot_duration
          ,sum((cast(scaling_weighting as float))) as total_impacts
          ,sum((cast(brought_audience_hh as float))) as brought_audience_impacts
          ,sum((cast(aspirational_audience_hh as float))) as aspirational_audience_impacts
          ,sum((cast(social_explorers_hh as float))) as social_explorers_impacts
          ,sum((cast(enthusiastic_influencers_hh as float))) as enthusiastic_influencers_impacts
          ,sum((cast(content_routiners_hh as float))) as content_routiners_impacts
          ,sum((cast(safe_and_savy_hh as float))) as safe_and_savy_impacts
          ,sum((cast(considered_balancers_hh as float))) as considered_balancers_impacts
          ,sum((cast(do_not_targets_hh as float))) as do_not_targets_impacts
into       allspots_may_impacts
from       allspots_may_viewing_data
group by   barb_date_of_transmission
          ,barb_spot_start_time
          ,daypart
          ,clearcast_commercial_no
          ,sales_house
          ,media_pack
          ,channel_new
          ,genre_description
          ,sub_genre_description
          ,programme_name;

select * from allspots_may_impacts;


-- ADJUST IMPACTS FOR SPOTS NOT EQUAL TO 30 SECONDS --

-- QA --
/*
select   barb_date_of_transmission,clearcast_commercial_no,sum(total_impacts)
from     allspots_may_impacts
group by barb_date_of_transmission,clearcast_commercial_no;
*/















