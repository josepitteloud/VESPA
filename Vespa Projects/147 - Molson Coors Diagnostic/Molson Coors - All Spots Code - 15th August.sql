------------------------------------------------------------------------
--                                                                    --
--        Project: Molson Coors  - ALL SPOTS CODE                     --
--        Version: v1.0                                               --
--        Created: 12/03/2012                                         --
--        Lead:                                                       --
--        Analyst: Hannah Starmer                                     --
--        SK Prod: 4                                                  --
--                                                                    --
--                                                                    --
--        PART A. SPOT DATA                                           --
--        PART B. VIEWING DATA                                        --
--        PART C. PIVOT                                               --
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

------------
-- SET UP --
------------
CREATE VARIABLE @var_period_start       datetime;
CREATE VARIABLE @var_period_end         datetime;
CREATE VARIABLE @var_sql                varchar(2000);
CREATE VARIABLE @scanning_day           datetime;
CREATE VARIABLE @var_num_days           smallint;
CREATE VARIABLE @var_cntr               smallint;
-- THINGS YOU NEED TO CHANGE --


SET @var_period_start  = '2012-08-15';
SET @var_period_end    = '2012-08-15';


-------------------------------------------------
--         PART A: SPOT DATA                   --
-------------------------------------------------


-- CAMPAIGN SPOT DATA --
-- SELECT THE SPOT DATA THAT IS REQUIRED FOR ANALYSIS --



IF OBJECT_ID('molson_coors_all_spot_data') IS NOT NULL DROP TABLE molson_coors_all_spot_data_15
SELECT   A.clearcast_commercial_no
        ,A.service_key
        ,A.utc_spot_start_date_time
        ,cast(A.utc_spot_start_date_time as date) as utc_start_date
        ,A.utc_spot_end_date_time
        ,A.spot_position_in_break
        ,A.no_spots_in_break
        ,A.spot_duration
        ,A.barb_date_of_transmission
        ,A.barb_spot_start_time
        ,B.Full_Name
        ,a.sti_code
        ,round((no_spots_in_break/2),0) as mid_break
        ,TRIM(B.Full_Name) AS spot_channel_name
INTO     molson_coors_all_spot_data_15
FROM     neighbom.BARB_MASTER_SPOT_DATA                         A
LEFT OUTER JOIN
         VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES  B
ON       A.service_key=B.service_key
AND      A.barb_date_of_transmission between B.effective_from and B.effective_to
WHERE    a.barb_date_of_transmission ='2012-08-15';
-------------------------------------------------
--         PART B: VIEWING DATA                --
-------------------------------------------------




--    COMPILE CAPPING FILES FOR CHOSEN SCALING UNIVERSE    --

--    CREATE TABLE TEMPLATES   --


if object_id('molson_coors_allspots_viewing_data15') is not null drop table molson_coors_allspots_viewing_data15;


create table molson_coors_allspots_viewing_data15 (
 Cb_Row_Id                       bigint      not null
,Account_Number                  varchar(20) not null
,Subscriber_Id                   integer
,cb_key_household                bigint
,Programme_Trans_Sk              bigint      not null
,Timeshifting                    varchar(10)
,Viewing_Starts                  timestamp
,Viewing_Stops                   timestamp
,Viewing_Duration                bigint
,time_in_seconds_since_recording int
,instance_start_date_time_utc      datetime
,original_broadcast_date_time_utc datetime
,instance_duration                int
,service_key                     bigint
,utc_spot_start_date_time              datetime
,utc_spot_end_date_time              datetime
,full_name                    varchar(255)
,spot_duration               int
,clearcast_commercial_no        varchar(25)
,barb_date_of_transmission      datetime
,barb_spot_start_time           int
);
commit;


--    LOOP THROUGH THE DAILY CAPPING TABLES AND POPULATE TABLE     --


insert into molson_coors_allspots_viewing_data15 (
                 Cb_Row_Id
                ,Account_Number
                ,Subscriber_Id
                ,cb_key_household
                ,Programme_Trans_Sk
                ,Timeshifting
                ,Viewing_Starts
                ,Viewing_Stops
                ,Viewing_Duration
                ,time_in_seconds_since_recording
                ,instance_start_date_time_utc
                ,original_broadcast_date_time_utc
                ,instance_duration
                ,service_key
                ,utc_spot_start_date_time
                ,utc_spot_end_date_time
                ,full_name
                ,spot_duration
                ,clearcast_commercial_no
                ,barb_date_of_transmission
                ,barb_spot_start_time          )
    select
                 cap.Cb_Row_Id
                ,cap.Account_Number
                ,cap.Subscriber_Id
                ,b.cb_key_household
                ,cap.Programme_Trans_Sk
                ,cap.Timeshifting
                ,cap.Viewing_Starts
                ,cap.Viewing_Stops
                ,cap.Viewing_Duration
                ,b.time_in_seconds_since_recording
                ,b.instance_start_date_time_utc
                ,original_broadcast_date_time_utc = dateadd(ss,-1*time_in_seconds_since_recording,instance_start_date_time_utc)
                ,datediff(ss,instance_start_date_time_utc,instance_end_date_time_utc) as instance_duration
                ,prog.service_key
                ,spot.utc_spot_start_date_time
                ,spot.utc_spot_end_date_time
                ,spot.full_name
                ,spot.spot_duration
                ,spot.clearcast_commercial_no
                ,spot.barb_date_of_transmission
                ,spot.barb_spot_start_time
from             vespa_analysts.VESPA_DAILY_AUGS_20120815 as cap

inner join       sk_prod.Vespa_events_all as b
on               cap.cb_row_id=b.pk_viewing_prog_instance_fact

inner join       molson_coors_Program_details_ALL_SPOTS as prog
on               cap.Programme_Trans_Sk = prog.dk_programme_instance_dim

inner join       molson_coors_all_spot_data_15 as spot
on               prog.service_key=spot.service_key

where
    (            dateadd(second,time_in_seconds_since_recording*-1,viewing_starts) between utc_spot_start_date_time and utc_spot_end_date_time
        or       dateadd(second,time_in_seconds_since_recording*-1,viewing_stops)  between utc_spot_start_date_time and utc_spot_end_date_time
        or       dateadd(second,time_in_seconds_since_recording*-1,viewing_starts) < utc_spot_start_date_time
        and      dateadd(second,time_in_seconds_since_recording*-1,viewing_stops)> utc_spot_end_date_time
        and      b.panel_id in (12)
    ) ;


-- SPOT VIEWED FLAG --

if object_id('molson_coors_all_spot_data_15_v2') is not null drop table molson_coors_all_spot_data_15_v2;

select *
       ,flag = 1
       ,sum(flag) over (order by utc_spot_start_date_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as identifier
into molson_coors_all_spot_data_15_v2
from molson_coors_all_spot_data_15;



alter table molson_coors_allspots_viewing_data15
        add (spot_viewed integer default 0
           ,spot_identifier integer
           ,barb_spot_start_time   integer
           ,spot_position_in_break integer
           ,no_spots_in_break   integer);

Update molson_coors_allspots_viewing_data15
        set vw.spot_viewed = case when (timeshifting = 'LIVE' and vw.viewing_starts < vw.utc_spot_start_date_time
                                  and  viewing_stops> vw.utc_spot_end_date_time)
                                  or (vw.original_broadcast_date_time_utc < vw.utc_spot_start_date_time
                                  and  dateadd(second,vw.instance_duration,vw.original_broadcast_date_time_utc)> vw.utc_spot_end_date_time)

                                 then 1 else 0 end
            ,vw.spot_identifier = spot.identifier
            ,vw.barb_spot_start_time = spot.barb_spot_start_time
            ,vw.spot_position_in_break = spot.spot_position_in_break
            ,vw.no_spots_in_break = spot.no_spots_in_break
from molson_coors_allspots_viewing_data15  vw
join molson_coors_all_spot_data_15_v2       spot
on   vw.utc_spot_start_date_time    = spot.utc_spot_start_date_time
and  vw.utc_spot_end_date_time = spot.utc_spot_end_date_time
and  vw.service_key = spot.service_key
and  vw.full_name =  spot.full_name
and  vw.clearcast_commercial_no =  spot.clearcast_commercial_no;
;



-- ADD ON SCALING WEIGHTINGS --



ALTER TABLE molson_coors_allspots_viewing_data15
ADD ( weighting_date        date
     ,scaling_segment_ID    int
     ,weightings            float default 0);


update molson_coors_allspots_viewing_data15
set    a.weighting_date=cast(a.original_broadcast_date_time_utc as date)
from   molson_coors_allspots_viewing_data15 A;


update   molson_coors_allspots_viewing_data15
set      scaling_segment_ID = l.scaling_segment_ID
from     molson_coors_allspots_viewing_data15 as b
inner join
         bednaszs.v_SC2_Intervals as l
on       b.account_number = l.account_number
and      b.weighting_date between l.reporting_starts and l.reporting_ends;


update    molson_coors_allspots_viewing_data15
set       weightings = s.weighting
from      molson_coors_allspots_viewing_data15 as b
inner join
          bednaszs.v_SC2_Weightings as s
on        b.weighting_date = s.scaling_day
and       b.scaling_segment_ID = s.scaling_segment_ID;



-- ADD ON MEDIA PACK, SALES HOUSE AND FIX CHANNEL NAME --

-- CHANNEL NAME --




alter table molson_coors_allspots_viewing_data15
        add (channel_new varchar(50));


update            molson_coors_allspots_viewing_data15
set               cub.channel_new= (case when tmp.channel is not null then tmp.channel else cub.full_name end)
from              molson_coors_allspots_viewing_data15 as cub
left outer join   te_channel_lkup as tmp
on                tmp.service_key = cub.service_key;

-- CHANNEL GROUPING (MEDIA PACK) AND SALES HOUSE --


alter table molson_coors_allspots_viewing_data15
        add (media_pack varchar(25)
           ,sales_house varchar(25));

update            molson_coors_allspots_viewing_data15
set               cub.media_pack = tmp.channel_category
                  ,cub.sales_house = tmp.primary_sales_house
from              molson_coors_allspots_viewing_data15 as cub
left outer join   LkUpPack as tmp
on                tmp.service_key = cub.service_key;

-- ADD ON PROGRAMME DETAILS --

alter table molson_coors_allspots_viewing_data15
        add (program_air_date                date
,program_air_datetime            datetime
,program_air_end_datetime        datetime
,programme_duration              int
,broadcast_daypart               varchar(30)
,genre_description               varchar(30)
,sub_genre_description           varchar(30)
,channel_name                    varchar(30)
,programme_name                  varchar(30));


update            molson_coors_allspots_viewing_data15
set               a.program_air_date=b.program_air_date
                 ,a.program_air_datetime=b.program_air_datetime
                 ,a.program_air_end_datetime=b.program_air_end_datetime
                 ,a.programme_duration=b.programme_duration
                 ,a.genre_description=b.genre_description
                 ,a.sub_genre_description=b.sub_genre_description
                 ,a.programme_name=b.programme_name
from              molson_coors_allspots_viewing_data15 as A
left outer join   molson_coors_Program_details_all_spots  as B
on               a.Programme_Trans_Sk = b.dk_programme_instance_dim;


