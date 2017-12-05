------------------------------------------------------------------------
--                                                                    --
--        Project: Molson Coors                                       --
--        Version: v1.0                                               --
--        Created: 12/03/2012                                         --
--        Lead:                                                       --
--        Analyst: Hannah Starmer                                     --
--        SK Prod: 4                                                  --
--                                                                    --
--                                                                    --
--        PART A. SPOT DATA                                           --
--        PART B. HOUSEHOLD AGGREGATE                                 --
--        PART C. VIEWING DATA                                        --
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
CREATE VARIABLE @var_sql                varchar(4000);
CREATE VARIABLE @scanning_day           datetime;
CREATE VARIABLE @var_num_days           smallint;
CREATE VARIABLE @var_cntr               smallint;
-- THINGS YOU NEED TO CHANGE --


SET @var_period_start  = '2012-08-01';
SET @var_period_end    = '2012-09-28';


-------------------------------------------------
--         PART A: SPOT DATA                   --
-------------------------------------------------


-- CAMPAIGN SPOT DATA --
-- SELECT THE SPOT DATA THAT IS REQUIRED FOR ANALYSIS --


IF OBJECT_ID('molson_coors_spot_data1') IS NOT NULL DROP TABLE molson_coors_spot_data1
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
INTO     molson_coors_spot_data1
FROM     neighbom.BARB_MASTER_SPOT_DATA                         A
LEFT OUTER JOIN
         VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES  B
-- BRINGS IN THE SPOT NAME INFORMATION FROM VARIOUS SOURCES --
ON       A.service_key=B.service_key
AND      A.local_date_of_transmission between B.effective_from and B.effective_to
WHERE    A.clearcast_commercial_no IN ('VCCMCCA021030','VCCMCCL005010','VCCMCCL028030','VCCMCCL029010','VCCMCCL032010',
                                       'AAEFOST026040','AAEFOST027030','MUMSTCI009040','MUMSTCI014030')
AND      (DATE(A.utc_spot_start_date_time) BETWEEN DATE('2012/07/31') AND DATE('2012/09/21')
OR        DATE(A.utc_spot_end_date_time)   BETWEEN DATE('2012/07/31') AND DATE('2012/09/21'));

select * from molson_coors_spot_data;



-------------------------------------------------
--         PART B: HOUSEHOLD AGGREGATE         --
-------------------------------------------------


-- IDENTIFY IF HOUSEHOLD CONTAINS A MALE, BROUGHT AGE RANGE OR ASPIRATIONAL AGE RANGE --

if object_id('hh_age_gender') is not null drop table hh_age_gender;
select    a.cb_key_household
         ,max(a.hh_contain_18_to_24) as hh_contain_18_to_24
         ,max(a.hh_contain_16_to_34) as hh_contain_16_to_34
         ,max(hh_contain_over_18) as hh_contain_over_18
         ,max(a.male_present) as hh_contain_male
into      hh_age_gender
from      (select  cb_key_household
                  ,cb_key_db_individual
                  ,case when p_actual_age between 18 and 24 then 1 else 0 end as hh_contain_18_to_24
                  ,case when p_actual_age between 16 and 34 then 1 else 0 end as hh_contain_16_to_34
                  ,case when p_actual_age>18 then 1 else 0 end as hh_contain_over_18
                  ,case when p_gender in ('0') then 1 else 0 end as male_present
           from sk_prod.experian_consumerview) A
group by cb_key_household;



-- PICK UP MOSAIC TYPE  --
if object_id('hh_mosaic') is not null drop table hh_mosaic;
select     distinct cb_key_household
           ,h_mosaic_uk_type
into       hh_mosaic
from       sk_prod.experian_consumerview;



-- USE MOSAIC GROUPS TO CREATE BUCKLE SEGMENTS --
-- CREATE BROUGHT AUDIENCE AND ASPIRATIONAL AUDIENCE FLAGS --

if object_id('mc_hh_aggregate') is not null drop table mc_hh_aggregate;
select     b.cb_key_household
          ,b.h_mosaic_uk_type
          ,c.hh_contain_18_to_24
          ,c.hh_contain_16_to_34
          ,c.hh_contain_over_18
          ,c.hh_contain_male
          ,case when c.hh_contain_male=1 and c.hh_contain_16_to_34=1 then 1 else 0 end as brought_audience
          ,case when c.hh_contain_male=1 and c.hh_contain_18_to_24=1 then 1 else 0 end as aspirational_audience
          ,case when b.h_mosaic_uk_type in ('16','34','35','37','45','46','47','48',
                                            '49','57','58','59','60','62','64','65','66','67') then 1 else 0 end as Social_Explorers
          ,case when b.h_mosaic_uk_type in ('16','34','35','37','45','46','47','48',
                                            '49','57','58','59','60','62','64','65','66','67') then 'Social Explorers'
                when b.h_mosaic_uk_type in ('1','2','5','7','9','27','29','30',
                                            '31','32','33','36','55','61','63') then 'Enthusiastic Influencers'
                when b.h_mosaic_uk_type in ('13','17','18','21','23',
                                            '24','25','26','54') then 'Content Routiners'
                when b.h_mosaic_uk_type in ('14','15','28','38','39','40','41',
                                            '42','50','51','52','53','56') then 'Safe & Savvy'
                when b.h_mosaic_uk_type in ('3','4','6','8','10',
                                            '11','12','19','20','22') then 'Considered Balancers'
                when b.h_mosaic_uk_type in ('43','44','99') then 'Do Not Target'
                else 'Unknown'
           end as buckle_segments
into      mc_hh_aggregate
from      hh_mosaic  B
left outer join
          hh_age_gender                C
on b.cb_key_household=c.cb_key_household;

------------------------------
-- IDENTIFY ACTIVE SKY BASE --
------------------------------

if object_id('sky_active_customers') is not null drop table sky_active_customers;

SELECT   account_number
        ,cb_key_household
        ,current_short_description
        ,rank() over (PARTITION BY account_number ORDER BY effective_from_dt desc, cb_row_id) AS rank
        ,convert(bit, 0)  AS uk_standard_account
INTO sky_active_customers
FROM sk_prod.cust_subs_hist
WHERE subscription_sub_type IN ('DTV Primary Viewing')
        AND status_code IN ('AC','AB','PC')
        AND effective_from_dt    <= @var_period_start
        AND effective_to_dt      > @var_period_start
        AND effective_from_dt    <> effective_to_dt
        AND EFFECTIVE_FROM_DT    IS NOT NULL
        AND cb_key_household     > 0
        AND cb_key_household     IS NOT NULL
        AND account_number       IS NOT NULL
        AND service_instance_id  IS NOT NULL;

-- REMOVE DUPLICATES --
delete from sky_active_customers where rank > 1;

-- TAKE ONLY UK CUSTOMERS --

UPDATE sky_active_customers
  SET
  uk_standard_account = CASE WHEN b.acct_type='Standard' AND b.account_number <>'?' AND b.pty_country_code ='GBR'
                             THEN 1
                             ELSE 0 END
FROM sky_active_customers AS a
inner join sk_prod.cust_single_account_view AS b
ON a.account_number = b.account_number;


DELETE FROM sky_active_customers WHERE uk_standard_account = 0;


-- SUMMARISE TABLE UP TO HOUSEHOLD LEVEL --


if object_id('sky_active_households') is not null drop table sky_active_households;
select  cb_key_household
       ,sum(uk_standard_account) as ac
into sky_active_households
from sky_active_customers
group by cb_key_household;

-- CREATE HOUSEHOLD AGGREGATE WHICH INCLUDES SKY BASE --

if object_id('mc_sky_hh_aggregate') is not null drop table mc_sky_hh_aggregate;
select     COALESCE(a.cb_key_household,b.cb_key_household) as cb_key_household
          ,case when a.ac is not null then 1 else 0 end as sky_flag
          ,b.h_mosaic_uk_type
          ,b.hh_contain_18_to_24
          ,b.hh_contain_16_to_34
          ,b.hh_contain_over_18
          ,b.hh_contain_male
          ,b.brought_audience
          ,b.aspirational_audience
          ,b.buckle_segments
into   mc_sky_hh_aggregate
from   sky_active_households         A
left outer join
       mc_hh_aggregate     B
on a.cb_key_household=b.cb_key_household;

--9224846 Row(s) affected

-------------------------------------------------
--         PART C: VIEWING DATA                --
-------------------------------------------------


-- GET PROGRAM DATA FOR TIME PERIOD --


IF object_id('molson_coors_Program_details') IS NOT NULL DROP TABLE molson_coors_Program_details;

select
       dk_programme_instance_dim
      ,pay_free_indicator
      ,cast(broadcast_start_date_time_utc as date) as program_air_date
      ,broadcast_start_date_time_utc as program_air_datetime
      ,broadcast_end_date_time_utc as program_air_end_datetime
      ,broadcast_daypart
      ,genre_description
      ,sub_genre_description
      ,channel_name
      ,programme_name
      ,service_key
      ,datediff(mi,broadcast_start_date_time_utc,broadcast_end_date_time_utc) as programme_duration
  into molson_coors_Program_details
  from sk_prod.vespa_programme_schedule
 where broadcast_start_date_time_utc >= dateadd(month, -1, @var_period_start)
   and broadcast_start_date_time_utc <= @var_period_end;



create unique hg index idx1 on Program_details(dk_programme_instance_dim);

select program_air_date,count(*) from molson_coors_Program_details group by program_air_date;



--    COMPILE CAPPING FILES FOR CHOSEN SCALING UNIVERSE    --

--    CREATE TABLE TEMPLATES   --


if object_id('molson_coors_viewing_data_nwk_all') is not null drop table molson_coors_viewing_data_nwk_all;


create table molson_coors_viewing_data_nwk_all (
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
,event_start_date_time_utc       datetime
,instance_start_date_time_utc    datetime
,instance_end_date_time_utc      datetime
,broadcast_start_date_time_utc   datetime
,original_broadcast_date_time_utc datetime
,instance_duration                int
,original_broadcast_end_date_time datetime
,Capped_Flag                     tinyint
,Capped_Event_End_Time           datetime
,Scaling_Segment_Id              bigint
,Scaling_Weighting               int
,BARB_Minute_Start               datetime
,BARB_Minute_End                 datetime
,program_air_date                date
,program_air_datetime            datetime
,program_air_end_datetime        datetime
,programme_duration              int
,broadcast_daypart               varchar(30)
,genre_description               varchar(30)
,sub_genre_description           varchar(30)
,channel_name                    varchar(30)
,programme_name                  varchar(30)
,service_key                     bigint
,utc_spot_start_date_time              datetime
,utc_spot_end_date_time              datetime
,utc_break_start_date_time              datetime
,utc_break_end_date_time              datetime
,full_name                    varchar(255)
,vespa_name                  varchar(255)
,techedge_name               varchar(255)
,infosys_name                varchar(255)
,spot_position_in_break         int
,no_spots_in_break           int
,spot_duration               int
,clearcast_commercial_no        varchar(25)
,barb_date_of_transmission      datetime
,barb_spot_start_time           int
);
commit;


--    LOOP THROUGH THE DAILY CAPPING TABLES AND POPULATE TABLE     --


SET @var_sql = '
    insert into molson_coors_viewing_data_nwk_all (
                 Cb_Row_Id
                ,Account_Number
                ,Subscriber_Id
                ,cb_key_household
                ,Programme_Trans_Sk
                ,Timeshifting
                ,Viewing_Starts
                ,Viewing_Stops
                ,Viewing_Duration
                ,Capped_Flag
                ,Capped_Event_End_Time
                ,time_in_seconds_since_recording
                ,instance_start_date_time_utc
                ,original_broadcast_date_time_utc
                ,instance_duration

                ,program_air_date
                ,program_air_datetime
                ,program_air_end_datetime
                ,programme_duration
                ,broadcast_daypart
                ,genre_description
                ,programme_name
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
                ,cap.Capped_Flag
                ,cap.Capped_Event_End_Time
                ,b.time_in_seconds_since_recording
                ,b.instance_start_date_time_utc
                ,original_broadcast_date_time_utc = dateadd(ss,-1*time_in_seconds_since_recording,instance_start_date_time_utc)
                ,datediff(ss,instance_start_date_time_utc,instance_end_date_time_utc) as instance_duration

                ,prog.program_air_date
                ,prog.program_air_datetime
                ,prog.program_air_end_datetime
                ,prog.programme_duration
                ,prog.genre_description
                ,prog.sub_genre_description
                ,prog.programme_name
                ,prog.service_key
                ,spot.utc_spot_start_date_time
                ,spot.utc_spot_end_date_time
                ,spot.full_name
                ,spot.spot_duration
                ,spot.clearcast_commercial_no
                ,spot.barb_date_of_transmission
                ,spot.barb_spot_start_time
from             vespa_analysts.VESPA_DAILY_AUGS_##^^*^*## as cap

inner join       sk_prod.Vespa_events_all as b
on               cap.cb_row_id=b.pk_viewing_prog_instance_fact

inner join       molson_coors_Program_details as prog
on               cap.Programme_Trans_Sk = prog.dk_programme_instance_dim

inner join       molson_coors_spot_data1 as spot
on               prog.service_key=spot.service_key

where
    (            dateadd(second,time_in_seconds_since_recording*-1,viewing_starts) between utc_spot_start_date_time and utc_spot_end_date_time
        or       dateadd(second,time_in_seconds_since_recording*-1,viewing_stops)  between utc_spot_start_date_time and utc_spot_end_date_time
        or       dateadd(second,time_in_seconds_since_recording*-1,viewing_starts) < utc_spot_start_date_time
        and      dateadd(second,time_in_seconds_since_recording*-1,viewing_stops)> utc_spot_end_date_time
        and      b.panel_id in (12)
    )
    ';



SET @scanning_day = @var_period_start;

while @scanning_day <= dateadd(dd,0,@var_period_end)
begin
    EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(@scanning_day, 'yyyymmdd')))
    commit

    set @scanning_day = dateadd(day, 1, @scanning_day)
end;
commit;


-- SPOT VIEWED FLAG --

if object_id('molson_coors_spot_data12') is not null drop table molson_coors_spot_data12;

select *
       ,flag = 1
       ,sum(flag) over (order by utc_spot_start_date_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as identifier
into molson_coors_spot_data12
from molson_coors_spot_data1;
-- identifier is unique across the various advertising campaigns





alter table molson_coors_viewing_data_nwk_all
        add (spot_viewed integer default 0
           ,spot_identifier integer);

Update molson_coors_viewing_data_nwk_all
        set vw.spot_viewed = case when (timeshifting = 'LIVE' and vw.viewing_starts < vw.utc_spot_start_date_time
                                  and  viewing_stops> vw.utc_spot_end_date_time)
                                  or (vw.original_broadcast_date_time_utc < vw.utc_spot_start_date_time
                                  and  dateadd(second,vw.instance_duration,vw.original_broadcast_date_time_utc)> vw.utc_spot_end_date_time)

                                 then 1 else 0 end
            ,vw.spot_identifier = spot.identifier
from molson_coors_viewing_data_nwk_all  vw
join molson_coors_spot_data12       spot
on   vw.utc_spot_start_date_time    = spot.utc_spot_start_date_time
and  vw.utc_spot_end_date_time = spot.utc_spot_end_date_time
and  vw.service_key = spot.service_key
and  vw.full_name =  spot.full_name
and  vw.clearcast_commercial_no =  spot.clearcast_commercial_no;



-- ADD ON SCALING WEIGHTINGS --


ALTER TABLE molson_coors_viewing_data_nwk_all
ADD ( weighting_date        date
     ,scaling_segment_ID    int
     ,weightings            float default 0);


update molson_coors_viewing_data_nwk_all
set    weighting_date=cast(a.viewing_starts as date)
from   molson_coors_viewing_data_nwk_all A;


update   molson_coors_viewing_data_nwk_all
set      scaling_segment_ID = l.scaling_segment_ID
from     molson_coors_viewing_data_nwk_all as b
inner join
         bednaszs.v_SC2_Intervals as l -- Sebastian made a special table for us containing phase II SCALING weights
on       b.account_number = l.account_number
and      b.weighting_date between l.reporting_starts and l.reporting_ends;


update    molson_coors_viewing_data_nwk_all
set       weightings = s.weighting
from      molson_coors_viewing_data as b
inner join
          bednaszs.v_SC2_Weightings as s
on        b.weighting_date = s.scaling_day
and       b.scaling_segment_ID = s.scaling_segment_ID;



-- ADD ON MEDIA PACK, SALES HOUSE AND FIX CHANNEL NAME --

-- CHANNEL NAME --


select service_key,count(*)
into #service
from molson_coors_spot_data
group by service_key;

select  te_channel
       ,service_key
       ,count(*)
into #te_channel_lkup
from molson_coors_spot_barb_data
group by te_channel
       ,service_key;

--drop table te_channel_lkup;
select a.service_key
      ,b.te_channel
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
            when a.service_key in (1305) then 'FX'
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
            when a.service_key in (1301) then 'Sky Sports 1'
            when a.service_key in (1302) then 'Sky Sports 2'
            when a.service_key in (1333) then 'Sky Sports 3'
            when a.service_key in (1322) then 'Sky Sports 4'
            when a.service_key in (1471) then 'Sky Sports Active 1'
            when a.service_key in (1472) then 'Sky Sports Active 2'
            when a.service_key in (1473) then 'Sky Sports Active 3'
            when a.service_key in (1474) then 'Sky Sports Active 4'
            when a.service_key in (1475) then 'Sky Sports Active 5'
            when a.service_key in (1306) then 'Sky Sports F1'
            when a.service_key in (1314) then 'Sky Sports News'
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
            when a.service_key in (1815) then 'Sky Movies Modern Greats'
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
            else te_channel
       end as channel
into te_channel_lkup
from #service         A
left outer join
     #te_channel_lkup B
on   a.service_key=b.service_key;


alter table molson_coors_viewing_data_nwk_all
        drop channel_new ;


alter table molson_coors_viewing_data_nwk_all
        add (channel_new varchar(50));


update            molson_coors_viewing_data_nwk_all
set               cub.channel_new= (case when tmp.channel is not null then tmp.channel else cub.full_name end)
from              molson_coors_viewing_data_nwk_all as cub
left outer join   te_channel_lkup as tmp
on                tmp.service_key = cub.service_key;



-- CHANNEL GROUPING (MEDIA PACK) AND SALES HOUSE --


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



--if object_id('LkUpPack') is not null drop table LkUpPack
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


alter table molson_coors_viewing_data_nwk_all
        add (media_pack varchar(25)
           ,sales_house varchar(25));

update            molson_coors_viewing_data_nwk_all
set               cub.media_pack = tmp.channel_category
                  ,cub.sales_house = tmp.primary_sales_house
from              molson_coors_viewing_data_nwk_all as cub
left outer join   LkUpPack as tmp
on                tmp.service_key = cub.service_key;

--------------------------------------------------------
--         PART D: CREATE CLIENT SPOTS FILE           --
--------------------------------------------------------


if object_id('mc_hh_impacts') is not null drop table mc_hh_impacts;

select     a.cb_key_household
          ,a.weightings
          ,case when a.spot_viewed=1 and a.clearcast_commercial_no in ('VCCMCCA021030')
                then a.weightings else 0 end as carling_exposed_hh
          ,case when a.spot_viewed=1 and a.clearcast_commercial_no in ('MUMSTCI009040','MUMSTCI014030')
                then a.weightings else 0 end as stella_cidre_exposed_hh
          ,case when a.spot_viewed=1 and a.clearcast_commercial_no in ('AAEFOST026040','AAEFOST027030')
                then a.weightings else 0 end as fosters_exposed_hh
          ,case when a.spot_viewed=1 and a.clearcast_commercial_no in ('VCCMCCL005010','VCCMCCL028030','VCCMCCL029010','VCCMCCL032010')
                then a.weightings else 0 end as coors_light_exposed_hh

          ,cast(a.utc_spot_start_date_time as date) utc_start_date
          ,convert(varchar(8),a.utc_spot_start_date_time,108) as spot_time
          ,case when convert(varchar(8),utc_spot_start_date_time,108)
                between '06:00:00' and '08:59:59' then 'Breakfast Time'
                when convert(varchar(8),utc_spot_start_date_time,108)
                between '09:00:00' and '17:29:59' then 'Daytime'
                when convert(varchar(8),utc_spot_start_date_time,108)
                between '17:30:00' and '19:59:59' then 'Early Peak'
                when convert(varchar(8),utc_spot_start_date_time,108)
                between '20:00:00' and '22:59:59' then 'Late Peak'
                when convert(varchar(8),utc_spot_start_date_time,108)
                between '23:00:00' and '23:59:59' then 'Post Peak'
                when convert(varchar(8),utc_spot_start_date_time,108)
                between '00:00:00' and '00:29:59' then 'Post Peak'
                when convert(varchar(8),utc_spot_start_date_time,108)
                between '00:30:00' and '05:59:59' then 'Night Time'
           else 'Unknown'
           end as daypart
          ,a.utc_spot_start_date_time
          ,a.barb_date_of_transmission
          ,a.barb_spot_start_time
          ,case when a.clearcast_commercial_no in ('VCCMCCA021030') then 'Carling'
                when a.clearcast_commercial_no in ('MUMSTCI009040','MUMSTCI014030') then 'Stella Cidre'
                when a.clearcast_commercial_no in ('AAEFOST026040','AAEFOST027030') then 'Fosters'
                when a.clearcast_commercial_no in ('VCCMCCL005010','VCCMCCL028030','VCCMCCL029010','VCCMCCL032010') then 'Coors Light'
                else 'Unknown'
           end as client_spot_flag
          ,a.clearcast_commercial_no
          ,a.channel_new
          ,a.sales_house
          ,a.media_pack
          ,a.genre_description
          ,a.sub_genre_description
          ,a.programme_name
          ,max(spot_duration) as spot_duration
          ,max(a.spot_identifier) as spot_identifier
          ,count(utc_spot_start_date_time) as spots
into       mc_hh_impacts
from       molson_coors_viewing_data_nwk_all       A
where      a.spot_viewed=1 and  a.cb_key_household is not null
group by   a.cb_key_household
          ,a.weightings
          ,carling_exposed_hh
          ,stella_cidre_exposed_hh
          ,fosters_exposed_hh
          ,coors_light_exposed_hh
          ,utc_start_date
          ,spot_time
          ,utc_spot_start_date_time
          ,daypart
          ,a.barb_date_of_transmission
          ,a.barb_spot_start_time
          ,client_spot_flag
          ,a.clearcast_commercial_no
          ,a.channel_new
          ,a.sales_house
          ,a.media_pack
          ,a.genre_description
          ,a.sub_genre_description
          ,a.programme_name;



-- JOIN ON HOUSEHOLD AGGREGATE TABLE --



if object_id('mc_hh_exp_impacts') is not null drop table mc_hh_exp_impacts;

select     a.*
          ,b.h_mosaic_uk_type
          ,b.hh_contain_18_to_24
          ,b.hh_contain_16_to_34
          ,b.hh_contain_over_18
          ,b.hh_contain_male
          ,b.brought_audience
          ,b.aspirational_audience
          ,b.Social_Explorers
          ,b.buckle_segments
          ,case when b.brought_audience=1 then a.weightings else 0 end as brought_audience_hh
          ,case when b.aspirational_audience=1 then a.weightings else 0 end as aspirational_audience_hh
          ,case when b.buckle_segments in ('Social Explorers') then a.weightings else 0 end as social_explorers_hh
          ,case when b.buckle_segments in ('Enthusiastic Influencers') then a.weightings else 0 end as enthusiastic_influencers_hh
          ,case when b.buckle_segments in ('Content Routiners') then a.weightings else 0 end as content_routiners_hh
          ,case when b.buckle_segments in ('Safe & Savvy') then a.weightings else 0 end as safe_and_savy_hh
          ,case when b.buckle_segments in ('Considered Balancers') then a.weightings else 0 end as considered_balancers_hh
          ,case when b.buckle_segments in ('Do Not Target') then a.weightings else 0 end as do_not_targets_hh
          ,case when b.buckle_segments in ('Unknown') then a.weightings else 0 end as unknowns_hh
          ,case when a.weightings<1 then 1 else 0 end as zero_weighting_hh
into       mc_hh_exp_impacts
from       mc_hh_impacts       A
left outer join
           molson_coors_hh_aggregate     B
on         a.cb_key_household=b.cb_key_household;





-- IMPACT PIVOT TABLE --

select top 10 * from mc_impacts

if object_id('mc_impacts') is not null drop table mc_impacts

select     barb_date_of_transmission
          ,barb_spot_start_time
          ,utc_spot_start_date_time
          ,daypart
          ,client_spot_flag
          ,clearcast_commercial_no
          ,channel_new
          ,sales_house
          ,media_pack
          ,genre_description
          ,sub_genre_description
          ,programme_name
          ,max(spot_duration) as spot_duration
          ,sum(weightings*spots) as total_impacts
          ,sum(carling_exposed_hh*spots) as carling_impacts
          ,sum(stella_cidre_exposed_hh*spots) as stella_cidre_impacts
          ,sum(fosters_exposed_hh*spots) as fosters_impacts
          ,sum(coors_light_exposed_hh*spots) as coors_impacts
          ,sum(brought_audience_hh*spots) as brought_audience_impacts
          ,sum(aspirational_audience_hh*spots) as aspirational_audience_impacts
          ,sum(social_explorers_hh*spots) as social_explorers_impacts
          ,sum(enthusiastic_influencers_hh*spots) as enthusiastic_influencers_impacts
          ,sum(content_routiners_hh*spots) as content_routiners_impacts
          ,sum(safe_and_savy_hh*spots) as safe_and_savy_impacts
          ,sum(considered_balancers_hh*spots) as considered_balancers_impacts
          ,sum(do_not_targets_hh*spots) as do_not_targets_impacts
          ,sum(unknowns_hh*spots) as unknowns_impacts
          ,sum(zero_weighting_hh) as zero_weighting_hh
into       mc_impacts
from       mc_hh_exp_impacts
group by   barb_date_of_transmission
          ,barb_spot_start_time
          ,utc_spot_start_date_time
          ,daypart
          ,clearcast_commercial_no
          ,client_spot_flag
          ,channel_new
          ,sales_house
          ,media_pack
          ,genre_description
          ,sub_genre_description
          ,programme_name;

select * from mc_impacts;



-- APPEND HH AGGREGATE TABLE WITH EXPOSURE TO COMMERCIALS FLAGS --

drop table mc_hh_update;
select    a.cb_key_household
         ,case when a.carling_exposed_hh >0 then 1 else 0 end as carling_exposed
         ,case when a.stella_cidre_exposed_hh >0 then 1 else 0 end as stella_cidre_exposed
         ,case when a.fosters_exposed_hh >0 then 1 else 0 end as fosters_exposed
         ,case when a.coors_light_exposed_hh >0 then 1 else 0 end as coors_light_exposed
into      mc_hh_update
from
(select   cb_key_household
         ,max(carling_exposed_hh) as carling_exposed_hh
         ,max(stella_cidre_exposed_hh) as stella_cidre_exposed_hh
         ,max(fosters_exposed_hh) as fosters_exposed_hh
         ,max(coors_light_exposed_hh) as coors_light_exposed_hh
 from     mc_hh_exp_impacts
 group by cb_key_household
 ) A;

-- UPDATE HH AGGREGATE TABLES --


alter table mc_hh_aggregate
      add ( carling_exposed integer
           ,stella_cidre_exposed integer
           ,fosters_exposed integer
           ,coors_light_exposed integer);


update mc_hh_aggregate
 set   a.carling_exposed=b.carling_exposed
      ,a.stella_cidre_exposed=b.stella_cidre_exposed
      ,a.fosters_exposed=b.fosters_exposed
      ,a.coors_light_exposed=b.coors_light_exposed
 from  mc_hh_aggregate    A
 left outer join
       mc_hh_update       B
 on    a.cb_key_household=b.cb_key_household;

-- SKY BASE HH AGGREGATE --


alter table mc_sky_hh_aggregate
      add ( carling_exposed integer
           ,stella_cidre_exposed integer
           ,fosters_exposed integer
           ,coors_light_exposed integer);


update mc_sky_hh_aggregate
 set   a.carling_exposed=b.carling_exposed
      ,a.stella_cidre_exposed=b.stella_cidre_exposed
      ,a.fosters_exposed=b.fosters_exposed
      ,a.coors_light_exposed=b.coors_light_exposed
 from  mc_sky_hh_aggregate    A
 left outer join
       mc_hh_update       B
 on    a.cb_key_household=b.cb_key_household;

--------------------------------------------------------
-- REACH CALCULATIONS  --
--------------------------------------------------------

-- TAKE MIDDAY WEIGHT --

-- MIDDAY WEIGHT 13TH AUGUST --

drop table molson_coors_midday_scaling;

select    a.account_number
         ,cast('2012-08-13' as date) as scaling_date
         ,a.reporting_starts
         ,a.reporting_ends
         ,a.scaling_segment_id
into     molson_coors_midday_scaling
from     bednaszs.v_SC2_Intervals   A                                           --NEW SCALING TABLE PHASE II
where    scaling_date between a.reporting_starts and a.reporting_ends;

ALTER TABLE molson_coors_midday_scaling
ADD (weightings            float);

update    molson_coors_midday_scaling
set       weightings = s.weighting
from      molson_coors_midday_scaling as b
inner join
           bednaszs.v_SC2_Weightings as s                                -- NEW SCALING TABLE PHASE II
on        b.scaling_date = s.scaling_day
and       b.scaling_segment_ID = s.scaling_segment_ID;

-- GET LOOKUP BETWEEN ACCOUNT TO CB_KEY_HOUSEHOLD --

drop table acct_hh;
select account_number,cb_key_household
into   acct_hh
from   sk_prod.cust_single_account_view
where  account_number in (select distinct account_number from molson_coors_midday_scaling);

-- JOIN AND SUMMARISE TO HOUSEHOLD LEVEL --

alter table molson_coors_midday_scaling
      add ( cb_key_household  bigint);

update molson_coors_midday_scaling
 set   a.cb_key_household=b.cb_key_household
 from  molson_coors_midday_scaling           A
 left outer join
       acct_hh                               B
 on    a.account_number=b.account_number;

-- TO HOUSEHOLD LEVEL --
drop table molson_coors_midday_scaling_hh;
select    a.cb_key_household
         ,sum(a.weightings) as midday_weightings
into      molson_coors_midday_scaling_hh
from      molson_coors_midday_scaling    A
group by  a.cb_key_household;

-- UPDATE HH AGGREGATE TABLE --



alter table mc_hh_aggregate
      add ( midday_weightings  integer);


update mc_hh_aggregate A
 set   a.midday_weightings = b.midday_weightings
 from  mc_hh_aggregate    A
 left outer join
       molson_coors_midday_scaling_hh       B
 on    a.cb_key_household=b.cb_key_household;


-- UPDATE SKY HH AGGREGATE TABLE --


alter table mc_sky_hh_aggregate
      add ( midday_weightings  integer);


update mc_sky_hh_aggregate A
 set   a.midday_weightings = b.midday_weightings
 from  mc_sky_hh_aggregate    A
 left outer join
       molson_coors_midday_scaling_hh       B
 on    a.cb_key_household=b.cb_key_household;

-- UPDATE HH IMPACTS TABLE AND CREATE  --



alter table mc_hh_exp_impacts
      add ( midday_weightings  integer);


update mc_hh_exp_impacts
 set   a.midday_weightings=b.midday_weightings
 from  mc_hh_exp_impacts    A
 left outer join
       molson_coors_midday_scaling_hh       B
 on    a.cb_key_household=b.cb_key_household;


-- ADJUST THE IMPACT TABLE FOR SPOT GREATER THAN 30 SECONDS --

drop table mc_impacts_adjusted;
select     barb_date_of_transmission
          ,barb_spot_start_time
          ,utc_spot_start_date_time
          ,daypart
          ,clearcast_commercial_no
          ,channel_new
          ,sales_house
          ,media_pack
          ,genre_description
          ,sub_genre_description
          ,programme_name
          ,case when spot_duration=40 then (1.33*total_impacts)
                when spot_duration=50 then (1.66*total_impacts)
                when spot_duration=60 then (2*total_impacts)
                else total_impacts
           end as total_impacts
          ,case when spot_duration=40 then (1.33*carling_impacts)
                when spot_duration=50 then (1.66*carling_impacts)
                when spot_duration=60 then (2*carling_impacts)
                else carling_impacts
           end as carling_impacts
          ,case when spot_duration=40 then (1.33*stella_cidre_impacts)
                when spot_duration=50 then (1.66*stella_cidre_impacts)
                when spot_duration=60 then (2*stella_cidre_impacts)
                else stella_cidre_impacts
           end as stella_cidre_impacts
          ,case when spot_duration=40 then (1.33*fosters_impacts)
                when spot_duration=50 then (1.66*fosters_impacts)
                when spot_duration=60 then (2*fosters_impacts)
                else fosters_impacts
           end as fosters_impacts
          ,case when spot_duration=40 then (1.33*coors_impacts)
                when spot_duration=50 then (1.66*coors_impacts)
                when spot_duration=60 then (2*coors_impacts)
                else coors_impacts
           end as coors_impacts
          ,case when spot_duration=40 then (1.33*brought_audience_impacts)
                when spot_duration=50 then (1.66*brought_audience_impacts)
                when spot_duration=60 then (2*brought_audience_impacts)
                else brought_audience_impacts
           end as brought_impacts
          ,case when spot_duration=40 then (2*aspirational_audience_impacts)
                when spot_duration=50 then (1.66*aspirational_audience_impacts)
                when spot_duration=60 then (2*aspirational_audience_impacts)
                else aspirational_audience_impacts
           end as aspirational_impacts
          ,case when spot_duration=40 then (1.33*social_explorers_impacts)
                when spot_duration=50 then (1.66*social_explorers_impacts)
                when spot_duration=60 then (2*social_explorers_impacts)
                else social_explorers_impacts
           end as social_explorers_impacts
          ,case when spot_duration=40 then (1.33*enthusiastic_influencers_impacts)
                when spot_duration=50 then (1.66*enthusiastic_influencers_impacts)
                when spot_duration=60 then (2*enthusiastic_influencers_impacts)
                else enthusiastic_influencers_impacts
           end as enthusiastic_influencers_impacts
          ,case when spot_duration=40 then (1.33*content_routiners_impacts)
                when spot_duration=50 then (1.66*content_routiners_impacts)
                when spot_duration=60 then (2*content_routiners_impacts)
                else content_routiners_impacts
           end as content_routiners_impacts
          ,case when spot_duration=40 then (1.33*safe_and_savy_impacts)
                when spot_duration=50 then (1.66*safe_and_savy_impacts)
                when spot_duration=60 then (2*safe_and_savy_impacts)
                else safe_and_savy_impacts
           end as safe_and_savy_impacts
          ,case when spot_duration=40 then (1.33*considered_balancers_impacts)
                when spot_duration=50 then (1.66*considered_balancers_impacts)
                when spot_duration=60 then (2*considered_balancers_impacts)
                else considered_balancers_impacts
           end as considered_balancers_impacts
          ,case when spot_duration=40 then (1.33*do_not_targets_impacts)
                when spot_duration=50 then (1.66*do_not_targets_impacts)
                when spot_duration=60 then (2*do_not_targets_impacts)
                else do_not_targets_impacts
           end as do_not_targets_impacts
into mc_impacts_adjusted
from mc_impacts;

select * from mc_impacts_adjusted;
