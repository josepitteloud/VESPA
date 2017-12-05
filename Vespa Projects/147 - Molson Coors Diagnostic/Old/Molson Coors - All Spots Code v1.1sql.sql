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
CREATE VARIABLE @var_sql                varchar(4000);
CREATE VARIABLE @scanning_day           datetime;
CREATE VARIABLE @var_num_days           smallint;
CREATE VARIABLE @var_cntr               smallint;
-- THINGS YOU NEED TO CHANGE --


SET @var_period_start  = '2012-08-14';
SET @var_period_end    = '2012-08-29';


-------------------------------------------------
--         PART A: SPOT DATA                   --
-------------------------------------------------


-- CAMPAIGN SPOT DATA --
-- SELECT THE SPOT DATA THAT IS REQUIRED FOR ANALYSIS --


IF OBJECT_ID('molson_coors_all_spot_data_V2') IS NOT NULL DROP TABLE molson_coors_all_spot_data_V2
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
INTO     molson_coors_all_spot_data_v2
FROM     neighbom.BARB_MASTER_SPOT_DATA                         A
LEFT OUTER JOIN
         VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES  B
ON       A.service_key=B.service_key
AND      A.barb_date_of_transmission between B.effective_from and B.effective_to
WHERE    a.barb_date_of_transmission between '2012-08-15' and '2012-08-29'
AND      mid_break=spot_position_in_break;

-- PROGRAM DATA --
drop table molson_coors_Program_details_ALL_SPOTS;
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
into molson_coors_Program_details_ALL_SPOTS
from sk_prod.vespa_programme_schedule
where broadcast_start_date_time_utc >= dateadd(month, -2, @var_period_start)
and broadcast_start_date_time_utc <= @var_period_end;

-------------------------------------------------
--         PART B: VIEWING DATA                --
-------------------------------------------------




--    COMPILE CAPPING FILES FOR CHOSEN SCALING UNIVERSE    --

--    CREATE TABLE TEMPLATES   --


if object_id('molson_coors_allspots_viewing_data_new2') is not null drop table molson_coors_allspots_viewing_data_new2;


create table molson_coors_allspots_viewing_data_new2 (
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
,original_broadcast_date_time_utc datetime
,instance_duration
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


SET @var_sql = '
    insert into molson_coors_allspots_viewing_data_new2 (
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
                ,cap.Capped_Flag
                ,cap.Capped_Event_End_Time
                ,b.time_in_seconds_since_recording
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
from             vespa_analysts.VESPA_DAILY_AUGS_##^^*^*## as cap

inner join       sk_prod.Vespa_events_all as b
on               cap.cb_row_id=b.pk_viewing_prog_instance_fact

inner join       molson_coors_Program_details_ALLSPOTS as prog
on               cap.Programme_Trans_Sk = prog.dk_programme_instance_dim

inner join       molson_coors_all_spot_data_V2 as spot
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

;



-- SPOT VIEWED FLAG --

if object_id('molson_coors_all_spot_data_V2_2') is not null drop table molson_coors_all_spot_data_V2_2;

select *
       ,flag = 1
       ,sum(flag) over (order by utc_spot_start_date_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as identifier
into molson_coors_all_spot_data_V2_2
from molson_coors_all_spot_data_V2;

--select * from molson_coors_all_spot_data_V2_2;


alter table molson_coors_allspots_viewing_data_new2
        add (spot_viewed integer default 0
           ,spot_identifier integer);

Update molson_coors_allspots_viewing_data_new2
        set vw.spot_viewed = case when (timeshifting = 'LIVE' and vw.viewing_starts < vw.utc_spot_start_date_time
                                  and  viewing_stops> vw.utc_spot_end_date_time)
                                  or (vw.original_broadcast_date_time_utc < vw.utc_spot_start_date_time
                                  and  dateadd(second,vw.instance_duration,vw.original_broadcast_date_time_utc)> vw.utc_spot_end_date_time)

                                 then 1 else 0 end
            ,vw.spot_identifier = spot.identifier
from molson_coors_allspots_viewing_data_new2  vw
join molson_coors_all_spot_data_V2_2       spot
on   vw.utc_spot_start_date_time    = spot.utc_spot_start_date_time
and  vw.utc_spot_end_date_time = spot.utc_spot_end_date_time
and  vw.service_key = spot.service_key
and  vw.full_name =  spot.full_name
and  vw.clearcast_commercial_no =  spot.clearcast_commercial_no;
;



-- ADD ON SCALING WEIGHTINGS --



ALTER TABLE molson_coors_allspots_viewing_data_new2
ADD ( weighting_date        date
     ,scaling_segment_ID    int
     ,weightings            float default 0);


update molson_coors_allspots_viewing_data_new2
set    weighting_date=cast(a.original_broadcast_date_time_utc as date)
from   molson_coors_allspots_viewing_data_new2 A;


update   molson_coors_allspots_viewing_data_new2
set      scaling_segment_ID = l.scaling_segment_ID
from     molson_coors_allspots_viewing_data_new2 as b
inner join
         bednaszs.v_SC2_Intervals as l
on       b.account_number = l.account_number
and      b.weighting_date between l.reporting_starts and l.reporting_ends;


update    molson_coors_allspots_viewing_data_new2
set       weightings = s.weighting
from      molson_coors_allspots_viewing_data_new2 as b
inner join
          bednaszs.v_SC2_Weightings as s
on        b.weighting_date = s.scaling_day
and       b.scaling_segment_ID = s.scaling_segment_ID;



-- ADD ON MEDIA PACK, SALES HOUSE AND FIX CHANNEL NAME --

-- CHANNEL NAME --




alter table molson_coors_allspots_viewing_data_new2
        add (channel_new varchar(50));


update            molson_coors_allspots_viewing_data_new2
set               cub.channel_new= (case when tmp.channel is not null then tmp.channel else cub.full_name end)
from              molson_coors_allspots_viewing_data_new2 as cub
left outer join   te_channel_lkup as tmp
on                tmp.service_key = cub.service_key;

-- CHANNEL GROUPING (MEDIA PACK) AND SALES HOUSE --




alter table molson_coors_allspots_viewing_data_new2
        add (media_pack varchar(25)
           ,sales_house varchar(25));

update            molson_coors_allspots_viewing_data_new2
set               cub.media_pack = tmp.channel_category

                  ,cub.sales_house = tmp.primary_sales_house
from              molson_coors_allspots_viewing_data_new2 as cub
left outer join   LkUpPack as tmp
on                tmp.service_key = cub.service_key;


-- ADD ON PROGRAMME DETAILS --

alter table molson_coors_allspots_viewing_data_new2
        add (,program_air_datetime            datetime
             ,programme_duration              int
             ,genre_description               varchar(30)
             ,sub_genre_description           varchar(30)
             ,programme_name                  varchar(30));


update            molson_coors_allspots_viewing_data_new2
set               a.program_air_datetime=b.program_air_datetime
                 ,a.genre_description=b.genre_description
                 ,a.sub_genre_description=b.sub_genre_description
                 ,a.programme_name=b.programme_name
from              molson_coors_allspots_viewing_data_new2 as A
left outer join   molson_coors_Program_details_all_spots  as B
on               a.Programme_Trans_Sk = b.dk_programme_instance_dim;


--------------------------------------------------------
--         PART C: CREATE CLIENT SPOTS FILE           --
--------------------------------------------------------


if object_id('molson_coors_allspots_hh_impacts') is not null drop table molson_coors_allspots_hh_impacts;

select     a.cb_key_household
          ,a.weightings
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
into       molson_coors_allspots_hh_impacts
from       molson_coors_allspots_viewing_data_new2       A
where      a.spot_viewed=1 and  a.cb_key_household is not null
group by   a.cb_key_household
          ,a.weightings
          ,utc_start_date
          ,spot_time
          ,daypart
          ,a.utc_spot_start_date_time
          ,a.barb_date_of_transmission
          ,a.barb_spot_start_time
          ,a.clearcast_commercial_no
          ,a.channel_new
          ,a.sales_house
          ,a.media_pack
          ,a.genre_description
          ,a.sub_genre_description
          ,a.programme_name;

grant select on molson_coors_allspots_hh_impacts to gillh;
grant select on molson_coors_allspots_hh_impacts to chans;


-- JOIN ON HOUSEHOLD AGGREGATE TABLE --


if object_id('molson_coors_allspots_hh_exp_impacts') is not null drop table molson_coors_allspots_hh_exp_impacts;

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
into       molson_coors_allspots_hh_exp_impacts
from       molson_coors_allspots_hh_impacts       A
left outer join
           molson_coors_hh_aggregate     B
on         a.cb_key_household=b.cb_key_household;

grant select on molson_coors_allspots_hh_exp_impacts to gillh;
grant select on molson_coors_allspots_hh_exp_impacts to chans;


-- IMPACT PIVOT TABLE --

if object_id('molson_coors_all_spots_impacts') is not null drop table molson_coors_all_spots_impacts;

select     barb_date_of_transmission
          ,barb_spot_start_time
          ,utc_spot_start_date_time
          ,daypart
          ,clearcast_commercial_no
          ,spot_duration
          ,channel_new
          ,sales_house
          ,media_pack
          ,genre_description
          ,sub_genre_description
          ,programme_name
          ,sum(weightings*spots) as total_impacts
          ,sum(brought_audience_hh*spots) as brought_audience_impacts
          ,sum(aspirational_audience_hh*spots) as aspirational_audience_impacts
          ,sum(social_explorers_hh*spots) as social_explorers_impacts
          ,sum(enthusiastic_influencers_hh*spots) as enthusiastic_influencers_impacts
          ,sum(content_routiners_hh*spots) as content_routiners_impacts
          ,sum(safe_and_savy_hh*spots) as safe_and_savy_impacts
          ,sum(considered_balancers_hh*spots) as considered_balancers_impacts
          ,sum(do_not_targets_hh*spots) as do_not_targets_impacts
          ,sum(unknowns_hh*spots) as unknowns_impacts
into       molson_coors_all_spots_impacts
from       molson_coors_allspots_hh_exp_impacts
group by   barb_date_of_transmission
          ,barb_spot_start_time
          ,utc_spot_start_date_time
          ,daypart
          ,clearcast_commercial_no
          ,spot_duration
          ,channel_new
          ,sales_house
          ,media_pack
          ,genre_description
          ,sub_genre_description
          ,programme_name;

grant select on molson_coors_all_spots_impacts to gillh;
grant select on molson_coors_all_spots_impacts to chans;



