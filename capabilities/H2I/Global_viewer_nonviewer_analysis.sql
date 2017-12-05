 /*


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
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:                                                 Skyview H2I
**Analysts:                             Angel Donnarumma        (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson          (Jason.Thompson@skyiq.co.uk)
                                                                                ,Hoi Yu Tang            (HoiYu.Tang@skyiq.co.uk)
                                                                                ,Jose Pitteloud         (jose.pitteloud@skyiq.co.uk)
**Stakeholder:                          SkyIQ
                                                                                ,Jose Loureda           (Jose.Loureda@skyiq.co.uk)
**Due Date:                             11/07/2014
**Project Code (Insight Collation):     v289
**Sharepoint Folder:

        http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FSIGEvolved%2FShared%20Documents%2F01%20Analysis%20Requests%2FV289%20-%20Skyview%20Futures%2F01%20Plans%20Briefs%20and%20Project%20Admin

**Business Brief:

		This is an exploratory script to extract some insight around the BARB viewers and non-viewers, also taking into account
		whether a BARB panellist's viewing is delivered via the PVF or PV2 (one day later) files.
		
**Module:


--------------------------------------------------------------------------------------------------------------
*/

-- Set the target date
CREATE OR REPLACE VARIABLE @processing_date DATE = '2013-09-20';


------------------------------------------------
-- Update barb_weights table (straight from M00)
------------------------------------------------

if object_id('barb_weights') is not null
    drop table barb_weights
;

select  distinct
        date_of_activity_db1
    ,   reporting_panel_code
    ,   household_number
    ,   person_number
    ,   processing_weight/10 as processing_weight
into    barb_weights
from    sk_prod_confidential_customer.BARB_PANEL_MEM_RESP_WGHT
where   
        date_of_activity =  '2013-09-20'
    and reporting_panel_code = 50
;

create hg index hg1 on barb_weights(household_number);
create lf index lf1 on barb_weights(person_number);
grant select on barb_weights to vespa_group_low_security;




---------------------------------------------------------
-- M04.1 - Preparing transient tables (straight from M04)
---------------------------------------------------------
if object_id('skybarb') is not null
    drop table skybarb
;

select
        demo.household_number                                                                           as house_id
    ,   demo.person_number                                                                                     as person
    ,   datepart(year,today())-datepart(year,demo.date_of_birth)       as age
    ,   case
            when demo.sex_code = 1 then 'Male'
            when demo.sex_code = 2 then 'Female'
            else 'Unknown'
        end     as sex
    ,   case
            when demo.household_status in (4,2)  then 1
            else 0
        end     as head
into    skybarb
from
                BARB_INDV_PANELMEM_DET  as demo
    inner join  (
                    select  distinct household_number
                    from    BARB_PANEL_DEMOGR_TV_CHAR
                    where
                            @processing_date between date_valid_from and date_valid_to
                        and reception_capability_code_1 = 2
                )   as barb_sky_panelists       on  demo.household_number   = barb_sky_panelists.household_number
where
        @processing_date between demo.date_valid_from and demo.date_valid_to
    and demo.person_membership_status = 0
;

create hg index hg1     on skybarb(house_id);
create lf index lf1     on skybarb(person);

grant select on skybarb to vespa_group_low_security;                




----------------------------------------------------------
-- M04.2 - Final BARB Data Preparation (straight from M04)
----------------------------------------------------------



/*
        Now constructing a table to be able to check minutes watched across all households based on Barb (weighted to show UK):
        Channel pack, household size, programme genre and the part of the day where these actions happened (breakfast, lunch, etc...)
*/


if object_id('skybarb_fullview') is not null
    drop table skybarb_fullview
;

select
        mega.*
    ,   z.sex
    ,   case   
            when z.age between 1 and 19     then '0-19'
            when z.age between 20 and 24    then '20-24'
            when z.age between 25 and 34    then '25-34'
            when z.age between 35 and 44    then '35-44'
            when z.age between 45 and 64    then '45-64'
            when z.age >= 65                then '65+'  
        end     as ageband
into    skybarb_fullview
from
                (
                    select  --ska.service_key
                            barbskyhhsize.thesize  as hhsize
                        ,   barbskyhhsize.hh_weight
                        ,   base.*
                    from
                                    (
                                        -- multiple aggregations to derive part of the day where the viewing session took place
                                        -- and a workaround to get the minutes watched per each person in the household multiplied
                                        -- by their relevant weights to show the minutes watched by UK (as per barb scaling exercise)...
                                        select
                                                viewing.household_number
                                            ,   viewing.PVF_PV2		-- we've also now pulled this field into skybarb_fullview
                                            ,   dense_rank() over   (   
                                                                        partition by
                                                                                cast(viewing.local_start_time_of_session as date)
                                                                            ,   viewing.household_number
                                                                        order by
                                                                                viewing.set_number
                                                                            ,   viewing.local_start_time_of_session
                                                                    )   as  session_id
                                            ,   dense_rank() over   (
                                                                        partition by    viewing.household_number
                                                                        order by        viewing.local_tv_event_start_date_time||'-'||viewing.set_number
                                                                    )   as  event_id
                                            ,   set_number
                                            ,   viewing.programme_name
                                            ,   local_start_time_of_session    as start_time_of_session
                                            ,   local_end_time_of_session              as end_time_of_session
                                            ,   local_tv_instance_start_date_time      as instance_start
                                            ,   local_tv_instance_end_date_time        as instance_end
                                            ,   local_tv_event_start_date_time     as event_Start
                                            ,   duration_of_session
                                            ,   db1_station_code
                                            ,   case when local_start_time_of_recording is null then local_start_time_of_session else local_start_time_of_recording end as session_start_date_time                                                               -- This field was to link to VPS for programme data
                                            ,   case when local_start_time_of_recording is null then local_end_time_of_session else dateadd(mi, Duration_of_session, local_start_time_of_recording) end as session_end_date_time -- This field was to link to VPS for programme data
                                            ,   case
                                                    when cast(local_start_time_of_session as time) between '00:00:00.000' and '05:59:59.000' then 'night'
                                                    when cast(local_start_time_of_session as time) between '06:00:00.000' and '08:59:59.000' then 'breakfast'
                                                    when cast(local_start_time_of_session as time) between '09:00:00.000' and '11:59:59.000' then 'morning'
                                                    when cast(local_start_time_of_session as time) between '12:00:00.000' and '14:59:59.000' then 'lunch'
                                                    when cast(local_start_time_of_session as time) between '15:00:00.000' and '17:59:59.000' then 'early prime'
                                                    when cast(local_start_time_of_session as time) between '18:00:00.000' and '20:59:59.000' then 'prime'
                                                    when cast(local_start_time_of_session as time) between '21:00:00.000' and '23:59:59.000' then 'late night'
                                                end     as session_daypart
                                            ,   viewing.channel_pack
                                            ,   viewing.channel_name
                                            ,   viewing.genre_description      as programme_genre
                                            ,   weights.person_number
                                            ,   weights.processing_weight      as processing_weight
                                            ,   case when person_1_viewing   = 1 and person_number = 1   then processing_weight*duration_of_session else 0 end as person_1
                                            ,   case when person_2_viewing   = 1 and person_number = 2   then processing_weight*duration_of_session else 0 end as person_2
                                            ,   case when person_3_viewing   = 1 and person_number = 3   then processing_weight*duration_of_session else 0 end as person_3
                                            ,   case when person_4_viewing   = 1 and person_number = 4   then processing_weight*duration_of_session else 0 end as person_4
                                            ,   case when person_5_viewing   = 1 and person_number = 5   then processing_weight*duration_of_session else 0 end as person_5
                                            ,   case when person_6_viewing   = 1 and person_number = 6   then processing_weight*duration_of_session else 0 end as person_6
                                            ,   case when person_7_viewing   = 1 and person_number = 7   then processing_weight*duration_of_session else 0 end as person_7
                                            ,   case when person_8_viewing   = 1 and person_number = 8   then processing_weight*duration_of_session else 0 end as person_8
                                            ,   case when person_9_viewing   = 1 and person_number = 9   then processing_weight*duration_of_session else 0 end as person_9
                                            ,   case when person_10_viewing  = 1 and person_number = 10  then processing_weight*duration_of_session else 0 end as person_10
                                            ,   case when person_11_viewing  = 1 and person_number = 11  then processing_weight*duration_of_session else 0 end as person_11
                                            ,   case when person_12_viewing  = 1 and person_number = 12  then processing_weight*duration_of_session else 0 end as person_12
                                            ,   case when person_13_viewing  = 1 and person_number = 13  then processing_weight*duration_of_session else 0 end as person_13
                                            ,   case when person_14_viewing  = 1 and person_number = 14  then processing_weight*duration_of_session else 0 end as person_14
                                            ,   case when person_15_viewing  = 1 and person_number = 15  then processing_weight*duration_of_session else 0 end as person_15
                                            ,   case when person_16_viewing  = 1 and person_number = 16  then processing_weight*duration_of_session else 0 end as person_16
                                            ,   person_1+person_2+person_3+person_4+person_5+person_6+person_7+person_8+person_9+person_10+person_11+person_12+person_13+person_14+person_15+person_16 as theflag
                                            ,   broadcast_start_date_time_local
                                            ,   broadcast_end_date_time_local
                                            --,case when local_start_time_of_recording >= broadcast_start_date_time_local then local_start_time_of_recording else broadcast_start_date_time_local end x
                                            --,case when local_end_time_of_recording <= broadcast_end_date_time_local then dateadd(mi,1,local_end_time_of_recording) else broadcast_end_date_time_local end y
                                            --,case when local_start_time_of_recording is not null then datediff(mi,x,y) else barb_instance_duration end as target--as progwatch_duration
                                            --,datediff(minute,instance_start,instance_end)         as progwatch_duration 
                                            ,   barb_instance_duration as progwatch_duration
                                            ,   progwatch_duration * processing_weight as progscaled_duration
                                        from    
                                                        ripolile.latest_barb_viewing_table  as viewing
                                            inner join  barb_weights                        as weights  on  viewing.household_number    = weights.household_number
                                        where
                                                viewing.sky_stb_holder_hh = 'Y'
                                            and viewing.panel_or_guest_flag = 'Panel'
                                            and cast(viewing.local_start_time_of_session as date) between @processing_date-29 and @processing_date
                                            --and           viewing.pvf_pv2 = 'PVF' -- To be remove for final version!!!! (We need to consider both PVF and PV2
                                    )   as  base
                        inner join  (
                                        -- fixing barb sample to only barb panellists with Sky (table from prior step)
                                        /*
                                                a.head is a bit that is only set on (1) for the head of household
                                                so below multiplication will only bring the head of household weight 
                                        */
                                        select
                                                a.house_id
                                            ,   count(distinct a.person)        as thesize
                                            ,   sum(a.head*b.processing_weight) as hh_weight
                                        from
                                                        skybarb as a
                                            left join   barb_weights    as b    on  a.house_id  = b.household_number
                                                                                and a.person    = b.person_number
                                        group   by  a.house_id
                                        having  hh_weight > 0
                                    )   as  barbskyhhsize       on      base.household_number   = barbskyhhsize.house_id
--/*	these two inner joins may be replaced with left joins and appropriate defaults for unknown channels etc.
                        inner join  (
                                            -- mapping the db1 station code to the actual service key to find meta data for service key
                                            -- done on the join after this one...
                                            select  db1_station_code, service_key
                                            from    BARB_Channel_Map_v
                                            where   main_sk = 'Y'
                                    )   as map                  on  base.db1_station_code   = map.db1_station_code
                        inner join  (
                                            -- getting metadata for service key
                                            select  service_key
                                                            ,channel_genre
                                                            ,channel_pack
                                            from    vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES
                                            where   activex = 'Y'
                                    )   as ska                  on  map.service_key         = ska.service_key
                    where   base.theflag > 0
--*/
                )       as  mega
    inner join  skybarb as  z   on  mega.household_number   = z.house_id
                                and mega.person_number      = z.person
;


-- create hg index hg1 on skybarb_fullview     (service_key);	-- disabled this since we don't need it, and allows for the 2 inner joins above to also be disabled
create hg index hg2 on skybarb_fullview     (household_number);
create lf index lf1 on skybarb_fullview     (channel_pack);
create lf index lf2 on skybarb_fullview     (programme_genre);
create dttm index dt1 on skybarb_fullview   (start_time_of_session);
create dttm index dt2 on skybarb_fullview   (end_time_of_session);
create dttm index dt3 on skybarb_fullview   (session_start_date_time);
create dttm index dt4 on skybarb_fullview   (session_end_date_time);

grant select on skybarb_fullview to vespa_group_low_security;

/*
SELECT TOP 20 * FROM skybarb_fullview;

*/




------------------------------------------------------------------------------------------------------------------
-- Tag a viewer as returning data via PVF or PV2 files. 
-- A viewer may get picked up in both PVF and PV2, in which case this query makes sure that PVF takes precedence
------------------------------------------------------------------------------------------------------------------
DROP TABLE #TMP1;

SELECT
        start_date_of_session
    ,   sex
    ,   ageband
    ,   household_number
    ,   person_number
    ,   individual
    ,   processing_weight
    ,   PVF_PV2
INTO    #TMP1
FROM
    (
        SELECT
                date(start_time_of_session)                 AS  start_date_of_session
            ,   sex
            ,   ageband
            ,   household_number                    -- we'll need this later
            ,   person_number                       -- we'll need this later
            ,   household_number || '-' || person_number    AS  individual
            ,   processing_weight
            ,   PVF_PV2
            ,   COUNT() OVER    (   PARTITION BY
                                                start_date_of_session
                                            ,   sex
                                            ,   ageband
                                            ,   individual
                                            ,   processing_weight
                                )                           AS  num_entries
            ,   CASE
                    WHEN    PVF_PV2 = 'PVF'                                 THEN    'PVF'
                    WHEN    (PVF_PV2 = 'PV2'    AND num_entries = 1)        THEN    'PV2'
                    WHEN    num_entries = 2                                 THEN    'PVF'
                    ELSE                                                            NULL
                END     AS  PVF_PV2_ADJUSTED
        FROM    skybarb_fullview
        GROUP BY
                start_date_of_session
            ,   sex
            ,   ageband
            ,   household_number                    -- we'll need this later
            ,   person_number                       -- we'll need this later
            ,   individual
            ,   processing_weight
            ,   PVF_PV2
    )   AS  A
WHERE   PVF_PV2 =   PVF_PV2_ADJUSTED    -- This ensures we only have new viewers from PV2
ORDER BY
        start_date_of_session
    ,   sex
    ,   ageband
    ,   household_number
    ,   person_number
    ,   individual
    ,   processing_weight
    ,   PVF_PV2
;

CREATE DATE INDEX DT_IDX_1 ON #TMP1(start_date_of_session);
CREATE HG INDEX HG_IDX_1 ON #TMP1(household_number);
CREATE HG INDEX HG_IDX_2 ON #TMP1(person_number);
CREATE HG INDEX HG_IDX_3 ON #TMP1(individual);
CREATE HG INDEX HG_IDX_4 ON #TMP1(PVF_PV2);





------------------------------------------------------------------------------------------------------------------------
-- Ok, so far, we have calculated the VIEWERS and split them according to the data return (PVF/PV2)
-- Now we need the non-viewers by joining onto the the HOUSEHOLDS THAT REPORT VIEWING AND GET ALL OF THE OTHER OCCUPANTS
------------------------------------------------------------------------------------------------------------------------

-- Create temporary table of VIEWER households
DROP TABLE #HHD;
SELECT
        start_date_of_session
    ,   household_number
INTO    #HHD
FROM    #TMP1
GROUP BY
        start_date_of_session
    ,   household_number
;
CREATE DATE INDEX DT_IDX_1 ON #HHD(start_date_of_session);
CREATE HG INDEX HG_IDX_1 ON #HHD(household_number);



-- Now create the final table of individuals by PVF/PV2 and viewer/non-viewer dimension
DROP TABLE  #TMP2;

SELECT
        start_date_of_session
    ,   PVF_PV2
    ,   individual
    ,   processing_weight
    ,   sex
    ,   ageband
    ,   VIEWER
INTO    #TMP2
FROM
    (
        SELECT
                HHD.start_date_of_session
            ,   WEI.household_number || '-' || WEI.person_number    AS  individual
            ,   WEI.processing_weight
            ,   SKB.sex
            ,   CASE
                    WHEN SKB.age BETWEEN 1 AND 19     THEN '0-19'
                    WHEN SKB.age BETWEEN 20 AND 24    THEN '20-24'
                    WHEN SKB.age BETWEEN 25 AND 34    THEN '25-34'
                    WHEN SKB.age BETWEEN 35 AND 44    THEN '35-44'
                    WHEN SKB.age BETWEEN 45 AND 64    THEN '45-64'
                    WHEN SKB.age >= 65                THEN '65+'
                END                                                 AS ageband                                   -- Note: also re-used the field name 'ageband' here for the same reasons as above

            ,   PV.PVF_PV2
            ,   CASE
                    WHEN    BAS.person_number  IS NULL     THEN     0
                    ELSE                                            1
                END                                                 AS  VIEWER
            ,   COUNT()     OVER    (   PARTITION BY
                                                HHD.start_date_of_session
                                            ,   WEI.household_number
                                            ,   PV.PVF_PV2
                                    )                                   AS  HHSIZE  -- this aggregation should really be taken outside of this query...
            ,   SUM(VIEWER) OVER    (   PARTITION BY
                                                HHD.start_date_of_session
                                            ,   WEI.household_number
                                            ,   PV.PVF_PV2
                                    )                                   AS  HH_VIEWERS
        FROM
                        barb_weights    AS  WEI
            INNER JOIN  skybarb         AS  SKB     ON  WEI.household_number        =   SKB.house_id            -- we need skybarb to get use the gender-age data for non-viewers
                                                    AND WEI.person_number           =   SKB.person
            INNER JOIN  #HHD            AS  HHD     ON  WEI.household_number        =   HHD.household_number
            CROSS JOIN  (
                            SELECT  'PVF'   AS  PVF_PV2 UNION ALL
                            SELECT  'PV2'   AS  PVF_PV2
                        )               AS  PV
            LEFT JOIN   #TMP1           AS  BAS     ON  HHD.household_number        =   BAS.household_number    -- now join to identify our viewing and non-viewing individuals
                                                    AND WEI.person_number           =   BAS.person_number
                                                    AND HHD.start_date_of_session   =   BAS.start_date_of_session
                                                    AND PV.PVF_PV2                  =   BAS.PVF_PV2
    )   AS  A
WHERE   HH_VIEWERS  >   0   -- filter out rows where there is no actual viewing from that household (usually lack of PV2)
ORDER BY
        start_date_of_session
    ,   PVF_PV2
    ,   individual
    ,   processing_weight
    ,   sex
    ,   ageband
    ,   VIEWER
;


CREATE DATE INDEX DT_IDX_1 ON #TMP2(start_date_of_session);
CREATE HG INDEX HG_IDX_1 ON #TMP2(PVF_PV2);
CREATE HG INDEX HG_IDX_2 ON #TMP2(individual);
CREATE HG INDEX HG_IDX_3 ON #TMP2(processing_weight);
CREATE HG INDEX HG_IDX_4 ON #TMP2(sex);
CREATE HG INDEX HG_IDX_5 ON #TMP2(ageband);
CREATE HG INDEX HG_IDX_6 ON #TMP2(VIEWER);




-- Finally, perform the aggregation to give us the date/viewer-non-viewer/PVF-PV2/gender/age split!
SELECT
        start_date_of_session
    ,   sex
    ,   ageband
    ,   PVF_PV2
    ,   VIEWER
    ,   COUNT(DISTINCT individual)      AS  number_of_viewers
    ,   SUM(processing_weight)          AS  number_of_viewers_weighted
FROM    #TMP2
GROUP BY    
        start_date_of_session
    ,   sex
    ,   ageband
    ,   PVF_PV2
    ,   VIEWER
ORDER BY
        start_date_of_session
    ,   sex
    ,   ageband
    ,   PVF_PV2
    ,   VIEWER
;






















