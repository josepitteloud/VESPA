-- Star Schema version
----------------------


/* Scenario 0 (internal reconcilation)
        total seconds viewed & unique HHs (actual)
        viewing between 20/12 & 24/12
        split by Day
        split by viewing type (live/playback etc.)
*/
  if object_id('uat_scenario0_star') is not null then drop table uat_scenario0_star endif;
  select bas.household_key
        ,hsh.cb_key_household
        ,hsh.account_number
        ,bas.event_viewed_flag
        ,bas.programme_viewed_flag
        ,shf.viewing_type
        ,bds.utc_day_date as viewing_date
        ,bdt.utc_time_minute as start_time
        ,bdx.utc_time_minute as end_time
        ,bas.viewed_duration
        ,case
           when bas.viewed_duration % 10 = 0 then bas.viewed_duration
           when bas.viewed_duration % 10 = 1 then bas.viewed_duration - 1
             else bas.viewed_duration
         end viewed_duration_revised
    into uat_scenario0_star
    from sk_prod.viq_viewing_data_uat                as bas
         inner join sk_prod.viq_date                 as bds on bas.viewing_start_date_key   = bds.pk_datehour_dim
         inner join sk_prod.viq_time                 as bdt on bas.viewing_start_time_key   = bdt.pk_time_dim
         inner join sk_prod.viq_time                 as bdx on bas.viewing_end_time_key     = bdx.pk_time_dim
         inner join sk_prod.viq_time_shift           as shf on bas.time_shift_key           = shf.pk_timeshift_dim
         inner join sk_prod.viq_household            as hsh on bas.household_key            = hsh.household_key
         inner join sk_prod.viq_programme_uat        as prg on bas.prog_inst_programme_key  = prg.pk_programme_dim
         inner join sk_prod.viq_channel_uat          as chn on bas.prog_inst_channel_key    = chn.pk_channel_dim
   where viewing_start_date_key >= 2012122000                                                   -- Scenario filter
     and viewing_start_date_key <= 2012122423                                                   -- Scenario filter
     and bas.household_key is not null                                                          -- Common filter
     and bas.household_key > 0                                                                  -- Common filter
;
commit;

  select
         viewing_type
        ,viewing_date
        ,sum(viewed_duration) as sum_viewed_duration
        ,sum(viewed_duration_revised) as sum_viewed_duration_revised
        ,count(distinct household_key) as cnt_household_key
        ,count(distinct cb_key_household) as cnt_cb_key_household
    from uat_scenario0_star
group by viewing_type
        ,viewing_date
;








/*
Scenario 1
        total seconds viewed (metrics)
        watching Channel 4 (filter)
        split by Channel 4 SD & HD (attribute)
        split by Day (viewing between 04/12 & 13/12 )	(attribute)
        split by Mosaic Segments (attribute)
(reviewed)
*/
  if object_id('uat_scenario1_star') is not null then drop table uat_scenario1_star endif;
  select channel_name
        ,mosaic_segments
        ,dat.local_day_long
        ,sum(viewed_duration) as seconds
    into uat_scenario1_star
    from sk_prod.viq_viewing_data_uat       as bas
         inner join sk_prod.viq_channel_uat as chn on bas.prog_inst_channel_key  = chn.pk_channel_dim
         inner join sk_prod.viq_household   as hsh on bas.household_key          = hsh.household_key
         inner join sk_prod.viq_date        as dat on bas.viewing_start_date_key = dat.pk_datehour_dim
   where channel_name in ('Channel 4', 'Channel 4 HD')                                          -- Scenario filter
     and dat.local_day_date between '2013-01-03' and '2013-01-09'
group by channel_name
        ,mosaic_segments
        ,dat.local_day_long
; -- 0 records
commit;



/*
select c.CHANNEL_NAME, h.MOSAIC_SEGMENTS, ds.LOCAL_DAY_LONG, sum(vd.DURATION)
from VIEWING_DATA vd
              inner join CHANNEL  c on vd.PROG_INST_CHANNEL_KEY = c.CHANNEL_KEY
              inner join household h on vd.HOUSEHOLD_KEY = h.HOUSEHOLD_KEY
              inner join date ds on vd.VIEWING_ST_DATE_KEY = ds.DATE_KEY
where c.CHANNEL_NAME in('Channel 4', 'Channel 4 HD')
and ds.LOCAL_DAY_DATE between = '2013-01-03' and '2013-01-09'
Group by ds.LOCAL_DAY_LONG, c.CHANNEL_NAME, h.MOSAIC_SEGMENTS
order by ds.LOCAL_DAY_LONG, c.CHANNEL_NAME,h.MOSAIC_SEGMENTS
*/











/*
Scenario 2.1
        total number of unique households (actual)	(metrics)
        VOSDAL viewing	(filter)
        watching "Top Gear"	(filter)
        watching for 3 minutes or more	(filter)
        split by Channel Group	(attribute)
        split by Day (viewing between 04/12 & 13/12) 	(attribute)
        split by Household Composition	(attribute)
(reviewed)

Scenario 2.2
        total number of unique households (scaled)	(metrics)
        VOSDAL viewing	(filter)
        watching Top Gear	(filter)
        watching for 3 minutes or more	(filter)
        split by Channel Group	(attribute)
        split by Day (viewing on 11/12) 	(attribute)
        split by Household Composition	(attribute)
(reviewed)
*/
  if object_id('uat_scenario2_star') is not null then drop table uat_scenario2_star endif;
  select grouping_indicator
        ,local_day_date
        ,household_composition
        ,count(distinct hsh.cb_key_household) as HH_cnt
    into uat_scenario2_star
    from sk_prod.viq_viewing_data_uat                    as bas
           inner join sk_prod.viq_channel_uat              as chn on bas.prog_inst_channel_key    = chn.pk_channel_dim
           inner join sk_prod.viq_date                     as dts on bas.viewing_start_date_key   = dts.pk_datehour_dim
           inner join sk_prod.viq_programme_uat            as prg on bas.prog_inst_programme_key  = prg.pk_programme_dim
           inner join sk_prod.viq_household                as hsh on bas.household_key            = hsh.household_key
           inner join sk_prod.viq_time_shift               as shf on bas.time_shift_key           = shf.pk_timeshift_dim
       --  inner join sk_prod.viq_viewing_data_scaling_uat as scl on bas.household_key            = scl.household_key
       --                                                        and dts.utc_day_date             = scl.adjusted_event_start_date_vespa         -- ??? Check cartesian
   where viewing_type       = 'Vosdal'                                                          -- Scenario filter
     and lower(programme_name) = 'top gear'                                                     -- Scenario filter
     and bas.programme_viewed_flag = 1                                                          -- Scenario filter
     and dts.local_day_date between '2013-01-01' and '2013-01-07'
   group by grouping_indicator
        ,local_day_date
        ,household_composition;

     --and viewing_start_date_key  >= 2012120400                                                  -- Scenario filter
    -- and viewing_start_date_key  <= 2012121323                                                  -- Scenario filter
--     and bas.household_key is not null                                                          -- Common filter
--     and bas.household_key > 0                                                                  -- Common filter
;
commit;




  if object_id('uat_scenario2_star') is not null then drop table uat_scenario2_star endif;
  select grouping_indicator
        ,local_day_date
        ,household_composition
        ,sum(calculated_scaling_weight) as HH_cnt_scaled
    into uat_scenario2_star
    from sk_prod.viq_viewing_data_uat                    as bas
           inner join sk_prod.viq_channel_uat              as chn on bas.prog_inst_channel_key    = chn.pk_channel_dim
           inner join sk_prod.viq_date                     as dts on bas.viewing_start_date_key   = dts.pk_datehour_dim
           inner join sk_prod.viq_programme_uat            as prg on bas.prog_inst_programme_key  = prg.pk_programme_dim
           inner join sk_prod.viq_household                as hsh on bas.household_key            = hsh.household_key
           inner join sk_prod.viq_time_shift               as shf on bas.time_shift_key           = shf.pk_timeshift_dim
           inner join sk_prod.viq_viewing_data_scaling_uat as scl on bas.household_key            = scl.household_key
                                                                 and dts.utc_day_date             = scl.adjusted_event_start_date_vespa         -- ??? Check cartesian
   where viewing_type       = 'Vosdal'                                                          -- Scenario filter
     and lower(programme_name) = 'top gear'                                                     -- Scenario filter
     and bas.programme_viewed_flag = 1                                                          -- Scenario filter
     and dts.local_day_date between '2013-01-01' and '2013-01-07'
   group by grouping_indicator
        ,local_day_date
        ,household_composition;

     --and viewing_start_date_key  >= 2012120400                                                  -- Scenario filter
    -- and viewing_start_date_key  <= 2012121323                                                  -- Scenario filter
--     and bas.household_key is not null                                                          -- Common filter
--     and bas.household_key > 0                                                                  -- Common filter
;
commit;


/*
2.1

select c.CHANNEL_GROUP, ds.LOCAL_DAY_DATE,
       h.HOUSEHOLD_COMPOSITION, (count(distinct h.cb_key_household)) as 'total households'
from VIEWING_DATA vd inner join CHANNEL c on vd.PROG_INST_CHANNEL_KEY = c.CHANNEL_KEY
    Inner join PROGRAMME p on vd.PROG_INST_PROG_KEY = p.PROGRAMME_KEY
    inner join household h on vd.HOUSEHOLD_KEY = h.HOUSEHOLD_KEY
    inner join TIME_SHIFT ts on vd.TIME_SHIFT_KEY = ts.TIME_SHIFT_KEY
    inner join date ds on vd.VIEWING_ST_DATE_KEY = ds.DATE_KEY
   -- inner join VIEWING_DATA_SCALING vds on vds.HOUSEHOLD_KEY = vd.HOUSEHOLD_KEY
   --            and vds.SCALING_DATE_KEY / 100 = vd.VIEWING_ST_DATE_KEY / 100

where p.PROGRAMME_NAME = 'Top Gear'
and ts.VIEWING_TYPE = 'vosdal'
and vd.PROGRAMME_VIEWED_FLAG = 1
and ds.LOCAL_DAY_DATE between '2013-01-01' and '2013-01-07'
group by c.CHANNEL_GROUP, ds.LOCAL_DAY_DATE, h.HOUSEHOLD_COMPOSITION


2.2
select c.CHANNEL_GROUP, ds.LOCAL_DAY_DATE,
       h.HOUSEHOLD_COMPOSITION, sum(round(vds.[WEIGHTING],0)) as 'total households'
from VIEWING_DATA vd inner join CHANNEL c on vd.PROG_INST_CHANNEL_KEY = c.CHANNEL_KEY
    Inner join PROGRAMME p on vd.PROG_INST_PROG_KEY = p.PROGRAMME_KEY
    inner join household h on vd.HOUSEHOLD_KEY = h.HOUSEHOLD_KEY
    inner join TIME_SHIFT ts on vd.TIME_SHIFT_KEY = ts.TIME_SHIFT_KEY
    inner join date ds on vd.VIEWING_ST_DATE_KEY = ds.DATE_KEY
    inner join VIEWING_DATA_SCALING vds on vds.HOUSEHOLD_KEY = vd.HOUSEHOLD_KEY
               and vds.SCALING_DATE_KEY / 100 = vd.VIEWING_ST_DATE_KEY / 100

where p.PROGRAMME_NAME = 'Top Gear'
and ts.VIEWING_TYPE = 'vosdal'
and vd.PROGRAMME_VIEWED_FLAG = 1
and ds.LOCAL_DAY_DATE between '2013-01-01' and '2013-01-07'
group by c.CHANNEL_GROUP, ds.LOCAL_DAY_DATE, h.HOUSEHOLD_COMPOSITION



*/





/*
Scenario 3
      total hours viewed	(metrics)
      between 14 & 21 days after original transmission time	(filter)
      watching Eastenders 	(filter)
      broadcasted on 06 December between 19:30-22:30 	(filter)
      split by Affluence Bands	(attribute)
(reviewed)
*/
  if object_id('uat_scenario3_star') is not null then drop table uat_scenario3_star endif;
  select 1.0 * round(1.0 * sum(viewed_duration) / 3600, 4) as hours
        ,count(*)
        ,count(distinct cb_key_household)
        ,affluence_bands
    into uat_scenario3_star
    from sk_prod.VIQ_VIEWING_DATA_UAT                as bas
      --     inner join sk_prod.viq_date                 as vws on bas.viewing_start_date_key   = vws.pk_datehour_dim
           inner join sk_prod.viq_date                 as bds on bas.broadcast_start_date_key = bds.pk_datehour_dim
           inner join sk_prod.viq_time                 as bts on bas.broadcast_start_time_key = bts.pk_time_dim
           inner join sk_prod.viq_programme_uat        as prg on bas.prog_inst_programme_key  = prg.pk_programme_dim
           inner join sk_prod.viq_household            as hsh on bas.household_key            = hsh.household_key
           inner join sk_prod.viq_time_shift           as shf on bas.time_shift_key           = shf.pk_timeshift_dim
   where lower(programme_name) = 'eastenders'                                                   -- Scenario filter
    -- and vws.local_day_date between '2013-01-05' and '2013-01-09'
     and bds.local_day_date            = '2013-01-03'                                           -- Scenario filter
     and bts.local_time_minute         >= '19:30:00'                                            -- Scenario filter
     and bts.local_time_minute         <= '22:30:00'                                            -- Scenario filter
     and shf.elapsed_days in ('3','4','5','6')
group by affluence_bands
;
commit;


/*
select h.AFFLUENCE_BANDS, round((sum(vd.DURATION)/60.0)/60.0, 4)
  from VIEWING_DATA vd inner join date BroadcastDate on vd.PROG_INST_BROADCAST_ST_DATE_KEY = BroadcastDate.DATE_KEY
                      inner join time BroadcastTime on vd.PROG_INST_BROADCAST_ST_TIME_KEY = BroadcastTime.TIME_KEY
                      Inner join PROGRAMME p on vd.PROG_INST_PROG_KEY = p.PROGRAMME_KEY
                      inner join household h on vd.HOUSEHOLD_KEY = h.HOUSEHOLD_KEY
                      inner join TIME_SHIFT ts on vd.TIME_SHIFT_KEY = ts.TIME_SHIFT_KEY
                     -- inner join VIEWING_DATA_SCALING vds on vds.HOUSEHOLD_KEY = vd.HOUSEHOLD_KEY
          -- and vds.SCALING_DATE_KEY / 100 = vd.VIEWING_ST_DATE_KEY / 100
where BroadcastDate.LOCAL_DAY_DATE = '2013-01-03'
  and BroadcastTime.LOCAL_TIME between '19:30:00' and '22:30:00'
  and p.PROGRAMME_NAME = 'EastEnders'
 -- and p.PROGRAMME_GENRE = 'Entertainment'
 -- and p.PROGRAMME_SUB_GENRE = 'Soaps'
  and ts.ELAPSED_DAYS between 4 and 6
group by h.AFFLUENCE_BANDS
*/









/*
Scenario 4
      total minutes viewed	(metrics)
      watching “3-2-1”  	(filter)
      on Challenge 	(filter)
      broadcasted between 04/12 and 13/12 	(filter)
      watched by 31/12 	(filter)
      split by individual episode in series number 	(attribute)
      split by Live, VOSDAL, Timeshift 1-7 days, Timeshift 8-28 days	(attribute)
(reviewed)
*/
  if object_id('uat_scenario4_star') is not null then drop table uat_scenario4_star endif;
  select episode_number
        ,timeshift_band
        ,viewing_type
        ,sum(viewed_duration) as minutes
    into uat_scenario4_star
    from sk_prod.viq_viewing_data_uat                as bas
           inner join sk_prod.viq_programme_uat        as prg on bas.prog_inst_programme_key  = prg.pk_programme_dim
           inner join sk_prod.viq_channel_uat          as chn on bas.prog_inst_channel_key    = chn.pk_channel_dim
           inner join sk_prod.viq_time_shift           as shf on bas.time_shift_key           = shf.pk_timeshift_dim
           inner join sk_prod.viq_date                 as bds on bas.broadcast_start_date_key = bds.pk_datehour_dim
   where lower(programme_name) = 'eggheads'                                                               -- Scenario filter
     and lower(channel_name) like 'challenge'                                                   -- Scenario filter
     and local_day_date between '2013-01-03' and '2013-01-13'
group by episode_number
        ,viewing_type
        ,timeshift_band
; --
commit;



/*
SELECT p.EPISODE
	, ts.BAND,ts.VIEWING_TYPE
	, SUM(vd.[DURATION]) AS DURATION
FROM [dbo].[VIEWING_DATA] vd INNER JOIN [dbo].[PROGRAMME] p ON p.PROGRAMME_KEY = vd.PROG_INST_PROG_KEY
                            INNER JOIN [dbo].[CHANNEL] c ON c.channel_key = vd.[PROG_INST_CHANNEL_KEY]
                            INNER JOIN date d ON vd.PROG_INST_BROADCAST_ST_DATE_KEY = d.DATE_KEY
                        --    INNER JOIN date d1 ON vd.VIEWING_ST_DATE_KEY = d1.DATE_KEY
                            INNER JOIN time_shift ts ON ts.TIME_SHIFT_KEY = vd.TIME_SHIFT_KEY
WHERE d.LOCAL_DAY_DATE between '2013-01-03' and '2013-01-07'
-- AND d1.LOCAL_DAY_DATE <'2012-12-31'
AND c.CHANNEL_NAME = 'Challenge'
AND p.PROGRAMME_NAME = '3-2-1'
-- AND p.PROGRAMME_GENRE = 'Entertainment'
-- AND p.PROGRAMME_SUB_GENRE = 'Game Shows'
GROUP BY p.EPISODE
	, ts.VIEWING_TYPE
	 ,ts.BAND
*/



















/*
Scenario 5
      total number of unique households (actual) that watched repeat programmes	(metrics)
      viewing between 10/12 & 12/12	(filter)
      split by Channel Genre	(attribute)
      split by broadband (yes/no) and further by broadband line tenure 	(attribute)
      split by Region	(attribute)
(reviewed)
*/
  if object_id('uat_scenario5_star') is not null then drop table uat_scenario5_star endif;
  select chn.channel_genre
        ,sch.repeat_flag
        ,hh_has_broadband
        ,tenure_broadband
        ,government_region
        ,count(distinct cb_key_household) as HHs
    into uat_scenario5_star
    from sk_prod.viq_viewing_data_uat                             as bas
         inner join sk_prod.viq_channel_uat                       as chn on bas.prog_inst_channel_key    = chn.pk_channel_dim
         inner join sk_prod.viq_household                         as hsh on bas.household_key            = hsh.household_key
         inner join sk_prod.viq_prog_sched_properties             as sch on cast(bas.prog_inst_properties_key as bigint)  = sch.pk_programme_instance_properties
         inner join sk_prod.viq_date                              as bds on bas.viewing_start_date_key = bds.pk_datehour_dim
   where bds.local_day_date  between '2013-01-05' and '2013-01-07'                                           -- Scenario filter
   -- and bas.household_key is not null                                                          -- Common filter
   -- and bas.household_key > 0                                                                  -- Common filter
group by channel_genre
        ,repeat_flag
        ,hh_has_broadband
        ,tenure_broadband
        ,government_region
;
commit;



/*
SELECT c.CHANNEL_GENRE,
       psp.REPEAT_FLAG,
       h.HH_HAS_BROADBAND,
       h.TENURE_BROADBAND,
       h.GOVERNMENT_REGION
     , COUNT(DISTINCT h.cb_key_household )
  FROM VIEWING_DATA vd
        INNER JOIN HOUSEHOLD h ON h.HOUSEHOLD_KEY = vd.HOUSEHOLD_KEY
        INNER JOIN CHANNEL c ON c.CHANNEL_KEY = vd.PROG_INST_CHANNEL_KEY
        INNER JOIN PROGRAMME_SCHEDULE_PROPERTIES psp ON psp.PROGRAMME_SCHEDULE_PROPERTIES_KEY = vd.PROGRAMME_SCHEDULE_PROPERTIES_KEY
        INNER JOIN dbo.DATE ViewingDate ON vd.VIEWING_ST_DATE_KEY = ViewingDate.DATE_KEY
WHERE ViewingDate.LOCAL_DAY_DATE between '2013-01-05' and '2013-01-07'
-- AND BroadcastDate.LOCAL_DAY_DATE between '2013-12-10' and '2012-12-12'
-- AND psp.REPEAT_FLAG = 0
GROUP BY c.CHANNEL_GENRE, psp.REPEAT_FLAG, h.HH_HAS_BROADBAND, h.TENURE_BROADBAND, h.GOVERNMENT_REGION
ORDER BY c.CHANNEL_GENRE
       , psp.REPEAT_FLAG
       , h.HH_HAS_BROADBAND
       , h.TENURE_BROADBAND
       , h.GOVERNMENT_REGION
*/








/*
Scenario 6
        total Programme Scheduled duration      (metrics)
        broadcasted between 05/12 & 04/12       (filter)
        split by Channel Type   (attribute)
        split by Channel Genre  (attribute)
        split by Day    (attribute)
(reviewed)
*/
  if object_id('uat_scenario6_star') is not null then drop table uat_scenario6_star endif;
  select pay_free_indicator
        ,channel_genre
        ,utc_day_date
        ,sum(programme_instance_duration) as prgm_duration
    into uat_scenario6_star
    from sk_prod.viq_programme_schedule_uat          as prg
         inner join sk_prod.viq_channel_uat          as chn on prg.dk_channel_id            = chn.pk_channel_dim
         inner join sk_prod.viq_date                 as vws on prg.dk_start_datehour        = vws.pk_datehour_dim
   where dk_start_datehour >= 2013010100
     and dk_start_datehour <= 2013011023
group by pay_free_indicator
        ,channel_genre
        ,utc_day_date
;
commit;



/*
select c.CHANNEL_TYPE, c.CHANNEL_genre, d.LOCAL_DAY_DATE
     , sum(ps.PROGRAMME_SCHEDULE_DURATION) as 'programme schedule duration'
from PROGRAMME_SCHEDULE ps
            inner join channel c on ps.CHANNEL_KEY = c.CHANNEL_KEY
            inner join date d on ps.BROADCAST_START_DATE_KEY = d.DATE_KEY
where d.LOCAL_DAY_DATE between '2013-01-01' and '2013-01-10'
group by c.CHANNEL_TYPE, c.CHANNEL_GENRE, d.LOCAL_DAY_DATE
*/











/*
Scenario 7
      total scaled duration	(metrics)
      watched Live	(filter)
      broadcasted between 27/12 & 31/12 (by broadcast UTC start date) 	(filter)
      watched more than one episode in series	(filter)
            watched by 31/12 	(filter)
      split by Programme Genre	(attribute)
      split by number of episodes watched	(attribute)
(reviewed)
*/

  if object_id('uat_scenario7_star') is not null then drop table uat_scenario7_star end if;
  select bas.household_key
        ,prg.genre_description
        ,season_ref
        ,count(distinct episode_number) as episodes_watched
        -- ,calculated_scaling_weight
        ,sum(viewed_duration) as viewed_duration
        --,sum(viewed_duration * calculated_scaling_weight) as viewed_duration_scaled
    into uat_scenario7_star
    from sk_prod.viq_viewing_data_uat                    as bas
--         inner join sk_prod.viq_viewing_data_scaling_uat as scl on bas.household_key            = scl.household_key
--                                                               and bds.utc_day_date             = scl.adjusted_event_start_date_vespa
         inner join sk_prod.viq_programme_uat            as prg on bas.prog_inst_programme_key  = prg.pk_programme_dim
         inner join sk_prod.viq_time_shift               as shf on bas.time_shift_key           = shf.pk_timeshift_dim
   where viewing_type = 'Live'                                                                  -- Scenario filter
     and broadcast_start_date_key  >= 2013010200                                                -- Scenario filter
     and broadcast_start_date_key  <= 2013010523                                                -- Scenario filter
   group by bas.household_key, genre_description, season_ref
;
commit;


select
      genre_description,
      episodes_watched,
      sum(viewed_duration) as viewed_duration
  from uat_scenario7_star
 where episodes_watched > 1
 group by genre_description, episodes_watched;



/*

  if object_id('uat_scenario7_star_results') is not null then drop table uat_scenario7_star_results end if;
  select 7                                          as Scenario
        ,genre_description                          as Attribute_1
        ,episodes_watched                           as Attribute_2
        ,cast(null as varchar(50))                  as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,sum(duration_scaled)                       as Metric
    into uat_scenario7_star_results
    from (select
                household_key,
                season_ref,
                genre_description,
                count(distinct episode_number) episodes_watched,
                sum(viewed_duration_scaled) as duration_scaled
            from uat_scenario7_star
           group by household_key, season_ref, genre_description) a
   where episodes_watched > 1
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5;
commit;
 */



/*
select
      PROGRAMME_GENRE,
      episodes_watched,
      sum(DURATION) as DURATION
  from (select
              vd.household_key,
              p.PROGRAMME_GENRE,
              p.season,
              count(distinct p.episode) as episodes_watched,
              SUM(vd.DURATION) as DURATION
        from VIEWING_DATA vd
            --    INNER JOIN HOUSEHOLD h on h.HOUSEHOLD_KEY = vd.HOUSEHOLD_KEY
                INNER JOIN  [dbo].[PROGRAMME] p ON p.PROGRAMME_KEY = vd.PROG_INST_PROG_KEY
                inner join TIME_SHIFT ts on vd.TIME_SHIFT_KEY = ts.TIME_SHIFT_KEY
            --    INNER JOIN VIEWING_DATA_SCALING vds ON vds.HOUSEHOLD_KEY = vd.HOUSEHOLD_KEY
            --                AND vds.SCALING_DATE_KEY / 100 = vd.VIEWING_ST_DATE_KEY / 100
        where ts.VIEWING_TYPE = 'live'
          and broadcast_st_date_key >= 2013010200
          and broadcast_st_date_key <= 2013010523
        group by vd.household_key, p.PROGRAMME_GENRE, p.season) a
where episodes_watched > 1
group by PROGRAMME_GENRE, episodes_watched
*/












/*
Scenario 8
      total hours viewed 	(metrics)
      viewing between 17/12 & 30/12 	(filter)
      for programmes broadcasted between 17/12 & 30/12 	(filter)
      split by Live, VOSDAL, Playback	(attribute)
      split by Sensitive Channel 	(attribute)
      split by Repeat Flag	(attribute)
(reviewed)
*/
  if object_id('uat_scenario8_star') is not null then drop table uat_scenario8_star end if;
  select shf.viewing_type
        ,chn.sensitive_channel
        ,sch.repeat_flag
        ,sum(viewed_duration) as duration
    into uat_scenario8_star
    from sk_prod.viq_viewing_data_uat                    as bas
         inner join sk_prod.viq_channel_uat              as chn on bas.prog_inst_channel_key    = chn.pk_channel_dim
         inner join sk_prod.viq_time_shift               as shf on bas.time_shift_key           = shf.pk_timeshift_dim
         inner join sk_prod.viq_prog_sched_properties    as sch on cast(bas.prog_inst_properties_key as bigint) = sch.pk_programme_instance_properties
         inner join sk_prod.viq_date                     as vws on bas.viewing_start_date_key   = vws.pk_datehour_dim
         inner join sk_prod.viq_date                     as brs on bas.broadcast_start_date_key = brs.pk_datehour_dim
   where vws.local_day_date between '2013-01-02' and '2013-01-10'
     and brs.local_day_date between '2013-01-02' and '2013-01-10'
group by viewing_type
        ,sensitive_channel
        ,repeat_flag
;
commit;



/*
select ts.VIEWING_TYPE
     , c.[SENSITIVE_CHANNEL]
     , psp.REPEAT_FLAG
	 , sum(vd.duration)
from [dbo].[VIEWING_DATA] vd
              inner join [dbo].[CHANNEL] c on c.CHANNEL_KEY = vd.PROG_INST_CHANNEL_KEY
              inner join time_shift ts on ts.TIME_SHIFT_KEY = vd.TIME_SHIFT_KEY
              inner join [dbo].[PROGRAMME_SCHEDULE_PROPERTIES] psp on psp.PROGRAMME_SCHEDULE_PROPERTIES_KEY = Vd.PROGRAMME_SCHEDULE_PROPERTIES_KEY
              inner join date ViewingDate on vd.VIEWING_ST_DATE_KEY = ViewingDate.DATE_KEY
              inner join date BroadcastDate on vd.BROADCAST_ST_DATE_KEY = BroadcastDate.DATE_KEY
 where ViewingDate.LOCAL_DAY_DATE between '2013-01-02' and '2013-01-10'
   and BroadcastDate.LOCAL_DAY_DATE between '2013-01-02' and '2013-01-10'
 group by ts.VIEWING_TYPE, c.[SENSITIVE_CHANNEL], psp.REPEAT_FLAG;
*/










/*
Scenario 9
      total number of 3min+ events viewed	(metrics)
      watched between 04/12 & 06/12 	(filter)
      split by Live, VOSDAL, Playback	(attribute)
(reviewed)
*/
  if object_id('uat_scenario9_star') is not null then drop table uat_scenario9_star end if;
  select shf.viewing_type
        ,count(*) as events_num
    into uat_scenario9_star
    from sk_prod.viq_viewing_data_uat                as bas
         inner join sk_prod.viq_time_shift           as shf on bas.time_shift_key           = shf.pk_timeshift_dim
         inner join sk_prod.viq_date                 as vws on bas.viewing_start_date_key   = vws.pk_datehour_dim
   where event_viewed_flag = 1                                                                  -- Scenario filter
     and vws.local_day_date between '2013-01-05' and '2013-01-08'
group by viewing_type
;
commit;


/*
select
      ts.VIEWING_TYPE,
      count(*) as 'total number of 3min+ events viewed'
from VIEWING_DATA vd
            inner join TIME_SHIFT ts on vd.TIME_SHIFT_KEY = ts.TIME_SHIFT_KEY
            inner join date d on vd.VIEWING_ST_DATE_KEY = d.DATE_KEY
where vd.event_viewed_flag = '1'
  and d.LOCAL_DAY_DATE between '2013-01-05' and '2013-01-08'
group by ts.VIEWING_TYPE
*/









/*
Scenario 10.1
      total number of unique households (actual)	(metrics)
      viewing between 10/12 &12/12 	(filter)
      split by Household composition	(attribute)
      split by Property Type	(attribute)
      split by Government Region	(attribute)
      split by Tenure DTH	(attribute)
(reviewed)

Scenario 10.2
      total number of unique households (actual)	(metrics)
      viewing between 10/12 &12/12 	(filter)
      split by BARB ITV Region	(attribute)
      split by BARB BBC Region	(attribute)
      split by ABC1 Males in HH	(attribute)
      split by Social Class	(attribute)
(reviewed)

Scenario 10.3
      total number of unique households (actual)	(metrics)
      viewing between 10/12 &12/12 	(filter)
      split by Current Package	(attribute)
      split by Sky Product Set	(attribute)
      split by Previous Sports Downgrade	(attribute)
      split by Value Segments	(attribute)
(reviewed)

Scenario 10.4
      total number of unique households (actual)	(metrics)
      viewing between 04/12 & 08/12 	(filter)
      split by Active Sky Reward User	(attribute)
      split by Missed Payment Last Year	(attribute)
      split by Discount Offer Last 6 Months	(attribute)
(reviewed)

Scenario 10.5
      total number of unique households (actual)	(metrics)
      viewing between 04/12 & 08/12 	(filter)
      split by HH Turnaround Last Year	(attribute)
      split by Previous Movies Downgrade	(attribute)
(reviewed)
*/
  if object_id('uat_scenario10_star') is not null then drop table uat_scenario10_star end if;
  select hsh.cb_key_household
        ,hsh.household_composition
        ,case
            when hsh.property_type = '0' then 'Purpose built flats'
            when hsh.property_type = '1' then 'Converted flats'
            when hsh.property_type = '2' then 'Farm'
            when hsh.property_type = '3' then 'Named building'
            when hsh.property_type = '4' then 'Other type'
              else 'Unclassified'
         end as hh_property_type
        ,hsh.government_region
        ,hsh.tenure_dth
        ,hsh.barb_itv_region
        ,hsh.barb_bbc_region
        ,hsh.abc1_males_in_hh
        ,hsh.social_class
        ,hsh.current_package
        ,hsh.sky_product_set
        ,hsh.prev_sports_downgrade
        ,hsh.value_segment
        ,hsh.HH_Active_Sky_Rewards_User
        ,hsh.HH_Missed_Payment_Last_Year
        ,hsh.Discount_Offer_Last_6M
        ,hsh.HH_Turnaround_Last_Year
        ,hsh.Prev_Movies_Downgrade
    into uat_scenario10_star
    from sk_prod.viq_viewing_data_uat                as bas
         inner join sk_prod.viq_household            as hsh on bas.household_key            = hsh.household_key
         inner join sk_prod.viq_date                 as vws on bas.viewing_start_date_key   = vws.pk_datehour_dim
   where vws.local_day_date between '2013-01-05' and '2013-01-10'
group by cb_key_household
        ,time_period
        ,household_composition
        ,hh_property_type
        ,government_region
        ,tenure_dth
        ,barb_itv_region
        ,barb_bbc_region
        ,abc1_males_in_hh
        ,social_class
        ,current_package
        ,sky_product_set
        ,prev_sports_downgrade
        ,value_segment
        ,HH_Active_Sky_Rewards_User
        ,HH_Missed_Payment_Last_Year
        ,Discount_Offer_Last_6M
        ,HH_Turnaround_Last_Year
        ,Prev_Movies_Downgrade
;
commit;


  select 10.1                                       as Scenario
        ,Household_composition                      as Attribute_1
        ,hh_property_type                           as Attribute_2
        ,government_region                          as Attribute_3
        ,cast(Tenure_DTH as varchar(50))            as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,count(distinct cb_key_household)           as Metric
    from uat_scenario10_star
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

  union all

  select 10.2                                       as Scenario
        ,barb_itv_region                            as Attribute_1
        ,barb_bbc_region                            as Attribute_2
        ,cast(abc1_males_in_hh as varchar(50))      as Attribute_3
        ,Social_Class                               as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,count(distinct cb_key_household)           as Metric
    from uat_scenario10_star
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

  union all

  select 10.3                                       as Scenario
        ,Current_package                            as Attribute_1
        ,sky_product_set                            as Attribute_2
        ,cast(prev_sports_downgrade as varchar(50)) as Attribute_3
        ,Value_Segment                              as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,count(distinct cb_key_household)           as Metric
    from uat_scenario10_star
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

  union all

  select 10.4                                       as Scenario
        ,cast(HH_Active_Sky_Rewards_User as varchar(50)) as Attribute_1
        ,cast(HH_Missed_Payment_Last_Year as varchar(50)) as Attribute_2
        ,cast(Discount_Offer_Last_6M as varchar(50)) as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,count(distinct cb_key_household)           as Metric
    from uat_scenario10_star
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

  union all

  select 10.5                                       as Scenario
        ,cast(HH_Turnaround_Last_Year as varchar(50)) as Attribute_1
        ,cast(Prev_Movies_Downgrade as varchar(50)) as Attribute_2
        ,cast(null as varchar(50))                  as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,count(distinct cb_key_household)           as Metric
    from uat_scenario10_star
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5




  select HH_Active_Sky_Rewards_User
        ,count(distinct cb_key_household)           as Metric
    from uat_scenario10_star
group by HH_Active_Sky_Rewards_User






/*
Scenario 11.1
      total number of unique households (scaled)	(metrics)
      as of 20/12 	(filter)
      split by BARB region	(attribute)
      split by Financial Outlook	(attribute)
(reviewed)
*/
  if object_id('uat_scenario11_1_star') is not null then drop table uat_scenario11_1_star end if;
  select hsh.cb_key_household
        ,hsh.barb_bbc_region
        ,hsh.financial_outlook
        ,scl.calculated_scaling_weight
    into uat_scenario11_1_star
    from sk_prod.viq_viewing_data_scaling_uat as scl
         inner join sk_prod.viq_household                as hsh on scl.household_key            = hsh.household_key
   where scl.adjusted_event_start_date_vespa = '2013-01-08'                                     -- Scenario filter
group by hsh.cb_key_household
        ,hsh.barb_bbc_region
        ,hsh.financial_outlook
        ,scl.calculated_scaling_weight
;
commit;

select
      barb_bbc_region,
      financial_outlook,
      sum(calculated_scaling_weight) as HHs_scaled,
      count(distinct cb_key_household) as HHs
  from uat_scenario11_1_star
 group by barb_bbc_region, financial_outlook;








/*
Scenario 11.2
      total number of unique households (scaled)	(metrics)
      viewing between 10/12 & 16/12 	(filter)
      for programmes broadcasted between 03/12 & 13/12 	(filter)
      watching “Top Gear” 	(filter)
      split by Live, VOSDAL, Playback	(attribute)
(reviewed)
*/
  if object_id('uat_scenario11_2_star') is not null then drop table uat_scenario11_2_star end if;
  select hsh.cb_key_household
        ,shf.viewing_type
        ,scl.calculated_scaling_weight
    into uat_scenario11_2_star
    from sk_prod.viq_viewing_data_uat                    as bas
         inner join sk_prod.viq_date                     as bds on bas.broadcast_start_date_key = bds.pk_datehour_dim
         inner join sk_prod.viq_date                     as vws on bas.viewing_start_date_key   = vws.pk_datehour_dim
         inner join sk_prod.viq_viewing_data_scaling_uat as scl on bas.household_key            = scl.household_key
                                                               and bds.utc_day_date             = scl.adjusted_event_start_date_vespa
         inner join sk_prod.viq_programme_uat            as prg on bas.prog_inst_programme_key  = prg.pk_programme_dim
         inner join sk_prod.viq_time_shift               as shf on bas.time_shift_key           = shf.pk_timeshift_dim
         inner join sk_prod.viq_household                as hsh on bas.household_key            = hsh.household_key
   where bds.local_day_date between '2013-01-02' and '2013-01-05'
     and vws.local_day_date between '2013-01-07' and '2013-01-10'
     and lower(programme_name) = 'top gear'                                                            -- Scenario filter
group by hsh.cb_key_household
        ,shf.viewing_type
        ,scl.calculated_scaling_weight
; --
commit;


  select 11.2                                       as Scenario
        ,viewing_type                               as Attribute_1
        ,cast(null as varchar(50))                  as Attribute_2
        ,cast(null as varchar(50))                  as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,sum(calculated_scaling_weight)             as Metric
    from uat_scenario11_2_star
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5













/* ##### SUMMARY/OUTPUT ##### */
  select 1                                          as Scenario
        ,channel_name                               as Attribute_1
        ,mosaic_segments                            as Attribute_2
        ,vespa_day_long                             as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,seconds                                    as Metric
    from uat_scenario1_star

  union all

  select 2.1                                        as Scenario
        ,grouping_indicator                         as Attribute_1
        ,cast(utc_day_date as varchar(50))          as Attribute_2
        ,household_composition                      as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,count(distinct household_key)              as Metric
    from uat_scenario2_star
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

  union all

  select 2.2                                        as Scenario
        ,grouping_indicator                         as Attribute_1
        ,cast(utc_day_date as varchar(50))          as Attribute_2
        ,household_composition                      as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,sum(weighting)                             as Metric
    from uat_scenario2_star
    where utc_day_date = '2012-12-11'
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

  union all

  select 3                                          as Scenario
        ,affluence_bands                            as Attribute_1
        ,cast(null as varchar(50))                  as Attribute_2
        ,cast(null as varchar(50))                  as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,hours                                      as Metric
    from uat_scenario3_star

  union all

  select 4                                          as Scenario
        ,cast(episode_number as varchar(50))        as Attribute_1
        ,viewing_type                               as Attribute_2
        ,cast(null as varchar(50))                  as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,minutes                                    as Metric
    from uat_scenario4_star

  union all

  select 5                                          as Scenario
        ,channel_genre                              as Attribute_1
        ,cast(hh_has_broadband as varchar(50))      as Attribute_2
        ,cast(tenure_broadband as varchar(50))      as Attribute_3
        ,government_region                          as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,count(household_key)                       as Metric
    from uat_scenario5_star
   -- where repeat_flag = 1
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

  union all

  select 6                                          as Scenario
        ,pay_free_indicator                         as Attribute_1
        ,channel_genre                              as Attribute_2
        ,cast(utc_day_date as varchar(50))          as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,prgm_duration                              as Metric
    from uat_scenario6_star

  union all

  select Scenario   -- 7
        ,Attribute_1
        ,cast(Attribute_2 as varchar(50))           as Attribute_2
        ,Attribute_3
        ,Attribute_4
        ,Attribute_5
        ,Metric
    from uat_scenario7_star_results

  union all

  select 8                                          as Scenario
        ,viewing_type                               as Attribute_1
        ,sensitive_channel                          as Attribute_2
        ,cast(repeat_flag as varchar(50))           as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,1.0 * duration / 3600                      as Metric
    from uat_scenario8_star

  union all

  select 9                                          as Scenario
        ,viewing_type                               as Attribute_1
        ,cast(null as varchar(50))                  as Attribute_2
        ,cast(null as varchar(50))                  as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,events_num                                 as Metric
    from uat_scenario9_star

  union all

  select 10.1                                       as Scenario
        ,Household_composition                      as Attribute_1
        ,hh_property_type                           as Attribute_2
        ,government_region                          as Attribute_3
        ,cast(Tenure_DTH as varchar(50))            as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,count(distinct household_key)              as Metric
    from uat_scenario10_star
   where time_period = 2
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

  union all

  select 10.2                                       as Scenario
        ,barb_itv_region                            as Attribute_1
        ,barb_bbc_region                            as Attribute_2
        ,cast(abc1_males_in_hh as varchar(50))      as Attribute_3
        ,Social_Class                               as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,count(distinct household_key)              as Metric
    from uat_scenario10_star
   where time_period = 2
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

  union all

  select 10.3                                       as Scenario
        ,Current_package                            as Attribute_1
        ,sky_product_set                            as Attribute_2
        ,cast(prev_sports_downgrade as varchar(50)) as Attribute_3
        ,Value_Segment                              as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,count(distinct household_key)              as Metric
    from uat_scenario10_star
   where time_period = 2
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

  union all

  select 10.4                                       as Scenario
        ,cast(HH_Active_Sky_Rewards_User as varchar(50)) as Attribute_1
        ,cast(HH_Missed_Payment_Last_Year as varchar(50)) as Attribute_2
        ,cast(Discount_Offer_Last_6M as varchar(50)) as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,count(distinct household_key)              as Metric
    from uat_scenario10_star
   where time_period = 1
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

  union all

  select 10.5                                       as Scenario
        ,cast(HH_Turnaround_Last_Year as varchar(50)) as Attribute_1
        ,cast(Prev_Movies_Downgrade as varchar(50)) as Attribute_2
        ,cast(null as varchar(50))                  as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,count(distinct household_key)              as Metric
    from uat_scenario10_star
   where time_period = 1
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

  union all

  select 11.1                                       as Scenario
        ,barb_bbc_region                            as Attribute_1
        ,financial_outlook                          as Attribute_2
        ,cast(null as varchar(50))                  as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,sum(calculated_scaling_weight)             as Metric
    from uat_scenario11_1_star
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

  union all


  select 11.2                                       as Scenario
        ,viewing_type                               as Attribute_1
        ,cast(null as varchar(50))                  as Attribute_2
        ,cast(null as varchar(50))                  as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,sum(calculated_scaling_weight)             as Metric
    from uat_scenario11_2_star
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

order by 1, 2, 3, 4, 5, 6;

























