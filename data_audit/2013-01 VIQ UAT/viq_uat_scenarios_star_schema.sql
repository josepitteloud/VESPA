-- Star Schema version
----------------------


/* Scenario 0 (internal reconcilation)
        total seconds viewed & unique HHs (actual)
        viewing between 20/12 & 24/12
        split by Day
        split by viewing type (live/playback etc.)
*/
  create variable @varTX  date;
  set @varTX = '2013-04-30';

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
    from sk_prod.viq_viewing_data                    as bas
         inner join sk_prod.viq_date                 as bds on bas.viewing_start_date_key   = bds.pk_datehour_dim
         inner join sk_prod.viq_time                 as bdt on bas.viewing_start_time_key   = bdt.pk_time_dim
         inner join sk_prod.viq_time                 as bdx on bas.viewing_end_time_key     = bdx.pk_time_dim
         inner join sk_prod.viq_time_shift           as shf on bas.time_shift_key           = shf.pk_timeshift_dim
         inner join sk_prod.viq_household            as hsh on bas.household_key            = hsh.household_key
         inner join sk_prod.viq_programme            as prg on bas.prog_inst_programme_key  = prg.pk_programme_dim
         inner join sk_prod.viq_channel              as chn on bas.prog_inst_channel_key    = chn.pk_channel_dim
   where viewing_start_date_key >= cast(dateformat(@varTX, 'yyyymmdd00') as int)                -- Scenario filter
     and viewing_start_date_key <= cast(dateformat(@varTX, 'yyyymmdd23') as int)                -- Scenario filter
     and cb_key_household is not null                                                           -- Common filter
     and cb_key_household > 0                                                                   -- Common filter
;
commit;


if object_id('uat_scenario0_star_acc') is not null then drop table uat_scenario0_star_acc endif;
select
      account_number,
      case
        when viewing_type = 'Live' then 'LIVE'
          else 'RECORDED'
      end as Live_Recorded,
      sum(viewed_duration) as duration
  into uat_scenario0_star_acc
  from uat_scenario0_star
 where viewing_date = '2013-04-30'
 group by account_number, Live_Recorded;
commit;

create hg index idx1 on uat_scenario0_star_acc(account_Number);
create lf index idx2 on uat_scenario0_star_acc(Live_Recorded);


  select
        case
          when viewing_type = 'Live' then 'LIVE'
            else 'RECORDED'
        end as Live_Recorded
        ,viewing_date
        ,sum(viewed_duration) as sum_viewed_duration
        ,sum(viewed_duration_revised) as sum_viewed_duration_revised
        ,count(distinct cb_key_household) as cnt_cb_key_household
        ,count(distinct account_Number) as cnt_account_number
    from uat_scenario0_star
group by Live_Recorded
        ,viewing_date
order by 2, 1
;


if object_id('uat_scenario0_syb_star_comp') is not null then drop table uat_scenario0_syb_star_comp endif;
select
      coalesce(syb.account_number, star.account_number) as account_number,
      coalesce(syb.Live_Recorded, star.Live_Recorded) as Live_Recorded,
      case when syb.duration is null then -1 else syb.duration end as syb_duration,
      case when star.duration is null then -1 else star.duration end as star_duration,
      case
        when syb.account_number is null then 'Star only'
        when star.account_number is null then 'Syb only'
        when syb.duration <> star.duration then 'Both (different)'
          else 'Both (same)'
      end as Exist
  into uat_scenario0_syb_star_comp
  from uat_scenario0_syb_acc syb full join uat_scenario0_star_acc star
    on syb.account_number = star.account_number
   and syb.Live_Recorded = star.Live_Recorded;
commit;

select Exist, count(*) as cnt from uat_scenario0_syb_star_comp group by Exist;





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
        ,vespa_day_long
        ,sum(viewed_duration) as seconds
    into uat_scenario1_star
    from sk_prod.viq_viewing_data_uat       as bas
         inner join sk_prod.viq_channel     as chn on bas.prog_inst_channel_key  = chn.pk_channel_dim
         inner join sk_prod.viq_household   as hsh on bas.household_key          = hsh.household_key
         inner join sk_prod.viq_time        as tms on bas.viewing_start_time_key = tms.pk_time_dim
         inner join sk_prod.viq_date        as dat on bas.viewing_start_time_key = dat.pk_datehour_dim
   where channel_name in ('Channel 4', 'Channel 4 HD')                                          -- Scenario filter
     and viewing_start_date_key  >= 2012120400                                                  -- Scenario filter
     and viewing_start_date_key  <= 2012121323                                                  -- Scenario filter
     and bas.household_key is not null                                                          -- Common filter
     and bas.household_key > 0                                                                  -- Common filter
group by channel_name
        ,mosaic_segments
        ,vespa_day_long
; -- 0 records
commit;


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
  select bas.household_key
        ,dts.utc_day_date
        ,grouping_indicator
        ,household_composition
        ,calculated_scaling_weight as weighting
    into uat_scenario2_star
    from sk_prod.viq_viewing_data_uat                    as bas
         inner join sk_prod.viq_channel_uat              as chn on bas.prog_inst_channel_key    = chn.pk_channel_dim
         inner join sk_prod.viq_date                     as dts on bas.viewing_start_date_key   = dts.pk_datehour_dim
         inner join sk_prod.viq_programme_uat            as prg on bas.prog_inst_programme_key  = prg.pk_programme_dim
         inner join sk_prod.viq_household                as hsh on bas.household_key            = hsh.household_key
         inner join sk_prod.viq_time_shift               as shf on bas.time_shift_key           = shf.pk_timeshift_dim
         inner join sk_prod.viq_time                     as tms on bas.viewing_start_time_key   = tms.pk_time_dim
         inner join sk_prod.viq_time                     as tme on bas.viewing_end_time_key     = tme.pk_time_dim
         inner join sk_prod.viq_viewing_data_scaling_uat as scl on bas.household_key            = scl.household_key
                                                               and dts.utc_day_date             = scl.adjusted_event_start_date_vespa         -- ??? Check cartesian
   where viewing_type       = 'Vosdal'                                                          -- Scenario filter
     and lower(programme_name) = 'top gear'                                                     -- Scenario filter
     and bas.programme_viewed_flag = 1                                                          -- Scenario filter
     and viewing_start_date_key  >= 2012120400                                                  -- Scenario filter
     and viewing_start_date_key  <= 2012121323                                                  -- Scenario filter
     and bas.household_key is not null                                                          -- Common filter
     and bas.household_key > 0                                                                  -- Common filter
;
commit;


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
  select sum(1.0 * viewed_duration / 3600) as hours
        ,affluence_bands
    into uat_scenario3_star
    from sk_prod.VIQ_VIEWING_DATA_UAT                as bas
         inner join sk_prod.viq_date                 as vds on bas.viewing_start_date_key   = vds.pk_datehour_dim
         inner join sk_prod.viq_time                 as vts on bas.viewing_start_time_key   = vts.pk_time_dim
         inner join sk_prod.viq_date                 as bds on bas.broadcast_start_date_key = bds.pk_datehour_dim
         inner join sk_prod.viq_time                 as bts on bas.broadcast_start_time_key = bts.pk_time_dim
         inner join sk_prod.viq_programme_uat        as prg on bas.prog_inst_programme_key  = prg.pk_programme_dim
         inner join sk_prod.viq_household            as hsh on bas.household_key            = hsh.household_key
   where vds.utc_day_date between '2012-12-20' and '2012-12-27'                                 -- Scenario filter
     and lower(programme_name) = 'eastenders'                                                   -- Scenario filter
     and bds.utc_day_date              = '2012-12-06'                                           -- Scenario filter
     and bts.utc_time_minute           >= '19:30:00'                                            -- Scenario filter
     and bts.utc_time_minute           <= '22:30:00'                                            -- Scenario filter
     and bas.household_key is not null                                                          -- Common filter
     and bas.household_key > 0                                                                  -- Common filter
group by affluence_bands
;
commit;


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
  select sum(viewed_duration / 60) as minutes
        ,episode_number
        ,case when viewing_type in ('Live', 'Vosdal') then viewing_type
              when cast(elapsed_days as smallint) <= 7 then 'Timeshift 1-7 days'
              when cast(elapsed_days as smallint) <= 28 then 'Timeshift 8-28 days'
         end as viewing_type
    into uat_scenario4_star
    from sk_prod.viq_viewing_data_uat                as bas
         inner join sk_prod.viq_programme_uat        as prg on bas.prog_inst_programme_key  = prg.pk_programme_dim
         inner join sk_prod.viq_channel_uat          as chn on bas.prog_inst_channel_key    = chn.pk_channel_dim
         inner join sk_prod.viq_time_shift           as shf on bas.time_shift_key           = shf.pk_timeshift_dim
   where programme_name = '3-2-1'                                                               -- Scenario filter
     and lower(channel_name) like 'challenge'                                                   -- Scenario filter
     and broadcast_start_date_key  >= 2012120400                                                -- Scenario filter
     and broadcast_start_date_key  <= 2012121323                                                -- Scenario filter
     and viewing_start_date_key    <= 2012123123                                                -- Scenario filter
     and bas.household_key is not null                                                          -- Common filter
     and bas.household_key > 0                                                                  -- Common filter
group by episode_number
        ,viewing_type
; --
commit;


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
  select bas.household_key
        ,channel_genre
        ,repeat_flag
        ,hh_has_broadband
        ,tenure_broadband
        ,government_region
    into uat_scenario5_star
    from sk_prod.viq_viewing_data_uat                             as bas
         inner join sk_prod.viq_channel_uat                       as chn on bas.prog_inst_channel_key    = chn.pk_channel_dim
         inner join sk_prod.viq_household                         as hsh on bas.household_key            = hsh.household_key
         inner join sk_prod.viq_prog_sched_properties             as sch on bas.prog_inst_programme_key  = sch.pk_programme_instance_properties
   where viewing_start_date_key    >= 2012121000                                                -- Scenario filter
     and viewing_start_date_key    <= 2012121223                                                -- Scenario filter
     and bas.household_key is not null                                                          -- Common filter
     and bas.household_key > 0                                                                  -- Common filter
group by bas.household_key
        ,channel_genre
        ,repeat_flag
        ,hh_has_broadband
        ,tenure_broadband
        ,government_region
;
commit;


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
    from sk_prod.viq_programme_schedule              as prg
         inner join sk_prod.viq_channel              as chn on prg.dk_channel_id            = chn.pk_channel_dim
         inner join sk_prod.viq_date                 as vws on prg.dk_start_datehour        = vws.pk_datehour_dim
   where dk_start_datehour >= 2012120500
     and dk_start_datehour <= 2013010423
group by pay_free_indicator
        ,channel_genre
        ,utc_day_date
;
commit;


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
        ,genre_description
        ,season_ref
        ,episode_number
        ,calculated_scaling_weight
        ,viewed_duration
        ,viewed_duration * calculated_scaling_weight as viewed_duration_scaled
    into uat_scenario7_star
    from sk_prod.viq_viewing_data_uat                    as bas
         inner join sk_prod.viq_date                     as bds on bas.viewing_start_date_key   = bds.pk_datehour_dim
         inner join sk_prod.viq_viewing_data_scaling_uat as scl on bas.household_key            = scl.household_key
                                                               and bds.utc_day_date             = scl.adjusted_event_start_date_vespa
         inner join sk_prod.viq_programme_uat            as prg on bas.prog_inst_programme_key  = prg.pk_programme_dim
         inner join sk_prod.viq_time_shift               as shf on bas.time_shift_key           = shf.pk_timeshift_dim
   where viewing_type = 'Live'                                                                  -- Scenario filter
     and broadcast_start_date_key  >= 2012122700                                                -- Scenario filter
     and broadcast_start_date_key  <= 2012123123                                                -- Scenario filter
     and viewing_start_date_key    <= 2012123123                                                -- Scenario filter
     and bas.household_key is not null                                                          -- Common filter
     and bas.household_key > 0                                                                  -- Common filter

;
commit;


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
  select sum(viewed_duration) as duration
        ,viewing_type
        ,sensitive_channel
        ,repeat_flag
    into uat_scenario8_star
    from sk_prod.viq_viewing_data_uat                    as bas
         inner join sk_prod.viq_channel_uat              as chn on bas.prog_inst_channel_key    = chn.pk_channel_dim
         inner join sk_prod.viq_time_shift               as shf on bas.time_shift_key           = shf.pk_timeshift_dim
         inner join sk_prod.viq_prog_sched_properties    as sch on bas.prog_inst_programme_key  = sch.pk_programme_instance_properties
   where viewing_start_date_key    >= 2012121700                                                -- Scenario filter
     and viewing_start_date_key    <= 2012123023                                                -- Scenario filter
     and broadcast_start_date_key  >= 2012121700                                                -- Scenario filter
     and broadcast_start_date_key  <= 2012123023                                                -- Scenario filter
     and bas.household_key is not null                                                          -- Common filter
     and bas.household_key > 0                                                                  -- Common filter
group by viewing_type
        ,sensitive_channel
        ,repeat_flag
;
commit;


/*
Scenario 9
      total number of 3min+ events viewed	(metrics)
      watched between 04/12 & 06/12 	(filter)
      split by Live, VOSDAL, Playback	(attribute)
(reviewed)
*/
  if object_id('uat_scenario9_star') is not null then drop table uat_scenario9_star end if;
  select count(*) as events_num
        ,viewing_type
    into uat_scenario9_star
    from sk_prod.viq_viewing_data_uat                as bas
         inner join sk_prod.viq_time_shift           as shf on bas.time_shift_key           = shf.pk_timeshift_dim
   where event_viewed_flag = 1                                                                  -- Scenario filter
     and viewing_start_date_key    >= 2012120400                                                -- Scenario filter
     and viewing_start_date_key    <= 2012120623                                                -- Scenario filter
     and bas.household_key is not null                                                          -- Common filter
     and bas.household_key > 0                                                                  -- Common filter
group by viewing_type
;
commit;


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
  select bas.household_key
        ,case
            when viewing_start_date_key between 2012120400 and 2012120823 then 1
            when viewing_start_date_key between 2012121000 and 2012121223 then 2
              else 0
         end                                       as time_period
        ,household_composition
        ,case
            when property_type = '0' then 'Purpose built flats'
            when property_type = '1' then 'Converted flats'
            when property_type = '2' then 'Farm'
            when property_type = '3' then 'Named building'
            when property_type = '4' then 'Other type'
              else 'Unclassified'
         end as hh_property_type
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
    into uat_scenario10_star
    from sk_prod.viq_viewing_data_uat                as bas
         inner join sk_prod.viq_household            as hsh on bas.household_key            = hsh.household_key
   where (
            (
             viewing_start_date_key    >= 2012120400                                            -- Scenario filter
             and
             viewing_start_date_key    <= 2012120823                                            -- Scenario filter
            )
            or
            (
             viewing_start_date_key    >= 2012121000                                            -- Scenario filter
             and
             viewing_start_date_key    <= 2012121223                                            -- Scenario filter
            )
         )
     and bas.household_key is not null                                                          -- Common filter
     and bas.household_key > 0                                                                  -- Common filter
group by bas.household_key
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


/*
Scenario 11.1
      total number of unique households (scaled)	(metrics)
      as of 20/12 	(filter)
      split by BARB region	(attribute)
      split by Financial Outlook	(attribute)
(reviewed)
*/
  if object_id('uat_scenario11_1_star') is not null then drop table uat_scenario11_1_star end if;
  select scl.household_key
        ,calculated_scaling_weight
        ,barb_bbc_region
        ,financial_outlook
    into uat_scenario11_1_star
    from sk_prod.viq_viewing_data_scaling_uat as scl
         inner join sk_prod.viq_household                as hsh on scl.household_key            = hsh.household_key
   where scl.adjusted_event_start_date_vespa = '2012-12-20'                                     -- Scenario filter
     and scl.household_key is not null                                                          -- Common filter
     and scl.household_key > 0                                                                  -- Common filter
group by scl.household_key
        ,calculated_scaling_weight
        ,barb_bbc_region
        ,financial_outlook
;
commit;


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
  select bas.household_key
        ,calculated_scaling_weight
        ,viewing_type
    into uat_scenario11_2_star
    from sk_prod.viq_viewing_data_uat                    as bas
         inner join sk_prod.viq_date                     as bds on bas.viewing_start_date_key   = bds.pk_datehour_dim
         inner join sk_prod.viq_viewing_data_scaling_uat as scl on bas.household_key            = scl.household_key
                                                               and bds.utc_day_date             = scl.adjusted_event_start_date_vespa
         inner join sk_prod.viq_programme_uat            as prg on bas.prog_inst_programme_key  = prg.pk_programme_dim
         inner join sk_prod.viq_time_shift               as shf on bas.time_shift_key           = shf.pk_timeshift_dim
   where viewing_start_date_key    >= 2012121000                                                -- Scenario filter
     and viewing_start_date_key    <= 2012121623                                                -- Scenario filter
     and broadcast_start_date_key  >= 2012120300                                                -- Scenario filter
     and broadcast_start_date_key  <= 2012121323                                                -- Scenario filter
     and programme_name = 'Top Gear'                                                            -- Scenario filter
     and bas.household_key is not null                                                          -- Common filter
     and bas.household_key > 0                                                                  -- Common filter
group by bas.household_key
        ,calculated_scaling_weight
        ,viewing_type
; --
commit;




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

























