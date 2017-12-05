-- Vespa Events All Version
---------------------------
/*
Potential discrepancy source:
  - aggregations at HH or ACC level?
  - viewing/broadcast period - instance/viewing truncated if goes beyond the period or only "start" date counts?

*/




/*
Scenario 1
        total seconds viewed (metrics)
        watching Channel 4 (filter)
        split by Channel 4 SD & HD (attribute)
        split by Day (viewing between 04/12 & 13/12 )	(attribute)
        split by Mosaic Segments (attribute)
(reviewed)
*/
  if object_id('uat_scenario1_syb') is not null then drop table uat_scenario1_syb endif;
  select cb_key_household
        ,channel_name                                                                  as channel_name
        ,cast('Unclassified' as varchar(30))                                           as mosaic_segment
        ,case
           when datepart(weekday, instance_start_date_time_utc) = 1 then 'Sunday'
           when datepart(weekday, instance_start_date_time_utc) = 2 then 'Monday'
           when datepart(weekday, instance_start_date_time_utc) = 3 then 'Tuesday'
           when datepart(weekday, instance_start_date_time_utc) = 4 then 'Wednesday'
           when datepart(weekday, instance_start_date_time_utc) = 5 then 'Thursday'
           when datepart(weekday, instance_start_date_time_utc) = 6 then 'Friday'
           when datepart(weekday, instance_start_date_time_utc) = 7 then 'Saturday'
             else '??'
         end as viewing_day
        ,sum(datediff(second,instance_start_date_time_utc,capping_end_date_time_utc))  as seconds
    into uat_scenario1_syb
    from sk_prod.vespa_events_all
   where channel_name in ('Channel 4', 'Channel 4 HD')                                          -- Scenario filter
     and instance_start_date_time_utc >= '2012-12-04 00:00:00'                                  -- Scenario filter
     and instance_start_date_time_utc <= '2012-12-13 23:59:59'                                  -- Scenario filter
     and cb_key_household is not null                                                           -- Common filter
     and cb_key_household > 0                                                                   -- Common filter
     and capped_full_flag = 0                                                                   -- Common filter
     and panel_id = 12                                                                          -- Common filter
group by cb_key_household
        ,channel_name
        ,mosaic_segment
        ,viewing_day
;
commit;

create hg index idx1 on uat_scenario1_syb(cb_key_household);


update uat_scenario1_syb as bas
   set mosaic_segment = case exp.h_mosaic_uk_group when     'A' then      'Alpha Territory'
                                                   when     'B' then      'Professional Rewards'
                                                   when     'C' then      'Rural Solitude'
                                                   when     'D' then      'Small Town Diversity'
                                                   when     'E' then      'Active Retirement'
                                                   when     'F' then      'Suburban Mindsets'
                                                   when     'G' then      'Careers and Kids'
                                                   when     'H' then      'New Homemakers'
                                                   when     'I' then      'Ex-Council Community'
                                                   when     'J' then      'Claimant Cultures'
                                                   when     'K' then      'Upper Floor Living'
                                                   when     'L' then      'Elderly Needs'
                                                   when     'M' then      'Industrial Heritage'
                                                   when     'N' then      'Terraced Melting Pot'
                                                   when     'O' then      'Liberal Opinions'
                                                   when     'U' then      'Unclassified' end
  from sk_prod.experian_consumerview  as exp
 where bas.cb_key_household = exp.cb_key_household
;
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
  if object_id('uat_scenario2_syb') is not null then drop table uat_scenario2_syb endif;
  select cb_key_household
        ,grouping_indicator
        ,date(instance_start_date_time_utc)            as viewing_date
        ,cast('Unclassified' as varchar(50))           as household_composition
        ,cast(0 as float) as weighting
    into uat_scenario2_syb
    from sk_prod.vespa_events_all
   where live_recorded = 'RECORDED'                                                             -- Scenario filter
     and date(instance_start_date_time_utc) = date(broadcast_start_date_time_utc)               -- Scenario filter
     and lower(programme_name) = 'top gear'                                                     -- Scenario filter
     and datediff(second,instance_start_date_time_utc,capping_end_date_time_utc) > 180          -- Scenario filter - ??? assumed continuous - what's the DW/definition?
     and instance_start_date_time_utc >= '2012-12-04 00:00:00'                                  -- Scenario filter
     and instance_start_date_time_utc <= '2012-12-13 23:59:59'                                  -- Scenario filter
     and cb_key_household is not null                                                           -- Common filter
     and cb_key_household > 0                                                                   -- Common filter
     and capped_full_flag = 0                                                                   -- Common filter
     and panel_id = 12                                                                          -- Common filter
group by cb_key_household
        ,grouping_indicator
        ,viewing_date
        ,household_composition
        ,weighting
;
commit;

create hg index idx1 on uat_scenario2_syb(cb_key_household);


  update uat_scenario2_syb as bas
     set household_composition = case exp.h_household_composition when  '00' then     'Families'
                                                                      when  '01' then     'Extended family'
                                                                       when '02' then     'Extended household'
                                                                      when  '03' then     'Pseudo family'
                                                                      when  '04' then     'Single male'
                                                                      when  '05' then     'Single female'
                                                                       when '06' then     'Male homesharers'
                                                                       when '07' then     'Female homesharers'
                                                                       when '08' then     'Mixed homesharers'
                                                                       when '09' then     'Abbreviated male families'
                                                                       when '10' then     'Abbreviated female families'
                                                                       when '11' then     'Multi-occupancy dwelling'
                                                                       when 'U'  then     'Unclassified' end
    from sk_prod.experian_consumerview  as exp
   where bas.cb_key_household = exp.cb_key_household
;
commit;


  update uat_scenario2_syb as bas
     set bas.weighting = inl.calculated_scaling_weight
    from sk_prod.viq_viewing_data_scaling as inl
   where bas.cb_key_household = inl.cb_key_household
     and bas.viewing_date = inl.adjusted_event_start_date_vespa
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
  if object_id('uat_scenario3_syb') is not null then drop table uat_scenario3_syb endif;
  select cb_key_household
        ,cast('Unclassified' as varchar(50))                                              as affluence_band
        ,sum(datediff(hour,instance_start_date_time_utc,capping_end_date_time_utc)) as hours
    into uat_scenario3_syb
    from sk_prod.vespa_events_all
   where instance_start_date_time_utc >= '2012-12-20 00:00:00'                                  -- Scenario filter - 14 days after TX
     and instance_start_date_time_utc <= '2012-12-27 23:59:59'                                  -- Scenario filter - 21 after TX
     and lower(programme_name) = 'eastenders'                                                   -- Scenario filter
     and broadcast_start_date_time_utc >= '2012-12-06 19:00'                                    -- Scenario filter
     and broadcast_start_date_time_utc <= '2012-12-06 22:30'                                    -- Scenario filter
     and cb_key_household is not null                                                           -- Common filter
     and cb_key_household > 0                                                                   -- Common filter
     and capped_full_flag = 0                                                                   -- Common filter
     and panel_id = 12                                                                          -- Common filter
group by cb_key_household
        ,affluence_band
;
commit;

create hg index idx1 on uat_scenario3_syb(cb_key_household);


  update uat_scenario3_syb as bas
     set affluence_band = exp.h_affluence_v2
    from sk_prod.experian_consumerview  as exp
   where bas.cb_key_household = exp.cb_key_household
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
  if object_id('uat_scenario4_syb') is not null then drop table uat_scenario4_syb endif;
  select episode_number
        ,case when live_recorded = 'LIVE'                                                      then 'Live'
              when date(event_start_date_time_utc) - date(broadcast_start_date_time_utc)  =  0 then 'Vosdal'
              when date(event_start_date_time_utc) - date(broadcast_start_date_time_utc) <=  7 then 'Timeshift 1-7 days'
              when date(event_start_date_time_utc) - date(broadcast_start_date_time_utc) <= 28 then 'Timeshift 8-28 days'
              else                                                                                  'Other' end as live_recorded
        ,sum(datediff(minute,instance_start_date_time_utc,capping_end_date_time_utc)) as minutes
    into uat_scenario4_syb
    from sk_prod.vespa_events_all
   where programme_name = '3-2-1'
     and lower(channel_name) like 'challenge'                                                   -- Scenario filter
     and broadcast_start_date_time_utc  >= '2012-12-04 00:00:00'                                -- Scenario filter
     and broadcast_start_date_time_utc  <= '2012-12-13 23:59:59'                                -- Scenario filter
     and instance_start_date_time_utc   <= '2012-12-31 23:59:59'                                -- Scenario filter
     and cb_key_household is not null                                                           -- Common filter
     and cb_key_household > 0                                                                   -- Common filter
     and capped_full_flag = 0                                                                   -- Common filter
     and panel_id = 12                                                                          -- Common filter
group by episode_number
        ,live_recorded
;
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
  if object_id('uat_scenario5_syb') is not null then drop table uat_scenario5_syb endif;
  select cb_key_household
        ,genre_description
        ,repeat_flag
        ,cast(0 as bit) as bb
        ,cast(0 as int) as bb_tenure
        ,cast(null as varchar(15)) as cb_address_postcode
        ,cast(null as varchar(30)) as govt_region
    into uat_scenario5_syb
    from sk_prod.vespa_events_all
   where instance_start_date_time_utc <= '2012-12-10 00:00:00'                                  -- Scenario filter
     and instance_start_date_time_utc >= '2012-12-12 23:59:59'                                  -- Scenario filter
     and cb_key_household is not null                                                           -- Common filter
     and cb_key_household > 0                                                                   -- Common filter
     and capped_full_flag = 0                                                                   -- Common filter
     and panel_id = 12                                                                          -- Common filter
group by cb_key_household
        ,genre_description
        ,repeat_flag
        ,bb
        ,bb_tenure
        ,cb_address_postcode
        ,govt_region
;
commit;

create hg index idx1 on uat_scenario5_syb(cb_key_household);
create lf index idx2 on uat_scenario5_syb(cb_address_postcode);


  update uat_scenario5_syb as bas
     set bb = 1,
         bb_tenure = datediff(month, first_activation_dt, now())
    from sk_prod.cust_subs_hist as csh
   where bas.cb_key_household = csh.cb_key_household
     and csh.subscription_sub_type ='Broadband DSL Line'
     AND (       status_code in ('AC','AB')
      OR (status_code='PC' AND prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
      OR (status_code='CF' AND prev_status_code='PC'                                  )
      OR (status_code='AP' AND sale_type='SNS Bulk Migration'                         ))
     and effective_to_dt = '9999-09-09'
;
commit;

  update uat_scenario5_syb as bas
     set bas.cb_address_postcode = sav.cb_address_postcode
    from sk_prod.cust_single_account_view as sav
   where bas.cb_key_household = sav.cb_key_household
;
commit;

  update uat_scenario5_syb as bas
     set bas.govt_region = bpe.GOVERNMENT_REGION
    from sk_prod.BROADBAND_POSTCODE_EXCHANGE as bpe
   where bas.cb_address_postcode = bpe.cb_address_postcode
;
commit;
-- ??? 0 results

/*
Scenario 6
        total Programme Scheduled duration      (metrics)
        broadcasted between 01/09 & 30/09       (filter)
        split by Channel Type   (attribute)
        split by Channel Genre  (attribute)
        split by Day    (attribute)
(reviewed)
*/

  if object_id('uat_scenario6_syb') is not null then drop table uat_scenario6_syb end if;
  select pay_free_indicator
        ,genre_description
        ,case
           when datepart(weekday, broadcast_start_date_time_utc) = 1 then 'Sunday'
           when datepart(weekday, broadcast_start_date_time_utc) = 2 then 'Monday'
           when datepart(weekday, broadcast_start_date_time_utc) = 3 then 'Tuesday'
           when datepart(weekday, broadcast_start_date_time_utc) = 4 then 'Wednesday'
           when datepart(weekday, broadcast_start_date_time_utc) = 5 then 'Thursday'
           when datepart(weekday, broadcast_start_date_time_utc) = 6 then 'Friday'
           when datepart(weekday, broadcast_start_date_time_utc) = 7 then 'Saturday'
             else '??'
         end as broadcast_day
        ,sum(datediff(second,broadcast_start_date_time_utc,broadcast_end_date_time_utc)) as duration
    into uat_scenario6_syb
    from sk_prod.vespa_programme_schedule
   where broadcast_start_date_time_utc >= '2012-12-05 00:00:00'
     and broadcast_start_date_time_utc <= '2013-01-04 23:59:59'
group by pay_free_indicator
        ,genre_description
        ,broadcast_day
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
  create table uat_scenario7_syb (
    cb_key_household            bigint      default null,
    genre_description           varchar(20) default null,
    season_ref                  varchar(10) default null,
    episodes_watched            int         default null,
    weighting                   float       default null,
    duration                    int         default null,
    duration_scaled             float       default null,
    broadcast_start_date_utc    date        default null
  );

create hg index idx1 on uat_scenario7_syb(cb_key_household);
create date index idx2 on uat_scenario7_syb(broadcast_start_date_utc);


  if object_id('uat_scenario7_syb') is not null then drop table uat_scenario7_syb end if;
  select cb_key_household
        ,genre_description
        ,season_ref
        ,count(distinct episode_number) episodes_watched
        ,cast(0 as float) as weighting
        ,sum(datediff(second, instance_start_date_time_utc, capping_end_date_time_utc)) as duration
        ,cast(0 as float) as duration_scaled
        ,date(broadcast_start_date_time_utc) as broadcast_start_date_utc
    into uat_scenario7_syb
    from sk_prod.vespa_events_all
   where live_recorded = 'LIVE'                                                                 -- Scenario filter
     and broadcast_start_date_time_utc  >= '2012-12-27 00:00:00'                                -- Scenario filter
     and broadcast_start_date_time_utc  <= '2012-12-31 23:59:59'                                -- Scenario filter
     and instance_start_date_time_utc   <= '2012-12-31 23:59:59'                                -- Scenario filter
     and cb_key_household is not null                                                           -- Common filter
     and cb_key_household > 0                                                                   -- Common filter
     and capped_full_flag = 0                                                                   -- Common filter
     and panel_id = 12                                                                          -- Common filter
group by cb_key_household
        ,genre_description
        ,season_ref
        ,weighting
        ,broadcast_start_date_utc
;
commit;


  update uat_scenario7_syb as bas
     set bas.weighting        = case
                                  when inl.calculated_scaling_weight is not null then inl.calculated_scaling_weight
                                    else 0
                                end,
         bas.duration_scaled  = case
                                  when inl.calculated_scaling_weight is not null then inl.calculated_scaling_weight * bas.duration
                                    else 0
                                end
    from sk_prod.viq_viewing_data_scaling as inl
   where bas.cb_key_household = inl.cb_key_household
     and bas.broadcast_start_date_utc = inl.adjusted_event_start_date_vespa
;
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
  if object_id('uat_scenario8_syb') is not null then drop table uat_scenario8_syb end if;
  select case when live_recorded = 'LIVE'                                                      then 'Live'
              when date(event_start_date_time_utc) - date(broadcast_start_date_time_utc)  =  0 then 'Vosdal'
              else                                                                                  'Playback' end as live_recorded
        ,sensitive_channel
        ,repeat_flag
        ,sum(datediff(hour, instance_start_date_time_utc, capping_end_date_time_utc)) as duration
    into uat_scenario8_syb
    from sk_prod.vespa_events_all
   where instance_start_date_time_utc  >= '2012-12-17 00:00:00'                                 -- Scenario filter
     and instance_start_date_time_utc  <= '2012-12-30 23:59:59'                                 -- Scenario filter
     and broadcast_start_date_time_utc >= '2012-12-17 00:00:00'                                 -- Scenario filter
     and broadcast_start_date_time_utc <= '2012-12-30 23:59:59'                                 -- Scenario filter
     and cb_key_household is not null                                                           -- Common filter
     and cb_key_household > 0                                                                   -- Common filter
     and capped_full_flag = 0                                                                   -- Common filter
     and panel_id = 12                                                                          -- Common filter
group by live_recorded
        ,sensitive_channel
        ,repeat_flag
;
commit;


/*
Scenario 9
      total number of 3min+ events viewed	(metrics)
      watched between 04/12 & 06/12 	(filter)
      split by Live, VOSDAL, Playback	(attribute)
*/
  if object_id('uat_scenario9_syb') is not null then drop table uat_scenario9_syb end if;
  select count(1) as events_num
        ,case when live_recorded = 'LIVE'                                                then 'Live'
              when date(event_start_date_time_utc) = date(broadcast_start_date_time_utc) then 'Vosdal'
              else                                                                            'Playback' end as live_recorded
    into uat_scenario9_syb
    from sk_prod.vespa_events_all
   where instance_start_date_time_utc  >= '2012-12-04 00:00:00'                                 -- Scenario filter
     and instance_start_date_time_utc  <= '2012-12-06 23:59:59'                                 -- Scenario filter
     and datediff(second, instance_start_date_time_utc, capping_end_date_time_utc) >= 180       -- Scenario filter - ??? should be event start/end difference or instance? DW flag definition?
     and cb_key_household is not null                                                           -- Common filter
     and cb_key_household > 0                                                                   -- Common filter
     and capped_full_flag = 0                                                                   -- Common filter
     and panel_id = 12                                                                          -- Common filter
group by live_recorded
; --
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
  if object_id('uat_scenario10_syb') is not null then drop table uat_scenario10_syb end if;
  create table bednaszs.uat_scenario10_syb (
    cb_key_household                bigint      default null,
    account_number                  varchar(20) default null,
    time_period                     smallint    default null,
    cb_address_postcode             varchar(20) default null,
    household_composition           varchar(50) default null,
    property_type                   varchar(50) default null,
    govt_region                     varchar(50) default null,
    tenure_dth                      varchar(50) default null,
    itv_region                      varchar(50) default null,
    bbc_region                      varchar(50) default null,
    men_in_hh                       varchar(50) default null,
    abc1_males_in_household         varchar(50) default null,
    social_class                    varchar(50) default null,
    current_package                 varchar(50) default null,
    sky_product                     varchar(50) default null,
    previous_sports_downgrade       bit         default null,
    value_segment                   varchar(50) default null,
    active_sky_reward_user          bit         default null,
    missed_payment_last_year        bit         default null,
    discount_offer_last_6_months    bit         default null,
    hh_turnaround_last_year         bit         default null,
    previous_movies_downgrade       bit         default null,
    ABC1_MALES_IN_HH                varchar(50) default null
  );

  create hg index idx1 on uat_scenario10_syb(cb_key_household);
  create hg index idx2 on uat_scenario10_syb(account_number);
  create hg index idx3 on uat_scenario10_syb(cb_address_postcode);
  create lf index idx4 on uat_scenario10_syb(time_period);


  insert into uat_scenario10_syb
  select cb_key_household
        ,account_number
        ,case
            when instance_start_date_time_utc between '2012-12-04 00:00:00' and '2012-12-08 23:59:59' then 1
            when instance_start_date_time_utc between '2012-12-10 00:00:00' and '2012-12-12 23:59:59' then 2
              else 0
         end                                       as time_period
        ,null                                      as cb_address_postcode
        ,'Unclassified'                            as household_composition
        ,'Unclassified'                            as property_type
        ,'Unclassified'                            as govt_region
        ,'Unclassified'                            as tenure_dth
        ,null                                      as itv_region
        ,null                                      as bbc_region
        ,null                                      as men_in_hh --required for abc1 males in hh
        ,null                                      as abc1_males_in_household
        ,null                                      as social_class
        ,null                                      as current_package
        ,null                                      as sky_product
        ,0                                         as previous_sports_downgrade
        ,null                                      as value_segment
        ,0                                         as Active_Sky_Reward_User
        ,0                                         as Missed_Payment_Last_Year
        ,0                                         as Discount_Offer_Last_6_Months
        ,0                                         as HH_Turnaround_Last_Year
        ,0                                         as Previous_Movies_Downgrade
        ,null                                      as ABC1_MALES_IN_HH
    from sk_prod.vespa_events_all
   where (
            (
             instance_start_date_time_utc  >= '2012-12-04 00:00:00'                             -- Scenario filter
             and
             instance_start_date_time_utc  <= '2012-12-08 23:59:59'                             -- Scenario filter
            )
            or
            (
             instance_start_date_time_utc  >= '2012-12-10 00:00:00'                             -- Scenario filter
             and
             instance_start_date_time_utc  <= '2012-12-12 23:59:59'                             -- Scenario filter
            )
         )
     and cb_key_household is not null                                                           -- Common filter
     and cb_key_household > 0                                                                   -- Common filter
     and capped_full_flag = 0                                                                   -- Common filter
     and panel_id = 12                                                                          -- Common filter
group by cb_key_household
        ,account_number
        ,time_period
;
commit;

  update uat_scenario10_syb as bas
     set household_composition = case exp.h_household_composition when '00' then     'Families'
                                                                  when '01' then     'Extended family'
                                                                  when '02' then     'Extended household'
                                                                  when '03' then     'Pseudo family'
                                                                  when '04' then     'Single male'
                                                                  when '05' then     'Single female'
                                                                  when '06' then     'Male homesharers'
                                                                  when '07' then     'Female homesharers'
                                                                  when '08' then     'Mixed homesharers'
                                                                  when '09' then     'Abbreviated male families'
                                                                  when '10' then     'Abbreviated female families'
                                                                  when '11' then     'Multi-occupancy dwelling'
                                                                  when 'U'  then     'Unclassified' end
        ,property_type = case h_property_type                     when  0   then     'Purpose built flats'
                                                                  when  1   then     'Converted flats'
                                                                  when  2   then     'Farm'
                                                                  when  3   then     'Named building'
                                                                  when  4   then     'Other type' end
    from sk_prod.experian_consumerview  as exp
   where bas.cb_key_household = exp.cb_key_household
;
commit;

  update uat_scenario10_syb as bas
     set bas.cb_address_postcode = sav.cb_address_postcode
    from sk_prod.cust_single_account_view as sav
   where bas.cb_key_household = sav.cb_key_household
;
commit;

  update uat_scenario10_syb as bas
     set bas.govt_region = bpe.GOVERNMENT_REGION
    from sk_prod.BROADBAND_POSTCODE_EXCHANGE as bpe
   where bas.cb_address_postcode = bpe.cb_address_postcode
;
commit;

  update uat_scenario10_syb as bas
     set itv_region = barb_desc_itv
        ,bbc_region = barb_desc_bbc
    from sk_prod.barb_tv_regions as btr
   where btr.cb_address_postcode = bas.cb_address_postcode
;
commit;

  update uat_scenario10_syb as bas
     set tenure_dth = cast(datediff(month, acct_first_account_activation_dt, now()) as varchar(5))
    from sk_prod.cust_single_account_view as sav
   where bas.account_number = sav.account_number
;
commit;

  update uat_scenario10_syb as bas
     set current_package = case when prem_sports = 2 and prem_movies = 2 then 'Top Tier'
                                when prem_sports = 2 and prem_movies = 0 then 'Dual Sports'
                                when prem_sports = 2 and prem_movies = 2 then 'Dual Movies'
                                when prem_sports = 1 and prem_movies = 0 then 'Single Sports'
                                when prem_sports = 0 and prem_movies = 1 then 'Single Movies'
                                when prem_sports = 0 and prem_movies = 0 and (mixes = 0 or
                                                                             (mixes = 1 and (style_culture=1 or  variety=1)) or
                                                                             (mixes = 2 and  style_culture=1 and variety=1))
                                                                         then 'Entertainment'
                                when prem_sports = 0 and prem_movies = 0 then 'Entertainment Extra'
                                                                         else 'Other Premiums' end

    from sk_prod.cust_subs_hist as csh
         inner join sk_prod.cust_entitlement_lookup as cel on csh.current_short_description = cel.short_description
   where bas.account_number = csh.account_number
     and effective_from_dt <= now()
     and effective_to_dt > now()
     and status_code in ('AC', 'PC', 'AB')
; --
commit;


  select csh.account_number
        ,MAX(CASE WHEN csh.subscription_sub_type ='Broadband DSL Line'
                   AND (       status_code in ('AC','AB')
                           OR (status_code='PC' AND prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
                           OR (status_code='CF' AND prev_status_code='PC'                                  )
                           OR (status_code='AP' AND sale_type='SNS Bulk Migration'                         )
                        )                                    THEN 1 ELSE 0 END)  AS bb
        ,MAX(CASE WHEN csh.subscription_sub_type = 'SKY TALK SELECT'
                   AND (     csh.status_code = 'A'
                         OR (csh.status_code = 'FBP' AND prev_status_code in ('PC','A'))
                         OR (csh.status_code = 'RI'  AND prev_status_code in ('FBP','A'))
                         OR (csh.status_code = 'PC'  AND prev_status_code = 'A')
                        )                                  THEN 1 ELSE 0 END)   AS talk
        ,MAX(CASE  WHEN csh.subscription_sub_type ='SKY TALK LINE RENTAL'
                   AND csh.status_code in ('A','CRQ','R')  THEN 1 ELSE 0 END) AS wlr
    into #products
    from sk_prod.cust_subs_hist as csh
         inner join uat_scenario10_syb as bas on csh.account_number = bas.account_number
     and effective_from_dt <= now()
     and effective_to_dt > now()
group by csh.account_number
;
commit;

  update uat_scenario10_syb as bas
     set sky_product =
            CASE WHEN (bb = 0) AND (talk = 0) AND (wlr = 0) THEN 'TV Only'
                 WHEN (bb = 1) AND (talk = 0) AND (wlr = 0) THEN 'TV and Broadband'
                 WHEN (bb = 0) AND (talk = 1) AND (wlr = 0) THEN 'TV and SkyTalk'
                 WHEN (bb = 1) AND (talk = 1) AND (wlr = 0) THEN 'TV, SkyTalk and Broadband'
                 WHEN (bb = 1) AND (talk = 0) AND (wlr = 1) THEN 'TV, Broadband and Line Rental'
                 WHEN (bb = 0) AND (talk = 1) AND (wlr = 1) THEN 'TV, SkyTalk and Line Rental'
                 WHEN (bb = 1) AND (talk = 1) AND (wlr = 1) THEN 'TV, SkyTalk and Line Rental and Broadband'
                 ELSE '??'
            END
    from #products as prd
   where bas.account_number = prd.account_number
;
commit;

  select bas.account_number
    into #sports_downgrade
    from uat_scenario10_syb as bas
         inner join sk_prod.cust_subs_hist as nsh on bas.account_number = nsh.account_number
                                                 and nsh.subscription_sub_type = 'DTV Primary Viewing'
                                                 and nsh.status_code in ('AC','PC','BC')
         inner join sk_prod.cust_entitlement_lookup as nel on nsh.current_short_description = nel.short_description
         inner join sk_prod.cust_subs_hist as osh on bas.account_number = osh.account_number
                                                 and nsh.subscription_sub_type = 'DTV Primary Viewing'
                                                 and osh.status_code in ('AC','PC','BC')
         inner join sk_prod.cust_entitlement_lookup as oel on osh.current_short_description = oel.short_description
   where nsh.effective_from_dt > osh.effective_from_dt
     and nel.prem_sports < oel.prem_sports
group by bas.account_number
;
commit;

  update uat_scenario10_syb as bas
     set previous_sports_downgrade = 1
    from #sports_downgrade as spt
   where bas.account_number = spt.account_number
;
commit;

  select bas.account_number
    into #movies_downgrade
    from uat_scenario10_syb as bas
         inner join sk_prod.cust_subs_hist as nsh on bas.account_number = nsh.account_number
                                                 and nsh.subscription_sub_type = 'DTV Primary Viewing'
                                                 and nsh.status_code in ('AC','PC','BC')
         inner join sk_prod.cust_entitlement_lookup as nel on nsh.current_short_description = nel.short_description
         inner join sk_prod.cust_subs_hist as osh on bas.account_number = osh.account_number
                                                 and nsh.subscription_sub_type = 'DTV Primary Viewing'
                                                 and osh.status_code in ('AC','PC','BC')
         inner join sk_prod.cust_entitlement_lookup as oel on osh.current_short_description = oel.short_description
   where nsh.effective_from_dt > osh.effective_from_dt
     and nel.prem_movies < oel.prem_movies
group by bas.account_number
;
commit;

  update uat_scenario10_syb as bas
     set previous_movies_downgrade = 1
    from #movies_downgrade as mov
   where bas.account_number = mov.account_number
;
commit;

  update uat_scenario10_syb as bas
     set value_segment = vsd.value_seg
   from sk_prod.VALUE_SEGMENTS_DATA  as vsd
   where bas.account_number = vsd.account_number
;
commit;

  update uat_scenario10_syb as bas
     set Active_Sky_Reward_User = 1
    from sk_prod.sky_rewards_competitions as src
  where bas.account_number = src.account_number
;
commit;

  update uat_scenario10_syb as bas
     set missed_payment_last_year = 1
    from sk_prod.cust_bills as bil
  where bas.account_number = bil.account_number
    and status = 'Unbilled'
    and payment_due_dt between dateadd(month, -12, now()) and now()
;
commit;

  update uat_scenario10_syb as bas
     set Discount_Offer_Last_6_Months = 1
    from sk_prod.cust_product_offers as cpo
   where bas.account_number = cpo.account_number
     and offer_end_dt > dateadd(month, -6, now())
;
commit;

  update uat_scenario10_syb as bas
     set HH_Turnaround_Last_Year = 1
    from sk_prod.cust_change_attempt as cca
   where bas.account_number = cca.account_number
     and turnaround_flag in ('S', 'F')
     and attempt_date >= dateadd(month, -12, now())
;
commit;

--paste from Alan's code
select *
  into #caci_sc
from (select distinct
         c.cb_row_id
        ,c.cb_key_individual
        ,c.cb_key_household
        ,playpen.exp_cb_key_household--just for a test
        ,c.lukcat_fr_de_nrs AS social_grade
        ,playpen.p_head_of_household
        ,rank() over(PARTITION BY playpen.exp_cb_key_household ORDER BY playpen.p_head_of_household desc, c.lukcat_fr_de_nrs asc, c.cb_row_id desc) as rank_id
from sk_prod.CACI_SOCIAL_CLASS as c,
     sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD as playpen,
     sk_prod.experian_consumerview e
where c.cb_key_individual = e.cb_key_individual
  and e.exp_cb_key_individual = playpen.exp_cb_key_individual
  and e.cb_address_dps is NOT NULL
  ) d
--end paste
;
commit;

  update uat_scenario10_syb as bas
     set bas.social_class = cac.social_grade
    from #caci_sc as cac
   where bas.cb_key_household = cac.cb_key_household
;
commit;

  update uat_scenario10_syb as bas
     set ABC1_MALES_IN_HH = case when social_class in ('A','B','C1') and MEN_IN_HH    NOT IN ('UNKNOWN', 'No men in HH')
                            then 1 else 0 end
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
  if object_id('uat_scenario11_1_syb') is not null then drop table uat_scenario11_1_syb end if;
  select cb_key_household
        ,calculated_scaling_weight
        ,cast(null as varchar(20)) as postcode -- needed for region
        ,cast(null as varchar(30)) as barb_region
        ,cast(null as varchar(50)) as financial_outlook
    into uat_scenario11_1_syb
    from sk_prod.viq_viewing_data_scaling
   where adjusted_event_start_date_vespa = '2012-12-20'                                         -- Scenario filter
     and cb_key_household is not null                                                           -- Common filter
     and cb_key_household > 0                                                                   -- Common filter
; --
commit;

create hg index idx1 on uat_scenario11_1_syb(cb_key_household);
create hg index idx3 on uat_scenario11_1_syb(postcode);

  update uat_scenario11_1_syb as bas
     set bas.postcode = sav.cb_address_postcode
    from sk_prod.cust_single_account_view as sav
   where bas.cb_key_household = sav.cb_key_household
;
commit;

  update uat_scenario11_1_syb as bas
     set barb_region = barb_desc_itv
    from sk_prod.barb_tv_regions as btr
   where btr.cb_address_postcode = bas.postcode
;
commit;

  update uat_scenario11_1_syb as bas
     set bas.financial_outlook = CASE h_fss_v3_group        WHEN    'A' THEN    'Bright Futures'
                                                            WHEN    'B' THEN    'Single Endeavours'
                                                            WHEN    'C' THEN    'Young Essentials'
                                                            WHEN    'D' THEN    'Growing Rewards'
                                                            WHEN    'E' THEN    'Family Interest'
                                                            WHEN    'F' THEN    'Accumulated Wealth'
                                                            WHEN    'G' THEN    'Consolidating Assets'
                                                            WHEN    'H' THEN    'Balancing Budgets'
                                                            WHEN    'I' THEN    'Stretched Finances'
                                                            WHEN    'J' THEN    'Established Reserves'
                                                            WHEN    'K' THEN    'Seasoned Economy'
                                                            WHEN    'L' THEN    'Platinum Pensions'
                                                            WHEN    'M' THEN    'Sunset Security'
                                                            WHEN    'N' THEN    'Traditional Thrift'
                                                            WHEN    'U' THEN    'UNKNOWN'
                                                            ELSE                'UNKNOWN'
                                                            END
    from sk_prod.experian_consumerview as con
   where bas.cb_key_household = con.cb_key_household
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
  if object_id('uat_scenario11_2_syb') is not null then drop table uat_scenario11_2_syb end if;
  select cb_key_household
        ,case when live_recorded = 'LIVE'                                                   then 'Live'
              when date(instance_start_date_time_utc) = date(broadcast_start_date_time_utc) then 'Vosdal'
              else                                                                             'Playback'
         end as live_recorded
        ,max(cast(0 as float)) as weighting
        ,date(instance_start_date_time_utc) as viewing_date
    into uat_scenario11_2_syb
    from sk_prod.vespa_events_all
   where instance_start_date_time_utc     <= '2012-12-10'                                       -- Scenario filter
     and instance_start_date_time_utc     >= '2012-12-16'                                       -- Scenario filter
     and broadcast_start_date_time_utc    <= '2012-12-03'                                       -- Scenario filter
     and broadcast_start_date_time_utc    >= '2012-12-13'                                       -- Scenario filter
     and programme_name = 'Top Gear'                                                            -- Scenario filter
     and cb_key_household is not null                                                           -- Common filter
     and cb_key_household > 0                                                                   -- Common filter
     and capped_full_flag = 0                                                                   -- Common filter
     and panel_id = 12                                                                          -- Common filter
group by cb_key_household
        ,live_recorded
        ,viewing_date
; --
commit;

  update uat_scenario2_syb as bas
     set bas.weighting = inl.calculated_scaling_weight
    from sk_prod.viq_viewing_data_scaling as inl
   where bas.cb_key_household = inl.cb_key_household
     and bas.viewing_date = inl.adjusted_event_start_date_vespa
;
commit;






/* ##### SUMMARY/OUTPUT ##### */
  select 1                                          as Scenario
        ,viewing_day                                as Attribute_1
        ,channel_name                               as Attribute_2
        ,mosaic_segment                             as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,sum(seconds)                               as Metric
    from uat_scenario1_syb
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

  union all

  select 2.1                                        as Scenario
        ,grouping_indicator                         as Attribute_1
        ,cast(viewing_date as varchar(10))           as Attribute_2
        ,household_composition                      as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,count(cb_key_household)                    as Metric
    from uat_scenario2_syb
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

  union all

  select 2.2                                        as Scenario
        ,grouping_indicator                         as Attribute_1
        ,cast(viewing_date as varchar(10))          as Attribute_2
        ,household_composition                      as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,sum(weighting)                             as Metric
    from uat_scenario2_syb
   where viewing_date = '2012-12-11'
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

  union all

  select 3                                          as Scenario
        ,affluence_band                             as Attribute_1
        ,cast(null as varchar(50))                  as Attribute_2
        ,cast(null as varchar(50))                  as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,sum(hours)                                 as Metric
    from uat_scenario3_syb
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

  union all

  select 4                                          as Scenario
        ,cast(episode_number as varchar(50))        as Attribute_1
        ,live_recorded                              as Attribute_2
        ,cast(null as varchar(50))                  as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,minutes                                    as Metric
    from uat_scenario4_syb

  union all

  select 5                                          as Scenario
        ,genre_description                          as Attribute_1
        ,cast(bb as varchar(50))                    as Attribute_2
        ,cast(bb_tenure as varchar(50))             as Attribute_3
        ,govt_region                                as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,count(cb_key_household)                    as Metric
    from uat_scenario5_syb
  -- where repeat_flag = 1
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

  union all

  select 6                                          as Scenario
        ,pay_free_indicator                         as Attribute_1
        ,genre_description                          as Attribute_2
        ,broadcast_day                              as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,duration                                   as Metric
    from uat_scenario6_syb

  union all

  select 7                                          as Scenario
        ,genre_description                          as Attribute_1
        ,cast(episodes_watched as varchar(50))      as Attribute_2
        ,cast(null as varchar(50))                  as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,sum(duration_scaled)                       as Metric
    from uat_scenario7_syb
   where episodes_watched > 1
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

  union all

  select 8                                          as Scenario
        ,live_recorded                              as Attribute_1
        ,sensitive_channel                          as Attribute_2
        ,cast(repeat_flag as varchar(50))           as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,duration                                   as Metric
    from uat_scenario8_syb

  union all

  select 9                                          as Scenario
        ,live_recorded                              as Attribute_1
        ,cast(null as varchar(50))                  as Attribute_2
        ,cast(null as varchar(50))                  as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,events_num                                 as Metric
    from uat_scenario9_syb

  union all

  select 10.1                                       as Scenario
        ,Household_composition                      as Attribute_1
        ,Property_Type                              as Attribute_2
        ,Govt_Region                                as Attribute_3
        ,Tenure_DTH                                 as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,count(distinct cb_key_household)           as Metric
    from uat_scenario10_syb
   where time_period = 2
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

  union all

  select 10.2                                       as Scenario
        ,ITV_Region                                 as Attribute_1
        ,BBC_Region                                 as Attribute_2
        ,abc1_males_in_household                    as Attribute_3
        ,Social_Class                               as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,count(distinct cb_key_household)           as Metric
    from uat_scenario10_syb
   where time_period = 2
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

  union all

  select 10.3                                       as Scenario
        ,Current_package                            as Attribute_1
        ,Sky_Product                                as Attribute_2
        ,cast(Previous_Sports_Downgrade as varchar(50)) as Attribute_3
        ,Value_Segment                              as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,count(distinct cb_key_household)           as Metric
    from uat_scenario10_syb
   where time_period = 2
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

  union all

  select 10.4                                       as Scenario
        ,cast(Active_Sky_Reward_User as varchar(50)) as Attribute_1
        ,cast(Missed_Payment_Last_Year as varchar(50)) as Attribute_2
        ,cast(Discount_Offer_Last_6_Months as varchar(50)) as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,count(distinct cb_key_household)           as Metric
    from uat_scenario10_syb
   where time_period = 1
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

  union all

  select 10.5                                       as Scenario
        ,cast(HH_Turnaround_Last_Year as varchar(50)) as Attribute_1
        ,cast(Previous_Movies_Downgrade as varchar(50)) as Attribute_2
        ,cast(null as varchar(50))                  as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,count(distinct cb_key_household)           as Metric
    from uat_scenario10_syb
   where time_period = 1
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

  union all

  select 11.1                                       as Scenario
        ,barb_region                                as Attribute_1
        ,financial_outlook                          as Attribute_2
        ,cast(null as varchar(50))                  as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,sum(calculated_scaling_weight)             as Metric
    from uat_scenario11_1_syb
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

  union all


  select 11.2                                       as Scenario
        ,live_recorded                              as Attribute_1
        ,cast(null as varchar(50))                  as Attribute_2
        ,cast(null as varchar(50))                  as Attribute_3
        ,cast(null as varchar(50))                  as Attribute_4
        ,cast(null as varchar(50))                  as Attribute_5
        ,sum(weighting)                             as Metric
    from uat_scenario11_2_syb
group by Scenario, Attribute_1, Attribute_2, Attribute_3, Attribute_4, Attribute_5

order by 1, 2, 3, 4, 5, 6;





















