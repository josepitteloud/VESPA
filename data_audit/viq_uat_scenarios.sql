-- Vespa Events All Version
---------------------------

-- Scenario 1
--  total seconds viewed (metrics)
--  watching Channel 4 (filter)
--  split by Channel 4 SD & HD (attribute)
--  split by Day (viewing between 01/10 & 14/10) (attribute)
--  split by Mosaic Segments (attribute)

/*
drop table uat_scenario1_part1;
drop table uat_scenario1;
drop table uat_scenario2_part1;
drop table uat_scenario2_1;
drop table uat_scenario2_2;
drop table uat_scenario3_part1;
drop table uat_scenario3;
drop table uat_scenario8;
drop table uat_scenario10_part1;
*/
  select cb_key_household
        ,channel_name                                                                  as channel_name
        ,cast('Unclassified' as varchar(100))                                          as mosaic_segment
        ,sum(datediff(second,instance_start_date_time_utc,instance_end_date_time_utc)) as seconds
        ,date(event_start_date_time_utc)                                               as dt
    into uat_scenario1_part1
    from sk_prod.vespa_events_all
   where channel_name in ('Channel 4', 'Channel 4 HD')
     and dt between '2012-12-04' and '2012-12-13'
     and cb_key_household is not null
     and cb_key_household > 0
group by cb_key_household
        ,channel_name
        ,mosaic_segment
        ,dt
; --3,147,089

update uat_scenario1_part1 as bas
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
; --2998071

  select channel_name
        ,case datepart(weekday,dt) when 1 then 'Sunday'
                                   when 2 then 'Monday'
                                   when 3 then 'Tuesday'
                                   when 4 then 'Wednesday'
                                   when 5 then 'Thursday'
                                   when 6 then 'Friday'
                                   when 7 then 'Saturday' end as dt
        ,mosaic_segment
        ,sum(seconds) as seconds
    into uat_scenario1
    from uat_scenario1_part1
group by channel_name
        ,dt
        ,mosaic_segment
; --448

select * from uat_scenario1;


/*
Scenario 2.1
        total number of unique households (actual)      (metrics)
        VOSDAL viewing  (filter)
        watching Top Gear       (filter)
        watching for 3 minutes or more  (filter)
        split by Channel Group broadcasted on   (attribute)
        split by Day (viewing between 01/09 & 30/09)    (attribute)
        split by Household Composition  (attribute)

Scenario 2.2
        total number of unique households (scaled)      (metrics)
        VOSDAL viewing  (filter)
        watching Top Gear       (filter)
        watching for 3 minutes or more  (filter)
        split by Channel Group broadcasted on   (attribute)
        split by Day (viewing between 01/09 & 30/09)    (attribute)
        split by Household Composition  (attribute)
*/

  select account_number
        ,cb_key_household
        ,case datepart(weekday,event_start_date_time_utc) when 1 then 'Sunday'
                                                                when 2 then 'Monday'
                                                                when 3 then 'Tuesday'
                                                                when 4 then 'Wednesday'
                                                                when 5 then 'Thursday'
                                                                when 6 then 'Friday'
                                                                when 7 then 'Saturday' end as dt
        ,cast('Unclassified' as varchar(50))           as household_composition
        ,cast(0 as int)   as scaling_segment_id
        ,cast(0 as float) as weighting
        ,event_start_date_time_utc
    into uat_scenario2_part1
    from sk_prod.vespa_events_all
   where datediff(second,instance_start_date_time_utc,instance_end_date_time_utc) > 180
     and instance_start_date_time_utc > broadcast_start_date_time_utc
     and date(instance_start_date_time_utc) = date(broadcast_start_date_time_utc)
     and programme_name = 'Top Gear'
     and event_start_date_time_utc between '2012-12-04' and '2012-12-14'
group by account_number
        ,cb_key_household
        ,dt
        ,household_composition
        ,event_start_date_time_utc
;

  update uat_scenario2_part1 as bas
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

  update uat_scenario2_part1
     set scaling_segment_ID = l.scaling_segment_ID
    from uat_scenario2_part1 as b
         inner join vespa_analysts.SC2_intervals as l on b.account_number = l.account_number
                                                     and b.event_start_date_time_utc between l.reporting_starts and l.reporting_ends
;

  update uat_scenario2_part1
     set weighting = s.weighting
    from uat_scenario2_part1 as b
         inner join vespa_analysts.SC2_weightings as s on date(b.event_start_date_time_utc) = s.scaling_day
                                                      and b.scaling_segment_ID = s.scaling_segment_ID
;

  select dt
        ,household_composition
--        ,grouping_indicator
        ,count(1) as households
    into uat_scenario2_1
    from uat_scenario2_part1
group by dt
        ,household_composition
--        ,grouping_indicator
;

  select dt
        ,household_composition
--        ,grouping_indicator
        ,sum(weighting) as scaled_households
    into uat_scenario2_2
    from uat_scenario2_part1
group by dt
        ,household_composition
--        ,grouping_indicator
        ;

select * from uat_scenario2_1;
select * from uat_scenario2_2;

/*
Scenario 3
        total hours viewed      (metrics)
        viewed between 15th October and 22nd October    (filter)
        watching {programme XXX}        (filter)
        broadcasted on 01 October between 19:30-22:30   (filter)
        split by Affluence Bands        (attribute)
*/

  select cast('Unclassified' as varchar)                                              as affluence_band
        ,sum(datediff(second,instance_start_date_time_utc,instance_end_date_time_utc)) as seconds
        ,cb_key_household
    into uat_scenario3_part1
    from sk_prod.vespa_events_all
   where programme_name = 'EastEnders'
     and genre_description='Entertainment'
     and sub_genre_description='Soaps'
     and instance_end_date_time_utc       > '2012-10-15'
     and instance_start_date_time_utc     < '2012-10-22'
     and broadcast_end_date_time_utc   > '2012-10-01 19:00'
     and broadcast_start_date_time_utc < '2012-10-01 22:30'
     and cb_key_household is not null
     and cb_key_household > 0
group by affluence_band
        ,cb_key_household
;

  update uat_scenario3_part1 as bas
     set affluence_band = exp.h_affluence_v2
    from sk_prod.experian_consumerview  as exp
   where bas.cb_key_household = exp.cb_key_household
;

  select affluence_band
        ,sum(seconds)/60 as hours
    into uat_scenario3
    from uat_scenario3_part1
group by affluence_band
;
select * from uat_scenario3


/*
Scenario 4
        total minutes viewed    (metrics)
        watching {series XX}    (filter)
        on {channel YY} (filter)
        broadcasted between 01/09 and 30/09     (filter)
        split by individual episode number      (attribute)
        split by Live, VOSDAL, Timeshift 1-7 days, Timeshift 8-28 days  (attribute)
*/
  select sum(datediff(minute,instance_start_date_time_utc,instance_end_date_time_utc)) as minutes
        ,episode_number
        ,case when live_recorded = 'LIVE'                                                      then 'Live'
              when date(event_start_date_time_utc) - date(broadcast_start_date_time_utc)  =  0 then 'Vosdal'
              when date(event_start_date_time_utc) - date(broadcast_start_date_time_utc) <=  7 then 'Timeshift 2-7 days'
              when date(event_start_date_time_utc) - date(broadcast_start_date_time_utc) <= 28 then 'Timeshift 8-28 days'
              else                                                                                  'Other' end as live_recorded
    into uat_scenario4
    from sk_prod.vespa_events_all
   where programme_name = 'A League Of Their Own'
     and channel_name like 'Sky1'
     and season_ref = 228792
     and date(event_start_date_time_utc) = '2012-10-01'
     and broadcast_end_date_time_utc    >= '2012-09-01'
     and broadcast_start_date_time_utc  <= '2012-09-30'
group by episode_number
        ,live_recorded
; --
select * from uat_scenario4;


/*
Scenario 5
        total number of unique households (actual) that watched repeat programmes       (metrics)
        viewing between 01/09 & 30/09   (filter)
        split by Channel Genre  (attribute)
        split by broadband (yes/no) and further by broadband line tenure        (attribute)
        split by Region (attribute)
*/
  select cb_key_household
        ,channel_genre
        ,cast(0 as bit) as bb
        ,cast(0 as int) as bb_tenure
        ,cast(null as varchar) as cb_address_postcode
        ,cast(null as varchar) as govt_region
    into uat_scenario5_part1
    from sk_prod.vespa_events_all
   where instance_start_date_time_utc <= '2012-09-30 23:59:59'
     and instance_end_date_time_utc   >= '2012-09-01'
     and repeat_flag = 0
group by cb_key_household
        ,channel_genre
        ,bb
        ,bb_tenure
        ,cb_address_postcode
        ,govt_region
;

  update uat_scenario5_part1 as bas
     set bb = 1
    from sk_prod.cust_subs_hist as csh
   where bas.cb_key_household = csh.cb_key_household
     and csh.subscription_sub_type ='Broadband DSL Line'
     AND (       status_code in ('AC','AB')
      OR (status_code='PC' AND prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
      OR (status_code='CF' AND prev_status_code='PC'                                  )
      OR (status_code='AP' AND sale_type='SNS Bulk Migration'                         ))
     and effective_to_dt = '9999-09-09'
;

  update uat_scenario5_part1 as bas
     set bas.cb_address_postcode = sav.cb_address_postcode
    from sk_prod.cust_single_account_view as sav
   where bas.cb_key_household = sav.cb_key_household
;

  update uat_scenario5_part1 as bas
     set bas.govt_region = bpe.GOVERNMENT_REGION
    from sk_prod.BROADBAND_POSTCODE_EXCHANGE as bpe
   where bas.cb_address_postcode = bpe.cb_address_postcode
;

  select count(cb_key_household) as households
        ,govt_region
        ,bb
        ,bb_tenure
    from uat_scenario5_part1
group by govt_region
        ,bb
        ,bb_tenure
;

/*
Scenario 6
        total Programme Scheduled duration      (metrics)
        broadcasted between 01/09 & 30/09       (filter)
        split by Channel Type   (attribute)
        split by Channel Genre  (attribute)
        split by Day    (attribute)
*/
  select max(datediff(minute,broadcast_start_date_time_utc,broadcast_end_date_time_utc)) as minute
        ,pay_free_indicator
        ,channel_genre
        ,date(broadcast_start_date_time_utc) as dt
    into uat_scenario6
    from sk_prod.vespa_events_all
   where dt >= '2012-09-01'
     and dt <= '2012-09-30'
group by pay_free_indicator
        ,channel_genre
        ,dt
; --

select * from uat_scenario6


/*
Scenario 7
    total scaled duration   (metrics)
    watched Live    (filter)
    broadcasted between 01/09 & 30/09       (filter)
    watched more than one episode in series (filter)
    split by Programme Genre        (attribute)
    split by number of episodes watched     (attribute)
*/

  select account_number
        ,genre_description
        ,episode_number
        ,season_ref
        ,cast(0 as int)   as scaling_segment_id
        ,cast(0 as float) as weighting
        ,sum(datediff(second, instance_start_date_time_utc, instance_end_date_time_utc)) as duration
    into uat_scenario7_part1
    from sk_prod.vespa_events_all
    where live_recorded = 'LIVE'
      and broadcast_end_date_time_utc    >= '2012-09-01'
      and broadcast_start_date_time_utc  <= '2012-09-30 23:59:59'
group by account_number
        ,genre_description
        ,episode_number
        ,season_ref
        ,scaling_segment_id
        ,weighting
;

  update uat_scenario7_part1 as bas
     set scaling_segment_ID = inl.scaling_segment_ID
    from vespa_analysts.SC2_intervals as inl
   where bas.account_number = inl.account_number
     and bas.weighting_date between inl.reporting_starts and inl.reporting_ends
;

  update uat_scenario7_part1 as bas
     set weighting = wei.weighting
    from vespa_analysts.SC2_weightings as wei
   where bas.weighting_date = wei.scaling_day
     and bas.scaling_segment_ID = wei.scaling_segment_ID
;

;


/*
Scenario 8
    total hours viewed      (metrics)
    viewing between 01/09 & 30/09   (filter)
    for programmes broadcasted between 01/09 & 30/09        (filter)
    split by Live, VOSDAL, Playback (attribute)
    split by Sensitive Channel      (attribute)
    split by Repeat Flag    (attribute)
*/

  select sum(datediff(second, instance_start_date_time_utc, instance_end_date_time_utc))/60 as duration
        ,case when live_recorded = 'LIVE'                                                      then 'Live'
              when date(event_start_date_time_utc) - date(broadcast_start_date_time_utc)  =  0 then 'Vosdal'
              when date(event_start_date_time_utc) - date(broadcast_start_date_time_utc) <=  7 then 'Timeshift 2-7 days'
              when date(event_start_date_time_utc) - date(broadcast_start_date_time_utc) <= 28 then 'Timeshift 8-28 days'
              else                                                                                  'Other' end as live_recorded
        ,sensitive_channel
        ,repeat_flag
    into uat_scenario8
    from sk_prod.vespa_events_all
   where broadcast_end_date_time_utc    >= '2012-09-01'
     and broadcast_start_date_time_utc  <= '2012-09-30 23:59:59'
group by live_recorded
        ,sensitive_channel
        ,repeat_flag
;

select * from uat_scenario8
/*
--Scenario 9
        total number of 3min+ events viewed     (metrics)
        watched between 10/09 & 16/09   (filter)
        split by Live, VOSDAL, Playback (attribute)
*/

  select count(1) as cow
        ,case when live_recorded = 'LIVE'                                                then 'Live'
              when date(event_start_date_time_utc) = date(broadcast_start_date_time_utc) then 'Vosdal'
              else                                                                            'Playback' end as live_recorded
    into uat_scenario9
    from sk_prod.vespa_events_all
   where instance_end_date_time_utc    >= '2012-12-04'
     and instance_start_date_time_utc  < '2012-12-07'
     and datediff(second, instance_start_date_time_utc, instance_end_date_time_utc) >= 180
group by live_recorded
; --
select * from uat_scenario9


/*
Scenario 10.1
        total number of unique households (actual)      (metrics)
        viewing between 10/09 & 24/09   (filter)
        split by Household composition  (attribute)
        split by Property Type  (attribute)
        split by Government Region      (attribute)
        split by Tenure DTH     (attribute)

Scenario 10.2
        total number of unique households (actual)      (metrics)
        viewing between 10/09 & 24/09   (filter)
        split by BARB ITV Region        (attribute)
        split by BARB BBC Region        (attribute)
        split by ABC1 Males in HH       (attribute)
        split by Social Class   (attribute)

Scenario 10.3
        total number of unique households (actual)      (metrics)
        viewing between 10/09 & 24/09   (filter)
        split by Current Package        (attribute)
        split by Sky Product Set        (attribute)
        split by Previous Sports Downgrade      (attribute)
        split by Value Segments (attribute)

Scenario 10.4
        total number of unique households (actual)      (metrics)
        viewing between 10/09 & 24/09   (filter)
        split by Active Sky Reward User (attribute)
        split by Missed Payment Last Year       (attribute)
        split by Discount Offer Last 6 Months   (attribute)

Scenario 10.5
        total number of unique households (actual)      (metrics)
        viewing between 10/09 & 24/09   (filter)
        split by HH Turnaround Last Year        (attribute)
        split by Previous Movies Downgrade      (attribute)
*/

  select cb_key_household
        ,account_number
        ,cast(null as varchar(20)) as cb_address_postcode
        ,cast('Unclassified' as varchar)           as household_composition
        ,cast('Unclassified' as varchar)           as property_type
        ,cast('Unclassified' as varchar)           as govt_region
        ,cast('Unclassified' as varchar)           as tenure_dth
        ,cast(null as varchar)                     as itv_region
        ,cast(null as varchar)                     as bbc_region
        ,cast(null as varchar)                     as men_in_hh --required for abc1 males in hh
        ,cast(null as varchar)                     as abc1_males_in_household
        ,cast(null as varchar)                     as social_class
        ,cast(null as varchar)                     as current_package
        ,cast(null as varchar)                     as sky_product
        ,cast(0 as bit)                            as previous_sports_downgrade
        ,cast(null as varchar)                     as value_segment
        ,cast(0 as bit)                            as Active_Sky_Reward_User
        ,cast(0 as bit)                            as Missed_Payment_Last_Year
        ,cast(0 as bit)                            as Discount_Offer_Last_6_Months
        ,cast(0 as bit)                            as HH_Turnaround_Last_Year
        ,cast(0 as bit)                            as Previous_Movies_Downgrade
    into uat_scenario10_part1
    from sk_prod.vespa_events_all
   where instance_end_date_time_utc    >= '2012-09-10'
     and instance_start_date_time_utc  <= '2012-09-24 23:59:59'
group by cb_key_household
        ,account_number
;

  update uat_scenario10_part1 as bas
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

  update uat_scenario10_part1 as bas
     set bas.cb_address_postcode = sav.cb_address_postcode
    from sk_prod.cust_single_account_view as sav
   where bas.cb_key_household = sav.cb_key_household
;

  update uat_scenario10_part1 as bas
     set bas.govt_region = bpe.GOVERNMENT_REGION
    from sk_prod.BROADBAND_POSTCODE_EXCHANGE as bpe
   where bas.cb_address_postcode = bpe.cb_address_postcode
;

  update uat_scenario10_part1 as bas
     set itv_region = barb_desc_itv
        ,bbc_region = barb_desc_bbc
    from sk_prod.barb_tv_regions as btr
   where btr.cb_address_postcode = bas.cb_address_postcode
;

  update uat_scenario10_part1 as bas
     set tenure_dth = cast(datediff(month, '2012-11-27', acct_first_account_activation_dt) as varchar)
    from sk_prod.cust_single_account_view as sav
   where bas.account_number = sav.account_number
;

  update uat_scenario10_part1 as bas
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
     and effective_to_dt = '9999-09-09'
     and status_code in ('AC', 'PC', 'AB')
; --

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
         inner join uat_scenario10_part1 as bas on csh.account_number = bas.account_number
   where effective_to_dt = '9999-09-09'
group by csh.account_number
;

  update uat_scenario10_part1 as bas
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

  select bas.account_number
    into #sports_downgrade
    from uat_scenario10_part1 as bas
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

  update uat_scenario10_part1 as bas
     set previous_sports_downgrade = 1
    from #sports_downgrade as spt
   where bas.account_number = spt.account_number
;

  select bas.account_number
    into #movies_downgrade
    from uat_scenario10_part1 as bas
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

  update uat_scenario10_part1 as bas
     set previous_movies_downgrade = 1
    from #movies_downgrade as mov
   where bas.account_number = mov.account_number
;

  update uat_scenario10_part1 as bas
     set value_segment = vsd.value_seg
   from sk_prod.VALUE_SEGMENTS_DATA  as vsd
   where bas.account_number = vsd.account_number
;

  update uat_scenario10_part1 as bas
     set Active_Sky_Reward_User = 1
    from sk_prod.sky_rewards_competitions as src
  where bas.account_number = src.account_number
;

  update uat_scenario10_part1 as bas
     set missed_payment_last_year = 1
    from sk_prod.cust_bills as bil
  where bas.account_number = bil.account_number
    and status = 'Unbilled'
    and payment_due_dt between '2011-11-28' and '2012-11-27'
;

  update uat_scenario10_part1 as bas
     set Discount_Offer_Last_6_Months = 1
    from sk_prod.cust_product_offers as cpo
   where bas.account_number = cpo.account_number
     and offer_end_dt > '2012-06-27'
;

  update uat_scenario10_part1 as bas
     set HH_Turnaround_Last_Year = 1
    from sk_prod.cust_change_attempt as cca
   where bas.account_number = cca.account_number
     and turnaround_flag in ('S', 'F')
     and attempt_date >= '2011-11-27'
;

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

  update uat_scenario10_part1 as bas
     set bas.social_class = cac.social_grade
    from #caci_sc as cac
   where bas.cb_key_household = cac.cb_key_household
;

  update uat_scenario10_part1 as bas
     set ABC1_MALES_IN_HH = case when social_class in ('A','B','C1') and MEN_IN_HH    NOT IN ('UNKNOWN', 'No men in HH')
                            then 1 else 0 end
;




--10.1
  select count(1)
    into greenj.uat_scenario10_1
    from uat_scenario10_part1
group by Household_composition
        ,Property_Type
        ,Govt_Region
        ,Tenure_DTH
;

--10.2
  select count(1)
    into greenj.uat_scenario10_2
    from uat_scenario10_part1
group by ITV_Region
        ,BBC_Region
        ,abc1_males_in_household
        ,Social_Class
;

--10.3
  select count(1)
    into greenj.uat_scenario10_3
    from uat_scenario10_part1
group by Current_package
        ,Sky_Product
        ,Previous_Sports_Downgrade
        ,Value_Segment
;

--10.4
  select count(1)
    into greenj.uat_scenario10_4
    from uat_scenario10_part1
group by Active_Sky_Reward_User
        ,Missed_Payment_Last_Year
        ,Discount_Offer_Last_6_Months
;

--10.5
  select count(1)
    into greenj.uat_scenario10_5
    from uat_scenario10_part1
group by HH_Turnaround_Last_Year
        ,Previous_Movies_Downgrade
;


--11.1
--total number of unique households (scaled)      (metrics)
--split by BARB region    (attribute)
--split by Financial Outlook      (attribute)

--11.2
--total number of unique households (scaled)      (metrics)
--viewing between 05/09 & 07/09   (filter)
--for programmes broadcasted between 05/09 & 07/09        (filter)
--watching BBC News       (filter)
--split by Live, VOSDAL, Playback (attribute)

  select cb_key_household
        ,cast(null as varchar(20)) as postcode -- needed for region
        ,cast(null as varchar) as barb_region
        ,cast(null as varchar) as financial_outlook
        ,case when live_recorded = 'LIVE'                                                then 'Live'
              when date(event_start_date_time_utc) = date(broadcast_start_date_time_utc) then 'Vosdal'
              else                                                                            'Playback' end as live_recorded
    into uat_scenario11_part1
    from sk_prod.vespa_events_all
   where event_end_date_time_utc     < '2012-09-08'
     and event_end_date_time_utc     > '2012-09-05'
     and broadcast_end_date_time_utc < '2012-09-08'
     and broadcast_end_date_time_utc > '2012-09-05'
     and programme_name = 'BBC News'
group by cb_key_household
        ,barb_region
        ,financial_outlook
        ,live_recorded
; --

  update uat_scenario11_part1 as bas
     set bas.postcode = sav.cb_address_postcode
    from sk_prod.cust_single_account_view as sav
   where bas.cb_key_household = sav.cb_key_household
;

  update uat_scenario11_part1 as bas
     set barb_region = barb_desc_itv
    from sk_prod.barb_tv_regions as btr
   where btr.cb_address_postcode = bas.postcode
;

  update uat_scenario11_part1 as bas
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

  select count(1) as households
        ,barb_region
        ,financial_outlook
    into uat_scenario11_1
    from uat_scenario11_part1
group by barb_region
        ,financial_outlook
;

  select count(1) as households
        ,live_recorded
    into uat_scenario11_2
    from uat_scenario11_part1
group by live_recorded
;

select * from uat_scenario11_1;
select * from uat_scenario11_2;




---
/*
Qs:

 2  : There is no data in grouping_indicator. There is a field called epg_group...
 3  : There is no affluence_band field, have used h_affluence_v2
 5  : There isno data in the repeat_flag field
 7  : Should we only look at programme views of at least 3 mins? no
 For a specific programme? no
 sum of all series,
      or just the ones from the series that have viewing from different episodes? e.g. someone watched Series 9 episode 1, Series 10 episode 3 and Series 10 episode 4. Do we use all of them, or just series 10? just 10
10.2: We need a definition for abc1 males & social class
11.1: Is this BARB_ITV region? Also, I have included date restrictions from 11.2.
Is BBC News a programme, or channels + genre? prog
Fininacial outlook not in consumerview
*/







select top 1 * from sk_prod.experian_consumerview

