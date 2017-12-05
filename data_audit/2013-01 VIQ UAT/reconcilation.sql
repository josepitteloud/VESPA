
    -- ### Get sybase basic results ###
begin


    declare @varTargetDate          date
    declare @varLastDay             date

    declare @varFilterStartHour     int
    declare @varFilterEndHour       int

    declare @varPrevDayStartHour    int
    declare @varPrevDayEndHour      int
    declare @varCurrDayStartHour    int
    declare @varCurrDayEndHour      int

    set @varTargetDate = '2012-12-10'
    set @varLastDay    = '2012-12-10'

    if object_id('recon_syb') is not null drop table recon_syb
    create table recon_syb (
        day_processed                       date        default null,
        pk_viewing_prog_instance_fact       bigint      default null,
        cb_change_date                      date        default null,
        cb_key_household                    bigint      default null,
        account_number                      varchar(20) default null,
        viewing_date                        date        default null,
        live_recorded                       varchar(20) default null,
        viewed_duration                     int default null,
        viewed_duration_actual              int default null,
        service_type_description            varchar(40) default null,
        type_of_viewing_event               varchar(40) default null,
        reported_playback_speed             numeric(4, 0) default null,
        dk_barb_min_start_datehour_dim      bigint      default null,
        dk_barb_min_end_datehour_dim        bigint      default null,
        barb_min_start_date_time_utc        datetime    default null,
        barb_min_end_date_time_utc          datetime    default null,
        barb_min_start_date_time_utc_adj    datetime    default null,
        barb_min_end_date_time_utc_adj      datetime    default null,
        instance_start_date_time_utc        datetime    default null,
        instance_end_date_time_utc          datetime    default null
    )

    create hg index idx1 on recon_syb(cb_key_household)
    create hg index idx2 on recon_syb(account_number)
    create date index idx3 on recon_syb(viewing_date)
    create dttm index idx4 on recon_syb(barb_min_start_date_time_utc_adj)
    create dttm index idx5 on recon_syb(barb_min_end_date_time_utc_adj)
    create date index idx6 on recon_syb(day_processed)
    create dttm index idx7 on recon_syb(barb_min_start_date_time_utc)
    create dttm index idx8 on recon_syb(barb_min_end_date_time_utc)


    while @varTargetDate <= @varLastDay
        begin

            set @varFilterStartHour     = (dateformat(@varTargetDate - 1, 'yyyymmdd00'))
            set @varFilterEndHour       = (dateformat(@varTargetDate, 'yyyymmdd23'))

            set @varPrevDayStartHour    = (dateformat(@varTargetDate - 1, 'yyyymmdd00'))
            set @varPrevDayEndHour      = (dateformat(@varTargetDate - 1, 'yyyymmdd23'))

            set @varCurrDayStartHour    = (dateformat(@varTargetDate, 'yyyymmdd00'))
            set @varCurrDayEndHour      = (dateformat(@varTargetDate, 'yyyymmdd23'))


            insert into recon_syb
              select @varTargetDate as day_processed
                    ,pk_viewing_prog_instance_fact
                    ,cb_change_date
                    ,cb_key_household
                    ,account_number
                    ,null as viewing_date
                    ,live_recorded
                    ,null as viewed_duration
                    ,null as viewed_duration_actual
                    ,service_type_description
                    ,type_of_viewing_event
                    ,reported_playback_speed
                    ,dk_barb_min_start_datehour_dim
                    ,dk_barb_min_end_datehour_dim
                    ,barb_min_start_date_time_utc
                    ,barb_min_end_date_time_utc
                    ,null as barb_min_start_date_time_utc_adj
                    ,null as barb_min_end_date_time_utc_adj
                    ,instance_start_date_time_utc
                    ,instance_end_date_time_utc

                from sk_prod.vespa_events_all
               where dk_barb_min_start_datehour_dim <= @varFilterEndHour                                    -- Scenario filter
                 and dk_barb_min_end_datehour_dim >= @varFilterStartHour                                    -- Scenario filter
                 and cb_key_household is not null                                                           -- Common filter
                 and cb_key_household > 0                                                                   -- Common filter

                   -- VIQ filter
                 and panel_id in (4, 12)                                                                    -- VIQ filter
                 and capped_full_flag = 0
                 and (
                        type_of_viewing_event = 'TV Channel Viewing'
                        or
                        type_of_viewing_event = 'HD Viewing Event'
                        or
                           (
                            type_of_viewing_event = 'Other Service Viewing Event' and
                            service_type_description = 'High Definition TV test service'
                           )
                        or
                        type_of_viewing_event = 'Sky+ time-shifted viewing event'
                     )
                 and dk_barb_min_start_datehour_dim <> -1
                 and dk_barb_min_start_time_dim <> -1
                 and dk_barb_min_end_datehour_dim <> -1
                 and dk_barb_min_end_time_dim <> -1
                 and duration > 0
                 and playback_speed = 1
                 and video_playing_flag = 1
                 and datediff(hour, instance_start_date_time_utc, broadcast_start_date_time_utc) <= 720
                 and time_in_seconds_since_recording > 0

            commit

            set @varTargetDate = @varTargetDate + 1

        end


end;
commit;



update recon_syb
  set viewing_date                        = case
                                              when ( date(barb_min_start_date_time_utc) < day_processed and date(barb_min_end_date_time_utc) < day_processed ) or
                                                   ( date(barb_min_start_date_time_utc) > day_processed and date(barb_min_end_date_time_utc) > day_processed ) then null
                                                else day_processed
                                            end,

      barb_min_start_date_time_utc_adj    = case
                                                -- Starts on the previous day
                                              when ( date(barb_min_start_date_time_utc) <  day_processed ) and
                                                   ( date(barb_min_end_date_time_utc)   >= day_processed ) then date(barb_min_end_date_time_utc)

                                                -- Starts on the day
                                                else barb_min_start_date_time_utc
                                            end,

      barb_min_end_date_time_utc_adj      = case
                                                -- Ends on the following day
                                              when ( date(barb_min_start_date_time_utc) <= day_processed ) and
                                                   ( date(barb_min_end_date_time_utc)   >  day_processed ) then dateadd(minute, -1, cast(date(barb_min_start_date_time_utc) + 1 as datetime))

                                                -- Ends on the day
                                                else barb_min_end_date_time_utc
                                            end
;
commit;


update recon_syb
  set viewed_duration                     = datediff(second, barb_min_start_date_time_utc_adj, barb_min_end_date_time_utc_adj) + 60,
      viewed_duration_actual              = datediff(second, instance_start_date_time_utc, instance_end_date_time_utc)
;
commit;






    -- ### Get star schema basic results ###
  if object_id('recon_star') is not null then drop table recon_star endif;
  select viewing_data_id
        ,bas.household_key
        ,cb_key_household
        ,account_number
        ,bds.utc_day_date as viewing_start_date
        ,bts.utc_time_minute as viewing_start_time
        ,bde.utc_day_date as viewing_end_date
        ,bte.utc_time_minute as viewing_end_time
        ,cast(bds.utc_day_date || ' ' || bts.utc_time_minute as datetime) as viewing_start
        ,cast(bde.utc_day_date || ' ' || bte.utc_time_minute as datetime) as viewing_end
        ,viewing_type as live_recorded
        ,viewed_duration as viewed_duration_original
        ,cast(null as smallint) as viewed_duration
    into recon_star
    from sk_prod.viq_viewing_data_uat                as bas
         inner join sk_prod.viq_date                 as bds on bas.viewing_start_date_key   = bds.pk_datehour_dim
         inner join sk_prod.viq_date                 as bde on bas.viewing_end_date_key     = bde.pk_datehour_dim
         inner join sk_prod.viq_time                 as bts on bas.viewing_start_time_key   = bts.pk_time_dim
         inner join sk_prod.viq_time                 as bte on bas.viewing_end_time_key     = bte.pk_time_dim
         inner join sk_prod.viq_time_shift           as shf on bas.time_shift_key           = shf.pk_timeshift_dim
         inner join sk_prod.viq_household            as hsh on bas.household_key            = hsh.household_key
   where viewing_start_date_key >= 2012121000                                            -- Scenario filter
     and viewing_start_date_key <= 2012121323                                            -- Scenario filter
     and bas.household_key is not null                                                          -- Common filter
     and bas.household_key > 0                                                                  -- Common filter
;
commit;

create hg index idx1 on recon_star(cb_key_household);
create hg index idx2 on recon_star(account_number);
create date index idx3 on recon_star(viewing_start_date);
create dttm index idx4 on recon_star(viewing_start);
create dttm index idx5 on recon_star(viewing_end);

update recon_star
   set viewed_duration = datediff(second, viewing_start, viewing_end) + 60;
commit;





    -- ### Compare ###
select
      coalesce(a.viewing_date, b.viewing_start_date) as viewing_date
     ,coalesce(a.live_recorded2, b.live_recorded2) as live_recorded2
     ,a.Cnt_Syb
     ,b.Cnt_Star
     ,a.Cnt_HHs_Syb
     ,b.Cnt_HHs_Star
     ,b.Cnt_HHs_Star2
  from (select
              viewing_date
             ,upper(live_recorded) as live_recorded2
             ,count(*) as Cnt_Syb
             ,sum(viewed_duration) as Viewing_Syb
             ,count(distinct cb_key_household) as Cnt_HHs_Syb
          from recon_syb
         where live_recorded is not null
           and barb_min_start_date_time_utc < barb_min_end_date_time_utc      -- Exclude 1 minute long events, exclude start> end events
           and viewing_date is not null
         group by live_recorded2, viewing_date) a
       full join
       (select
              viewing_start_date
             ,upper(case when live_recorded = 'Vosdal' then 'Recorded' else live_recorded end) as live_recorded2
             ,count(*) as Cnt_Star
             ,sum(viewed_duration) as Viewing_Star
             ,count(distinct cb_key_household) as Cnt_HHs_Star
             ,count(distinct household_key) as Cnt_HHs_Star2
          from recon_star
         where viewing_start < viewing_end                                    -- Exclude 1 minute long events, exclude start> end events
         group by live_recorded2, viewing_start_date) b
       on a.viewing_date = b.viewing_start_date
      and a.live_recorded2 = b.live_recorded2;
commit;



  -- Check HH differences
if object_id('recon_HHs_comparison') is not null then drop table recon_HHs_comparison endif;
select
      coalesce(a.cb_key_household, b.cb_key_household) as cb_key_household
     ,coalesce(a.account_number, b.account_number) as account_number
     ,case
        when a.cb_key_household is null then 'Star'
        when b.cb_key_household is null then 'Sybase'
          else 'Both'
      end HH_pool
     ,case
        when a.account_number is null then 'Star'
        when b.account_number is null then 'Sybase'
          else 'Both'
      end ACC_pool
  into recon_HHs_comparison
  from (select distinct
              cb_key_household
              ,min(account_number) as account_number
          from recon_syb
         where live_recorded is not null
           and barb_min_start_date_time_utc < barb_min_end_date_time_utc      -- Exclude 1 minute long events, exclude start> end events
           and viewing_date is not null
           and viewing_date = '2012-12-10'
         group by cb_key_household) a
       full join
       (select distinct
              cb_key_household
              ,min(account_number) as account_number
          from recon_star
         where viewing_start < viewing_end                                    -- Exclude 1 minute long events, exclude start> end events
           and viewing_start_date = '2012-12-10'
         group by cb_key_household) b
       on a.cb_key_household = b.cb_key_household;
commit;





if object_id('recon_syb_acc_sample') is not null then drop table recon_syb_acc_sample endif;
select *
  into recon_syb_acc_sample
  from sk_prod.vespa_events_all
 where dk_barb_min_start_datehour_dim >= 2013012900                                           -- Scenario filter
   and dk_barb_min_start_datehour_dim <= 2013012923                                           -- Scenario filter
   and cb_key_household is not null                                                           -- Common filter
   and cb_key_household > 0                                                                   -- Common filter
   --and capped_full_flag = 0                                                                   -- Common filter
   --and panel_id = 12                                                                          -- Common filter
   and barb_min_start_date_time_utc is not null
   and barb_min_end_date_time_utc is not null
   and barb_min_start_date_time_utc < barb_min_end_date_time_utc      -- Exclude 1 minute long events, exclude start> end events
   and account_number in ('621058055339','621003732420','621247511762','621016198817','620007413631')
   --and type_of_viewing_event <> 'Non viewing event'
   --and (reported_playback_speed is null or reported_playback_speed = 2)
   -- and instance_start_date_time_utc < instance_end_date_time_utc     -- remove 0sec instances
 order by account_number, subscriber_id, barb_min_start_date_time_utc, barb_min_end_date_time_utc
;
commit;

select * from recon_syb_acc_sample;


if object_id('recon_star_acc_sample') is not null then drop table recon_star_acc_sample endif;
select bas.viewing_data_id
      ,cb_key_household
      ,account_number
      ,bas.viewing_start_time_key
      ,bas.viewing_end_time_key
      ,bas.event_viewed_flag
      ,bas.programme_viewed_flag
      ,chn.service_type_description
      ,chn.epg_group_name
      ,chn.channel_name
      ,prg.programme_name
      ,shf.viewing_type
      ,bds.utc_day_date as viewing_date
      ,bdt.utc_time_minute as start_time
      ,bdx.utc_time_minute as end_time
      ,bas.viewed_duration
  into recon_star_acc_sample
  from sk_prodreg.viq_viewing_data                 as bas
       inner join sk_prod.viq_date                 as bds on bas.viewing_start_date_key   = bds.pk_datehour_dim
       inner join sk_prod.viq_time                 as bdt on bas.viewing_start_time_key   = bdt.pk_time_dim
       inner join sk_prod.viq_time                 as bdx on bas.viewing_end_time_key     = bdx.pk_time_dim
       inner join sk_prod.viq_time_shift           as shf on bas.time_shift_key           = shf.pk_timeshift_dim
       inner join sk_prod.viq_household            as hsh on bas.household_key            = hsh.household_key
       inner join sk_prod.viq_programme_uat        as prg on bas.prog_inst_programme_key  = prg.pk_programme_dim
       inner join sk_prod.viq_channel_uat          as chn on bas.prog_inst_channel_key    = chn.pk_channel_dim
 where viewing_start_date_key >= 2013012900                                            -- Scenario filter
   and viewing_start_date_key <= 2013012923                                            -- Scenario filter
   -- and bas.household_key is not null                                                          -- Common filter
   -- and bas.household_key > 0                                                                  -- Common filter
   and account_number in ('621058055339','621003732420','621247511762','621016198817','620007413631')
 order by account_number, bds.utc_day_date, bdt.utc_time_minute, bdx.utc_time_minute
;
commit;

select * from recon_star_acc_sample;





