

begin

    declare @varBuildId   int
    declare @varStartDate date
    declare @varEndDate   date
    declare @varSql       varchar(10000)

    set @varStartDate = '2013-04-29'      -- A thursday
    set @varEndDate   = '2013-05-01'      -- The following Wednesday

    execute logger_create_run 'VIQ_UAT', 'UAT', @varBuildId output


    if object_id('uat_scenario0_daily_accounts') is not null drop table uat_scenario0_daily_accounts
    create table uat_scenario0_daily_accounts (
      source                        varchar(20) default null,
      viewing_day                   date        default null,
      cb_key_household              bigint      default null,
      account_number                varchar(20) default null,
      live_recorded                 varchar(20) default null,
      duration                      int         default null
    )

    create hg index idx1 on uat_scenario0_daily_accounts(account_Number)
    create lf index idx2 on uat_scenario0_daily_accounts(Live_Recorded)
    create lf index idx3 on uat_scenario0_daily_accounts(source)
    create date index idx4 on uat_scenario0_daily_accounts(viewing_day)

    execute logger_add_event @varBuildId, 3, '1) Summary tables created'


    while @varStartDate <= @varEndDate
        begin

              execute logger_add_event @varBuildId, 3, '>>>>> Processing day ' || @varStartDate || ' <<<<<'


                -- ########################################################################################
                -- ##### Sybase records #####
              if object_id('uat_scenario0_syb') is not null drop table uat_scenario0_syb
              select cb_key_household
                    ,account_number
                    ,live_recorded
                    ,date(barb_min_start_date_time_utc) as viewing_day
                    ,barb_min_start_date_time_utc
                    ,barb_min_end_date_time_utc

                    ,case
                       when (dk_barb_min_start_datehour_dim between cast(dateformat(@varStartDate - 1, 'yyyymmdd00') as int) and cast(dateformat(@varStartDate - 1, 'yyyymmdd23') as int)) and                                  -- TX - 1(00-23)
                            (dk_barb_min_end_datehour_dim between cast(dateformat(@varStartDate, 'yyyymmdd00') as int) and cast(dateformat(@varStartDate, 'yyyymmdd23') as int)) then cast(@varStartDate as datetime)           -- TX (00-23)
                         else barb_min_start_date_time_utc
                     end as barb_min_start_date_time_utc_adj

                    ,case
                       when (dk_barb_min_start_datehour_dim between cast(dateformat(@varStartDate, 'yyyymmdd00') as int) and cast(dateformat(@varStartDate, 'yyyymmdd23') as int)) and                                          -- TX (00-23)
                            (dk_barb_min_end_datehour_dim >= cast(dateformat(@varStartDate + 1, 'yyyymmdd00') as int)) then cast(@varStartDate + 1 as datetime)                                                                 -- TX + 1 (00)
                         else barb_min_end_date_time_utc
                     end as barb_min_end_date_time_utc_adj

                    ,datediff(second,
                               case
                                 when (dk_barb_min_start_datehour_dim between cast(dateformat(@varStartDate - 1, 'yyyymmdd00') as int) and cast(dateformat(@varStartDate - 1, 'yyyymmdd23') as int)) and                        -- TX - 1(00-23)
                                      (dk_barb_min_end_datehour_dim between cast(dateformat(@varStartDate, 'yyyymmdd00') as int) and cast(dateformat(@varStartDate, 'yyyymmdd23') as int)) then cast(@varStartDate as datetime) -- TX (00-23)
                                   else barb_min_start_date_time_utc
                               end,
                               case
                                 when (dk_barb_min_start_datehour_dim between cast(dateformat(@varStartDate, 'yyyymmdd00') as int) and cast(dateformat(@varStartDate, 'yyyymmdd23') as int)) and                                -- TX (00-23)
                                      (dk_barb_min_end_datehour_dim >= cast(dateformat(@varStartDate + 1, 'yyyymmdd00') as int)) then dateadd(second, -60, cast(@varStartDate + 1 as datetime))                                 -- TX + 1 (00)
                                   else barb_min_end_date_time_utc
                               end
                             ) + 60 as seconds_viewed
                    ,date(barb_min_start_date_time_utc_adj) as viewing_day_adj
                into uat_scenario0_syb
                from sk_prod.vespa_dp_prog_viewed_201304
               where dk_barb_min_start_datehour_dim >= cast(dateformat(@varStartDate - 1, 'yyyymmdd00') as int)                                           -- Scenario filter                                                    -- TX - 1 (00)
                 and dk_barb_min_start_datehour_dim <= cast(dateformat(@varStartDate, 'yyyymmdd23') as int)                                       -- Scenario filter                                                            -- TX (23)
                 and cb_key_household is not null                                                           -- Common filter
                 and cb_key_household > 0                                                                   -- Common filter
                 and capped_full_flag = 0                                                                   -- Common filter
                 and panel_id = 12                                                                          -- Common filter
            commit

            execute logger_add_event @varBuildId, 3, '2) Sybase data pulled'


            insert into uat_scenario0_daily_accounts
            select
                  'Sybase',
                  @varStartDate,
                  null,
                  account_number,
                  Live_Recorded,
                  sum(seconds_viewed) as duration
              from uat_scenario0_syb
             where viewing_day_adj = @varStartDate
             group by account_number, Live_Recorded
            commit

            execute logger_add_event @varBuildId, 3, '3) Sybase cummary created'


                -- ########################################################################################
                -- ##### VIQ records #####
            if object_id('uat_scenario0_star') is not null drop table uat_scenario0_star
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
             where viewing_start_date_key >= cast(dateformat(@varStartDate, 'yyyymmdd00') as int)                -- Scenario filter
               and viewing_start_date_key <= cast(dateformat(@varStartDate, 'yyyymmdd23') as int)                -- Scenario filter
               and cb_key_household is not null                                                           -- Common filter
               and cb_key_household > 0                                                                   -- Common filter
            commit

            execute logger_add_event @varBuildId, 3, '4) VIQ data pulled'


            insert into uat_scenario0_daily_accounts
            select
                  'VIQ',
                  @varStartDate,
                  null,
                  account_number,
                  case
                    when viewing_type = 'Live' then 'LIVE'
                      else 'RECORDED'
                  end as Live_Recorded,
                  sum(viewed_duration) as duration
              from uat_scenario0_star
             group by account_number, Live_Recorded
            commit

            execute logger_add_event @varBuildId, 3, '5) VIQ cummary created'


                -- ########################################################################################
            set @varStartDate = @varStartDate + 1

        end


        -- ########################################################################################
        -- ##### Data match #####
    if object_id('uat_scenario0_syb_viq_comp') is not null drop table uat_scenario0_syb_viq_comp
    select
          coalesce(syb.viewing_day, viq.viewing_day) as viewing_day,
          trim(case
                 when syb.account_number is null then 'VIQ only'
                 when viq.account_number is null then 'Syb only'
                 when syb.duration <> viq.duration then 'Both (different)'
                   else 'Both (same)'
               end) as Source,
          coalesce(syb.account_number, viq.account_number) as account_number,
          coalesce(syb.Live_Recorded, viq.Live_Recorded) as Live_Recorded,
          case when syb.duration is null then -1 else syb.duration end as Syb_Duration,
          case when viq.duration is null then -1 else viq.duration end as VIQ_Duration
      into uat_scenario0_syb_viq_comp
      from (select * from uat_scenario0_daily_accounts where Source = 'Sybase') syb
            full join
           (select * from uat_scenario0_daily_accounts where Source = 'VIQ') viq
        on syb.account_number = viq.account_number
       and syb.Live_Recorded = viq.Live_Recorded
       and syb.viewing_day = viq.viewing_day
    commit


        -- ########################################################################################
        -- ##### Summaries #####
    select
          Source,
          viewing_day,
          Live_Recorded,
          count(*) as Cnt,
          count(distinct Account_Number) as Cnt_Accounts,
          sum(Syb_Duration) as Syb_Duration,
          sum(VIQ_Duration) as VIQ_Duration
      from uat_scenario0_syb_viq_comp
     group by Source, viewing_day, Live_Recorded
     order by 1, 2, 3

    select
          Source,
          viewing_day,
          count(*) as Cnt,
          count(distinct Account_Number) as Cnt_Accounts,
          sum(Syb_Duration) as Syb_Duration,
          sum(VIQ_Duration) as VIQ_Duration
      from uat_scenario0_syb_viq_comp
     group by Source, viewing_day
     order by 1, 2


        -- ########################################################################################
    execute logger_get_latest_job_events 'VIQ_UAT', 4

end










