/*###############################################################################
# Created on:   07/08/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Capping Metrics Report - Universe preparation
#                 This procedure prepares required data which is subsequently used
#                 in other modules
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 1.0 - universe preparation
#               STEP 2.1 - scaling variables calculation
#               STEP 2.2 - other variables calculation
#               STEP 3.0 - viewing data preparation
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - Vespa_Analysts.SC2_Segments_Lookup_vX_X (relevant version)
#     - sk_prod.value_segments_five_yrs
#     - sk_prod.experian_consumerview cv
#     - sk_prod.playpen_consumerview_person_and_household
#     - sk_prod.cust_single_account_view
#     - sk_prod.bb_postcode_to_exchange
#     - sk_prod.broadband_postcode_exchange
#     - sk_prod.easynet_rollout_data det
#     - sk_prod.sky_player_usage_detail
#     - Vespa_Analysts.Vespa_Daily_Augs_YYYYMMDD (for the period)
#     - Vespa_Analysts.Channel_Map_Dev_Service_Key_Attributes
#
# => Procedures required:
#     - "vespa_getSourceDataView_v01" -
            location: \Vespa\capabilities\_process_definitions\proc__vespa_getSourceDataView.sql
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 07/08/2013  SBE   v01 - initial version
#
###############################################################################*/


if object_id('CAP_Metrics_Rep_Universe_Preparation') is not null then drop procedure CAP_Metrics_Rep_Universe_Preparation end if;
create procedure CAP_Metrics_Rep_Universe_Preparation
      @parStartDate             date,                -- Calculation period start date
      @parEndDate               date,                -- Calculation period end date
      @parRefreshIdentifier     varchar(40) = '',    -- Logger - refresh identifier
      @parBuildId               bigint = null        -- Logger - add events to an existing logger process
as
begin

        -- ##############################################################################################################
        -- ##### STEP 0.1 - preparing environment                                                                   #####
        -- ##############################################################################################################

        -- ###############################################################################
        -- ##### Define and set variables                                            #####
        -- ###############################################################################

      declare @varReportingStartDate          date
      declare @varReportingEndDate            date
      declare @varBuildId                     bigint              -- Logger ID (so all builds end up in same queue)
      declare @varProcessIdentifier           varchar(20)         -- Logger - process ID
      declare @varSQL                         varchar(15000)
      declare @varCurrentDate                 date                -- Temp vars
      declare @varTmpDate                     date                -- Temp vars

      set @varReportingStartDate = @parStartDate

      set @varReportingEndDate = @parEndDate

      set @varProcessIdentifier        = 'CAPMetRep_Univ_v01'

      if (@parBuildId is not null)
          set @varBuildId = @parBuildId


        -- ###############################################################################
        -- ##### Create logger event                                                 #####
        -- ###############################################################################
      if (@parBuildId is null)
          execute logger_create_run @varProcessIdentifier, @parRefreshIdentifier, @varBuildId output

      execute logger_add_event @varBuildId, 3, '####### Capping Metrics Report [Universe] - process started #######', null
      execute logger_add_event @varBuildId, 3, '>>>>> Step 0.1: Preparing environment <<<<<', null
      execute logger_add_event @varBuildId, 3, 'Process identifier: ' || @varProcessIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Refresh identifier: ' || @parRefreshIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Build ID: ' || @varBuildId, null
      execute logger_add_event @varBuildId, 3, 'User context: ' || @varUsername, null
      execute logger_add_event @varBuildId, 3, 'Period: ' || dateformat(@varReportingStartDate, 'dd/mm/yyyy') || ' - ' || dateformat(@varReportingEndDate, 'dd/mm/yyyy'), null



        -- ##############################################################################################################
        -- ##### STEP 1.0 - universe preparation                                                                    #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 1.0: Universe preparation <<<<<', null

      if object_id('CAP_Metrics_Rep_tmp_Universe') is not null drop table CAP_Metrics_Rep_tmp_Universe

      create table CAP_Metrics_Rep_tmp_Universe (
          Id_Key                  bigint          identity,
          Account_Number          varchar(20)     default null,
          CB_Key_Household        bigint          default null,
          Event_Date              date            default null,
          Scaling_Segment_Id      bigint          default 0,

          Universe                varchar(20)     default '(not scaled)',
          Region                  varchar(20)     default '(not scaled)',
          HH_Composition          varchar(30)     default '(not scaled)',
          Tenure                  varchar(20)     default '(not scaled)',
          TV_Package              varchar(20)     default '(not scaled)',
          Box_Type                varchar(30)     default '(not scaled)',

          Value_Segment           varchar(15)     default 'Bedding In',
          Experian_Mosaic	        varchar(30)     default 'U: Unknown MOSAIC',
          Financial_Strategy_Segm varchar(30)     default 'U: Unknown FSS',
          OnOffNet_Area           varchar(20)     default 'Unknown',
          SkyGO_User              varchar(20)     default 'non-SkyGo user',

      )

      create hg index idx1 on CAP_Metrics_Rep_tmp_Universe(Account_Number)
      create hg index idx2 on CAP_Metrics_Rep_tmp_Universe(CB_Key_Household)
      create date index idx3 on CAP_Metrics_Rep_tmp_Universe(Event_Date)
      create unique index idx4 on CAP_Metrics_Rep_tmp_Universe(Account_Number, Event_Date)


      set @varCurrentDate = @varReportingStartDate

        -- Create universe based on weekly snapshots
      while @varCurrentDate <= @varReportingEndDate
          begin

              if object_id('Vespa_Analysts.Vespa_Daily_Augs_' || dateformat(@varCurrentDate, 'yyyymmdd')) is not null
                begin
                    set @varSQL = '
                                    insert into CAP_Metrics_Rep_tmp_Universe
                                           (Account_Number, Event_Date)
                                      select distinct
                                            Account_Number,
                                            ''' || @varCurrentDate || '''
                                        from Vespa_Analysts.Vespa_Daily_Augs_' || dateformat(@varCurrentDate, 'yyyymmdd') || '
                                    commit

                                    execute logger_add_event ' || @varBuildId || ', 3, ''Day processed: ' || dateformat(@varCurrentDate, 'dd/mm/yyyy') || ''', @@rowcount

                                  '

                    execute (@varSQL)

                end

              set @varCurrentDate = @varCurrentDate + 1
          end


        -- Append CB_KEY_HOUSEHOLD
      update CAP_Metrics_Rep_tmp_Universe base
         set base.Cb_Key_Household    = det.Cb_Key_Household
        from sk_prod.Cust_Single_Account_View det
       where base.Account_Number = det.Account_Number
      commit

      execute logger_add_event @varBuildId, 3, 'Cb Key Household updated', @@rowcount


        -- Append scaling segment id information
      update CAP_Metrics_Rep_tmp_Universe base
         set base.Scaling_Segment_Id    = intv.Scaling_Segment_Id
        from Vespa_Analysts.SC2_Intervals intv
       where base.Account_Number = intv.Account_Number
         and base.Event_Date >= intv.reporting_starts
         and base.Event_Date <= intv.reporting_ends
      commit

      execute logger_add_event @varBuildId, 3, 'Scaling Segment Id updated', @@rowcount



        -- ##############################################################################################################
        -- ##### STEP 2.1 - scaling variables calculation                                                           #####
        -- ##############################################################################################################
      update CAP_Metrics_Rep_tmp_Universe base
         set
             base.Universe          = det.Universe,
             base.Region            = det.Isba_TV_Region,
             base.HH_Composition    = case det.HHComposition
                                        when '00' then 'Families'
                                        when '01' then 'Extended families'
                                        when '02' then 'Extended households'
                                        when '03' then 'Pseudo families'
                                        when '04' then 'Single males'
                                        when '05' then 'Single females'
                                        when '06' then 'Male homesharers'
                                        when '07' then 'Female homesharers'
                                        when '08' then 'Mixed homesharers'
                                        when '09' then 'Abbreviated male families'
                                        when '10' then 'Abbreviated female families'
                                        when '11' then 'Multi-occupancy dwellings'
                                        when 'U'  then 'Unclassified'
                                        when 'NS' then 'Unclassified'
                                      end,
             base.Tenure            = det.Tenure,
             base.TV_Package        = det.Package,
             base.Box_Type          = det.BoxType
        from Vespa_Analysts.SC2_Segments_Lookup_v2_1 det
       where base.Scaling_Segment_Id = det.Scaling_Segment_Id
      commit

      execute logger_add_event @varBuildId, 3, '>>>>> Step 2.1: Scaling variables calculation <<<<<', @@rowcount



        -- ##############################################################################################################
        -- ##### STEP 2.2 - other variables calculation                                                             #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 2.2: Other variables calculation <<<<<', null

        -- Value Segments
      if object_id('CAP_Metrics_Rep_tmp_Value_Segment_Dates') is not null drop table CAP_Metrics_Rep_tmp_Value_Segment_Dates

      create table CAP_Metrics_Rep_tmp_Value_Segment_Dates (
          Id_Key                  bigint          identity,
          Event_Date              date            default null,
          Value_Segment_Date      date            default null
      )

      create date index idx1 on CAP_Metrics_Rep_tmp_Value_Segment_Dates(Event_Date)
      create date index idx2 on CAP_Metrics_Rep_tmp_Value_Segment_Dates(Value_Segment_Date)

      set @varCurrentDate = @varReportingStartDate

        -- Create universe based on weekly snapshots
      while @varCurrentDate <= @varReportingEndDate
          begin

              set @varTmpDate = (select max(value_seg_date)
                                   from sk_prod.value_segments_five_yrs
                                  where value_seg_date <= @varCurrentDate)

              insert into CAP_Metrics_Rep_tmp_Value_Segment_Dates
                     (Event_Date, Value_Segment_Date)
                values (
                        @varCurrentDate,
                        @varTmpDate
                       )
              commit

              set @varCurrentDate = @varCurrentDate + 1
          end

      execute logger_add_event @varBuildId, 3, 'Value segment dates lookup completed', null


      update CAP_Metrics_Rep_tmp_Universe base
         set base.Value_Segment     = val.Value_Segment
        from CAP_Metrics_Rep_tmp_Value_Segment_Dates lk,
             sk_prod.value_segments_five_yrs val
       where base.Event_Date = lk.Event_Date
         and lk.Value_Segment_Date = val.value_seg_date
         and base.Account_Number = val.Account_Number
      commit

      execute logger_add_event @varBuildId, 3, 'Value segments updated', @@rowcount



        -- Consumer view data
      if object_id('CAP_Metrics_Rep_tmp_Consumer_View') is not null drop table CAP_Metrics_Rep_tmp_Consumer_View
      select
            cv.cb_row_id as consumerview_cb_row_id,
            cv.CB_Key_Individual,
            cv.CB_Key_Household,
            pp.p_head_of_household,
            cv.H_Mosaic_UK_Group,
            cv.H_FSS_Group,
            rank() over(partition by cv.CB_Key_Household order by pp.p_head_of_household desc, pp.exp_cb_key_db_person desc,
                                                                  cv.cb_row_id desc, pp.cb_row_id desc) as Rank_Sequence
        into CAP_Metrics_Rep_tmp_Consumer_View
        from sk_prod.experian_consumerview cv,
             sk_prod.playpen_consumerview_person_and_household pp
       where cv.exp_cb_key_db_individual = pp.exp_cb_key_db_individual
         and cv.cb_key_individual is not null
      commit

      create lf index idx1 on CAP_Metrics_Rep_tmp_Consumer_View(Rank_Sequence)
      delete from CAP_Metrics_Rep_tmp_Consumer_View where Rank_Sequence > 1

      create unique hg index idx2 on CAP_Metrics_Rep_tmp_Consumer_View(CB_Key_Household)
      create hg index idx3 on CAP_Metrics_Rep_tmp_Consumer_View(CB_Key_Individual)

      update CAP_Metrics_Rep_tmp_Universe base
         set base.Experian_Mosaic	          = case det.H_Mosaic_UK_Group
                                                when 'A' then 'A: Alpha Territory'
                                                when 'B' then 'B: Professional Rewards'
                                                when 'C' then 'C: Rural Solitude'
                                                when 'D' then 'D: Small Town Diversity'
                                                when 'E' then 'E: Active Retirement'
                                                when 'F' then 'F: Suburban Mindsets'
                                                when 'G' then 'G: Careers and Kids'
                                                when 'H' then 'H: New Homemakers'
                                                when 'I' then 'I: Ex-Council Community'
                                                when 'J' then 'J: Claimant Cultures'
                                                when 'K' then 'K: Upper Floor Living'
                                                when 'L' then 'L: Elderly Needs'
                                                when 'M' then 'M: Industrial Heritage'
                                                when 'N' then 'N: Terraced Melting Pot'
                                                when 'O' then 'O: Liberal Opinions'
                                                  else 'U: Unknown MOSAIC'
                                              end,
             base.Financial_Strategy_Segm   = case det.H_FSS_Group
                                                when 'A' then 'A: Successful Start'
                                                when 'B' then 'B: Happy Housemates'
                                                when 'C' then 'C: Surviving Singles'
                                                when 'D' then 'D: On The Breadline'
                                                when 'E' then 'E: Flourishing Families'
                                                when 'F' then 'F: Credit Hungry Families'
                                                when 'G' then 'G: Gilt Edged Lifestyles'
                                                when 'H' then 'H: Mid Life Affluence'
                                                when 'I' then 'I: Modest Mid Years'
                                                when 'J' then 'J: Advancing Status'
                                                when 'K' then 'K: Ageing Workers'
                                                when 'L' then 'L: Wealthy Retirement'
                                                when 'M' then 'M: Elderly Deprivation'
                                                  else 'U: Unknown FSS'
                                              end

        from CAP_Metrics_Rep_tmp_Consumer_View det
       where base.CB_Key_Household = det.CB_Key_Household
      commit

      execute logger_add_event @varBuildId, 3, 'Consumer view variables updated', @@rowcount


        -- On/Off net
      if object_id('CAP_Metrics_Rep_tmp_OnOff_Net') is not null drop table CAP_Metrics_Rep_tmp_OnOff_Net
      if object_id('CAP_Metrics_Rep_tmp_OnOff_Net_Exch_Lookup') is not null drop table CAP_Metrics_Rep_tmp_OnOff_Net_Exch_Lookup
      select
            account_number,
            min(cb_address_postcode) as Postcode,
            cast('Unknown' as varchar(20)) as OnOffNet_Area
        into CAP_Metrics_Rep_tmp_OnOff_Net
        from sk_prod.cust_single_account_view
       group by account_number
      commit

      update CAP_Metrics_Rep_tmp_OnOff_Net set Postcode = upper(replace(Postcode, ' ', ''))
      commit

      create unique hg index idx1 on CAP_Metrics_Rep_tmp_OnOff_Net(account_number)
      create hg index idx2 on CAP_Metrics_Rep_tmp_OnOff_Net(Postcode)


        -- 1) Get BB_POSTCODE_TO_EXCHANGE postcodes
      select
            Postcode,
            max(exchange_id) as exchID
        into #p2e
        from sk_prod.BB_POSTCODE_TO_EXCHANGE
       group by Postcode
      commit

      update #p2e set Postcode = upper(replace(Postcode, ' ', ''))
      commit

      create unique hg index idx1 on #p2e(Postcode)


        -- 2) Get BROADBAND_POSTCODE_EXCHANGE postcodes
      select
           cb_address_postcode as Postcode,
           max(mdfcode) as exchID
        into #bpe
        from sk_prod.broadband_postcode_exchange
       group by Postcode
      commit

      update #bpe set Postcode = upper(replace(Postcode, ' ', ''))
      commit

      create unique hg index idx1 on #bpe(Postcode)


        -- 3) Combine postcode lists taking BB_POSTCODE_TO_EXCHANGE exchange_id's where possible
      select
            coalesce(#p2e.Postcode, #bpe.Postcode) as Postcode,
            coalesce(#p2e.exchID, #bpe.exchID) as Exchange_ID,
            cast('OffNet Area' as varchar(20)) as OnOffNet_Area
        into CAP_Metrics_Rep_tmp_OnOff_Net_Exch_Lookup
        from #p2e full join #bpe on #bpe.Postcode = #p2e.Postcode
      commit

      create unique hg index idx1 on CAP_Metrics_Rep_tmp_OnOff_Net_Exch_Lookup(Postcode)

      drop table #p2e
      drop table #bpe


        -- 4) Update with latest Easynet exchange information
      update CAP_Metrics_Rep_tmp_OnOff_Net_Exch_Lookup base
         set OnOffNet_Area = 'OnNet Area'
        from sk_prod.easynet_rollout_data det
       where base.Exchange_ID = det.Exchange_ID
         and det.exchange_status = 'ONNET'
      commit


        -- Get account number list
      update CAP_Metrics_Rep_tmp_OnOff_Net base
         set base.OnOffNet_Area = det.OnOffNet_Area
        from CAP_Metrics_Rep_tmp_OnOff_Net_Exch_Lookup det
       where base.Postcode = det.Postcode
      commit

        -- Final update to the base table
      update CAP_Metrics_Rep_tmp_Universe base
         set base.OnOffNet_Area	  = det.OnOffNet_Area
        from CAP_Metrics_Rep_tmp_OnOff_Net det
       where base.Account_Number = det.Account_Number
      commit

      execute logger_add_event @varBuildId, 3, 'On/OffNet area variable updated', @@rowcount


        -- Sky GO user
      update CAP_Metrics_Rep_tmp_Universe base
         set base.SkyGO_User	  = 'SkyGo user'
        from (select
                    Account_Number,
                    Activity_Dt
                from sk_prod.sky_player_usage_detail det
               where Activity_Dt >= '2011-08-18'
               group by Account_Number, Activity_Dt) det
       where base.Account_Number = det.Account_Number
         and base.Event_Date >= det.Activity_Dt
      commit

      execute logger_add_event @varBuildId, 3, 'SkyGo user variable updated', @@rowcount



        -- ##############################################################################################################
        -- ##### STEP 3.0 - viewing data preparation                                                                #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 3.0: Viewing data preparation <<<<<', null

      execute vespa_getSourceDataView_v01 @varReportingStartDate, @varReportingEndDate, 'v_CAP_Metrics_Rep_tmp_Raw_Viewing', 1, @parRefreshIdentifier, @varBuildId
      execute logger_add_event @varBuildId, 3, 'Raw viewing data view created', null


      if object_id('CAP_Metrics_Rep_tmp_Viewing_Records') is not null drop table CAP_Metrics_Rep_tmp_Viewing_Records
      select
            vw.pk_viewing_prog_instance_fact,
            vw.account_number,
            vw.subscriber_id,
            vw.service_key,

            vw.type_of_viewing_event,
            cast(case
                   when vw.live_recorded = 'LIVE' then 1
                     else 0
                  end as bit)                         as live_flag,

            trim(case
                   when vw.live_recorded = 'LIVE' then 'Live'
                   when date(broadcast_start_date_time_utc) = date(vw.event_start_date_time_utc) then 'VOSDAL'
                     else 'Playback'
                 end) as Timeshift_Type,

            vw.dk_event_start_datehour_dim,
            date(vw.event_start_date_time_utc)        as event_start_date,
            vw.event_start_date_time_utc              as event_start_date_time,
            vw.event_end_date_time_utc                as event_end_date_time,
            case
              when vw.capping_end_date_time_utc is not null then vw.capping_end_date_time_utc
                else vw.event_end_date_time_utc
            end                                       as event_end_date_time_capped,

            vw.instance_start_date_time_utc           as instance_start_date_time,
            vw.instance_end_date_time_utc             as instance_end_date_time,
            case
              when vw.capped_partial_flag = 1 then vw.capping_end_date_time_utc
                else vw.instance_end_date_time_utc
            end                                       as instance_end_date_time_capped,

            cast(null as datetime)                    as instance_start_date_time_augs,
            cast(null as datetime)                    as instance_end_date_time_augs,

            vw.capped_full_flag,
            vw.capped_partial_flag,
            vw.duration                               as event_duration,
            cast(
                  datediff(
                           second,
                           vw.event_start_date_time_utc,
                           case
                             when vw.capping_end_date_time_utc is not null then vw.capping_end_date_time_utc
                               else vw.event_end_date_time_utc
                           end
                          )
                  as bigint)                          as event_duration_capped,

            datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
                                                      as instance_duration,
            case
              when vw.capped_full_flag = 1 then 0
              when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
                else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
            end                                       as instance_duration_capped,

            cast(0 as int)                            as instance_duration_capped_augs,

            cast('Unknown' as varchar(100))           as Channel_Pack,
            case
              when Genre_Description is null then 'Unknown'
                else Genre_Description
            end                                       as Programme_Genre,

            case
              when datepart(weekday, vw.event_start_date_time_utc) in (7, 1) then 'Weekend'
                else 'Weekday'
            end                                       as Weekday,

            case
              when datepart(hour, vw.event_start_date_time_utc) between  0 and  3 then '00:00 - 03:59'
              when datepart(hour, vw.event_start_date_time_utc) between  4 and  5 then '04:00 - 05:59'
              when datepart(hour, vw.event_start_date_time_utc) between  6 and  9 then '06:00 - 09:59'
              when datepart(hour, vw.event_start_date_time_utc) between 10 and 14 then '10:00 - 14:59'
              when datepart(hour, vw.event_start_date_time_utc) between 15 and 19 then '15:00 - 19:59'
              when datepart(hour, vw.event_start_date_time_utc) between 20 and 22 then '20:00 - 22:59'
              when datepart(hour, vw.event_start_date_time_utc) between 23 and 23 then '23:00 - 23:59'
                else 'Unknown'
            end                                       as Time_Division,

            case
              when ( datepart(hour, vw.event_start_date_time_utc) = 23 ) or
                   ( datepart(hour, vw.event_start_date_time_utc) = 0 and datepart(minute, vw.event_start_date_time_utc) <= 29 )
                                                                                  then 'Post Peak (00:00 - 00:30 and 23:00 - 23:59)'

              when datepart(hour, vw.event_start_date_time_utc) between  0 and  5 then 'Night Time (00:30 - 05:59)'
              when datepart(hour, vw.event_start_date_time_utc) between  6 and  8 then 'Breakfast Time (06:00 - 08:59)'
              when ( datepart(hour, vw.event_start_date_time_utc) between  9 and 16 ) or
                   ( datepart(hour, vw.event_start_date_time_utc) = 17 and datepart(minute, vw.event_start_date_time_utc) <= 29 )
                                                                                  then 'Day Time (09:00 - 17:29)'
              when datepart(hour, vw.event_start_date_time_utc) between 17 and  19 then 'Early Peak (17:30 - 19:59)'
              when datepart(hour, vw.event_start_date_time_utc) between 20 and  22 then 'Late Peak (20:00 - 22:59)'
                else 'Unknown'
            end                                       as Standard_Day_Parts

         into CAP_Metrics_Rep_tmp_Viewing_Records
         from v_CAP_Metrics_Rep_tmp_Raw_Viewing vw
        where (reported_playback_speed is null or reported_playback_speed = 2)
          and Duration > 6                                                  -- Maintain minimum event duration
          and instance_start_date_time_utc < instance_end_date_time_utc     -- Remove 0sec instances
          and Panel_id = 12
          and broadcast_start_date_time_utc >= dateadd(hour, -(24 * 28), event_start_date_time_utc)
          and dk_event_start_datehour_dim >= cast(dateformat(@varReportingStartDate, 'yyyymmdd00') as int)
          and dk_event_start_datehour_dim <= cast(dateformat(@varReportingEndDate, 'yyyymmdd23') as int)
          and type_of_viewing_event <> 'Non viewing event'
          and type_of_viewing_event is not null
          and account_number is not null
          and subscriber_id is not null
      commit

      execute logger_add_event @varBuildId, 3, 'Raw viewing data extract completed (data)', @@rowcount

      create hg index idx01 on CAP_Metrics_Rep_tmp_Viewing_Records(pk_viewing_prog_instance_fact)
      create hg index idx02 on CAP_Metrics_Rep_tmp_Viewing_Records(account_number)
      create hg index idx03 on CAP_Metrics_Rep_tmp_Viewing_Records(subscriber_id)
      create lf index idx04 on CAP_Metrics_Rep_tmp_Viewing_Records(service_key)
      create date index idx05 on CAP_Metrics_Rep_tmp_Viewing_Records(event_start_date)

      execute logger_add_event @varBuildId, 3, 'Raw viewing data extract completed (indices)', null


      update CAP_Metrics_Rep_tmp_Viewing_Records base
         set base.Channel_Pack = trim(cl.channel_pack)
        from Vespa_Analysts.Channel_Map_Dev_Service_Key_Attributes as cl
       where base.Service_Key = cl.Service_Key
      commit

      execute logger_add_event @varBuildId, 3, 'Channel pack updated', @@rowcount


      set @varCurrentDate = @varReportingStartDate

        -- Create universe based on weekly snapshots
      while @varCurrentDate <= @varReportingEndDate
          begin

              if object_id('Vespa_Analysts.Vespa_Daily_Augs_' || dateformat(@varCurrentDate, 'yyyymmdd')) is not null
                begin
                    set @varSQL = '
                                    update CAP_Metrics_Rep_tmp_Viewing_Records base
                                       set base.instance_start_date_time_augs = det.viewing_starts,
                                           base.instance_end_date_time_augs   = det.viewing_stops,
                                           base.instance_duration_capped_augs = datediff(second, base.instance_start_date_time, det.viewing_stops)
                                        from Vespa_Analysts.Vespa_Daily_Augs_' || dateformat(@varCurrentDate, 'yyyymmdd') || ' det
                                       where base.pk_viewing_prog_instance_fact = det.cb_row_id
                                    commit

                                    execute logger_add_event ' || @varBuildId || ', 3, ''AUG information updated - by ID (' || dateformat(@varCurrentDate, 'dd/mm/yyyy') || ')'', @@rowcount

                                    update CAP_Metrics_Rep_tmp_Viewing_Records base
                                       set base.instance_start_date_time_augs = det.viewing_starts,
                                           base.instance_end_date_time_augs   = det.viewing_stops,
                                           base.instance_duration_capped_augs = datediff(second, base.instance_start_date_time, det.viewing_stops)
                                        from Vespa_Analysts.Vespa_Daily_Augs_' || dateformat(@varCurrentDate, 'yyyymmdd') || ' det
                                       where base.Subscriber_Id = det.Subscriber_Id
                                         and base.instance_start_date_time = det.viewing_starts
                                         and base.instance_duration_capped_augs = 0             -- Only not updated records
                                    commit

                                    execute logger_add_event ' || @varBuildId || ', 3, ''AUG information updated - by time (' || dateformat(@varCurrentDate, 'dd/mm/yyyy') || ')'', @@rowcount

                                  '

                    execute (@varSQL)

                end

              set @varCurrentDate = @varCurrentDate + 1
          end



        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '####### Capping Metrics Report [Universe] - process completed #######', null
      execute logger_add_event @varBuildId, 3, ' ', null


end;


