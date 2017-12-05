/*###############################################################################
# Created on:   18/07/2013
# Created by:   Mandy Ng (MNG)
# Description:  VESPA Aggregations - viewing data extract
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 1.0 - cleaning Channel Mapping data
#               STEP 2.0 - creating view to filter relevant records
#               STEP 3.0 - pulling relevant viewing records for the universe
#               STEP 3.1 - calculating total viewing duration for Programme Instance
#               STEP 4.0 - creating viewing data sample
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - sk_prod.vespa_dp_prog_viewed_YYYYMM
#     - VESPA_Shared.VAggr_01_Account_Attributes base
#     - VESPA_Analysts.Channel_Map_Prod_Service_Key_Attributes
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 28/06/2013  MNG   Initial version
# 10/07/2013  MNG   Adapt to the current weighting table (sk_prod.viq_viewing_data_scaling)
# 19/07/2013  MNG   Adapt to Don's package movement table
# 25/07/2013  SBE   Include additional filtering criteria
# 26/08/2013  SBE   Parametrised, made period independent, naming change: Segm => Aggr
# 17/09/2013  SBE   Procedure created
# 22/10/2013  SBE   Input parameter "Manual start date" added to enable easy process restarts
# 28/10/2013  SBE   - Genre flags added
#                   - Customer Attributes tables sourced now from VESPA_Shared
# 14/11/2013  SBE   - Top 20 programmes flags created
# 21/02/2014  SBE   Removed reference to "bednaszs" schema
# 13/05/2014  ABA   Made EE Rebranding changes to channel_type(s) when selecting from channel mapping
# 23/05/2014  ABA   Made EE Rebranding changes to column names
# 24/06/2014  ABA   Expanded to include panel 11 (Broadband reporting panel)
#
###############################################################################*/


if object_id('VAggr_2_Viewing_Data') is not null then drop procedure VAggr_2_Viewing_Data end if;
create procedure VAggr_2_Viewing_Data
      @parPeriodKey             bigint,
      @parManualStartDate       date = null,
      @parSourceTable           varchar(50),
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

      declare @varBuildId                     bigint              -- Logger ID (so all builds end up in same queue)
      declare @varProcessIdentifier           varchar(20)         -- Logger - process ID
      declare @varSQL                         varchar(25000)
      declare @varStartDate                   date
      declare @varEndDate                     date
      declare @varEventStartHour              int
      declare @varEventEndHour                int

      set @varProcessIdentifier        = 'VAggr_2_Vw_Data_v01'

      select
            @varStartDate = date(Period_Start),
            @varEndDate   = date(Period_End)
        from VESPA_Shared.Aggr_Period_Dim
       where Period_Key = @parPeriodKey

      if (@parManualStartDate is not null)
          set @varStartDate = @parManualStartDate

      if (@parBuildId is not null)
          set @varBuildId = @parBuildId


        -- ###############################################################################
        -- ##### Create logger event                                                 #####
        -- ###############################################################################
      if (@parBuildId is null)
          execute logger_create_run @varProcessIdentifier, @parRefreshIdentifier, @varBuildId output

      execute logger_add_event @varBuildId, 3, '####### VESPA Aggregations [Viewing data] - process started #######', null
      execute logger_add_event @varBuildId, 3, '>>>>> Step 0.1: Preparing environment <<<<<', null
      execute logger_add_event @varBuildId, 3, 'Process identifier: ' || @varProcessIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Refresh identifier: ' || @parRefreshIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Build ID: ' || @varBuildId, null
      execute logger_add_event @varBuildId, 3, 'Period: ' || dateformat(@varStartDate, 'dd/mm/yyyy')  || ' - ' || dateformat(@varEndDate, 'dd/mm/yyyy'), null



      -- ##############################################################################################################
      -- ##### STEP 1.0 - cleaning Channel Mapping data                                                           #####
      -- ##############################################################################################################
/*
      execute logger_add_event @varBuildId, 3, '>>>>> Step 1.0: Cleaning Channel Mapping data  <<<<<', null

      truncate table VAggr_02_Channel_Mapping

      insert into VAggr_02_Channel_Mapping
        select
              *
          into VAggr_02_Channel_Mapping
          from kaganov.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES_25_07
      commit

        -- Some manual updates/data cleansing
      update VAggr_02_Channel_Mapping
         set Effective_To = '2011-05-02 06:00:25.000'
       where Effective_From = '2012-01-01 06:00:25.000'
         and Service_Key = 3548
      commit

      update VAggr_02_Channel_Mapping
         set channel_type = 'NR - FTA'
       where Service_Key = 3410
      commit

      execute logger_add_event @varBuildId, 3, 'Channel Mapping table created', null
*/



      -- ##############################################################################################################
      -- ##### STEP 2.0 - creating view to filter relevant records                                                #####
      -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 2.0: Creating view to filter relevant records  <<<<<', null

      drop view if exists v_VAggr_02_Viewing_Events

      set @varSQL = '
                      create view v_VAggr_02_Viewing_Events as
                        select
                              vw.pk_viewing_prog_instance_fact,
                              vw.account_number,
                              vw.subscriber_id,
                              vw.service_key,
                              vw.dk_programme_instance_dim,

                              vw.Genre_Description,
                              vw.Sub_Genre_Description,

                              cm.Channel_Type,
                              cm.Format,

                              vw.type_of_viewing_event,
                              cast(case
                                     when vw.live_recorded = ''LIVE'' then 0
                                       else 1
                                    end as bit)                         as Playback_Flag,
                              vw.reported_playback_speed,

                              datediff(second, vw.broadcast_start_date_time_utc, vw.broadcast_end_date_time_utc)
                                                                        as Prog_Instance_Broadcast_Duration,

                              vw.dk_event_start_datehour_dim,
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

                              date(event_start_date_time)               as event_start_date,
                              date(event_end_date_time)                 as event_end_date,
                              date(instance_start_date_time)            as instance_start_date,
                              date(instance_end_date_time)              as instance_end_date,

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
                                    as bigint)                        as event_duration_capped,
                              case
                                when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
                                  else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
                              end                                     as instance_duration

                          from sk_prod.##^1^## vw
                                  left join VESPA_Analysts.Channel_Map_Prod_Service_Key_Attributes cm     on vw.Service_Key = cm.Service_Key
                                                                                                         and cm.Effective_From < ''' || @varEndDate || '''
                                                                                                         and cm.Effective_To >= ''' || @varEndDate || '''
                                  --left join VAggr_02_Channel_Mapping cm                                 on vw.Service_Key = cm.Service_Key
                                  --                                                                     and cm.Effective_To = ''2999-12-31 00:00:00.000''
                         where capped_full_flag = 0
                           and panel_id in (11,12)
                           and instance_start_date_time_utc < instance_end_date_time_utc              -- Remove 0sec instances
                           and (reported_playback_speed is null or reported_playback_speed = 2)
                           and broadcast_start_date_time_utc >= dateadd(hour, -(24*28), event_start_date_time_utc)
                           and account_number is not null
                           and subscriber_id is not null
                           and type_of_viewing_event in (''HD Viewing Event'', ''Sky+ time-shifted viewing event'', ''TV Channel Viewing'', ''Other Service Viewing Event'')
                    '
      execute( replace(@varSQL, '##^1^##', @parSourceTable) )

      execute logger_add_event @varBuildId, 3, 'View created', null



      -- ##############################################################################################################
      -- ##### STEP 3.0 - pulling relevant viewing records for the universe                                       #####
      -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 3.0: Pulling relevant viewing records for the universe  <<<<<', null

      if (@parManualStartDate is null)
        begin
            truncate table VAggr_02_Viewing_Events
            execute logger_add_event @varBuildId, 3, 'Viewing data table emptied', null
        end


      while @varStartDate <= @varEndDate
          begin

              set @varEventStartHour    = (dateformat(@varStartDate, 'yyyymmdd00'))
              set @varEventEndHour      = (dateformat(@varStartDate, 'yyyymmdd23'))

              delete from VAggr_02_Viewing_Events
               where Event_Start_Date = @varStartDate
              commit

              execute logger_add_event @varBuildId, 3, 'Viewing data rows for current period removed', @@rowcount


              insert into VAggr_02_Viewing_Events
                select
                        -- Keys
                      vw.pk_viewing_prog_instance_fact,
                      base.Account_Number,
                      vw.Subscriber_Id,
                      vw.Service_Key,

                        -- Dates/durations
                      vw.Event_Start_Date,
                      vw.Instance_Start_Date,
                      vw.Instance_Start_Date_Time,
                      vw.Instance_End_Date_Time,
                      vw.Instance_Duration,

                        -- Programme instance viewing/broadcast
                      vw.Dk_Programme_Instance_Dim                            as Prog_Instance_Id,
                      vw.Prog_Instance_Broadcast_Duration,
                      0                                                       as Prog_Instance_Viewed_Duration,

                        -- Flags
                      vw.Playback_Flag as F_Playback,
                      case when (vw.Format = 'HD') then 1 else 0 end          as F_Format_HD,
                      case when (vw.Format = 'SD') then 1 else 0 end          as F_Format_SD,
                      case when (vw.Format = '3D') then 1 else 0 end          as F_Format_3D,

                      case
                        when vw.Channel_Type in ('Retail - Original', 'Retail - Original (NI only)') then 1
                          else 0
                      end                                                     as F_CType_Original_Pay,
                      case
                        when vw.Channel_Type in ('Retail - Variety', 'Retail - Original', 'Retail - Original (NI only)') then 1
                          else 0
                      end                                                     as F_CType_Variety_Pay,
                      case
                        when vw.Channel_Type in ('Retail - Family', 'Retail - Variety', 'Retail - Original',
                                                 'Retail - Original (NI only)', 'Retail - 3D') then 1
                          else 0
                      end                                                     as F_CType_Family_Pay,

                      case
                        when vw.Channel_Type in ('Retail - Original', 'Retail - Variety', 'Retail - Family',
                                                 'Retail - Original (NI only)', 'Retail - 3D') then 1
                          else 0
                      end                                                     as F_CType_O_V_F_Pay,
                      case
                        when vw.Channel_Type in ('NR - FTA', 'NR - Automatically Entitled', 'NR - FTA (Sky)', 'NR - Regional Variant') then 1
                          else 0
                      end                                                     as F_CType_O_V_F_FTA,
                      case
                        when vw.Channel_Type in ('Retail - Original', 'Retail - Variety', 'Retail - Family',
                                                 'Retail - Original (NI only)', 'Retail - 3D',
                                                 'NR - FTA', 'NR - Automatically Entitled', 'NR - FTA (Sky)', 'NR - Regional Variant') then 1
                          else 0
                      end                                                     as F_CType_O_V_F_Any,
                      case
                        when vw.Channel_Type in ('Retail - Movies', 'Retail - Movies + HD Pack') then 1
                          else 0
                      end                                                     as F_CType_Retail_Movies,
                      case
                        when vw.Channel_Type in ('Retail - ALC / Movies Pack + Family') then 1
                          else 0
                      end                                                     as F_CType_Retail_ALC_Movies_Pack,
                      case
                        when vw.Channel_Type in ('Retail - Sports', 'Retail - Sports + HD Pack') then 1
                          else 0
                      end                                                     as F_CType_Retail_Sports,
                      case
                        when vw.Channel_Type in ('Retail - ALC', 'Retail - ALC + Family') then 1
                          else 0
                      end                                                     as F_CType_Retail_ALa_Carte,
                      case
                        when vw.Channel_Type in ('NR - Conditional Access', 'NR - Pay-per-view') then 1
                          else 0
                      end                                                     as F_CType_3rd_Party,
                      case
                        when vw.Channel_Type in ('Retail - 3D', 'Retail - Adult Nightly', 'Retail – ALC', 'Retail - ALC / Movies Pack + Family',
                                                 'Retail - ALC + Family', 'Retail - Original', 'Retail - Original (NI only)',
                                                 'Retail - Variety', 'Retail - Family', 'Retail – Movies', 'Retail - Movies + HD Pack',
                                                 'Retail - Pay-per-night', 'Retail - Pay-per-view', 'Retail - PPV HD', 'Retail - ROI Bonus',
                                                 'Retail – Sports', 'Retail - Sports + HD Pack') then 1
                          else 0
                      end                                                     as F_CType_Pay,

                      case
                        when vw.Genre_Description = 'Sports' then 1
                          else 0
                      end                                                     as F_Genre_Sport,


                        -- ##### Genres #####
                        -- Individual genres (non-premium)
                      case
                        when vw.Genre_Description = 'Children' then 1
                          else 0
                      end                                                     as F_Genre_Non_Prem_Children,
                      case
                        when vw.Genre_Description = 'Movies' then 1
                          else 0
                      end                                                     as F_Genre_Non_Prem_Movies,
                      case
                        when vw.Genre_Description = 'News & Documentaries' or
                             (
                                vw.Genre_Description = 'Entertainment' and
                                vw.Sub_Genre_Description in ('Chat Show','Factual','Medical','Motors','Reviews','Technology')
                             ) then 1
                          else 0
                      end                                                     as F_Genre_Non_Prem_News_Documentaries,
                      case
                        when vw.Genre_Description = 'Sports' then 1
                          else 0
                      end                                                     as F_Genre_Non_Prem_Sports,
                      case
                        when vw.Sub_Genre_Description in ('Action', 'Sci-Fi') then 1
                          else 0
                      end                                                     as F_Genre_Non_Prem_Action_SciFi,
                      case
                        when ( vw.Genre_Description = 'Entertainment' and vw.Sub_Genre_Description in ('Antiques','Art&Lit','Arts','Ballet','Cooking','Fashion','Gardening',
                                                                                                       'Home','Lifestyle','Magazine','Opera','Travel') ) or
                             (vw.Genre_Description = 'Specialist' and vw.Sub_Genre_Description in ('Events', 'Shopping')) or
                             vw.Genre_Description = 'Music & Radio' then 1
                          else 0
                      end                                                     as F_Genre_Non_Prem_Arts_Lifestyle,
                      case
                        when (vw.Genre_Description = 'Entertainment' and vw.Sub_Genre_Description in ('Comedy','Game Shows','Animation')) or
                             (vw.Genre_Description = 'Specialist' and vw.Sub_Genre_Description = 'Gaming') then 1
                          else 0
                      end                                                     as F_Genre_Non_Prem_Comedy_GameShows,
                      case
                        when vw.Genre_Description = 'Entertainment' and vw.Sub_Genre_Description in ('Detective','Drama','Soaps') then 1
                          else 0
                      end                                                     as F_Genre_Non_Prem_Drama_Crime,

                        -- Individual genres (Movies premium)
                      case
                        when vw.Genre_Description = 'Movies' and vw.Sub_Genre_Description in ('Action','Adventure','War','Western') then 1
                          else 0
                      end                                                     as F_Genre_Prem_Movies_Action_Adventure,
                      case
                        when vw.Genre_Description = 'Movies' and vw.Sub_Genre_Description in ('Comedy') then 1
                          else 0
                      end                                                     as F_Genre_Prem_Movies_Comedy,
                      case
                        when vw.Genre_Description = 'Movies' and vw.Sub_Genre_Description in ('Drama','Romance','Musical','Factual') then 1
                          else 0
                      end                                                     as F_Genre_Prem_Movies_Drama_Romance,
                      case
                        when vw.Genre_Description = 'Movies' and vw.Sub_Genre_Description in ('Family','Animation') then 1
                          else 0
                      end                                                     as F_Genre_Prem_Movies_Family,
                      case
                        when vw.Genre_Description = 'Movies' and vw.Sub_Genre_Description in ('Horror','Thriller') then 1
                          else 0
                      end                                                     as F_Genre_Prem_Movies_Horror_Thriller,
                      case
                        when vw.Genre_Description = 'Movies' and vw.Sub_Genre_Description in ('Fantasy','Sci-Fi') then 1
                          else 0
                      end                                                     as F_Genre_Prem_Movies_SciFi_Fantasy,

                        -- Individual genres (Sports premium)
                      case
                        when vw.Genre_Description = 'Sports' and vw.Sub_Genre_Description in ('American Football','Baseball','Basketball','Ice Hockey') then 1
                          else 0
                      end                                                     as F_Genre_Prem_Sports_American,
                      case
                        when vw.Genre_Description = 'Sports' and vw.Sub_Genre_Description in ('Boxing','Wrestling') then 1
                          else 0
                      end                                                     as F_Genre_Prem_Sports_Boxing_Wrestling,
                      case
                        when vw.Genre_Description = 'Sports' and vw.Sub_Genre_Description in ('Cricket') then 1
                          else 0
                      end                                                     as F_Genre_Prem_Sports_Cricket,
                      case
                        when vw.Genre_Description = 'Sports' and vw.Sub_Genre_Description in ('Football') then 1
                          else 0
                      end                                                     as F_Genre_Prem_Sports_Football,
                      case
                        when vw.Genre_Description = 'Sports' and vw.Sub_Genre_Description in ('Golf') then 1
                          else 0
                      end                                                     as F_Genre_Prem_Sports_Golf,
                      case
                        when vw.Genre_Description = 'Sports' and vw.Sub_Genre_Description in ('Motor Sport', 'Extreme') then 1
                          else 0
                      end                                                     as F_Genre_Prem_Sports_Motor_Extreme,
                      case
                        when vw.Genre_Description = 'Sports' and vw.Sub_Genre_Description in ('Rugby') then 1
                          else 0
                      end                                                     as F_Genre_Prem_Sports_Rugby,
                      case
                        when vw.Genre_Description = 'Sports' and vw.Sub_Genre_Description = 'Tennis' then 1
                          else 0
                      end                                                     as F_Genre_Prem_Sports_Tennis,
                      case
                        when vw.Genre_Description = 'Sports' and vw.Sub_Genre_Description in ('Athletics', 'Watersports', 'Wintersports', 'Equestrian',
                                                                                              'Fishing', 'Racing', 'Darts', 'Snooker/Pool') then 1
                          else 0
                      end                                                     as F_Genre_Prem_Sports_Niche_Sports,

                        -- Individual channels
                      case
                        when vw.Service_Key in (2011) then 1
                          else 0
                      end                                                     as F_Channel_BBC_News,
                      case
                        when vw.Service_Key in (2075, 2076, 2073, 2106, 2105, 2002, 2155, 2102, 2005, 2156, 2004, 2082, 2153, 2152, 2154,
                                                2003, 2083, 2151, 2101, 2104, 2103, 2006, 2018, 2074, 2061, 2017, 2016, 2015, 2081, 2075) then 1
                          else 0
                      end                                                     as F_Channel_BBC1_BBC2_BBC3_BB4,
                      case
                        when vw.Service_Key in (3625, 3661, 3627, 3663) then 1
                          else 0
                      end                                                     as F_Channel_BT_Sports,
                      case
                        when vw.Service_Key in (2019) then 1
                          else 0
                      end                                                     as F_Channel_CBeebies,
                      case
                        when vw.Service_Key in (1666, 4075, 1621, 1623, 1624, 1626, 1622, 1625, 1667, 1670, 1672, 1673, 1675, 1671, 1674) then 1
                          else 0
                      end                                                     as F_Channel_Channel_4,
                      case
                        when vw.Service_Key in (1828, 4058, 1801, 1829, 1800, 1830, 1839) then 1
                          else 0
                      end                                                     as F_Channel_Channel_5,
                      case
                        when vw.Service_Key in (2306, 3809, 2320, 2376) then 1
                          else 0
                      end                                                     as F_Channel_Dave,
                      case
                        when vw.Service_Key in (2401, 4003, 2407, 3760, 2408, 2410, 2403, 1353, 2406, 1351, 4548, 2405, 2409, 2404) then 1
                          else 0
                      end                                                     as F_Channel_Discovery_All,
                      case
                        when vw.Service_Key in (1875, 4086, 1891, 1879) then 1
                          else 0
                      end                                                     as F_Channel_History,
                      case
                        when vw.Service_Key in (6240, 6260, 6272, 6089, 6180, 6381, 6128, 6110, 6011, 6010, 6015, 6300, 6145, 6200, 6130,
                                                6355, 6504, 6503, 6505, 6502, 6000, 6155, 6141, 6143, 6140, 6142, 6365, 6325, 6371, 6210,
                                                6220, 6390, 6391, 6126, 6230, 6020, 6012, 6030, 6127, 6040, 6125, 6065, 6161, 6160, 6532,
                                                6241, 6533, 6261, 6534, 6274) then 1
                          else 0
                      end                                                     as F_Channel_ITV_All,
                      case
                        when vw.Service_Key in (2501, 3831, 2508, 2509, 2507, 2516, 2506, 2521, 3508, 4006, 2512, 2503, 2515) then 1
                          else 0
                      end                                                     as F_Channel_MTV,
                      case
                        when vw.Service_Key in (1847, 4025, 1806, 4031, 1822) then 1
                          else 0
                      end                                                     as F_Channel_NatGeo_All,
                      case
                        when vw.Service_Key in (1402, 4061, 1401, 1403) then 1
                          else 0
                      end                                                     as F_Channel_Sky_1,
                      case
                        when vw.Service_Key in (1412, 4053, 1413, 1414) then 1
                          else 0
                      end                                                     as F_Channel_Sky_Atlantic,
                      case
                        when vw.Service_Key in (2201, 4066, 4335, 2203, 4334, 2205) then 1
                          else 0
                      end                                                     as F_Channel_Sky_Living,
                      case
                        when vw.Service_Key in (1404, 4050, 4645, 1406) then 1
                          else 0
                      end                                                     as F_Channel_Sky_News,
                      case
                        when vw.Service_Key in (1301, 1701, 4002) then 1
                          else 0
                      end                                                     as F_Channel_Sky_Sports_1,
                      case
                        when vw.Service_Key in (1302, 4081, 4081) then 1
                          else 0
                      end                                                     as F_Channel_Sky_Sports_2,
                      case
                        when vw.Service_Key in (1306, 3835) then 1
                          else 0
                      end                                                     as F_Channel_Sky_Sports_F1,
                      case
                        when vw.Service_Key in (2617, 3810, 5880, 2616) then 1
                          else 0
                      end                                                     as F_Channel_Watch


                  from VESPA_Shared.Aggr_Account_Attributes base
                          inner join v_VAggr_02_Viewing_Events vw                      on base.Account_Number = vw.Account_Number
                                                                                      and base.Period_Key = @parPeriodKey
                                                                                      and vw.dk_event_start_datehour_dim between @varEventStartHour and @varEventEndHour
                                                                                      and vw.event_duration_capped > 6
                                                                                      and (
                                                                                            vw.type_of_viewing_event in ('HD Viewing Event', 'TV Channel Viewing')
                                                                                            or
                                                                                            (
                                                                                              vw.type_of_viewing_event = 'Other Service Viewing Event'
                                                                                              and
                                                                                              vw.Channel_Type in ('Retail - Pay-per-night', 'Retail - Pay-per-view',
                                                                                                                  'Retail - PPV HD', 'NR - Pay-per-view')
                                                                                            )
                                                                                            or
                                                                                            (
                                                                                              vw.type_of_viewing_event = 'Sky+ time-shifted viewing event'
                                                                                              and
                                                                                              vw.Channel_Type <> 'NR - FTA - Radio'
                                                                                            )
                                                                                          )

              commit

              execute logger_add_event @varBuildId, 3, 'Day processed: ' || dateformat(@varStartDate, 'dd/mm/yyyy'), @@rowcount

              set @varStartDate = @varStartDate + 1
          end

      execute logger_add_event @varBuildId, 3, 'Viewing data created', null



      -- ##############################################################################################################
      -- ##### STEP 3.1 calculating total viewing duration for Programme Instance                                 #####
      -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 3.1: Calculating total viewing duration for Programme Instance <<<<<', null

      if object_id('VAggr_tmp_Prog_Instance_Summary') is not null drop table VAggr_tmp_Prog_Instance_Summary
      select
            Account_Number,
            Prog_Instance_Id,
            sum(Instance_Duration) as Total_Viewing_Duration
        into VAggr_tmp_Prog_Instance_Summary
        from VAggr_02_Viewing_Events
       group by Account_Number, Prog_Instance_Id
      commit

      create   hg index idx01 on VAggr_tmp_Prog_Instance_Summary(Account_Number)
      create   hg index idx02 on VAggr_tmp_Prog_Instance_Summary(Prog_Instance_Id)

      execute logger_add_event @varBuildId, 3, 'Programme instance summmaries created', @@rowcount


      set option query_temp_space_limit = 0

      update VAggr_02_Viewing_Events base
         set base.Prog_Instance_Viewed_Duration = det.Total_Viewing_Duration
        from VAggr_tmp_Prog_Instance_Summary det
       where base.Account_Number = det.Account_Number
         and base.Prog_Instance_Id = det.Prog_Instance_Id
         and base.Prog_Instance_Id > 0
      commit

      execute logger_add_event @varBuildId, 3, 'Viewing data records updated', @@rowcount



      -- ##############################################################################################################
      -- ##### STEP 4.0 - creating viewing data sample                                                            #####
      -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 3.1: Creating viewing data sample <<<<<', null
      declare @varMinPk         bigint
      declare @varMaxPk         bigint
      declare @varRecCount      bigint
      declare @varTargetCount   bigint
      declare @varMaxRatio      decimal(15, 10)

      set @varTargetCount = 500000
      execute logger_add_event @varBuildId, 3, 'Target records count: ' || @varTargetCount, null

      set @varMinPk       = (select min(pk_viewing_prog_instance_fact) from VAggr_02_Viewing_Events) + 1
      set @varMaxPk       = (select max(pk_viewing_prog_instance_fact) from VAggr_02_Viewing_Events)
      set @varRecCount    = (select count(*) from VAggr_02_Viewing_Events)
      set @varMaxRatio    = (1.0 * @varTargetCount / @varRecCount)

      truncate table VAggr_02_Viewing_Events_Sample
      insert into VAggr_02_Viewing_Events_Sample
        select
              *
          from VAggr_02_Viewing_Events
         where (1.0 * (pk_viewing_prog_instance_fact -  @varMinPk) / @varMaxPk) between 0 and @varMaxRatio   -- Not TRULY random but "should do" for this purpose
      commit


      set @varTargetCount = (select count(*) from VAggr_02_Viewing_Events_Sample)
      execute logger_add_event @varBuildId, 3, 'Actual records count: ' || @varTargetCount, null
      execute logger_add_event @varBuildId, 3, 'Sample viewing data created', @@rowcount



        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '####### VESPA Aggregations [Viewing data] - process completed #######', null
      execute logger_add_event @varBuildId, 3, ' ', null
      commit

end;




