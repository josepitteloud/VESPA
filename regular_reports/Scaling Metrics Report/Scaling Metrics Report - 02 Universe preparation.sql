/*###############################################################################
# Created on:   27/06/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Scaling Metrics Report - Universe preparation
#                 This procedure prepares required data which is subsequently used
#                 in other modules
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 1.0 - universe preparation
#               STEP 2.1 - scaling variables calculation
#               STEP 2.2 - other variables calculation
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - Vespa_Analysts.SC2_Sky_Base_Segment_Snapshots
#     - Vespa_Analysts.SC2_Intervals
#     - Vespa_Analysts.SC2_Weightings
#     - Vespa_Analysts.SC2_Segments_Lookup_vX_X (relevant version)
#     - sk_prod.value_segments_five_yrs
#     - sk_prod.experian_consumerview cv
#     - sk_prod.playpen_consumerview_person_and_household
#     - sk_prod.cust_single_account_view
#     - sk_prod.bb_postcode_to_exchange
#     - sk_prod.broadband_postcode_exchange
#     - sk_prod.easynet_rollout_data det
#     - sk_prod.sky_player_usage_detail
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 27/06/2013  SBE   v01 - initial version
#
###############################################################################*/


if object_id('SC_Metrics_Rep_Universe_Preparation') is not null then drop procedure SC_Metrics_Rep_Universe_Preparation end if;
create procedure SC_Metrics_Rep_Universe_Preparation
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

      set @varProcessIdentifier        = 'SCMetRep_Univ_v01'

      if (@parBuildId is not null)
          set @varBuildId = @parBuildId


        -- ###############################################################################
        -- ##### Create logger event                                                 #####
        -- ###############################################################################
      if (@parBuildId is null)
          execute logger_create_run @varProcessIdentifier, @parRefreshIdentifier, @varBuildId output

      execute logger_add_event @varBuildId, 3, '####### Scaling Metrics Report [Universe] - process started #######', null
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
      if object_id('SC_Metrics_Rep_tmp_Universe') is not null drop table SC_Metrics_Rep_tmp_Universe

      create table SC_Metrics_Rep_tmp_Universe (
          Id_Key                  bigint          identity,
          Account_Number          varchar(20)     default null,
          CB_Key_Household        bigint          default null,
          Scaling_Date            date            default null,
          Expected_Boxes          smallint        default 0,
          Vespa_Panel_Flag        bit             default 0,
          Scaling_Segment_Id      bigint          default 0,
          Scaling_Weight          decimal(15, 6)  default 0,

          Universe                varchar(20)     default null,
          Region                  varchar(20)     default null,
          HH_Composition          varchar(30)     default null,
          Tenure                  varchar(20)     default null,
          TV_Package              varchar(20)     default null,
          Box_Type                varchar(30)     default null,

          Value_Segment           varchar(15)     default 'Bedding In',
          Experian_Mosaic	        varchar(30)     default 'U: Unknown MOSAIC',
          Financial_Strategy_Segm varchar(30)     default 'U: Unknown FSS',
          OnOffNet_Area           varchar(20)     default 'Unknown',
          SkyGO_User              varchar(20)     default 'non-SkyGo user',

      )

      create hg index idx1 on SC_Metrics_Rep_tmp_Universe(Account_Number)
      create hg index idx2 on SC_Metrics_Rep_tmp_Universe(CB_Key_Household)
      create date index idx3 on SC_Metrics_Rep_tmp_Universe(Scaling_Date)
      create unique index idx4 on SC_Metrics_Rep_tmp_Universe(Account_Number, Scaling_Date)


      set @varCurrentDate = @varReportingStartDate

        -- Create universe based on weekly snapshots
      while @varCurrentDate <= @varReportingEndDate
          begin

              set @varTmpDate = (select max(profiling_date)
                                   from Vespa_Analysts.SC2_Sky_Base_Segment_Snapshots
                                  where profiling_date <= @varCurrentDate)

              insert into SC_Metrics_Rep_tmp_Universe
                     (Account_Number, CB_Key_Household, Scaling_Date, Expected_Boxes, Scaling_Segment_Id)
                select
                      Account_Number,
                      max(cb_key_household),
                      @varCurrentDate,
                      max(expected_boxes),
                      max(Scaling_Segment_Id)
                  from Vespa_Analysts.SC2_Sky_Base_Segment_Snapshots
                 where profiling_date = @varTmpDate
                 group by Account_Number
              commit


              execute logger_add_event @varBuildId, 3, 'Day processed: ' || dateformat(@varCurrentDate, 'dd/mm/yyyy') ||
                                                            ' (profiling date used: ' || dateformat(@varTmpDate, 'dd/mm/yyyy') || ')', @@rowcount

              set @varCurrentDate = @varCurrentDate + 1
          end


        -- Append panel/scaling information
      update SC_Metrics_Rep_tmp_Universe base
         set base.Vespa_Panel_Flag  = 1,
             base.Scaling_Weight    = weig.Weighting
        from Vespa_Analysts.SC2_Intervals intv,
             Vespa_Analysts.SC2_Weightings weig
       where base.Account_Number = intv.Account_Number
         and base.Scaling_Date >= intv.reporting_starts
         and base.Scaling_Date <= intv.reporting_ends
         and intv.Scaling_Segment_Id = weig.Scaling_Segment_Id
         and base.Scaling_Date = weig.Scaling_Day
      commit

      execute logger_add_event @varBuildId, 3, 'Panel flags created', @@rowcount



        -- ##############################################################################################################
        -- ##### STEP 2.1 - scaling variables calculation                                                           #####
        -- ##############################################################################################################
      update SC_Metrics_Rep_tmp_Universe base
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
      if object_id('SC_Metrics_Rep_tmp_Value_Segment_Dates') is not null drop table SC_Metrics_Rep_tmp_Value_Segment_Dates

      create table SC_Metrics_Rep_tmp_Value_Segment_Dates (
          Id_Key                  bigint          identity,
          Scaling_Date            date            default null,
          Value_Segment_Date      date            default null
      )

      create date index idx1 on SC_Metrics_Rep_tmp_Value_Segment_Dates(Scaling_Date)
      create date index idx2 on SC_Metrics_Rep_tmp_Value_Segment_Dates(Value_Segment_Date)

      set @varCurrentDate = @varReportingStartDate

        -- Create universe based on weekly snapshots
      while @varCurrentDate <= @varReportingEndDate
          begin

              set @varTmpDate = (select max(value_seg_date)
                                   from sk_prod.value_segments_five_yrs
                                  where value_seg_date <= @varCurrentDate)

              insert into SC_Metrics_Rep_tmp_Value_Segment_Dates
                     (Scaling_Date, Value_Segment_Date)
                values (
                        @varCurrentDate,
                        @varTmpDate
                       )
              commit

              set @varCurrentDate = @varCurrentDate + 1
          end

      execute logger_add_event @varBuildId, 3, 'Value segment dates lookup completed', null


      update SC_Metrics_Rep_tmp_Universe base
         set base.Value_Segment     = val.Value_Segment
        from SC_Metrics_Rep_tmp_Value_Segment_Dates lk,
             sk_prod.value_segments_five_yrs val
       where base.Scaling_Date = lk.Scaling_Date
         and lk.Value_Segment_Date = val.value_seg_date
         and base.Account_Number = val.Account_Number
      commit

      execute logger_add_event @varBuildId, 3, 'Value segments updated', @@rowcount


        -- Consumer view data
      if object_id('SC_Metrics_Rep_tmp_Consumer_View') is not null drop table SC_Metrics_Rep_tmp_Consumer_View
      select
            cv.cb_row_id as consumerview_cb_row_id,
            cv.CB_Key_Individual,
            cv.CB_Key_Household,
            pp.p_head_of_household,
            cv.H_Mosaic_UK_Group,
            cv.H_FSS_Group,
            rank() over(partition by cv.CB_Key_Household order by pp.p_head_of_household desc, pp.exp_cb_key_db_person desc,
                                                                  cv.cb_row_id desc, pp.cb_row_id desc) as Rank_Sequence
        into SC_Metrics_Rep_tmp_Consumer_View
        from sk_prod.experian_consumerview cv,
             sk_prod.playpen_consumerview_person_and_household pp
       where cv.exp_cb_key_db_individual = pp.exp_cb_key_db_individual
         and cv.cb_key_individual is not null
      commit

      create lf index idx1 on SC_Metrics_Rep_tmp_Consumer_View(Rank_Sequence)
      delete from SC_Metrics_Rep_tmp_Consumer_View where Rank_Sequence > 1

      create unique hg index idx2 on SC_Metrics_Rep_tmp_Consumer_View(CB_Key_Household)
      create hg index idx3 on SC_Metrics_Rep_tmp_Consumer_View(CB_Key_Individual)

      update SC_Metrics_Rep_tmp_Universe base
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

        from SC_Metrics_Rep_tmp_Consumer_View det
       where base.CB_Key_Household = det.CB_Key_Household
      commit

      execute logger_add_event @varBuildId, 3, 'Consumer view variables updated', @@rowcount


        -- On/Off net
      if object_id('SC_Metrics_Rep_tmp_OnOff_Net') is not null drop table SC_Metrics_Rep_tmp_OnOff_Net
      if object_id('SC_Metrics_Rep_tmp_OnOff_Net_Exch_Lookup') is not null drop table SC_Metrics_Rep_tmp_OnOff_Net_Exch_Lookup
      select
            account_number,
            min(cb_address_postcode) as Postcode,
            cast('Unknown' as varchar(20)) as OnOffNet_Area
        into SC_Metrics_Rep_tmp_OnOff_Net
        from sk_prod.cust_single_account_view
       group by account_number
      commit

      update SC_Metrics_Rep_tmp_OnOff_Net set Postcode = upper(replace(Postcode, ' ', ''))
      commit

      create unique hg index idx1 on SC_Metrics_Rep_tmp_OnOff_Net(account_number)
      create hg index idx2 on SC_Metrics_Rep_tmp_OnOff_Net(Postcode)


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
        into SC_Metrics_Rep_tmp_OnOff_Net_Exch_Lookup
        from #p2e full join #bpe on #bpe.Postcode = #p2e.Postcode
      commit

      create unique hg index idx1 on SC_Metrics_Rep_tmp_OnOff_Net_Exch_Lookup(Postcode)

      drop table #p2e
      drop table #bpe


        -- 4) Update with latest Easynet exchange information
      update SC_Metrics_Rep_tmp_OnOff_Net_Exch_Lookup base
         set OnOffNet_Area = 'OnNet Area'
        from sk_prod.easynet_rollout_data det
       where base.Exchange_ID = det.Exchange_ID
         and det.exchange_status = 'ONNET'
      commit


        -- Get account number list
      update SC_Metrics_Rep_tmp_OnOff_Net base
         set base.OnOffNet_Area = det.OnOffNet_Area
        from SC_Metrics_Rep_tmp_OnOff_Net_Exch_Lookup det
       where base.Postcode = det.Postcode
      commit

        -- Final update to the base table
      update SC_Metrics_Rep_tmp_Universe base
         set base.OnOffNet_Area	  = det.OnOffNet_Area
        from SC_Metrics_Rep_tmp_OnOff_Net det
       where base.Account_Number = det.Account_Number
      commit

      execute logger_add_event @varBuildId, 3, 'On/OffNet area variable updated', @@rowcount


        -- Sky GO user
      update SC_Metrics_Rep_tmp_Universe base
         set base.SkyGO_User	  = 'SkyGo user'
        from (select
                    Account_Number,
                    Activity_Dt
                from sk_prod.sky_player_usage_detail det
               where Activity_Dt >= '2011-08-18'
               group by Account_Number, Activity_Dt) det
       where base.Account_Number = det.Account_Number
         and base.Scaling_Date >= det.Activity_Dt
      commit

      execute logger_add_event @varBuildId, 3, 'SkyGo user variable updated', @@rowcount



        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '####### Scaling Metrics Report [Universe] - process completed #######', null
      execute logger_add_event @varBuildId, 3, ' ', null


end;


