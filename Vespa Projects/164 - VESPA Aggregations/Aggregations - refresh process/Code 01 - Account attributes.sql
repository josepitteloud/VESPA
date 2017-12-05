/*###############################################################################
# Created on:   28/06/2013
# Created by:   Mandy Ng (MNG)
# Description:  VESPA Aggregations - account attributes
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 1.0 - creating universe
#               STEP 2.0 - getting package/product portfolio at the end of the period
#               STEP 3.0 - getting package/product spin-downs/downgrades
#               STEP 4.0 - getting PVR enable account flag
#               STEP 5.0 - getting universe model score
#               STEP 6.0 - uploading results to VESPA_Shared
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - VESPA_Shared.Aggr_Period_Dim
#     - sk_prod.viq_viewing_data_scaling
#     - sk_prod.cust_subs_hist
#     - sk_prod.cust_entitlement_lookup
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 28/06/2013  MNG   Initial version
# 10/07/2013  MNG   Adapt to the current weighting table (sk_prod.viq_viewing_data_scaling)
# 19/07/2013  MNG   Adapt to Don's package movement table
# 26/07/2013  SBE   Revised & streamlined
# 26/08/2013  SBE   Parametrised, made period independent, moved to VESPA_Shared,
#                   naming change: Segm => Aggr
# 17/09/2013  SBE   Procedure created
# 18/11/2013  SBE   Weights source selection added (PROD or VESPA_Analysts)
# 21/02/2014  SBE   Removed reference to "bednaszs" schema
# 13/05/2014  ABA   EE Re-branding package name changes
#
###############################################################################*/


if object_id('VAggr_1_Account_Attributes') is not null then drop procedure VAggr_1_Account_Attributes end if;
create procedure VAggr_1_Account_Attributes
      @parPeriodKey             bigint,
      @parScalingWeightDate     date,
      @parScalingSource         varchar(10) = '',
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

      declare @varScalingSource               varchar(10)
      declare @varBuildId                     bigint              -- Logger ID (so all builds end up in same queue)
      declare @varProcessIdentifier           varchar(20)         -- Logger - process ID
      declare @varSQL                         varchar(25000)
      declare @varStartDate                   date
      declare @varEndDate                     date
      declare @varAccsCurrPeriod              bigint

      set @varProcessIdentifier        = 'VAggr_1_Acc_Attr_v01'

      select
            @varStartDate = date(Period_Start),
            @varEndDate   = date(Period_End)
        from VESPA_Shared.Aggr_Period_Dim
       where Period_Key = @parPeriodKey

      set @varScalingSource = upper(@parScalingSource)
      if (@varScalingSource is null or @varScalingSource = '')
          set @varScalingSource = 'PROD'

      if (@parBuildId is not null)
          set @varBuildId = @parBuildId


        -- ###############################################################################
        -- ##### Create logger event                                                 #####
        -- ###############################################################################
      if (@parBuildId is null)
          execute logger_create_run @varProcessIdentifier, @parRefreshIdentifier, @varBuildId output

      execute logger_add_event @varBuildId, 3, '####### VESPA Aggregations [Account attributes] - process started #######', null
      execute logger_add_event @varBuildId, 3, '>>>>> Step 0.1: Preparing environment <<<<<', null
      execute logger_add_event @varBuildId, 3, 'Process identifier: ' || @varProcessIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Refresh identifier: ' || @parRefreshIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Build ID: ' || @varBuildId, null
      execute logger_add_event @varBuildId, 3, 'Period: ' || dateformat(@varStartDate, 'dd/mm/yyyy')  || ' - ' || dateformat(@varEndDate, 'dd/mm/yyyy'), null
      execute logger_add_event @varBuildId, 3, 'Scaling weights source: ' || @varScalingSource, null



      -- ##############################################################################################################
      -- ##### STEP 1.0 - creating universe                                                                       #####
      -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 1.0: Creating universe <<<<<', null

      truncate table VAggr_01_Account_Attributes

        -- Get weights for the period and calculate median weight
      if object_id('VAggr_tmp_Account_Weights') is not null drop table VAggr_tmp_Account_Weights

--this essentially can be used for the panel info, but we only need one entry for an account rather than all
      if @varScalingSource = 'PROD'
        begin
              select
                    Account_Number,
                    Cb_Key_Household,
                    Scaling_Date,
                    Scaling_Weight,
                    rank() over (partition by Account_Number order by Scaling_Weight, Scaling_Date) as Weight_Rank,
                    cast(0 as smallint) as Days_Data_Returned,
                    cast(0 as bit) as Median_Scaling_Weight_Flag
                into VAggr_tmp_Account_Weights
                from (select
                            Account_Number,
                            max(Cb_Key_Household) as Cb_Key_Household,
                            adjusted_event_start_date_vespa as Scaling_Date,
                            max(calculated_scaling_weight) as Scaling_Weight
                        from sk_prod.viq_viewing_data_scaling
                       where adjusted_event_start_date_vespa between @varStartDate and @varEndDate
                       group by Account_Number, Scaling_Date) det
              commit

              create        hg index idx01 on VAggr_tmp_Account_Weights(Account_Number)
              create        hg index idx02 on VAggr_tmp_Account_Weights(Cb_Key_Household)
              create      date index idx03 on VAggr_tmp_Account_Weights(Scaling_Date)

              execute logger_add_event @varBuildId, 3, 'Pulled the list of qualifying accounts (source: ' || @varScalingSource || ')', @@rowcount

        end

      if @varScalingSource = 'VA'
        begin
              select
                    Account_Number,
                    Cb_Key_Household,
                    Scaling_Date,
                    Scaling_Weight,
                    rank() over (partition by Account_Number order by Scaling_Weight, Scaling_Date) as Weight_Rank,
                    cast(0 as smallint) as Days_Data_Returned,
                    cast(0 as bit) as Median_Scaling_Weight_Flag
                into VAggr_tmp_Account_Weights
                from (select
                            itv.Account_Number,
                            cast(null as bigint) as Cb_Key_Household,
                            wgh.Scaling_Day as Scaling_Date,
                            max(wgh.Weighting) as Scaling_Weight
                        from VESPA_Analysts.SC2_Intervals itv,
                             VESPA_Analysts.SC2_Weightings wgh
                       where itv.Scaling_Segment_Id = wgh.Scaling_Segment_Id
                         and itv.Reporting_Starts <= wgh.Scaling_Day
                         and itv.Reporting_Ends >= wgh.Scaling_Day
                         and wgh.Scaling_Day between @varStartDate and @varEndDate
                       group by itv.Account_Number, wgh.Scaling_Day) det
              commit

              create        hg index idx01 on VAggr_tmp_Account_Weights(Account_Number)
              create        hg index idx02 on VAggr_tmp_Account_Weights(Cb_Key_Household)
              create      date index idx03 on VAggr_tmp_Account_Weights(Scaling_Date)

              execute logger_add_event @varBuildId, 3, 'Pulled the list of qualifying accounts (source: ' || @varScalingSource || ')', @@rowcount

              update VAggr_tmp_Account_Weights base
                 set base.Cb_Key_Household = det.Cb_Key_Household
                from sk_prod.Cust_Single_Account_View det
               where base.Account_Number = det.Account_Number
              commit

              execute logger_add_event @varBuildId, 3, 'CB_HOUSEHOLD KEYs updated', @@rowcount

        end


      update VAggr_tmp_Account_Weights base
         set base.Days_Data_Returned  = det.Days_Data_Returned,
             base.Median_Scaling_Weight_Flag   = case                                                                      -- Flag mid-record, if even number take the "lower" of the two competing
                                                    when base.Weight_Rank = ceil(1.0 * det.Days_Data_Returned / 2) then 1
                                                      else 0
                                                  end
        from (select
                    Account_Number,
                    count(*) as Days_Data_Returned
                from VAggr_tmp_Account_Weights
               group by Account_Number) det
       where base.Account_Number = det.Account_Number
      commit


        -- Get list of those who will be included in the aggregation
      insert into VAggr_01_Account_Attributes
             (Account_Number, Cb_Key_Household, Period_Key, Panel_Id, Median_Scaling_Weight,
              Median_Scaling_Weight_Date, Days_Data_Returned, Days_Period)
        select
              Account_Number,
              Cb_Key_Household,
              @parPeriodKey,
              12,
              Scaling_Weight,
              Scaling_Date,
              Days_Data_Returned,
              datediff(day, cast(@varStartDate as date), cast(@varEndDate as date)) + 1
          from VAggr_tmp_Account_Weights
         where Median_Scaling_Weight_Flag = 1
      commit

      execute logger_add_event @varBuildId, 3, 'Universe created', @@rowcount



      if @varScalingSource = 'PROD'
        begin

              update VAggr_01_Account_Attributes base
                 set base.Scaling_Weight       = sc.calculated_scaling_weight,
                     base.Scaling_Weight_Date  = @parScalingWeightDate
                from sk_prod.viq_viewing_data_scaling sc
               where base.Account_Number = sc.Account_Number
                 and sc.adjusted_event_start_date_vespa = @parScalingWeightDate
              commit

              execute logger_add_event @varBuildId, 3, 'Scaling weights added', @@rowcount

        end

      if @varScalingSource = 'VA'
        begin

              update VAggr_01_Account_Attributes base
                 set base.Scaling_Weight       = sc.calculated_scaling_weight,
                     base.Scaling_Weight_Date  = @parScalingWeightDate
                from (select
                            itv.Account_Number,
                            wgh.Scaling_Day,
                            max(wgh.Weighting) as calculated_scaling_weight
                        from VESPA_Analysts.SC2_Intervals itv,
                             VESPA_Analysts.SC2_Weightings wgh
                       where itv.Scaling_Segment_Id = wgh.Scaling_Segment_Id
                         and itv.Reporting_Starts <= wgh.Scaling_Day
                         and itv.Reporting_Ends >= wgh.Scaling_Day
                         and wgh.Scaling_Day = @parScalingWeightDate
                       group by itv.Account_Number, wgh.Scaling_Day) sc
               where base.Account_Number = sc.Account_Number
              commit

              execute logger_add_event @varBuildId, 3, 'Scaling weights added', @@rowcount

        end



      -- ##############################################################################################################
      -- ##### STEP 2.0 - get package/product portfolio at the end of the period                                  #####
      -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 2.0: Getting package/product portfolio at the end of the period <<<<<', null

      if object_id('VAggr_tmp_Account_Portfolio_Snapshot') is not null drop table VAggr_tmp_Account_Portfolio_Snapshot
      select
            base.Account_Number,

              -- ##### DTV subscription #####
            max(case
                  when csh.subscription_sub_type = 'DTV Primary Viewing' and csh.status_code in ('AC', 'PC', 'AB') then 1
                    else 0
                end) as DTV_Sub,

              -- ##### Original #####
            max(case
                  when cel.mixes = 0                                                                then 1      -- Original
                  when cel.mixes = 1 and (cel.style_culture = 1 or cel.variety = 1)                 then 1      -- Original
                  when cel.mixes = 2 and (cel.style_culture + cel.variety = 2)                      then 1      -- Original
                    else 0
                end) as DTV_Pack_Original,

              -- ##### Variety #####
            max(case
                  when cel.mixes = 0                                                                then 0      -- Original
                  when cel.mixes = 1 and (cel.style_culture = 1 or cel.variety = 1)                 then 0      -- Original
                  when cel.mixes = 2 and (cel.style_culture + cel.variety = 2)                      then 0      -- Original
                  when cel.product_sk in      ( 43672,43669,43670,43664,43667,43663,43668,43673,
                                                43677,43674,43676,43666,43665,43662,43671,43675)    then 0      -- Family
                  when cel.mixes > 0                                                                then 1      -- Variety
                    else 0
                end) as DTV_Pack_Variety,

              -- ##### Family #####
            max(case
                  when cel.mixes = 0                                                                then 0      -- Original
                  when cel.mixes = 1 and (cel.style_culture = 1 or cel.variety = 1)                 then 0      -- Original
                  when cel.mixes = 2 and (cel.style_culture + cel.variety = 2)                      then 0      -- Original
                  when cel.product_sk in      ( 43672,43669,43670,43664,43667,43663,43668,43673,
                                                43677,43674,43676,43666,43665,43662,43671,43675)    then 1      -- Family
                  when cel.mixes > 0                                                                then 0      -- Variety
                    else 0
                end) as DTV_Pack_Family,

              -- ##### Sports premium #####
            max(case
                  when cel.prem_sports > 0                                                          then 1
                    else 0
                end) as Prem_Sports,

              -- ##### Movies premium #####
            max(case
                  when cel.prem_movies > 0                                                          then 1
                    else 0
                end) as Prem_Movies,

              -- ##### HD subscription #####
            max(case
                  when csh.subscription_sub_type = 'DTV HD' and csh.status_code in ('AC', 'PC', 'AB') then 1
                    else 0
                end) as HD_Sub,

              -- ##### 3D TV subscription #####
            max(case
                  when csh.subscription_sub_type = '3DTV' and csh.status_code in ('AC', 'PC', 'AB') then 1
                    else 0
                end) as TV3D_Sub,

              -- ##### ESPN subscription #####
            max(case
                  when csh.subscription_sub_type = 'ESPN' and csh.status_code in ('AC', 'PC', 'AB') then 1
                    else 0
                end) as ESPN_Sub,

              -- ##### DTV Chelsea TV subscription #####
            max(case
                  when csh.subscription_sub_type = 'DTV Chelsea TV' and csh.status_code in ('AC', 'PC', 'AB') then 1
                    else 0
                end) as ChelseaTV_Sub,

              -- ##### DTV MUTV subscription #####
            max(case
                  when csh.subscription_sub_type = 'DTV MUTV' and csh.status_code in ('AC', 'PC', 'AB') then 1
                    else 0
                end) as MUTV_Sub,

              -- ##### MGM subscription #####
            max(case
                  when csh.subscription_sub_type = 'MGM' and csh.status_code in ('AC', 'PC', 'AB') then 1
                    else 0
                end) as MGM_Sub

        into VAggr_tmp_Account_Portfolio_Snapshot
        from VAggr_01_Account_Attributes base
                inner join sk_prod.cust_subs_hist csh             on base.Account_Number = csh.Account_Number
                                                                 and csh.effective_from_dt <= @varEndDate
                                                                 and csh.effective_to_dt > @varEndDate
                left join sk_prod.cust_entitlement_lookup as cel  on csh.current_short_description = cel.short_description
       group by base.Account_Number
      commit

      create unique hg index idx01 on VAggr_tmp_Account_Portfolio_Snapshot(Account_Number)


        -- Append to the main table
      update VAggr_01_Account_Attributes base
         set base.Ent_DTV_Sub                   = det.DTV_Sub,
             base.Ent_DTV_Pack_Original         = det.DTV_Pack_Original,
             base.Ent_DTV_Pack_Variety          = det.DTV_Pack_Variety,
             base.Ent_DTV_Pack_Family           = det.DTV_Pack_Family,
             base.Ent_DTV_Prem_Sports           = det.Prem_Sports,
             base.Ent_DTV_Prem_Movies           = det.Prem_Movies,
             base.Ent_HD_Sub                    = det.HD_Sub,
             base.Ent_TV3D_Sub                  = det.TV3D_Sub,
             base.Ent_ESPN_Sub                  = det.ESPN_Sub,
             base.Ent_ChelseaTV_Sub             = det.ChelseaTV_Sub,
             base.Ent_MUTV_Sub                  = det.MUTV_Sub,
             base.Ent_MGM_Sub                   = det.MGM_Sub
        from VAggr_tmp_Account_Portfolio_Snapshot det
       where base.Account_Number = det.Account_Number
      commit

      execute logger_add_event @varBuildId, 3, 'Portfolio snapshot created', @@rowcount



      -- ##############################################################################################################
      -- ##### STEP 3.0 - getting package/product spin-downs/downgrades                                           #####
      -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 3.0: Getting package/product spin-downs/downgrades <<<<<', null

        -- Get all active subscription Ids within the period, so further analysis is based at subscription level,
        -- subsequently aggregated to account level
      if object_id('VAggr_tmp_Subscription_Base') is not null drop table VAggr_tmp_Subscription_Base
      select
            csh.Subscription_Id,
            cast(0 as bit) as Dummy                                                                               -- Sybase does not like single-column permanent tables
        into VAggr_tmp_Subscription_Base
        from VAggr_01_Account_Attributes base,
             sk_prod.cust_subs_hist csh
       where base.Account_Number = csh.Account_Number
         and csh.effective_from_dt <= @varEndDate                                                                -- Active at any point within the period
         and csh.effective_to_dt > @varStartDate
         and csh.effective_from_dt < csh.effective_to_dt                                                          -- Must remain active for 1 day at least
         and csh.status_code in ('AC', 'PC', 'AB')
         and (                                                                                                    -- Only relevant subscriptions to speed up later on
              csh.subscription_sub_type = 'DTV Primary Viewing' or
              csh.subscription_sub_type = 'DTV HD' or
              csh.subscription_sub_type = '3DTV' or
              csh.subscription_sub_type = 'ESPN' or
              csh.subscription_sub_type = 'DTV Chelsea TV' or
              csh.subscription_sub_type = 'DTV MUTV' or
              csh.subscription_sub_type = 'MGM'
             )
       group by csh.Subscription_Id
      commit

      create unique hg index idx01 on VAggr_tmp_Subscription_Base(Subscription_Id)


        -- Now calculate active/inactive flag for each record for each package/sub within the period
      if object_id('VAggr_tmp_Subscription_History') is not null drop table VAggr_tmp_Subscription_History
      select
            csh.Subscription_Id,
            csh.subscription_sub_type,

              -- ##### DTV #####
            case
              when csh.status_code in ('AC', 'PC', 'AB') then 1
                else 0
            end as Act_Sub,

              -- ##### Original #####
            case
              when cel.mixes = 0                                                                then 1      -- Original
              when cel.mixes = 1 and (cel.style_culture = 1 or cel.variety = 1)                 then 1      -- Original
              when cel.mixes = 2 and (cel.style_culture + cel.variety = 2)                      then 1      -- Original
                else 0
            end as Act_DTV_Pack_Original,

              -- ##### Variety #####
            case
              when cel.mixes = 0                                                                then 0      -- Original
              when cel.mixes = 1 and (cel.style_culture = 1 or cel.variety = 1)                 then 0      -- Original
              when cel.mixes = 2 and (cel.style_culture + cel.variety = 2)                      then 0      -- Original
              when cel.product_sk in      ( 43672,43669,43670,43664,43667,43663,43668,43673,
                                            43677,43674,43676,43666,43665,43662,43671,43675)    then 0      -- Family
              when cel.mixes > 0                                                                then 1      -- Variety
                else 0
            end as Act_DTV_Pack_Variety,

              -- ##### Family #####
            case
              when cel.mixes = 0                                                                then 0      -- Original
              when cel.mixes = 1 and (cel.style_culture = 1 or cel.variety = 1)                 then 0      -- Original
              when cel.mixes = 2 and (cel.style_culture + cel.variety = 2)                      then 0      -- Original
              when cel.product_sk in      ( 43672,43669,43670,43664,43667,43663,43668,43673,
                                            43677,43674,43676,43666,43665,43662,43671,43675)    then 1      -- Family
              when cel.mixes > 0                                                                then 0      -- Variety
                else 0
            end as Act_DTV_Pack_Family,

              -- ##### Sports premium #####
            case
              when cel.prem_sports > 0                                                          then 1
                else 0
            end as Act_Prem_Sports,

              -- ##### Movies premium #####
            case
              when cel.prem_movies > 0                                                          then 1
                else 0
            end as Act_Prem_Movies

        into VAggr_tmp_Subscription_History
        from VAggr_tmp_Subscription_Base base,
             sk_prod.cust_subs_hist csh,
             sk_prod.cust_entitlement_lookup cel
       where base.Subscription_Id = csh.Subscription_Id
         and csh.effective_from_dt <= @varEndDate                                                                -- Active at any point within the period
         and csh.effective_to_dt > @varStartDate
         and csh.effective_from_dt < csh.effective_to_dt                                                          -- Status must last at least 1 day
         and csh.current_short_description = cel.short_description
      commit

      create        hg index idx01 on VAggr_tmp_Subscription_History(Subscription_Id)
      create        lf index idx02 on VAggr_tmp_Subscription_History(subscription_sub_type)


        -- Summarise by subscription Id and count how many records have relevant package active and how many records there are overall per subscription
      if object_id('VAggr_tmp_Subscription_Summary') is not null drop table VAggr_tmp_Subscription_Summary
      select
            Subscription_Id,

            sum(case when subscription_sub_type = 'DTV Primary Viewing' then Act_Sub else 0 end)                        as Act_DTV_Sub_Recs,
            sum(case when subscription_sub_type = 'DTV Primary Viewing' then Act_DTV_Pack_Original else 0 end)          as Act_DTV_Pack_Original_Recs,
            sum(case when subscription_sub_type = 'DTV Primary Viewing' then Act_DTV_Pack_Variety else 0 end)           as Act_DTV_Pack_Variety_Recs,
            sum(case when subscription_sub_type = 'DTV Primary Viewing' then Act_DTV_Pack_Family else 0 end)            as Act_DTV_Pack_Family_Recs,
            sum(case when subscription_sub_type = 'DTV Primary Viewing' then Act_Prem_Sports else 0 end)                as Act_Prem_Sports_Recs,
            sum(case when subscription_sub_type = 'DTV Primary Viewing' then Act_Prem_Movies else 0 end)                as Act_Prem_Movies_Recs,
            sum(case when subscription_sub_type = 'DTV HD' then Act_Sub else 0 end)                                     as Act_HD_Sub_Recs,
            sum(case when subscription_sub_type = '3DTV' then Act_Sub else 0 end)                                       as Act_TV3D_Sub_Recs,
            sum(case when subscription_sub_type = 'ESPN' then Act_Sub else 0 end)                                       as Act_ESPN_Sub_Recs,
            sum(case when subscription_sub_type = 'DTV Chelsea TV' then Act_Sub else 0 end)                             as Act_ChelseaTV_Sub_Recs,
            sum(case when subscription_sub_type = 'DTV MUTV' then Act_Sub else 0 end)                                   as Act_MUTV_Sub_Recs,
            sum(case when subscription_sub_type = 'MGM' then Act_Sub else 0 end)                                        as Act_MGM_Sub_Recs,

            sum(case when subscription_sub_type = 'DTV Primary Viewing' then 1 else 0 end)                              as All_DTV_Sub_Recs,
            sum(case when subscription_sub_type = 'DTV HD' then 1 else 0 end)                                           as All_HD_Sub_Recs,
            sum(case when subscription_sub_type = '3DTV' then 1 else 0 end)                                             as All_TV3D_Sub_Recs,
            sum(case when subscription_sub_type = 'ESPN' then 1 else 0 end)                                             as All_ESPN_Sub_Recs,
            sum(case when subscription_sub_type = 'DTV Chelsea TV' then 1 else 0 end)                                   as All_ChelseaTV_Sub_Recs,
            sum(case when subscription_sub_type = 'DTV MUTV' then 1 else 0 end)                                         as All_MUTV_Sub_Recs,
            sum(case when subscription_sub_type = 'MGM' then 1 else 0 end)                                              as All_MGM_Sub_Recs

        into VAggr_tmp_Subscription_Summary
        from VAggr_tmp_Subscription_History
       group by Subscription_Id
      commit

      create unique hg index idx01 on VAggr_tmp_Subscription_Summary(Subscription_Id)

      execute logger_add_event @varBuildId, 3, 'Package/subscription downgrade movements created', @@rowcount


        -- Account Number <=> Subscription ID lookup is also required
      if object_id('VAggr_tmp_Account_Subscription_Lookup') is not null drop table VAggr_tmp_Account_Subscription_Lookup
      select
            csh.Account_Number,
            base.Subscription_Id
        into VAggr_tmp_Account_Subscription_Lookup
        from VAggr_tmp_Subscription_Base base,
             sk_prod.cust_subs_hist csh
       where base.Subscription_Id = csh.Subscription_Id
       group by csh.Account_Number, base.Subscription_Id
      commit

      create        hg index idx01 on VAggr_tmp_Account_Subscription_Lookup(Account_Number)
      create        hg index idx02 on VAggr_tmp_Account_Subscription_Lookup(Subscription_Id)


        -- Append to the main table
        -- The rule is that when number of "active" records for each package is lower than total number of records for that package/sub, then
        -- one can conclude there was a "movement". However, when there are multiple subscriptions per account, at least one must remain active for the
        -- entire period for the account to qualify
      update VAggr_01_Account_Attributes base
         set base.Movmt_DTV_Sub                   = det.DTV_Sub,
             base.Movmt_DTV_Pack_Original         = det.DTV_Pack_Original,
             base.Movmt_DTV_Pack_Variety          = det.DTV_Pack_Variety,
             base.Movmt_DTV_Pack_Family           = det.DTV_Pack_Family,
             base.Movmt_DTV_Prem_Sports           = det.DTV_Prem_Sports,
             base.Movmt_DTV_Prem_Movies           = det.DTV_Prem_Movies,
             base.Movmt_HD_Sub                    = det.HD_Sub,
             base.Movmt_TV3D_Sub                  = det.TV3D_Sub,
             base.Movmt_ESPN_Sub                  = det.ESPN_Sub,
             base.Movmt_ChelseaTV_Sub             = det.ChelseaTV_Sub,
             base.Movmt_MUTV_Sub                  = det.MUTV_Sub,
             base.Movmt_MGM_Sub                   = det.MGM_Sub
        from (select
                    acc.Account_Number,
                    min(case when Act_DTV_Sub_Recs                < All_DTV_Sub_Recs        then 1 else 0 end) as DTV_Sub,
                    min(case when Act_DTV_Pack_Original_Recs      < All_DTV_Sub_Recs        then 1 else 0 end) as DTV_Pack_Original,
                    min(case when Act_DTV_Pack_Variety_Recs       < All_DTV_Sub_Recs        then 1 else 0 end) as DTV_Pack_Variety,
                    min(case when Act_DTV_Pack_Family_Recs        < All_DTV_Sub_Recs        then 1 else 0 end) as DTV_Pack_Family,
                    min(case when Act_Prem_Sports_Recs            < All_DTV_Sub_Recs        then 1 else 0 end) as DTV_Prem_Sports,
                    min(case when Act_Prem_Movies_Recs            < All_DTV_Sub_Recs        then 1 else 0 end) as DTV_Prem_Movies,
                    min(case when Act_HD_Sub_Recs                 < All_HD_Sub_Recs         then 1 else 0 end) as HD_Sub,
                    min(case when Act_TV3D_Sub_Recs               < All_TV3D_Sub_Recs       then 1 else 0 end) as TV3D_Sub,
                    min(case when Act_ESPN_Sub_Recs               < All_ESPN_Sub_Recs       then 1 else 0 end) as ESPN_Sub,
                    min(case when Act_ChelseaTV_Sub_Recs          < All_ChelseaTV_Sub_Recs  then 1 else 0 end) as ChelseaTV_Sub,
                    min(case when Act_MUTV_Sub_Recs               < All_MUTV_Sub_Recs       then 1 else 0 end) as MUTV_Sub,
                    min(case when Act_MGM_Sub_Recs                < All_MGM_Sub_Recs        then 1 else 0 end) as MGM_Sub
                from VAggr_tmp_Account_Subscription_Lookup acc,
                     VAggr_tmp_Subscription_Summary sub
               where acc.Subscription_Id = sub.Subscription_Id
               group by acc.Account_Number) det
       where base.Account_Number = det.Account_Number
      commit


        -- Now set to null movement fields for not relevant subscriptions to avoid confusion
      update VAggr_01_Account_Attributes base
         set base.Movmt_DTV_Sub                   = case when det.DTV_Sub                   = 0 then null else base.Movmt_DTV_Sub                 end,
             base.Movmt_DTV_Pack_Original         = case when det.DTV_Pack_Original         = 0 then null else base.Movmt_DTV_Pack_Original       end,
             base.Movmt_DTV_Pack_Variety          = case when det.DTV_Pack_Variety          = 0 then null else base.Movmt_DTV_Pack_Variety        end,
             base.Movmt_DTV_Pack_Family           = case when det.DTV_Pack_Family           = 0 then null else base.Movmt_DTV_Pack_Family         end,
             base.Movmt_DTV_Prem_Sports           = case when det.Prem_Sports               = 0 then null else base.Movmt_DTV_Prem_Sports         end,
             base.Movmt_DTV_Prem_Movies           = case when det.Prem_Movies               = 0 then null else base.Movmt_DTV_Prem_Movies         end,
             base.Movmt_HD_Sub                    = case when det.HD_Sub                    = 0 then null else base.Movmt_HD_Sub                  end,
             base.Movmt_TV3D_Sub                  = case when det.TV3D_Sub                  = 0 then null else base.Movmt_TV3D_Sub                end,
             base.Movmt_ESPN_Sub                  = case when det.ESPN_Sub                  = 0 then null else base.Movmt_ESPN_Sub                end,
             base.Movmt_ChelseaTV_Sub             = case when det.ChelseaTV_Sub             = 0 then null else base.Movmt_ChelseaTV_Sub           end,
             base.Movmt_MUTV_Sub                  = case when det.MUTV_Sub                  = 0 then null else base.Movmt_MUTV_Sub                end,
             base.Movmt_MGM_Sub                   = case when det.MGM_Sub                   = 0 then null else base.Movmt_MGM_Sub                 end
        from VAggr_tmp_Account_Portfolio_Snapshot det
       where base.Account_Number = det.Account_Number
      commit

      execute logger_add_event @varBuildId, 3, 'Universe details updated', @@rowcount



      -- ##############################################################################################################
      -- ##### STEP 4.0 - getting PVR enable account flag                                                         #####
      -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 4.0: Getting PVR flags  <<<<<', null

      if object_id('VAggr_tmp_PVR_Boxes') is not null drop table VAggr_tmp_PVR_Boxes
      select
            a.*
        into VAggr_tmp_PVR_Boxes
        from (select
                    account_number,
                    service_instance_id,
                    box_replaced_dt,
                    case when trim(x_pvr_type) like 'PVR%' then 1
                      else 0
                    end as PVR_Flag,
                    rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) hist_sequence
                from sk_prod.cust_set_top_box) a
       where hist_sequence = 1
         and box_replaced_dt = '9999-09-09'
      commit

      create hg index idx1 on VAggr_tmp_PVR_Boxes(account_number)


      update VAggr_01_Account_Attributes base
         set base.Ent_PVR_Enabled   = bx.PVR_Flag
        from (select
                    Account_Number,
                    max(PVR_Flag) as PVR_Flag
                from VAggr_tmp_PVR_Boxes
               group by Account_Number) bx
       where base.Account_Number = bx.Account_Number
      commit

      execute logger_add_event @varBuildId, 3, 'Account PVR flag created', @@rowcount



      -- ##############################################################################################################
      -- ##### STEP 5.0 - getting universe model score                                                            #####
      -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 5.0: Getting CQM model scores  <<<<<', null

      update VAggr_01_Account_Attributes base
         set base.Acc_Univ_Model_Score   = det.Model_Score
        from sk_prod.id_v_universe_all det
       where base.Cb_Key_Household = det.Cb_Key_Household
      commit

      execute logger_add_event @varBuildId, 3, 'Customer quality model score created', @@rowcount



      -- ##############################################################################################################
      -- ##### STEP 6.0 - uploading results to VESPA_Shared                                                       #####
      -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 6.0: Uploading results to VESPA_Shared  <<<<<', null
      set @varAccsCurrPeriod = (select count(*)
                                  from VESPA_Shared.Aggr_Account_Attributes
                                 where Period_Key = @parPeriodKey)

      if @varAccsCurrPeriod > 0
          begin
              message 'VAggr_1_Acc_Attr_v01 says:\n' ||
                      @varAccsCurrPeriod || ' records already exists - manual load required\n\n' ||
                      'TABLE: VESPA_Shared.Aggr_Account_Attributes\n' ||
                      'PERIOD: ' || dateformat(@varStartDate, 'dd/mm/yyyy') || ' - ' || dateformat(@varEndDate, 'dd/mm/yyyy') || '\n ' type action to client
          end
        else
          begin
              insert into VESPA_Shared.Aggr_Account_Attributes
                     (Account_Number, Cb_Key_Household, Period_Key, Panel_Id, Median_Scaling_Weight, Median_Scaling_Weight_Date,
                      Scaling_Weight, Scaling_Weight_Date, Days_Data_Returned, Days_Period, Acc_Univ_Model_Score,
                      Ent_DTV_Sub, Ent_DTV_Pack_Original, Ent_DTV_Pack_Variety,
                      Ent_DTV_Pack_Family, Ent_DTV_Prem_Sports, Ent_DTV_Prem_Movies, Ent_HD_Sub, Ent_TV3D_Sub, Ent_ESPN_Sub,
                      Ent_ChelseaTV_Sub, Ent_MUTV_Sub, Ent_MGM_Sub, Ent_PVR_Enabled, Movmt_DTV_Sub, Movmt_DTV_Pack_Original,
                      Movmt_DTV_Pack_Variety, Movmt_DTV_Pack_Family, Movmt_DTV_Prem_Sports, Movmt_DTV_Prem_Movies,
                      Movmt_HD_Sub, Movmt_TV3D_Sub, Movmt_ESPN_Sub, Movmt_ChelseaTV_Sub, Movmt_MUTV_Sub, Movmt_MGM_Sub)
                select
                      Account_Number, Cb_Key_Household, Period_Key, Panel_Id, Median_Scaling_Weight, Median_Scaling_Weight_Date,
                      Scaling_Weight, Scaling_Weight_Date, Days_Data_Returned, Days_Period, Acc_Univ_Model_Score,
                      Ent_DTV_Sub, Ent_DTV_Pack_Original, Ent_DTV_Pack_Variety,
                      Ent_DTV_Pack_Family, Ent_DTV_Prem_Sports, Ent_DTV_Prem_Movies, Ent_HD_Sub, Ent_TV3D_Sub, Ent_ESPN_Sub,
                      Ent_ChelseaTV_Sub, Ent_MUTV_Sub, Ent_MGM_Sub, Ent_PVR_Enabled, Movmt_DTV_Sub, Movmt_DTV_Pack_Original,
                      Movmt_DTV_Pack_Variety, Movmt_DTV_Pack_Family, Movmt_DTV_Prem_Sports, Movmt_DTV_Prem_Movies,
                      Movmt_HD_Sub, Movmt_TV3D_Sub, Movmt_ESPN_Sub, Movmt_ChelseaTV_Sub, Movmt_MUTV_Sub, Movmt_MGM_Sub
                  from VAggr_01_Account_Attributes
              commit
          end

      execute logger_add_event @varBuildId, 3, 'Result uploaded', @@rowcount



        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '####### VESPA Aggregations [Account attributes] - process completed #######', null
      execute logger_add_event @varBuildId, 3, ' ', null
      commit

end;


