/*###############################################################################
# Created on:   12/09/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  VESPA Aggregations - creating groupings (High & Low levels)
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 1.0 - creating groupings
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#   => VAggr_Meta_Run_Schedule
#   => VAggr_Meta_Aggr_Definitions
#   => VESPA_Shared.Aggr_Aggregation_Dim
#   => VESPA_Shared.Aggr_Fact
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 28/08/2013  SBE   v01 - initial version
# 13/11/2013  SBE   Data types updated to accommodate large values
# 20/11/2013  SBE   Rollback mechanism implemented to handle server disconnection issues
# 21/02/2014  SBE   Removed reference to "bednaszs" schema
#
###############################################################################*/


if object_id('VAggr_5_Grouping') is not null then drop procedure VAggr_5_Grouping end if;
create procedure VAggr_5_Grouping
      @parPeriodKey             bigint,
      @parDestSchema            varchar(40) = '',    -- Fact table location (schema name)
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
      declare @varLoopCurrId                  smallint
      declare @varLoopCurrSeq                 smallint
      declare @varLoopIterNum                 smallint

      declare @varGroupScaled                 bit
      declare @varLL_Bin_Number               smallint
      declare @varHLType                      varchar(20)
      declare @varHLBinNumber                 smallint
      declare @varHLGroup1LowerCutoff         smallint
      declare @varHLGroup1UpperCutoff         smallint
      declare @varHLGroup1Label               varchar(15)
      declare @varHLGroup2LowerCutoff         smallint
      declare @varHLGroup2UpperCutoff         smallint
      declare @varHLGroup2Label               varchar(15)
      declare @varHLGroup3LowerCutoff         smallint
      declare @varHLGroup3UpperCutoff         smallint
      declare @varHLGroup3Label               varchar(15)
      declare @varHLGroup4LowerCutoff         smallint
      declare @varHLGroup4UpperCutoff         smallint
      declare @varHLGroup4Label               varchar(15)
      declare @varHLGroup5LowerCutoff         smallint
      declare @varHLGroup5UpperCutoff         smallint
      declare @varHLGroup5Label               varchar(15)

      declare @varLastMetricGroupKey          bigint


      set @varProcessIdentifier        = 'VAggr_5_Grouping_v01'

      if (@parDestSchema is null or @parDestSchema = '')
          set @parDestSchema = user_name()

      if (@parBuildId is not null)
          set @varBuildId = @parBuildId


        -- ###############################################################################
        -- ##### Create logger event                                                 #####
        -- ###############################################################################
      if (@parBuildId is null)
          execute logger_create_run @varProcessIdentifier, @parRefreshIdentifier, @varBuildId output

      execute logger_add_event @varBuildId, 3, '####### VESPA Aggregations [Aggregation groupings] - process started #######', null
      execute logger_add_event @varBuildId, 3, '>>>>> Step 0.1: Preparing environment <<<<<', null
      execute logger_add_event @varBuildId, 3, 'Process identifier: ' || @varProcessIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Refresh identifier: ' || @parRefreshIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Build ID: ' || @varBuildId, null
      execute logger_add_event @varBuildId, 3, 'Schema used: ' || @parDestSchema, null


        -- ##############################################################################################################
        -- ##### STEP 1.0 - creating groupings                                                                      #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 1.0: Calculating low & high level groupings <<<<<', null

      if object_id('VAggr_tmp_Scaling_Array') is not null drop table VAggr_tmp_Scaling_Array
      create table VAggr_tmp_Scaling_Array (
            Id                                  bigint            identity,
            Row_Id                              bigint            default null)
      create        hg index idx01 on VAggr_tmp_Scaling_Array(Row_Id)

      insert into VAggr_tmp_Scaling_Array (Row_Id)
        select top 30000
              row_number() over (order by Id) as Row_Id
          from VESPA_Shared.Aggr_Account_Attributes
        order by Row_Id
      commit
      execute logger_add_event @varBuildId, 3, 'Scaling array created', @@rowcount


      set @varLoopCurrId  = 0
      set @varLoopCurrSeq = 1
      set @varLoopIterNum = 0

      set @varLL_Bin_Number         = 0
      set @varHLType                = '???'
      set @varHLBinNumber           = 0
      set @varHLGroup1LowerCutoff   = 0
      set @varHLGroup1UpperCutoff   = 0
      set @varHLGroup1Label         = '???'
      set @varHLGroup2LowerCutoff   = 0
      set @varHLGroup2UpperCutoff   = 0
      set @varHLGroup2Label         = '???'
      set @varHLGroup3LowerCutoff   = 0
      set @varHLGroup3UpperCutoff   = 0
      set @varHLGroup3Label         = '???'
      set @varHLGroup4LowerCutoff   = 0
      set @varHLGroup4UpperCutoff   = 0
      set @varHLGroup4Label         = '???'
      set @varHLGroup5LowerCutoff   = 0
      set @varHLGroup5UpperCutoff   = 0
      set @varHLGroup5Label         = '???'

        -- Get number of iterations
      set @varLoopIterNum = (select
                                   count(*)
                              from VESPA_Shared.Aggr_Aggregation_Dim aggr,
                                   VAggr_Meta_Aggr_Definitions def,
                                   VAggr_Meta_Run_Schedule lnk
                             where aggr.Aggregation_Key = def.Aggregation_Key
                               and aggr.Aggregation_Key = lnk.Aggregation_Key
                               and lnk.Period_Key = @parPeriodKey
                               and lnk.Grouping_Run = 1
                               and lnk.Grouping_Processed_Flag = 0
                               --and lnk.Id between 52 and 52
                               )

      execute logger_add_event @varBuildId, 3, 'Number of aggregations scheduled for processing: ' || @varLoopIterNum, null


      while @varLoopCurrSeq <= @varLoopIterNum
        begin

              -- Get current aggregation Id
            set @varLoopCurrId = (select top 1
                                        Aggregation_Key
                                    from (select
                                                row_number() over (order by lnk.Run_Sequence, aggr.Aggregation_Key) as Seq_Order,
                                                aggr.Aggregation_Key
                                            from VESPA_Shared.Aggr_Aggregation_Dim aggr,
                                                 VAggr_Meta_Aggr_Definitions def,
                                                 VAggr_Meta_Run_Schedule lnk
                                           where aggr.Aggregation_Key = def.Aggregation_Key
                                             and aggr.Aggregation_Key = lnk.Aggregation_Key
                                             and lnk.Period_Key = @parPeriodKey
                                             and lnk.Grouping_Run = 1
                                             and lnk.Grouping_Processed_Flag = 0) a
                                   order by Seq_Order)
                                   --where Seq_Order = @varLoopCurrSeq)


              -- Get current derivation rule
            select
                  @varGroupScaled           = Group_Scaled,
                  @varLL_Bin_Number         = LL_Bin_Number,
                  @varHLType                = HL_Type,
                  @varHLBinNumber           = HL_Bin_Number,
                  @varHLGroup1LowerCutoff   = HL_Group1_Lower_Cutoff,
                  @varHLGroup1UpperCutoff   = HL_Group1_Upper_Cutoff,
                  @varHLGroup1Label         = HL_Group1_Label,
                  @varHLGroup2LowerCutoff   = HL_Group2_Lower_Cutoff,
                  @varHLGroup2UpperCutoff   = HL_Group2_Upper_Cutoff,
                  @varHLGroup2Label         = HL_Group2_Label,
                  @varHLGroup3LowerCutoff   = HL_Group3_Lower_Cutoff,
                  @varHLGroup3UpperCutoff   = HL_Group3_Upper_Cutoff,
                  @varHLGroup3Label         = HL_Group3_Label,
                  @varHLGroup4LowerCutoff   = HL_Group4_Lower_Cutoff,
                  @varHLGroup4UpperCutoff   = HL_Group4_Upper_Cutoff,
                  @varHLGroup4Label         = HL_Group4_Label,
                  @varHLGroup5LowerCutoff   = HL_Group5_Lower_Cutoff,
                  @varHLGroup5UpperCutoff   = HL_Group5_Upper_Cutoff,
                  @varHLGroup5Label         = HL_Group5_Label
              from VAggr_Meta_Grouping_Rules
             where Aggregation_Key = @varLoopCurrId

            execute logger_add_event @varBuildId, 3, '-- Processing aggregation #' || @varLoopCurrId || ' (' || @varLoopCurrSeq || ' out of ' || @varLoopIterNum || ') --', null



              -- Reset values (for incomplete runs)
            set @varSql = '
                            delete from ' || @parDestSchema || '.Aggr_Metric_Group_Dim
                             where Metric_Group_Key in (select
                                                              Metric_Group_Key
                                                          from ' || @parDestSchema || '.Aggr_Fact
                                                         where Period_Key = ' || @parPeriodKey || '
                                                           and Aggregation_Key = ' || @varLoopCurrId || ')
                               and Metric_Group_Key > 3
                            commit
                            execute logger_add_event ' || @varBuildId || ', 3, ''Resetting values - existing metric groups deleted'', @@rowcount
                          '
            execute(@varSql)

            set @varSql = '
                            update ' || @parDestSchema || '.Aggr_Fact
                               set Metric_Group_Key = null
                             where Period_Key = ' || @parPeriodKey || '
                               and Aggregation_Key = ' || @varLoopCurrId || '
                               and Metric_Group_Key is not null
                            commit
                            execute logger_add_event ' || @varBuildId || ', 3, ''Resetting values - metric keys reset'', @@rowcount
                          '
            execute(@varSql)

            set @varSql = '
                            delete from ' || @parDestSchema || '.Aggr_Low_Level_Group_Summaries
                             where Period_Key = ' || @parPeriodKey || '
                               and Aggregation_Key = ' || @varLoopCurrId || '
                            commit
                            execute logger_add_event ' || @varBuildId || ', 3, ''Resetting values - existing bin summaries deleted'', @@rowcount
                          '
            execute(@varSql)



              -- Get latest ID from the DIM table
            if ( upper(@parDestSchema) = 'VESPA_SHARED' )
              begin
                    set @varLastMetricGroupKey = (select
                                                        max(Metric_Group_Key)
                                                    from VESPA_Shared.Aggr_Metric_Group_Dim)
              end
            else
              begin
                    set @varLastMetricGroupKey = (select
                                                        max(Metric_Group_Key)
                                                    from Aggr_Metric_Group_Dim)
              end

            set @varLastMetricGroupKey = coalesce(@varLastMetricGroupKey, 0)



              -- "Inflate" table or not - depending whether scaling grouping should be applied on scaled universe or not
            if object_id('VAggr_tmp_Grouping_Universe') is not null drop table VAggr_tmp_Grouping_Universe
            create table VAggr_tmp_Grouping_Universe (
                Id                                  bigint            not null identity,
                Row_Id                              bigint            null     default null,
                Aggregation_Key                     bigint            not null default 0,
                Account_Number                      varchar(20)       null     default null,
                Median_Scaling_Weight               decimal(30, 6)    null     default 0,
                Metric_Value                        decimal(30, 6)    not null default 0
            )
            create hg index idx0 on VAggr_tmp_Grouping_Universe(Row_Id)
            create hg index idx1 on VAggr_tmp_Grouping_Universe(Account_Number)

            if (@varGroupScaled = 1)
              begin
                    set @varSql = '
                                    insert into VAggr_tmp_Grouping_Universe (Row_Id, Aggregation_Key, Account_Number, Median_Scaling_Weight, Metric_Value)
                                      select
                                            arr.Row_Id,
                                            fct.Aggregation_Key,
                                            fct.Account_Number,
                                            acc.Median_Scaling_Weight,
                                            fct.Metric_Value
                                        from VESPA_Shared.Aggr_Account_Attributes acc
                                                inner join ' || @parDestSchema || '.Aggr_Fact fct       on acc.Account_Number = fct.Account_Number
                                                cross join VAggr_tmp_Scaling_Array arr
                                       where arr.Row_Id <= round(acc.Median_Scaling_Weight, 0)
                                         and fct.Period_Key = ' || @parPeriodKey || '
                                         and acc.Period_Key = ' || @parPeriodKey || '
                                         and fct.Aggregation_Key = ' || @varLoopCurrId || '
                                         and fct.Metric_Value > 0
                                    commit
                                    execute logger_add_event ' || @varBuildId || ', 3, ''Universe table created for SCALED grouping'', @@rowcount
                                  '
                    execute(@varSql)
              end
            else
              begin
                    set @varSql = '
                                    insert into VAggr_tmp_Grouping_Universe (Row_Id, Aggregation_Key, Account_Number, Median_Scaling_Weight, Metric_Value)
                                      select
                                            fct.Fact_Key,
                                            fct.Aggregation_Key,
                                            fct.Account_Number,
                                            0,
                                            fct.Metric_Value
                                        from ' || @parDestSchema || '.Aggr_Fact fct
                                       where fct.Period_Key = ' || @parPeriodKey || '
                                         and fct.Aggregation_Key = ' || @varLoopCurrId || '
                                         and fct.Metric_Value > 0
                                    commit
                                    execute logger_add_event ' || @varBuildId || ', 3, ''Universe table created for NON-SCALED grouping'', @@rowcount
                                  '
                    execute(@varSql)
              end



              -- Process grouping
            if object_id('VAggr_tmp_Grouping_Definitions') is not null drop table VAggr_tmp_Grouping_Definitions
            create table VAggr_tmp_Grouping_Definitions (
                Id                                  bigint            not null identity,
                Metric_Group_Key                    bigint            not null default 0,
                Period_Key                          bigint            not null default 0,
                Aggregation_Key                     bigint            not null default 0,
                Group_Name                          varchar(50)       null     default null,
                LL_Bin                              smallint          null     default 0,
                LL_Lower_Boundary                   decimal(30, 6)    null     default 0,
                LL_Upper_Boundary_Orig              decimal(30, 6)    null     default 0,
                LL_Upper_Boundary                   decimal(30, 6)    null     default 0,
                LL_Bin_Name                         varchar(15)       null     default '???',
                HL_Name                             varchar(15)       null     default '???',
                HL_Lower_Boundary                   decimal(30, 6)    null     default 0,
                HL_Upper_Boundary                   decimal(30, 6)    null     default 0
            )
            create hg index idx0 on VAggr_tmp_Grouping_Definitions(Metric_Group_Key)
            create hg index idx1 on VAggr_tmp_Grouping_Definitions(Period_Key)
            create hg index idx2 on VAggr_tmp_Grouping_Definitions(Aggregation_Key)


              -- Insert "Not eligible", "Excluded" and "Did not watch" groups
            insert into VAggr_tmp_Grouping_Definitions
                   (Metric_Group_Key, Period_Key, Aggregation_Key, Group_Name, LL_Lower_Boundary, LL_Upper_Boundary, LL_Bin_Name, HL_Name)
                 values (1, @parPeriodKey, @varLoopCurrId, 'Not eligible', -3, -3, 'Not eligible', 'Not eligible')

            insert into VAggr_tmp_Grouping_Definitions
                   (Metric_Group_Key, Period_Key, Aggregation_Key, Group_Name, LL_Lower_Boundary, LL_Upper_Boundary, LL_Bin_Name, HL_Name)
                 values (2, @parPeriodKey, @varLoopCurrId, 'Excluded', -2, -2, 'Excluded', 'Excluded')

            insert into VAggr_tmp_Grouping_Definitions
                   (Metric_Group_Key, Period_Key, Aggregation_Key, Group_Name, LL_Lower_Boundary, LL_Upper_Boundary, LL_Bin_Name, HL_Name)
                 values (3, @parPeriodKey, @varLoopCurrId, 'Did not watch', -1, -1, 'Did not watch', 'Did not watch')
            commit



              -- Insert generic groups
            insert into VAggr_tmp_Grouping_Definitions
                   (Metric_Group_Key, Period_Key, Aggregation_Key, Group_Name, LL_Bin, LL_Lower_Boundary, LL_Upper_Boundary_Orig,
                    LL_Upper_Boundary, LL_Bin_Name, HL_Name, HL_Lower_Boundary, HL_Upper_Boundary)
              select
                    row_number() over (order by a.Period_Key, a.Aggregation_Key) + @varLastMetricGroupKey as Metric_Group_Key,
                    a.Period_Key,
                    a.Aggregation_Key,
                    'Standard grouping - Bins & H/M/L',
                    a.LL_Bin,
                    min(case
                          when a.LL_Bin = @varLL_Bin_Number then 0
                            else a.Metric_Value
                        end) as LL_Lower_Boundary,
                    max(a.Metric_Value)       as LL_Upper_Boundary_Orig,
                    case
                      when a.LL_Bin = 1 then 999999999999999.999999
                        else lag(LL_Lower_Boundary, 1) over (partition by a.Period_Key, a.Aggregation_Key order by LL_Lower_Boundary desc) - 0.000001
                    end as LL_Upper_Boundary,
                    'Bin ' || repeat('0', 2 - length( cast(a.LL_Bin as varchar(2)) )) || a.LL_Bin
                                              as LL_Bin_Name,

                    case
                      when a.LL_Bin between @varHLGroup1UpperCutoff and @varHLGroup1LowerCutoff then @varHLGroup1Label
                      when a.LL_Bin between @varHLGroup2UpperCutoff and @varHLGroup2LowerCutoff then @varHLGroup2Label
                      when a.LL_Bin between @varHLGroup3UpperCutoff and @varHLGroup3LowerCutoff then @varHLGroup3Label
                      when a.LL_Bin between @varHLGroup4UpperCutoff and @varHLGroup4LowerCutoff then @varHLGroup4Label
                      when a.LL_Bin between @varHLGroup5UpperCutoff and @varHLGroup5LowerCutoff then @varHLGroup5Label
                        else '???'
                    end as HL_Name,

                    case
                      --when a.LL_Bin = @varLL_Bin_Number then @varLL_Bin_Number
                      when a.LL_Bin between @varHLGroup1UpperCutoff and @varHLGroup1LowerCutoff then @varHLGroup1LowerCutoff
                      when a.LL_Bin between @varHLGroup2UpperCutoff and @varHLGroup2LowerCutoff then @varHLGroup2LowerCutoff
                      when a.LL_Bin between @varHLGroup3UpperCutoff and @varHLGroup3LowerCutoff then @varHLGroup3LowerCutoff
                      when a.LL_Bin between @varHLGroup4UpperCutoff and @varHLGroup4LowerCutoff then @varHLGroup4LowerCutoff
                      when a.LL_Bin between @varHLGroup5UpperCutoff and @varHLGroup5LowerCutoff then @varHLGroup5LowerCutoff
                        else 99
                    end as HL_Lower_Boundary,

                    case
                      --when a.LL_Bin = @varLL_Bin_Number then @varLL_Bin_Number
                      when a.LL_Bin between @varHLGroup1UpperCutoff and @varHLGroup1LowerCutoff then @varHLGroup1UpperCutoff
                      when a.LL_Bin between @varHLGroup2UpperCutoff and @varHLGroup2LowerCutoff then @varHLGroup2UpperCutoff
                      when a.LL_Bin between @varHLGroup3UpperCutoff and @varHLGroup3LowerCutoff then @varHLGroup3UpperCutoff
                      when a.LL_Bin between @varHLGroup4UpperCutoff and @varHLGroup4LowerCutoff then @varHLGroup4UpperCutoff
                      when a.LL_Bin between @varHLGroup5UpperCutoff and @varHLGroup5LowerCutoff then @varHLGroup5UpperCutoff
                        else 99
                    end as HL_Upper_Boundary

                from (select
                            @parPeriodKey           as Period_Key,
                            @varLoopCurrId          as Aggregation_Key,
                            Metric_Value,
                            ntile(@varLL_Bin_Number) over (order by Metric_Value desc) as LL_Bin
                        from VAggr_tmp_Grouping_Universe) a
               group by a.Period_Key, a.Aggregation_Key, a.LL_Bin
               order by a.Period_Key, a.Aggregation_Key
            commit

            execute logger_add_event @varBuildId, 3, 'Group boundaries calculated', @@rowcount



              -- Update fact records
            set @varSql = '
                            update ' || @parDestSchema || '.Aggr_Fact base
                               set base.Metric_Group_Key = grps.Metric_Group_Key
                              from VAggr_tmp_Grouping_Definitions grps
                             where base.Period_Key = grps.Period_Key
                               and base.Aggregation_Key = grps.Aggregation_Key
                               and base.Metric_Value between grps.LL_Lower_Boundary and grps.LL_Upper_Boundary
                            commit

                            execute logger_add_event ' || @varBuildId || ', 3, ''Fact table records updated'', @@rowcount
                          '
            execute(@varSql)



              -- Pump results to the final table
            set @varSql = '
                            insert into ' || @parDestSchema || '.Aggr_Metric_Group_Dim
                                   (Metric_Group_Key, Group_Name, Low_Level_Banding, High_Level_Banding, Low_Level_Banding_Min,
                                    Low_Level_Banding_Max, High_Level_Banding_Min, High_Level_Banding_Max)
                              select
                                    Metric_Group_Key,
                                    Group_Name,
                                    LL_Bin_Name,
                                    HL_Name,
                                    LL_Lower_Boundary,
                                    LL_Upper_Boundary,
                                    HL_Lower_Boundary,
                                    HL_Upper_Boundary
                                from VAggr_tmp_Grouping_Definitions
                               where Metric_Group_Key > 3
                            commit

                            execute logger_add_event ' || @varBuildId || ', 3, ''New DIM records created'', @@rowcount
                          '
            execute(@varSql)



              -- Update bin statistics
            insert into VESPA_Shared.Aggr_Low_Level_Group_Summaries
                   (Period_Key, Aggregation_Key, Metric_Group_Key, Group_Name, Group_Lower_Boundary, Group_Upper_Boundary,
                    Group_Width, Median_Weights_Sum, Scaling_Weights_Sum, Non_Scaled_Volume, Non_Scaled_Mean, Non_Scaled_Median,
                    Non_Scaled_Stdev, Non_Scaled_Min, Non_Scaled_Max, Non_Scaled_Range)
              select
                    @parPeriodKey,
                    fct.Aggregation_Key,
                    grp.Metric_Group_Key,
                    max(grp.Low_Level_Banding),
                    max(grp.Low_Level_Banding_Min),
                    max(grp.Low_Level_Banding_Max),

                    max(grp.Low_Level_Banding_Max - grp.Low_Level_Banding_Min),
                    sum(acc.Median_Weight),
                    sum(acc.Scaling_Weight),
                    count(*),
                    avg(fct.Metric_Value),
                    percentile_cont(0.5) within group (order by Metric_Value ASC),

                    stddev(fct.Metric_Value),
                    min(fct.Metric_Value),
                    max(fct.Metric_Value),
                    max(fct.Metric_Value) - min(fct.Metric_Value)

                from VESPA_Shared.Aggr_Fact fct left join (select
                                                       Account_Number,
                                                       max( round(acc.Median_Scaling_Weight, 0) ) as Median_Weight,
                                                       max( round(acc.Scaling_Weight, 0) ) as Scaling_Weight
                                                   from VESPA_Shared.Aggr_Account_Attributes acc
                                                  where Period_Key = @parPeriodKey
                                                  group by Account_Number) acc
                         on fct.Account_Number = acc.Account_Number,
                     VESPA_Shared.Aggr_Metric_Group_Dim grp
               where fct.Period_key = @parPeriodKey
                 and fct.Aggregation_Key = @varLoopCurrId
                 and fct.Metric_Group_Key = grp.Metric_Group_Key
               group by fct.Aggregation_Key, grp.Metric_Group_Key
               order by 1, 2, 4
            commit
            execute logger_add_event @varBuildId, 3, 'Bin summaries created', @@rowcount



              -- Updating run as processed
            update VAggr_Meta_Run_Schedule
               set Grouping_Processed_Flag = 1
             where Period_Key = @parPeriodKey
               and Aggregation_Key = @varLoopCurrId
            commit

            execute logger_add_event @varBuildId, 3, 'Grouping run flagged as completed in this run', @@rowcount



            set @varLoopCurrSeq = @varLoopCurrSeq + 1

        end



        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '####### VESPA Aggregations [Aggregation groupings] - process completed #######', null
      execute logger_add_event @varBuildId, 3, ' ', null
      commit

end;




