/*###############################################################################
# Created on:   01/11/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  VESPA Aggregations - streamlined binning process
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 1.0 - calculating bins
#               STEP 2.0 - calculating bin stats
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects used:
#   => VAggrAnal_Fact                 - this table holds all records for a given
#                                       aggregation. Multiple aggregations can be
#                                       stored, differentiated by Aggregation_Key.
#                                       All account filters to be applied, negative
#                                       values (Excluded/DNW/Not eligible) can exist
#                                       and are not included in the binning process
#   => VAggrAnal_Account_Attributes   - related customer attributes. Required fields:
#                                         - Account_Number
#                                         - Median_Scaling_Weight
#   => VAggrAnal_Metric_Group_Dim     - dimension table which holds calculated
#                                       boundaries for calculated bins. Link is created
#                                       in the fact table by updating Metric_Group_Key
#                                       with a relevant ID
#   => VAggrAnal_Bin_Summaries        - table which holds bin summaries of the binning
#                                       process
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 01/11/2013  SBE   v01 - initial version
#
###############################################################################*/

  -- ###############################################################################
  -- ##### Execution                                                           #####
  -- ###############################################################################
/*
--execute VAggrAnal_Binning_Setup;

execute VAggrAnal_Binning 52, '', 1, 10, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute logger_get_latest_job_events 'VAggrAnal_Binning', 4;

execute VAggrAnal_Binning_Cleanup;

alter view v_VAggrAnal_Bin_Summaries as
  select *
    from VAggrAnal_Bin_Summaries
   where Aggregation_Key between 163 and 185;
*/

/*
  -- Sample data
insert into VAggrAnal_Fact (Aggregation_Key, Metric_Group_Key, Account_Number, Metric_Value)
  select
        Aggregation_Key,
        null,
        Account_Number,
        Metric_Value
    from VESPA_Shared.Aggr_Fact
   where aggregation_key = 52;
commit;


insert into VAggrAnal_Account_Attributes (Account_Number, Median_Scaling_Weight)
select
      Account_Number,
      Median_Scaling_Weight
  from VESPA_Shared.Aggr_Account_Attributes
 where Period_Key = 5;
commit;

*/

  -- ###############################################################################
  -- ##### Environment set up                                                  #####
  -- ###############################################################################
if object_id('VAggrAnal_Binning_Setup') is not null then drop procedure VAggrAnal_Binning_Setup end if;
create procedure VAggrAnal_Binning_Setup
as
begin

        -- VAggrAnal_Fact
      if object_id('VAggrAnal_Fact') is not null drop table VAggrAnal_Fact
      create table VAggrAnal_Fact (
            Fact_Key                                bigint            not null identity,
            Aggregation_Key                         bigint            not null default null,
            Metric_Group_Key                        bigint            null     default null,
            Account_Number                          varchar(20)       not null,
            Metric_Value                            decimal(30, 6)    not null default 0,
            Updated_On                              datetime          not null default timestamp,
            Updated_By                              varchar(30)       not null default user_name()
      )
      create        hg index idx01 on VAggrAnal_Fact(Aggregation_Key)
      create        hg index idx02 on VAggrAnal_Fact(Metric_Group_Key)
      create        hg index idx03 on VAggrAnal_Fact(Account_Number)
      grant select on VAggrAnal_Fact to vespa_group_low_security


        -- VAggrAnal_Account_Attributes
      if object_id('VAggrAnal_Account_Attributes') is not null drop table VAggrAnal_Account_Attributes
      create table VAggrAnal_Account_Attributes (
            Attribute_Key                           bigint            not null identity,
            Account_Number                          varchar(20)       null     default null,
            Median_Scaling_Weight                   decimal(30, 6)    null     default 0,
            Updated_On                              datetime          not null default timestamp,
            Updated_By                              varchar(30)       not null default user_name()
      )
      create        hg index idx01 on VAggrAnal_Account_Attributes(Account_Number)
      grant select on VAggrAnal_Account_Attributes to vespa_group_low_security


        -- VAggrAnal_Metric_Group_Dim
      if object_id('VAggrAnal_Metric_Group_Dim') is not null drop table VAggrAnal_Metric_Group_Dim
      create table VAggrAnal_Metric_Group_Dim (
            Metric_Group_Key                        bigint            not null default 0,
            Group_Name                              varchar(50)       null     default null,
            Low_Level_Banding                       varchar(15)       null     default null,
            Low_Level_Banding_Min                   decimal(30, 6)    null     default null,
            Low_Level_Banding_Max                   decimal(30, 6)    null     default null,
            Updated_On                              datetime          not null default timestamp,
            Updated_By                              varchar(30)       not null default user_name()
      )
      create unique hg index idx01 on VAggrAnal_Metric_Group_Dim(Metric_Group_Key)
      grant select on VAggrAnal_Metric_Group_Dim to vespa_group_low_security

      insert into VAggrAnal_Metric_Group_Dim (Metric_Group_Key, Group_Name, Low_Level_Banding) values (1, 'Not eligible', 'Not eligible')
      insert into VAggrAnal_Metric_Group_Dim (Metric_Group_Key, Group_Name, Low_Level_Banding) values (2, 'Excluded', 'Excluded')
      insert into VAggrAnal_Metric_Group_Dim (Metric_Group_Key, Group_Name, Low_Level_Banding) values (3, 'Did not watch', 'Did not watch')
      commit


        -- VAggrAnal_Bin_Summaries
      if object_id('VAggrAnal_Bin_Summaries') is not null drop table VAggrAnal_Bin_Summaries
      create table VAggrAnal_Bin_Summaries (
            Summary_Key                             bigint            not null identity,
            Aggregation_Key                         bigint            not null default null,
            Aggregation_Name                        varchar(50)       null     default null,
            Bin_Name                                varchar(15)       null     default null,
            Bin_Lower_Boundary                      decimal(30, 6)    null     default null,
            Bin_Upper_Boundary                      decimal(30, 6)    null     default null,
            Bin_Width                               decimal(30, 6)    null     default null,

            Bin_Scaled_Volume                       bigint            null     default 0,
            Bin_Non_Scaled_Volume                   bigint            null     default 0,

            Bin_Non_Scaled_Mean                     decimal(30, 6)    null     default null,
            Bin_Non_Scaled_Median                   decimal(30, 6)    null     default null,
            Bin_Non_Scaled_Stdev                    decimal(30, 6)    null     default null,
            Bin_Non_Scaled_Min                      decimal(30, 6)    null     default null,
            Bin_Non_Scaled_Max                      decimal(30, 6)    null     default null,

            Updated_On                              datetime          not null default timestamp,
            Updated_By                              varchar(30)       not null default user_name()
      )
      create hg index idx01 on VAggrAnal_Bin_Summaries(Aggregation_Key)
      grant select on VAggrAnal_Bin_Summaries to vespa_group_low_security


end;



  -- ###############################################################################
  -- ##### Environment clean up                                                #####
  -- ###############################################################################
if object_id('VAggrAnal_Binning_Cleanup') is not null then drop procedure VAggrAnal_Binning_Cleanup end if;
create procedure VAggrAnal_Binning_Cleanup
as
begin

      if object_id('VAggrAnal_Fact') is not null drop table VAggrAnal_Fact
      if object_id('VAggrAnal_Account_Attributes') is not null drop table VAggrAnal_Account_Attributes
      if object_id('VAggrAnal_Metric_Group_Dim') is not null drop table VAggrAnal_Metric_Group_Dim
      if object_id('VAggrAnal_tmp_Scaling_Array') is not null drop table VAggrAnal_tmp_Scaling_Array
      if object_id('VAggrAnal_tmp_Grouping_Universe') is not null drop table VAggrAnal_tmp_Grouping_Universe
      if object_id('VAggrAnal_tmp_Grouping_Definitions') is not null drop table VAggrAnal_tmp_Grouping_Definitions

end;



  -- ###############################################################################
  -- ##### Binning results                                                     #####
  -- ###############################################################################
if object_id('VAggrAnal_Binning') is not null then drop procedure VAggrAnal_Binning end if;
create procedure VAggrAnal_Binning
      @parAggregationKey        smallint = 0,
      @parAggregationName       varchar(50) = '',
      @parGroupScaled           bit = 0,
      @parNumBins               tinyint = 0,
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

      declare @varLastMetricGroupKey          bigint


      set @varProcessIdentifier        = 'VAggrAnal_Binning'

      if (@parAggregationKey is null)
          set @parAggregationKey = 0

      if (@parAggregationName is null or @parAggregationName = '')
          set @parAggregationName = '(undefined)'

      if (@parGroupScaled is null)
          set @parGroupScaled = 0

      if (@parNumBins is null or @parNumBins = 0)
          set @parNumBins = 10

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
      execute logger_add_event @varBuildId, 3, 'Schema used: ' || user_name(), null
      execute logger_add_event @varBuildId, 3, 'Aggregation Id: ' || @parAggregationKey, null
      execute logger_add_event @varBuildId, 3, 'Aggregation name: "' || @parAggregationName || '"', null
      execute logger_add_event @varBuildId, 3, 'Number of bins defined: ' || @parNumBins, null


      execute logger_add_event @varBuildId, 3, '>>>>> Step 0.1: Resetting values <<<<<', null

      delete from VAggrAnal_Metric_Group_Dim
       where Metric_Group_Key in (select
                                        Metric_Group_Key
                                    from VAggrAnal_Fact
                                   where Aggregation_Key = @parAggregationKey)
         and Metric_Group_Key > 3
      commit
      execute logger_add_event @varBuildId, 3, 'Metric groups deleted', @@rowcount

      update VAggrAnal_Fact
         set Metric_Group_Key = null
       where Aggregation_Key = @parAggregationKey
      commit
      execute logger_add_event @varBuildId, 3, 'Metric keys reset', @@rowcount

      delete from VAggrAnal_Bin_Summaries
       where Aggregation_Key = @parAggregationKey
      commit
      execute logger_add_event @varBuildId, 3, 'Bin summaries deleted', @@rowcount



        -- ##############################################################################################################
        -- ##### STEP 1.0 - calculating bins                                                                        #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 1.0: Calculating bins <<<<<', null

        -- Get the latest grouping ID
      set @varLastMetricGroupKey = (select
                                          max(Metric_Group_Key)
                                      from VAggrAnal_Metric_Group_Dim)
      set @varLastMetricGroupKey = coalesce(@varLastMetricGroupKey, 0)



        -- "Inflate" table or not - depending whether scaling grouping should be applied on scaled universe or not
      if object_id('VAggrAnal_tmp_Grouping_Universe') is not null drop table VAggrAnal_tmp_Grouping_Universe
      create table VAggrAnal_tmp_Grouping_Universe (
          Id                                  bigint            not null identity,
          Row_Id                              bigint            null     default null,
          Aggregation_Key                     bigint            not null default 0,
          Account_Number                      varchar(20)       null     default null,
          Median_Scaling_Weight               decimal(30, 6)    null     default 0,
          Metric_Value                        decimal(30, 6)    not null default 0
      )
      create hg index idx0 on VAggrAnal_tmp_Grouping_Universe(Row_Id)
      create hg index idx1 on VAggrAnal_tmp_Grouping_Universe(Account_Number)

      if (@parGroupScaled = 1)
        begin

              if object_id('VAggrAnal_tmp_Scaling_Array') is not null drop table VAggrAnal_tmp_Scaling_Array
              create table VAggrAnal_tmp_Scaling_Array (
                    Id                                  bigint            identity,
                    Row_Id                              bigint            default null)
              create        hg index idx01 on VAggrAnal_tmp_Scaling_Array(Row_Id)

              insert into VAggrAnal_tmp_Scaling_Array (Row_Id)
                select top 30000
                      row_number() over (order by Id) as Row_Id
                  from VESPA_Shared.Aggr_Account_Attributes
                order by Row_Id
              commit
              execute logger_add_event @varBuildId, 3, 'Scaling array created', @@rowcount


              insert into VAggrAnal_tmp_Grouping_Universe (Row_Id, Aggregation_Key, Account_Number, Median_Scaling_Weight, Metric_Value)
                select
                      arr.Row_Id,
                      fct.Aggregation_Key,
                      fct.Account_Number,
                      acc.Median_Scaling_Weight,
                      fct.Metric_Value
                  from VAggrAnal_Account_Attributes acc
                          inner join VAggrAnal_Fact fct       on acc.Account_Number = fct.Account_Number
                          cross join VAggrAnal_tmp_Scaling_Array arr
                 where arr.Row_Id <= round(acc.Median_Scaling_Weight, 0)
                   and fct.Aggregation_Key = @parAggregationKey
                   and fct.Metric_Value > 0
              commit
              execute logger_add_event @varBuildId, 3, 'Universe table created for SCALED grouping', @@rowcount
        end
      else
        begin
              insert into VAggrAnal_tmp_Grouping_Universe (Row_Id, Aggregation_Key, Account_Number, Median_Scaling_Weight, Metric_Value)
                select
                      fct.Fact_Key,
                      fct.Aggregation_Key,
                      fct.Account_Number,
                      0,
                      fct.Metric_Value
                  from VAggrAnal_Fact fct
                 where fct.Aggregation_Key = @parAggregationKey
                   and fct.Metric_Value > 0
              commit
              execute logger_add_event @varBuildId, 3, 'Universe table created for NON-SCALED grouping', @@rowcount
        end



        -- Process grouping
      if object_id('VAggrAnal_tmp_Grouping_Definitions') is not null drop table VAggrAnal_tmp_Grouping_Definitions
      create table VAggrAnal_tmp_Grouping_Definitions (
          Id                                  bigint            not null identity,
          Metric_Group_Key                    bigint            not null default 0,
          Aggregation_Key                     bigint            not null default 0,
          Group_Name                          varchar(50)       null     default null,
          LL_Bin                              smallint          null     default 0,
          LL_Lower_Boundary                   decimal(30, 6)    null     default 0,
          LL_Upper_Boundary_Orig              decimal(30, 6)    null     default 0,
          LL_Upper_Boundary                   decimal(30, 6)    null     default 0,
          LL_Bin_Name                         varchar(15)       null     default '???'
      )
      create hg index idx0 on VAggrAnal_tmp_Grouping_Definitions(Metric_Group_Key)
      create hg index idx2 on VAggrAnal_tmp_Grouping_Definitions(Aggregation_Key)


        -- Insert "Not eligible", "Excluded" and "Did not watch" groups
      insert into VAggrAnal_tmp_Grouping_Definitions
             (Metric_Group_Key, Aggregation_Key, Group_Name, LL_Lower_Boundary, LL_Upper_Boundary, LL_Bin_Name)
           values (1, @parAggregationKey, 'Not eligible', -3, -3, 'Not eligible')

      insert into VAggrAnal_tmp_Grouping_Definitions
             (Metric_Group_Key, Aggregation_Key, Group_Name, LL_Lower_Boundary, LL_Upper_Boundary, LL_Bin_Name)
           values (2, @parAggregationKey, 'Excluded', -2, -2, 'Excluded')

      insert into VAggrAnal_tmp_Grouping_Definitions
             (Metric_Group_Key, Aggregation_Key, Group_Name, LL_Lower_Boundary, LL_Upper_Boundary, LL_Bin_Name)
           values (3, @parAggregationKey, 'Did not watch', -1, -1, 'Did not watch')
      commit



        -- Insert generic groups
      insert into VAggrAnal_tmp_Grouping_Definitions
             (Metric_Group_Key, Aggregation_Key, Group_Name, LL_Bin, LL_Lower_Boundary, LL_Upper_Boundary_Orig,
              LL_Upper_Boundary, LL_Bin_Name)
        select
              row_number() over (order by a.Aggregation_Key) + @varLastMetricGroupKey as Metric_Group_Key,
              a.Aggregation_Key,
              'Standard grouping - Bins & H/M/L',
              a.LL_Bin,
              min(case
                    when a.LL_Bin = @parNumBins then 0
                      else a.Metric_Value
                  end) as LL_Lower_Boundary,
              max(a.Metric_Value)       as LL_Upper_Boundary_Orig,
              case
                when a.LL_Bin = 1 then 999999999999999.999999
                  else lag(LL_Lower_Boundary, 1) over (partition by a.Aggregation_Key order by LL_Lower_Boundary desc) - 0.000001
              end as LL_Upper_Boundary,
              'Bin ' || repeat('0', 2 - length( cast(a.LL_Bin as varchar(2)) )) || a.LL_Bin
                                        as LL_Bin_Name

          from (select
                      @parAggregationKey      as Aggregation_Key,
                      Metric_Value,
                      ntile(@parNumBins) over (order by Metric_Value desc) as LL_Bin
                  from VAggrAnal_tmp_Grouping_Universe) a
         group by a.Aggregation_Key, a.LL_Bin
         order by a.Aggregation_Key
      commit

      execute logger_add_event @varBuildId, 3, 'Group boundaries calculated', @@rowcount



        -- Update fact records
      update VAggrAnal_Fact base
         set base.Metric_Group_Key = grps.Metric_Group_Key
        from VAggrAnal_tmp_Grouping_Definitions grps
       where base.Aggregation_Key = grps.Aggregation_Key
         and base.Metric_Value between grps.LL_Lower_Boundary and grps.LL_Upper_Boundary
      commit
      execute logger_add_event @varBuildId, 3, 'Fact table records updated', @@rowcount



        -- Pump results to the final table
      insert into VAggrAnal_Metric_Group_Dim
             (Metric_Group_Key, Group_Name, Low_Level_Banding, Low_Level_Banding_Min, Low_Level_Banding_Max)
        select
              Metric_Group_Key,
              Group_Name,
              LL_Bin_Name,
              LL_Lower_Boundary,
              LL_Upper_Boundary
          from VAggrAnal_tmp_Grouping_Definitions
         where Metric_Group_Key > 3
      commit
      execute logger_add_event @varBuildId, 3, 'New DIM records created', @@rowcount



        -- ##############################################################################################################
        -- ##### STEP 2.0 - calculating bin stats                                                                   #####
        -- ##############################################################################################################
      insert into VAggrAnal_Bin_Summaries
             (Aggregation_Key, Aggregation_Name, Bin_Name, Bin_Lower_Boundary, Bin_Upper_Boundary, Bin_Width, Bin_Scaled_Volume,
              Bin_Non_Scaled_Volume, Bin_Non_Scaled_Mean, Bin_Non_Scaled_Median, Bin_Non_Scaled_Stdev, Bin_Non_Scaled_Min, Bin_Non_Scaled_Max)
        select
              a.Aggregation_Key,
              @parAggregationName,
              c.Low_Level_Banding,
              max(c.Low_Level_Banding_Min),
              max(c.Low_Level_Banding_Max),
              max(c.Low_Level_Banding_Max - c.Low_Level_Banding_Min),
              sum(b.Sum_Accounts),
              count(*),
              avg(a.Metric_Value),
              percentile_cont(0.5) within group (order by Metric_Value ASC),
              stddev(a.Metric_Value),
              min(a.Metric_Value),
              max(a.Metric_Value)
          from VAggrAnal_Fact a left join (select
                                                 Account_Number,
                                                 count(*) as Sum_Accounts
                                             from VAggrAnal_tmp_Grouping_Universe
                                            group by Account_Number) b
                   on a.Account_Number = b.Account_Number,
               VAggrAnal_Metric_Group_Dim c
         where a.Aggregation_Key = @parAggregationKey
           and a.Metric_Group_Key = c.Metric_Group_Key
         group by a.Aggregation_Key, c.Low_Level_Banding
         order by c.Low_Level_Banding
      commit
      execute logger_add_event @varBuildId, 3, 'Bin summaries created', @@rowcount



        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '####### VESPA Aggregations [Aggregation groupings] - process completed #######', null
      execute logger_add_event @varBuildId, 3, ' ', null
      commit

end;



-- ##############################################################################################################
-- ##############################################################################################################
-- ##############################################################################################################

