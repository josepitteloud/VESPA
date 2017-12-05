/*###############################################################################
# Created on:   18/06/2014
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  EPL rights project - Rio survey analysis
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 18/06/2014  SBE   Initial version
# 11/07/2014  SBE   Additional A2 question variations added
#                       Football: 4, Other Sport 4            (provided)
#                       Football: 4, Other Sport 3 or 4       (required)
#                       Football: 4, Other Sport 3            (required)
#                       Football: 3, Other Sport 4            (required)
#                       Football: 3, Other Sport 3 or 4       (required)
#                       Football: 3, Other Sport 3            (required)
#                       Football: 3 or 4, Other Sport 4       (required)
#                       Football: 3 or 4, Other Sport 3 or 4  (provided)
#                       Football: 3 or 4, Other Sport 3       (required)
#
###############################################################################*/


  -- ##############################################################################################################
  -- ##### Create structures                                                                                  #####
  -- ##############################################################################################################
if object_id('EPL_60_Rio_Survey_Results') is not null then drop table EPL_60_Rio_Survey_Results end if;
create table EPL_60_Rio_Survey_Results (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

      -- Account
    Response_Id                             bigint            null      default 0,
    Resp_Id                                 bigint            null      default 0,
    Account_Number                          varchar(30)       null      default null,
    S1_1                                    smallint          null      default null,
    S1_2                                    smallint          null      default null,
    S1_3                                    smallint          null      default null,
    S1_4                                    smallint          null      default null,
    S1_5                                    smallint          null      default null,
    S1_6                                    smallint          null      default null,
    S1_7                                    smallint          null      default null,
    S1_8                                    smallint          null      default null,
    S1_9                                    smallint          null      default null,
    S1_99                                   smallint          null      default null,
    fS2                                     smallint          null      default null,
    fS3                                     smallint          null      default null,
    dAge                                    smallint          null      default null,
    S3a                                     smallint          null      default null,
    S3b                                     smallint          null      default null,
    S3c                                     smallint          null      default null,
    S4a                                     smallint          null      default null,
    S5b_1                                   smallint          null      default null,
    S5b_2                                   smallint          null      default null,
    S5b_3                                   smallint          null      default null,
    S5c_1                                   smallint          null      default null,
    S5c_2                                   smallint          null      default null,
    S5c_3                                   smallint          null      default null,
    S5d_1                                   smallint          null      default null,
    S5d_2                                   smallint          null      default null,
    S5d_3                                   smallint          null      default null,
    S5d_4                                   smallint          null      default null,
    S5d_5                                   smallint          null      default null,
    S5d_6                                   smallint          null      default null,
    S5d_7                                   smallint          null      default null,
    S5d_8                                   smallint          null      default null,
    S6                                      smallint          null      default null,
    DS6                                     smallint          null      default null,
    S8                                      smallint          null      default null,
    S10_1                                   smallint          null      default null,
    S10_3                                   smallint          null      default null,
    S10_5                                   smallint          null      default null,
    dQuotaGroup                             smallint          null      default null,
    dA1a_1                                  smallint          null      default null,
    dA1a_2                                  smallint          null      default null,
    dA1a_3                                  smallint          null      default null,
    dA1a_4                                  smallint          null      default null,
    dA1a_5                                  smallint          null      default null,
    dA1a_6                                  smallint          null      default null,
    A1a_1                                   smallint          null      default null,
    A1a_2                                   smallint          null      default null,
    A1a_3                                   smallint          null      default null,
    A1a_4                                   smallint          null      default null,
    A1a_5                                   smallint          null      default null,
    A1a_6                                   smallint          null      default null,
    A1B_1                                   smallint          null      default null,
    A1B_2                                   smallint          null      default null,
    A1B_3                                   smallint          null      default null,
    fA2_1                                   smallint          null      default null,
    fA2_2                                   smallint          null      default null,
    fA2_3                                   smallint          null      default null,
    fA2_4                                   smallint          null      default null,
    fA2_5                                   smallint          null      default null,
    fA2_6                                   smallint          null      default null,
    fA2_7                                   smallint          null      default null,
    fA2_8                                   smallint          null      default null,
    Other_Sports                            smallint          null      default null,
    A2b_1                                   smallint          null      default null,
    A2b_2                                   smallint          null      default null,
    A2b_3                                   smallint          null      default null,
    A2b_4                                   smallint          null      default null,
    A2b_5                                   smallint          null      default null,
    A2b_6                                   smallint          null      default null,
    A2b_7                                   smallint          null      default null,
    A2b_8                                   smallint          null      default null,
    A2b_9                                   smallint          null      default null,
    A2b_10                                  smallint          null      default null,
    A2b_11                                  smallint          null      default null,
    A2b_12                                  smallint          null      default null,
    A2b_13                                  smallint          null      default null,
    A2b_14                                  smallint          null      default null,
    A2b_15                                  smallint          null      default null,
    A2b_16                                  smallint          null      default null,
    A2b_17                                  smallint          null      default null,
    A2b_18                                  smallint          null      default null,
    A2b_19                                  smallint          null      default null,
    A2b_20                                  smallint          null      default null,
    A2b_21                                  smallint          null      default null,
    A2b_22                                  smallint          null      default null,
    A2b_23                                  smallint          null      default null,
    A2b_24                                  smallint          null      default null,
    A3                                      smallint          null      default null,
    D1                                      smallint          null      default null,
    D4                                      smallint          null      default null,
    D5                                      smallint          null      default null,
    D5a_1                                   smallint          null      default null,
    D5a_2                                   smallint          null      default null,
    D5a_3                                   smallint          null      default null,
    D6a                                     smallint          null      default null,
    D7                                      smallint          null      default null
);
create        hg   index idx01 on EPL_60_Rio_Survey_Results(Response_Id);
create        hg   index idx02 on EPL_60_Rio_Survey_Results(Resp_Id);
create unique hg   index idx03 on EPL_60_Rio_Survey_Results(Account_Number);
grant select on EPL_60_Rio_Survey_Results to vespa_group_low_security;


truncate table EPL_60_Rio_Survey_Results;
load table EPL_60_Rio_Survey_Results
    (Response_Id',', Resp_Id',', Account_Number',', S1_1',', S1_2',', S1_3',', S1_4',', S1_5',', S1_6',', S1_7',', S1_8',',
     S1_9',', S1_99',', fS2',', fS3',', dAge',', S3a',', S3b',', S3c',', S4a',', S5b_1',', S5b_2',', S5b_3',', S5c_1',',
     S5c_2',', S5c_3',', S5d_1',', S5d_2',', S5d_3',', S5d_4',', S5d_5',', S5d_6',', S5d_7',', S5d_8',', S6',', DS6',',
     S8',', S10_1',', S10_3',', S10_5',', dQuotaGroup',', dA1a_1',', dA1a_2',', dA1a_3',', dA1a_4',', dA1a_5',', dA1a_6',',
     A1a_1',', A1a_2',', A1a_3',', A1a_4',', A1a_5',', A1a_6',', A1B_1',', A1B_2',', A1B_3',', fA2_1',', fA2_2',', fA2_3',',
     fA2_4',', fA2_5',', fA2_6',', fA2_7',', fA2_8',', Other_Sports',', A2b_1',', A2b_2',', A2b_3',', A2b_4',', A2b_5',',
     A2b_6',', A2b_7',', A2b_8',', A2b_9',', A2b_10',', A2b_11',', A2b_12',', A2b_13',', A2b_14',', A2b_15',', A2b_16',',
     A2b_17',', A2b_18',', A2b_19',', A2b_20',', A2b_21',', A2b_22',', A2b_23',', A2b_24',', A3',', D1',', D4',', D5',',
     D5a_1',', D5a_2',', D5a_3',', D6a',', D7'\n')
  from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Sebastian/EPL Rio data v02.csv'
  quotes off
  escapes off
  skip 1                        -- file contain header
  -- limit 1000
  notify 1000
  delimited by ',';
commit;

delete from EPL_60_Rio_Survey_Results
 where Response_Id is null
    or Account_Number is null
    or Account_Number = '';
commit;

/*
##### AUDIT #####

-- select * from EPL_60_Rio_Survey_Results;
select 'S1_1' as Var_Name, sum(S1_1) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S1_2' as Var_Name, sum(S1_2) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S1_3' as Var_Name, sum(S1_3) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S1_4' as Var_Name, sum(S1_4) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S1_5' as Var_Name, sum(S1_5) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S1_6' as Var_Name, sum(S1_6) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S1_7' as Var_Name, sum(S1_7) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S1_8' as Var_Name, sum(S1_8) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S1_9' as Var_Name, sum(S1_9) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S1_99' as Var_Name, sum(S1_99) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'fS2' as Var_Name, sum(fS2) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'fS3' as Var_Name, sum(fS3) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'dAge' as Var_Name, sum(dAge) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S3a' as Var_Name, sum(S3a) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S3b' as Var_Name, sum(S3b) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S3c' as Var_Name, sum(S3c) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S4a' as Var_Name, sum(S4a) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S5b_1' as Var_Name, sum(S5b_1) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S5b_2' as Var_Name, sum(S5b_2) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S5b_3' as Var_Name, sum(S5b_3) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S5c_1' as Var_Name, sum(S5c_1) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S5c_2' as Var_Name, sum(S5c_2) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S5c_3' as Var_Name, sum(S5c_3) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S5d_1' as Var_Name, sum(S5d_1) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S5d_2' as Var_Name, sum(S5d_2) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S5d_3' as Var_Name, sum(S5d_3) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S5d_4' as Var_Name, sum(S5d_4) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S5d_5' as Var_Name, sum(S5d_5) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S5d_6' as Var_Name, sum(S5d_6) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S5d_7' as Var_Name, sum(S5d_7) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S5d_8' as Var_Name, sum(S5d_8) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S6' as Var_Name, sum(S6) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'DS6' as Var_Name, sum(DS6) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S8' as Var_Name, sum(S8) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S10_1' as Var_Name, sum(S10_1) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S10_3' as Var_Name, sum(S10_3) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'S10_5' as Var_Name, sum(S10_5) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'dQuotaGroup' as Var_Name, sum(dQuotaGroup) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'dA1a_1' as Var_Name, sum(dA1a_1) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'dA1a_2' as Var_Name, sum(dA1a_2) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'dA1a_3' as Var_Name, sum(dA1a_3) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'dA1a_4' as Var_Name, sum(dA1a_4) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'dA1a_5' as Var_Name, sum(dA1a_5) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'dA1a_6' as Var_Name, sum(dA1a_6) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A1a_1' as Var_Name, sum(A1a_1) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A1a_2' as Var_Name, sum(A1a_2) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A1a_3' as Var_Name, sum(A1a_3) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A1a_4' as Var_Name, sum(A1a_4) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A1a_5' as Var_Name, sum(A1a_5) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A1a_6' as Var_Name, sum(A1a_6) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A1B_1' as Var_Name, sum(A1B_1) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A1B_2' as Var_Name, sum(A1B_2) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A1B_3' as Var_Name, sum(A1B_3) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'fA2_1' as Var_Name, sum(fA2_1) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'fA2_2' as Var_Name, sum(fA2_2) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'fA2_3' as Var_Name, sum(fA2_3) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'fA2_4' as Var_Name, sum(fA2_4) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'fA2_5' as Var_Name, sum(fA2_5) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'fA2_6' as Var_Name, sum(fA2_6) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'fA2_7' as Var_Name, sum(fA2_7) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'fA2_8' as Var_Name, sum(fA2_8) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'Other Sports' as Var_Name, sum(Other_Sports) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A2b_1' as Var_Name, sum(A2b_1) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A2b_2' as Var_Name, sum(A2b_2) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A2b_3' as Var_Name, sum(A2b_3) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A2b_4' as Var_Name, sum(A2b_4) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A2b_5' as Var_Name, sum(A2b_5) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A2b_6' as Var_Name, sum(A2b_6) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A2b_7' as Var_Name, sum(A2b_7) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A2b_8' as Var_Name, sum(A2b_8) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A2b_9' as Var_Name, sum(A2b_9) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A2b_10' as Var_Name, sum(A2b_10) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A2b_11' as Var_Name, sum(A2b_11) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A2b_12' as Var_Name, sum(A2b_12) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A2b_13' as Var_Name, sum(A2b_13) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A2b_14' as Var_Name, sum(A2b_14) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A2b_15' as Var_Name, sum(A2b_15) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A2b_16' as Var_Name, sum(A2b_16) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A2b_17' as Var_Name, sum(A2b_17) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A2b_18' as Var_Name, sum(A2b_18) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A2b_19' as Var_Name, sum(A2b_19) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A2b_20' as Var_Name, sum(A2b_20) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A2b_21' as Var_Name, sum(A2b_21) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A2b_22' as Var_Name, sum(A2b_22) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A2b_23' as Var_Name, sum(A2b_23) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A2b_24' as Var_Name, sum(A2b_24) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'A3' as Var_Name, sum(A3) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'D1' as Var_Name, sum(D1) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'D4' as Var_Name, sum(D4) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'D5' as Var_Name, sum(D5) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'D5a_1' as Var_Name, sum(D5a_1) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'D5a_2' as Var_Name, sum(D5a_2) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'D5a_3' as Var_Name, sum(D5a_3) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'D6a' as Var_Name, sum(D6a) as Var_Value from EPL_60_Rio_Survey_Results union all
select 'D7' as Var_Name, sum(D7) as Var_Value from EPL_60_Rio_Survey_Results;

*/


  -- ##############################################################################################################
  -- ###### Respondent profiling & volumes + weighting                                                       ######
  -- ##############################################################################################################
if object_id('EPL_61_Rio_Survey_Profiles') is not null then drop table EPL_61_Rio_Survey_Profiles end if;
select
      a.Account_Number,
      case
        when b.Account_Number is not null then 1
          else 0
      end as Survey_Respondent,
      a.Latest_Active_Date,
      a.Value_Segment,
      a.Base_Package,
      a.Prem_Movies,
      a.Prem_Sports,
      a.TV_Package,
      a.SkyTalk,
      a.Broadband,
      a.HD,
      a.Multiscreen,
      a.Sky_Product,
      a.Number_Of_Sky_Products,
      a.BT_Sport_Viewier,
      a.Pay_TV_Consumption_Segment,
      a.On_Demand_Usage_Segment,
      a.Sky_Go_EPL_Usage_Segment,
      a.Sky_Go_Any_Usage_Segment,
      a.HH_Composition,
      a.Region,
      a.Affluence_Band,
      a.FSS,
      a.CQM_Score,
      a.Cable_Area,
      a.Sports_Segment_SIG,
      c.Sky_Atlantic_SoV,
      cast(0 as decimal(15, 6)) as Weight

  into EPL_61_Rio_Survey_Profiles
  from EPL_04_Profiling_Variables a left join EPL_60_Rio_Survey_Results b
          on a.Account_Number = b.Account_number,
       EPL_04_Eng_Matrix c
 where a.Period = 1
   and a.Prem_Sports > 0
   and a.Account_Number = c.Account_Number
   and a.Period = c.Period
   and c.Metric = 'Overall';
commit;

if object_id('EPL_62_Rio_Survey_Profiles_Results') is not null then drop table EPL_62_Rio_Survey_Profiles_Results end if;
create table EPL_62_Rio_Survey_Profiles_Results (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

    Profile_Variable                        varchar(50)       null      default '???',
    Variable_Category                       varchar(100)      null      default '???',
    Universe_Volume                         bigint            null      default 0,
    Universe_Volume_Scaled                  bigint            null      default 0,
    Survey_Volume                           bigint            null      default 0,
    Survey_Volume_Weighted                  bigint            null      default 0
);
create        lf   index idx01 on EPL_62_Rio_Survey_Profiles_Results(Profile_Variable);
create        lf   index idx02 on EPL_62_Rio_Survey_Profiles_Results(Variable_Category);
grant select on EPL_62_Rio_Survey_Profiles_Results to vespa_group_low_security;


if object_id('EPL_63_Rio_Survey_Weights') is not null then drop table EPL_63_Rio_Survey_Weights end if;
select
      Broadband,
      case
        when Value_Segment = 'A) Platinum' then 'Platinum'
        when Value_Segment = 'F) Unstable' then 'Unstable'
          else 'Other'
      end as xValue_Segment,

      count(*) as Universe_Volume,
      sum(Survey_Respondent) as Survey_Volume
  into EPL_63_Rio_Survey_Weights
  from EPL_61_Rio_Survey_Profiles
 group by Broadband, xValue_Segment;
commit;


update EPL_61_Rio_Survey_Profiles base
   set base.Weight = 1.0 * 0.008605402 * det.Universe_Volume / det.Survey_Volume        -- 0.008605402: proportion of panellists to the universe
  from EPL_63_Rio_Survey_Weights det
 where base.Broadband = det.Broadband
   and (
        (base.Value_Segment = 'A) Platinum' and det.xValue_Segment = 'Platinum')
        or
        (base.Value_Segment = 'F) Unstable' and det.xValue_Segment = 'Unstable')
        or
        (base.Value_Segment not in ('A) Platinum', 'F) Unstable') and det.xValue_Segment = 'Other')
       )
 ;
commit;



if object_id('EPL_60_Rio_Survey_Profiles') is not null then drop procedure EPL_60_Rio_Survey_Profiles end if;
create procedure EPL_60_Rio_Survey_Profiles
      @parVarOrder              varchar(3) = '',
      @parVariable              varchar(100) = ''
as
begin

      declare @varSQL                         varchar(25000)

      execute logger_add_event 0, 0, '##### Processing variable: ' || @parVariable || ' #####', null

      set @varSQL = '
                      insert into EPL_62_Rio_Survey_Profiles_Results
                            (Profile_Variable, Variable_Category, Universe_Volume, Universe_Volume_Scaled, Survey_Volume, Survey_Volume_Weighted)
                        select
                              ''' || replace(@parVarOrder || ') ' || @parVariable, '_', ' ') || '''
                                                                                      as xProfile_Variable,
                              case
                                when cast(' || @parVariable || ' as varchar(100)) = ''Unknown'' then ''{Unknown}''
                                  else trim(cast(' || @parVariable || ' as varchar(100)))
                              end                                                     as xVariable_Category,
                              count(*)                                                as xUniverse_Volume,
                              sum(Scaling_Weight)                                     as xUniverse_Volume_Scaled,
                              sum(Survey_Respondent)                                  as xSurvey_Volume,
                              sum(Survey_Respondent * Weight)                         as xSurvey_Volume_Weighted
                          from EPL_61_Rio_Survey_Profiles a,
                               EPL_05_Scaling_Weights b
                         where a.Account_Number = b.Account_Number
                           and b.Period = 1
                         group by xVariable_Category
                      commit

                      execute logger_add_event 0, 0, ''Variable "' || replace(@parVariable, '_', ' ') || '" processed'', @@rowcount
                    '
      execute(@varSQL)

end;

truncate table EPL_62_Rio_Survey_Profiles_Results;
execute EPL_60_Rio_Survey_Profiles '001', 'Value_Segment';
execute EPL_60_Rio_Survey_Profiles '002', 'Base_Package';
execute EPL_60_Rio_Survey_Profiles '003', 'Prem_Movies';
execute EPL_60_Rio_Survey_Profiles '004', 'Prem_Sports';
execute EPL_60_Rio_Survey_Profiles '005', 'TV_Package';
execute EPL_60_Rio_Survey_Profiles '006', 'SkyTalk';
execute EPL_60_Rio_Survey_Profiles '007', 'Broadband';
execute EPL_60_Rio_Survey_Profiles '008', 'HD';
execute EPL_60_Rio_Survey_Profiles '009', 'Multiscreen';
execute EPL_60_Rio_Survey_Profiles '010', 'Sky_Product';
execute EPL_60_Rio_Survey_Profiles '011', 'Number_Of_Sky_Products';
execute EPL_60_Rio_Survey_Profiles '012', 'BT_Sport_Viewier';
execute EPL_60_Rio_Survey_Profiles '013', 'Sports_Segment_SIG';
execute EPL_60_Rio_Survey_Profiles '014', 'Sky_Atlantic_SoV';
execute EPL_60_Rio_Survey_Profiles '015', 'Pay_TV_Consumption_Segment';
execute EPL_60_Rio_Survey_Profiles '016', 'On_Demand_Usage_Segment';
execute EPL_60_Rio_Survey_Profiles '017', 'Sky_Go_EPL_Usage_Segment';
execute EPL_60_Rio_Survey_Profiles '018', 'Sky_Go_Any_Usage_Segment';
execute EPL_60_Rio_Survey_Profiles '019', 'HH_Composition';
execute EPL_60_Rio_Survey_Profiles '020', 'Region';
execute EPL_60_Rio_Survey_Profiles '021', 'Affluence_Band';
execute EPL_60_Rio_Survey_Profiles '022', 'FSS';
execute EPL_60_Rio_Survey_Profiles '023', 'CQM_Score';
execute EPL_60_Rio_Survey_Profiles '024', 'Cable_Area';



  -- ##############################################################################################################
  -- ###### Create output pivot                                                                              ######
  -- ##############################################################################################################
if object_id('EPL_65_Rio_Survey_Pivot') is not null then drop table EPL_65_Rio_Survey_Pivot end if;
select
      a.x02_Low_Content_Flag,
      case
        when a.x40_Prem_Sports > 0 then 'Yes'
          else 'No'
      end                                           as x03_Sports_Premium,

        -- Account list provided in a spreadsheet
      case
        when a.x00_Account_Number in ('200003215510', '620034381579', '630038498188', '220027860588', '621110243279', '620022447721',
                                      '210126901003', '220007051836', '220000236699', '200001446802', '621566221266', '630057935748',
                                      '620033561023', '220014705994', '620013919902', '240030296950', '220000262778', '620036853716',
                                      '210060888596', '621010612409', '210025344966', '630006839561', '210108516670', '220015600178',
                                      '621009222319', '630046444216', '220018447361', '200002016943', '620016624459', '220026003297',
                                      '210038887340', '210049631778', '621064170114', '210073560968', '620006141209', '621032989637',
                                      '220015240108', '621081221551', '200004453672', '240019573320', '220019658016', '621020920990',
                                      '620009683090', '621024690052', '621206717921') then 'Yes'
          else 'No'
      end                                           as x04_Group_A_Respondents,

      a.x05a_EPL_SOC__HML,
      a.x07a_EPL_SoSV__HML,
      a.x08a_Sport_SoV__HML,
      a.x09a_Movies_SoV__HML,
      a.x22a_CL_SOC__HML,
      a.x30a_Sky_Atlantic_SoV__HML,

      a.x34_Value_Segment,
      a.x35_Sports_Segment_SIG,
      a.x39_Prem_Movies,
      a.x42_Broadband,
      a.x49_BT_Sport_Viewier,
      a.x57_FSS,

      e.xRisk_Segment_1                             as x81_Risk__All_EPL_Lost__Risk_Group,
      e.xRisk_Segment_3                             as x83_Risk__All_EPL_Lost__Segment,
      e.xRisk_Segment_2                             as x82_Risk__Most_EPL_Lost__Risk_Group,
      e.xRisk_Segment_4                             as x84_Risk__Most_EPL_Lost__Segment,

      -- S10 (movies)
      c.S10_1                                       as x89a_surv__S10_1,
      c.S10_3                                       as x89a_surv__S10_3,
      c.S10_5                                       as x89a_surv__S10_5,

      -- A1A (value of channels to Sky subs)
      c.A1a_1                                       as x89b_surv__A1a_1,
      c.A1a_2                                       as x89b_surv__A1a_2,
      c.A1a_3                                       as x89b_surv__A1a_3,
      c.A1a_4                                       as x89b_surv__A1a_4,
      c.A1a_5                                       as x89b_surv__A1a_5,
      c.A1a_6                                       as x89b_surv__A1a_6,

      -- A1B (channels worth paying for)
      c.A1B_1                                       as x89c_surv__A1B_1,
      c.A1B_2                                       as x89c_surv__A1B_2,
      c.A1B_3                                       as x89c_surv__A1B_3,

      -- A2A (enjoyment of different sports)
      c.fA2_1                                       as x89d_surv__fA2_1,
      c.fA2_2                                       as x89d_surv__fA2_2,
      c.fA2_3                                       as x89d_surv__fA2_3,
      c.fA2_4                                       as x89d_surv__fA2_4,
      c.fA2_5                                       as x89d_surv__fA2_5,
      c.fA2_6                                       as x89d_surv__fA2_6,
      c.fA2_7                                       as x89d_surv__fA2_7,
      c.fA2_8                                       as x89d_surv__fA2_8,
      c.Other_Sports                                as x89d_surv__Other_Sports,

      -- A2B (sports worth paying for)
      c.A2b_1                                       as x89e_surv__A2b_1,
      c.A2b_2                                       as x89e_surv__A2b_2,
      c.A2b_3                                       as x89e_surv__A2b_3,
      c.A2b_4                                       as x89e_surv__A2b_4,
      c.A2b_5                                       as x89e_surv__A2b_5,
      c.A2b_6                                       as x89e_surv__A2b_6,
      c.A2b_7                                       as x89e_surv__A2b_7,
      c.A2b_8                                       as x89e_surv__A2b_8,
      c.A2b_9                                       as x89e_surv__A2b_9,
      c.A2b_10                                      as x89e_surv__A2b_10,
      c.A2b_11                                      as x89e_surv__A2b_11,
      c.A2b_12                                      as x89e_surv__A2b_12,
      c.A2b_13                                      as x89e_surv__A2b_13,
      c.A2b_14                                      as x89e_surv__A2b_14,
      c.A2b_15                                      as x89e_surv__A2b_15,
      c.A2b_16                                      as x89e_surv__A2b_16,
      c.A2b_17                                      as x89e_surv__A2b_17,
      c.A2b_18                                      as x89e_surv__A2b_18,
      c.A2b_19                                      as x89e_surv__A2b_19,
      c.A2b_20                                      as x89e_surv__A2b_20,
      c.A2b_21                                      as x89e_surv__A2b_21,
      c.A2b_22                                      as x89e_surv__A2b_22,
      c.A2b_23                                      as x89e_surv__A2b_23,
      c.A2b_24                                      as x89e_surv__A2b_24,

      -- A3 (viewing to EPL)
      c.A3                                          as x89f_surv__A3,

      sum(a.x90_Accounts_Unscaled)                  as x90_Accounts_Unscaled,
      sum(a.x91_Accounts_Scaled)                    as x91_Accounts_Scaled,
      sum(case
            when b.Account_Number is null then 0
              else b.Survey_Respondent
          end)                                      as x98_Survey_Volume_Unweighted,
      sum(case
            when b.Account_Number is null then 0
              else b.Survey_Respondent * b.Weight
          end)                                      as x99_Survey_Volume_Weighted

  into EPL_65_Rio_Survey_Pivot
  from EPL_10_Results a
          left join EPL_61_Rio_Survey_Profiles b  on a.x00_Account_Number = b.Account_Number
          left join EPL_60_Rio_Survey_Results c   on a.x00_Account_Number = c.Account_Number,
       EPL_07_Risk_Groups_View e
 where a.x03_Metric = 'Overall'
   and a.x00_Account_Number = e.Account_Number
   and e.Period = 1
 group by
      x02_Low_Content_Flag,
      x03_Sports_Premium,
      x04_Group_A_Respondents,

      x05a_EPL_SOC__HML,
      x07a_EPL_SoSV__HML,
      x08a_Sport_SoV__HML,
      x09a_Movies_SoV__HML,
      x22a_CL_SOC__HML,
      x30a_Sky_Atlantic_SoV__HML,

      x34_Value_Segment,
      x35_Sports_Segment_SIG,
      x39_Prem_Movies,
      x42_Broadband,
      x49_BT_Sport_Viewier,
      x57_FSS,

      x81_Risk__All_EPL_Lost__Risk_Group,
      x83_Risk__All_EPL_Lost__Segment,
      x82_Risk__Most_EPL_Lost__Risk_Group,
      x84_Risk__Most_EPL_Lost__Segment,

      x89a_surv__S10_1,
      x89a_surv__S10_3,
      x89a_surv__S10_5,


      x89b_surv__A1a_1,
      x89b_surv__A1a_2,
      x89b_surv__A1a_3,
      x89b_surv__A1a_4,
      x89b_surv__A1a_5,
      x89b_surv__A1a_6,


      x89c_surv__A1B_1,
      x89c_surv__A1B_2,
      x89c_surv__A1B_3,


      x89d_surv__fA2_1,
      x89d_surv__fA2_2,
      x89d_surv__fA2_3,
      x89d_surv__fA2_4,
      x89d_surv__fA2_5,
      x89d_surv__fA2_6,
      x89d_surv__fA2_7,
      x89d_surv__fA2_8,
      x89d_surv__Other_Sports,


      x89e_surv__A2b_1,
      x89e_surv__A2b_2,
      x89e_surv__A2b_3,
      x89e_surv__A2b_4,
      x89e_surv__A2b_5,
      x89e_surv__A2b_6,
      x89e_surv__A2b_7,
      x89e_surv__A2b_8,
      x89e_surv__A2b_9,
      x89e_surv__A2b_10,
      x89e_surv__A2b_11,
      x89e_surv__A2b_12,
      x89e_surv__A2b_13,
      x89e_surv__A2b_14,
      x89e_surv__A2b_15,
      x89e_surv__A2b_16,
      x89e_surv__A2b_17,
      x89e_surv__A2b_18,
      x89e_surv__A2b_19,
      x89e_surv__A2b_20,
      x89e_surv__A2b_21,
      x89e_surv__A2b_22,
      x89e_surv__A2b_23,
      x89e_surv__A2b_24,


      x89f_surv__A3
      ;
commit;


  -- ##############################################################################################################
  -- ###### Analysis of responses to A2 & A3 questions                                                       ######
  -- ##############################################################################################################
if object_id('EPL_66_Rio_Survey_qA2_A3_Analysis') is not null then drop table EPL_66_Rio_Survey_qA2_A3_Analysis end if;
select
      base.Account_Number,
      case
        when e.Prem_Sports > 0 then 'Yes'
          else 'No'
      end as Sports_Premium,
      f.Scaling_Weight,
      case
        when a.Survey_Respondent is null then 0
          else a.Survey_Respondent
      end as Survey_Respondent,
      case
        when a.Survey_Respondent is null then 'All other'
        when a.Survey_Respondent = 0 then 'All other'
          else 'Selected'
      end as Survey_Respondent_Profiling,
      case
        when b.Account_Number is null then 0
          else a.Survey_Respondent * a.Weight
      end as Survey_Respondent_Weighted,

      -- ##### Question A3 ######
      case
        when b.Account_Number is null then '99) Not a survey respondent'
        when b.A3 is null then '98) No answer'
        when b.A3 = 1 then '01) All games: ALL televised matches'
        when b.A3 = 2 then '02) All games: MOST of the televised matches'
        when b.A3 = 3 then '03) All games: SOME of the televised matches'
        when b.A3 = 4 then '04) All games: NONE - I watch them elsewhere'
        when b.A3 = 5 then '05) Important games: ALL televised matches'
        when b.A3 = 6 then '06) Important games: MOST of the televised matches'
        when b.A3 = 7 then '07) Important games: SOME of the televised matches'
        when b.A3 = 8 then '08) Important games: NONE - I watch them elsewhere'
        when b.A3 = 9 then '09) I don''t watch any Premier League matches'
          else '???'
      end as Q_A3_Group,


      -- ##### All requested combinations #####
      --  Football: 4, Other Sport 4            (version 1)
      --  Football: 4, Other Sport 3 or 4       (version 3)
      --  Football: 4, Other Sport 3            (version 4)
      --  Football: 3, Other Sport 4            (version 5)
      --  Football: 3, Other Sport 3 or 4       (version 6)
      --  Football: 3, Other Sport 3            (version 7)
      --  Football: 3 or 4, Other Sport 3 or 4  (version 2)
      --  Football: 3 or 4, Other Sport 4       (version 8)
      --  Football: 3 or 4, Other Sport 3       (version 9)

      -- ##### Question A2 - Football: 4, Other Sport 4 (version 1) #####
      case
        when b.Account_Number is null then '99) Not a survey respondent'
        when b.fA2_3 is null then '98) No answer'

        when b.fA2_3 = 4 and
             b.fA2_1 < 4 and b.fA2_2 < 4 and b.fA2_4 < 4 and b.fA2_5 < 4 and
             b.fA2_6 < 4 and b.fA2_7 < 4 and b.fA2_8 < 4                          then 'Group 1'

        when b.fA2_3 = 4 and
             (
              case when b.fA2_1 = 4 then 1 else 0 end +
              case when b.fA2_2 = 4 then 1 else 0 end +
              case when b.fA2_4 = 4 then 1 else 0 end +
              case when b.fA2_5 = 4 then 1 else 0 end +
              case when b.fA2_6 = 4 then 1 else 0 end +
              case when b.fA2_7 = 4 then 1 else 0 end +
              case when b.fA2_8 = 4 then 1 else 0 end
             ) = 1                                                                then 'Group 2'

        when b.fA2_3 = 4 and
             (
              case when b.fA2_1 = 4 then 1 else 0 end +
              case when b.fA2_2 = 4 then 1 else 0 end +
              case when b.fA2_4 = 4 then 1 else 0 end +
              case when b.fA2_5 = 4 then 1 else 0 end +
              case when b.fA2_6 = 4 then 1 else 0 end +
              case when b.fA2_7 = 4 then 1 else 0 end +
              case when b.fA2_8 = 4 then 1 else 0 end
             ) >= 2                                                               then 'Group 3'

        when b.fA2_3 < 4                                                          then 'Group 4'

          else '???'
      end as Q_A2_Group_v1,


      -- ##### Question A2 - Football: 4, Other Sport 3 or 4 (version 3) #####
      case
        when b.Account_Number is null then '99) Not a survey respondent'
        when b.fA2_3 is null then '98) No answer'

        when b.fA2_3 = 4 and
             b.fA2_1 not in (3, 4) and b.fA2_2 not in (3, 4) and b.fA2_4 not in (3, 4) and
             b.fA2_5 not in (3, 4) and b.fA2_6 not in (3, 4) and b.fA2_7 not in (3, 4) and
             b.fA2_8 not in (3, 4)                                                then 'Group 1'

        when b.fA2_3 = 4 and
             (
              case when b.fA2_1 in (3, 4) then 1 else 0 end +
              case when b.fA2_2 in (3, 4) then 1 else 0 end +
              case when b.fA2_4 in (3, 4) then 1 else 0 end +
              case when b.fA2_5 in (3, 4) then 1 else 0 end +
              case when b.fA2_6 in (3, 4) then 1 else 0 end +
              case when b.fA2_7 in (3, 4) then 1 else 0 end +
              case when b.fA2_8 in (3, 4) then 1 else 0 end
             ) = 1                                                                then 'Group 2'

        when b.fA2_3 = 4 and
             (
              case when b.fA2_1 in (3, 4) then 1 else 0 end +
              case when b.fA2_2 in (3, 4) then 1 else 0 end +
              case when b.fA2_4 in (3, 4) then 1 else 0 end +
              case when b.fA2_5 in (3, 4) then 1 else 0 end +
              case when b.fA2_6 in (3, 4) then 1 else 0 end +
              case when b.fA2_7 in (3, 4) then 1 else 0 end +
              case when b.fA2_8 in (3, 4) then 1 else 0 end
             ) >= 2                                                               then 'Group 3'

        when b.fA2_3 < 4                                                          then 'Group 4'

          else '???'
      end as Q_A2_Group_v3,


      -- ##### Question A2 - Football: 4, Other Sport 3 (version 4) #####
      case
        when b.Account_Number is null then '99) Not a survey respondent'
        when b.fA2_3 is null then '98) No answer'

        when b.fA2_3 = 4 and
             b.fA2_1 not in (3) and b.fA2_2 not in (3) and b.fA2_4 not in (3) and
             b.fA2_5 not in (3) and b.fA2_6 not in (3) and b.fA2_7 not in (3) and
             b.fA2_8 not in (3)                                                   then 'Group 1'

        when b.fA2_3 = 4 and
             (
              case when b.fA2_1 in (3) then 1 else 0 end +
              case when b.fA2_2 in (3) then 1 else 0 end +
              case when b.fA2_4 in (3) then 1 else 0 end +
              case when b.fA2_5 in (3) then 1 else 0 end +
              case when b.fA2_6 in (3) then 1 else 0 end +
              case when b.fA2_7 in (3) then 1 else 0 end +
              case when b.fA2_8 in (3) then 1 else 0 end
             ) = 1                                                                then 'Group 2'

        when b.fA2_3 = 4 and
             (
              case when b.fA2_1 in (3) then 1 else 0 end +
              case when b.fA2_2 in (3) then 1 else 0 end +
              case when b.fA2_4 in (3) then 1 else 0 end +
              case when b.fA2_5 in (3) then 1 else 0 end +
              case when b.fA2_6 in (3) then 1 else 0 end +
              case when b.fA2_7 in (3) then 1 else 0 end +
              case when b.fA2_8 in (3) then 1 else 0 end
             ) >= 2                                                               then 'Group 3'

        when b.fA2_3 < 4                                                          then 'Group 4'

          else '???'
      end as Q_A2_Group_v4,


      -- ##### Question A2 - Football: 3, Other Sport 4 (version 5) #####
      case
        when b.Account_Number is null then '99) Not a survey respondent'
        when b.fA2_3 is null then '98) No answer'

        when b.fA2_3 = 3 and
             b.fA2_1 < 4 and b.fA2_2 < 4 and b.fA2_4 < 4 and b.fA2_5 < 4 and
             b.fA2_6 < 4 and b.fA2_7 < 4 and b.fA2_8 < 4                          then 'Group 1'

        when b.fA2_3 = 3 and
             (
              case when b.fA2_1 = 4 then 1 else 0 end +
              case when b.fA2_2 = 4 then 1 else 0 end +
              case when b.fA2_4 = 4 then 1 else 0 end +
              case when b.fA2_5 = 4 then 1 else 0 end +
              case when b.fA2_6 = 4 then 1 else 0 end +
              case when b.fA2_7 = 4 then 1 else 0 end +
              case when b.fA2_8 = 4 then 1 else 0 end
             ) = 1                                                                then 'Group 2'

        when b.fA2_3 = 3 and
             (
              case when b.fA2_1 = 4 then 1 else 0 end +
              case when b.fA2_2 = 4 then 1 else 0 end +
              case when b.fA2_4 = 4 then 1 else 0 end +
              case when b.fA2_5 = 4 then 1 else 0 end +
              case when b.fA2_6 = 4 then 1 else 0 end +
              case when b.fA2_7 = 4 then 1 else 0 end +
              case when b.fA2_8 = 4 then 1 else 0 end
             ) >= 2                                                               then 'Group 3'

        when b.fA2_3 <> 3                                                         then 'Group 4'

          else '???'
      end as Q_A2_Group_v5,


      -- ##### Question A2 - Football: 3, Other Sport 3 or 4 (version 6) #####
      case
        when b.Account_Number is null then '99) Not a survey respondent'
        when b.fA2_3 is null then '98) No answer'

        when b.fA2_3 = 3 and
             b.fA2_1 not in (3, 4) and b.fA2_2 not in (3, 4) and b.fA2_4 not in (3, 4) and
             b.fA2_5 not in (3, 4) and b.fA2_6 not in (3, 4) and b.fA2_7 not in (3, 4) and
             b.fA2_8 not in (3, 4)                                                then 'Group 1'

        when b.fA2_3 = 3 and
             (
              case when b.fA2_1 in (3, 4) then 1 else 0 end +
              case when b.fA2_2 in (3, 4) then 1 else 0 end +
              case when b.fA2_4 in (3, 4) then 1 else 0 end +
              case when b.fA2_5 in (3, 4) then 1 else 0 end +
              case when b.fA2_6 in (3, 4) then 1 else 0 end +
              case when b.fA2_7 in (3, 4) then 1 else 0 end +
              case when b.fA2_8 in (3, 4) then 1 else 0 end
             ) = 1                                                                then 'Group 2'

        when b.fA2_3 = 3 and
             (
              case when b.fA2_1 in (3, 4) then 1 else 0 end +
              case when b.fA2_2 in (3, 4) then 1 else 0 end +
              case when b.fA2_4 in (3, 4) then 1 else 0 end +
              case when b.fA2_5 in (3, 4) then 1 else 0 end +
              case when b.fA2_6 in (3, 4) then 1 else 0 end +
              case when b.fA2_7 in (3, 4) then 1 else 0 end +
              case when b.fA2_8 in (3, 4) then 1 else 0 end
             ) >= 2                                                               then 'Group 3'

        when b.fA2_3 <> 3                                                         then 'Group 4'

          else '???'
      end as Q_A2_Group_v6,


      -- ##### Question A2 - Football: 3, Other Sport 3 (version 7) #####
      case
        when b.Account_Number is null then '99) Not a survey respondent'
        when b.fA2_3 is null then '98) No answer'

        when b.fA2_3 = 3 and
             b.fA2_1 not in (3) and b.fA2_2 not in (3) and b.fA2_4 not in (3) and
             b.fA2_5 not in (3) and b.fA2_6 not in (3) and b.fA2_7 not in (3) and
             b.fA2_8 not in (3)                                                   then 'Group 1'

        when b.fA2_3 = 3 and
             (
              case when b.fA2_1 in (3) then 1 else 0 end +
              case when b.fA2_2 in (3) then 1 else 0 end +
              case when b.fA2_4 in (3) then 1 else 0 end +
              case when b.fA2_5 in (3) then 1 else 0 end +
              case when b.fA2_6 in (3) then 1 else 0 end +
              case when b.fA2_7 in (3) then 1 else 0 end +
              case when b.fA2_8 in (3) then 1 else 0 end
             ) = 1                                                                then 'Group 2'

        when b.fA2_3 = 3 and
             (
              case when b.fA2_1 in (3) then 1 else 0 end +
              case when b.fA2_2 in (3) then 1 else 0 end +
              case when b.fA2_4 in (3) then 1 else 0 end +
              case when b.fA2_5 in (3) then 1 else 0 end +
              case when b.fA2_6 in (3) then 1 else 0 end +
              case when b.fA2_7 in (3) then 1 else 0 end +
              case when b.fA2_8 in (3) then 1 else 0 end
             ) >= 2                                                               then 'Group 3'

        when b.fA2_3 <> 3                                                         then 'Group 4'

          else '???'
      end as Q_A2_Group_v7,


      -- ##### Question A2 - Football: 3 or 4, Other Sport 3 or 4 (version 2) #####
      case
        when b.Account_Number is null then '99) Not a survey respondent'
        when b.fA2_3 is null then '98) No answer'

        when b.fA2_3 in (3, 4) and
             b.fA2_1 not in (3, 4) and b.fA2_2 not in (3, 4) and b.fA2_4 not in (3, 4) and
             b.fA2_5 not in (3, 4) and b.fA2_6 not in (3, 4) and b.fA2_7 not in (3, 4) and
             b.fA2_8 not in (3, 4)                                                then 'Group 1'

        when b.fA2_3 in (3, 4) and
             (
              case when b.fA2_1 in (3, 4) then 1 else 0 end +
              case when b.fA2_2 in (3, 4) then 1 else 0 end +
              case when b.fA2_4 in (3, 4) then 1 else 0 end +
              case when b.fA2_5 in (3, 4) then 1 else 0 end +
              case when b.fA2_6 in (3, 4) then 1 else 0 end +
              case when b.fA2_7 in (3, 4) then 1 else 0 end +
              case when b.fA2_8 in (3, 4) then 1 else 0 end
             ) = 1                                                                then 'Group 2'

        when b.fA2_3 in (3, 4) and
             (
              case when b.fA2_1 in (3, 4) then 1 else 0 end +
              case when b.fA2_2 in (3, 4) then 1 else 0 end +
              case when b.fA2_4 in (3, 4) then 1 else 0 end +
              case when b.fA2_5 in (3, 4) then 1 else 0 end +
              case when b.fA2_6 in (3, 4) then 1 else 0 end +
              case when b.fA2_7 in (3, 4) then 1 else 0 end +
              case when b.fA2_8 in (3, 4) then 1 else 0 end
             ) >= 2                                                               then 'Group 3'

        when b.fA2_3 < 3                                                          then 'Group 4'

          else '???'
      end as Q_A2_Group_v2,


      -- ##### Question A2 - Football: 3 or 4, Other Sport 4 (version 8) #####
      case
        when b.Account_Number is null then '99) Not a survey respondent'
        when b.fA2_3 is null then '98) No answer'

        when b.fA2_3 in (3, 4) and
             b.fA2_1 not in (4) and b.fA2_2 not in (4) and b.fA2_4 not in (4) and
             b.fA2_5 not in (4) and b.fA2_6 not in (4) and b.fA2_7 not in (4) and
             b.fA2_8 not in (4)                                                   then 'Group 1'

        when b.fA2_3 in (3, 4) and
             (
              case when b.fA2_1 in (4) then 1 else 0 end +
              case when b.fA2_2 in (4) then 1 else 0 end +
              case when b.fA2_4 in (4) then 1 else 0 end +
              case when b.fA2_5 in (4) then 1 else 0 end +
              case when b.fA2_6 in (4) then 1 else 0 end +
              case when b.fA2_7 in (4) then 1 else 0 end +
              case when b.fA2_8 in (4) then 1 else 0 end
             ) = 1                                                                then 'Group 2'

        when b.fA2_3 in (3, 4) and
             (
              case when b.fA2_1 in (4) then 1 else 0 end +
              case when b.fA2_2 in (4) then 1 else 0 end +
              case when b.fA2_4 in (4) then 1 else 0 end +
              case when b.fA2_5 in (4) then 1 else 0 end +
              case when b.fA2_6 in (4) then 1 else 0 end +
              case when b.fA2_7 in (4) then 1 else 0 end +
              case when b.fA2_8 in (4) then 1 else 0 end
             ) >= 2                                                               then 'Group 3'

        when b.fA2_3 < 3                                                          then 'Group 4'

          else '???'
      end as Q_A2_Group_v8,


      -- ##### Question A2 - Football: 3 or 4, Other Sport 3 (version 9) #####
      case
        when b.Account_Number is null then '99) Not a survey respondent'
        when b.fA2_3 is null then '98) No answer'

        when b.fA2_3 in (3, 4) and
             b.fA2_1 not in (3) and b.fA2_2 not in (3) and b.fA2_4 not in (3) and
             b.fA2_5 not in (3) and b.fA2_6 not in (3) and b.fA2_7 not in (3) and
             b.fA2_8 not in (3)                                                   then 'Group 1'

        when b.fA2_3 in (3, 4) and
             (
              case when b.fA2_1 in (3) then 1 else 0 end +
              case when b.fA2_2 in (3) then 1 else 0 end +
              case when b.fA2_4 in (3) then 1 else 0 end +
              case when b.fA2_5 in (3) then 1 else 0 end +
              case when b.fA2_6 in (3) then 1 else 0 end +
              case when b.fA2_7 in (3) then 1 else 0 end +
              case when b.fA2_8 in (3) then 1 else 0 end
             ) = 1                                                                then 'Group 2'

        when b.fA2_3 in (3, 4) and
             (
              case when b.fA2_1 in (3) then 1 else 0 end +
              case when b.fA2_2 in (3) then 1 else 0 end +
              case when b.fA2_4 in (3) then 1 else 0 end +
              case when b.fA2_5 in (3) then 1 else 0 end +
              case when b.fA2_6 in (3) then 1 else 0 end +
              case when b.fA2_7 in (3) then 1 else 0 end +
              case when b.fA2_8 in (3) then 1 else 0 end
             ) >= 2                                                               then 'Group 3'

        when b.fA2_3 < 3                                                          then 'Group 4'

          else '???'
      end as Q_A2_Group_v9,



      -- ###### Demographics etc [HH composition (% of respondents in a ‘Family’)] ######
      case
        when e.HH_Composition in ('Abbreviated female families', 'Abbreviated male families', 'Extended family',
                                  'Extended household', 'Families', 'Pseudo family') then 'Selected'
          else 'All other'
      end as HH_Composition,

      -- ###### Demographics etc [Value segment (% of respondents ‘Unstable’)] ######
      case
        when e.Value_Segment in ('F) Unstable') then 'Selected'
          else 'All other'
      end as Value_Segment,

      -- ###### Demographics etc [FSS (% of respondents in ‘Consolidating assets’, ‘Balancing budgets’, ‘Stretched finances’ or ‘Traditional thrift’)] ######
      case
        when e.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift') then 'Selected'
          else 'All other'
      end as FSS,

      -- ###### Other sports viewing / SIG [% in ‘Cricket’ SIGs (5, 11, 20)] ######
      case
        when e.Sports_Segment_SIG in ('SIG 99 - Unknown') then 'Unknown'
        when e.Sports_Segment_SIG in ('SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans') then 'Selected'
          else 'All other'
      end as SIG_Cricket,

      -- ###### Other sports viewing / SIG [% in ‘Rugby’ SIGs (1, 15)] ######
      case
        when e.Sports_Segment_SIG in ('SIG 99 - Unknown') then 'Unknown'
        when e.Sports_Segment_SIG in ('SIG 15 - Club Rugby Fans', 'SIG 01 - International Rugby Fans') then 'Selected'
          else 'All other'
      end as SIG_Rugby,

      -- ###### Demographics etc [% Sky BB penetration] ######
      case
        when e.Broadband = 'Broadband' then 'Selected'
          else 'All other'
      end as Broadband,

      -- ###### Demographics etc [% of respondents BT Sport viewers] ######
      case
        when e.BT_Sport_Viewier = 'Yes' then 'Selected'
          else 'All other'
      end as BT_Sport_Viewer,

      -- ###### PL matches watched on Sky Sports [Average EPL SoC (note: SS only, excl. BT Sport)] ######
      c.Calculated_SOC as SoC_EPL_Sky_Sports,

      -- ###### Basic content viewing / Atlantic [Average Sky Excl Channels SoV] ######
      d.Calculated_SOV as SoV_Sky_Exclusive_Channels,

      -- ###### Demographics etc [Average no. of Sky products] ######
      e.Number_Of_Sky_Products

  into EPL_66_Rio_Survey_qA2_A3_Analysis
  from EPL_04_Profiling_Variables base
          left join EPL_61_Rio_Survey_Profiles a              on base.Account_Number = a.Account_Number
          left join EPL_60_Rio_Survey_Results b               on base.Account_Number = b.Account_Number
          left join EPL_03_SOCs_Sky_Sports_Only_Summaries c   on base.Account_Number = c.Account_Number
                                                             and c.Metric = 'Live games - overall'
                                                             and c.Period = 1,
       EPL_03_SOVs d,
       EPL_04_Profiling_Variables e,
       EPL_05_Scaling_Weights f

 where base.Period = 1
   and base.Account_Number = d.Account_Number
   and base.Account_Number = e.Account_Number
   and base.Account_Number = f.Account_Number
   and base.Period = d.Period
   and base.Period = e.Period
   and base.Period = f.Period
   and d.Metric = 'Sky exclusive channels SoV'
   ;
commit;


if object_id('EPL_67_Rio_Survey_qA2_A3_Analysis_Results') is not null then drop table EPL_67_Rio_Survey_qA2_A3_Analysis_Results end if;
create table EPL_67_Rio_Survey_qA2_A3_Analysis_Results (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

      -- Account
    Lookup_Key                              varchar(200)      null      default null,
    Breakdown_Type                          varchar(100)      null      default null,
    Profiling_Variable                      varchar(100)      null      default null,
    Q_A3_Group                              varchar(100)      null      default null,
    Q_A2_Group                              varchar(100)      null      default null,
    Cell_Volume                             bigint            null      default 0,
    Metric_Value                            decimal(15, 6)    null      default 0,
    Universe_Total_Unscaled                 decimal(15, 6)    null      default 0,
    Universe_Total_Scaled                   decimal(15, 6)    null      default 0,
    Universe_Sport_Subs_Unscaled            decimal(15, 6)    null      default 0,
    Universe_Sport_Subs_Scaled              decimal(15, 6)    null      default 0,
    Universe_Survey_Resp_Weigthed           decimal(15, 6)    null      default 0
);


if object_id('EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof') is not null then drop procedure EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof end if;
create procedure EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof
      @varBreakdownType         varchar(100) = '',
      @varProfilingVariable     varchar(100) = '',
      @varQA2GroupVariable      varchar(100) = ''
as
begin

      declare @varSQL                         varchar(25000)

      execute logger_add_event 0, 0, '##### Processing: ' || @varBreakdownType || ', Variable: ' || @varProfilingVariable || ' #####', null

      set @varSQL = '
                      delete from EPL_67_Rio_Survey_qA2_A3_Analysis_Results
                       where Breakdown_Type = ''' || @varBreakdownType || '''
                         and Profiling_Variable = ''' || @varProfilingVariable || '''
                      commit
                      execute logger_add_event 0, 0, ''Existing records deleted'', @@rowcount

                      insert into EPL_67_Rio_Survey_qA2_A3_Analysis_Results
                            (Breakdown_Type, Profiling_Variable, Q_A3_Group, Q_A2_Group, Cell_Volume, Metric_Value)
                        select
                              ''' || @varBreakdownType || '''                         as xBreakdown_Type,
                              ''' || @varProfilingVariable || '''                     as xProfile_Variable,
                              trim(a.Q_A3_Group),
                              trim(a.' || @varQA2GroupVariable || ')                  as xQ_A2_Group,
                              sum(b.Cell_Volume)                                      as xCell_Volume,
                              sum(case when b.Sum_Respondents is null then 0 else b.Sum_Respondents end) / sum(a.Sum_Respondents)
                                                                                      as xMetric_Value

                          from (select
                                      Q_A3_Group,
                                      ' || @varQA2GroupVariable || ',
                                      sum(Survey_Respondent_Weighted) as Sum_Respondents
                                  from EPL_66_Rio_Survey_qA2_A3_Analysis
                                 where ' || @varProfilingVariable || ' in (''Selected'', ''All other'')   -- Sum/all
                                   and Survey_Respondent_Weighted > 0
                                 group by Q_A3_Group,
                                          ' || @varQA2GroupVariable || ') a

                                left join

                                (select
                                      Q_A3_Group,
                                      ' || @varQA2GroupVariable || ',
                                      sum(Survey_Respondent) as Cell_Volume,
                                      sum(Survey_Respondent_Weighted) as Sum_Respondents
                                  from EPL_66_Rio_Survey_qA2_A3_Analysis
                                 where ' || @varProfilingVariable || ' in (''Selected'')                  -- Selected category only
                                   and Survey_Respondent_Weighted > 0
                                 group by Q_A3_Group,
                                          ' || @varQA2GroupVariable || ') b

                                on a.Q_A3_Group = b.Q_A3_Group
                               and a.' || @varQA2GroupVariable || '  = b.' || @varQA2GroupVariable || '
                         group by xBreakdown_Type,
                                  xProfile_Variable,
                                  a.Q_A3_Group,
                                  xQ_A2_Group
                      commit
                      execute logger_add_event 0, 0, ''New matrix records added'', @@rowcount


                      update EPL_67_Rio_Survey_qA2_A3_Analysis_Results base
                         set base.Universe_Total_Unscaled       = det.xUniverse_Total_Unscaled,
                             base.Universe_Total_Scaled         = det.xUniverse_Total_Scaled,
                             base.Universe_Sport_Subs_Unscaled  = det.xUniverse_Sport_Subs_Unscaled,
                             base.Universe_Sport_Subs_Scaled    = det.xUniverse_Sport_Subs_Scaled,
                             base.Universe_Survey_Resp_Weigthed = det.xUniverse_Survey_Resp_Weigthed
                        from (select
                                    1.0 * b.Universe_Total_Unscaled / a.Universe_Total_Unscaled             as xUniverse_Total_Unscaled,
                                    b.Universe_Total_Scaled / a.Universe_Total_Scaled                       as xUniverse_Total_Scaled,
                                    1.0 * b.Universe_Sport_Subs_Unscaled / a.Universe_Sport_Subs_Unscaled   as xUniverse_Sport_Subs_Unscaled,
                                    b.Universe_Sport_Subs_Scaled / a.Universe_Sport_Subs_Scaled             as xUniverse_Sport_Subs_Scaled,
                                    b.Universe_Survey_Resp_Weigthed / a.Universe_Survey_Resp_Weigthed       as xUniverse_Survey_Resp_Weigthed
                                from (
                                      select
                                            count(*)                                  as Universe_Total_Unscaled,
                                            sum(Scaling_Weight)                       as Universe_Total_Scaled,
                                            sum(case when Sports_Premium = ''Yes'' then 1 else 0 end)
                                                                                      as Universe_Sport_Subs_Unscaled,
                                            sum(case when Sports_Premium = ''Yes'' then Scaling_Weight else 0 end)
                                                                                      as Universe_Sport_Subs_Scaled,
                                            sum(Survey_Respondent_Weighted)           as Universe_Survey_Resp_Weigthed
                                        from EPL_66_Rio_Survey_qA2_A3_Analysis
                                       where ' || @varProfilingVariable || ' in (''Selected'', ''All other'')
                                     ) a,
                                     (
                                      select
                                            count(*)                                  as Universe_Total_Unscaled,
                                            sum(Scaling_Weight)                       as Universe_Total_Scaled,
                                            sum(case when Sports_Premium = ''Yes'' then 1 else 0 end)
                                                                                      as Universe_Sport_Subs_Unscaled,
                                            sum(case when Sports_Premium = ''Yes'' then Scaling_Weight else 0 end)
                                                                                      as Universe_Sport_Subs_Scaled,
                                            sum(Survey_Respondent_Weighted)           as Universe_Survey_Resp_Weigthed
                                        from EPL_66_Rio_Survey_qA2_A3_Analysis
                                       where ' || @varProfilingVariable || ' in (''Selected'')   -- Sum/all
                                      ) b
                              ) det

                       where base.Breakdown_Type = ''' || @varBreakdownType || '''
                         and base.Profiling_Variable = ''' || @varProfilingVariable || '''
                       commit
                      execute logger_add_event 0, 0, ''Universe values updated'', @@rowcount

                    '
      execute(@varSQL)

end;

-- ##### All requested combinations #####
--  Football: 4, Other Sport 4            (version 1)
--  Football: 4, Other Sport 3 or 4       (version 3)
--  Football: 4, Other Sport 3            (version 4)
--  Football: 3, Other Sport 4            (version 5)
--  Football: 3, Other Sport 3 or 4       (version 6)
--  Football: 3, Other Sport 3            (version 7)
--  Football: 3 or 4, Other Sport 3 or 4  (version 2)
--  Football: 3 or 4, Other Sport 4       (version 8)
--  Football: 3 or 4, Other Sport 3       (version 9)


--  Football: 4, Other Sport 4 (version 1)
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 1', 'Survey_Respondent_Profiling', 'Q_A2_Group_v1';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 1', 'HH_Composition', 'Q_A2_Group_v1';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 1', 'Value_Segment', 'Q_A2_Group_v1';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 1', 'FSS', 'Q_A2_Group_v1';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 1', 'SIG_Cricket', 'Q_A2_Group_v1';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 1', 'SIG_Rugby', 'Q_A2_Group_v1';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 1', 'Broadband', 'Q_A2_Group_v1';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 1', 'BT_Sport_Viewer', 'Q_A2_Group_v1';

--  Football: 4, Other Sport 3 or 4 (version 3)
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 3', 'Survey_Respondent_Profiling', 'Q_A2_Group_v3';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 3', 'HH_Composition', 'Q_A2_Group_v3';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 3', 'Value_Segment', 'Q_A2_Group_v3';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 3', 'FSS', 'Q_A2_Group_v3';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 3', 'SIG_Cricket', 'Q_A2_Group_v3';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 3', 'SIG_Rugby', 'Q_A2_Group_v3';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 3', 'Broadband', 'Q_A2_Group_v3';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 3', 'BT_Sport_Viewer', 'Q_A2_Group_v3';

--  Football: 4, Other Sport 3 (version 4)
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 4', 'Survey_Respondent_Profiling', 'Q_A2_Group_v4';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 4', 'HH_Composition', 'Q_A2_Group_v4';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 4', 'Value_Segment', 'Q_A2_Group_v4';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 4', 'FSS', 'Q_A2_Group_v4';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 4', 'SIG_Cricket', 'Q_A2_Group_v4';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 4', 'SIG_Rugby', 'Q_A2_Group_v4';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 4', 'Broadband', 'Q_A2_Group_v4';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 4', 'BT_Sport_Viewer', 'Q_A2_Group_v4';

--  Football: 3, Other Sport 4 (version 5)
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 5', 'Survey_Respondent_Profiling', 'Q_A2_Group_v5';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 5', 'HH_Composition', 'Q_A2_Group_v5';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 5', 'Value_Segment', 'Q_A2_Group_v5';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 5', 'FSS', 'Q_A2_Group_v5';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 5', 'SIG_Cricket', 'Q_A2_Group_v5';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 5', 'SIG_Rugby', 'Q_A2_Group_v5';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 5', 'Broadband', 'Q_A2_Group_v5';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 5', 'BT_Sport_Viewer', 'Q_A2_Group_v5';

--  Football: 3, Other Sport 3 or 4 (version 6)
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 6', 'Survey_Respondent_Profiling', 'Q_A2_Group_v6';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 6', 'HH_Composition', 'Q_A2_Group_v6';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 6', 'Value_Segment', 'Q_A2_Group_v6';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 6', 'FSS', 'Q_A2_Group_v6';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 6', 'SIG_Cricket', 'Q_A2_Group_v6';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 6', 'SIG_Rugby', 'Q_A2_Group_v6';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 6', 'Broadband', 'Q_A2_Group_v6';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 6', 'BT_Sport_Viewer', 'Q_A2_Group_v6';

--  Football: 3, Other Sport 3 (version 7)
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 7', 'Survey_Respondent_Profiling', 'Q_A2_Group_v7';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 7', 'HH_Composition', 'Q_A2_Group_v7';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 7', 'Value_Segment', 'Q_A2_Group_v7';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 7', 'FSS', 'Q_A2_Group_v7';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 7', 'SIG_Cricket', 'Q_A2_Group_v7';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 7', 'SIG_Rugby', 'Q_A2_Group_v7';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 7', 'Broadband', 'Q_A2_Group_v7';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 7', 'BT_Sport_Viewer', 'Q_A2_Group_v7';

--  Football: 3 or 4, Other Sport 3 or 4 (version 2)
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 2', 'Survey_Respondent_Profiling', 'Q_A2_Group_v2';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 2', 'HH_Composition', 'Q_A2_Group_v2';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 2', 'Value_Segment', 'Q_A2_Group_v2';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 2', 'FSS', 'Q_A2_Group_v2';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 2', 'SIG_Cricket', 'Q_A2_Group_v2';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 2', 'SIG_Rugby', 'Q_A2_Group_v2';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 2', 'Broadband', 'Q_A2_Group_v2';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 2', 'BT_Sport_Viewer', 'Q_A2_Group_v2';

--  Football: 3 or 4, Other Sport 4 (version 8)
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 8', 'Survey_Respondent_Profiling', 'Q_A2_Group_v8';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 8', 'HH_Composition', 'Q_A2_Group_v8';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 8', 'Value_Segment', 'Q_A2_Group_v8';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 8', 'FSS', 'Q_A2_Group_v8';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 8', 'SIG_Cricket', 'Q_A2_Group_v8';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 8', 'SIG_Rugby', 'Q_A2_Group_v8';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 8', 'Broadband', 'Q_A2_Group_v8';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 8', 'BT_Sport_Viewer', 'Q_A2_Group_v8';

--  Football: 3 or 4, Other Sport 3 (version 9)
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 9', 'Survey_Respondent_Profiling', 'Q_A2_Group_v9';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 9', 'HH_Composition', 'Q_A2_Group_v9';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 9', 'Value_Segment', 'Q_A2_Group_v9';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 9', 'FSS', 'Q_A2_Group_v9';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 9', 'SIG_Cricket', 'Q_A2_Group_v9';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 9', 'SIG_Rugby', 'Q_A2_Group_v9';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 9', 'Broadband', 'Q_A2_Group_v9';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Prof 'Version 9', 'BT_Sport_Viewer', 'Q_A2_Group_v9';




if object_id('EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg') is not null then drop procedure EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg end if;
create procedure EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg
      @varBreakdownType         varchar(100) = '',
      @varProfilingVariable     varchar(100) = '',
      @varQA2GroupVariable      varchar(100) = ''
as
begin

      declare @varSQL                         varchar(25000)

      execute logger_add_event 0, 0, '##### Processing: ' || @varBreakdownType || ', Variable: ' || @varProfilingVariable || ' #####', null

      set @varSQL = '
                      delete from EPL_67_Rio_Survey_qA2_A3_Analysis_Results
                       where Breakdown_Type = ''' || @varBreakdownType || '''
                         and Profiling_Variable = ''' || @varProfilingVariable || '''
                      commit
                      execute logger_add_event 0, 0, ''Existing records deleted'', @@rowcount

                      insert into EPL_67_Rio_Survey_qA2_A3_Analysis_Results
                            (Breakdown_Type, Profiling_Variable, Q_A3_Group, Q_A2_Group, Cell_Volume, Metric_Value)
                        select
                              ''' || @varBreakdownType || '''                         as xBreakdown_Type,
                              ''' || @varProfilingVariable || '''                     as xProfile_Variable,
                              trim(a.Q_A3_Group),
                              trim(a.' || @varQA2GroupVariable || ')                  as xQ_A2_Group,
                              count(*)                                                as xCell_Volume,
                              sum(a.' || @varProfilingVariable || ') / sum(a.Survey_Respondent_Weighted)
                                                                                      as xMetric_Value

                          from EPL_66_Rio_Survey_qA2_A3_Analysis a
                         where Survey_Respondent_Weighted > 0
                         group by Q_A3_Group,
                                  ' || @varQA2GroupVariable || '
                      commit
                      execute logger_add_event 0, 0, ''New matrix records added'', @@rowcount


                      update EPL_67_Rio_Survey_qA2_A3_Analysis_Results base
                         set base.Universe_Total_Unscaled       = det.Universe_Total_Unscaled,
                             base.Universe_Total_Scaled         = det.Universe_Total_Scaled,
                             base.Universe_Sport_Subs_Unscaled  = det.Universe_Sport_Subs_Unscaled,
                             base.Universe_Sport_Subs_Scaled    = det.Universe_Sport_Subs_Scaled,
                             base.Universe_Survey_Resp_Weigthed = det.Universe_Survey_Resp_Weigthed
                        from (select
                                    1.0 * sum(' || @varProfilingVariable || ') / count(*)           as Universe_Total_Unscaled,
                                    sum(' || @varProfilingVariable || ' * Scaling_Weight) / sum(Scaling_Weight)
                                                                                                    as Universe_Total_Scaled,
                                    1.0 * sum(case when Sports_Premium = ''Yes'' then ' || @varProfilingVariable || ' else 0 end) /
                                      sum(case when Sports_Premium = ''Yes'' then 1 else 0 end)   as Universe_Sport_Subs_Unscaled,

                                    sum(case when Sports_Premium = ''Yes'' then ' || @varProfilingVariable || ' * Scaling_Weight else 0 end) /
                                      sum(case when Sports_Premium = ''Yes'' then Scaling_Weight else 0 end)
                                                                                                    as Universe_Sport_Subs_Scaled,
                                    sum(case when Survey_Respondent_Weighted > 0 then ' || @varProfilingVariable || ' else 0 end) /
                                      sum(Survey_Respondent_Weighted)                               as Universe_Survey_Resp_Weigthed
                                from EPL_66_Rio_Survey_qA2_A3_Analysis) det
                       where base.Breakdown_Type = ''' || @varBreakdownType || '''
                         and base.Profiling_Variable = ''' || @varProfilingVariable || '''
                       commit
                      execute logger_add_event 0, 0, ''Universe values updated'', @@rowcount

                    '
      execute(@varSQL)

end;

-- ##### All requested combinations #####
--  Football: 4, Other Sport 4            (version 1)
--  Football: 4, Other Sport 3 or 4       (version 3)
--  Football: 4, Other Sport 3            (version 4)
--  Football: 3, Other Sport 4            (version 5)
--  Football: 3, Other Sport 3 or 4       (version 6)
--  Football: 3, Other Sport 3            (version 7)
--  Football: 3 or 4, Other Sport 3 or 4  (version 2)
--  Football: 3 or 4, Other Sport 4       (version 8)
--  Football: 3 or 4, Other Sport 3       (version 9)

--  Football: 4, Other Sport 4 (version 1)
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 1', 'SoC_EPL_Sky_Sports', 'Q_A2_Group_v1';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 1', 'SoV_Sky_Exclusive_Channels', 'Q_A2_Group_v1';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 1', 'Number_Of_Sky_Products', 'Q_A2_Group_v1';

--  Football: 4, Other Sport 3 or 4 (version 3)
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 3', 'SoC_EPL_Sky_Sports', 'Q_A2_Group_v3';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 3', 'SoV_Sky_Exclusive_Channels', 'Q_A2_Group_v3';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 3', 'Number_Of_Sky_Products', 'Q_A2_Group_v3';

--  Football: 4, Other Sport 3 (version 4)
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 4', 'SoC_EPL_Sky_Sports', 'Q_A2_Group_v4';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 4', 'SoV_Sky_Exclusive_Channels', 'Q_A2_Group_v4';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 4', 'Number_Of_Sky_Products', 'Q_A2_Group_v4';

--  Football: 3, Other Sport 4 (version 5)
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 5', 'SoC_EPL_Sky_Sports', 'Q_A2_Group_v5';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 5', 'SoV_Sky_Exclusive_Channels', 'Q_A2_Group_v5';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 5', 'Number_Of_Sky_Products', 'Q_A2_Group_v5';

--  Football: 3, Other Sport 3 or 4 (version 6)
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 6', 'SoC_EPL_Sky_Sports', 'Q_A2_Group_v6';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 6', 'SoV_Sky_Exclusive_Channels', 'Q_A2_Group_v6';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 6', 'Number_Of_Sky_Products', 'Q_A2_Group_v6';

--  Football: 3, Other Sport 3 (version 7)
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 7', 'SoC_EPL_Sky_Sports', 'Q_A2_Group_v7';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 7', 'SoV_Sky_Exclusive_Channels', 'Q_A2_Group_v7';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 7', 'Number_Of_Sky_Products', 'Q_A2_Group_v7';

--  Football: 3 or 4, Other Sport 3 or 4 (version 2)
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 2', 'SoC_EPL_Sky_Sports', 'Q_A2_Group_v2';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 2', 'SoV_Sky_Exclusive_Channels', 'Q_A2_Group_v2';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 2', 'Number_Of_Sky_Products', 'Q_A2_Group_v2';

--  Football: 3 or 4, Other Sport 4 (version 8)
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 8', 'SoC_EPL_Sky_Sports', 'Q_A2_Group_v8';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 8', 'SoV_Sky_Exclusive_Channels', 'Q_A2_Group_v8';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 8', 'Number_Of_Sky_Products', 'Q_A2_Group_v8';

--  Football: 3 or 4, Other Sport 3 (version 9)
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 9', 'SoC_EPL_Sky_Sports', 'Q_A2_Group_v9';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 9', 'SoV_Sky_Exclusive_Channels', 'Q_A2_Group_v9';
execute EPL_61_Rio_Survey_qA2_A3_Analysis_Results_Avg 'Version 9', 'Number_Of_Sky_Products', 'Q_A2_Group_v9';



update EPL_67_Rio_Survey_qA2_A3_Analysis_Results
   set Lookup_Key  = Breakdown_Type || Profiling_Variable || Q_A3_Group || Q_A2_Group;
commit;



if object_id('EPL_66_Rio_Survey_qA2_A3_Analysis_Pivot') is not null then drop table EPL_66_Rio_Survey_qA2_A3_Analysis_Pivot end if;
select
      Sports_Premium                                            as x01_Sports_Premium,

      Q_A3_Group                                                as x02_Q_A3_Group,
      Q_A2_Group_v1                                             as x03_Q_A2_Group_v1,
      Q_A2_Group_v2                                             as x04_Q_A2_Group_v2,

      sum(SoC_EPL_Sky_Sports)                                   as x10a_Sum__SoC_EPL_Sky_Sports,
      sum(1.0 * SoC_EPL_Sky_Sports * Scaling_Weight)            as x10b_Sum__SoC_EPL_Sky_Sports_Scaled,
      sum(SoV_Sky_Exclusive_Channels)                           as x11a_Sum__SoV_Sky_Exclusive_Channels,
      sum(1.0 * SoV_Sky_Exclusive_Channels * Scaling_Weight)    as x11b_Sum__SoV_Sky_Exclusive_Channels_Scaled,
      HH_Composition                                            as x12_HH_Composition,
      Value_Segment                                             as x13_Value_Segment,
      sum(Number_Of_Sky_Products)                               as x14a_Sum__Number_Of_Sky_Products,
      sum(1.0 * Number_Of_Sky_Products * Scaling_Weight)        as x14b_Sum__Number_Of_Sky_Products_Scaled,
      FSS                                                       as x15_FSS,
      BT_Sport_Viewer                                           as x16_BT_Sport_Viewer,
      Broadband                                                 as x17_Broadband,
      SIG_Cricket                                               as x18_SIG_Cricket,
      SIG_Rugby                                                 as x19_SIG_Rugby,

      count(*)                                                  as x91_Universe_Unscaled,
      sum(Scaling_Weight)                                       as x92_Universe_Scaled,
      sum(Survey_Respondent)                                    as x93_Respondents,
      sum(Survey_Respondent_Weighted)                           as x94_Respondents_Weighted

  into EPL_66_Rio_Survey_qA2_A3_Analysis_Pivot
  from EPL_66_Rio_Survey_qA2_A3_Analysis

 group by x01_Sports_Premium,
          x02_Q_A3_Group,
          x03_Q_A2_Group_v1,
          x04_Q_A2_Group_v2,

          x12_HH_Composition,
          x13_Value_Segment,
          x15_FSS,
          x16_BT_Sport_Viewer,
          x17_Broadband,
          x18_SIG_Cricket,
          x19_SIG_Rugby;
commit;
































