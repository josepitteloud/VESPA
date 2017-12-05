-- ##############################################################################################################
-- ##### Create metadata tables                                                                             #####
-- ##############################################################################################################
if object_id('VAggr_Meta_Grouping_Rules') is not null then drop table VAggr_Meta_Grouping_Rules end if;
create table VAggr_Meta_Grouping_Rules (
      Id                                  bigint            identity,
      Aggregation_Key                     bigint            not null,
      Group_Scaled                        bit               null     default 1,
      LL_Bin_Number                       smallint          null     default 0,
      HL_Type                             varchar(20)       null     default '???',     -- "binning", "low level based"
      HL_Bin_Number                       smallint          null     default 0,
      HL_Group1_Upper_Cutoff              smallint          null     default 0,
      HL_Group1_Lower_Cutoff              smallint          null     default 0,
      HL_Group1_Label                     varchar(15)       null     default '???',
      HL_Group2_Upper_Cutoff              smallint          null     default 0,
      HL_Group2_Lower_Cutoff              smallint          null     default 0,
      HL_Group2_Label                     varchar(15)       null     default '???',
      HL_Group3_Upper_Cutoff              smallint          null     default 0,
      HL_Group3_Lower_Cutoff              smallint          null     default 0,
      HL_Group3_Label                     varchar(15)       null     default '???',
      HL_Group4_Upper_Cutoff              smallint          null     default 0,
      HL_Group4_Lower_Cutoff              smallint          null     default 0,
      HL_Group4_Label                     varchar(15)       null     default '???',
      HL_Group5_Upper_Cutoff              smallint          null     default 0,
      HL_Group5_Lower_Cutoff              smallint          null     default 0,
      HL_Group5_Label                     varchar(15)       null     default '???',
      Updated_On                          datetime          default timestamp,
      Updated_By                          varchar(30)       default user_name()
);
create unique    index idx00 on VAggr_Meta_Grouping_Rules(Aggregation_Key);
grant select on VAggr_Meta_Grouping_Rules to vespa_group_low_security;



if object_id('VAggr_Meta_Run_Schedule') is not null then drop table VAggr_Meta_Run_Schedule end if;
create table VAggr_Meta_Run_Schedule (
      Id                                  bigint            identity,
      Period_Key                          bigint            not null,
      Aggregation_Key                     bigint            not null,
      Thread_Id                           tinyint           null     default 1,
      Run_Sequence                        tinyint           null     default 99,
      Run_Processed_Flag                  tinyint           not null default 0,
      Grouping_Run                        tinyint           not null default 0,
      Grouping_Processed_Flag             tinyint           not null default 0,
      Updated_On                          datetime          default timestamp,
      Updated_By                          varchar(30)       default user_name()
);
create unique    index idx00 on VAggr_Meta_Run_Schedule(Period_Key, Aggregation_Key);
create        lf index idx01 on VAggr_Meta_Run_Schedule(Thread_Id);
grant select on VAggr_Meta_Run_Schedule to vespa_group_low_security;

if object_id('VAggr_Meta_Run_Schedule_Thread_1') is not null then drop table VAggr_Meta_Run_Schedule_Thread_1 end if;
create table VAggr_Meta_Run_Schedule_Thread_1 (
      Id                                  bigint            not null,
      Period_Key                          bigint            not null,
      Aggregation_Key                     bigint            not null,
      Thread_Id                           tinyint           null     default 1,
      Run_Sequence                        tinyint           null     default 99,
      Run_Processed_Flag                  tinyint           not null default 0,
      Grouping_Run                        tinyint           not null default 0,
      Grouping_Processed_Flag             tinyint           not null default 0,
      Updated_On                          datetime          default timestamp,
      Updated_By                          varchar(30)       default user_name()
);
create unique    index idx00 on VAggr_Meta_Run_Schedule_Thread_1(Period_Key, Aggregation_Key);
create        lf index idx01 on VAggr_Meta_Run_Schedule_Thread_1(Thread_Id);
create unique hg index idx02 on VAggr_Meta_Run_Schedule_Thread_1(Id);
grant select on VAggr_Meta_Run_Schedule_Thread_1 to vespa_group_low_security;

if object_id('VAggr_Meta_Run_Schedule_Thread_2') is not null then drop table VAggr_Meta_Run_Schedule_Thread_2 end if;
create table VAggr_Meta_Run_Schedule_Thread_2 (
      Id                                  bigint            not null,
      Period_Key                          bigint            not null,
      Aggregation_Key                     bigint            not null,
      Thread_Id                           tinyint           null     default 1,
      Run_Sequence                        tinyint           null     default 99,
      Run_Processed_Flag                  tinyint           not null default 0,
      Grouping_Run                        tinyint           not null default 0,
      Grouping_Processed_Flag             tinyint           not null default 0,
      Updated_On                          datetime          default timestamp,
      Updated_By                          varchar(30)       default user_name()
);
create unique    index idx00 on VAggr_Meta_Run_Schedule_Thread_2(Period_Key, Aggregation_Key);
create        lf index idx01 on VAggr_Meta_Run_Schedule_Thread_2(Thread_Id);
create unique hg index idx02 on VAggr_Meta_Run_Schedule_Thread_2(Id);
grant select on VAggr_Meta_Run_Schedule_Thread_2 to vespa_group_low_security;

if object_id('VAggr_Meta_Run_Schedule_Thread_3') is not null then drop table VAggr_Meta_Run_Schedule_Thread_3 end if;
create table VAggr_Meta_Run_Schedule_Thread_3 (
      Id                                  bigint            not null,
      Period_Key                          bigint            not null,
      Aggregation_Key                     bigint            not null,
      Thread_Id                           tinyint           null     default 1,
      Run_Sequence                        tinyint           null     default 99,
      Run_Processed_Flag                  tinyint           not null default 0,
      Grouping_Run                        tinyint           not null default 0,
      Grouping_Processed_Flag             tinyint           not null default 0,
      Updated_On                          datetime          default timestamp,
      Updated_By                          varchar(30)       default user_name()
);
create unique    index idx00 on VAggr_Meta_Run_Schedule_Thread_3(Period_Key, Aggregation_Key);
create        lf index idx01 on VAggr_Meta_Run_Schedule_Thread_3(Thread_Id);
create unique hg index idx02 on VAggr_Meta_Run_Schedule_Thread_3(Id);
grant select on VAggr_Meta_Run_Schedule_Thread_3 to vespa_group_low_security;

if object_id('VAggr_Meta_Run_Schedule_Thread_4') is not null then drop table VAggr_Meta_Run_Schedule_Thread_4 end if;
create table VAggr_Meta_Run_Schedule_Thread_4 (
      Id                                  bigint            not null,
      Period_Key                          bigint            not null,
      Aggregation_Key                     bigint            not null,
      Thread_Id                           tinyint           null     default 1,
      Run_Sequence                        tinyint           null     default 99,
      Run_Processed_Flag                  tinyint           not null default 0,
      Grouping_Run                        tinyint           not null default 0,
      Grouping_Processed_Flag             tinyint           not null default 0,
      Updated_On                          datetime          default timestamp,
      Updated_By                          varchar(30)       default user_name()
);
create unique    index idx00 on VAggr_Meta_Run_Schedule_Thread_4(Period_Key, Aggregation_Key);
create        lf index idx01 on VAggr_Meta_Run_Schedule_Thread_4(Thread_Id);
create unique hg index idx02 on VAggr_Meta_Run_Schedule_Thread_4(Id);
grant select on VAggr_Meta_Run_Schedule_Thread_4 to vespa_group_low_security;



if object_id('VAggr_Meta_Aggr_Definitions') is not null then drop table VAggr_Meta_Aggr_Definitions end if;
create table VAggr_Meta_Aggr_Definitions (
      Aggregation_Key                     bigint            not null,
      Derivation                          varchar(15000)    not null,
      Metric_1                            smallint          not null default 0,
      Metric_2                            smallint          not null default 0,
      Metric_3                            smallint          not null default 0,
      Metric_4                            smallint          not null default 0,
      Metric_5                            smallint          not null default 0,
      Updated_On                          datetime          default timestamp,
      Updated_By                          varchar(30)       default user_name()
);
create unique    index idx00 on VAggr_Meta_Aggr_Definitions(Aggregation_Key);
grant select on VAggr_Meta_Aggr_Definitions to vespa_group_low_security;


































