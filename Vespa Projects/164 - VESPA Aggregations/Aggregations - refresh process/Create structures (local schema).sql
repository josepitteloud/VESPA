-- ##############################################################################################################
-- ##### Create working tables (local user)                                                                 #####
-- ##############################################################################################################
-- if object_id('VAggr_01_Account_Attributes') is not null then drop table VAggr_01_Account_Attributes end if;
create table VAggr_01_Account_Attributes (
      Id                                  bigint            identity,
      Account_Number                      varchar(20)                default null,
      Cb_Key_Household                    bigint            null     default null,
      Period_Key                          bigint            not null default 0,
      Panel_Id                            tinyint           not null default 0,

      Median_Scaling_Weight               decimal(15, 6)    null     default 0,
      Median_Scaling_Weight_Date          date              null     default null,
      Scaling_Weight                      decimal(15, 6)    null     default 0,
      Scaling_Weight_Date                 date              null     default null,

      Days_Data_Returned                  smallint          null     default 0,
      Days_Period                         smallint          null     default 0,

        -- Other variables
      Acc_Univ_Model_Score                smallint          null     default 0,

        -- Package/product portfolio variables
      Ent_DTV_Sub                         bit               null     default 0,
      Ent_DTV_Pack_Original               bit               null     default 0,
      Ent_DTV_Pack_Variety                bit               null     default 0,
      Ent_DTV_Pack_Family                 bit               null     default 0,
      Ent_DTV_Prem_Sports                 bit               null     default 0,
      Ent_DTV_Prem_Movies                 bit               null     default 0,
      Ent_HD_Sub                          bit               null     default 0,
      Ent_TV3D_Sub                        bit               null     default 0,
      Ent_ESPN_Sub                        bit               null     default 0,
      Ent_ChelseaTV_Sub                   bit               null     default 0,
      Ent_MUTV_Sub                        bit               null     default 0,
      Ent_MGM_Sub                         bit               null     default 0,
      Ent_PVR_Enabled                     bit               null     default 0,

        -- Package/product movement flags
      Movmt_DTV_Sub                       bit               null     default 0,
      Movmt_DTV_Pack_Original             bit               null     default 0,
      Movmt_DTV_Pack_Variety              bit               null     default 0,
      Movmt_DTV_Pack_Family               bit               null     default 0,
      Movmt_DTV_Prem_Sports               bit               null     default 0,
      Movmt_DTV_Prem_Movies               bit               null     default 0,
      Movmt_HD_Sub                        bit               null     default 0,
      Movmt_TV3D_Sub                      bit               null     default 0,
      Movmt_ESPN_Sub                      bit               null     default 0,
      Movmt_ChelseaTV_Sub                 bit               null     default 0,
      Movmt_MUTV_Sub                      bit               null     default 0,
      Movmt_MGM_Sub                       bit               null     default 0,

      Updated_On                          datetime          not null default timestamp,
      Updated_By                          varchar(30)       not null default user_name()
);

create unique hg index idx01 on VAggr_01_Account_Attributes(Account_Number);
create        hg index idx02 on VAggr_01_Account_Attributes(Cb_Key_Household);
create        hg index idx03 on VAggr_01_Account_Attributes(Period_Key);
grant select on VAggr_01_Account_Attributes to vespa_group_low_security;



-- if object_id('VAggr_02_Viewing_Events') is not null then drop table VAggr_02_Viewing_Events end if;
create table VAggr_02_Viewing_Events (
      pk_viewing_prog_instance_fact       bigint                     default null,
      Account_Number                      varchar(20)                default null,
      Subscriber_Id                       numeric(8, 0)              default null,
      Service_Key                         bigint                     default null,

      Event_Start_Date                    date                       default null,
      Instance_Start_Date                 date                       default null,
      Instance_Start_Date_Time            timestamp                  default null,
      Instance_End_Date_Time              timestamp                  default null,
      Instance_Duration                   int                        default null,

      Prog_Instance_Id                    bigint            null     default null,
      Prog_Instance_Broadcast_Duration    bigint            null     default null,
      Prog_Instance_Viewed_Duration       bigint            null     default null,

      F_Playback                          bit               null     default 0,
      F_Format_HD                         bit               null     default 0,
      F_Format_SD                         bit               null     default 0,
      F_Format_3D                         bit               null     default 0,

      F_CType_Original_Pay                bit               null     default 0,
      F_CType_Variety_Pay                 bit               null     default 0,
      F_CType_Family_Pay                  bit               null     default 0,
      F_CType_O_V_F_Pay                   bit               null     default 0,
      F_CType_O_V_F_FTA                   bit               null     default 0,
      F_CType_O_V_F_Any                   bit               null     default 0,

      F_CType_Retail_Movies               bit               null     default 0,
      F_CType_Retail_ALC_Movies_Pack      bit               null     default 0,
      F_CType_Retail_Sports               bit               null     default 0,
      F_CType_Retail_ALa_Carte            bit               null     default 0,
      F_CType_3rd_Party                   bit               null     default 0,
      F_CType_Pay                         bit               null     default 0,

      F_Genre_Sport                       bit               null     default 0,

       -- Individual genres (non-premium)
      F_Genre_Non_Prem_Children           bit               null     default 0,
      F_Genre_Non_Prem_Movies             bit               null     default 0,
      F_Genre_Non_Prem_News_Documentaries bit               null     default 0,
      F_Genre_Non_Prem_Sports             bit               null     default 0,
      F_Genre_Non_Prem_Action_SciFi       bit               null     default 0,
      F_Genre_Non_Prem_Arts_Lifestyle     bit               null     default 0,
      F_Genre_Non_Prem_Comedy_GameShows   bit               null     default 0,
      F_Genre_Non_Prem_Drama_Crime        bit               null     default 0,

       -- Individual genres (Movies premium)
      F_Genre_Prem_Movies_Action_Adventure bit              null     default 0,
      F_Genre_Prem_Movies_Comedy          bit               null     default 0,
      F_Genre_Prem_Movies_Drama_Romance   bit               null     default 0,
      F_Genre_Prem_Movies_Family          bit               null     default 0,
      F_Genre_Prem_Movies_Horror_Thriller bit               null     default 0,
      F_Genre_Prem_Movies_SciFi_Fantasy   bit               null     default 0,

         -- Individual genres (Sports premium)
      F_Genre_Prem_Sports_American       bit                null     default 0,
      F_Genre_Prem_Sports_Boxing_Wrestling bit               null     default 0,
      F_Genre_Prem_Sports_Cricket        bit                null     default 0,
      F_Genre_Prem_Sports_Football       bit                null     default 0,
      F_Genre_Prem_Sports_Golf           bit                null     default 0,
      F_Genre_Prem_Sports_Motor_Extreme  bit                null     default 0,
      F_Genre_Prem_Sports_Rugby          bit                null     default 0,
      F_Genre_Prem_Sports_Tennis         bit                null     default 0,
      F_Genre_Prem_Sports_Niche_Sport    bit                null     default 0,

        -- Individual channels
      F_Channel_BBC_News                 bit                null     default 0,
      F_Channel_BBC1_BBC2_BBC3_BB4       bit                null     default 0,
      F_Channel_BT_Sports                bit                null     default 0,
      F_Channel_CBeebies                 bit                null     default 0,
      F_Channel_Channel_4                bit                null     default 0,
      F_Channel_Channel_5                bit                null     default 0,
      F_Channel_Dave                     bit                null     default 0,
      F_Channel_Discovery_All            bit                null     default 0,
      F_Channel_History                  bit                null     default 0,
      F_Channel_ITV_All                  bit                null     default 0,
      F_Channel_MTV                      bit                null     default 0,
      F_Channel_NatGeo_All               bit                null     default 0,
      F_Channel_Sky_1                    bit                null     default 0,
      F_Channel_Sky_Atlantic             bit                null     default 0,
      F_Channel_Sky_Living               bit                null     default 0,
      F_Channel_Sky_News                 bit                null     default 0,
      F_Channel_Sky_Sports_1             bit                null     default 0,
      F_Channel_Sky_Sports_2             bit                null     default 0,
      F_Channel_Sky_Sports_F1            bit                null     default 0,
      F_Channel_Watch                    bit                null     default 0

);

create   hg index idx01 on VAggr_02_Viewing_Events(pk_viewing_prog_instance_fact);
create   hg index idx02 on VAggr_02_Viewing_Events(Account_Number);
create   hg index idx03 on VAggr_02_Viewing_Events(Service_Key);
create date index idx04 on VAggr_02_Viewing_Events(Event_Start_Date);
create date index idx05 on VAggr_02_Viewing_Events(Instance_Start_Date);
create dttm index idx06 on VAggr_02_Viewing_Events(Instance_Start_Date_Time);
create   hg index idx07 on VAggr_02_Viewing_Events(Prog_Instance_Id);
grant select on VAggr_02_Viewing_Events to vespa_group_low_security;



-- if object_id('VAggr_02_Viewing_Events_Sample') is not null then drop table VAggr_02_Viewing_Events_Sample end if;
create table VAggr_02_Viewing_Events_Sample (
      pk_viewing_prog_instance_fact       bigint                     default null,
      Account_Number                      varchar(20)                default null,
      Subscriber_Id                       numeric(8, 0)              default null,
      Service_Key                         bigint                     default null,

      Event_Start_Date                    date                       default null,
      Instance_Start_Date                 date                       default null,
      Instance_Start_Date_Time            timestamp                  default null,
      Instance_End_Date_Time              timestamp                  default null,
      Instance_Duration                   int                        default null,

      Prog_Instance_Id                    bigint            null     default null,
      Prog_Instance_Broadcast_Duration    bigint            null     default null,
      Prog_Instance_Viewed_Duration       bigint            null     default null,

      F_Playback                          bit               null     default 0,
      F_Format_HD                         bit               null     default 0,
      F_Format_SD                         bit               null     default 0,
      F_Format_3D                         bit               null     default 0,

      F_CType_Original_Pay                bit               null     default 0,
      F_CType_Variety_Pay                 bit               null     default 0,
      F_CType_Family_Pay                  bit               null     default 0,
      F_CType_O_V_F_Pay                   bit               null     default 0,
      F_CType_O_V_F_FTA                   bit               null     default 0,
      F_CType_O_V_F_Any                   bit               null     default 0,

      F_CType_Retail_Movies               bit               null     default 0,
      F_CType_Retail_ALC_Movies_Pack      bit               null     default 0,
      F_CType_Retail_Sports               bit               null     default 0,
      F_CType_Retail_ALa_Carte            bit               null     default 0,
      F_CType_3rd_Party                   bit               null     default 0,
      F_CType_Pay                         bit               null     default 0,

      F_Genre_Sport                       bit               null     default 0,

       -- Individual genres (non-premium)
      F_Genre_Non_Prem_Children           bit               null     default 0,
      F_Genre_Non_Prem_Movies             bit               null     default 0,
      F_Genre_Non_Prem_News_Documentaries bit               null     default 0,
      F_Genre_Non_Prem_Sports             bit               null     default 0,
      F_Genre_Non_Prem_Action_SciFi       bit               null     default 0,
      F_Genre_Non_Prem_Arts_Lifestyle     bit               null     default 0,
      F_Genre_Non_Prem_Comedy_GameShows   bit               null     default 0,
      F_Genre_Non_Prem_Drama_Crime        bit               null     default 0,

       -- Individual genres (Movies premium)
      F_Genre_Prem_Movies_Action_Adventure bit              null     default 0,
      F_Genre_Prem_Movies_Comedy          bit               null     default 0,
      F_Genre_Prem_Movies_Drama_Romance   bit               null     default 0,
      F_Genre_Prem_Movies_Family          bit               null     default 0,
      F_Genre_Prem_Movies_Horror_Thriller bit               null     default 0,
      F_Genre_Prem_Movies_SciFi_Fantasy   bit               null     default 0,

         -- Individual genres (Sports premium)
      F_Genre_Prem_Sports_American       bit                null     default 0,
      F_Genre_Prem_Sports_Boxing_Wrestling bit               null     default 0,
      F_Genre_Prem_Sports_Cricket        bit                null     default 0,
      F_Genre_Prem_Sports_Football       bit                null     default 0,
      F_Genre_Prem_Sports_Golf           bit                null     default 0,
      F_Genre_Prem_Sports_Motor_Extreme  bit                null     default 0,
      F_Genre_Prem_Sports_Rugby          bit                null     default 0,
      F_Genre_Prem_Sports_Tennis         bit                null     default 0,
      F_Genre_Prem_Sports_Niche_Sport    bit                null     default 0,

        -- Individual channels
      F_Channel_BBC_News                 bit                null     default 0,
      F_Channel_BBC1_BBC2_BBC3_BB4       bit                null     default 0,
      F_Channel_BT_Sports                bit                null     default 0,
      F_Channel_CBeebies                 bit                null     default 0,
      F_Channel_Channel_4                bit                null     default 0,
      F_Channel_Channel_5                bit                null     default 0,
      F_Channel_Dave                     bit                null     default 0,
      F_Channel_Discovery_All            bit                null     default 0,
      F_Channel_History                  bit                null     default 0,
      F_Channel_ITV_All                  bit                null     default 0,
      F_Channel_MTV                      bit                null     default 0,
      F_Channel_NatGeo_All               bit                null     default 0,
      F_Channel_Sky_1                    bit                null     default 0,
      F_Channel_Sky_Atlantic             bit                null     default 0,
      F_Channel_Sky_Living               bit                null     default 0,
      F_Channel_Sky_News                 bit                null     default 0,
      F_Channel_Sky_Sports_1             bit                null     default 0,
      F_Channel_Sky_Sports_2             bit                null     default 0,
      F_Channel_Sky_Sports_F1            bit                null     default 0,
      F_Channel_Watch                    bit                null     default 0

);

create   hg index idx01 on VAggr_02_Viewing_Events_Sample(pk_viewing_prog_instance_fact);
create   hg index idx02 on VAggr_02_Viewing_Events_Sample(Account_Number);
create   hg index idx03 on VAggr_02_Viewing_Events_Sample(Service_Key);
create date index idx04 on VAggr_02_Viewing_Events_Sample(Event_Start_Date);
create date index idx05 on VAggr_02_Viewing_Events_Sample(Instance_Start_Date);
create dttm index idx06 on VAggr_02_Viewing_Events_Sample(Instance_Start_Date_Time);
create   hg index idx07 on VAggr_02_Viewing_Events_Sample(Prog_Instance_Id);
grant select on VAggr_02_Viewing_Events_Sample to vespa_group_low_security;



-- if object_id('Aggr_Fact') is not null then drop table Aggr_Fact end if;
create table Aggr_Fact (
      Fact_Key                                bigint            not null identity,
      Period_Key                              bigint            not null default 0,
      Aggregation_Key                         bigint            not null default 0,
      Metric_Group_Key                        bigint            null     default null,
      Account_Number                          varchar(20)       not null,
      Panel_Id                                tinyint           not null default 0,
      Metric_Value                            decimal(30, 6)    not null default 0,
      Updated_On                              datetime          not null default timestamp,
      Updated_By                              varchar(30)       not null default user_name()
);

create        hg index idx01 on Aggr_Fact(Aggregation_Key);
create        hg index idx02 on Aggr_Fact(Period_Key);
create        hg index idx03 on Aggr_Fact(Metric_Group_Key);
create        hg index idx04 on Aggr_Fact(Account_Number);
create        lf index idx05 on Aggr_Fact(Panel_Id);


-- if object_id('VAggr_Fact_Thread_1') is not null then drop table VAggr_Fact_Thread_1 end if;
create table VAggr_Fact_Thread_1 (
      Fact_Key                                bigint            not null identity,
      Period_Key                              bigint            not null default 0,
      Aggregation_Key                         bigint            not null default 0,
      Metric_Group_Key                        bigint            null     default null,
      Account_Number                          varchar(20)       not null,
      Panel_Id                                tinyint           not null default 0,
      Metric_Value                            decimal(30, 6)    not null default 0,
      Updated_On                              datetime          not null default timestamp,
      Updated_By                              varchar(30)       not null default user_name()
);

create        hg index idx01 on VAggr_Fact_Thread_1(Aggregation_Key);
create        hg index idx02 on VAggr_Fact_Thread_1(Period_Key);
create        hg index idx03 on VAggr_Fact_Thread_1(Metric_Group_Key);
create        hg index idx04 on VAggr_Fact_Thread_1(Account_Number);
create        lf index idx05 on VAggr_Fact_Thread_1(Panel_Id);


-- if object_id('VAggr_Fact_Thread_2') is not null then drop table VAggr_Fact_Thread_2 end if;
create table VAggr_Fact_Thread_2 (
      Fact_Key                                bigint            not null identity,
      Period_Key                              bigint            not null default 0,
      Aggregation_Key                         bigint            not null default 0,
      Metric_Group_Key                        bigint            null     default null,
      Account_Number                          varchar(20)       not null,
      Panel_Id                                tinyint           not null default 0,
      Metric_Value                            decimal(30, 6)    not null default 0,
      Updated_On                              datetime          not null default timestamp,
      Updated_By                              varchar(30)       not null default user_name()
);

create        hg index idx01 on VAggr_Fact_Thread_2(Aggregation_Key);
create        hg index idx02 on VAggr_Fact_Thread_2(Period_Key);
create        hg index idx03 on VAggr_Fact_Thread_2(Metric_Group_Key);
create        hg index idx04 on VAggr_Fact_Thread_2(Account_Number);
create        lf index idx05 on VAggr_Fact_Thread_2(Panel_Id);


-- if object_id('VAggr_Fact_Thread_3') is not null then drop table VAggr_Fact_Thread_3 end if;
create table VAggr_Fact_Thread_3 (
      Fact_Key                                bigint            not null identity,
      Period_Key                              bigint            not null default 0,
      Aggregation_Key                         bigint            not null default 0,
      Metric_Group_Key                        bigint            null     default null,
      Account_Number                          varchar(20)       not null,
      Panel_Id                                tinyint           not null default 0,
      Metric_Value                            decimal(30, 6)    not null default 0,
      Updated_On                              datetime          not null default timestamp,
      Updated_By                              varchar(30)       not null default user_name()
);

create        hg index idx01 on VAggr_Fact_Thread_3(Aggregation_Key);
create        hg index idx02 on VAggr_Fact_Thread_3(Period_Key);
create        hg index idx03 on VAggr_Fact_Thread_3(Metric_Group_Key);
create        hg index idx04 on VAggr_Fact_Thread_3(Account_Number);
create        lf index idx05 on VAggr_Fact_Thread_3(Panel_Id);


-- if object_id('VAggr_Fact_Thread_4') is not null then drop table VAggr_Fact_Thread_4 end if;
create table VAggr_Fact_Thread_4 (
      Fact_Key                                bigint            not null identity,
      Period_Key                              bigint            not null default 0,
      Aggregation_Key                         bigint            not null default 0,
      Metric_Group_Key                        bigint            null     default null,
      Account_Number                          varchar(20)       not null,
      Panel_Id                                tinyint           not null default 0,
      Metric_Value                            decimal(30, 6)    not null default 0,
      Updated_On                              datetime          not null default timestamp,
      Updated_By                              varchar(30)       not null default user_name()
);

create        hg index idx01 on VAggr_Fact_Thread_4(Aggregation_Key);
create        hg index idx02 on VAggr_Fact_Thread_4(Period_Key);
create        hg index idx03 on VAggr_Fact_Thread_4(Metric_Group_Key);
create        hg index idx04 on VAggr_Fact_Thread_4(Account_Number);
create        lf index idx05 on VAggr_Fact_Thread_4(Panel_Id);



-- if object_id('Aggr_Metric_Group_Dim') is not null then drop table Aggr_Metric_Group_Dim end if;
create table Aggr_Metric_Group_Dim (
      Metric_Group_Key                        bigint            not null default 0,
      Group_Name                              varchar(50)       null     default null,
      Low_Level_Banding                       varchar(15)       null     default null,
      High_Level_Banding                      varchar(15)       null     default null,
      Low_Level_Banding_Min                   decimal(30, 6)    null     default null,
      Low_Level_Banding_Max                   decimal(30, 6)    null     default null,
      High_Level_Banding_Min                  decimal(30, 6)    null     default null,
      High_Level_Banding_Max                  decimal(30, 6)    null     default null,
      Updated_On                              datetime          not null default timestamp,
      Updated_By                              varchar(30)       not null default user_name()
);

create unique hg index idx01 on Aggr_Metric_Group_Dim(Metric_Group_Key);

insert into Aggr_Metric_Group_Dim (Metric_Group_Key, Group_Name, Low_Level_Banding, High_Level_Banding) values (1, 'Not eligible', 'Not eligible', 'Not eligible');
insert into Aggr_Metric_Group_Dim (Metric_Group_Key, Group_Name, Low_Level_Banding, High_Level_Banding) values (2, 'Excluded', 'Excluded', 'Excluded');
insert into Aggr_Metric_Group_Dim (Metric_Group_Key, Group_Name, Low_Level_Banding, High_Level_Banding) values (3, 'Did not watch', 'Did not watch', 'Did not watch');


-- if object_id('Aggr_Low_Level_Group_Summaries') is not null then drop table Aggr_Low_Level_Group_Summaries end if;
create table Aggr_Low_Level_Group_Summaries (
      Group_Summary_Key                       bigint            not null identity,
      Period_Key                              bigint            not null default 0,
      Aggregation_Key                         bigint            not null default null,
      Metric_Group_Key                        bigint            not null default null,

      Group_Name                              varchar(15)       null     default null,
      Group_Lower_Boundary                    decimal(30, 6)    null     default null,
      Group_Upper_Boundary                    decimal(30, 6)    null     default null,
      Group_Width                             decimal(30, 6)    null     default null,

      Median_Weights_Sum                      bigint            null     default 0,
      Scaling_Weights_Sum                     bigint            null     default 0,
      Non_Scaled_Volume                       bigint            null     default 0,
      Non_Scaled_Mean                         decimal(30, 6)    null     default null,
      Non_Scaled_Median                       decimal(30, 6)    null     default null,
      Non_Scaled_Stdev                        decimal(30, 6)    null     default null,
      Non_Scaled_Min                          decimal(30, 6)    null     default null,
      Non_Scaled_Max                          decimal(30, 6)    null     default null,
      Non_Scaled_Range                        decimal(30, 6)    null     default null,

      Updated_On                              datetime          not null default timestamp,
      Updated_By                              varchar(30)       not null default user_name()
);

create        hg index idx01 on Aggr_Low_Level_Group_Summaries(Period_Key);
create        hg index idx02 on Aggr_Low_Level_Group_Summaries(Aggregation_Key);
create        hg index idx03 on Aggr_Low_Level_Group_Summaries(Metric_Group_Key);
create unique hg index idx04 on Aggr_Low_Level_Group_Summaries(Period_Key, Aggregation_Key, Metric_Group_Key);





















