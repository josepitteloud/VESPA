/*###############################################################################
# Created on:   17/03/2014
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  EPL rights project - Profiling variables
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 17/03/2014  SBE   Initial version
# 04/12/2014  SBE   New variables added
#
###############################################################################*/


  -- ##############################################################################################################
  -- ##### Create structures                                                                                  #####
  -- ##############################################################################################################
if object_id('EPL_04_Go_Streams') is not null then drop table EPL_04_Go_Streams end if;
create table EPL_04_Go_Streams (
    Pk_Identifier                           bigint            identity,

      -- Account
    Account_Number                          varchar(20)       null      default null,
    Account_Id                              varchar(50)       null      default null,
    Go_Stream                               varchar(25)       null      default null,
    Programme                               varchar(100)      null      default null,
    Broadcast_Date                          date              null      default null,
    Streams_Initiated                       bigint            null      default 0,

    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name()
);
create        hg   index idx01 on EPL_04_Go_Streams(Account_Number);
create        hg   index idx02 on EPL_04_Go_Streams(Account_Id);
create        date index idx03 on EPL_04_Go_Streams(Broadcast_Date);


if object_id('EPL_04_Profiling_Variables') is not null then drop table EPL_04_Profiling_Variables end if;
create table EPL_04_Profiling_Variables (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),
    Rand_Num                                decimal(15, 10)   null      default null,

      -- Account
    Account_Number                          varchar(20)       null      default null,
    Period                                  tinyint           null      default 0,
    Latest_Active_Date                      date              null      default null,

      -- Variables
    Value_Segment                           varchar(25)       null      default 'Z) Unknown',

    Sports_Segment_SIG                      varchar(50)       null      default 'SIG 99 - Unknown',
    Sports_Segment_SIG_v1                   varchar(50)       null      default 'Unknown',
    Sports_Segment_SIG_v2                   varchar(50)       null      default 'Unknown',
    Sports_Segment_SIG_v3                   varchar(50)       null      default 'Unknown',
    Sports_Segment_SIG_v4                   varchar(50)       null      default 'Unknown',
    Sports_Segment_SIG_v5                   varchar(50)       null      default 'Unknown',
    Survey__EPL_Main_Reason                 varchar(25)       null      default 'Unknown',
    Survey__Num_Of_Sports_Claimed           varchar(25)       null      default 'Unknown',

    Base_Package                            varchar(25)       null      default 'Unknown',
    Prem_Movies                             smallint          null      default 0,
    Prem_Sports                             smallint          null      default 0,
    TV_Package                              varchar(50)       null      default 'Unknown',

    SkyTalk                                 varchar(25)       null      default 'No SkyTalk',
    Broadband                               varchar(25)       null      default 'No Broadband',
    HD                                      varchar(25)       null      default 'No HD',
    Multiscreen                             varchar(25)       null      default 'No Multiscreen',
    Sky_Product                             varchar(50)       null      default 'Unknown',
    Number_Of_Sky_Products                  tinyint           null      default 0,
    Number_Of_Sky_Products_GO_OD            tinyint           null      default 0,
    Number_Of_Sky_Products_No_BB            tinyint           null      default 0,
    Number_Of_Sky_Products_No_DTV           tinyint           null      default 0,

    Sports_Tenure_Continuous                varchar(25)       null      default 'Unknown',

    Sports_Downgrade_Events_Num             bigint            null      default 0,
    Sports_Downgrade_Event                  varchar(25)       null      default 'Unknown',
    Sports_Upgrade_Events_Num               bigint            null      default 0,
    Sports_Upgrade_Event                    varchar(25)       null      default 'Unknown',

    BT_Sport_Viewier                        varchar(25)       null      default 'Unknown',
    Pay_TV_Consumption_Level                decimal(15, 6)    null      default 0,
    Pay_TV_Consumption_Segment              varchar(25)       null      default 'Unknown',

    On_Demand_Streams_Num                   bigint            null      default 0,
    On_Demand_Programmes_Num                bigint            null      default 0,
    On_Demand_Usage_Segment                 varchar(25)       null      default '1) Non-OnDemand user',

    Sky_Go_EPL_Streams_Num                  bigint            null      default 0,
    Sky_Go_EPL_Programmes_Num               bigint            null      default 0,
    Sky_Go_EPL_Usage_Segment                varchar(25)       null      default '1) Non-Sky Go user',

    Sky_Go_Any_Usage_Segment                varchar(25)       null      default '1) Non-Sky Go user',

    HH_Composition                          varchar(50)       null      default 'Unknown',
    Region                                  varchar(100)      null      default 'Unknown',
    Affluence_Band                          varchar(50)       null      default 'Unknown',
    FSS                                     varchar(50)       null      default '99) Unknown',
    CQM_Score                               varchar(10)       null      default 'Unknown',
    Cable_Area                              varchar(10)       null      default 'Unknown'

);
create        hg   index idx01 on EPL_04_Profiling_Variables(Account_Number);
create        lf   index idx02 on EPL_04_Profiling_Variables(Period);
create        date index idx03 on EPL_04_Profiling_Variables(Latest_Active_Date);
create unique hg   index idx04 on EPL_04_Profiling_Variables(Account_Number, Period);
grant select on EPL_04_Profiling_Variables to vespa_group_low_security;


if object_id('EPL_04_Sports_Interest_Groups') is not null then drop table EPL_04_Sports_Interest_Groups end if;
select
      Account_Number,
      Cluster_Number as SIG_Number,
      cast(case
             when Cluster_Number =  1 then 'SIG 01 - International Rugby Fans'
             when Cluster_Number =  2 then 'SIG 02 - Flower of Scotland'
             when Cluster_Number =  3 then 'SIG 03 - Sports Traditionalists'
             when Cluster_Number =  4 then 'SIG 04 - Football Heartland'
             when Cluster_Number =  5 then 'SIG 05 - Cricket Enthusiasts'
             when Cluster_Number =  6 then 'SIG 06 - Motor Sport Fans'
             when Cluster_Number =  7 then 'SIG 07 - Football Fanatics (Single Provider)'
             when Cluster_Number =  8 then 'SIG 08 - F1 Super Fans'
             when Cluster_Number =  9 then 'SIG 09 - Super Sports Fans'
             when Cluster_Number = 10 then 'SIG 10 - Sports Disengaged'
             when Cluster_Number = 11 then 'SIG 11 - Cricket Fanatics'
             when Cluster_Number = 12 then 'SIG 12 - Football Fanatics (Multi Provider)'
             when Cluster_Number = 13 then 'SIG 13 - Fast Card and Football'
             when Cluster_Number = 14 then 'SIG 14 - Tennis Fans'
             when Cluster_Number = 15 then 'SIG 15 - Club Rugby Fans'
             when Cluster_Number = 16 then 'SIG 16 - FTA Football Fans'
             when Cluster_Number = 17 then 'SIG 17 - Volatile Football Fans'
             when Cluster_Number = 18 then 'SIG 18 - Football and Little Else'
             when Cluster_Number = 19 then 'SIG 19 - Big Name Brands'
             when Cluster_Number = 20 then 'SIG 20 - Cricket Fans'
               else 'SIG 99 - Unknown'
           end as varchar(50)) as SIG
  into EPL_04_Sports_Interest_Groups
  from skoczej.v250_cluster_numbers;
commit;
create unique hg   index idx01 on EPL_04_Sports_Interest_Groups(Account_Number);
-- select count(*), count(distinct Account_Number) from EPL_04_Sports_Interest_Groups;
-- select SIG_Number, SIG, count(*) from EPL_04_Sports_Interest_Groups group by SIG_Number, SIG order by 1, 2;


if object_id('EPL_04_Survey_Data') is not null then drop table EPL_04_Survey_Data end if;
select
      *
  into EPL_04_Survey_Data
  from dbarnett.v250_sports_rights_survey_responses_winscp;
commit;
alter table EPL_04_Survey_Data rename ID_Name to Account_Number;
create unique hg   index idx01 on EPL_04_Survey_Data(Account_Number);
grant select on EPL_04_Survey_Data to vespa_group_low_security;
grant select on EPL_04_Survey_Data to vespa_crouchr;


  -- Add specific interest groups
alter table EPL_04_Survey_Data
  add (Interest_Group__Football       varchar(50) null default '???',
       Interest_Group__Rugby          varchar(50) null default '???',
       Interest_Group__Other_5_7      varchar(50) null default '???',
       Interest_Group__Other_7_Only   varchar(50) null default '???');
commit;

  -- Football
update EPL_04_Survey_Data
   set Interest_Group__Football = case
                                    when trim(Q26_g) like '7%' or trim(Q26_h) like '7%' or trim(Q26_i) like '7%' then '7'
                                    when trim(Q26_g) like '6%' or trim(Q26_h) like '6%' or trim(Q26_i) like '6%' then '5-6'
                                    when trim(Q26_g) like '5%' or trim(Q26_h) like '5%' or trim(Q26_i) like '5%' then '5-6'
                                    when trim(Q26_g) <> '' or trim(Q26_h) <> '' or trim(Q26_i) <> '' then '1-4'
                                      else 'N/A'
                                  end;
commit;
-- select Q26_g, Q26_h, Q26_i, Interest_Group__Football, count(*) from EPL_04_Survey_Data group by Q26_g, Q26_h, Q26_i, Interest_Group__Football order by Q26_g desc, Q26_h desc, Q26_i desc;


  -- Rugby
update EPL_04_Survey_Data
   set Interest_Group__Rugby    = case
                                    when trim(Q26_m) like '7%' or trim(Q26_n) like '7%' then '7'
                                    when trim(Q26_m) like '6%' or trim(Q26_n) like '6%' then '5-6'
                                    when trim(Q26_m) like '5%' or trim(Q26_n) like '5%' then '5-6'
                                    when trim(Q26_m) <> '' or trim(Q26_n) <> '' then '1-4'
                                      else 'N/A'
                                  end;
commit;
-- select Q26_m, Q26_n, Interest_Group__Rugby, count(*) from EPL_04_Survey_Data group by Q26_m, Q26_n, Interest_Group__Rugby order by Q26_m desc, Q26_n desc;


  -- Other (5-7)
update EPL_04_Survey_Data base
   set Interest_Group__Other_5_7    = case
                                        when trim(base.Q26_a) = '' then '9) N/A'
                                        when det.Other_5_7 =  0 then '1) No other 5-7'
                                        when det.Other_5_7 =  1 then '2) One other 5-7'
                                        when det.Other_5_7 >= 2 then '3) Two or more other 5-7'
                                      end
  from (select
              Account_Number,
              case when trim(Q26_a) like '5%' or trim(Q26_a) like '6%' or trim(Q26_a) like '7%' then 1 else 0 end +
              case when trim(Q26_c) like '5%' or trim(Q26_c) like '6%' or trim(Q26_c) like '7%' then 1 else 0 end +
              case when trim(Q26_d) like '5%' or trim(Q26_d) like '6%' or trim(Q26_d) like '7%' then 1 else 0 end +
              case when trim(Q26_e) like '5%' or trim(Q26_e) like '6%' or trim(Q26_e) like '7%' then 1 else 0 end +
              case when trim(Q26_f) like '5%' or trim(Q26_f) like '6%' or trim(Q26_f) like '7%' then 1 else 0 end +
              case when trim(Q26_j) like '5%' or trim(Q26_j) like '6%' or trim(Q26_j) like '7%' then 1 else 0 end +
              case when trim(Q26_l) like '5%' or trim(Q26_l) like '6%' or trim(Q26_l) like '7%' then 1 else 0 end +
              case when trim(Q26_m) like '5%' or trim(Q26_m) like '6%' or trim(Q26_m) like '7%' then 1 else 0 end +
              case when trim(Q26_n) like '5%' or trim(Q26_n) like '6%' or trim(Q26_n) like '7%' then 1 else 0 end +
              case when trim(Q26_o) like '5%' or trim(Q26_o) like '6%' or trim(Q26_o) like '7%' then 1 else 0 end +
              case when trim(Q26_p) like '5%' or trim(Q26_p) like '6%' or trim(Q26_p) like '7%' then 1 else 0 end as Other_5_7

          from EPL_04_Survey_Data) det
 where base.Account_Number = det.Account_Number;
commit;


  -- Other (7 only)
update EPL_04_Survey_Data base
   set Interest_Group__Other_7_Only = case
                                        when trim(base.Q26_a) = '' then '9) N/A'
                                        when det.Other_7_Only =  0 then '1) No other 7'
                                        when det.Other_7_Only =  1 then '2) One other 7'
                                        when det.Other_7_Only >= 2 then '3) Two or more other 7'
                                      end
  from (select
              Account_Number,
              case when trim(Q26_a) like '7%' then 1 else 0 end +
              case when trim(Q26_c) like '7%' then 1 else 0 end +
              case when trim(Q26_d) like '7%' then 1 else 0 end +
              case when trim(Q26_e) like '7%' then 1 else 0 end +
              case when trim(Q26_f) like '7%' then 1 else 0 end +
              case when trim(Q26_j) like '7%' then 1 else 0 end +
              case when trim(Q26_l) like '7%' then 1 else 0 end +
              case when trim(Q26_m) like '7%' then 1 else 0 end +
              case when trim(Q26_n) like '7%' then 1 else 0 end +
              case when trim(Q26_o) like '7%' then 1 else 0 end +
              case when trim(Q26_p) like '7%' then 1 else 0 end as Other_7_Only

          from EPL_04_Survey_Data) det
 where base.Account_Number = det.Account_Number;
commit;
/*
select Account_Number,substr(trim(Q26_a), 1, 1) as Q26_a, substr(trim(Q26_c), 1, 1) as Q26_c, substr(trim(Q26_d), 1, 1) as Q26_d, substr(trim(Q26_e), 1, 1) as Q26_e,
       substr(trim(Q26_f), 1, 1) as Q26_f, substr(trim(Q26_j), 1, 1) as Q26_j, substr(trim(Q26_l), 1, 1) as Q26_l, substr(trim(Q26_m), 1, 1) as Q26_m,
       substr(trim(Q26_n), 1, 1) as Q26_n, substr(trim(Q26_o), 1, 1) as Q26_o, substr(trim(Q26_p), 1, 1) as Q26_p,
       Interest_Group__Other_5_7, Interest_Group__Other_7_Only
  from EPL_04_Survey_Data;
*/



  -- ##############################################################################################################
  -- ##### Create profiling base                                                                              #####
  -- ##############################################################################################################
insert into EPL_04_Profiling_Variables
      (Account_Number, Period)
  select
      Account_Number,
      Period
  from (select
              Period,
              Account_Number
          from EPL_01_Universe
         group by Period, Account_Number
        having max(valid_account_flag) = 1) det;
commit;

create variable @multiplier bigint;
set @multiplier = datepart(millisecond, now()) + 1;

update EPL_04_Profiling_Variables
   set Rand_Num = rand(number(*) * @multiplier);
commit;


  -- ##############################################################################################################
  -- ##### Load Sky Go streams extract                                                                        #####
  -- ##############################################################################################################
truncate table EPL_04_Go_Streams;
load table  EPL_04_Go_Streams
(
  Account_Id'\x09',
  Go_Stream',',
  Programme'[',
  Broadcast_Date'\x09',
  Streams_Initiated'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Sebastian/20140317_Champions_league.txt'
  QUOTES OFF
  ESCAPES OFF
  --LIMIT 100
  STRIP RTRIM;
commit;

update EPL_04_Go_Streams
   set Programme = trim(Programme);
commit;

delete from EPL_04_Go_Streams
 where Account_Id = 'ACCOUNT_ID'
    or Account_Id is null
    or Streams_Initiated is null;
commit;

update EPL_04_Go_Streams base
   set base.Account_Number = det.Account_Number
  from sk_prod.cust_single_account_view det
 where base.Account_Id = det.acct_cust_account_id;
commit;


select count(*) from EPL_04_Go_Streams;
select count(*) from EPL_04_Go_Streams where Streams_Initiated is null;
select count(*) from EPL_04_Go_Streams where Account_Number is null;
select * from EPL_04_Go_Streams;



  -- ##############################################################################################################
  -- ##### Update profiling variables                                                                         #####
  -- ##############################################################################################################
  -- Latest active date
update EPL_04_Profiling_Variables base
   set base.Latest_Active_Date  = case
                                    when det.Last_Effective_To_Dt > '2013-07-31' then cast('2013-07-31' as date)
                                      else det.Last_Effective_To_Dt
                                  end
  from (select
              Account_Number,
              max(Effective_To_Dt) as Last_Effective_To_Dt
          from sk_prod.cust_subs_hist
         where subscription_sub_type = 'DTV Primary Viewing'
           and status_code in ('AC', 'PC', 'AB')
           and Effective_From_Dt <= '2013-07-31'
           and Effective_From_Dt < Effective_To_Dt
         group by Account_Number) det
 where base.Account_Number = det.Account_Number
   and Period = 2;
commit;

update EPL_04_Profiling_Variables base
   set base.Latest_Active_Date  = case
                                    when det.Last_Effective_To_Dt > '2014-02-28' then cast('2014-02-28' as date)
                                      else det.Last_Effective_To_Dt
                                  end
  from (select
              Account_Number,
              max(Effective_To_Dt) as Last_Effective_To_Dt
          from sk_prod.cust_subs_hist
         where subscription_sub_type = 'DTV Primary Viewing'
           and status_code in ('AC', 'PC', 'AB')
           and Effective_From_Dt <= '2014-02-28'
           and Effective_From_Dt < Effective_To_Dt
         group by Account_Number) det
 where base.Account_Number = det.Account_Number
   and Period = 1;
commit;


  -- a. Value segment
update EPL_04_Profiling_Variables base
   set base.Value_Segment = case
                              when det.Value_Segment like '%Platinum%' then 'A) Platinum'
                              when det.Value_Segment like '%Gold%' then 'B) Gold'
                              when det.Value_Segment like '%Silver%' then 'C) Silver'
                              when det.Value_Segment like '%Bronze%' then 'D) Bronze'
                              when det.Value_Segment like '%Copper%' then 'E) Copper'
                              when det.Value_Segment like '%Unstable%' then 'F) Unstable'
                              when det.Value_Segment like '%Bedding In%' then 'G) Bedding In'
                                else 'Z) Unknown'
                            end
  from (select
              case
                when Value_Seg_Date = '2013-07-30' then 2
                when Value_Seg_Date = '2014-03-03' then 1
                  else 0
              end as Period,
              Account_Number,
              max(Value_Segment) as Value_Segment
          from sk_prod.value_segments_five_yrs
         group by Account_Number, Period) det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period;
commit;


  -- b. Sports segment for  segmented accounts (output from Sports Rights Evaluation tool)
update  EPL_04_Profiling_Variables
set    Sports_Segment_SIG                      = 'SIG 99 - Unknown';
commit;

update EPL_04_Profiling_Variables base
   set base.Sports_Segment_SIG = det.SIG
  from EPL_04_Sports_Interest_Groups det
 where base.Account_Number = det.Account_Number;
commit;

  -- 13/11/2014
  -- Flag "KEY SIGs" with random redistribution of unknowns
/*
-- GET PROFILE
select
      prof.Sports_Segment_SIG,
      count(*) as Unscaled_Volume,
      sum(Scaling_Weight) as Scaled_Volume
  from EPL_04_Eng_Matrix det left join EPL_05_Scaling_Weights b  on det.Account_Number = b.Account_Number
                                                                and det.Period = b.Period,
       EPL_04_Profiling_Variables prof
 where det.Metric = 'Overall'
   and det.Account_Number = prof.Account_Number
   and det.Period = prof.Period
   and prof.Prem_Sports > 0
   and det.EPL_SoSV in ('Medium')
 group by prof.Sports_Segment_SIG;
*/

update EPL_04_Profiling_Variables
   set Sports_Segment_SIG_v1  = case
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.01883089 then 'SIG 01 - International Rugby Fans'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.06071739 then 'SIG 02 - Flower of Scotland'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.07692898 then 'SIG 03 - Sports Traditionalists'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.17353447 then 'SIG 04 - Football Heartland'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.22784818 then 'SIG 05 - Cricket Enthusiasts'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.24092375 then 'SIG 06 - Motor Sport Fans'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.33356787 then 'SIG 07 - Football Fanatics (Single Provider)'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.34791498 then 'SIG 08 - F1 Super Fans'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.38536268 then 'SIG 09 - Super Sports Fans'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.4306857  then 'SIG 10 - Sports Disengaged'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.46800326 then 'SIG 11 - Cricket Fanatics'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.51644123 then 'SIG 12 - Football Fanatics (Multi Provider)'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.56656451 then 'SIG 13 - Fast Card and Football'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.57022751 then 'SIG 14 - Tennis Fans'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.60449859 then 'SIG 15 - Club Rugby Fans'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.62947866 then 'SIG 16 - FTA Football Fans'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.77321171 then 'SIG 17 - Volatile Football Fans'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.89036527 then 'SIG 18 - Football and Little Else'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.98829463 then 'SIG 19 - Big Name Brands'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' then 'SIG 20 - Cricket Fans'
                                    else Sports_Segment_SIG
                                end;
commit;

--CHECK
/*
select
      prof.Sports_Segment_SIG_v1,
      count(*) as Unscaled_Volume,
      sum(Scaling_Weight) as Scaled_Volume
  from EPL_04_Eng_Matrix det left join EPL_05_Scaling_Weights b  on det.Account_Number = b.Account_Number
                                                                and det.Period = b.Period,
       EPL_04_Profiling_Variables prof
 where det.Metric = 'Overall'
   and det.Account_Number = prof.Account_Number
   and det.Period = prof.Period
   and prof.Prem_Sports > 0
   and det.EPL_SoSV in ('Medium')
 group by prof.Sports_Segment_SIG_v1;
*/



/*
-- GET PROFILE
select
      prof.Sports_Segment_SIG,
      count(*) as Unscaled_Volume,
      sum(Scaling_Weight) as Scaled_Volume
  from EPL_04_Eng_Matrix det left join EPL_05_Scaling_Weights b  on det.Account_Number = b.Account_Number
                                                                and det.Period = b.Period,
       EPL_04_Profiling_Variables prof
 where det.Metric = 'Overall'
   and det.Account_Number = prof.Account_Number
   and det.Period = prof.Period
   and prof.Prem_Sports > 0
   and det.EPL_SoSV in ('Medium')
   and det.EPL_SOC in ('Medium', 'High')
 group by prof.Sports_Segment_SIG;
*/

update EPL_04_Profiling_Variables
   set Sports_Segment_SIG_v2  = case
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.00687668 then 'SIG 01 - International Rugby Fans'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.0555446 then 'SIG 02 - Flower of Scotland'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.06468152 then 'SIG 03 - Sports Traditionalists'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.17722883 then 'SIG 04 - Football Heartland'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.24049291 then 'SIG 05 - Cricket Enthusiasts'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.24600304 then 'SIG 06 - Motor Sport Fans'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.35383889 then 'SIG 07 - Football Fanatics (Single Provider)'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.36093805 then 'SIG 08 - F1 Super Fans'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.40459393 then 'SIG 09 - Super Sports Fans'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.41364552 then 'SIG 10 - Sports Disengaged'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.45715696 then 'SIG 11 - Cricket Fanatics'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.51335517 then 'SIG 12 - Football Fanatics (Multi Provider)'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.56971863 then 'SIG 13 - Fast Card and Football'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.57277787 then 'SIG 14 - Tennis Fans'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.61008325 then 'SIG 15 - Club Rugby Fans'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.6236893 then 'SIG 16 - FTA Football Fans'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.75885987 then 'SIG 17 - Volatile Football Fans'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.88823404 then 'SIG 18 - Football and Little Else'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 0.98880068 then 'SIG 19 - Big Name Brands'
                                  when Sports_Segment_SIG = 'SIG 99 - Unknown' and Rand_Num <= 1 then 'SIG 20 - Cricket Fans'
                                    else Sports_Segment_SIG
                                end;
commit;
--CHECK
/*
select
      prof.Sports_Segment_SIG_v2,
      count(*) as Unscaled_Volume,
      sum(Scaling_Weight) as Scaled_Volume
  from EPL_04_Eng_Matrix det left join EPL_05_Scaling_Weights b  on det.Account_Number = b.Account_Number
                                                                and det.Period = b.Period,
       EPL_04_Profiling_Variables prof
 where det.Metric = 'Overall'
   and det.Account_Number = prof.Account_Number
   and det.Period = prof.Period
   and prof.Prem_Sports > 0
   and det.EPL_SoSV in ('Medium')
   and det.EPL_SOC in ('Medium', 'High')
 group by prof.Sports_Segment_SIG_v2;
*/

update EPL_04_Profiling_Variables
   set Sports_Segment_SIG_v3  = case
                                  when Sports_Segment_SIG_v1 in ('SIG 01 - International Rugby Fans', 'SIG 06 - Motor Sport Fans', 'SIG 08 - F1 Super Fans',
                                                                 'SIG 09 - Super Sports Fans', 'SIG 10 - Sports Disengaged', 'SIG 11 - Cricket Fanatics',
                                                                 'SIG 14 - Tennis Fans', 'SIG 15 - Club Rugby Fans', 'SIG 20 - Cricket Fans') then 'Low risk SIGs'    -- Key SIGs = Low risk SIGs
                                    else 'High risk SIGs'                                                                                                             -- Non-key SIGs = High risk SIGs
                                end,
       Sports_Segment_SIG_v4  = case
                                  when Sports_Segment_SIG_v2 in ('SIG 01 - International Rugby Fans', 'SIG 06 - Motor Sport Fans', 'SIG 08 - F1 Super Fans',
                                                                 'SIG 09 - Super Sports Fans', 'SIG 10 - Sports Disengaged', 'SIG 11 - Cricket Fanatics',
                                                                 'SIG 14 - Tennis Fans', 'SIG 15 - Club Rugby Fans', 'SIG 20 - Cricket Fans') then 'Low risk SIGs'    -- Key SIGs = Low risk SIGs
                                    else 'High risk SIGs'                                                                                                             -- Non-key SIGs = High risk SIGs
                                end;
commit;

-- select Sports_Segment_SIG_v3, Sports_Segment_SIG_v1, count(*) as Cnt from EPL_04_Profiling_Variables group by Sports_Segment_SIG_v3, Sports_Segment_SIG_v1 order by 1, 2;
-- select Sports_Segment_SIG_v4, Sports_Segment_SIG_v2, count(*) as Cnt from EPL_04_Profiling_Variables group by Sports_Segment_SIG_v4, Sports_Segment_SIG_v2 order by 1, 2;




  -- c. Summary responses from sports research data (being analysed as part of Sports Rights Evaluation tool):
  --    i. Claimed EPL as main reason for Sport subs (Y/N)
  --              Claimed EPL as main reason for Sport subs (Y/N) = where Q33_a
  --              = “Interested and availability would be main factor in decision whether to pay for that channel”
  --    ii. Total no. of other sports claimed as main reasons for Sports subs
  --              Total no. of other sports claimed as main reasons for Sports subs:
  --              Other sports are captured within Q27_a through to Q42_d
  --              = “Interested and availability would be main factor in decision whether to pay for that channel”
update EPL_04_Profiling_Variables base
   set base.Survey__EPL_Main_Reason       = det.EPL_Main_Reason,
       base.Survey__Num_Of_Sports_Claimed = case
                                              when det.Num_Of_Sports_Claimed = 0 then '1) 0 claimed'
                                              when det.Num_Of_Sports_Claimed <= 5 then '2) 1-5 claimed'
                                              when det.Num_Of_Sports_Claimed > 5 then '3) 6 or more claimed'
                                                else '???'
                                            end
  from (select
              Account_Number,
              Q33_a,
              case
                when trim(Q33_a) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 'Yes'
                  else 'No'
              end as EPL_Main_Reason,
              case when trim(Q27_a) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q27_b) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q27_c) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q28_a) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q28_b) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q28_c) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q29_a) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q29_b) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q29_c) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q29_d) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q29_e) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q30_a) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q30_b) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q30_c) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q31_a) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q32_a) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q32_b) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q32_c) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q33_a) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q33_b) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q33_c) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q33_d) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q33_e) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q34_a) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q34_b) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q34_c) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q34_d) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q34_e) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q35_a) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q35_b) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q35_c) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q35_d) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q36_a) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q36_b) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q36_c) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q36_d) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q37_a) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q37_b) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q37_c) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q38_a) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q38_b) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q39_a) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q39_b) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q39_c) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q39_d) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q39_e) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q39_f) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q40_a) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q40_b) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q40_c) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q41_a) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q41_b) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q42_a) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q42_b) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q42_c) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end +
              case when trim(Q42_d) = 'Interested and availability would be main factor in decision whether to pay for that channel' then 1 else 0 end     as Num_Of_Sports_Claimed

          from EPL_04_Survey_Data
          ) det
 where base.Account_Number = det.Account_Number;
commit;


  -- d. Sports tenure (continuous)
  -- f. Upgrade / downgrade during period
if object_id('EPL_tmp_Sports_Ent_Hist') is not null then drop table EPL_tmp_Sports_Ent_Hist end if;
select
      base.Account_Number,
      base.Latest_Active_Date,
      cast(case
             when det.Effective_From_Dt between '2013-02-01' and '2013-07-31' then 1
               else 0
           end as bit) as Period_2,
      cast(case
             when det.Effective_From_Dt between '2013-08-01' and '2014-02-28' then 1
               else 0
           end as bit) as Period_1,
      det.Effective_From_Dt,
      det.Effective_To_Dt,

      lag(Status_Code) over (partition by base.Account_Number, base.Latest_Active_Date order by det.Effective_From_Dt, det.Effective_To_Dt) Prev_Status_Code,
      det.Status_Code,
      cast(case
             when Prev_Status_Code <> det.Status_Code then 1
               else 0
           end as bit) as Status_Changed,

      lag(cel.Prem_Sports) over (partition by base.Account_Number, base.Latest_Active_Date order by det.Effective_From_Dt, det.Effective_To_Dt) Prev_Prem_Sports,
      cel.Prem_Sports,
      cast(case
             when Prev_Prem_Sports <> cel.Prem_Sports then 1
               else 0
           end as bit) as Prem_Sports_Changed,

      case
        when cel.Prem_Sports > 0 and Prev_Prem_Sports = 0 then 1
        when Prev_Status_Code in ('PO', 'SC', 'IT', 'SU', 'EN') and det.Status_Code in ('AC', 'PC', 'AB') then 2
          else 0
      end as Sports_Upgrade_Event,

      case
        when cel.Prem_Sports = 0 and Prev_Prem_Sports > 0 then 1
        when det.Status_Code in ('PO', 'SC') and Prev_Status_Code in ('AC', 'PC', 'AB') then 2
          else 0
      end as Sports_Downgrade_Event

  into EPL_tmp_Sports_Ent_Hist
  from EPL_04_Profiling_Variables base,
       sk_prod.cust_subs_hist det
          left join sk_prod.cust_entitlement_lookup as cel  on det.current_short_description = cel.short_description
 where base.Account_Number = det.Account_Number
   and det.Subscription_Sub_Type = 'DTV Primary Viewing'
   and det.Effective_From_Dt < det.Effective_To_Dt
   and det.Effective_From_Dt <= base.Latest_Active_Date;
commit;
create        hg   index idx01 on EPL_tmp_Sports_Ent_Hist(Account_Number);
create        lf   index idx02 on EPL_tmp_Sports_Ent_Hist(Sports_Upgrade_Event);
create        lf   index idx03 on EPL_tmp_Sports_Ent_Hist(Sports_Downgrade_Event);


  -- Sports tenure (continuous)
update EPL_04_Profiling_Variables base
   set base.Sports_Tenure_Continuous    = case
                                            when datediff(day, det.Sport_Since_Dt, base.Latest_Active_Date) <= 90        then '1) <=3 months'
                                            when datediff(day, det.Sport_Since_Dt, base.Latest_Active_Date) <= 180       then '2) 4-6 months'
                                            when datediff(day, det.Sport_Since_Dt, base.Latest_Active_Date) <= 365       then '3) 7-12 months'
                                            when datediff(day, det.Sport_Since_Dt, base.Latest_Active_Date) <= 2 * 365   then '4) 1-2 years'
                                            when datediff(day, det.Sport_Since_Dt, base.Latest_Active_Date) <= 5 * 365   then '5) 3-5 years'
                                              else '6) 5+ years'
                                          end
  from (select
              Account_Number,
              Latest_Active_Date,
              max(Effective_From_Dt) as Sport_Since_Dt
          from EPL_tmp_Sports_Ent_Hist
         where Sports_Upgrade_Event in (1, 2)                                                -- Upgrades from 0 to 1/2 Sports AND since recent (re)activation
         group by Account_Number, Latest_Active_Date) det
 where base.Account_Number = det.Account_Number
   and base.Latest_Active_Date = det.Latest_Active_Date
   and base.Prem_Sports > 0;
commit;


  -- Upgrade / downgrade during period
update EPL_04_Profiling_Variables base
   set base.Sports_Downgrade_Events_Num = det.Sports_Downgrade_Events,
       base.Sports_Downgrade_Event      = case
                                            when det.Sports_Downgrade_Events > 0 then 'Yes'
                                              else 'Unknown'
                                          end,
       base.Sports_Upgrade_Events_Num   = det.Sports_Upgrade_Events,
       base.Sports_Upgrade_Event        = case
                                            when det.Sports_Upgrade_Events > 0 then 'Yes'
                                              else 'Unknown'
                                          end
  from (select
              Account_Number,
              Latest_Active_Date,
              sum(case
                    when Latest_Active_Date = '2013-07-31' and Period_2 = 1 and Sports_Upgrade_Event = 1 then 1
                    when Latest_Active_Date = '2014-02-28' and Period_1 = 1 and Sports_Upgrade_Event = 1 then 1
                      else 0
                  end) as Sports_Upgrade_Events,
              sum(case
                    when Latest_Active_Date = '2013-07-31' and Period_2 = 1 and Sports_Downgrade_Event = 1 then 1
                    when Latest_Active_Date = '2014-02-28' and Period_1 = 1 and Sports_Downgrade_Event = 1 then 1
                      else 0
                  end) as Sports_Downgrade_Events
          from EPL_tmp_Sports_Ent_Hist
         where Period_1 = 1                                                                 -- Within the period only
            or Period_2 = 1
         group by Account_Number, Latest_Active_Date) det
 where base.Account_Number = det.Account_Number
   and base.Latest_Active_Date = det.Latest_Active_Date;
commit;



  -- e. Package holding (Basic / EE+ / Movies / Dual Movies/Sports / Top Tier)
  -- g. Product holding (including Talk/BB/HD/Multiscreen)
update EPL_04_Profiling_Variables base
   set base.Base_Package    = case
                                when hist.Base_Package  = 1                                                       then 'Ent'        -- Entertainment
                                when hist.Base_Package  = 2                                                       then 'Ent Extra'  -- Entertainment Extra
                                when hist.Base_Package  = 3                                                       then 'Ent Extra+' -- Entertainment Extra Plus
                                  else 'Unknown'
                              end,
       base.Prem_Movies     = hist.Prem_Movies,
       base.Prem_Sports     = hist.Prem_Sports,
       base.SkyTalk         = case
                                when hist.SkyTalk = 1 then 'SkyTalk'
                                  else 'No SkyTalk'
                              end,
       base.Broadband       = case
                                when hist.Broadband = 1 then 'Broadband'
                                  else 'No Broadband'
                              end,
       base.HD              = case
                                when hist.xHD = 1 then 'HD'
                                  else 'No HD'
                              end,
       base.Multiscreen     = case
                                when hist.Multiscreen = 1 then 'Multiscreen'
                                  else 'No Multiscreen'
                              end
  from (select
              a.Account_Number,
              a.Latest_Active_Date,
              max(case
                    when det.subscription_sub_type <> 'DTV Primary Viewing' then 0
                    when cel.mixes = 0                                                                then 1        -- Entertainment
                    when cel.mixes = 1 and (cel.style_culture = 1 or cel.variety = 1)                 then 1        -- Entertainment
                    when cel.mixes = 2 and (cel.style_culture + cel.variety = 2)                      then 1        -- Entertainment
                    when cel.product_sk in      ( 43672,43669,43670,43664,43667,43663,43668,43673,
                                                  43677,43674,43676,43666,43665,43662,43671,43675)    then 3        -- Entertainment Extra Plus
                    when cel.mixes > 0                                                                then 2        -- Entertainment Extra
                      else 0
                  end) as Base_Package,
              max(case
                    when det.subscription_sub_type = 'DTV Primary Viewing' then cel.prem_movies
                      else 0
                  end) as Prem_Movies,
              max(case
                    when det.subscription_sub_type = 'DTV Primary Viewing' then cel.Prem_Sports
                      else 0
                  end) as Prem_Sports,

              max(case
                    when (det.subscription_sub_type = 'SKY TALK SELECT') and
                         (
                            det.status_code = 'A' or
                            (det.status_code = 'FBP' and prev_status_code in ('PC','A')) or
                            (det.status_code = 'RI'  and prev_status_code in ('FBP','A')) or
                            (det.status_code = 'PC'  and prev_status_code = 'A')
                         ) then 1
                      else 0
                  end) as SkyTalk,

              max(case
                    when det.subscription_sub_type = 'Broadband DSL Line' and
                                     (       det.status_code in ('AC','AB')
                                         or (det.status_code='PC' AND det.prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
                                         or (det.status_code='CF' AND det.prev_status_code='PC'                                  )
                                         or (det.status_code='AP' AND det.sale_type='SNS Bulk Migration'                         )
                                      ) then 1
                      else 0
                  end) as Broadband,
              max(case
                    when det.subscription_sub_type = 'DTV HD' and det.status_code in ('AC','AB','PC') then 1
                      else 0
                  end) as xHD,
              max(case
                    when det.subscription_sub_type = 'DTV Extra Subscription' and det.status_code in ('AC','AB','PC') then 1
                      else 0
                  end) as Multiscreen

          from EPL_04_Profiling_Variables a,
               sk_prod.cust_subs_hist det
                    left join sk_prod.cust_entitlement_lookup as cel  on det.current_short_description = cel.short_description
         where a.Account_Number = det.Account_Number
           and det.Effective_From_Dt <= a.Latest_Active_Date
           and det.Effective_To_Dt > a.Latest_Active_Date
           and det.Effective_From_Dt < det.Effective_To_Dt
         group by a.Account_Number, a.Latest_Active_Date) hist
 where base.Account_Number = hist.Account_Number
   and base.Latest_Active_Date = hist.Latest_Active_Date;
commit;

update EPL_04_Profiling_Variables base
   set base.TV_Package      = Base_Package ||
                              case
                                when Prem_Movies > 0 and Prem_Sports > 0 then ' & Top Tier'
                                when Prem_Movies > 0 then ' & Dual Movies'
                                when Prem_Sports > 0 then ' & Dual Sports'
                                  else ' & No Premium'
                              end,
       base.Sky_Product     = 'DTV' ||
                              case
                                when HD not like 'No%' then ', HD'
                                  else ''
                              end ||
                              case
                                when Multiscreen not like 'No%' then ', Multiscreen'
                                  else ''
                              end ||
                              case
                                when Broadband not like 'No%' then ', Broadband'
                                  else ''
                              end;
commit;


  -- h. BT Sport viewer flag
update EPL_04_Profiling_Variables base
   set base.BT_Sport_Viewier              = case
                                              when det.BT_Flag = 1 then 'Yes'
                                                else 'No'
                                            end
  from (select
              Account_Number,
              Period,
              max(case
                    when First_BT_Viewing is null then 0
                      else 1
                  end) as BT_Flag
          from EPL_01_Universe
         group by Account_Number, Period) det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period;
commit;


  -- i. Watches < 1 hour of Pay TV per day flag
if object_id('EPL_tmp_Pay_TV_Viewing') is not null then drop table EPL_tmp_Pay_TV_Viewing end if;
select
      b.Account_Number,
      b.Period,
      sum(case
            when Pay_TV_Type in (1, 2, 3, 4, 5) then Viewing_Duration
              else 0
          end) as Pay_TV_Viewing,
      sum(Viewing_Duration) as Total_Viewing
  into EPL_tmp_Pay_TV_Viewing
  from EPL_02_Viewing_Summary b
 where b.Pay_TV_Type <= 5         -- Exclude PPV
 group by b.Account_Number, b.Period;
commit;
create        hg   index idx01 on EPL_tmp_Pay_TV_Viewing(Account_Number);
create        lf   index idx02 on EPL_tmp_Pay_TV_Viewing(Period);


update EPL_04_Profiling_Variables base
   set base.Pay_TV_Consumption_Level      = 1.0 * det.Pay_TV_Viewing / det.Days_Data_Available,
       base.Pay_TV_Consumption_Segment    = case
                                              when 1.0 * det.Pay_TV_Viewing / det.Days_Data_Available < 60 * 60 then 'Low Pay TV'
                                                else 'Normal'
                                            end
  from (select
              un.Account_Number,
              un.Period,
              un.Days_Data_Available,
              vw.Pay_TV_Viewing,
              vw.Total_Viewing
          from (select
                      Period,
                      Account_Number,
                      Days_Data_Available
                  from EPL_01_Universe
                 group by Period, Account_Number, Days_Data_Available
                having max(valid_account_flag) = 1) un,
               EPL_tmp_Pay_TV_Viewing vw
         where un.Account_Number = vw.Account_Number
           and un.Period = vw.Period) det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period;
commit;


  -- j. Propensity to use On Demand – No. of initiated streams
/*
  -- GET DISTRIBUTION FOR HML GROUPING
select
      Period,
      Total_Programmes,
      count(*) as Cnt,
      count(distinct Account_Number) as Accts
  from (select
              a.Account_Number,
              case
                when last_modified_dt <= '2013-07-31' then 2
                when last_modified_dt >= '2013-08-01' then 1
                  else 0
              end as Period,
              count(distinct pdl_cdn_video_weblogs_sk) as Total_Programmes,
              count(*) as Total_Streams
          from (select
                      Account_Number
                  from EPL_04_Profiling_Variables
                 group by Account_Number) acc,
                 sk_prod.Cust_anytime_plus_downloads a
         where acc.Account_Number = a.Account_Number
           and a.x_content_type_desc = 'PROGRAMME'                                  -- to exclude trailers
           and a.x_actual_downloaded_size_mb > 1                                    -- to exclude any spurious header/trailer download records
           and a.last_modified_dt between '2013-02-01' and '2014-02-28'
         group by a.Account_Number, Period) det
 group by Period, Total_Programmes;
*/

update EPL_04_Profiling_Variables base
   set base.On_Demand_Streams_Num      = det.Total_Streams,
       base.On_Demand_Programmes_Num   = det.Total_Programmes,
       base.On_Demand_Usage_Segment    = case
                                           when det.Total_Programmes >= 74 then '4) Heavy OnDemand user'
                                           when det.Total_Programmes >= 15 then '3) Moderate OnDemand user'
                                           when det.Total_Programmes >= 1 then '2) Light OnDemand user'
                                             else '1) Non-OnDemand user'
                                         end
  from (select
              a.Account_Number,
              case
                when last_modified_dt <= '2013-07-31' then 2
                when last_modified_dt >= '2013-08-01' then 1
                  else 0
              end as Period,
              count(distinct pdl_cdn_video_weblogs_sk) as Total_Programmes,
              count(*) as Total_Streams
          from sk_prod.Cust_anytime_plus_downloads a
         where a.x_content_type_desc = 'PROGRAMME'                                  -- to exclude trailers
           and a.x_actual_downloaded_size_mb > 1                                    -- to exclude any spurious header/trailer download records
           and a.last_modified_dt between '2013-02-01' and '2014-02-28'
         group by a.Account_Number, Period) det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period;
commit;


  -- k. Propensity to consume EPL via Sky Go – No. of initiated streams*
/*
  -- GET DISTRIBUTION FOR HML GROUPING
select
      Period,
      Total_Programmes,
      count(*) as Cnt,
      count(distinct Account_Number) as Accts
  from (select
              a.Account_Number,
              case
                when Broadcast_Date <= '2013-07-31' then 2
                when Broadcast_Date >= '2013-08-01' then 1
                  else 0
              end as Period,
              count(distinct Programme || Broadcast_Date) as Total_Programmes,
              sum(a.Streams_Initiated) as Total_Streams
          from EPL_04_Go_Streams a
         where a.Account_Number is not null
         group by a.Account_Number, Period) det
 group by Period, Total_Programmes;
*/

update EPL_04_Profiling_Variables base
   set base.Sky_Go_EPL_Streams_Num    = det.Total_Streams,
       base.Sky_Go_EPL_Programmes_Num = det.Total_Programmes,
       base.Sky_Go_EPL_Usage_Segment  = case
                                          when det.Total_Programmes >= 10 then '4) Heavy Sky Go user'
                                          when det.Total_Programmes >= 3 then '3) Moderate Sky Go user'
                                          when det.Total_Programmes >= 1 then '2) Light Sky Go user'
                                            else '1) Non-Sky Go user'
                                        end
  from (select
              a.Account_Number,
              case
                when Broadcast_Date <= '2013-07-31' then 2
                when Broadcast_Date >= '2013-08-01' then 1
                  else 0
              end as Period,
              count(distinct Programme || Broadcast_Date) as Total_Programmes,
              sum(a.Streams_Initiated) as Total_Streams
          from EPL_04_Go_Streams a
         where a.Account_Number is not null
         group by a.Account_Number, Period) det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period;
commit;


  -- l. Household composition
  -- m. Region
  -- n. Affluence
update EPL_04_Profiling_Variables base
   set base.HH_Composition  = case
                                when sav.Household_Composition is null or sav.Household_Composition like 'Un%' then 'Unknown'
                                  else sav.Household_Composition
                              end,
       base.Region          = case
                                when sav.Region is null or sav.Region like 'Un%' then 'Unknown'
                                  else sav.Region
                              end,
       base.Affluence_Band  = case
                                when sav.Affluence_Bands is null or sav.Affluence_Bands like 'Unkn%' then 'Unknown'
                                  else sav.Affluence_Bands
                              end
  from sk_prod.cust_single_account_view sav
 where base.Account_Number = sav.Account_Number;
commit;


  -- Cable_Area
update EPL_04_Profiling_Variables base
   set base.Cable_Area    = case
                              when lower(det.Cable_Postcode) = 'n' then 'No'
                              when lower(det.Cable_Postcode) = 'y' then 'Yes'
                                else 'Unknown'
                            end
  from (select
              a.Account_Number,
              max(b.Cable_Postcode) as Cable_Postcode
          from sk_prod.cust_single_account_view a left join sk_prod.broadband_postcode_exchange b
                  on replace(a.cb_address_postcode, ' ', '') = replace(b.cb_address_postcode,' ', '')
         group by a.Account_Number) det
 where base.Account_Number = det.Account_Number;
commit;


  -- CQM Score
update EPL_04_Profiling_Variables base
   set base.CQM_Score     = case
                              when det.Model_Score between 1 and 9 then '0' || trim(cast(det.Model_Score as varchar(5)))
                              when det.Model_Score is not null then cast(det.Model_Score as varchar(5))
                                else 'Unknown'
                            end
  from (select
              a.Account_Number,
              max(b.Model_Score) as Model_Score
          from sk_prod.cust_single_account_view a,
               sk_prod.id_v_universe_all b
         where a.Cb_Key_Household = b.Cb_Key_Household
         group by a.Account_Number) det
 where base.Account_Number = det.Account_Number;
commit;


  -- FSS
if object_id('EPL_tmp_Consumerview') is not null then drop table EPL_tmp_Consumerview end if;
select
      cv.cb_row_id,
      cv.cb_key_individual,
      cv.cb_key_family,
      cv.cb_key_household,
      max(pp.p_head_of_household) as head_of_household,
      rank() over(partition by cv.cb_key_household order by head_of_household desc, cv.cb_row_id) AS HH_Rank,

      cv.h_fss_v3_group

  into EPL_tmp_Consumerview
  from sk_prod.experian_consumerview cv,
       sk_prod.playpen_consumerview_person_and_household pp
 where cv.exp_cb_key_db_individual = pp.exp_cb_key_db_individual
   and cv.cb_key_individual is not null
 group by
      cv.cb_row_id,
      cv.cb_key_individual,
      cv.cb_key_family,
      cv.cb_key_household,

      cv.h_fss_v3_group;
commit;

create        hg   index idx01 on EPL_tmp_Consumerview(cb_key_household);
create        lf   index idx02 on EPL_tmp_Consumerview(HH_Rank);
create unique hg   index idx03 on EPL_tmp_Consumerview(cb_key_household, HH_Rank);


update EPL_04_Profiling_Variables base
   set base.FSS           = case
                              when det.h_fss_v3_group = 'A' then '01) Bright Futures'
                              when det.h_fss_v3_group = 'B' then '02) Single Endeavours'
                              when det.h_fss_v3_group = 'C' then '03) Young Essentials'
                              when det.h_fss_v3_group = 'D' then '04) Growing Rewards'
                              when det.h_fss_v3_group = 'E' then '05) Family Interest'
                              when det.h_fss_v3_group = 'F' then '06) Accumulated Wealth'
                              when det.h_fss_v3_group = 'G' then '07) Consolidating Assets'
                              when det.h_fss_v3_group = 'H' then '08) Balancing Budgets'
                              when det.h_fss_v3_group = 'I' then '09) Stretched Finances'
                              when det.h_fss_v3_group = 'J' then '10) Established Reserves'
                              when det.h_fss_v3_group = 'K' then '11) Seasoned Economy'
                              when det.h_fss_v3_group = 'L' then '12) Platinum Pensions'
                              when det.h_fss_v3_group = 'M' then '13) Sunset Security'
                              when det.h_fss_v3_group = 'N' then '14) Traditional Thrift'
                                else '99) Unknown'
                            end
  from (select
              a.Account_Number,
              max(b.h_fss_v3_group) as h_fss_v3_group
          from sk_prod.cust_single_account_view a,
               EPL_tmp_Consumerview b
         where a.Cb_Key_Household = b.Cb_Key_Household
           and b.HH_Rank = 1
         group by a.Account_Number) det
 where base.Account_Number = det.Account_Number;
commit;


  -- Number of Sky products
  -- RCrouch:
  --   1) Average number of products subscribed to as a metric  ( rather than having HD, DTV , Multiroom etc. as just dimensions )
  --       a. This will  be one number not split by type of product
  --   2) The products are
  --       a. DTV
  --       b. Movies
  --       c. BB
  --       d. Talk
  --       e. HD
  --       f. Multiroom
  --   3) This will give a maximum score of 6 per subscriber
update EPL_04_Profiling_Variables base
   set base.Number_Of_Sky_Products      = case when base.Base_Package <> 'Unknown' then 1 else 0 end +
                                          case when base.Prem_Movies > 0 then 1 else 0 end +
                                          case when base.Broadband = 'Broadband' then 1 else 0 end +
                                          case when base.SkyTalk = 'SkyTalk' then 1 else 0 end +
                                          case when base.HD = 'HD' then 1 else 0 end +
                                          case when base.Multiscreen ='Multiscreen' then 1 else 0 end;
commit;


  -- Number of Sky products (including "Sky GO" and "On Demand")
  --    For Number of products – I’ll use existing Sky Go and Sky OD definitions that are already in the pivot
update EPL_04_Profiling_Variables base
   set base.Number_Of_Sky_Products_GO_OD
                                        = Number_Of_Sky_Products +
                                          case when base.Sky_Go_EPL_Usage_Segment <> '1) Non-Sky Go user' then 1 else 0 end +
                                          case when base.On_Demand_Usage_Segment <> '1) Non-OnDemand user' then 1 else 0 end;
commit;


  -- Number of Sky products (excluding "Broadband")
update EPL_04_Profiling_Variables base
   set base.Number_Of_Sky_Products_No_BB
                                        = case when base.Base_Package <> 'Unknown' then 1 else 0 end +
                                          case when base.Prem_Movies > 0 then 1 else 0 end +
                                          -- case when base.Broadband = 'Broadband' then 1 else 0 end +
                                          case when base.SkyTalk = 'SkyTalk' then 1 else 0 end +
                                          case when base.HD = 'HD' then 1 else 0 end +
                                          case when base.Multiscreen ='Multiscreen' then 1 else 0 end +
                                          case when base.Sky_Go_EPL_Usage_Segment <> '1) Non-Sky Go user' then 1 else 0 end +
                                          case when base.On_Demand_Usage_Segment <> '1) Non-OnDemand user' then 1 else 0 end;
commit;


  -- Number of Sky products (excluding "DTV")
  -- 5) Sky products: We need to:
  --    a. Take Sky Movies out of Sky products number
  --    b. Take DTV out of Sky products
  --    c. Swap Sky talk for Sky BB
  --    d. Put the high threshold at 4 out of 5 remaining products
update EPL_04_Profiling_Variables base
   set base.Number_Of_Sky_Products_No_DTV
                                        = -- case when base.Base_Package <> 'Unknown' then 1 else 0 end +
                                          -- case when base.Prem_Movies > 0 then 1 else 0 end +
                                          case when base.Broadband = 'Broadband' then 1 else 0 end +
                                          -- case when base.SkyTalk = 'SkyTalk' then 1 else 0 end +
                                          case when base.HD = 'HD' then 1 else 0 end +
                                          case when base.Multiscreen ='Multiscreen' then 1 else 0 end +
                                          case when base.Sky_Go_EPL_Usage_Segment <> '1) Non-Sky Go user' then 1 else 0 end +
                                          case when base.On_Demand_Usage_Segment <> '1) Non-OnDemand user' then 1 else 0 end;
commit;



/*
select
      case when Base_Package = 'Unknown' then 'U' else 'OK' end as Pack,
      Prem_Movies,
      Broadband,
      SkyTalk,
      HD,
      Multiscreen,
      Number_Of_Sky_Products,
      count(*) as cc
  from EPL_04_Profiling_Variables base
 group by Pack, Prem_Movies, Broadband, SkyTalk, HD, Multiscreen , Number_Of_Sky_Products
 order by Pack, Prem_Movies, Broadband, SkyTalk, HD, Multiscreen , Number_Of_Sky_Products;
*/


  -- Number of Sky products
  -- RCrouch:
  -- Dimension flags for On demand – have they used in the last 6 months
update EPL_04_Profiling_Variables base
   set base.Sky_Go_Any_Usage_Segment  = case
                                          when det.Total_Streams > 0 then '2) Sky Go user'
                                            else '1) Non-Sky Go user'
                                        end
  from (select
              a.Account_Number,
              case
                when Activity_dt between '2013-02-01' and '2013-07-31' then 2
                when Activity_dt between '2013-08-01' and '2014-02-28' then 1
                  else 0
              end as Period,
              count(*) as Total_Streams
          from sk_prod.sky_player_usage_detail a
         where a.Account_Number is not null
           and Activity_dt between '2013-02-01' and '2014-02-28'
         group by a.Account_Number, Period) det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period;
commit;

  -- Update to correct a few inconsistent records
update EPL_04_Profiling_Variables base
   set base.Sky_Go_Any_Usage_Segment  = '2) Sky Go user'
 where base.Sky_Go_EPL_Usage_Segment <> '1) Non-Sky Go user'
   and base.Sky_Go_Any_Usage_Segment <> '2) Sky Go user';
commit;


/*
select Sky_Go_EPL_Usage_Segment, Sky_Go_Any_Usage_Segment, count(*) as Cnt
  from EPL_04_Profiling_Variables
 group by Sky_Go_EPL_Usage_Segment, Sky_Go_Any_Usage_Segment
 order by Sky_Go_EPL_Usage_Segment, Sky_Go_Any_Usage_Segment
*/



  -- ##############################################################################################################
  -- ##### Add new variables                                                                                  #####
  -- ##############################################################################################################
alter table EPL_04_Profiling_Variables
  add (Interest_Group__Football       varchar(50) null default '???',
       Interest_Group__Rugby          varchar(50) null default '???',
       Interest_Group__Other_5_7      varchar(50) null default '???',
       Interest_Group__Other_7_Only   varchar(50) null default '???');
commit;

  -- ##############################################################################################################
  -- ##############################################################################################################
alter table EPL_04_Profiling_Variables
  add (Postcode_District              varchar(10) null default 'Unknown',
       Mosaic_Segment                 varchar(30) null default 'Unknown',
       Lifestage                      varchar(30) null default 'Unknown',
       Bills_Number                   smallint    null default 0,
       Bill_Payment_L12m              decimal(10, 2) null default 0,
       Bill_Payment_Avg_Monthly_L12m  decimal(10, 2) null default 0,
       Bill_Payment_Annuallised_Monthly_L12m  decimal(10, 2) null default 0,
       Bill_Balance_Due_L12m          decimal(10, 2) null default 0,
       Bill_Balance_Due_Avg_Monthly_L12m decimal(10, 2) null default 0,
       Bill_Balance_Due_Annuallised_Monthly_L12m decimal(10, 2) null default 0,
       Simple_Segment                 varchar(20) null default 'Unknown',
       Simple_Sub_Segment             varchar(60) null default 'Unknown');
commit;


  -- Get SAV values
update EPL_04_Profiling_Variables base
   set base.Postcode_District   = case
                                    when det.cb_address_postcode_district is null then 'Unknown'
                                      else det.cb_address_postcode_district
                                  end,
       base.Mosaic_Segment      = case
                                    when det.Mosaic_Segments is null then 'Unknown'
                                      else det.Mosaic_Segments
                                  end,
       base.Lifestage           = case
                                    when det.H_Lifestage is null then 'Unknown'
                                    when det.H_Lifestage = 'Missing' then 'Unknown'
                                      else det.H_Lifestage
                                  end
  from sk_prod.cust_single_account_view det
 where base.Account_Number = det.Account_Number;
commit;


  -- Get SIMPLE SEGMENTATION
update EPL_04_Profiling_Variables base
   set base.Simple_Segment      = case
                                    when det.Segment is null then 'Unknown'
                                      else det.Segment
                                  end,
       base.Simple_Sub_Segment  = case
                                    when det.Sub_Segment is null then 'Unknown'
                                      else det.Sub_Segment
                                  end
  from (select
              Account_Number,
              max(Segment) as Segment,
              max(Segment_Lev2) as Sub_Segment
          from zubizaa.simple_segmentation_history
         where Observation_Date = '2014-02-28'
         group by Account_Number) det
 where base.Account_Number = det.Account_Number;
commit;


  -- Get bills data
if object_id('EPL_tmp_Bills') is not null then drop table EPL_tmp_Bills end if;
select
      a.Account_Number,
      a.Payment_Due_Dt,
      a.Total_Paid_Amt,
      a.Balance_Due_Amt
  into EPL_tmp_Bills
  from sk_prod.cust_bills a,
       EPL_04_Profiling_Variables b
 where a.Account_Number = b.Account_Number
   and b.Period = 1
   and a.Status = 'Paid'
   and a.Payment_Due_Dt between '2013-03-01' and '2014-02-28';
commit;
create hg index idx1 on EPL_tmp_Bills(Account_Number);
create date index idx1 on EPL_tmp_Bills(Payment_Due_Dt);

update EPL_04_Profiling_Variables base
   set base.Bills_Number          = det.Bills_Number,
       base.Bill_Payment_L12m     = det.Bill_Payment_L12m,
       base.Bill_Balance_Due_L12m = det.Bill_Balance_Due_L12m
  from (select
              Account_Number,
              count(distinct Payment_Due_Dt) as Bills_Number,
              sum(-1.0 * Total_Paid_Amt) as Bill_Payment_L12m,
              sum(Balance_Due_Amt) as Bill_Balance_Due_L12m
          from EPL_tmp_Bills
         group by Account_Number) det
 where base.Account_Number = det.Account_Number;
commit;

update EPL_04_Profiling_Variables
   set Bill_Payment_Avg_Monthly_L12m              = case
                                                      when Bills_Number > 0 then 1.0 * Bill_Payment_L12m / Bills_Number
                                                        else 0
                                                    end,
       Bill_Payment_Annuallised_Monthly_L12m      = case
                                                      when Bills_Number > 0 then 12.0 * Bill_Payment_L12m / Bills_Number
                                                        else 0
                                                    end,
       Bill_Balance_Due_Avg_Monthly_L12m          = case
                                                      when Bills_Number > 0 then 1.0 * Bill_Balance_Due_L12m / Bills_Number
                                                        else 0
                                                    end,
       Bill_Balance_Due_Annuallised_Monthly_L12m  = case
                                                      when Bills_Number > 0 then 12.0 * Bill_Balance_Due_L12m / Bills_Number
                                                        else 0
                                                    end;
commit;







