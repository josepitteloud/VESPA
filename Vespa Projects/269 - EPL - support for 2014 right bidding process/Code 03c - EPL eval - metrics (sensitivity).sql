/*###############################################################################
# Created on:   20/08/2014
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  EPL rights project - Metric calculations - variations for sensitivities
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 20/08/2014  SBE   Initial version
#
###############################################################################*/


  -- ##############################################################################################################
  -- ##### Create structures                                                                                  #####
  -- ##############################################################################################################
if object_id('EPL_04_Eng_Matrix_Alternatives') is not null then drop table EPL_04_Eng_Matrix_Alternatives end if;
create table EPL_04_Eng_Matrix_Alternatives (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

      -- Account
    Account_Number                          varchar(20)       null      default null,
    Period                                  tinyint           null      default 0,
    Low_Content_Flag                        bit               null      default 0,

    Metric                                  varchar(50)       null      default null,
    Category                                varchar(50)       null      default null,

    EPL_SOC                                 varchar(30)       null      default 'Not calculated',       -- Share of EPL matches watched (tree - tier 3)
    EPL_SOC__Lower                          varchar(30)       null      default 'Not calculated',
    EPL_SOC__Higher                         varchar(30)       null      default 'Not calculated',

    EPL_SoSV                                varchar(30)       null      default 'Not calculated',       -- EPL share of sports viewing (tree - tier 1)
    EPL_SoSV__Lower                         varchar(30)       null      default 'Not calculated',
    EPL_SoSV__Higher                        varchar(30)       null      default 'Not calculated',

    Sport_SoV                               varchar(30)       null      default 'Not calculated',       -- Sports share of total pay (tree - tier 2)
    Sport_SoV__Lower                        varchar(30)       null      default 'Not calculated',
    Sport_SoV__Higher                       varchar(30)       null      default 'Not calculated',

    Movies_SoV                              varchar(30)       null      default 'Not calculated',
    Movies_SoV__Lower                       varchar(30)       null      default 'Not calculated',
    Movies_SoV__Higher                      varchar(30)       null      default 'Not calculated',

    Sky_Atlantic_Complete_Progs_Viewed      varchar(30)       null      default 'Not calculated',
    Sky_Atlantic_Complete_Progs_Viewed__Lower   varchar(30)       null      default 'Not calculated',
    Sky_Atlantic_Complete_Progs_Viewed__Higher  varchar(30)       null      default 'Not calculated'

);
create        hg   index idx01 on EPL_04_Eng_Matrix_Alternatives(Account_Number);
create        lf   index idx02 on EPL_04_Eng_Matrix_Alternatives(Period);
create        lf   index idx03 on EPL_04_Eng_Matrix_Alternatives(Metric);
grant select on EPL_04_Eng_Matrix_Alternatives to vespa_group_low_security;


  -- Overall level - base
insert into EPL_04_Eng_Matrix_Alternatives
      (Account_Number, Period, Metric, Category)
  select
        a.Account_Number,
        a.Period,
        'Overall',
        '(all)'
    from EPL_01_Universe a
   where a.Period = 1
   group by a.Account_Number, a.Period
  having max(a.Valid_Account_Flag) = 1;
commit;


  -- ##############################################################################################################
  -- EPL_SOC
update EPL_04_Eng_Matrix_Alternatives base
   set base.Low_Content_Flag    = case
                                    when det.Content_Available <= 3 then 1                        -- Low content flag for EPL games available
                                      else 0
                                  end,
       base.EPL_SOC             = case
                                    when det.Calculated_SOC =  0    then 'Did not watch'
                                    when det.Calculated_SOC <  0.20 then 'Low'
                                    when det.Calculated_SOC <= 0.50 then 'Medium'
                                      else 'High'
                                  end,
       base.EPL_SOC__Lower      = case
                                    when det.Calculated_SOC =  0    then 'Did not watch'
                                    when det.Calculated_SOC <  0.16 then 'Low'
                                    when det.Calculated_SOC <= 0.50 then 'Medium'
                                      else 'High'
                                  end,
       base.EPL_SOC__Higher     = case
                                    when det.Calculated_SOC =  0    then 'Did not watch'
                                    when det.Calculated_SOC <  0.24 then 'Low'
                                    when det.Calculated_SOC <= 0.50 then 'Medium'
                                      else 'High'
                                  end
  from EPL_03_SOCs_Summaries det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'Live games - overall'
   and base.Category = det.Category;
commit;



  -- ##############################################################################################################
  -- EPL_SoSV
update EPL_04_Eng_Matrix_Alternatives base
   set base.EPL_SoSV            = case
                                    when det.Calculated_SOV =  0    then 'Did not watch'
                                    when det.Calculated_SOV <  0.20 then 'Low'
                                    when det.Calculated_SOV <= 0.50 then 'Medium'
                                      else 'High'
                                  end,
       base.EPL_SoSV__Lower     = case
                                    when det.Calculated_SOV =  0    then 'Did not watch'
                                    when det.Calculated_SOV <  0.20 then 'Low'
                                    when det.Calculated_SOV <= 0.40 then 'Medium'
                                      else 'High'
                                  end,
       base.EPL_SoSV__Higher    = case
                                    when det.Calculated_SOV =  0    then 'Did not watch'
                                    when det.Calculated_SOV <  0.20 then 'Low'
                                    when det.Calculated_SOV <= 0.60 then 'Medium'
                                      else 'High'
                                  end
  from EPL_03_SOVs det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'EPL SoSV';
commit;



  -- ##############################################################################################################
  -- Sport_SoV
update EPL_04_Eng_Matrix_Alternatives base
   set base.Sport_SoV           = case
                                    when det.Calculated_SOV =  0    then 'Did not watch'
                                    when det.Calculated_SOV <  0.05 then 'Low'
                                    when det.Calculated_SOV <  0.25 then 'Medium'
                                    when det.Calculated_SOV <  0.45 then 'High'
                                      else 'Very high'
                                  end,
       base.Sport_SoV__Lower    = case
                                    when det.Calculated_SOV =  0    then 'Did not watch'
                                    when det.Calculated_SOV <  0.05 then 'Low'
                                    when det.Calculated_SOV <  0.20 then 'Medium'
                                    when det.Calculated_SOV <  0.45 then 'High'
                                      else 'Very high'
                                  end,
       base.Sport_SoV__Higher   = case
                                    when det.Calculated_SOV =  0    then 'Did not watch'
                                    when det.Calculated_SOV <  0.05 then 'Low'
                                    when det.Calculated_SOV <  0.35 then 'Medium'
                                    when det.Calculated_SOV <  0.45 then 'High'
                                      else 'Very high'
                                  end
  from EPL_03_SOVs det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'Sports SoV';
commit;



  -- ##############################################################################################################
  -- Movies_SoV
update EPL_04_Eng_Matrix_Alternatives base
   set base.Movies_SOV
                                = case
                                    when det.Calculated_SOV  = 0    then 'Did not watch'
                                    when det.Calculated_SOV <= 0.02 then 'Low'
                                    when det.Calculated_SOV <= 0.11 then 'Medium'
                                      else 'High'
                                  end,
       base.Movies_SOV__Lower
                                = case
                                    when det.Calculated_SOV  = 0    then 'Did not watch'
                                    when det.Calculated_SOV <= 0.02 then 'Low'
                                    when det.Calculated_SOV <= 0.08 then 'Medium'
                                      else 'High'
                                  end,
       base.Movies_SOV__Higher
                                = case
                                    when det.Calculated_SOV  = 0    then 'Did not watch'
                                    when det.Calculated_SOV <= 0.02 then 'Low'
                                    when det.Calculated_SOV <= 0.14 then 'Medium'
                                      else 'High'
                                  end
  from EPL_03_SOVs det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'Movies SoV';
commit;



  -- ##############################################################################################################
  -- Sky_Atlantic_Complete_Progs_Viewed
update EPL_04_Eng_Matrix_Alternatives base
   set base.Sky_Atlantic_Complete_Progs_Viewed
                                = case
                                    when det.Calculated_SOV  = 0     then 'Did not watch'
                                    when det.Calculated_SOV <= 0.020 then 'Low'
                                    when det.Calculated_SOV <= 0.100 then 'Medium'
                                      else 'High'
                                  end,
       base.Sky_Atlantic_Complete_Progs_Viewed__Lower
                                = case
                                    when det.Calculated_SOV  = 0     then 'Did not watch'
                                    when det.Calculated_SOV <= 0.020 then 'Low'
                                    when det.Calculated_SOV <= 0.080 then 'Medium'
                                      else 'High'
                                  end,
       base.Sky_Atlantic_Complete_Progs_Viewed__Higher
                                = case
                                    when det.Calculated_SOV  = 0     then 'Did not watch'
                                    when det.Calculated_SOV <= 0.020 then 'Low'
                                    when det.Calculated_SOV <= 0.120 then 'Medium'
                                      else 'High'
                                  end
  from EPL_03_SOVs det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'Sky Atlantic - number of complete programmes';
commit;



  -- ##############################################################################################################
  -- View for easy switching field when generating outcomes
drop view if exists EPL_04_Eng_Matrix_Alternatives_View;
create view EPL_04_Eng_Matrix_Alternatives_View as
  select
        a.Pk_Identifier,
        a.Updated_On,
        a.Updated_By,

          -- Account,
        a.Account_Number,
        a.Period,
        a.Low_Content_Flag,

        a.Metric,
        a.Category,

        -- ### Re-calculated fields ###
        a.EPL_SOC,
        -- a.EPL_SOC__Lower as EPL_SOC,
        -- a.EPL_SOC__Higher as EPL_SOC,

        a.EPL_SoSV,
        -- a.EPL_SoSV__Lower as EPL_SoSV,
        -- a.EPL_SoSV__Higher as EPL_SoSV,

        a.Sport_SoV,
        -- a.Sport_SoV__Lower as Sport_SoV,
        -- a.Sport_SoV__Higher as Sport_SoV,

        a.Movies_SoV,
        -- a.Movies_SoV__Lower as Movies_SoV,
        -- a.Movies_SoV__Higher as Movies_SoV,

        a.Sky_Atlantic_Complete_Progs_Viewed,
        -- a.Sky_Atlantic_Complete_Progs_Viewed__Lower as Sky_Atlantic_Complete_Progs_Viewed,
        -- a.Sky_Atlantic_Complete_Progs_Viewed__Higher as Sky_Atlantic_Complete_Progs_Viewed,


        -- ### Original fields ###
        -- b.EPL_SOC,
        b.EPL_SoNLC,
        -- b.EPL_SoSV,
        -- b.Sport_SoV,
        -- b.Movies_SoV,

        b.EPL_SOC__Pack_Monday_Evening,
        b.EPL_SOC__Pack_Midweek_Evening,
        b.EPL_SOC__Pack_Saturday_Lunch,
        b.EPL_SOC__Pack_Saturday_Afternoon,
        b.EPL_SOC__Pack_Sunday_Early_Afternoon,
        b.EPL_SOC__Pack_Sunday_Late_Afternoon,

        b.Sky_Atlantic_SoV,
        b.Entertainment_Pack_SoV,
        b.Sky_Excl_Channels_SoV,
        b.Sky_Virgin_Excl_Channels_SoV,

        b.EPL_Games_Watched__Pack_Mon_Ev,
        b.EPL_Games_Watched__Pack_Mid_Ev,
        b.EPL_Games_Watched__Pack_Sat_Lunch,
        b.EPL_Games_Watched__Pack_Sat_Aft,
        b.EPL_Games_Watched__Pack_Sun_Early_Aft,
        b.EPL_Games_Watched__Pack_Sun_Late_Aft,
        b.EPL_Games_Watched__Overall,

        b.EPL_Pack_SOV__Monday_Evening,
        b.EPL_Pack_SOV__Midweek_Evening,
        b.EPL_Pack_SOV__Saturday_Lunch,
        b.EPL_Pack_SOV__Saturday_Afternoon,
        b.EPL_Pack_SOV__Sunday_Early_Afternoon,
        b.EPL_Pack_SOV__Sunday_Late_Afternoon,

        -- b.Sky_Atlantic_Complete_Progs_Viewed,

        b.Sky_Branded_Channels

    from EPL_04_Eng_Matrix_Alternatives a,
         EPL_04_Eng_Matrix  b
   where a.Account_Number = b.Account_Number
     and a.Period = b.Period;
commit;




  -- ##############################################################################################################
  -- ##############################################################################################################












