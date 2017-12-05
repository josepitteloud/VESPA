/*###############################################################################
# Created on:   08/03/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Virtual panel management - Channel 4 & 5
#
# Procedure version created 23/09/2013 by Jon 
#
# Panel definition:
#               CHANNEL 4
#               ---------
#               All those subscribers/accounts that:
#               - were on the panels last week
#               - have data return metric > 0
#               - have ever downloaded any On Demand content
#
#               Additional conditions for the first round (08/03/2013):
#               - top up with manual selection of those who have recently downloaded
#                 On Demand content, less those that had the prefix issues in the
#                 recent enablement files which we will be disabling soon (SBE to advise)
#               Expected panel size is between 300k and 350k Subscriber Ids
#
#               CHANNEL 5
#               ---------
#               All Accounts on any vespa panel with at least one box where:
#               - reporting quality known
#               - reporting quality > 0
#
# To do:
#               - N/A
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 08/03/2013  SBE   v01 - initial version
# 15/03/2013  SBE   v02 - Channel 5 VP panel definition changed
#
###############################################################################*/

drop procedure VirtPan;
create procedure VirtPan as
begin

     create table VirtPan_Channel45_01_Universe_Selection (
        Row_Id                     bigint        identity,
        Run_Date                   date          default today(),
        Account_Number             varchar(50)   default null,
        Subscriber_id              bigint        default null,
        Source                     varchar(30)   default null,
        Panel_Id                   tinyint       default 0,
        On_Demand_DL_Ever          bit           default 0,
        Box_Reporting_Quality      decimal(10,3) default -1,
        Low_Data_Quality_Flag      bit           default 0,
        Channel4_Panel_Flag        bit           default 0,
        Channel5_Panel_Flag        bit           default 0,
        Random_Num                 decimal(10,6) default 0,
        Created_By                 varchar(30)   default user,
        Created_On                 timestamp     default timestamp
     )

     create date index idx1 on VirtPan_Channel45_01_Universe_Selection(Run_Date)
     create hg index idx2 on VirtPan_Channel45_01_Universe_Selection(Account_Number)
     create hg index idx3 on VirtPan_Channel45_01_Universe_Selection(Subscriber_id)
     create index idx4 on VirtPan_Channel45_01_Universe_Selection(Random_Num)
     create unique index idx5 on VirtPan_Channel45_01_Universe_Selection(Run_Date, Subscriber_id)


       -- #################################################################################
       -- ##### Create universe - get all boxes from all panels                       #####
       -- #################################################################################
     declare @var_multiplier bigint
     set @var_multiplier = datepart(millisecond,now()) + 1

     insert into VirtPan_Channel45_01_Universe_Selection
                 (Account_Number, Subscriber_id, Panel_Id, Source, Box_Reporting_Quality, Random_Num)
       select
             Account_Number,
             Subscriber_Id,
             Panel_Id_Vespa,
             'SBV',
             case
               when Reporting_Quality is null then -1
                 else Reporting_Quality
             end,
             rand(number(*) * @var_multiplier)
         from vespa_analysts.vespa_single_box_view
        where status_vespa = 'Enabled'
     commit

       -- #################################################################################
       -- ##### Append account/subscriber metrics                                     #####
       -- #################################################################################
       -- Append On Demand DL information
     update VirtPan_Channel45_01_Universe_Selection base
        set base.On_Demand_DL_Ever = 1
       from (select
                   Account_Number,
                   min(last_modified_dt) as first_dl_date,
                   max(last_modified_dt) as last_dl_date
               from sk_prod.cust_anytime_plus_downloads
              group by Account_Number) det
      where base.Account_Number = det.Account_Number
     commit

       -- #################################################################################
       -- ##### Data quality checks                                                   #####
       -- #################################################################################

       -- ##### Multiple accounts for a single box #####
     update VirtPan_Channel45_01_Universe_Selection base
        set base.Low_Data_Quality_Flag = 1
       from (select Subscriber_Id
                   ,count(distinct Account_Number) as Acc_Nums
               from VirtPan_Channel45_01_Universe_Selection
           group by Subscriber_Id) det
      where base.Subscriber_Id = det.Subscriber_Id
        and det.Acc_Nums > 1
     commit

       -- Propagate to all associated subscriber Ids within the the account
     update VirtPan_Channel45_01_Universe_Selection base
        set base.Low_Data_Quality_Flag = 1
       from (select Account_Number
                   ,max(Low_Data_Quality_Flag) as Low_Quality
               from VirtPan_Channel45_01_Universe_Selection
           group by Account_Number
             having Low_Quality > 0) det
      where base.Account_Number = det.Account_Number
     commit

       -- ##### Boxes within an account being on different panels #####
     update VirtPan_Channel45_01_Universe_Selection base
        set base.Low_Data_Quality_Flag = 1
       from (select Account_Number
                   ,count(distinct Panel_Id) as Panel_Nums
               from VirtPan_Channel45_01_Universe_Selection
           group by Account_Number
             having Panel_Nums > 1) det
      where base.Account_Number = det.Account_Number
     commit

       -- #################################################################################
       -- ##### Virtual panel selection                                               #####
       -- #################################################################################

       -- Channel 4 - 300-350k with On Demand DL and at least one box with reporting quality > 0
     select count(distinct Account_Number) as C4_Cnt_Accounts, count(*) as C4_Cnt_Boxes
       from VirtPan_Channel45_01_Universe_Selection
      where Run_Date = today()
        and Low_Data_Quality_Flag = 0
        and On_Demand_DL_Ever = 1
        and (
             (Box_Reporting_Quality > 0 and Source = 'SBV')          -- SBV boxes
             or
             (Box_Reporting_Quality = -1 and Source <> 'SBV')        -- Manual top-up boxes
            )

       -- Channel 5 - All accounts with at least one box with reporting quality known & >0
     select count(distinct Account_Number) as C5_Cnt_Accounts, count(*) as C4_Cnt_Boxes
       from VirtPan_Channel45_01_Universe_Selection
      where Run_Date = today()
        and Low_Data_Quality_Flag = 0
        and Box_Reporting_Quality > 0
        and Source = 'SBV'

        -- Reset
     update VirtPan_Channel45_01_Universe_Selection
        set Channel4_Panel_Flag = 0,
            Channel5_Panel_Flag = 0
     commit

     update VirtPan_Channel45_01_Universe_Selection base
        set Channel4_Panel_Flag = 1                                          -- Joined at Subscriber Id level, so account may include a mixture of boxes "on panel" and "not on panel"
       from (select Subscriber_Id
               from VirtPan_Channel45_01_Universe_Selection
              where Run_Date = today()
                and Low_Data_Quality_Flag = 0
                and On_Demand_DL_Ever = 1
                and (
                     (Box_Reporting_Quality > 0 and Source = 'SBV')          -- SBV boxes
                     or
                     (Box_Reporting_Quality = -1 and Source <> 'SBV')        -- Manual top-up boxes
                    )
              group by Subscriber_Id) det
      where base.Subscriber_Id = det.Subscriber_Id
     commit

     update VirtPan_Channel45_01_Universe_Selection base
        set Channel5_Panel_Flag = 1                                          -- Joined at Subscriber Id level, so account may include a mixture of boxes "on panel" and "not on panel"
       from (select Subscriber_Id
               from VirtPan_Channel45_01_Universe_Selection
              where Run_Date = today()
                and Low_Data_Quality_Flag = 0
                and Box_Reporting_Quality > 0
                and Source = 'SBV'
              group by Subscriber_Id) det
      where base.Subscriber_Id = det.Subscriber_Id
     commit

       -- #################################################################################
       -- ##### Populate final table                                                  #####
       -- #################################################################################
       -- Add non-existing accounts first
       insert into vespa_analysts.vespa_broadcast_reporting_vp_map (Account_Number, Vespa_Panel)
       select base.Account_Number
             ,base.Panel_Id
         from VirtPan_Channel45_01_Universe_Selection base left join vespa_analysts.vespa_broadcast_reporting_vp_map det on base.Account_Number = det.Account_Number
        where det.Account_Number is null
          and (
                 Channel4_Panel_Flag = 1
                 or
                 Channel5_Panel_Flag = 1
              )
        group by base.Account_Number, base.Panel_Id       -- Our table is Subscriber ID based, the destination is Account Number one

       -- Reset selection
     update vespa_analysts.vespa_broadcast_reporting_vp_map base
        set base.vp1     = 0,
            base.vp2     = 0
     commit

       -- Add flags
     update vespa_analysts.vespa_broadcast_reporting_vp_map base
        set base.vp1     = det.Channel4_Panel_Flag,
            base.vp2     = det.Channel5_Panel_Flag
       from (select
                   Account_Number,
                   max(Channel4_Panel_Flag) as Channel4_Panel_Flag,
                   max(Channel5_Panel_Flag) as Channel5_Panel_Flag
               from VirtPan_Channel45_01_Universe_Selection
              group by Account_Number) det
      where base.Account_Number = det.Account_Number
     commit

     drop table VirtPan_Channel45_01_Universe_Selection
end;
  -- #################################################################################
  -- #################################################################################




















