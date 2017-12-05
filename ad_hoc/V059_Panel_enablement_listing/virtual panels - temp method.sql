--temp method ahead of reconciliation

/*###############################################################################
# Created on:   08/03/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Virtual panel management - Channel 4 & 5
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

if object_id('VirtPan_Channel45_01_Universe_Selection') is not null then drop table VirtPan_Channel45_01_Universe_Selection end if;
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
);

create date index idx1 on VirtPan_Channel45_01_Universe_Selection(Run_Date);
create hg index idx2 on VirtPan_Channel45_01_Universe_Selection(Account_Number);
create hg index idx3 on VirtPan_Channel45_01_Universe_Selection(Subscriber_id);
create index idx4 on VirtPan_Channel45_01_Universe_Selection(Random_Num);
create unique index idx5 on VirtPan_Channel45_01_Universe_Selection(Run_Date, Subscriber_id);


  -- #################################################################################
  -- ##### Create universe - get all boxes from all panels                       #####
  -- #################################################################################
create variable @var_multiplier bigint;
set @var_multiplier = datepart(millisecond,now()) + 1;

  select subscriber_id
        ,panel
        ,max(dt) as max_dt
    into #panel_part1
    from vespa_analysts.alt_panel_data
   where data_received = 1
     and dt between '2013-06-01' and '2013-07-29'
group by subscriber_id
        ,panel
;

  select pan.subscriber_id
        ,pan.panel
        ,cast(0 as bigint) as account_number
    into #panel
    from #panel_part1 as pan
         inner join vespa_analysts.alt_panel_data as apd on pan.subscriber_id = apd.subscriber_id
                                                        and max_dt = dt
;

  update #panel as pan
     set account_number = cast(csi.account_number as bigint)
    from sk_prod.cust_service_instance as csi
   where pan.subscriber_id = csi.si_external_identifier
;

  select account_number
        ,count(1) as panel_count
    into #panel_count
    from #panel
group by account_number
  having panel_count > 1
;

  delete from #panel
   where account_number in (select account_number from #panel_count)
;

  insert into VirtPan_Channel45_01_Universe_Selection(Account_Number, Subscriber_id, Panel_Id, Source, Box_Reporting_Quality, Random_Num)
  select cast(Account_Number as varchar)
        ,Subscriber_Id
        ,Panel
        ,'APD'
        ,-1
        ,rand(number(*) * @var_multiplier)
    from #panel
;

  update VirtPan_Channel45_01_Universe_Selection as bas
     set bas.On_Demand_DL_Ever = 1
    from sk_prod.cust_anytime_plus_downloads as det
   where bas.Account_Number = det.Account_Number
;

  -- #################################################################################
  -- ##### Virtual panel selection                                               #####
  -- #################################################################################
  -- Channel 4 - 300-350k with On Demand DL and at least one box with reporting quality > 0
  update VirtPan_Channel45_01_Universe_Selection base
     set Channel4_Panel_Flag = 1
   where On_Demand_DL_Ever = 1
commit; --305,161

update VirtPan_Channel45_01_Universe_Selection base
   set Channel5_Panel_Flag = 1
commit; --833,431

  -- #################################################################################
  -- ##### Populate final table                                                  #####
  -- #################################################################################
  -- Add non-existing accounts first
  insert into vespa_analysts.vespa_broadcast_reporting_vp_map (Account_Number, Vespa_Panel)
  select bas.account_number
        ,bas.panel_id
    from VirtPan_Channel45_01_Universe_Selection as bas
         left join vespa_analysts.vespa_broadcast_reporting_vp_map as map on bas.account_number = map.account_number
   where map.account_number is null
;

  update vespa_analysts.vespa_broadcast_reporting_vp_map
     set vp1 = 0
        ,vp2 = 0
;

  update vespa_analysts.vespa_broadcast_reporting_vp_map as map
     set vp1 = Channel4_Panel_Flag
        ,vp2 = Channel5_Panel_Flag
    from VirtPan_Channel45_01_Universe_Selection as bas
   where map.account_number = bas.account_number
;



  -- Check stats
select sum(vp1) as C4, sum(vp2) as C5, count(*) as Cnt_Total_Accounts
  from vespa_analysts.vespa_broadcast_reporting_vp_map;

select vp1, vp2, count(*) as Cnt_Total_Accounts
  from vespa_analysts.vespa_broadcast_reporting_vp_map
 group by vp1, vp2;


  -- #################################################################################
  -- #################################################################################






















--check:
select top 10 * from VirtPan_Channel45_01_Universe_Selection



