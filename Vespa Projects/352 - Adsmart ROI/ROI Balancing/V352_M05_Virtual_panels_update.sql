/*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

-----------------------------------------------------------------------------------

**Project Name:                         Panel Balancing
**Analysts:                             Jon Green   (Jonathan.Green@skyiq.co.uk)
                                        Leonardo Ripoli
**Lead(s):                              Hoi Yu Tang (hoiyu.tang@skyiq.co.uk)
**Stakeholder:                          Jose Loureda
**Project Code (Insight Collation):     V306
**SharePoint Folder:                    http://sp-department.bskyb.com/sites/IQSKY/SIG/Insight%20Collation%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FIQSKY%2FSIG%2FInsight%20Collation%20Documents%2F01%20Analysis%20Requests%2FV306%20-%20Foundation%20-%20Platform%20Maintenance%2FPhase%202%2FPanel%20Balancing

**Business Brief:

Panel Balancing is a regular exercise to ensure that the viewing panel is as
representative of the wider Sky customer base as possible as the latter evolves over
time. Balancing is also crucial in ensuring that key KPI and contractual obligations
of account coverage are maintained.




**Module:                              M05_VirtPan

This module, previously independent, is now part of the Panel Balancing procedure.

Panel definition:
               CHANNEL 4
               ---------
               All those subscribers/accounts that:
               - were on the panels last week
               - have data return metric > 0
               - have ever downloaded any On Demand content

               Additional conditions for the first round (08/03/2013):
               - top up with manual selection of those who have recently downloaded
                 On Demand content, less those that had the prefix issues in the
                 recent enablement files which we will be disabling soon (SBE to advise)
               Expected panel size is between 300k and 350k Subscriber Ids

               CHANNEL 5
               ---------
               All Accounts on any vespa panel with at least one box where:
               - reporting quality known
               - reporting quality > 0


*/




  create or replace procedure V352_M05_VirtPan as begin

           MESSAGE cast(now() as timestamp)||' | VirtPan M05.0 - Create and initialise VirtPan_Channel45_01_Universe_Selection' TO CLIENT

                if object_id('VirtPan_Channel45_01_Universe_Selection') is not null begin
                    truncate table VirtPan_Channel45_01_Universe_Selection
--                        drop table VirtPan_Channel45_01_Universe_Selection
               end
              else begin
                      create table VirtPan_Channel45_01_Universe_Selection (
                             Row_Id                     bigint        identity null
                            ,Run_Date                   date          default today()
                            ,Account_Number             varchar(50)   default null
                            ,Subscriber_id              bigint        default null
                            ,Source                     varchar(30)   default null
                            ,Panel_Id                   tinyint       default 0 null
                            ,On_Demand_DL_Ever          bit           default 0
                            ,Box_Reporting_Quality      decimal(10,3) default -1 null
                            ,Low_Data_Quality_Flag      bit           default 0
                            ,Channel4_Panel_Flag        bit           default 0
                            ,Channel5_Panel_Flag        bit           default 0
                            ,Random_Num                 decimal(10,6) default 0 null
                            ,Created_By                 varchar(30)   default user
                            ,Created_On                 timestamp     default timestamp
                             )
                      create      date index idx1 on VirtPan_Channel45_01_Universe_Selection(Run_Date)
                      create        hg index idx2 on VirtPan_Channel45_01_Universe_Selection(Account_Number)
                      create unique hg index idx3 on VirtPan_Channel45_01_Universe_Selection(Subscriber_id)
                      create           index idx4 on VirtPan_Channel45_01_Universe_Selection(Random_Num)
               end

                   -- #################################################################################
                   -- ##### Create universe - get all boxes from all panels                       #####
                   -- #################################################################################

            MESSAGE cast(now() as timestamp)||' | VirtPan M05.1 - Create universe - insert accounts and subscribers' TO CLIENT

            declare @var_multiplier bigint
                set @var_multiplier = datepart(millisecond,now()) + 1

             insert into VirtPan_Channel45_01_Universe_Selection(
                    account_number
                   ,subscriber_id
                   ,panel_id
                   ,source
                   ,box_reporting_quality
                   ,random_num
                    )
             select vss.account_number
                   ,cast(card_subscriber_id as int) as subscriber_id
                   ,vss.panel_no
                   ,'SBV'
                   ,-1
                   ,rand(number(*) * @var_multiplier)
               from vespa_subscriber_status as vss
              where result = 'Enabled'
                and vss.panel_no in (5, 6, 7, 11, 12)
           group by vss.account_number
                   ,subscriber_id
                   ,vss.panel_no

            MESSAGE cast(now() as timestamp)||' | VirtPan M05.1 - Create universe - Add Reporting Quality from SBV' TO CLIENT

            update VirtPan_Channel45_01_Universe_Selection as bas
               set Box_Reporting_Quality = Reporting_Quality
              from vespa_analysts.vespa_single_box_view as sbv
             where bas.subscriber_id = sbv.subscriber_id
               and status_vespa = 'Enabled'

                -- #################################################################################
                -- ##### Append account/subscriber metrics                                     #####
                -- #################################################################################
                -- Append On Demand DL information

           MESSAGE cast(now() as timestamp)||' | VirtPan M05.2 - Append account-level On Demand download activity flag' TO CLIENT

            update VirtPan_Channel45_01_Universe_Selection base
               set base.On_Demand_DL_Ever = 1
              from (select Account_Number
                          ,min(last_modified_dt) as first_dl_date
                          ,max(last_modified_dt) as last_dl_date
                      from cust_anytime_plus_downloads
                  group by Account_Number) det
             where base.Account_Number = det.Account_Number





                -- #################################################################################
                -- ##### Data quality checks                                                   #####
                -- #################################################################################

                -- ##### Multiple accounts for a single box #####

           MESSAGE cast(now() as timestamp)||' | VirtPan M05.3 - Identify and flag subscribers associated with multiple accounts' TO CLIENT

            update VirtPan_Channel45_01_Universe_Selection base
               set base.Low_Data_Quality_Flag = 1
              from (select Subscriber_Id
                          ,count(distinct Account_Number) as Acc_Nums
                      from VirtPan_Channel45_01_Universe_Selection
                  group by Subscriber_Id) det
             where base.Subscriber_Id = det.Subscriber_Id
               and det.Acc_Nums > 1



                -- Propagate to all associated subscriber Ids within the the account
           MESSAGE cast(now() as timestamp)||' | VirtPan M05.3 - Identify and flag accounts containing subscribers associated with multiple accounts' TO CLIENT

            update VirtPan_Channel45_01_Universe_Selection base
               set base.Low_Data_Quality_Flag = 1
              from (select Account_Number
                          ,max(Low_Data_Quality_Flag) as Low_Quality
                      from VirtPan_Channel45_01_Universe_Selection
                  group by Account_Number
                    having Low_Quality > 0) det
             where base.Account_Number = det.Account_Number



                -- ##### Boxes within an account being on different panels #####
           MESSAGE cast(now() as timestamp)||' | VirtPan M05.3 - Identify and flag accounts containing subscribers associated with multiple accounts' TO CLIENT

            update VirtPan_Channel45_01_Universe_Selection base
               set base.Low_Data_Quality_Flag = 1
              from (select Account_Number
                          ,count(distinct Panel_Id) as Panel_Nums
                      from VirtPan_Channel45_01_Universe_Selection
                  group by Account_Number
                    having Panel_Nums > 1) det
            where base.Account_Number = det.Account_Number

              -- #################################################################################
              -- ##### Virtual panel selection                                               #####
              -- #################################################################################

              -- Reset
         MESSAGE cast(now() as timestamp)||' | VirtPan M05.4 - Reset Channel 4/5 panel flags' TO CLIENT

          update VirtPan_Channel45_01_Universe_Selection
             set Channel4_Panel_Flag = 0,
                 Channel5_Panel_Flag = 0



         MESSAGE cast(now() as timestamp)||' | VirtPan M05.4 - Reset Channel 4/5 panel flags' TO CLIENT

          update VirtPan_Channel45_01_Universe_Selection base
             set Channel4_Panel_Flag = 1                                          -- Joined at Subscriber Id level, so account may include a mixture of boxes "on panel" and "not on panel"
            from (select Subscriber_Id
                    from VirtPan_Channel45_01_Universe_Selection
                   where Run_Date = today()
                     and Low_Data_Quality_Flag = 0
                     and On_Demand_DL_Ever = 1
                     and (   (Box_Reporting_Quality > 0 and Source = 'SBV')          -- SBV boxes
                          or (Box_Reporting_Quality = -1 and Source <> 'SBV')        -- Manual top-up boxes
                         )
          group by Subscriber_Id) det
             where base.Subscriber_Id = det.Subscriber_Id

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


               -- #################################################################################
               -- ##### Populate final table                                                  #####
               -- #################################################################################
               -- Add non-existing accounts first

           insert into vespa_broadcast_reporting_vp_map (Account_Number, Vespa_Panel)
           select base.Account_Number
                 ,base.Panel_Id
             from VirtPan_Channel45_01_Universe_Selection base
                  left join vespa_broadcast_reporting_vp_map det on base.Account_Number = det.Account_Number
            where det.Account_Number is null
              and (   Channel4_Panel_Flag = 1
                   or Channel5_Panel_Flag = 1
                  )
          group by base.Account_Number, base.Panel_Id       -- Our table is Subscriber ID based, the destination is Account Number one

                -- Reset selection
            update vespa_broadcast_reporting_vp_map base
               set base.vp1     = 0
                  ,base.vp2     = 0

                -- Add flags
            update vespa_broadcast_reporting_vp_map base
               set base.vp1     = det.Channel4_Panel_Flag,
                   base.vp2     = det.Channel5_Panel_Flag
              from (select Account_Number
                          ,max(Channel4_Panel_Flag) as Channel4_Panel_Flag
                          ,max(Channel5_Panel_Flag) as Channel5_Panel_Flag
                      from VirtPan_Channel45_01_Universe_Selection
                  group by Account_Number) det
             where base.Account_Number = det.Account_Number

   commit
      end; --V352_M05_VirtPan
 commit;

 grant execute on V352_M05_VirtPan to vespa_group_low_security;
 commit;
