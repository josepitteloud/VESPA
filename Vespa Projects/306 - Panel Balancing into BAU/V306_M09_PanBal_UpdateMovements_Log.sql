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




**Module:                              M09_UpdateLog

This module updates the Panel movement log. It is not run as part of the balancing process, it is run independently when happy to go ahead with the movements.

*/


  create or replace procedure V306_M09_Update_Movements_Log
         @first_enablement date
      as begin

/*
              call dba.create_table('vespa_analysts', 'panel_movements_log',
                  'account_number             varchar(30)  DEFAULT NULL
                  ,card_subscriber_id         varchar(8)   DEFAULT NULL
                  ,source                     tinyint      DEFAULT NULL
                  ,destination                tinyint      DEFAULT NULL
                  ,RQ                         double       DEFAULT NULL
                  ,CA_callback_rate           double       DEFAULT NULL
                  ,CA_callback_day            tinyint      DEFAULT NULL
                  ,requested_enablement_dt    date         DEFAULT NULL
                  ,requested_movement_type    varchar(20)  DEFAULT NULL
                  ,requested_enablement_route varchar(3)   DEFAULT NULL
                  ,multiroom                  bit NOT NULL DEFAULT 0
                  ,last_ca_callback_route     varchar(7)   DEFAULT NULL
                  ,last_ondemand_download_dt  date         DEFAULT NULL
                  ,last_ca_callback_dt        date         DEFAULT NULL
                  ,request_created_dt         timestamp    DEFAULT NULL
                  ')
                  ;
          create hg index hgacc on vespa_analysts.panel_movements_log(account_number);
          create hg index hgsub on vespa_analysts.panel_movements_log(card_subscriber_id);
*/

           declare @now date
           declare @p6  int
           declare @p7  int
               set @now = now()

            insert into vespa_analysts.panel_movements_log(
                   account_number
                  ,card_subscriber_id
                  ,request_created_dt
                  ,requested_enablement_route
                  ,last_ca_callback_route
                  ,multiroom
                  ,requested_movement_type
                  ,source
                  ,destination
                  ,requested_enablement_dt
                   )
            select bas.account_number
                  ,card_subscriber_id
                  ,@now as request_created_dt
                  ,'OTA' as requested_enablement_route
                  ,'Unknown' as last_ca_callback_route
                  ,0 as multiroom
                  ,'Panel Balancing' as requested_movement_type
                  ,null as source
                  ,null as destination
                  ,@first_enablement as requested_enablement_dt
              from panbal_amends as bas
                   inner join cust_card_subscriber_link as stb on bas.account_number = stb.account_number
             where current_flag='Y'
              and movement <> 'Account to add to secondary panels as segment backup'
          group by bas.account_number
                  ,card_subscriber_id
                  ,request_created_dt
                  ,requested_enablement_route
                  ,last_ca_callback_route
                  ,multiroom
                  ,requested_movement_type
                  ,source
                  ,destination

                -- if there has been an on demand download in the last 6 months (by box)
            update vespa_analysts.panel_movements_log as bas
               set last_ondemand_download_dt = last_download_date
              from stb_connection_fact as stb
             where cast(bas.card_subscriber_id as int) = stb.scms_subscriber_id
               and request_created_dt = @now

            update vespa_analysts.panel_movements_log as bas
               set source = panel
                  ,bas.rq = sav.rq
                  ,ca_callback_rate = cbck_rate
              from panbal_sav as sav
             where bas.account_number = sav.account_number
               and request_created_dt = @now

--             select @p6 = count(1)
--               from vespa_analysts.vespa_single_box_view
--              where status_vespa = 'Enabled'
--                and panel_id_vespa = 6
--
--             select @p7 = count(1)
--               from vespa_analysts.vespa_single_box_view
--              where status_vespa = 'Enabled'
--                and panel_id_vespa = 7
--
--             update vespa_analysts.panel_movements_log as bas
--                set destination = 7
--              where destination = 6
--                and @p7 < @p6
--                and request_created_dt = @now

            update vespa_analysts.panel_movements_log as bas
               set multiroom = value
              from panbal_segment_snapshots as snp
                   inner join panbal_segments_lookup_normalised as lkp on snp.segment_id = lkp.segment_id
             where bas.account_number = snp.account_number
               and request_created_dt = @now
               and aggregation_variable = 6

            update vespa_analysts.panel_movements_log as bas
               set bas.ca_callback_day         = scm.cbk_day
                  ,last_CA_callback_dt = date(substr(date_time_received,7,4) || '-' || substr(date_time_received,4,2) || '-' || left(date_time_received,2))
                  ,requested_enablement_dt  = case when cbk_day is null                            then @first_enablement
                                                   when cast(cbk_day as int) >= datepart(day,@first_enablement) then dateadd(day, datepart(day,@first_enablement) - cast(cbk_day as int), @first_enablement)
                                                   else dateadd(day, cast(cbk_day as int) - datepart(day,@first_enablement), dateadd(month, 1, @first_enablement))
                                               end
              from vespa_analysts.Waterfall_SCMS_callback_data as scm
             where bas.account_number = scm.account_number
               and request_created_dt = @now
               and requested_enablement_route = 'KQ'

update vespa_analysts.panel_movements_log
set destination=11
where request_created_dt = @now

update vespa_analysts.panel_movements_log as pml
set destination = 12
from waterfall_base as wat
where pml.account_number = wat.account_number
and knockout_level_bb <> 9999
and request_created_dt = @now

      end; --V306_M09_Update_Movements_Log

    grant execute on V306_M09_Update_Movements_Log to vespa_group_low_security;





select top 10 * from stb_connection_fact

select movement,count() from panbal_amends group by movement


select count() from panel_movements_log_bak_20150922
6072933
select count() from vespa_analysts.panel_movements_log
6156233

select top 10 * from vespa_analysts.panel_movements_log

select count(distinct card_subscriber_id || request_created_dt) from vespa_analysts.panel_movements_log
6058371

select top 10 card_subscriber_id || request_created_dt as x
,count() as cow
 from vespa_analysts.panel_movements_log
group by x
having cow>1

select *
from vespa_analysts.panel_movements_log
where card_subscriber_id='00015487'
2015-04-07 00:00:00.000000

update vespa_analysts.panel_movements_log as bas
set requested_enablement_dt = jon.requested_enablement_dt
from panel_movements_log_bak_20150922 as jon
where bas.card_subscriber_id = jon.card_subscriber_id
and bas.request_created_dt = jon.request_created_dt

select max(requested_enablement_dt) from vespa_analysts.panel_movements_log

update vespa_analysts.panel_movements_log
set requested_enablement_dt = '2015-11-25'
where request_created_dt = @now

select source,destination,count()
from vespa_analysts.panel_movements_log
where request_created_dt = @now
group by source,destination

delete from
vespa_analysts.panel_movements_log
where request_created_dt = @now
and source in (11,12)

select case when knockout_level_bb = 9999 then 1 else 0 end as bb
,case when knockout_level_pstn = 9999 then 1 else 0 end as pstn
,count()
from waterfall_base as wat
inner join panbal_amends as ame on wat.account_number = ame.account_number
where movement <> 'Account to add to secondary panels as segment backup'
group by bb,pstn

select knockout_level_bb
,knockout_reason_bb
,knockout_level_pstn
,knockout_reason_pstn
,count()
from panbal_amends as ame
inner join waterfall_base as wat on wat.account_number=ame.account_number
where knockout_level_bb<9999
and knockout_level_pstn<9999
and movement <> 'Account to add to secondary panels as segment backup'
group by knockout_level_bb
,knockout_reason_bb
,knockout_level_pstn
,knockout_reason_pstn


select source,destination,count()
from vespa_analysts.panel_movements_log
where request_created_dt = '2015-11-24'
group by source,destination

delete from vespa_analysts.panel_movements_log
where request_created_dt = '2015-11-24'
and account_number in (select wat.account_number from waterfall_base as wat inner join panbal_amends as ame on wat.account_number = ame.account_number where knockout_level_bb=9999 and knockout_level_pstn<9999)

select count()
from vespa_analysts.panel_movements_log
where request_created_dt = '2015-11-24'

update vespa_analysts.panel_movements_log
set destination = 11
where request_created_dt = '2015-11-24'
and source = 5


