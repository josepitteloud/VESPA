--new, more basic Version created JG 20170503
--last updated JG 20170519
      if object_id('SBV_refresh_single_box_view') is not null drop procedure SBV_refresh_single_box_view;

  create procedure SBV_refresh_single_box_view
      as begin

            select account_number
              into #accs
              from cust_single_account_view
             where cust_active_dtv = 1
          group by account_number

            commit
            create unique hg index uhacc on #accs(account_number)

            select ccs.account_number
                  ,card_subscriber_id
                  ,cast(card_subscriber_id as int) as subscriber_id
              into #subs
              from cust_card_subscriber_link as ccs
                   inner join #accs as acc on ccs.account_number = acc.account_number
             where current_flag = 'Y'
               and subscriber_id is not null

            commit
            create hg index hgsub on #subs(subscriber_id)

            select subscriber_id
                  ,count() as cow
              into #dupes
              from #subs
          group by subscriber_id
            having cow > 1

         truncate table vespa_analysts.vespa_single_box_view

           insert into vespa_analysts.vespa_single_box_view
                  (account_number
                  ,card_subscriber_id
                  ,subscriber_id
                  )
            select sub.account_number
                  ,card_subscriber_id
                  ,sub.subscriber_id as sub
              from #subs as sub
                   left join #dupes as dup on sub.subscriber_id = dup.subscriber_id
             where dup.subscriber_id is null

            update vespa_analysts.vespa_single_box_view as bas
               set panel_id_vespa = panel_no
                  ,panel = cast(panel_no as varchar)
                  ,status_vespa = 'Enabled'
              from vespa_panel_status_manual as pan
             where bas.subscriber_id = pan.subscriber_id

            update vespa_analysts.vespa_single_box_view as bas
               set service_instance_id = csi.service_instance_id
              from cust_service_instance as csi
             where bas.card_subscriber_id = csi.si_external_identifier

            update vespa_analysts.vespa_single_box_view as bas
               set reporting_quality = rq
              from panbal_sav as sav
             where bas.account_number = sav.account_number

            select service_instance_id
                  ,max(issue_date) as issue
              into #cci
              from cust_card_issue_dim as cci
             where issue_date < today()
          group by service_instance_id

            commit
            create unique hg index uhser on #cci(service_instance_id)
            create date index dtiss on #cci(issue)

            select cci.service_instance_id
                  ,max(card_id) as card_id
              into #cci2
              from cust_card_issue_dim as cci
                   inner join #cci on cci.issue_date          = #cci.issue
                                  and cci.service_instance_id = #cci.service_instance_id
          group by cci.service_instance_id

            commit
            create unique hg index uhser on #cci2(service_instance_id)

            update vespa_analysts.vespa_single_box_view as bas
               set bas.viewing_card_id = cci.card_id
              from #cci2 as cci
             where bas.service_instance_id = cci.service_instance_id

            update vespa_analysts.vespa_single_box_view as bas
               set bas.user_sk = reg.user_sk
              from sky_player_registrant as reg
             where bas.service_instance_id = reg.src_system_id

            update vespa_analysts.vespa_single_box_view as bas
               set bas.sam_profile_id = reg.sam_profile_id
              from sky_player_registrant as reg
             where bas.service_instance_id = reg.src_system_id

     end;

select top 10 *    from SKY_PLAYER_LOGIN_DETAIL
samprofileid
user_sk

select top 10 *    from SKY_PLAYER_registrant
user_sk
sam_profile_id
account_number

select top 10 *    from vespa_analysts.vespa_single_box_view
where service_instance_id like 'CH%'

select count()
              from sky_player_registrant as reg
inner join vespa_analysts.vespa_single_box_view as bas
           on bas.subscriber_id = reg.subscription_id



select left(src_system_id,2) as l
,count()
--from cust_service_instance
from sky_player_registrant
group by l



select case when sam_profile_id is null then 0 else 1 end as s,count()
from vespa_analysts.vespa_single_box_view group by s


/*
create hg index hgser on vespa_analysts.vespa_single_box_view(service_instance_id)
create unique hg index uhcar on vespa_analysts.vespa_single_box_view(card_subscriber_id)
*/

/*
  create table vespa_analysts.vespa_single_box_view
              (subscriber_id       bigint
              ,card_subscriber_id  varchar(30)
              ,account_number      varchar(30)
              ,service_instance_id varchar(30)
              ,viewing_card_id     bigint
              ,Panel_ID_Vespa      smallint
              ,Panel               smallint    --only required for scaling
              ,Status_Vespa        varchar(20) --only required for scaling


alter table vespa_analysts.vespa_single_box_view add user_sk bigint
alter table vespa_analysts.vespa_single_box_view add sam_profile_id varchar(100)


--not updated
cb_key_individual
consumerview_cb_row_id
Panel_ID_4_cells_confirm
in_vespa_panel
in_vespa_panel_11
Is_Sky_View_candidate
Is_Sky_View_Selected
cust_active_dtv
uk_standard_account
alternate_panel_5
alternate_panel_6
alternate_panel_7
Enablement_date
Enablement_date_source
vss_request_dt
Sky_View_load_date
historic_result_date
Selection_date
vss_created_date
In_stb_log_snapshot
logs_every_day_30d
logs_returned_in_30d
reporting_quality
PS_Olive
PS_Vespa
PS_inferred_primary
PS_flag
PS_source
Box_type_subs
Box_type_physical
HD_box_subs
HD_box_physical
HD_1TB_physical
Box_is_3D
Account_anytime_plus
Box_has_anytime_plus
PVR
prem_sports
prem_movies










