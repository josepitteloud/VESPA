            create variable                   @first_enablement date;
            create variable @now date;
            create variable @p6  int;
            create variable @p7  int;
               set @now = now();
set @first_enablement = '2015-01-01'

            insert into vespa_analysts.panel_movements_log(
                   account_number
                  ,card_subscriber_id
                  ,request_created_dt
                  ,requested_enablement_route
                  ,last_ca_callback_route
                  ,multiroom
                  ,requested_movement_type
                  ,destination
                   )
            select ccs.account_number
                  ,ccs.card_subscriber_id
                  ,@now
                  ,'KQ'
                  ,'Unknown' as last_ca_callback_route
                  ,0
                  ,'Panel Expansion'
                  ,case when knockout_level_bb = 9999 then 5 else 6 end
    from panbal_segments_lookup_normalised            as lkp
         inner join panbal_segment_snapshots          as snp on snp.segment_id     = lkp.segment_id
         inner join waterfall_base                    as bas on bas.account_number = snp.account_number
         inner join CUST_CARD_SUBSCRIBER_LINK         as ccs on ccs.account_number = snp.account_number
   where aggregation_variable = 5
     and value in ('Dual Sports'
                  ,'Other Premiums'
                  ,'Single Sports'
                  ,'Top Tier'
                   )
     and (knockout_level_bb = 9999 or knockout_level_pstn = 9999)
     and ccs.effective_to_dt = '9999-09-09'
;


                -- find last On Demand download date by box
            create table #dl_by_box(
                   card_id             varchar(30)
                  ,service_instance_id varchar(30)
                  ,subscriber_id       int
                  ,max_dt              date)
;
            insert into #dl_by_box(
                   card_id
                  ,max_dt)
            select card_id
                  ,max(last_modified_dt) as max_dt
              from CUST_ANYTIME_PLUS_DOWNLOADS as apd
                   inner join vespa_analysts.panel_movements_log as log on apd.account_number = log.account_number
             where log.request_created_dt = @now
          group by card_id
;

            commit;
            create unique hg index uhcar on #dl_by_box(card_id)
            create        hg index uhsub on #dl_by_box(subscriber_id)
            create        hg index uhser on #dl_by_box(service_instance_id)
;

            update #dl_by_box as bas
               set bas.service_instance_id = cid.service_instance_id
              from cust_card_issue_dim as cid
             where bas.card_id = left(cid.card_id, 8)
               and card_status = 'Enabled'
;
            update #dl_by_box as bas
               set bas.subscriber_id = csi.si_external_identifier
              from cust_service_instance as csi
             where bas.service_instance_id = csi.service_instance_id
               and effective_to_dt = '9999-09-09'
;
                -- if there has been an on demand download in the last 6 months (by box)
            update vespa_analysts.panel_movements_log as bas
               set last_ondemand_download_dt = max_dt
              from #dl_by_box as dls
             where cast(bas.card_subscriber_id as int) = dls.subscriber_id
               and request_created_dt = @now
;
            update vespa_analysts.panel_movements_log as bas
               set source = panel
                  ,bas.rq = sav.rq
                  ,ca_callback_rate = cbck_rate
              from panbal_sav as sav
             where bas.account_number = sav.account_number
               and request_created_dt = @now
;

            select @p6 = count(1)
              from vespa_analysts.vespa_single_box_view
             where status_vespa = 'Enabled'
               and panel_id_vespa = 6
;
            select @p7 = count(1)
              from vespa_analysts.vespa_single_box_view
             where status_vespa = 'Enabled'
               and panel_id_vespa = 7
;
            update vespa_analysts.panel_movements_log as bas
               set destination = 7
             where destination = 6
               and @p7 < @p6
               and request_created_dt = @now
;

            update vespa_analysts.panel_movements_log as bas
               set multiroom = value
              from panbal_segments_lookup_normalised            as lkp
                   inner join panbal_segment_snapshots          as snp on snp.segment_id     = lkp.segment_id
             where bas.account_number = snp.account_number
               and request_created_dt = @now
               and aggregation_variable = 6
;


            create variable @var_multiplier bigint;
                set @var_multiplier = datepart(millisecond,now()) + 1;
select bas.account_number
      ,rand(number(*) * @var_multiplier) as ran_no
into #temp
from vespa_analysts.panel_movements_log as bas
inner join vespa_analysts.Waterfall_SCMS_callback_data as scm on bas.account_number = scm.account_number
               and request_created_dt = @now
where cbk_day is null



            update vespa_analysts.panel_movements_log as bas
               set bas.ca_callback_day         = scm.cbk_day
                  ,last_CA_callback_dt = date(substr(date_time_received,7,4) || '-' || substr(date_time_received,4,2) || '-' || left(date_time_received,2))
                  ,requested_enablement_dt  = case when cbk_day is null then dateadd(day,cast((ran_no * 28) as int), @first_enablement)
                                                   when cast(cbk_day as int) >= datepart(day,@first_enablement) then dateadd(day, cast(cbk_day as int) - datepart(day,@first_enablement), @first_enablement)
                                                   else dateadd(day, cast(cbk_day as int) - datepart(day,@first_enablement), dateadd(month, 1, @first_enablement))
                                               end
              from vespa_analysts.Waterfall_SCMS_callback_data as scm
              left join #temp as tmp on scm.account_number = tmp.account_number
             where bas.account_number = scm.account_number
               and request_created_dt = @now
;


            update vespa_analysts.panel_movements_log as bas
set requested_enablement_dt='2015-01-01'
and request_created_dt = '2014-12-12'
and requested_enablement_dt is null





select bas.account_number
      ,rand(number(*) * @var_multiplier) as ran_no
into #temp2
from vespa_analysts.panel_movements_log as bas
               where request_created_dt = @now
and destination=6

update vespa_analysts.panel_movements_log as log
set destination = case when knockout_level_bb=9999 then 5 else 6 end
from waterfall_base as wat
where log.account_number = wat.account_number
where request_created_dt='2014-12-12'

update vespa_analysts.panel_movements_log as log
set destination = case when ran_no >.5 then 7 else 6 end
from #temp2 as tmp
where request_created_dt='2014-12-12'
and log.account_number = tmp.account_number
;
select source,destination,count(1)
from vespa_analysts.panel_movements_log
where request_created_dt='2014-12-12'
group by source,destination

select top 10 * from vespa_analysts.panel_movements_log
where request_created_dt='2014-12-12'

select requested_enablement_dt,count()
from vespa_analysts.panel_movements_log
where request_created_dt='2014-12-12'
group by requested_enablement_dt





