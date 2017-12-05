/*
Code to find the effect of Eco-Mode.

Eco Mode will affect models 'DRX 890', 'DRX 895', 'DRX 595', 'DRX 890W', 'DRX 895W'.
It is set on by default for boxes produced from April 2013, it can be user enabled for the rest.

A box needs to be in standby mode for at least 5 minutes before it enters deep sleep.
The Auto Standby function forces the box into standby after two hours of no use during the night or four hours of no use during the day.
e.g. If someone changed channel at 02.55 and did nothing, the box would go to standby at 04.55 and then deep sleep at 05.00.

*/

--Find the latest record for each service instance
  select * into #stb_active from
       (select account_number
              ,service_instance_id
              ,x_model_number
              ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) rank
          from sk_prod.cust_Set_top_box) as sub
   where rank = 1
; --24,477,536

--Find the boxes that have one of these model numbers
  select subscriber_id
        ,scaling_segment_id
    into #deeps
    from vespa_analysts.vespa_single_box_view as sbv
         inner join #stb_active as stb on sbv.service_instance_id = stb.service_instance_id
         left join vespa_analysts.SC2_Todays_panel_members as pan on sbv.account_number = pan.account_number
   where x_model_number in ('DRX 890', 'DRX 895', 'DRX 595', 'DRX 890W', 'DRX 895W')
group by subscriber_id
        ,scaling_segment_id
; --1,758,604

  create table eco_viewing_unordered(
                           account_number varchar(50)
                          ,subscriber_id int
                          ,event_start_date_time_utc datetime
                          ,event_end_date_time_utc datetime
                          ,log_received_start_date_time_utc datetime
);

  create table eco_viewing(id int identity
                          ,account_number varchar(50)
                          ,subscriber_id int
                          ,event_start_date_time_utc datetime
                          ,event_end_date_time_utc datetime
                          ,last_before_standby bit default 0
                          ,log_received_start_date_time_utc datetime
                          ,scaling_segment_id int
);

--We need non-viewing events as well
  insert into eco_viewing_unordered(account_number
                                   ,subscriber_id
                                   ,event_start_date_time_utc
                                   ,event_end_date_time_utc
                                   ,log_received_start_date_time_utc
)
  select vie.account_number
        ,vie.subscriber_id
        ,event_start_date_time_utc
        ,event_end_date_time_utc
        ,log_received_start_date_time_utc
    from sk_prod.VESPA_DP_PROG_VIEWED_CURRENT as vie
         inner join #deeps as dps on vie.subscriber_id = dps.subscriber_id
   where date(event_start_date_time_utc) between '2013-06-01' and '2013-06-07'
union
  select vie.account_number
        ,vie.subscriber_id
        ,event_start_date_time_utc
        ,event_end_date_time_utc
        ,log_received_start_date_time_utc
    from sk_prod.VESPA_DP_PROG_NON_VIEWED_CURRENT as vie
         inner join #deeps as dps on vie.subscriber_id = dps.subscriber_id
   where date(event_start_date_time_utc) between '2013-06-01' and '2013-06-07'
;

--put them into a new table so that they can be ordered
  insert into eco_viewing(account_number
                         ,subscriber_id
                         ,event_start_date_time_utc
                         ,event_end_date_time_utc
                         ,log_received_start_date_time_utc
)
  select account_number
        ,subscriber_id
        ,event_start_date_time_utc
        ,event_end_date_time_utc
        ,log_received_start_date_time_utc
    from eco_viewing_unordered
order by subscriber_id
        ,event_start_date_time_utc
;

create hg index idx1 on eco_viewing(subscriber_id);
create hg index idx2 on eco_viewing(event_start_date_time_utc);

--flag the last event before standby (we are assuming that standby and eco mode are enabled, and it is night time)
  update eco_viewing as bas
     set last_before_standby = 1
    from eco_viewing as lnk
   where bas.id = lnk.id + 1
     and bas.subscriber_id = lnk.subscriber_id
     and datediff(minute, lnk.event_end_date_time_utc, bas.event_start_date_time_utc) >= 125
;

  update eco_viewing as bas
     set bas.scaling_segment_id = lkp.scaling_segment_id
    from vespa_analysts.sc2_todays_panel_members as lkp
   where bas.account_number = lkp.account_number
;

---------
--results
---------

  select count(distinct bas.subscriber_id)
    from eco_viewing as bas
   where last_before_standby = 1
     and case when datepart(hour, event_start_date_time_utc) < 3 then dateadd(hour, 3, cast(date(event_start_date_time_utc)     as datetime))
                                                                 else dateadd(hour, 3, cast(date(event_start_date_time_utc) + 1 as datetime)) end < log_received_start_date_time_utc
; --55,835 boxes with events that would be missed (10%)

  select count(distinct subscriber_id)
    from sk_prod.VESPA_DP_PROG_VIEWED_CURRENT
   where date(event_start_date_time_utc) between '2013-06-01' and '2013-06-07'
; --539,715 boxes in total

  select count(distinct bas.subscriber_id || event_start_date_time_utc)
    from eco_viewing as bas
   where last_before_standby = 1
     and case when datepart(hour, event_start_date_time_utc) < 3 then dateadd(hour, 3, cast(date(event_start_date_time_utc)     as datetime))
                                                                 else dateadd(hour, 3, cast(date(event_start_date_time_utc) + 1 as datetime)) end < log_received_start_date_time_utc
; --97,647 events that would be missed (0.1%)

  select count(distinct subscriber_id || event_start_date_time_utc)
    from sk_prod.VESPA_DP_PROG_VIEWED_CURRENT
   where date(event_start_date_time_utc) between '2013-06-01' and '2013-06-07'
; --124,010,168 events in total




--Split by scaling variables:

  select count(distinct bas.subscriber_id || event_start_date_time_utc)
--        ,universe
--        ,isba_tv_region
        ,hhcomposition
--        ,tenure
--        ,package
--        ,boxtype
    from eco_viewing as bas
         inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as lkp on bas.scaling_segment_id = lkp.scaling_segment_id
   where last_before_standby = 1
     and case when datepart(hour, event_start_date_time_utc) < 3 then dateadd(hour, 3, cast(date(event_start_date_time_utc)     as datetime))
                                                                 else dateadd(hour, 3, cast(date(event_start_date_time_utc) + 1 as datetime)) end < log_received_start_date_time_utc
group by
--         universe
--        isba_tv_region
        hhcomposition
--        tenure
--        package
--        boxtype

select pan.account_number
      ,universe
      ,isba_tv_region
      ,hhcomposition
      ,tenure
      ,package
      ,boxtype
  into #helper
  from vespa_analysts.SC2_Todays_panel_members            as pan
       inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as lkp on pan.scaling_segment_id = lkp.scaling_segment_id
;

  select count(distinct subscriber_id || event_start_date_time_utc)
--        ,universe
--        ,isba_tv_region
--        ,hhcomposition
        ,tenure
--        ,package
--        ,boxtype
    from sk_prod.VESPA_DP_PROG_VIEWED_CURRENT as bas
         inner join #helper                   as hlp on bas.account_number = hlp.account_number
group by
--         universe
--        isba_tv_region
--        hhcomposition
        tenure
--        package
--        boxtype

































