----Box Return data---

---Which boxes return data for each of the days that a Grand Prix takes place---

select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21
/*
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-22 05:00:00' and '2011-05-23 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-22 05:00:00' and  adjusted_event_start_time <'2011-05-23 04:59:59' then 1
else 0
 end) as events_2011_05_22

--Monaco GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-28 05:00:00' and '2011-05-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-28 05:00:00' and  adjusted_event_start_time <'2011-05-29 04:59:59' then 1
else 0
 end) as events_2011_05_28
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-29 05:00:00' and '2011-05-30 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-29 05:00:00' and  adjusted_event_start_time <'2011-05-30 04:59:59' then 1
else 0
 end) as events_2011_05_29

--Canadian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-11 05:00:00' and '2011-06-12 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-11 05:00:00' and  adjusted_event_start_time <'2011-06-12 04:59:59' then 1
else 0
 end) as events_2011_06_11
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-12 05:00:00' and '2011-06-13 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-12 05:00:00' and  adjusted_event_start_time <'2011-06-13 04:59:59' then 1
else 0
 end) as events_2011_06_12

--European GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-25 05:00:00' and '2011-06-26 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-25 05:00:00' and  adjusted_event_start_time <'2011-06-26 04:59:59' then 1
else 0
 end) as events_2011_06_25
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-26 05:00:00' and '2011-06-27 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-26 05:00:00' and  adjusted_event_start_time <'2011-06-27 04:59:59' then 1
else 0
 end) as events_2011_06_26

--British GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-09 05:00:00' and '2011-07-10 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-09 05:00:00' and  adjusted_event_start_time <'2011-07-10 04:59:59' then 1
else 0
 end) as events_2011_07_09
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-10 05:00:00' and '2011-07-11 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-10 05:00:00' and  adjusted_event_start_time <'2011-07-11 04:59:59' then 1
else 0
 end) as events_2011_07_10

--German GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-23 05:00:00' and '2011-07-24 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-23 05:00:00' and  adjusted_event_start_time <'2011-07-24 04:59:59' then 1
else 0
 end) as events_2011_07_23
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-24 05:00:00' and '2011-07-25 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-24 05:00:00' and  adjusted_event_start_time <'2011-07-25 04:59:59' then 1
else 0
 end) as events_2011_07_24

--Hungarian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-30 05:00:00' and '2011-07-31 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-30 05:00:00' and  adjusted_event_start_time <'2011-07-31 04:59:59' then 1
else 0
 end) as events_2011_07_30
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-31 05:00:00' and '2011-08-01 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-31 05:00:00' and  adjusted_event_start_time <'2011-08-01 04:59:59' then 1
else 0
 end) as events_2011_07_31

--Belgian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-27 05:00:00' and '2011-08-28 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-27 05:00:00' and  adjusted_event_start_time <'2011-08-28 04:59:59' then 1
else 0
 end) as events_2011_08_27
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29

into vespa_analysts.daily_summary_by_subscriber_f1_event_days
from sk_prod.vespa_events_view
group by subscriber_id

;
commit;


select subscriber_id
,document_creation_date
,stb_log_creation_date
,adjusted_event_start_time
from
    sk_prod.VESPA_STB_PROG_EVENTS_20110828
where event_type = 'evEmptyLog'




--select count(*) from vespa_analysts.daily_summary_by_subscriber_f1_event_days;

select * from
    sk_prod.VESPA_STB_PROG_EVENTS_20110826
where  subscriber_id=39984  
order by document_creation_date

;




select subscriber_id 
,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29_01
from
    sk_prod.VESPA_STB_PROG_EVENTS_20110828

group by subscriber_id

commit;


union 
 subscriber_id 
,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29_02
from
    sk_prod.VESPA_STB_PROG_EVENTS_20110829
