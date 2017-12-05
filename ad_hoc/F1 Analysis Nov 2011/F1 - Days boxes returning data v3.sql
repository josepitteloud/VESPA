----Box Return data---

---Which boxes return data for each of the days that a Grand Prix takes place---

select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

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

into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
from sk_prod.VESPA_STB_PROG_EVENTS_20110521
group by subscriber_id

;commit;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

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


from sk_prod.VESPA_STB_PROG_EVENTS_20110522
group by subscriber_id

;




insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

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


from sk_prod.VESPA_STB_PROG_EVENTS_20110523
group by subscriber_id

;



insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

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


from sk_prod.VESPA_STB_PROG_EVENTS_20110528
group by subscriber_id

;



insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

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


from sk_prod.VESPA_STB_PROG_EVENTS_20110529
group by subscriber_id

;



insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

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


from sk_prod.VESPA_STB_PROG_EVENTS_20110530
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

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


from sk_prod.VESPA_STB_PROG_EVENTS_20110611
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

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


from sk_prod.VESPA_STB_PROG_EVENTS_20110612
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

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


from sk_prod.VESPA_STB_PROG_EVENTS_20110613
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

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


from sk_prod.VESPA_STB_PROG_EVENTS_20110625
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

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


from sk_prod.VESPA_STB_PROG_EVENTS_20110626
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

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


from sk_prod.VESPA_STB_PROG_EVENTS_20110627
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

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


from sk_prod.VESPA_STB_PROG_EVENTS_20110709
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

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


from sk_prod.VESPA_STB_PROG_EVENTS_20110710
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

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


from sk_prod.VESPA_STB_PROG_EVENTS_20110711
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

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


from sk_prod.VESPA_STB_PROG_EVENTS_20110723
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

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


from sk_prod.VESPA_STB_PROG_EVENTS_20110724
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

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


from sk_prod.VESPA_STB_PROG_EVENTS_20110725
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

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


from sk_prod.VESPA_STB_PROG_EVENTS_20110730
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

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


from sk_prod.VESPA_STB_PROG_EVENTS_20110731
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

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


from sk_prod.VESPA_STB_PROG_EVENTS_20110801
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

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


from sk_prod.VESPA_STB_PROG_EVENTS_20110827
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

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


from sk_prod.VESPA_STB_PROG_EVENTS_20110828
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

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


from sk_prod.VESPA_STB_PROG_EVENTS_20110829
group by subscriber_id

;




----Create overall summary from daily versions---
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(events_2011_05_21) as events_2011_05_21_any

,max(events_2011_05_22) as events_2011_05_22_any

--Monaco GP
,max(events_2011_05_28) as events_2011_05_28_any
,max(events_2011_05_29) as events_2011_05_29_any

--Canadian GP
,max(events_2011_06_11) as events_2011_06_11_any
,max(events_2011_06_12) as events_2011_06_12_any

--European GP
,max(events_2011_06_25) as events_2011_06_25_any
,max(events_2011_06_26) as events_2011_06_26_any

--British GP
,max(events_2011_07_09) as events_2011_07_09_any
,max(events_2011_07_10) as events_2011_07_10_any

--German GP
,max(events_2011_07_23) as events_2011_07_23_any
,max(events_2011_07_24) as events_2011_07_24_any

--Hungarian GP
,max(events_2011_07_30) as events_2011_07_30_any
,max(events_2011_07_31) as events_2011_07_31_any

--Belgian GP
,max(events_2011_08_27) as events_2011_08_27_any
,max(events_2011_08_29) as events_2011_08_29_any

into vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
group by subscriber_id

;
commit;

--select top 100 * from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped;
--select sum(events_2011_05_22_any),sum(events_2011_08_29_any) from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped;
drop table  vespa_analysts.daily_summary_by_subscriber_f1_event_days_full;
commit;
