DECLARE @latest_full_date 		date

-- No longer looking at reporting relative to which day the report is actioned on, instead
-- running it on a fixed Sunday -> Saturday data set for the week before. Well, whatever
-- the standardised end day of weekly reporting is as defined in our procedure.
execute vespa_analysts.Regulars_Get_report_end_date @latest_full_date output

SELECT sav.account_number
      ,rtm.rtm_detail
      ,dateadd(day, 7, max(sav.booking_dt)) data_chk
        , sav.prod_dtv_ordered_dt
      ,min(sav.cust_viewing_data_capture_allowed) cust_viewing_data_capture_allowed -- required specifically for opt out report
--      ,max(sav.booking_dt) as most_recent_DTV_booking - not used in any report?
      ,max(case when sav.prod_latest_dtv_status_code in ('AB','AC','PC') then 1 else 0 end) dtv_customer
      ,sav.booking_dt
      ,convert(tinyint, case  -- it's about RTMs collecting data, which is something that happens at the point of booking, hence booking date.
          when dateadd(day, 7, max(sav.booking_dt)) > @latest_full_date then 3 -- activated within the last week
          when max(sav.booking_dt) >= '2011-05-26' then 2 -- Chordant Fix in place from 26th of May
          when max(sav.booking_dt) between '2011-04-28' and '2011-05-25' then 1 -- RTMs collecting opt-out data since 28th of April
          else 0
       end) is_new_customer,
       SAV.CUST_VIEWING_CAPTURE_ALLWD_START_DT,
       SAV.CUST_PREV_VIEWING_CAPTURE_ALLOWED
       into vespa_OpDash_sky_base_listing
FROM CITEAM.RTM_DO_NOT_DELETE AS rtm
inner JOIN
sk_prod.cust_single_account_view AS sav
ON rtm.account_number = sav.account_number
WHERE sav.prod_latest_dtv_status_code IN ('AC','AB','PC')
GROUP BY sav.account_number,rtm_detail,sav.booking_dt,SAV.CUST_VIEWING_CAPTURE_ALLWD_START_DT,
SAV.CUST_PREV_VIEWING_CAPTURE_ALLOWED        , sav.prod_dtv_ordered_dt

commit
/*
drop table acct_start_date

select * into acct_start_date
from
(SELECT b.ACCOUNT_NUMBER, MIN(a.STATUS_START_DT) FIRST_CONTACT
FROM SK_PROD.CUST_SUBS_HIST a,  vespa_OpDash_sky_base_listing b
where a.account_number = b.account_number
GROUP BY b.ACCOUNT_NUMBER) t
*/
DROP TABLE VESPA_ACCOUNTS_CHECK

SELECT * INTO VESPA_ACCOUNTS_CHECK
FROM
(SELECT A.*,case when booking_dt < '2011-04-01' then '1 - HISTORIC'
when booking_dt BETWEEN  '2011-04-01' AND '2011-06-30' then '2 - APR_JUN_2011'
when booking_dt BETWEEN  '2011-07-01' AND '2011-09-30' then '3 - JUL_SEP_2011'
when booking_dt BETWEEN  '2011-10-01' AND '2011-12-31' then '4 - OCT_DEC_2011'
when booking_dt BETWEEN  '2012-01-01' AND '2012-03-31' then '5 - JAN_MAR_2012'
when booking_dt BETWEEN  '2012-04-01' AND '2012-06-30' then '6 - APR_JUN_2012'
when booking_dt BETWEEN  '2012-07-01' AND '2012-09-30' then '7 - JUL_SEP_2012'
when booking_dt BETWEEN  '2012-10-01' AND '2012-12-31' then '8 - OCT_DEC_2012'
when booking_dt BETWEEN  '2013-01-01' AND '2013-03-31' then '9 - JAN_MAR_2013'
when booking_dt BETWEEN  '2013-04-01' AND '2013-06-30' then '99 - APR_JUN_2013'
ELSE '0' END NEW_CUSTOMER_JOIN_QUARTER,
case when date(CUST_VIEWING_CAPTURE_ALLWD_START_DT) < '2011-04-01' then '1 - HISTORIC'
when date(CUST_VIEWING_CAPTURE_ALLWD_START_DT) BETWEEN  '2011-04-01' AND '2011-06-30' then '2 - APR_JUN_2011'
when date(CUST_VIEWING_CAPTURE_ALLWD_START_DT) BETWEEN  '2011-07-01' AND '2011-09-30' then '3 - JUL_SEP_2011'
when date(CUST_VIEWING_CAPTURE_ALLWD_START_DT) BETWEEN  '2011-10-01' AND '2011-12-31' then '4 - OCT_DEC_2011'
when date(CUST_VIEWING_CAPTURE_ALLWD_START_DT) BETWEEN  '2012-01-01' AND '2012-03-31' then '5 - JAN_MAR_2012'
when date(CUST_VIEWING_CAPTURE_ALLWD_START_DT) BETWEEN  '2012-04-01' AND '2012-06-30' then '6 - APR_JUN_2012'
when date(CUST_VIEWING_CAPTURE_ALLWD_START_DT) BETWEEN  '2012-07-01' AND '2012-09-30' then '7 - JUL_SEP_2012'
when date(CUST_VIEWING_CAPTURE_ALLWD_START_DT) BETWEEN  '2012-10-01' AND '2012-12-31' then '8 - OCT_DEC_2012'
when date(CUST_VIEWING_CAPTURE_ALLWD_START_DT) BETWEEN  '2013-01-01' AND '2013-03-31' then '9 - JAN_MAR_2013'
when date(CUST_VIEWING_CAPTURE_ALLWD_START_DT) BETWEEN  '2013-04-01' AND '2013-06-30' then '99 - APR_JUN_2013'
ELSE '0' END VESPA_CONSENT_DATE_QUARTER,
case when CUST_VIEWING_DATA_CAPTURE_ALLOWED = 'Y' AND CUST_PREV_VIEWING_CAPTURE_ALLOWED = '?' then 1 else 0 end OPT_IN_UNCHANGED,
case when CUST_VIEWING_DATA_CAPTURE_ALLOWED = 'N'  AND CUST_PREV_VIEWING_CAPTURE_ALLOWED = '?' then 1 else 0 end OPT_OUT_UNCHANGED,
case when CUST_VIEWING_DATA_CAPTURE_ALLOWED = '?'  AND CUST_PREV_VIEWING_CAPTURE_ALLOWED = '?' then 1 else 0 end OPT_QUESTION_UNCHANGED,
case when CUST_VIEWING_DATA_CAPTURE_ALLOWED = 'Y' AND CUST_PREV_VIEWING_CAPTURE_ALLOWED != '?' then 1 else 0 end OPT_IN_CHANGED,
case when CUST_VIEWING_DATA_CAPTURE_ALLOWED = 'N'  AND CUST_PREV_VIEWING_CAPTURE_ALLOWED != '?' then 1 else 0 end OPT_OUT_CHANGED,
case when CUST_VIEWING_DATA_CAPTURE_ALLOWED = '?'  AND CUST_PREV_VIEWING_CAPTURE_ALLOWED != '?' then 1 else 0 end OPT_QUESTION_CHANGED,
case when b.first_contact < '2011-04-01' then '1 - HISTORIC'
when b.first_contact BETWEEN  '2011-04-01' AND '2011-06-30' then '2 - APR_JUN_2011'
when b.first_contact BETWEEN  '2011-07-01' AND '2011-09-30' then '3 - JUL_SEP_2011'
when b.first_contact BETWEEN  '2011-10-01' AND '2011-12-31' then '4 - OCT_DEC_2011'
when b.first_contact BETWEEN  '2012-01-01' AND '2012-03-31' then '5 - JAN_MAR_2012'
when b.first_contact BETWEEN  '2012-04-01' AND '2012-06-30' then '6 - APR_JUN_2012'
when b.first_contact BETWEEN  '2012-07-01' AND '2012-09-30' then '7 - JUL_SEP_2012'
when b.first_contact BETWEEN  '2012-10-01' AND '2012-12-31' then '8 - OCT_DEC_2012'
when b.first_contact BETWEEN  '2013-01-01' AND '2013-03-31' then '9 - JAN_MAR_2013'
when b.first_contact BETWEEN  '2013-04-01' AND '2013-06-30' then '99 - APR_JUN_2013'
ELSE '0' END CUSTOMER_FIRST_CONTACT,
case when A.prod_dtv_ordered_dt < '2011-04-01' then '1 - HISTORIC'
when A.prod_dtv_ordered_dt BETWEEN  '2011-04-01' AND '2011-06-30' then '2 - APR_JUN_2011'
when A.prod_dtv_ordered_dt BETWEEN  '2011-07-01' AND '2011-09-30' then '3 - JUL_SEP_2011'
when A.prod_dtv_ordered_dt BETWEEN  '2011-10-01' AND '2011-12-31' then '4 - OCT_DEC_2011'
when A.prod_dtv_ordered_dt BETWEEN  '2012-01-01' AND '2012-03-31' then '5 - JAN_MAR_2012'
when A.prod_dtv_ordered_dt BETWEEN  '2012-04-01' AND '2012-06-30' then '6 - APR_JUN_2012'
when A.prod_dtv_ordered_dt BETWEEN  '2012-07-01' AND '2012-09-30' then '7 - JUL_SEP_2012'
when A.prod_dtv_ordered_dt BETWEEN  '2012-10-01' AND '2012-12-31' then '8 - OCT_DEC_2012'
when A.prod_dtv_ordered_dt BETWEEN  '2013-01-01' AND '2013-03-31' then '9 - JAN_MAR_2013'
when A.prod_dtv_ordered_dt BETWEEN  '2013-04-01' AND '2013-06-30' then '99 - APR_JUN_2013'
ELSE '0' END CUSTOMER_FIRST_CONTACT_NEW
FROM vespa_OpDash_sky_base_listing A,
acct_start_date b
where a.account_number = b.account_number
and UPPER(a.rtm_detail) not like '%ROI%') T





SELECT 
--NEW_CUSTOMER_JOIN_QUARTER,
customer_first_contact,
VESPA_CONSENT_DATE_QUARTER,
SUM(OPT_IN_UNCHANGED) OPT_IN_UNCHANGED,
SUM(OPT_OUT_UNCHANGED) OPT_OUT_UNCHANGED,
SUM(OPT_QUESTION_UNCHANGED) OPT_QUESTION_UNCHANGED,
SUM(OPT_IN_CHANGED) OPT_IN_CHANGED,
SUM(OPT_OUT_CHANGED) OPT_OUT_CHANGED,
SUM(OPT_QUESTION_CHANGED) OPT_QUESTION_CHANGED FROM
VESPA_ACCOUNTS_CHECK
WHERE VESPA_CONSENT_DATE_QUARTER != '0'
and NEW_CUSTOMER_JOIN_QUARTER != '0'
GROUP BY 
--NEW_CUSTOMER_JOIN_QUARTER,
customer_first_contact,
VESPA_CONSENT_DATE_QUARTER
ORDER BY 1,2

select * from VESPA_ACCOUNTS_CHECK
where NEW_CUSTOMER_JOIN_QUARTER = 'OCT_DEC_2012'
AND VESPA_CONSENT_DATE_QUARTER = 'JAN_MAR_2012'

NEW_CUSTOMER_JOIN_QUARTER,VESPA_CONSENT_DATE_QUARTER,OPT_IN,OPT_OUT
'JAN_MAR_2013','HISTORIC',2,0

SELECT * FROM SK_PROD.CUST_SUBS_HIST
WHERE ACCOUNT_NUMBER = '621695460983'
--AND UPPER(SUBSCRIPTION_SUB_TYPE) = 'DTV PRIMARY VIEWING'

max CUST_SUBS_HIST.effective_from_dt
where CUST_SUBS_HIST.subscription_sub_type = 'DTV PRIMARY VIEWING'
and CUST_SUBS_HIST.prev_status_code = '?'
and CUST_SUBS_HIST.status_code_changed = 'Y'


select count(1) from data_quality_dp_data_to_analyze
