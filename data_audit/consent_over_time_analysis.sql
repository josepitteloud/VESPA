MIDAS status for viewing

report on the most recent status and the one before

SAV

select top 100 account_number,CUST_VIEWING_DATA_CAPTURE_ALLOWED,
CUST_VIEWING_CAPTURE_ALLWD_START_DT,
CUST_PREV_VIEWING_CAPTURE_ALLOWED
from sk_prod.cust_single_account_view
where cust_active_dtv = 1

select min(CUST_VIEWING_CAPTURE_ALLWD_START_DT) from
sk_prod.cust_single_account_view
where CUST_VIEWING_CAPTURE_ALLWD_START_DT > '1900-01-01 00:00:00.000000'
and cust_active_dtv = 1

Total customers	9,449,652
Viewing Consent	8,381,487
No Viewing Consent	978,415
% Consent	88.7%

1026122
8508951
1081
612974

INSERT INTO vespa_OpDash_sky_base_listing (
    account_number
    ,rtm
    ,cust_viewing_data_capture_allowed
--    ,most_recent_DTV_booking - not used in a any report build
    ,DTV_customer
    ,is_new_customer
)

DECLARE @latest_full_date 		date

SELECT TOP 10 * FROM vespa_analysts.vespa_OpDash_new_joiners_RTMs

commit

SELECT TOP 10 * FROM

select * into acct_start_date
from
(SELECT b.ACCOUNT_NUMBER, MIN(a.STATUS_START_DT) FIRST_CONTACT
FROM SK_PROD.CUST_SUBS_HIST a,  vespa_OpDash_sky_base_listing b
where a.account_number = b.account_number
GROUP BY b.ACCOUNT_NUMBER) t

select distinct top 100 * from
(select account_number, status_start_dt, rank () over (partition by account_number order by status_start_dt) rank
from SK_PROD.CUST_SUBS_HIST) t
where rank = 1


DECLARE @latest_full_date 		date

-- No longer looking at reporting relative to which day the report is actioned on, instead
-- running it on a fixed Sunday -> Saturday data set for the week before. Well, whatever
-- the standardised end day of weekly reporting is as defined in our procedure.
execute vespa_analysts.Regulars_Get_report_end_date @latest_full_date output

drop table vespa_OpDash_sky_base_listing

SELECT sav.account_number
      ,rtm.rtm_detail
      ,dateadd(day, 7, max(sav.booking_dt)) data_chk
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
SAV.CUST_PREV_VIEWING_CAPTURE_ALLOWED
commit

select top 100 * from vespa_OpDash_sky_base_listing
where UPPER(rtm_detail) not like '%RTM%'
AND CUST_VIEWING_DATA_CAPTURE_ALLOWED != '?'


select top 100 * from vespa_OpDash_sky_base_listing

SELECT TOP 100 ACCOUNT_NUMBER, date(CUST_VIEWING_CAPTURE_ALLWD_START_DT),
case when booking_dt < '2011-04-01' then 'HISTORIC'
when booking_dt BETWEEN  '2011-04-01' AND '2011-06-30' then 'APR_JUN_2011'
when booking_dt BETWEEN  '2011-07-01' AND '2011-09-30' then 'JUL_SEP_2011'
when booking_dt BETWEEN  '2011-10-01' AND '2011-12-31' then 'OCT_DEC_2011'
when booking_dt BETWEEN  '2012-01-01' AND '2012-03-31' then 'JAN_MAR_2012'
when booking_dt BETWEEN  '2012-04-01' AND '2012-06-30' then 'APR_JUN_2012'
when booking_dt BETWEEN  '2012-07-01' AND '2012-09-30' then 'JUL_SEP_2012'
when booking_dt BETWEEN  '2012-10-01' AND '2012-12-31' then 'OCT_DEC_2012'
when booking_dt BETWEEN  '2013-01-01' AND '2012-03-31' then 'JAN_MAR_2013'
when booking_dt BETWEEN  '2013-04-01' AND '2012-06-30' then 'APR_JUN_2013'
ELSE '0' END NEW_CUSTOMER_JOIN_QUARTER,
case when date(CUST_VIEWING_CAPTURE_ALLWD_START_DT) < '2011-04-01' then 'HISTORIC'
when date(CUST_VIEWING_CAPTURE_ALLWD_START_DT) BETWEEN  '2011-04-01' AND '2011-06-30' then 'APR_JUN_2011'
when date(CUST_VIEWING_CAPTURE_ALLWD_START_DT) BETWEEN  '2011-07-01' AND '2011-09-30' then 'JUL_SEP_2011'
when date(CUST_VIEWING_CAPTURE_ALLWD_START_DT) BETWEEN  '2011-10-01' AND '2011-12-31' then 'OCT_DEC_2011'
when date(CUST_VIEWING_CAPTURE_ALLWD_START_DT) BETWEEN  '2012-01-01' AND '2012-03-31' then 'JAN_MAR_2012'
when date(CUST_VIEWING_CAPTURE_ALLWD_START_DT) BETWEEN  '2012-04-01' AND '2012-06-30' then 'APR_JUN_2012'
when date(CUST_VIEWING_CAPTURE_ALLWD_START_DT) BETWEEN  '2012-07-01' AND '2012-09-30' then 'JUL_SEP_2012'
when date(CUST_VIEWING_CAPTURE_ALLWD_START_DT) BETWEEN  '2012-10-01' AND '2012-12-31' then 'OCT_DEC_2012'
when date(CUST_VIEWING_CAPTURE_ALLWD_START_DT) BETWEEN  '2013-01-01' AND '2012-03-31' then 'JAN_MAR_2013'
when date(CUST_VIEWING_CAPTURE_ALLWD_START_DT) BETWEEN  '2013-04-01' AND '2012-06-30' then 'APR_JUN_2013'
ELSE '0' END VESPA_CONSENT_DATE_QUARTER,
CUST_VIEWING_DATA_CAPTURE_ALLOWED,CUST_PREV_VIEWING_CAPTURE_ALLOWED
FROM vespa_OpDash_sky_base_listing
where UPPER(rtm_detail) not like '%RTM%'
AND CUST_VIEWING_DATA_CAPTURE_ALLOWED != '?'

SELECT TOP 100 * FROM vespa_OpDash_sky_base_listing
where UPPER(rtm_detail) not like '%RTM%'
AND CUST_VIEWING_DATA_CAPTURE_ALLOWED != '?'