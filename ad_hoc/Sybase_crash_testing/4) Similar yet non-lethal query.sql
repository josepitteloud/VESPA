--------------------------------------------------------------- A03 - Declare variables
-- A03 - Declare variables


create variable @weekly_reference_date DATE;

-- Dynamic live date reference:
--SELECT @weekly_reference_date = dateadd(day,1,MAX(scaling_date)) FROM scaling_weights

-- For development, one fixed date where stuff happens:
set @weekly_reference_date = '2012-06-27';

-- Identifies the latest date in the scaling_weights table and adds a day to
-- give the start of the next scaling week.


--------------------------------------------------------------- A04 - Get weekly sample
-- A04 - Get weekly sample

-- Captures all active accounts in cust_subs_hist
SELECT   account_number
        ,cb_key_household
        ,current_short_description
        ,rank() over (PARTITION BY account_number ORDER BY effective_from_dt desc, cb_row_id) AS rank
        ,convert(bit, 0)  AS uk_standard_account
        ,convert(VARCHAR(20), NULL) AS isba_tv_region
  INTO #weekly_sample
  FROM sk_prod.cust_subs_hist
 WHERE subscription_sub_type IN ('DTV Primary Viewing')
   AND status_code IN ('AC','AB','PC')
   AND effective_from_dt <= @weekly_reference_date
   AND effective_to_dt > @weekly_reference_date
   AND effective_from_dt<>effective_to_dt
   AND EFFECTIVE_FROM_DT IS NOT NULL
   AND cb_key_household > 0
   AND cb_key_household IS NOT NULL
   AND account_number IS NOT NULL
   AND service_instance_id IS NOT NULL;

-- De-dupes accounts
COMMIT;
DELETE FROM #weekly_sample WHERE rank > 1;
COMMIT;

-- Create indices
CREATE UNIQUE INDEX fake_pk ON #weekly_sample (account_number);
CREATE INDEX for_ilu_joining ON #weekly_sample (cb_key_household);
CREATE INDEX for_package_join ON #weekly_sample (current_short_description);
COMMIT;

-- This guy is comparable to the lethat one; a multi-column update based on matching account number.
-- Not quite strictly like for like, the arrangement of physical tables, temporary tables and views
-- is slightly different to the fatal query, but that should make a difference as long as everything
-- has keys?

-- Take out ROIs (Republic of Ireland) and non-standard accounts as these are not currently in the scope of Vespa
UPDATE #weekly_sample;
SET
    uk_standard_account = CASE
        WHEN b.acct_type='Standard' AND b.account_number <>'?' AND b.pty_country_code ='GBR' THEN 1
        ELSE 0 END
    ,isba_tv_region = b.isba_tv_region
FROM #weekly_sample AS a
inner join sk_prod.cust_single_account_view AS b
ON a.account_number = b.account_number;

COMMIT;
