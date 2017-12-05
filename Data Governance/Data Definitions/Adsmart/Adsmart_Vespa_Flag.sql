/*###############################################################################
# Created on:   10/01/2014
# Created by:   Tony Kinnaird (TKD)
# Description:  Rule to derive whether an account being used for Analysis should be
#		regarded as being on VESPA for the purposes for this analysis
#		Code will need to be amended to VIQ Scaling tables instead of current
#		scaling tables
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 10/01/2014  TKD   v01 - initial version
#
###############################################################################*/



------------
-- SET UP --
------------
CREATE VARIABLE @var_period_start       datetime;
CREATE VARIABLE @var_period_end         datetime;
CREATE VARIABLE @scanning_day           datetime;

-- THINGS YOU NEED TO CHANGE SET DATE TO CAMPAIGN PERIOD--
SET @var_period_start  = '2013-08-29';
SET @var_period_end    = '2013-10-07';
--END OF THINGS YOU NEED TO CHANGE --


CREATE HG INDEX HG_vespa_ind ON #vespa_panel_accts(cb_key_household);

SET @scanning_day = @var_period_start;

WHILE @scanning_day <= DATEADD(dd,0,@var_period_end)
BEGIN
        INSERT INTO #vespa_panel_accts (
                account_number,
                scaling_weighting,
                scaling_date)
        SELECT  l.account_number,
                s.weighting,
                s.scaling_day
        FROM    vespa_analysts.SC2_intervals AS l
        JOIN    vespa_analysts.SC2_weightings AS s
        ON      l.scaling_segment_ID = s.scaling_segment_ID
                AND @scanning_day BETWEEN l.reporting_starts AND l.reporting_ends
                AND s.scaling_day = @scanning_day

    SET @scanning_day = DATEADD(DAY, 1, @scanning_day); END

-- Update the household field.
SELECT  account_number,cb_key_household
INTO    #acct_hh1
FROM    sk_prod.cust_single_account_view
WHERE   account_number IN (SELECT DISTINCT account_number FROM #vespa_panel_accts)
;

UPDATE  #vespa_panel_accts
SET     cb_key_household=b.cb_key_household
FROM    #vespa_panel_accts a
LEFT JOIN    #acct_hh1 b
ON      a.account_number=b.account_number;


--Summarise at HH level and add VESPA_flag to the VESPA panel table.
SELECT  a.cb_key_household,
        CASE WHEN mx > 0 THEN 1 ELSE 0 END as vespa_flag
INTO    #vespa_panel_hh
FROM(   SELECT  cb_key_household,
                MAX(scaling_weighting) AS mx
        FROM    #vespa_panel_accts
        GROUP BY cb_key_household) a;

