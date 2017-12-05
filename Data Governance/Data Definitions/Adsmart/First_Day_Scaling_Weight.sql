/*###############################################################################
# Created on:   10/01/2014
# Created by:   Tony Kinnaird (TKD)
# Description:  Rule to derive accounts and scaling weight to be applied to Adsmart
#		analysis
#		Code will need to be amended to VIQ Scaling tables instead of current
#		scaling tables
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 10/01/2014  TKD   v01 - initial version
#
###############################################################################*/




--will need to be amended once the Adsmart Changes for Scaling are in place.

declare @scaling_date date

set @Scaling_date = '2013-08-29'


SELECT  iv.account_number,
        CAST (@Scaling_date AS DATE) AS scaling_date,
        w.weighting *   9680000/tw.total_weight,
        w.weighting *  9680000/ tw.total_weight * 0.864,
        iv.reporting_starts,
        iv.reporting_ends,
        iv.scaling_segment_id
FROM    vespa_analysts.SC2_intervals iv
        join vespa_analysts.SC2_weightings w
                ON iv.scaling_segment_id = w.scaling_segment_id
                and w.scaling_day = @Scaling_date
                and @Scaling_date BETWEEN iv.reporting_starts AND iv.reporting_ends
        join (select sum(weighting) as total_weight
                FROM vespa_analysts.SC2_intervals iv
                        join vespa_analysts.SC2_weightings w
                        ON iv.scaling_segment_id = w.scaling_segment_id
                        and w.scaling_day = @Scaling_date
                         and @Scaling_date BETWEEN iv.reporting_starts AND iv.reporting_ends
                ) tw
        ON 1=1

-- Get a lookup between accounts and cb_key_household.
SELECT  account_number,cb_key_household
INTO    #acct_hh
FROM    sk_prod.cust_single_account_view
WHERE   account_number IN (SELECT DISTINCT account_number FROM #firstdday_scaling)
;

UPDATE  #firstdday_scaling
SET     cb_key_household=b.cb_key_household
FROM    #firstdday_scaling a
LEFT OUTER JOIN #acct_hh b
ON      a.account_number=b.account_number
;

-- created a sum of the weights by HH.
SELECT  cb_key_household,
        SUM(adsmart_weightings) AS firstday_scaling_weight
INTO    #firstdday_scaling_hh
FROM    #firstdday_scaling
GROUP BY cb_key_household
