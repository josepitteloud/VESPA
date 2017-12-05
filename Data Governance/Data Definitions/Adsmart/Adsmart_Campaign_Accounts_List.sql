/*###############################################################################
# Created on:   10/01/2014
# Created by:   Tony Kinnaird (TKD)
# Description:  Rule to create list of households to be utilised within Adsmart analysis
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 10/01/2014  TKD   v01 - initial version
#
###############################################################################*/



--Last date of Campaign - 

declare @adsmart_date date

set @adsmart_date = '2014-01-01'

--Identify the date of latest update for each account and cb_key_household within the AdSmart history table--
select
account_number,
cb_data_date
into #TEMP_HIST_1
from sk_prod.adsmart_history
group by
account_number,
cb_data_date;


-- Create Account Level Base—

Select
distinct account_number,
max(cb_data_date) as cb_data_date_1
into #TEMP_BASE_ACCOUNT
from #TEMP_HIST_1
where cb_data_date <= @adsmart_date
group by
account_number;


--derive household and account details as close to the campaign date as possible

Select
distinct b.account_number,
b.cb_key_household,
b.cb_data_date
into LW_TEMP_BASE_CB_KEY
from #TEMP_BASE_ACCOUNT a
LEFT JOIN sk_prod.adsmart_history b
ON a.account_number=b.account_number and a.cb_data_date_1=cb_data_date
JOIN sk_prod.CUST_SUBS_HIST subs1  ---- active on reporting date
ON b.account_number = subs1.account_number
AND @adsmart_date BETWEEN subs1.EFFECTIVE_FROM_DT AND subs1.EFFECTIVE_TO_DT
AND status_code in ('AB','AC','PC') AND subscription_sub_type ='DTV Primary Viewing'
AND subs1.EFFECTIVE_FROM_DT <> subs1.EFFECTIVE_TO_DT
group by
b.account_number,b.cb_key_household, b.cb_data_date;


--