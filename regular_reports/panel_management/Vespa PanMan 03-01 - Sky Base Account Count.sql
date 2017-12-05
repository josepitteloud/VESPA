-- Project Vespa: Panel Management Report - For the summary tab, we need the raw number of account in the sky base
declare @lastthursday 		date
DECLARE @latest_full_date 	date

execute vespa_analysts.Regulars_Get_report_end_date @latest_full_date output

set @lastthursday = @latest_full_date - 2

select	count(distinct sav.account_number)
from	/*sk_prod.*/cust_single_account_view as sav
where	sav.cust_active_dtv = 1
and		PROD_DTV_ACTIVATION_DT <= @lastthursday
and		sav.pty_country_code = 'GBR'
