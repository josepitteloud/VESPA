


--test2

create variable @RunID bigint;
exec sk_vespa_dq.logger_create_run 'Data_Quality_Household_Reporting', 'Latest Run', @RunID output;

begin

--household demo report metrics
exec sk_vespa_dq.data_quality_household_metrics @RunID


--adsmart report metrics

exec sk_vespa_dq.data_quality_adsmart_regression_checks 'LOCAL','2014-03-01','2014-03-07'

--linear slot report metrics

exec sk_vespa_dq.data_quality_linear_slot_regression_checks 'LOCAL','2014-03-01','2014-03-07'

--programme report metrics

exec sk_vespa_dq.data_quality_programme_regression_checks '2014-03-01','2014-03-07'


end

commit


