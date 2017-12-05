create variable @RunID bigint;

go

--execute procedure to start process
execute sk_vespa_dq.logger_create_run 'Data_Quality_Automated_Checks', 'Latest Run', @RunID output


begin
execute sk_vespa_dq.data_quality_automated_daily_run @RunID

--commit;
end