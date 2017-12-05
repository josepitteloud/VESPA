create variable @RunID bigint;

go

--execute procedure to start process
execute kinnairt.logger_create_run 'Data_Quality_Automated_Checks', 'Latest Run', @RunID output


begin
execute kinnairt.data_quality_automated_daily_run @RunID

--commit;
end