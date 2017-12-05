
--execute V079_Profile_Boxes '2012-05-16', '2012-05-16';
--execute citeam.logger_get_latest_job_events 'V079 BP', 4;


execute V079_run_capping '2012-05-16', 'Test_Run';
execute citeam.logger_get_latest_job_events 'V079 CAP', 4;

execute V079_result_calculation '2012-05-16', 'Test_Run';


