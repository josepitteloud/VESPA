create variable @RunID bigint;
exec logger_create_run 'Data_Quality_Checks', 'Latest Run', @RunID output;

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-01';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-02';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-03';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-04';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-05';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-06';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-07';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-08';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-09';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-10';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-11';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-12';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-13';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-14';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-15';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-16';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-17';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-18';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-19';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-20';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-21';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-22';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-23';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-24';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-25';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-26';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-27';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201302','2013-02-28';
--------------------------------------------------------------------------------------------

commit
