create variable @RunID bigint;
exec logger_create_run 'Data_Quality_Checks', 'Latest Run', @RunID output;

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-01';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-02';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-03';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-04';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-05';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-06';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-07';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-08';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-09';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-10';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-11';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-12';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-13';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-14';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-15';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-16';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-17';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-18';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-19';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-20';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-21';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-22';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-23';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-24';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-25';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-26';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-27';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-28';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-29';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-30';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201405','2014-05-31';
--------------------------------------------------------------------------------------------

commit
