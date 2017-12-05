create variable @RunID bigint;
exec logger_create_run 'Data_Quality_Checks', 'Latest Run', @RunID output;

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-01';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-02';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-03';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-04';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-05';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-06';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-07';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-08';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-09';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-10';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-11';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-12';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-13';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-14';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-15';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-16';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-17';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-18';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-19';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-20';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-21';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-22';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-23';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-24';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-25';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-26';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-27';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-28';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-29';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-30';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201308','2013-08-31';
--------------------------------------------------------------------------------------------

commit
