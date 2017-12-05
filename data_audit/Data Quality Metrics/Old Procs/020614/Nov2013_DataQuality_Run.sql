create variable @RunID bigint;
exec logger_create_run 'Data_Quality_Checks', 'Latest Run', @RunID output;

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-01';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-02';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-03';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-04';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-05';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-06';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-07';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-08';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-09';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-10';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-11';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-12';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-13';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-14';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-15';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-16';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-17';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-18';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-19';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-20';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-21';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-22';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-23';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-24';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-25';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-26';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-27';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-28';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-29';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201311','2013-11-30';
--------------------------------------------------------------------------------------------

commit
