create variable @RunID bigint;
exec logger_create_run 'Data_Quality_Checks', 'Latest Run', @RunID output;

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-01';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-02';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-03';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-04';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-05';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-06';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-07';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-08';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-09';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-10';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-11';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-12';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-13';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-14';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-15';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-16';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-17';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-18';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-19';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-20';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-21';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-22';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-23';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-24';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-25';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-26';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-27';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-28';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-29';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-30';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201403','2014-03-31';
--------------------------------------------------------------------------------------------

commit
