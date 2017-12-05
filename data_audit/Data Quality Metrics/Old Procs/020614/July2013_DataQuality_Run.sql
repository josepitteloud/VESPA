create variable @RunID bigint;
exec logger_create_run 'Data_Quality_Checks', 'Latest Run', @RunID output;

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-01';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-02';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-03';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-04';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-05';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-06';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-07';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-08';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-09';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-10';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-11';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-12';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-13';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-14';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-15';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-16';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-17';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-18';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-19';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-20';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-21';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-22';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-23';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-24';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-25';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-26';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-27';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-28';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-29';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-30';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201307','2013-07-31';
--------------------------------------------------------------------------------------------

commit
