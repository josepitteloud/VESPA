create variable @RunID bigint;
exec logger_create_run 'Data_Quality_Checks', 'Latest Run', @RunID output;

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-01';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-02';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-03';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-04';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-05';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-06';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-07';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-08';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-09';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-10';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-11';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-12';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-13';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-14';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-15';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-16';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-17';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-18';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-19';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-20';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-21';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-22';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-23';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-24';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-25';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-26';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-27';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-28';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-29';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-30';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201301','2013-01-31';
--------------------------------------------------------------------------------------------

commit
