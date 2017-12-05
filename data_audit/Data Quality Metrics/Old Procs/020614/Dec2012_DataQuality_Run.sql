create variable @RunID bigint;
exec logger_create_run 'Data_Quality_Checks', 'Latest Run', @RunID output;

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-01';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-02';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-03';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-04';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-05';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-06';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-07';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-08';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-09';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-10';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-11';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-12';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-13';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-14';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-15';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-16';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-17';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-18';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-19';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-20';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-21';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-22';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-23';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-24';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-25';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-26';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-27';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-28';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-29';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-30';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201212','2012-12-31';
--------------------------------------------------------------------------------------------

commit
