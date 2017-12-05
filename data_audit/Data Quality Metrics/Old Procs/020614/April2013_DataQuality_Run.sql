create variable @RunID bigint;
exec logger_create_run 'Data_Quality_Checks', 'Latest Run', @RunID output;

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-01';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-02';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-03';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-04';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-05';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-06';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-07';
--------------------------------------------------------------------------------------------
commit;
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-08';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-09';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-10';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-11';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-12';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-13';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-14';
--------------------------------------------------------------------------------------------

commit;
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-15';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-16';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-17';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-18';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-19';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-20';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-21';
--------------------------------------------------------------------------------------------

commit;
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-22';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-23';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-24';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-25';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-26';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-27';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-28';
--------------------------------------------------------------------------------------------

commit;
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-29';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201304','2013-04-30';
--------------------------------------------------------------------------------------------

commit;
