create variable @RunID bigint;
exec logger_create_run 'Data_Quality_Checks', 'Latest Run', @RunID output;

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-01';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-02';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-03';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-04';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-05';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-06';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-07';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-08';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-09';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-10';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-11';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-12';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-13';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-14';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-15';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-16';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-17';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-18';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-19';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-20';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-21';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-22';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-23';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-24';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-25';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-26';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-27';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-28';
--------------------------------------------------------------------------------------------

exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-29';
exec kinnairt.data_quality_data_processing_month 'null',@RunID,'201406','2014-06-30';
--------------------------------------------------------------------------------------------

commit
