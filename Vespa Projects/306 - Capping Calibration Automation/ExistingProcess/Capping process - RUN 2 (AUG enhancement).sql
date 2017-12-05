
drop view if exists Vespa_AugEnhancement_tmp_Source_Data;
create view Vespa_AugEnhancement_tmp_Source_Data as
  select * from sk_prod.VESPA_DP_PROG_VIEWED_201307
   where panel_id = 12
     and type_of_viewing_event <> 'Non viewing event'
     and type_of_viewing_event is not null
   union all
  select * from sk_prod.VESPA_DP_PROG_VIEWED_201308
   where panel_id = 12
     and type_of_viewing_event <> 'Non viewing event'
     and type_of_viewing_event is not null;
commit;


begin

    declare @varBuildId int
    declare @varStartDate date
    declare @varEndDate   date

    set @varStartDate = '2013-07-26'      -- A Friday
    set @varEndDate   = '2013-08-01'      -- The following Thursday

    execute logger_create_run 'Aug Enhancement', 'Weekly run', @varBuildId output
    commit

    execute aug_AugEnhancement_GetSourceData_v01 @varStartDate, @varEndDate, 1, null, @varBuildId

    while @varStartDate <= @varEndDate
        begin

            execute aug_AugEnhancement_S1_v01 dateformat(@varStartDate, 'yyyymmdd'), 1, null, @varBuildId
            commit

            set @varStartDate = @varStartDate + 1
        end

    execute aug_AugEnhancement_Cleanup_v01

    execute logger_get_latest_job_events 'Aug Enhancement', 4

end;









