
begin

    declare @parRunIdentifier varchar(15)
    declare @parTargetDate date

    set @parTargetDate      = '2012-10-04'
    set @parRunIdentifier   = 'Run_05'

    ----------------------------------------------------------------------------------
      -- Capping
    declare @CP2_build_ID int
    declare @var_sql varchar(15000)

    execute logger_create_run 'Capping calibration', 'Calibration run', @CP2_build_ID output
    commit

    execute V079_run_capping @parTargetDate, @CP2_build_ID
    commit


    ----------------------------------------------------------------------------------
      -- Table rename
    set @var_sql = '
                    if object_id(''V079_Daily_Events_Capped_##^^*^*##_' || @parRunIdentifier || ''') is not null
                       drop table V079_Daily_Events_Capped_##^^*^*##_' || @parRunIdentifier || '

                    alter table V079_Daily_Events_Capped
                         rename V079_Daily_Events_Capped_##^^*^*##__' || @parRunIdentifier || '
                   '

    execute(replace(@var_sql,'##^^*^*##', dateformat(@parTargetDate, 'yyyymmdd')))
    commit


    ----------------------------------------------------------------------------------
      -- Table rename
    execute V079_result_calculation @parTargetDate, @parRunIdentifier


    ----------------------------------------------------------------------------------
    execute logger_get_latest_job_events 'Capping calibration', 4

end;

