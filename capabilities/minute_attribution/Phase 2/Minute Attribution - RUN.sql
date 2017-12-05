

  -- ########################################################################
  -- #### Minute Attribution                                             ####
  -- ########################################################################
begin

    declare @varBuildId   int
    declare @varStartDate date
    declare @varEndDate   date
    declare @varSql       varchar(15000)

    set @varStartDate = '2013-05-15'
    set @varEndDate   = '2013-05-21'

    execute logger_create_run 'MA 5.0', 'Weekly MA run', @varBuildId output
    commit

    while @varStartDate <= @varEndDate
        begin

              -- ##### Get individual events from AUG table #####
            execute aug_MADataPreparation_v01 @varStartDate, 1, null, @varBuildId
            commit


              -- ##### Add BARB minute fields #####
            set @varSql = '
                           alter table Vespa_MADataPrep_tmp_Aug_' || dateformat(@varStartDate, 'yyyymmdd') || '
                             add (BARB_Minute_Start    datetime  default null,
                                  BARB_Minute_End      datetime  default null)
                          '
            execute(@varSql)
            commit


              -- ##### Run Minute Attribution #####
            set @varSql = '
                           execute Minute_Attribution_v05 ''Vespa_MADataPrep_tmp_Aug_' || dateformat(@varStartDate, 'yyyymmdd') || ''', now(), 1, null, ' || @varBuildId || '
                          '
            execute(@varSql)
            commit


              -- ##### Apend results back to AUG table #####
            set @varSql = '
                           update Vespa_Daily_Augs_' || dateformat(@varStartDate, 'yyyymmdd') || ' base
                              set base.BARB_Minute_Start  = null,
                                  base.BARB_Minute_End    = null
                           commit

                           update Vespa_Daily_Augs_' || dateformat(@varStartDate, 'yyyymmdd') || ' base
                              set base.BARB_Minute_Start  = det.BARB_Minute_Start,
                                  base.BARB_Minute_End    = det.BARB_Minute_End
                             from Vespa_MADataPrep_tmp_Aug_' || dateformat(@varStartDate, 'yyyymmdd') || ' det
                            where base.Cb_Row_Id = det.Instance_Id
                              and det.Aug_Date = ''' || @varStartDate || '''
                           commit
                          '
            execute(@varSql)
            commit

            execute aug_MADataPreparation_Cleanup_v01 @varStartDate
            execute Minute_Attribution_Cleanup_v01

            set @varStartDate = @varStartDate + 1
        end

    execute logger_get_latest_job_events 'MA 5.0', 4

end;












