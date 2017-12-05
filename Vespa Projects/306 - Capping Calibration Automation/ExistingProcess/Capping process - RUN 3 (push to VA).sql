

  -- ########################################################################
  -- #### Create AUG tables                                              ####
  -- ########################################################################
begin

    declare @varBuildId   int
    declare @varStartDate date
    declare @varEndDate   date
    declare @varSql       varchar(15000)

    set @varStartDate = '2013-07-26'      -- A Friday
    set @varEndDate   = '2013-08-01'      -- The following Thursday

    --execute logger_create_run 'Create VA AUG obj', 'Weekly run', @varBuildId output
    commit

    while @varStartDate <= @varEndDate
        begin

              -- ##### Create table in VA #####
            set @varSql = '
                           execute dba.sp_drop_table ''vespa_analysts'', ''Vespa_Daily_Augs_##^1^##''

                           execute dba.sp_create_table
                                               ''vespa_analysts'',
                                               ''Vespa_Daily_Augs_##^1^##'',
                                               ''
                                                 Cb_Row_Id                   bigint              primary key,
                                                 Account_Number              varchar(20)         not null,
                                                 Subscriber_Id               bigint              not null,
                                                 Programme_Trans_Sk          bigint,
                                                 Timeshifting                varchar(10),
                                                 Viewing_Starts              datetime,
                                                 Viewing_Stops               datetime,
                                                 Viewing_Duration            bigint,
                                                 Capped_Flag                 tinyint,
                                                 Capped_Event_End_Time       datetime,
                                                 Scaling_Segment_Id          bigint,
                                                 Scaling_Weighting           float,
                                                 BARB_Minute_Start           datetime,
                                                 BARB_Minute_End             datetime,
                                                 Event_Id                    bigint              default null,
                                                 Broadcast_Viewing_Starts    datetime            default null,
                                                 Broadcast_Viewing_Stops     datetime            default null,
                                                 Event_Start_Time            datetime            default null,
                                                 Uncapped_Event_End_Time     datetime            default null,
                                                 Time_Since_Recording        int                 default 0,
                                                 Live_Flag                   bit                 default 0,
                                                 Match_Quality               tinyint             default 9
                                               ''

                           create hg   index  idx1 on vespa_analysts.Vespa_Daily_Augs_##^1^## (Subscriber_Id)
                           create hg   index  idx2 on vespa_analysts.Vespa_Daily_Augs_##^1^## (Account_Number)
                           create hg   index  idx3 on vespa_analysts.Vespa_Daily_Augs_##^1^## (Programme_Trans_Sk)
                           create dttm index  idx4 on vespa_analysts.Vespa_Daily_Augs_##^1^## (Viewing_Starts)
                           create dttm index  idx5 on vespa_analysts.Vespa_Daily_Augs_##^1^## (Viewing_Stops)
                           create hg   index  idx6 on vespa_analysts.Vespa_Daily_Augs_##^1^## (Event_Id)
                           create dttm index  idx7 on vespa_analysts.Vespa_Daily_Augs_##^1^## (Broadcast_Viewing_Starts)
                           create dttm index  idx8 on vespa_analysts.Vespa_Daily_Augs_##^1^## (Broadcast_Viewing_Stops)
                           create dttm index  idx9 on vespa_analysts.Vespa_Daily_Augs_##^1^## (Event_Start_Time)
                           create dttm index idx10 on vespa_analysts.Vespa_Daily_Augs_##^1^## (Uncapped_Event_End_Time)
                           create lf   index idx11 on vespa_analysts.Vespa_Daily_Augs_##^1^## (Match_Quality)

                          '

            execute(replace(@varSql,'##^1^##', dateformat(@varStartDate, 'yyyymmdd')))
            commit

            --execute logger_add_event @varBuildId, 3, 'Table created for ' || convert(varchar(10), @varStartDate, 123)


              -- ##### Move data #####
            set @varSql = '
                           truncate table vespa_analysts.Vespa_Daily_Augs_##^1^##

                           insert into vespa_analysts.Vespa_Daily_Augs_##^1^##
                             select *
                               from Vespa_Daily_Augs_##^1^##
                           commit
                          '

            execute(replace(@varSql,'##^1^##', dateformat(@varStartDate, 'yyyymmdd')))
            commit

            --execute logger_add_event @varBuildId, 3, 'Data moved for ' || convert(varchar(10), @varStartDate, 123), @@rowcount




            set @varStartDate = @varStartDate + 1
        end

   -- execute logger_get_latest_job_events 'Create VA AUG obj', 4

end;


  -- #### Append Scaling (manual) ####
begin

    declare @varStartDate date
    declare @varEndDate   date
    declare @varSQL       varchar(15000)

    set @varStartDate = '2013-07-26'      -- A Friday
    set @varEndDate   = '2013-08-01'      -- The following Thursday

    while @varStartDate <= @varEndDate
        begin
            set @varSql = '
                          update Vespa_Analysts.Vespa_Daily_Augs_##^1^## base
                             set base.Scaling_Segment_Id    = null,
                                 base.Scaling_Weighting     = null
                          commit

                          update Vespa_Analysts.Vespa_Daily_Augs_##^1^## base
                             set base.Scaling_Segment_Id    = det.Scaling_Segment_Id,
                                 base.Scaling_Weighting     = det.Weighting
                            from (select
                                        a.Account_Number,
                                        b.Scaling_Segment_Id,
                                        b.Weighting
                                    from vespa_analysts.SC2_Intervals a,
                                         vespa_analysts.SC2_Weightings b
                                   where a.Scaling_Segment_Id = b.Scaling_Segment_Id
                                     and a.Reporting_Starts <= ''##^2^##''
                                     and a.Reporting_Ends >= ''##^2^##''
                                     and b.Scaling_Day = ''##^2^##'') det
                           where base.Account_Number = det.Account_Number
                          commit
                          '
            execute( replace(
                              replace(
                                      @varSql,
                                      '##^2^##',
                                      dateformat(@varStartDate, 'yyyy-mm-dd')
                                     ),
                              '##^1^##',
                              dateformat(@varStartDate, 'yyyymmdd')
                            )
                    )
            commit

            set @varStartDate = @varStartDate + 1
        end

end;





















