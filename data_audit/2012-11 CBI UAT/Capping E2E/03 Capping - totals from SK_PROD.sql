

begin


    if object_id('e2e_aggregate_collection_skp') is not null drop table e2e_aggregate_collection_skp
    create table e2e_aggregate_collection_skp (
         Timeshift              varchar(20)
        ,Date_Hour              datetime
        ,total_seconds          bigint
        ,boxes                  bigint
        ,Instances              bigint
    )

    declare @CP2_build_ID   int
    declare @varPeriodStart datetime
    declare @varPeriodEnd   datetime
    declare @varLastDate    datetime

    set @varPeriodStart   = '2012-12-03 00:00:00'
    set @varPeriodEnd     = '2012-12-03 01:00:00'
    set @varLastDate      = '2012-12-04 06:00:00'

    execute logger_create_run 'Capping_E2E_SK', 'X', @CP2_build_ID output

    while @varPeriodStart <= @varLastDate
        begin

            execute logger_add_event @CP2_build_ID, 3, 'Starting ' || @varPeriodStart || ' - ' || @varPeriodEnd, 0

            insert into e2e_aggregate_collection_skp
              select
                    Timeshift,
                    max(@varPeriodStart),
                    sum(
                        datediff(
                            second
                            ,case
                                when (INSTANCE_START_DATE_TIME_UTC >= @varPeriodStart) and (INSTANCE_START_DATE_TIME_UTC <= @varPeriodEnd) then INSTANCE_START_DATE_TIME_UTC
                                when (INSTANCE_START_DATE_TIME_UTC <  @varPeriodStart) and (Instance_End_Time_Capped  >= @varPeriodStart) then @varPeriodStart
                                  else null
                             end
                            ,case
                                when (Instance_End_Time_Capped >= @varPeriodStart) and (Instance_End_Time_Capped  <= @varPeriodEnd) then Instance_End_Time_Capped
                                when (Instance_End_Time_Capped >= @varPeriodEnd)   and (INSTANCE_START_DATE_TIME_UTC <= @varPeriodEnd) then @varPeriodEnd
                                  else null
                             end
                                )
                        ),
                    count(distinct case
                                     when (INSTANCE_START_DATE_TIME_UTC <= @varPeriodEnd) and (Instance_End_Time_Capped >= @varPeriodStart) then subscriber_id
                                       else null
                                   end
                         ),
                    sum(case
                          when (INSTANCE_START_DATE_TIME_UTC <= @varPeriodEnd) and (Instance_End_Time_Capped >= @varPeriodStart) then 1
                            else 0
                        end
                         )
                from e2e_test_raw_viewing3
               where capped_full_flag = 0
                 and Filter = 1
                 and DK_EVENT_START_DATEHOUR_DIM between 2012120300 and 2012120323
                 and Timeshift <> '???'
               group by Timeshift
            commit

            set @varPeriodStart = dateadd(hour, 1, @varPeriodStart)
            set @varPeriodEnd = dateadd(hour, 1, @varPeriodEnd)

        end

        execute logger_get_latest_job_events 'Capping_E2E_SK', 4

end















