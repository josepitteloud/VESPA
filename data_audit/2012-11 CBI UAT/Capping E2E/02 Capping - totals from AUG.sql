

begin


    if object_id('e2e_aggregate_collection_aug') is not null drop table e2e_aggregate_collection_aug
    create table e2e_aggregate_collection_aug (
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

    execute logger_create_run 'Capping_E2E', 'X', @CP2_build_ID output

    while @varPeriodStart <= @varLastDate
        begin

            execute logger_add_event @CP2_build_ID, 3, 'Starting ' || @varPeriodStart || ' - ' || @varPeriodEnd, 0

            insert into e2e_aggregate_collection_aug
              select
                    Timeshift,
                    max(@varPeriodStart),
                    sum(
                        datediff(
                            second
                            ,case
                                when (viewing_starts >= @varPeriodStart) and (viewing_starts <= @varPeriodEnd)   then viewing_starts
                                when (viewing_starts <  @varPeriodStart) and (viewing_stops  >= @varPeriodStart) then @varPeriodStart
                                  else null
                             end
                            ,case
                                when (viewing_stops >= @varPeriodStart) and (viewing_stops  <= @varPeriodEnd) then viewing_stops
                                when (viewing_stops >= @varPeriodEnd)   and (viewing_starts <= @varPeriodEnd) then @varPeriodEnd
                                  else null
                             end
                                )
                        ),
                    count(distinct case
                                     when (viewing_starts <= @varPeriodEnd) and (viewing_stops >= @varPeriodStart) then subscriber_id
                                       else null
                                   end
                         ),
                    sum(case
                          when (viewing_starts <= @varPeriodEnd) and (viewing_stops >= @varPeriodStart) then 1
                            else 0
                        end
                         )
                from e2e_aug_capped_data
               where capped_flag <> 3
                 and adjusted_event_start_time between '2012-12-03 00:00:00' and '2012-12-03 23:59:59'
               group by Timeshift
            commit

            set @varPeriodStart = dateadd(hour, 1, @varPeriodStart)
            set @varPeriodEnd = dateadd(hour, 1, @varPeriodEnd)

        end

        execute logger_get_latest_job_events 'Capping_E2E', 4

end















