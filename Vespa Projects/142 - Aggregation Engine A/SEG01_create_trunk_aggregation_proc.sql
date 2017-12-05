
/************************************************************************
 **  
 **  Assigns a different trunk_id to each metric in the build schedule
 **
 ************************************************************************/


CREATE or replace procedure SEG01_create_trunk_aggregation(
                    @_filter_id             bigint,
                    @_metric_schedule_id    bigint
                ) AS
BEGIN

    exec seg01_log 'SEG01_create_trunk_aggregation<'||now()||'>'

    DECLARE @_db_table_name                          varchar(64)
    DECLARE @_comma_seperated_list_of_tags_required  varchar(500) --should be set from a proc variable above
    DECLARE @rank                                    bigint
    DECLARE @max_rank                                bigint
    DECLARE @metric_id                               bigint
    DECLARE @metric_calculation                      varchar(200) -- metric
    DECLARE @trunk_id                                bigint
    DECLARE @group_by                                varchar(200)
    DECLARE @xsql                                    varchar(500)
    DECLARE @record_count                            bigint

    -- 1. Work out which columns we need for the data set - for now, just assume.
    --    ideally this would be set from the Meta-data that relates to the Trunk aggregation descriptor
    SET @_comma_seperated_list_of_tags_required = ' e.pk_viewing_prog_instance_fact, '||
                                                  ' e.dk_programme_instance_dim, '||
                                                  ' t.period_duration, '||
                                                  ' coalesce(aa.days_data_returned, 0) as days_data_returned, '||
                                                  ' coalesce(aa.ent_tv3d_sub,       0) as ent_tv3d_sub, '||
                                                  ' coalesce(aa.movmt_tv3d_sub,     0) as movmt_tv3d_sub, '||
                                                  ' cm.channel_type, '||
                                                  ' cm.format '

    exec SEG01_create_tmp_dataset_multiroot_tbl @_filter_id, @_comma_seperated_list_of_tags_required, @_db_table_name


   -- select @_db_table_name


    --log the build details for this aggregation
    INSERT into SEG01_tmp_event_table_log_tbl(table_name, filter_id, tags_required)
        values(@_db_table_name, @_filter_id, @_comma_seperated_list_of_tags_required)
    commit


    --build aggregation
    ---->   RUN FOR ALL METRICS


    --look up @_metric_schedule_id in the metric_schedule_table for the list of metrics to run on this data set
    select s.uniqid, s.metric_id, dense_rank() over(order by s.uniqid) rank, m.calculation, m.over_group
      into #metric_build_list_tmp
      from SEG01_metric_build_schedule_tbl s,  SEG01_metric_library_tbl m
     where s.metric_id = m.uniqid
       and s.metric_schedule_id = @_metric_schedule_id
  order by s.uniqid asc, s.metric_id asc


    --for each metric... in   #metric_build_list_tmp (for each rank)


    SET @rank = 1

    select @max_rank = max(rank)
      from #metric_build_list_tmp

    while(@rank<=@max_rank)--for each...
        BEGIN
            select @metric_calculation = calculation,
                   @metric_id = metric_id
              from #metric_build_list_tmp
             where rank = @rank

            exec SEG01_assign_trunk_aggregation_id  @trunk_id

            -- group by....  should insert trunk_id in here too..
            SET @group_by = 'account_number, '||@trunk_id||', filter_id, '||@metric_id||' '

            SET @xsql = '   select '||@group_by||', '||@metric_calculation||' '||
                        '     into #seg01_trunk_aggregate_tmp '||
                        '     from '||@_db_table_name||' '||
                        ' group by '||@group_by||' '

            execute(@xsql)

            select @record_count = count(1)
              from #seg01_trunk_aggregate_tmp

            insert into SEG01_trunk_aggregation_results_tbl
               select *
                 from #seg01_trunk_aggregate_tmp

            commit

            exec seg01_log '    inserted<'||@record_count||'> records into trunk_aggregation_results_tbl'

            SET @rank = @rank + 1
        END

--- is there an audit of the construction of this yet????
--  select top 1000 * from SEG01_trunk_aggregation_desc_tbl


END;


--------- END PROC ----------



--- testing -->


select top 1000 *
from SEG01_trunk_aggregation_results_tbl

select top 1000 *
from SEG01_metric_library_tbl

select top 10 *
  from seg01_trunk_filter_defn_tbl

select top 1000 *
from SEG01_root_aggregation_built_tbl

select *
from SEG01_tmp_event_table_log_tbl

select top 1000 *
from SEG01_combined_events_20131114164207_39tmp

select *
from SEG01_log_tbl

-- end testing

