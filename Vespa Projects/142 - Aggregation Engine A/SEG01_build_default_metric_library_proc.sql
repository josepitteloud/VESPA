
CREATE or replace procedure SEG01_build_default_metric_library_proc(
                ) AS
BEGIN
--                                    @_metric_name,             @_calculation,                                 @_over_group
execute SEG01_add_to_metric_library  'Total Account Duration',  'sum(period_duration)',                        'account_number'
execute SEG01_add_to_metric_library  'Total Duration',          'sum(period_duration) over()',                 ''
execute SEG01_add_to_metric_library  'Account Session Count',   'count(pk_viewing_prog_instance_fact)',        'account_number'
execute SEG01_add_to_metric_library  'Distinct Programmes',     'count(distinct dk_programme_instance_dim)',   'account_number'

END;
