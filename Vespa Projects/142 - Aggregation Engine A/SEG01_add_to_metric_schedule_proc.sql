


CREATE or replace procedure SEG01_add_to_metric_schedule(
                    @metric_id      BIGINT,
                    @schedule_id    BIGINT
                ) AS
BEGIN

INSERT into SEG01_metric_build_schedule_tbl(metric_schedule_id, metric_id)
    values(@schedule_id, @metric_id)
commit

END;


--test/build a sample
/*
select *
from SEG01_metric_library_tbl

declare @metric_schedule_id bigint

exec SEG01_assign_metric_schedule_id @metric_schedule_id

--add one schdule entry
exec SEG01_add_to_metric_schedule 1, @metric_schedule_id
exec SEG01_add_to_metric_schedule 4, @metric_schedule_id

select *
from SEG01_metric_build_schedule_tbl

*/
