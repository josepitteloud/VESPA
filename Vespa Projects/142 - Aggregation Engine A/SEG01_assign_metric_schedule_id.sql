
/*********************************************************************
 **  Assigns a metric_schedule ID. Basically just gets the next integer
 **  available.
 **
 *********************************************************************/

CREATE or replace procedure SEG01_assign_metric_schedule_id(
                    out @_metric_schedule_id    BIGINT
                ) AS
BEGIN


SELECT @_metric_schedule_id = coalesce(max( metric_schedule_id)+1,1)
  FROM SEG01_metric_build_schedule_tbl


END;


