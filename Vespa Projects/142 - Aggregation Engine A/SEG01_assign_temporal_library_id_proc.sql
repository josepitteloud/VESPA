

/*********************************************************************
 **  Assigns a temporal_library ID. Basically just gets the next integer
 **  available, and records the assigned date against it, and leaves
 **  a date column field free for later when the segmentation is
 **  actually created.
 **
 *********************************************************************/

CREATE or replace procedure SEG01_assign_temporal_library_id(
                     in @_definition_id              BIGINT,
                     in @_recurrence_start_datetime  DATETIME,
                    out @_temporalid                BIGINT
                ) AS
BEGIN


SELECT @_temporalid = temporal_id
  FROM SEG01_temporal_library_tbl
 WHERE definition_id = @_definition_id
   AND recurrence_start_datetime = @_recurrence_start_datetime

IF @_temporalid IS NULL
   BEGIN
      DECLARE @nowtime datetime

      SET @nowtime = now()

      --INSERT new blank row and return the id from the identiy column
      INSERT into SEG01_temporalid_lookup_tbl(allocated_date)
        VALUES (@nowtime)
      commit

      SELECT @_temporalid = uniqid
        from SEG01_temporalid_lookup_tbl
       where allocated_date = @nowtime
    END

END;


---
/*
select *
from SEG01_temporalid_lookup_tbl
*/
