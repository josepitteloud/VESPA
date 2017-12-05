

/*********************************************************************
 **  Assigns a filter ID. Basically just gets the next integer
 **  available, and records the assigned date against it, and leaves
 **  a date column field free for later when the segmentation is
 **  actually created.
 **
 *********************************************************************/

CREATE or replace procedure SEG01_assign_filterid(
                out @_filterid BIGINT
                ) AS
BEGIN


DECLARE @nowtime datetime

SET @nowtime = now()

--INSERT new blank row and return the id from the identiy column
INSERT into SEG01_filterid_lookup_tbl(allocated_date)
  VALUES (@nowtime)
commit

SELECT @_filterid = uniqid
  from SEG01_filterid_lookup_tbl
 where allocated_date = @nowtime

END;


---
/*
select *
from SEG01_filterid_lookup_tbl
*/
