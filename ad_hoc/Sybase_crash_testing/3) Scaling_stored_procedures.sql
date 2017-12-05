/*
        Purpose
        -------
        Replicate a server-destroying query.

        Sybase 15.2 apparently has problems cancelling a certain type of query.
        This will allegedly be fixed in 15.4, though we want to build some test
        situations with the query that gave us so much trouble the first time
        around.
        
        This script isolates the query that forced so many server restarts the
        first time around. Well, this query has all the stable stuff up to the
        big query that causes all the problems.
        
        The sectioning isn't quite linear, we're retaining whatever was in the
        troublesome build.
        
*/

IF object_id('scaling_get_weekly_sample') IS NOT NULL
    DROP PROCEDURE scaling_get_weekly_sample;
commit;
go
    
CREATE PROCEDURE scaling_get_weekly_sample
AS
BEGIN

insert into crash_tracking (flag_name) values ('Start: LETHAL UNCLEANSED test')

commit

-- Now we're migrating tables scaling_box_level_viewing and scaling_weekly_sample
-- across to the QA server, we don't need all the rest, only the stuff that...
-- doesn't work.

-- Identify boxtype of each box and whether it is a primary or a secondary box
SELECT  tgt.account_number
       ,SUM(CASE WHEN MR=1 THEN 1 ELSE 0 END) AS mr_boxes
       ,MAX(CASE WHEN MR=0 AND ((tgt.HD =1 AND HD1TBstb = 1) OR (tgt.HD =1 AND HDstb = 1))         THEN 4 -- HD ( inclusive of HD1TB)
                 WHEN MR=0 AND ((tgt.SP =1 AND tgt.HD1TBstb = 1) OR (tgt.SP =1 AND tgt.HDstb = 1)) THEN 3 -- HDx ( inclusive of HD1TB)
                 WHEN MR=0 AND tgt.SP =1                                                           THEN 2 -- Skyplus
                 ELSE                                                                              1 END) AS pb -- FDB
       ,MAX(CASE WHEN MR=1 AND ((tgt.HD =1 AND HD1TBstb = 1) OR (tgt.HD =1 AND HDstb = 1))         THEN 4 -- HD ( inclusive of HD1TB)
                 WHEN MR=1 AND ((tgt.SP =1 AND tgt.HD1TBstb = 1) OR (tgt.SP =1 AND tgt.HDstb = 1)) THEN 3 -- HDx ( inclusive of HD1TB)
                 WHEN MR=1 AND tgt.SP =1                                                           THEN 2 -- Skyplus
                 ELSE                                                                              1 END) AS sb -- FDB
        ,convert(varchar(20), null) as universe
        ,convert(varchar(30), null) as boxtype
  INTO #boxtype_ac -- drop table #boxtype_ac
  FROM scaling_box_level_viewing AS tgt
GROUP BY tgt.account_number

-- Create indices on box-level boxtype temp table
COMMIT
CREATE hg INDEX idx_ac ON #boxtype_ac(account_number)
commit

-- Build the combined flags 
update #boxtype_ac
set universe = CASE WHEN mr_boxes = 0 THEN 'A) Single box HH'
                         WHEN mr_boxes = 1 THEN 'B) Dual box HH'
                         ELSE 'C) Multiple box HH' END
    ,boxtype  =
        CASE WHEN       mr_boxes = 0 AND  pb =  3 AND sb =  1   THEN  'A) HDx & No_secondary_box'
             WHEN       mr_boxes = 0 AND  pb =  4 AND sb =  1   THEN  'B) HD & No_secondary_box'
             WHEN       mr_boxes = 0 AND  pb =  2 AND sb =  1   THEN  'C) Skyplus & No_secondary_box'
             WHEN       mr_boxes = 0 AND  pb =  1 AND sb =  1   THEN  'D) FDB & No_secondary_box'
             WHEN       mr_boxes > 0 AND  pb =  4 AND sb =  4   THEN  'E) HD & HD' -- If a hh has HD  then all boxes have HD (therefore no HD and HDx)
             WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  3) OR (pb =  3 AND sb =  4)  THEN  'E) HD & HD'
             WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  2) OR (pb =  2 AND sb =  4)  THEN  'F) HD & Skyplus'
             WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  1) OR (pb =  1 AND sb =  4)  THEN  'G) HD & FDB'
             WHEN       mr_boxes > 0 AND  pb =  3 AND sb =  3                            THEN  'H) HDx & HDx'
             WHEN       mr_boxes > 0 AND (pb =  3 AND sb =  2) OR (pb =  2 AND sb =  3)  THEN  'I) HDx & Skyplus'
             WHEN       mr_boxes > 0 AND (pb =  3 AND sb =  1) OR (pb =  1 AND sb =  3)  THEN  'J) HDx & FDB'
             WHEN       mr_boxes > 0 AND  pb =  2 AND sb =  2                            THEN  'K) Skyplus & Skyplus'
             WHEN       mr_boxes > 0 AND (pb =  2 AND sb =  1) OR (pb =  1 AND sb =  2)  THEN  'L) Skyplus & FDB'
                        ELSE   'M) FDB & FDB' END

commit

-- Append universe and boxtype

insert into crash_tracking (flag_name) values ('Checkpoint: LETHAL UNCLEANSED test')

commit

-- This guy is the problem one:
UPDATE scaling_weekly_sample
SET 
    universe    = ac.universe
    ,boxtype    = ac.boxtype
    ,mr_boxes   = ac.mr_boxes
FROM scaling_weekly_sample
inner join #boxtype_ac AS ac
on ac.account_number = scaling_weekly_sample.account_number
-- This update seems incredibly slow for what it's doing, but both
-- join columns should be indexed? Maybe we put the cases onto an
-- update on #boxtype and then it's just a PK <-> PK linked copy
-- operation? We'll see.

commit

-- ^^ to here currently building, stafforr, but then we hit table locking versions
-- with the old version that didn't work. But yeah, we need to reset the schema and
-- run this all again becase we've changed table structure. Hopefully a primary key
-- will make the update not kill the server? Are there dupes in there or something?
-- Guess we should check that too.

insert into crash_tracking (flag_name) values ('Complete: LETHAL UNCLEANSED test')

commit

end;
commit;
go
