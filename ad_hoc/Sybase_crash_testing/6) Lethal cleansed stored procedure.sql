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
        
        This is the version for data cleansed of all identifying details, and
        we've also clipped out the comments so it's no longer quite so obvious
        how any of it fits into the business logic, but sure, it'll be a test.
        
*/

IF object_id('lethal_sybase_test') IS NOT NULL
    DROP PROCEDURE lethal_sybase_test;
commit;
go
    
CREATE PROCEDURE lethal_sybase_test
AS
BEGIN

insert into crash_tracking (flag_name) values ('Start: LETHAL cleansed test')

commit

-- Identify flag10 and other flags for each key1
SELECT  tgt.key1
       ,SUM(CASE WHEN flag3=1 THEN 1 ELSE 0 END) AS flag12
       ,MAX(CASE WHEN flag3=0 AND ((tgt.flag5 =1 AND flag7 = 1) OR (tgt.flag5 =1 AND flag6 = 1))         THEN 4
                 WHEN flag3=0 AND ((tgt.flag4 =1 AND tgt.flag7 = 1) OR (tgt.flag4 =1 AND tgt.flag6 = 1)) THEN 3
                 WHEN flag3=0 AND tgt.flag4 =1                                                           THEN 2
                 ELSE                                                                              1 END) AS pb
       ,MAX(CASE WHEN flag3=1 AND ((tgt.flag5 =1 AND flag7 = 1) OR (tgt.flag5 =1 AND flag6 = 1))         THEN 4
                 WHEN flag3=1 AND ((tgt.flag4 =1 AND tgt.flag7 = 1) OR (tgt.flag4 =1 AND tgt.flag6 = 1)) THEN 3
                 WHEN flag3=1 AND tgt.flag4 =1                                                           THEN 2
                 ELSE                                                                              1 END) AS sb
        ,convert(varchar(20), null) as flag02
        ,convert(varchar(30), null) as flag10
  INTO #table3 -- drop table #table3
  FROM table2 AS tgt
GROUP BY tgt.key1

-- Create indices on key2 flag10 temp table
COMMIT
CREATE hg INDEX idx_ac ON #table3(key1)
commit

-- Build the combined flags 
update #table3
set flag02 = CASE WHEN flag12 = 0 THEN 'A) flag02 value A'
                         WHEN flag12 = 1 THEN 'B) flag02 value B'
                         ELSE 'C) flag02 value C' END
    ,flag10  =
        CASE WHEN       flag12 = 0 AND  pb =  3 AND sb =  1   THEN  'A) flag12 value A'
             WHEN       flag12 = 0 AND  pb =  4 AND sb =  1   THEN  'B) flag12 value B'
             WHEN       flag12 = 0 AND  pb =  2 AND sb =  1   THEN  'C) flag12 value C'
             WHEN       flag12 = 0 AND  pb =  1 AND sb =  1   THEN  'D) flag12 value D'
             WHEN       flag12 > 0 AND  pb =  4 AND sb =  4   THEN  'E) flag12 value E'
             WHEN       flag12 > 0 AND (pb =  4 AND sb =  3) OR (pb =  3 AND sb =  4)  THEN  'E) flag12 value E' -- yes this is the 2nd instance of E
             WHEN       flag12 > 0 AND (pb =  4 AND sb =  2) OR (pb =  2 AND sb =  4)  THEN  'F) flag12 value F'
             WHEN       flag12 > 0 AND (pb =  4 AND sb =  1) OR (pb =  1 AND sb =  4)  THEN  'G) flag12 value G'
             WHEN       flag12 > 0 AND  pb =  3 AND sb =  3                            THEN  'H) flag12 value H'
             WHEN       flag12 > 0 AND (pb =  3 AND sb =  2) OR (pb =  2 AND sb =  3)  THEN  'I) flag12 value I'
             WHEN       flag12 > 0 AND (pb =  3 AND sb =  1) OR (pb =  1 AND sb =  3)  THEN  'J) flag12 value J'
             WHEN       flag12 > 0 AND  pb =  2 AND sb =  2                            THEN  'K) flag12 value K'
             WHEN       flag12 > 0 AND (pb =  2 AND sb =  1) OR (pb =  1 AND sb =  2)  THEN  'L) flag12 value L'
                        ELSE   'M) flag12 value M' END

commit


-- Append flag02 and flag10

insert into crash_tracking (flag_name) values ('Checkpoint: LETHAL cleansed test')

commit

-- This guy is the problem one:
UPDATE table1
SET 
    flag02    = ac.flag02
    ,flag10    = ac.flag10
    ,flag12   = ac.flag12
FROM table1
inner join #table3 AS ac
on ac.key1 = table1.key1

commit

insert into crash_tracking (flag_name) values ('Complete: LETHAL cleansed test')

commit

end;
commit;
go

