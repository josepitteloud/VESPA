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
        
        This build is an adjustment on the one that killed the server: the table
        that was temporary is no longer temporary. Maybe that will make a
        difference?
        
*/

IF object_id('scaling_get_weekly_sample_maybe_lethal_3a') IS NOT NULL
    DROP PROCEDURE scaling_get_weekly_sample_maybe_lethal_3a;
commit;
go
    
CREATE PROCEDURE scaling_get_weekly_sample_maybe_lethal_3a
AS
BEGIN

-- This guy is the problem one:
UPDATE scaling_weekly_sample
SET 
    universe    = ac.universe
    ,boxtype    = ac.boxtype
    ,mr_boxes   = ac.mr_boxes
FROM scaling_weekly_sample
inner join boxtype_ac_3a AS ac
on ac.account_number = scaling_weekly_sample.account_number
-- This update seems incredibly slow for what it's doing, but both
-- join columns should be indexed? Maybe we put the cases onto an
-- update on #boxtype and then it's just a PK <-> PK linked copy
-- operation? We'll see.

commit

end;
commit;
go


-- OK, now run the thing (Warning: may hang on server forever):
create variable @thetime datetime;
set @thetime = now();
commit;
go

scaling_get_weekly_sample_maybe_lethal_3a;
commit;
go

select datediff(minute, @thetime, now());