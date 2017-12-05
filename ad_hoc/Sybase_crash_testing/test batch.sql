-- The testing batch for the query that keeps failing:

delete from crash_tracking;

-- First we want to check that the cleansed version without the bad query doesn't cause issues itself
nonlethal_sybase_test;
commit;
-- took about 40s, completed without errors.

-- We'll also check that the uncleansed version (which we're not sharing) doesn't do bad things (for completeness)
scaling_get_weekly_sample_nonlethal
commit;
-- also took about 40s, it's fine.

-- Now we'll add the one extra query in the cleansed version, and in theory this should hang forever and not be cancelable:
lethal_sybase_test;
commit;

-- But if it does work out, we need to try the uncleansed version again to see if that still does strange things.
scaling_get_weekly_sample;
commit;


-- After all that, or maybe in a separate session, we can check how things are going:
select * from crash_tracking
