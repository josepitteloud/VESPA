-- Clean out the transient tables used by OpDash. Transient means they get rebuilt
-- from week to week, but they're still permanent since they might get built, say
-- on a scheduler, and still need to be around when the reports get extracted.

execute vespa_analysts.OpDash_clear_transients;
-- Now in a proc since they're in vespa_analysts and need to be dropped by that user.
