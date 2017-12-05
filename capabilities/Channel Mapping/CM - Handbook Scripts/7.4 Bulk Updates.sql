-- 7.4 Bulk Updates

-- Tip: it is easier to bulk update the times and the version after diagnostic checks have been run and any errors have been fixed.
-- The examples below assume that the next release will be version 27.

CREATE VARIABLE @version int;
SET @version = #version#

update VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_BARB
SET effective_from =  dateadd(second,1,effective_from)
where datepart(second,effective_from) = @version-1

update VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_BARB
SET effective_to = (case when effective_to = '2999-12-31 00:00:00' then effective_to else dateadd(second,1,effective_to) end)
where datepart(second,effective_to) = @version-2

update VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES
SET effective_from =  dateadd(second,1,effective_from)
where datepart(second,effective_from) = @version-1

update VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES
SET effective_to = (case when effective_to = '2999-12-31 00:00:00' then effective_to else dateadd(second,1,effective_to) end)
where datepart(second,effective_to) = @version-2

update VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_landmark
SET effective_from =  dateadd(second,1,effective_from)
where datepart(second,effective_from) = @version

update VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_landmark
SET effective_to = (case when effective_to = '2999-12-31 00:00:00' then effective_to else dateadd(second,1,effective_to) end)
where datepart(second,effective_to) = @version-2

update VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_BARB
set version = @version

update VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_attributes
set version = @version

update VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_landmark
set version = @version
--*************************************************
-----------------setting active flag to Y if effective_to date is 31/12/2999 
update cortb.CHANNEL_MAP_DEV_SERVICE_KEY_attributes
set active_channel = 'Y'
where effective_to = '2999-12-31 00:00:00' 

update cortb.CHANNEL_MAP_DEV_SERVICE_KEY_attributes
set active_channel = 'N'
where effective_to <> '2999-12-31 00:00:00'
--*************************************************
-----------------setting channel_name to equal full_name
update cortb.CHANNEL_MAP_DEV_SERVICE_KEY_attributes
set channel_name = full_name
