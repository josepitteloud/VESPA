/* PA TRIAL DIM */

select	'The diff between rowcount and all pks should be 0. Test =>'	as Test
		,case when (count(1) - count(distinct pk_serial_number)) = 0 then 'PASSED' else 'FAILED' end	as result
from	pa_trial_dim
union
select	'All STB Serial Numbers ARE within the expected length of characters. Test =>'	as test
		,case when (sum(case when length(pk_serial_number)<16 then 1 else 0 end)) = 0 then 'PASSED' else 'FAILED' end	as result
from	pa_trial_dim


/* PA ACTION DIM */

select	'The diff between rowcount and all pks should be 0. Test =>'								as Test
		,case when (count(1) - count(distinct pk_action_id)) = 0 then 'PASSED' else 'FAILED' end	as result
from	pa_action_dim
union
select	'All entries ARE labeled with an EPG_SECTION. Test =>'	as Test
		,case when sum (case when epg_section is null then 1 else 0 end) = 0 then 'PASSED' else 'FAILED' end	as result
from	pa_action_dim
union
select	'All actions HAVE an ACTION_NAME. Test =>'	as Test
		,case when sum (case when action_name is null then 1 else 0 end) = 0 then 'PASSED' else 'FAILED' end	as result
from	pa_action_dim


/* PA TRIGGER DIM */

select	'The diff between rowcount and all pks should be 0. Test =>'	as Test
		,case when (count(1)-count(distinct pk_trigger_id)) = 0 then 'PASSED' else 'FAILED' end	as result
from	pa_trigger_dim
union
select	'All Triggers ARE labeled with a TRIGGER_TYPE. Test =>'			as test
		,case when (sum(case when trigger_type is null then 1 else 0 end)) = 0 then 'PASSED' else 'FAILED' end	as result
from	pa_trigger_dim
union
select	'All Triggers ARE labeled with a TRIGGER_NAME. Test =>'			as test
		,case when (sum(case when trigger_name is null then 1 else 0 end)) = 0 then 'PASSED' else 'FAILED' end	as result
from	pa_trigger_dim
union
select	'There is no discrepancy between the composed PK and source fields. Test =>'					as test
		,case when (sum(case when trigger_type||'-'||trigger_name <> pk_trigger_id then 1 else 0 end))= 0 then 'PASSED' else 'FAILED' end	as result
from	pa_trigger_dim


/* PA_DEVICE_DIM */

select	'The diff between rowcount and all pks should be 0. Test =>'	as Test
		,case when (count(1)-count(distinct pk_serial_number)) = 0 then 'PASSED' else 'FAILED' end as result
from	PA_DEVICE_DIM
union
select	'All STBs ARE indentified by their type. Test =>'	as test
		,case when sum(case when gateway is null then 1 else 0 end) = 0 then 'PASSED' else 'FAILED' end as result
from	PA_DEVICE_DIM
union
select	'All STBs ARE identified by their HARDWARE_NAME. Test =>'	as test
		,case when (sum(case when hardware_name is null then 1 else 0 end)) = 0 then 'PASSED' else 'FAILED' end as result
from	PA_DEVICE_DIM
union
select	'All STBs ARE associated with a DEVICE_VIEWING_CARD_NUMBER. Test =>'	as test
		,case when (sum(case when device_viewing_card_number is null then 1 else 0 end)) = 0 then 'PASSED' else 'FAILED' end as result
from	PA_DEVICE_DIM
union
select	'All STBs viewing card ARE within the expected char length. Test =>'	as test
		,case when (sum(case when length(device_viewing_card_number)<32 then 1 else 0 end)) = 0 then 'PASSED' else 'FAILED' end as result
from	PA_DEVICE_DIM
union
select	'All STBs ARE associated with just one owner (DEVICE_VIEWING_CARD_NUMBER). Test =>' as test
		,case when count(1)	= 0 then 'PASSED' else 'FAILED' end as result
from	(
			select	pk_serial_number
					,count(distinct device_viewing_card_number)	as nowners
			from	pa_device_dim
			group	by	pk_serial_number
		)	as base
where	nowners > 1


/* PA_ASSET_DIM */

select	'The diff between rowcount and all pks should be 0. Test =>'	as Test
		,case when (count(1)-count(distinct pk_asset_id)) = 0 then 'PASSED' else 'FAILED' end as Result
from	pa_asset_dim
union
select	'All Assets ARE associated with a SERVICE_KEY. Test =>'	as test
		,case when (sum(case when service_key is null then 1 else 0 end)) = 0 then 'PASSED' else 'FAILED' end as Result
from	pa_asset_dim
union
select	'All Assets HAVE a play length (PROGRAMME_DURATION_SECONDS). Test =>'	as Test
		,case when (sum(case when programme_duration_seconds < 1 then 1 else 0 end)) = 0 then 'PASSED' else 'FAILED' end as Result
from	pa_asset_dim
union
select	'All Assets ARE associated to a PROGRAMME_UUID. Test =>'		as Test
		,case when (sum(case when (programme_uuid is null or lower(programme_uuid) like '%unknown%')then 1 else 0 end)) = 0 then 'PASSED' else 'FAILED' end as Result
from	pa_asset_dim


/* PA_EVENTS_FACT */

select	'The diff between rowcount and all pks should be 0. Test =>'	as Test
		,case when (count(1)-count(distinct pk_event_id)) = 0 then 'PASSED' else 'FAILED' end as Result
from	PA_EVENTS_FACT
union
select	'All events ARE associated to a DK_SERIAL_NUMBER. Test =>'	as test
		,case when (sum(case when dk_serial_number is null then 1 else 0 end)) = 0 then 'PASSED' else 'FAILED' end as Result
from	PA_EVENTS_FACT
union
select	'All Events ARE associated to a DK_TRIGGER_ID. Test =>'	as test
		,case when (sum(case when dk_trigger_id is null then 1 else 0 end)) = 0 then 'PASSED' else 'FAILED' end as Result
from	PA_EVENTS_FACT
union
select	'All Events ARE associated to a DK_CHANNEL_ID. Test =>'	as test
		,case when (sum(case when dk_channel_id is null then 1 else 0 end)) = 0 then 'PASSED' else 'FAILED' end as Result
from	PA_EVENTS_FACT
union
select	'All Events ARE associated to a DK_ACTION_ID. Test =>'	as Test
		,case when (sum(case when dk_action_id is null then 1 else 0 end)) = 0 then 'PASSED' else 'FAILED' end as Result
from	PA_EVENTS_FACT
union
select	'All Events ARE associated to a DK_ASSET_ID. Test =>' as test
		,case when (sum(case when dk_asset_id is null then 1 else 0 end)) = 0 then 'PASSED' else 'FAILED' end as Result
from	PA_EVENTS_FACT
union
select	'All Events HAVE a timestamp (TIMEMS). Test =>'	as Test
		,case when (sum(case when timems < 0 then 1 else 0 end)) = 0 then 'PASSED' else 'FAILED' end as Result
from	PA_EVENTS_FACT
union
select	'All Events ARE associated to a DK_DATE. Test =>'	as Test
		,case when (sum(case when dk_date is null then 1 else 0 end)) = 0 then 'PASSED' else 'FAILED' end as Result
from	PA_EVENTS_FACT