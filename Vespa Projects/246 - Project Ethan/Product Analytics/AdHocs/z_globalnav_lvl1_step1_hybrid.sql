-----------------------
-- To be ran on Netezza
-----------------------

truncate table z_globalnav_lvl1_step1_hybrid;

commit;

insert	into z_globalnav_lvl1_step1_hybrid
select	index_
		,dk_asset_id
		,coalesce (b.SCREEN_PARENT,b.screen_name) as sessions
		,a.thelinkage
		,a.dk_action_id
		,a.asset_uuid
		,theprevious	as dk_previous
		,a.DK_current 	-- destination
		,a.DK_REFERRER_ID
		,a.dk_trigger_id
		,dk_serial_number
		,dk_date
		,remote_type
--into	z_globalnav_lvl1_step1_hybrid
from	(
			select	row_number() over	(order by timems)	as index_
					,dk_asset_id
					,timems
					,dk_date
					,a.dk_serial_number
					,dk_action_id
					,asset_uuid
					,case	when dk_previous in ('01400','N/A') then	dk_referrer_id 
							else dk_previous
					end		as theprevious
					,case	when (dk_referrer_id is not null and dk_referrer_id <> 'N/A') then dk_referrer_id
							when dk_previous like '0%'	then	dk_referrer_id -- to capture cases for Jsons without ref field as a bug
							else dk_previous
					end		as thelinkage
					,dk_current
					,dk_trigger_id
					,dk_referrer_id
					,remote_type
			from	pa_events_fact as a
					inner join z_pa_focus_group as c
					on	a.dk_serial_number	= c.dk_serial_number
--					inner join pa_trial_dim 	as c
--					on	a.dk_serial_number = c.pk_serial_number
--					and	c.dk_utype in (3,4)
			where	dk_date >= 20160301
		)	as a
		left join z_pa_screen_dim as b
		on	a.thelinkage = b.PK_SCREEN_ID
		and b.pk_screen_id not like '0%'
		and	b.SESSION_TYPE is not null;
		
		
commit;