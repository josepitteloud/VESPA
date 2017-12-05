/*
	The Actual Script to refresh the table
*/


truncate table z_pa_focus_group_MARCH; commit;

insert	into z_pa_focus_group_MARCH
select	stb_type
		,substr(serial,1,16) as dk_serial_number
from	(
			select	*
					,dense_rank() over	(
											partition by	stb_type
											order by		x
										)	as therank
			from	(
						select	distinct
								substr(dk_serial_number,3,1) 		as stb_type
								,dk_serial_number as serial
								,random() as x
						from	(
									select	distinct
											substr(dk_serial_number,3,1)	as stb_type
											,dk_serial_number
									from	ethan_pa_prod..pa_events_fact
									where	dk_date between 20160303 and 20160305
								)	as step0
					)	as step1
		)	as step2
where	(
			(stb_type = 'B' and therank <= (cast(2000 as float)*round(0.9,2))) -- Falcons
			--or (stb_type='D' and therank <= (cast(2000 as float)*round(0.535745,2))) -- MRs
			or (stb_type='C' and therank <= (cast(2000 as float)*round(0.1,2))) -- Xwings
		);

commit;


-----------------------
-- To be ran on Netezza
-----------------------
truncate table z_globalnav_lvl1_step1_march;

commit;

insert	into z_globalnav_lvl1_step1_march
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
--into	z_globalnav_lvl1_step1_march
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
					inner join z_pa_focus_group_MARCH as c
					on	substr(a.dk_serial_number,1,16)	= c.dk_serial_number
--					inner join pa_trial_dim 	as c
--					on	a.dk_serial_number = c.pk_serial_number
--					and	c.dk_utype in (3,4)
			where	dk_date >= 20160301
		)	as a
		left join Z_PA_SCREEN_DIM_V2 as b -- pa_screen_dim
		on	a.thelinkage = b.PK_SCREEN_ID
		and b.pk_screen_id not like '0%'
		and	b.SESSION_TYPE is not null;
		
		
commit;