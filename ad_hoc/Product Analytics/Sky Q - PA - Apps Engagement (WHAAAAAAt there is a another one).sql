-- APPs

--> BUILD FROM HERE...

select	count(distinct dk_serial_number)	as reach
		,count(distinct(case when dk_action_id = 04002 then dk_serial_number else null end))	as reach
		,sum(case when dk_action_id = 04002 then 1 else 0 end)									as napp_launches
from	z_building
limit	10