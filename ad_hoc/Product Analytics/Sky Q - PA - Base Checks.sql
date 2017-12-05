select	base.*
		,month_ref.nboxes as monthly_boxes
from	(
			select	date_
					,extract(month from date_)			as the_month
					,count(1) 							as hits
					,sum(Case when dk_Action_id in(02400,03000,00001,02000,02010,02002,02005,04002) then 1 else 0 end) as n_interactives
					,count(distinct dk_serial_number)	as nboxes
			from	z_pa_events_fact
			group	by	date_
						,the_month
		)	as base
		inner join	(
						select	extract(month from date_)			as the_month
								,count(distinct dk_serial_number)	as nboxes
						from	z_pa_events_fact
						group	by	the_month
					)	as month_ref
		on	base.the_month = month_ref.the_month