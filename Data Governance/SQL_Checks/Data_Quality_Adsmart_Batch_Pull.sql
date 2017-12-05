
----pull the most recent batch_date data----


select a.date_type, a.batch_date, a.date_value, a.slots_totals, a.actual_impressions,
a.segments_totals, a.households_totals, a.campaigns_totals from data_quality_slots_daily_reporting a,
(select date_type, batch_date, date_value, max(dq_sdr_id) dq_sdr_id
from data_quality_slots_daily_reporting
where batch_date = (select max(batch_date) from data_quality_slots_daily_reporting)
group by date_type, batch_date, date_value) b
where a.dq_sdr_id = b.dq_sdr_id
order by 1,3