
select a.*, b.calculated_scaling_weight into #tmp_viewing_chk
from
(select slot_data_key, household_key, substr(cast(viewed_start_date_key as varchar(10)),1,8) viewed_event_date,
scaling_factor from sk_prod.slot_data
where 
--substr(cast(viewed_start_date_key as varchar(10)),1,8) = '20130521'
 household_key > 0) a,
(select household_key, replace(cast(adjusted_event_start_date_vespa as varchar(12)),'-','') viewed_event_date,
 calculated_scaling_weight from sk_prod.viq_viewing_data_scaling
where replace(cast(adjusted_event_start_date_vespa as varchar(12)),'-','') = '20130521'
where  household_key > 0) b
where a.household_key = b.household_key
and a.viewed_event_date = b.viewed_event_date
and a.scaling_factor != b.calculated_scaling_weight

select a.*, B.CALCULATED_SCALING_WEIGHT into #tmp_viewing_chk_2
from
sk_prod.slot_data a,
#tmp_viewing_chk b
where A.SLOT_DATA_KEY = B.SLOT_DATA_KEY
and a.time_shift_key = 0

SELECT VIEWED_START_DATE_KEY, COUNT(1), count(distinct scaling_factor) scaling_weights FROM #tmp_viewing_chk_2
GROUP BY VIEWED_START_DATE_KEY
