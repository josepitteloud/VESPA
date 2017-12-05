SELECT 
TO_CHAR( fact_broadcast_date,'YYYYMMDD') broadcast_start_date,
(case when actual_impressions_sum is null then 0 else actual_impressions_sum end) total_impressions,
1 allowed_variance
FROM
(select date1.utc_day_date FACT_BROADCAST_DATE,count(1) records_count,
sum(slot.Actual_impressions) actual_impressions_sum
from
(select actual_impressions,  dk_broadcast_start_datehour_dim,
DK_adsmart_media_CAMPAIGN_DIM
from SMI_DW..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
where actual_impressions > 0) slot,
smi_dw..DATEHOUR_DIM date1,
smi_dw..campaign_dim d
where date1.pk_datehour_dim = slot.dk_broadcast_start_datehour_dim
and slot.DK_adsmart_media_CAMPAIGN_DIM = d.PK_CAMPAIGN_DIM
and d.ADSMART_FLAG = 'Y'
and slot.DK_adsmart_media_CAMPAIGN_DIM > 0
group by date1.utc_day_date)a
order by 1 