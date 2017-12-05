------------------query based on broadcast_date-----------------------

select fact_broadcast_date, 
(case when yesterdays_actual is null then 0 else yesterdays_actual end) yesterdays_actual,
(case when DELETE_actual_impressions_sum is null then 0 else DELETE_actual_impressions_sum end) DELETE_actual_impressions_sum,
(case when INSERT_actual_impressions_sum is null then 0 else INSERT_actual_impressions_sum end) INSERT_actual_impressions_sum,
(case when inserts_and_deletes is null then 0 else inserts_and_deletes end) inserts_and_deletes,
(case when actual_impressions_sum is null then 0 else actual_impressions_sum end) actual_impressions_sum
from
(SELECT 
C.fact_broadcast_date,
c.actual_impressions_sum - COALESCE(b.INSERT_actual_impressions_sum,0) + COALESCE(a.DELETE_actual_impressions_sum,0) yesterdays_actual,
A.DELETE_actual_impressions_sum, B.INSERT_actual_impressions_sum, d.total_impression inserts_and_deletes,
c.actual_impressions_sum
FROM
(select date1.broadcast_day_date FACT_BROADCAST_DATE,count(1) records_count,
sum(case when slot.actual_impressions > 0 then 1 else 0 end) records_with_impression,
sum(case when slot.actual_impressions <= 0 then 1 else 0 end) records_with_no_impression,
sum(slot.Actual_impressions) actual_impressions_sum
from
(select actual_impression actual_impressions,  dk_broadcast_start_datehour_dim,
dk_slot_instance_dim,DK_adsmart_media_CAMPAIGN_DIM
from SMI_ACCESS..V_VIEWING_SLOT_INSTANCE_FACT
where actual_impression > 0) slot,
smi_dw..DATEHOUR_DIM date1,
smi_dw..campaign_dim d
where date1.pk_datehour_dim = slot.dk_broadcast_start_datehour_dim
and slot.DK_adsmart_media_CAMPAIGN_DIM = d.PK_CAMPAIGN_DIM
and d.ADSMART_FLAG = 'Y'
and slot.DK_adsmart_media_CAMPAIGN_DIM > 0
and date1.broadcast_day_date between '2013-12-18' and date(now())
group by date1.broadcast_day_date) C
LEFT OUTER JOIN
(select date1.broadcast_day_date DELETE_BROADCAST_DATE
,count(1) DELETE_records_count
,sum(case when slot.actual_impression > 0 then 1 else 0 end) DELETE_records_with_impression
,sum(case when slot.actual_impression <= 0 then 1 else 0 end) DELETE_records_with_no_impression
,sum(slot.Actual_impression) DELETE_actual_impressions_sum
from
(Select actual_impression,  dk_broadcast_start_datehour_dim,
dk_slot_instance_dim from SMI_EXPORT..VIEWING_SLOT_INSTANCE_FACT_DELETES
where actual_impression > 0) slot
,smi_dw..DATEHOUR_DIM date1
where date1.pk_datehour_dim = slot.dk_broadcast_start_datehour_dim
and slot.actual_impression > 0
and date1.broadcast_day_date between '2013-12-18' and date(now())
group by date1.broadcast_day_date) A 
ON C.FACT_BROADCAST_DATE = A.DELETE_BROADCAST_DATE
LEFT OUTER JOIN
(select date1.broadcast_day_date INSERT_BROADCAST_DATE
,count(1) INSERT_records_count
,sum(case when slot.actual_impression > 0 then 1 else 0 end) INSERT_records_with_impression
,sum(case when slot.actual_impression <= 0 then 1 else 0 end) INSERT_records_with_no_impression
,sum(slot.Actual_impression) INSERT_actual_impressions_sum
from
(Select actual_impression,  dk_broadcast_start_datehour_dim,
dk_slot_instance_dim from SMI_EXPORT..VIEWING_SLOT_INSTANCE_FACT
where actual_impression > 0) slot
,smi_dw..DATEHOUR_DIM date1
where date1.pk_datehour_dim = slot.dk_broadcast_start_datehour_dim
and slot.actual_impression > 0
and date1.broadcast_day_date between '2013-12-18' and date(now())
group by date1.broadcast_day_date) B
ON C.FACT_BROADCAST_dATE = B.INSERT_BROADCAST_DATE
left outer join
(SELECT DH.BROADCAST_DAY_DATE, SUM(VSIF_I.ACTUAL_IMPRESSION) AS TOTAL_IMPRESSION	
	FROM SMI_EXPORT..VIEWING_SLOT_INSTANCE_FACT_DELETES VSIF_D	
	JOIN SMI_EXPORT..VIEWING_SLOT_INSTANCE_FACT VSIF_I	
		ON VSIF_D.PK_VIEWING_SLOT_INSTANCE_FACT = VSIF_I.PK_VIEWING_SLOT_INSTANCE_FACT
	JOIN SMI_DW..DATEHOUR_DIM DH	
		ON VSIF_D.DK_BROADCAST_START_DATEHOUR_DIM = DH.PK_DATEHOUR_DIM
		AND VSIF_I.ACTUAL_IMPRESSION > 0
		AND DH.BROADCAST_DAY_DATE between '2013-12-18' and date(now())
	GROUP BY DH.BROADCAST_DAY_DATE) d
on c.fact_broadcast_date = d.broadcast_day_date) a
order by 1 


---------------------------------------------query based on local date-------------------------------------------------

select fact_broadcast_date, 
(case when yesterdays_actual is null then 0 else yesterdays_actual end) yesterdays_actual,
(case when DELETE_actual_impressions_sum is null then 0 else DELETE_actual_impressions_sum end) DELETE_actual_impressions_sum,
(case when INSERT_actual_impressions_sum is null then 0 else INSERT_actual_impressions_sum end) INSERT_actual_impressions_sum,
(case when inserts_and_deletes is null then 0 else inserts_and_deletes end) inserts_and_deletes,
(case when actual_impressions_sum is null then 0 else actual_impressions_sum end) actual_impressions_sum
from
(SELECT 
C.fact_broadcast_date,
c.actual_impressions_sum - COALESCE(b.INSERT_actual_impressions_sum,0) + COALESCE(a.DELETE_actual_impressions_sum,0) yesterdays_actual,
A.DELETE_actual_impressions_sum, B.INSERT_actual_impressions_sum, d.total_impression inserts_and_deletes,
c.actual_impressions_sum
FROM
(select date1.local_day_date FACT_BROADCAST_DATE,count(1) records_count,
sum(case when slot.actual_impressions > 0 then 1 else 0 end) records_with_impression,
sum(case when slot.actual_impressions <= 0 then 1 else 0 end) records_with_no_impression,
sum(slot.Actual_impressions) actual_impressions_sum
from
(select actual_impression actual_impressions,  dk_broadcast_start_datehour_dim,
dk_slot_instance_dim,DK_adsmart_media_CAMPAIGN_DIM
from SMI_ACCESS..V_VIEWING_SLOT_INSTANCE_FACT
where actual_impression > 0) slot,
smi_dw..DATEHOUR_DIM date1,
smi_dw..campaign_dim d
where date1.pk_datehour_dim = slot.dk_broadcast_start_datehour_dim
and slot.DK_adsmart_media_CAMPAIGN_DIM = d.PK_CAMPAIGN_DIM
and d.ADSMART_FLAG = 'Y'
and slot.DK_adsmart_media_CAMPAIGN_DIM > 0
and date1.local_day_date between '2013-12-18' and date(now())
group by date1.local_day_date) C
LEFT OUTER JOIN
(select date1.local_day_date DELETE_BROADCAST_DATE
,count(1) DELETE_records_count
,sum(case when slot.actual_impression > 0 then 1 else 0 end) DELETE_records_with_impression
,sum(case when slot.actual_impression <= 0 then 1 else 0 end) DELETE_records_with_no_impression
,sum(slot.Actual_impression) DELETE_actual_impressions_sum
from
(Select actual_impression,  dk_broadcast_start_datehour_dim,
dk_slot_instance_dim from SMI_EXPORT..VIEWING_SLOT_INSTANCE_FACT_DELETES
where actual_impression > 0) slot
,smi_dw..DATEHOUR_DIM date1
where date1.pk_datehour_dim = slot.dk_broadcast_start_datehour_dim
and slot.actual_impression > 0
and date1.local_day_date between '2013-12-18' and date(now())
group by date1.local_day_date) A 
ON C.FACT_BROADCAST_DATE = A.DELETE_BROADCAST_DATE
LEFT OUTER JOIN
(select date1.local_day_date INSERT_BROADCAST_DATE
,count(1) INSERT_records_count
,sum(case when slot.actual_impression > 0 then 1 else 0 end) INSERT_records_with_impression
,sum(case when slot.actual_impression <= 0 then 1 else 0 end) INSERT_records_with_no_impression
,sum(slot.Actual_impression) INSERT_actual_impressions_sum
from
(Select actual_impression,  dk_broadcast_start_datehour_dim,
dk_slot_instance_dim from SMI_EXPORT..VIEWING_SLOT_INSTANCE_FACT
where actual_impression > 0) slot
,smi_dw..DATEHOUR_DIM date1
where date1.pk_datehour_dim = slot.dk_broadcast_start_datehour_dim
and slot.actual_impression > 0
and date1.local_day_date between '2013-12-18' and date(now())
group by date1.local_day_date) B
ON C.FACT_BROADCAST_dATE = B.INSERT_BROADCAST_DATE
left outer join
(SELECT DH.local_day_date, SUM(VSIF_I.ACTUAL_IMPRESSION) AS TOTAL_IMPRESSION	
	FROM SMI_EXPORT..VIEWING_SLOT_INSTANCE_FACT_DELETES VSIF_D	
	JOIN SMI_EXPORT..VIEWING_SLOT_INSTANCE_FACT VSIF_I	
		ON VSIF_D.PK_VIEWING_SLOT_INSTANCE_FACT = VSIF_I.PK_VIEWING_SLOT_INSTANCE_FACT
	JOIN SMI_DW..DATEHOUR_DIM DH	
		ON VSIF_D.DK_BROADCAST_START_DATEHOUR_DIM = DH.PK_DATEHOUR_DIM
		AND VSIF_I.ACTUAL_IMPRESSION > 0
		AND DH.local_day_date between '2013-12-18' and date(now())
	GROUP BY DH.local_day_date) d
on c.fact_broadcast_date = d.local_day_date) a
order by 1 






------------------query based on broadcast_date-----------------------

SELECT C.*, A.*, B.*, 
c.actual_impressions_sum + COALESCE(b.INSERT_actual_impressions_sum,0) - COALESCE(a.DELETE_actual_impressions_sum,0) ACTUAL_IMPRESSIONS_TOTAL
FROM
(select date1.broadcast_day_date FACT_BROADCAST_DATE,count(1) records_count,
sum(case when slot.actual_impressions > 0 then 1 else 0 end) records_with_impression,
sum(case when slot.actual_impressions <= 0 then 1 else 0 end) records_with_no_impression,
sum(slot.Actual_impressions) actual_impressions_sum
from
(Select actual_impressions,  dk_broadcast_start_datehour_dim,
dk_slot_instance_dim,DK_adsmart_media_CAMPAIGN_DIM
from SMI_dw..VIEWING_SLOT_INSTANCE_FACT_static ) slot,
smi_dw..DATEHOUR_DIM date1,
smi_dw..campaign_dim d
where date1.pk_datehour_dim = slot.dk_broadcast_start_datehour_dim
and slot.DK_adsmart_media_CAMPAIGN_DIM = d.PK_CAMPAIGN_DIM
and d.ADSMART_FLAG = 'Y'
and slot.DK_adsmart_media_CAMPAIGN_DIM > 0
and date1.broadcast_day_date between '2013-12-18' and date(now())
group by date1.broadcast_day_date
union all
select date1.broadcast_day_date FACT_BROADCAST_DATE,count(1) records_count,
sum(case when slot.actual_impressions > 0 then 1 else 0 end) records_with_impression,
sum(case when slot.actual_impressions <= 0 then 1 else 0 end) records_with_no_impression,
sum(slot.Actual_impressions) actual_impressions_sum
from
(Select actual_impressions,  dk_broadcast_start_datehour_dim,
dk_slot_instance_dim,DK_adsmart_media_CAMPAIGN_DIM
from SMI_dw..VIEWING_SLOT_INSTANCE_FACT_volatile) slot,
smi_dw..DATEHOUR_DIM date1,
smi_dw..campaign_dim d
where date1.pk_datehour_dim = slot.dk_broadcast_start_datehour_dim
and slot.DK_adsmart_media_CAMPAIGN_DIM = d.PK_CAMPAIGN_DIM
and d.ADSMART_FLAG = 'Y'
and slot.DK_adsmart_media_CAMPAIGN_DIM > 0
and date1.broadcast_day_date between '2013-12-18' and date(now())
group by date1.broadcast_day_date) C
LEFT OUTER JOIN
(select date1.broadcast_day_date DELETE_BROADCAST_DATE
,count(1) DELETE_records_count
,sum(case when slot.actual_impression > 0 then 1 else 0 end) DELETE_records_with_impression
,sum(case when slot.actual_impression <= 0 then 1 else 0 end) DELETE_records_with_no_impression
,sum(slot.Actual_impression) DELETE_actual_impressions_sum
from
(Select actual_impression,  dk_broadcast_start_datehour_dim,
dk_slot_instance_dim from SMI_EXPORT..VIEWING_SLOT_INSTANCE_FACT_DELETES) slot
,smi_dw..DATEHOUR_DIM date1
where date1.pk_datehour_dim = slot.dk_broadcast_start_datehour_dim
and slot.actual_impression > 0
and date1.broadcast_day_date between '2013-12-18' and date(now())
group by date1.broadcast_day_date) A 
ON C.FACT_BROADCAST_DATE = A.DELETE_BROADCAST_DATE
LEFT OUTER JOIN
(select date1.broadcast_day_date INSERT_BROADCAST_DATE
,count(1) INSERT_records_count
,sum(case when slot.actual_impression > 0 then 1 else 0 end) INSERT_records_with_impression
,sum(case when slot.actual_impression <= 0 then 1 else 0 end) INSERT_records_with_no_impression
,sum(slot.Actual_impression) INSERT_actual_impressions_sum
from
(Select actual_impression,  dk_broadcast_start_datehour_dim,
dk_slot_instance_dim from SMI_EXPORT..VIEWING_SLOT_INSTANCE_FACT) slot
,smi_dw..DATEHOUR_DIM date1
where date1.pk_datehour_dim = slot.dk_broadcast_start_datehour_dim
and date1.broadcast_day_date between '2013-12-18' and date(now())
group by date1.broadcast_day_date) B
ON C.FACT_BROADCAST_dATE = B.INSERT_BROADCAST_DATE
ORDER BY 1

