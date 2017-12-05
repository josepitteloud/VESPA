SELECT DISTINCT
        DATE(STB_EVENT_START_TIME) dt
        ,account_number
        ,account_number||'-1' AS person_number
                , SUM(DATEDIFF (MINUTE, STB_BROADCAST_START_TIME, STB_BROADCAST_END_TIME) ) minutes
INTO pitteloudj.UAT_Sky_romance
from pitteloudj.TE_VIEW_ind_viewing_live_vosdal
WHERE service_key = 1816
        AND person_1 = 1
GROUP BY dt, account_number
UNION ALL
SELECT DISTINCT
        DATE(STB_EVENT_START_TIME) dt
        ,account_number
        ,account_number||'-2'
                , SUM(DATEDIFF (MINUTE, STB_BROADCAST_START_TIME, STB_BROADCAST_END_TIME) ) minutes
from pitteloudj.TE_VIEW_ind_viewing_live_vosdal
WHERE service_key = 1816
        AND person_2 = 1
GROUP BY dt, account_number
UNION ALL
SELECT DISTINCT
        DATE(STB_EVENT_START_TIME) dt
        ,account_number
        ,account_number||'-3'
                , SUM(DATEDIFF (MINUTE, STB_BROADCAST_START_TIME, STB_BROADCAST_END_TIME) ) minutes
from pitteloudj.TE_VIEW_ind_viewing_live_vosdal
WHERE service_key = 1816
        AND person_3 = 1
GROUP BY dt, account_number
UNION ALL
SELECT DISTINCT
        DATE(STB_EVENT_START_TIME) dt
        ,account_number
        ,account_number||'-4'
                , SUM(DATEDIFF (MINUTE, STB_BROADCAST_START_TIME, STB_BROADCAST_END_TIME) ) minutes
from pitteloudj.TE_VIEW_ind_viewing_live_vosdal
WHERE service_key = 1816
        AND person_4 = 1
GROUP BY dt, account_number
UNION ALL
SELECT DISTINCT
        DATE(STB_EVENT_START_TIME) dt
        ,account_number
        ,account_number||'-5'
                , SUM(DATEDIFF (MINUTE, STB_BROADCAST_START_TIME, STB_BROADCAST_END_TIME) ) minutes
from pitteloudj.TE_VIEW_ind_viewing_live_vosdal
WHERE service_key = 1816
        AND person_5 = 1
GROUP BY dt, account_number
UNION ALL
SELECT DISTINCT
        DATE(STB_EVENT_START_TIME) dt
        ,account_number
        ,account_number||'-6'
                , SUM(DATEDIFF (MINUTE, STB_BROADCAST_START_TIME, STB_BROADCAST_END_TIME) ) minutes
from pitteloudj.TE_VIEW_ind_viewing_live_vosdal
WHERE service_key = 1816
        AND person_6 = 1
GROUP BY dt, account_number
UNION ALL
SELECT DISTINCT
        DATE(STB_EVENT_START_TIME) dt
        ,account_number
        ,account_number||'-7'
                , SUM(DATEDIFF (MINUTE, STB_BROADCAST_START_TIME, STB_BROADCAST_END_TIME) ) minutes
from pitteloudj.TE_VIEW_ind_viewing_live_vosdal
WHERE service_key = 1816
        AND person_7 = 1
GROUP BY dt, account_number
UNION ALL
SELECT DISTINCT
        DATE(STB_EVENT_START_TIME) dt
        ,account_number
        ,account_number||'-8'
                , SUM(DATEDIFF (MINUTE, STB_BROADCAST_START_TIME, STB_BROADCAST_END_TIME) ) minutes
from pitteloudj.TE_VIEW_ind_viewing_live_vosdal
WHERE service_key = 1816
        AND person_8 = 1
GROUP BY dt, account_number
UNION ALL
SELECT DISTINCT
        DATE(STB_EVENT_START_TIME) dt
        ,account_number
        ,account_number||'-9'
                , SUM(DATEDIFF (MINUTE, STB_BROADCAST_START_TIME, STB_BROADCAST_END_TIME) ) minutes
from pitteloudj.TE_VIEW_ind_viewing_live_vosdal
WHERE service_key = 1816
        AND person_9 = 1
GROUP BY dt, account_number
UNION ALL
SELECT DISTINCT
        DATE(STB_EVENT_START_TIME) dt
        ,account_number
        ,account_number||'-10'
                , SUM(DATEDIFF (MINUTE, STB_BROADCAST_START_TIME, STB_BROADCAST_END_TIME) ) minutes
from pitteloudj.TE_VIEW_ind_viewing_live_vosdal
WHERE service_key = 1816
        AND person_10 = 1
GROUP BY dt, account_number
UNION ALL

SELECT DISTINCT
        DATE(STB_EVENT_START_TIME) dt
        ,account_number
        ,account_number||'-11'
                , SUM(DATEDIFF (MINUTE, STB_BROADCAST_START_TIME, STB_BROADCAST_END_TIME) ) minutes
from pitteloudj.TE_VIEW_ind_viewing_live_vosdal
WHERE service_key = 1816
        AND person_11 = 1
GROUP BY dt, account_number
UNION ALL
SELECT DISTINCT
        DATE(STB_EVENT_START_TIME) dt
        ,account_number
        ,account_number||'-12'
                , SUM(DATEDIFF (MINUTE, STB_BROADCAST_START_TIME, STB_BROADCAST_END_TIME) ) minutes
from pitteloudj.TE_VIEW_ind_viewing_live_vosdal
WHERE service_key = 1816
        AND person_12 = 1
GROUP BY dt, account_number
UNION ALL
SELECT DISTINCT
        DATE(STB_EVENT_START_TIME) dt
        ,account_number
        ,account_number||'-13'
                , SUM(DATEDIFF (MINUTE, STB_BROADCAST_START_TIME, STB_BROADCAST_END_TIME) ) minutes
from pitteloudj.TE_VIEW_ind_viewing_live_vosdal
WHERE service_key = 1816
        AND person_13 = 1
GROUP BY dt, account_number
UNION ALL
SELECT DISTINCT
        DATE(STB_EVENT_START_TIME) dt
        ,account_number
        ,account_number||'-14'
                , SUM(DATEDIFF (MINUTE, STB_BROADCAST_START_TIME, STB_BROADCAST_END_TIME) ) minutes
from pitteloudj.TE_VIEW_ind_viewing_live_vosdal
WHERE service_key = 1816
        AND person_14 = 1
GROUP BY dt, account_number
UNION ALL
SELECT DISTINCT
        DATE(STB_EVENT_START_TIME) dt
        ,account_number
        ,account_number||'-15'
                , SUM(DATEDIFF (MINUTE, STB_BROADCAST_START_TIME, STB_BROADCAST_END_TIME) ) minutes
from pitteloudj.TE_VIEW_ind_viewing_live_vosdal
WHERE service_key = 1816
        AND person_15 = 1
GROUP BY dt, account_number
UNION ALL
SELECT DISTINCT
        DATE(STB_EVENT_START_TIME) dt
        ,account_number
        ,account_number||'-16'
                , SUM(DATEDIFF (MINUTE, STB_BROADCAST_START_TIME, STB_BROADCAST_END_TIME) ) minutes
from pitteloudj.TE_VIEW_ind_viewing_live_vosdal
WHERE service_key = 1816
        AND person_16 = 1
GROUP BY dt, account_number

ALTER TABLE  pitteloudj.UAT_Sky_romance ADD head        BIT DEFAULT 0
ALTER TABLE  pitteloudj.UAT_Sky_romance ADD weight Float

UPDATE pitteloudj.UAT_Sky_romance
SET a.weight  = ind_scaling_weight
FROM pitteloudj.UAT_Sky_romance AS a
JOIN pitteloudj.TE_VIEW_individual_details as b ON  a.person_number = CAST(b.account_number||'-'||b.person_number AS VARCHAR) and CAST(a.dt AS VARCHAR(10)) = b.dt
commit

UPDATE pitteloudj.UAT_Sky_romance
SET a.head  = head_of_hhd
FROM pitteloudj.UAT_Sky_romance AS a
JOIN pitteloudj.TE_VIEW_individual_details as b ON  a.person_number = CAST(b.account_number||'-'||b.person_number AS VARCHAR) and CAST(a.dt AS VARCHAR(10)) = b.dt


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT 
	dt
	,  COUNT(*) rows_
	, COUNT(DISTINCT ACCOUNT_NUMBER) unique_accounts
	, count(DISTINCT expression) individuals
	, SUM(weight) total_weight
	, SUM(CASE WHEN head=1 THEN weight ELSE 0 END ) HHs_weight
	, SUM(minutes) total_minutes
	, SUM(CASE WHEN head=1 THEN minutes ELSE 0 END ) HHs_minutes
	, SUM(minutes * weight) total_minutes
	, SUM(CASE WHEN head=1 THEN minutes * weight ELSE 0 END ) HHs_minutes
FROM  pitteloudj.UAT_Sky_romance
GROUP BY dt

