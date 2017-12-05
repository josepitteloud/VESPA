SELECT DISTINCT a.*
    , b.account_status
    , b.cb_key_household hh
    INTO t1
FROM pitteloudj.Leavers as a
INNER JOIN sk_prod.Cust_SINGLE_ACCOUNT_VIEW as b ON a.account = CAST(b.account_number as bigint)


INSERT INTO t2
SELECT 
    a.*
  , c.*
  , RANK() over(PARTITION BY cb_key_household ORDER BY cb_row_id) AS rank1 
 INTO t2
FROM t1 as a
INNER JOIN sk_prod.EXPERIAN_CONSUMERVIEW as c ON c.cb_key_household = a.hh

DELETE FROM t2 WHERE rank1 > 1

SELECT * FROM t2

--SP_columns EXPERIAN_CONSUMERVIEW 

/*
--raw metrics

 VESPA_Shared.VESPA_Aggr_Raw_All

--bin groups

 VESPA_Shared.VESPA_Aggr_Low_Level_All

--H/M/L grouping  

 VESPA_Shared.VESPA_Aggr_High_Level_All
 
 */
 
 
 /*
 select top 100 cb_key_household,
case head_of_household
when '0' then 'not head of household'
when '1'	then 'head of household'
when 'U'	then 'Unclassified'
else null end 'Head of household'
from sk_prod.experian_consumerview
where head_of_household is not null
*/