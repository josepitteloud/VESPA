/* Autor : Patrick Igonor*/
--Analysis on the customers that have requested to opt out vs those that opted in---

--Checks on tables of interest....

select top 10 * from vespa_analysts.SC2_Intervals -- scaling ID for each HHs
select top 10 * from vespa_analysts.SC2_Segments_lookup_v2_0 -- scaling segmentation variables/ID
select top 10 * from sk_prod.Cust_Single_Account_View
select top 10* from vespa_analysts.sc2_weightings
select top 10* from vespa_analysts.SC2_Sky_base_segment_snapshots ---This is the new table I got from Angel (Has All VESPA Panel)

--Checks on the disctribution of the customers that opted in and opted out
select CUST_VIEWING_DATA_CAPTURE_ALLOWED,count(*) 
from sk_prod.Cust_Single_Account_View
group by CUST_VIEWING_DATA_CAPTURE_ALLOWED
/*
'N',1519433
'Y',10986250
,18827
'?',12756996
*/

--Selecting the most current scaling segment ID for each account number
drop table Scaling_Segments_ID
select account_number,scaling_segment_id
into Scaling_Segments_ID
from vespa_analysts.SC2_Sky_base_segment_snapshots
where profiling_date = '2013-04-11' 
-- 9,415,498 row(s) affected

-- Look at number of active acounts
select cust_Active_DTV,count(*) 
from sk_prod.Cust_Single_Account_View
group by cust_Active_DTV
/*
0,15,144,480
1,10,137,026
*/

-- Look at number of active accounts in UK only
select count(*) 
from sk_prod.Cust_Single_Account_View
where cust_Active_DTV = 1 
and pty_country = 'Great Britain'
-- 9,426,924

-- Joining the above table to the Single Account View table based on account number 
-- and further joining this to the Segment look up table in order to be able to 
-- identify the scaling fields / variables
drop table Cust_Opt_In_Opt_Out
select  csa.account_number
       ,ssi.scaling_segment_ID
       ,lup.universe
       ,lup.isba_tv_region
       ,lup.hhcomposition
       ,lup.tenure
       ,lup.package
       ,lup.boxtype
       ,case when csa.CUST_VIEWING_DATA_CAPTURE_ALLOWED = 'Y' then 'Y'
             when csa.CUST_VIEWING_DATA_CAPTURE_ALLOWED = 'N' then 'N'
             else 'Unknown'
        end as 'Data_Consent'
into Cust_Opt_In_Opt_Out
from sk_prod.Cust_Single_Account_View csa
inner join Scaling_Segments_ID ssi
on csa.account_number = ssi.account_number
inner join vespa_analysts.SC2_Segments_lookup_v2_1 lup
on ssi.scaling_segment_ID = lup.scaling_segment_ID
where csa.cust_Active_DTV = 1
and csa.pty_country = 'Great Britain'
-- 9,348,354 row(s) affected

--There is one duplicate but not to worry, it's a combination of opt_in 
-- and unknown and since unknown will be filtered out, no cause for alarm---
select account_number
from Cust_Opt_In_Opt_Out
group by account_number 
having count(*) > 1

--Checking the duplicate---
select * from Cust_Opt_In_Opt_Out
where account_Number in ('210175036842')

---Profiling by the Scaling metrics---
select   Data_Consent
        ,universe
        ,isba_tv_region
        ,hhcomposition
        ,tenure
        ,package
        ,boxtype
        ,count(distinct account_number)as Num_HH
from Cust_Opt_In_Opt_Out
where Data_Consent <> 'Unknown'
group by  Data_Consent
         ,universe
         ,isba_tv_region
         ,hhcomposition
         ,tenure
         ,package
         ,boxtype
order by Num_HH desc

grant all on Cust_Opt_In_Opt_Out to igonorp;
grant all on Scaling_Segments_ID to igonorp;

