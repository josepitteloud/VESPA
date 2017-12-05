/* Autor : Patrick Igonor*/
--Analysis on the customers that have requested to opt out vs those that opted in---

--Checks on tables of interest....

select top 10 * from vespa_analysts.SC2_Intervals -- scaling ID for each HHs
select top 10 * from vespa_analysts.SC2_Segments_lookup -- scaling segmentation variables/ID
select top 10 * from sk_prod.Cust_Single_Account_View
select top 10* from vespa_analysts.sc2_weightings

--Checks on the disctribution of the customers that opted in and opted out
select CUST_VIEWING_DATA_CAPTURE_ALLOWED, count(*) from sk_prod.Cust_Single_Account_View
group by CUST_VIEWING_DATA_CAPTURE_ALLOWED

--Selecting the most current account number and scaling segment id

select account_number, scaling_segment_id
into Max_Scaling_Segments
from (select account_number,
             scaling_segment_Id,
             reporting_ends,
             max(reporting_ends) over(partition by account_number) max_reporting_end
        from vespa_analysts.SC2_Intervals) mss --max_scaling_segments
where mss.max_reporting_end = reporting_ends
--897,075 Row(s) affected

--Joining the above table to the Single Account View table based on account number and further joining this to the Segment_look up table in order to identify the scaling fields / variables / metrics

select  csa.account_number
       ,mss.scaling_segment_ID
       ,lup.universe
       ,lup.isba_tv_region
       ,lup.hhcomposition
       ,lup.tenure
       ,lup.package
       ,lup.boxtype
       ,case when csa.CUST_VIEWING_DATA_CAPTURE_ALLOWED = 'Y' then 'Opt_In'
             when csa.CUST_VIEWING_DATA_CAPTURE_ALLOWED = 'N' then 'Opt_Out'
             else 'Unknown'
        end as 'Opt_In_Opt_Out'
into   Cust_Opt_In_Opt_Out_Daily_Panel
from sk_prod.Cust_Single_Account_View csa
inner join Max_Scaling_Segments mss
on csa.account_number =mss.account_number
inner join  vespa_analysts.SC2_Segments_lookup lup
on mss.scaling_segment_ID = lup.scaling_segment_ID
where cust_Active_DTV != 0 and pty_country = 'Great Britain'

--832,347 Row(s) affected


---Profiling based on Scaling fields

select   Opt_In_Opt_Out
        ,universe
        ,isba_tv_region
        ,hhcomposition
        ,tenure
        ,package
        ,boxtype
        ,count(distinct account_number)as Num_HH
from Cust_Opt_In_Opt_Out_Daily_Panel
where Opt_In_Opt_Out <> 'Unknown'
group by  Opt_In_Opt_Out
         ,universe
         ,isba_tv_region
         ,hhcomposition
         ,tenure
         ,package
         ,boxtype
order by Num_HH

grant all on Cust_Opt_In_Opt_Out_Daily_Panel to limac;
grant all on Max_Scaling_Segments to limac;



