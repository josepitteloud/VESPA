/*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES
					  
--------------------------------------------------------------------------------------------------------------
**Project Name: 					At Risk Analysis
**Analysts:							Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):							Claudio Lima
**Stakeholder:						Vespa Team.
**Due Date:							
**Project Code (Insight Collation):	
**Sharepoint Folder:				http://rtci/Sky%20Projects/Forms/AllItems.aspx?RootFolder=%2fSky%20Projects%2fVespa%2fAnalysis
									%20Requests%2fV152%20-%20At%20Risk%20Definition&FolderCTID=&View={95B15B22-959B-4B62-809A-AD43E02001BD}
									
**Business Brief:

Identify customers at risk to support panel expansion

--------------------------------------------------------------------------------------------------------------


*/

select *
from sp_tables()
where table_owner in ('models')
order by table_name

select top 100 * from models.model_list -- list of models 
select top 100 * from models.model_percentiles -- model percentiles
select top 100 * from models.model_results -- summarised model results over time
select top 100 * from models.model_scores -- scores but only for 2011
select top 100 * from models.models_at_risk_all_by_month 
select top 100 * from models.pcdm_model_scores
select distinct model_name from models.prospect_model_scores
select top 100 * from models.BCG_SEGMENTS -- no permission
select top 100 * from models.model_scores_skprod3 -- no permission
select top 100 * from models.at_risk_monthly_table -- empty

-- List models by refresh date
select model_name,max(model_run_date)
from models.model_scores 
group by model_name
order by max(model_run_date) desc, model_name

-- List number of HHs on each segmentation
select model_name,count(distinct account_number)
from models.model_scores
where model_run_date >= '2013-02-01'
and model_run_date <= '2013-02-28'
group by model_name
order by model_name

-----------------------------------------------
-- BCG Segments
-----------------------------------------------

-- Number of HHs in BCG segments
select model_name,count(distinct account_number)
from models.model_scores
where model_run_date = '2013-02-28'
and model_name like 'BCG %'
group by model_name
order by model_name
/*
'BCG 10-12M',    32,374
'BCG 12-18M',    27,435
'BCG 12-24M',   205,049
'BCG 2YEARS',   110,462
'BCG 4-12M',    117,157
'BCG UNSTABLE', 105,851
*/

select model_name
        ,decile
        ,min(score)
        ,max(score)
        ,count(*)
from models.model_scores
where model_run_date = '2013-02-28'
and model_name like 'BCG %'
group by model_name,decile
order by model_name,decile

select top 100 * 
from models.model_scores
where model_run_date = '2013-02-28'
and model_name like 'BCG %'

-----------------------------------------------
-- Customer value segments
-----------------------------------------------

select top 100 * from sk_prod.VALUE_SEGMENTS_DATA

select count(distinct account_number) 
from sk_prod.VALUE_SEGMENTS_DATA 
-- 10,118,060

-- Look at customer value distribution
select cb_data_date
        ,value_seg
        ,count(distinct account_number)
from sk_prod.VALUE_SEGMENTS_DATA
group by cb_data_date
        ,value_seg
order by cb_data_date
        ,value_seg
/*
'2013-03-04','Bedding In',1932498
'2013-03-04','Bronze    ',1194602
'2013-03-04','Copper    ',1801910
'2013-03-04','Gold      ',1419585
'2013-03-04','Platinum  ',1176396
'2013-03-04','Silver    ',1382320
'2013-03-04','Unstable  ',1210749
*/

select count(distinct account_number)
from sk_prod.VALUE_SEGMENTS_DATA
where value_seg in ('Bedding In','Unstable')
-- 3,143,247

------------------------------------------------------------
-- Overlap between customer value and cuscan segmentations
------------------------------------------------------------

select count(distinct account_number)
from sk_prod.VALUE_SEGMENTS_DATA
where account_number in
(
select account_number
from models.model_scores
where model_run_date = '2013-02-28'
and model_name like 'BCG %'
)
-- 536,472 

select count(distinct account_number)
from models.model_scores
where model_run_date = '2013-02-28'
and model_name like 'BCG %'
-- 538,519

--------------------------------------------
-- At risk customer already in the panel
--------------------------------------------

select Status_Vespa,count(*)
from vespa_analysts.vespa_single_box_view
where account_number in
(
select account_number
from sk_prod.VALUE_SEGMENTS_DATA
where value_seg in ('Bedding In','Unstable')
)
group by Status_Vespa
order by Status_Vespa
/*
,26617
'DisablePending',15957
'Disabled',16606
'EnablePending',259
'EnableRequested',5328
'Enabled',300479
*/

----------------------------------------------------------
-- AD: Checking Overlaps between both models (VSD and BCG)
----------------------------------------------------------

select  bcg.account_number
        ,bcg.model_name
        ,vsd.value_seg
from
        (
            select  model_name
                    ,account_number
            from    models.model_scores
            where   model_run_date = '2013-02-28'
            and     model_name like 'BCG %'  -- Selecting specific models we want...
        )   as BCG
        inner join  (
                        select  distinct
                                value_seg
                                ,account_number
                        from    sk_prod.VALUE_SEGMENTS_DATA
                        where   value_seg in    (
                                                    'Bedding In'
                                                    ,'Unstable'
                                                )
                    ) as VSD
        on bcg.account_number = vsd.account_number -- 489405 Recs...
		
		


-----------------------------------------------------
-- AD: Checking CSAV table for spotting key variables
-----------------------------------------------------

describe SK_PROD.CUST_SINGLE_ACCOUNT_VIEW

-------------------------
-- AD: Plotting Variables
-------------------------
select  
        account_status
        ,count(distinct account_number) as totalAccounts
        ,min(acct_count_cancellation_attempts_in_12m) as minreac12m
        ,max(acct_count_cancellation_attempts_in_12m) as maxreac12m
        ,avg(acct_count_cancellation_attempts_in_12m) as avgreac12m
        ,stddev(acct_count_cancellation_attempts_in_12m) as stdreac12m
        ,min(acct_count_cust_cancels_12m) as mincustcancel12
        ,max(acct_count_cust_cancels_12m) as maxcustcancel12
        ,avg(acct_count_cust_cancels_12m) as avgcustcancel12
        ,stddev(acct_count_cust_cancels_12m) as stdcustcancel12
        ,min(acct_count_ivc_reactivations_in_12m) as minreact12
        ,max(acct_count_ivc_reactivations_in_12m) as maxreact12
        ,avg(acct_count_ivc_reactivations_in_12m) as avgreact12
        ,stddev(acct_count_ivc_reactivations_in_12m) as stdreact12
        ,min(acct_count_pending_cancels_in_12m) as minpending12
        ,max(acct_count_pending_cancels_in_12m) as maxpending12
        ,avg(acct_count_pending_cancels_in_12m) as avgpending12
        ,stddev(acct_count_pending_cancels_in_12m) as stdpending12
        ,min(cl_churn_attempt_score_6m) as minchurnscore
        ,max(cl_churn_attempt_score_6m) as maxchurnscore
        ,avg(cl_churn_attempt_score_6m) as avgchurnscore
        ,stddev(cl_churn_attempt_score_6m) as stdchurnscore
from    SK_PROD.CUST_SINGLE_ACCOUNT_VIEW
group   by  account_status

---------------------------------------------------------------------
-- AD: Overlaping CSAV plots with accounts on VSD for target segments
---------------------------------------------------------------------

select  distinct
        model.account_number
        ,case    when model.value_seg in ('Bedding In','Unstable')
                then model.value_seg
                else 'Other'
        end     as theSegments
        ,csav.cancelations
        ,csav.last_cancel_dt
        ,csav.try_cancel
        ,csav.cancel_attempt_dt
into    atrisk_plot2
from    sk_prod.VALUE_SEGMENTS_DATA as model
        inner join  (
                        select  account_number
                                ,sum(acct_count_cust_cancels_ever) as cancelations
                                ,max(acct_latest_pending_cancel_dt) as last_cancel_dt
                                ,sum(acct_count_cancellation_attempts_in_12m) as try_cancel
                                ,max(acct_latest_cancellation_attempt_dt) as cancel_attempt_dt
                        from    SK_PROD.CUST_SINGLE_ACCOUNT_VIEW
                        group   by  account_number
                    ) as csav
        on model.account_number = csav.account_number -- 10128993 row(s) affected

-- I've created the table... now checking, account should be a key field...
select  count(distinct account_number) 
from    sk_prod.VALUE_SEGMENTS_DATA

-- Checking at weird cases...
-- when we have cancelation but no date...

select  count(1)
from    atrisk_plot2
where   cancelations > 0
and     last_cancel_dt is null -- 17038

-- when we have cancelation date but not the count

select  count(1)
from    atrisk_plot2
where   (cancelations = 0 or cancelations is null)
and     last_cancel_dt is not null -- 345461

-- when we have cancel attempt but no date...

select  count(1)
from    atrisk_plot2
where   try_cancel > 0
and     cancel_attempt_dt is null -- 756

-- when we have cancel attempt date but not the count...

select  count(1)
from    atrisk_plot2
where   (try_cancel is null or try_cancel = 0)
and     cancel_attempt_dt is not null -- 4529905 -- I'm thinking that cancel date also gets reflected in this field...


