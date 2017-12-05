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

Identify customers at risk to support panel expansion, checking overlaps between both models we have (CVS and Churn) will help us contextualise
what "At Risk" is for us...

CVS: 	Shows segments of accounts base on the value we attribute to them (depending of packages they have, tenure, etc...).
Churn: 	Is a model that calculates the likelyhood of an account to cancel (cuscan) his/her Sky services.

--------------------------------------------------------------------------------------------------------------


*/

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

/*
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
*/

---------------------------------------------------------------------------
-- AD: Churn Model, Refer to SBF - churn model SQL file (added : 15/04/2013) 
---------------------------------------------------------------------------


select top 10 * from atrisk_churnmodel

-- We need to build awareness of where the hightes propensity of churn...
-- Overlaping CVS with the Churn Model displays where the customers at risk are...

-- 1 Overlap (all customers in CVS)
select  churn.churn_segment
        ,plot.thesegments
        ,count(distinct csav.account_number) as hits
from    sk_prod.cust_single_account_view    as csav
        left join atrisk_plot2              as plot
        on  plot.account_number = csav.account_number
        left join atrisk_churnmodel         as churn
        on  plot.account_number = churn.account_number
where   csav.cust_active_dtv = 1
and		csav.account_number not in	(
										select distinct account_number
										from	vespa_analysts.accounts_to_exclude 
									)
group   by  churn.churn_segment
            ,plot.thesegments
order   by  churn.churn_segment
            ,plot.thesegments

-- 1 churn reminder (all customers in CVS)
select  churn.churn_segment
        ,count(distinct csav.account_number) as hits
from    sk_prod.cust_single_account_view as csav
        left join atrisk_churnmodel as churn
        on churn.account_number = csav.account_number
where   churn.account_number not in ( select distinct account_number from atrisk_plot2 )
and		churn.account_number not in	(
										select distinct account_number
										from	vespa_analysts.accounts_to_exclude 
									)
group   by  churn.churn_segment



-- 2 Overlap (Customers already in Vespa Panel)
select  churn.churn_segment
        ,plot.thesegments
        ,count(distinct sbv.account_number) as hits
from    vespa_analysts.vespa_single_box_view    as sbv
        left join atrisk_plot2                  as plot
        on  plot.account_number = sbv.account_number
        left join atrisk_churnmodel            as churn
        on  plot.account_number = churn.account_number
where	sbv.account_number not in	(
										select distinct account_number
										from	vespa_analysts.accounts_to_exclude 
									)
group   by  churn.churn_segment
            ,plot.thesegments
order   by  churn.churn_segment
            ,plot.thesegments



-- 2 churn reminder (Customers already in Vespa Panel)
select  churn.churn_segment
        ,count(distinct churn.account_number) as hits
from    atrisk_churnmodel as churn
        inner join vespa_analysts.vespa_single_box_view as sbv
        on  churn.account_number = sbv.account_number
where   churn.account_number not in ( select distinct account_number from atrisk_plot2 )
and		churn.account_number not in	(
										select distinct account_number
										from	vespa_analysts.accounts_to_exclude 
									)
group   by  churn.churn_segment



/* KNOCKOUT LEVEL 23 */

-- 3 Overlap (Customers candidates for Vespa Panels)
select  churn.churn_segment
        ,plot.thesegments
        ,count(distinct water.account_number) as hits
from    vespa_analysts.waterfall_base   as water
        left join atrisk_plot2          as plot
        on  plot.account_number = water.account_number
        left join atrisk_churnmodel    as churn
        on  plot.account_number = churn.account_number
where   water.account_number not in (
                                        select  distinct account_number 
                                        from    vespa_analysts.vespa_single_box_view
                                    )
and     water.knockout_level >= 24
group   by  churn.churn_segment
            ,plot.thesegments
order   by  churn.churn_segment
            ,plot.thesegments


-- 3 churn reminder (Customers candidates for Vespa Panels)
select  churn.churn_segment
        ,count(distinct churn.account_number) as hits
from    atrisk_churnmodel as churn
        inner join vespa_analysts.waterfall_base as water
        on churn.account_number = water.account_number
        and water.knockout_level >=24
where   churn.account_number not in (select distinct account_number from atrisk_plot2)
group   by  churn.churn_segment



/* KNOCKOUT LEVEL 24 */

-- 3 Overlap (Customers candidates for Vespa Panels)
select  churn.churn_segment
        ,plot.thesegments
        ,count(distinct water.account_number) as hits
from    vespa_analysts.waterfall_base   as water
        left join atrisk_plot2          as plot
        on  plot.account_number = water.account_number
        left join atrisk_churnmodel    as churn
        on  plot.account_number = churn.account_number
where   water.account_number not in (
                                        select  distinct account_number 
                                        from    vespa_analysts.vespa_single_box_view
                                    )
and     water.knockout_level >= 28
group   by  churn.churn_segment
            ,plot.thesegments
order   by  churn.churn_segment
            ,plot.thesegments


-- 3 churn reminder (Customers candidates for Vespa Panels)
select  churn.churn_segment
        ,count(distinct churn.account_number) as hits
from    atrisk_churnmodel as churn
        inner join vespa_analysts.waterfall_base as water
        on churn.account_number = water.account_number
        and water.knockout_level >=28
where   churn.account_number not in (select distinct account_number from atrisk_plot2)
group   by  churn.churn_segment
