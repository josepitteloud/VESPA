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

Setting Basis For (SBF) enhancing the churn model table...

--------------------------------------------------------------------------------------------------------------


*/
---------------------------------------
-- AD: Churn Model (added : 15/04/2013)
---------------------------------------

-- Cool so now we have a churn model to support the At Risk Definition...

select top 10 * from Rhirani.At_Risk_Cuscan_Vesp

select top 10 * from atrisk_plot4 -- 5357669

-- Before acting, lets do a QA:

-- How many of these accounts are in the panel already?
select  count(distinct sbv.account_number) as hits
from    vespa_analysts.vespa_single_box_view    as sbv
        inner join Rhirani.At_Risk_Cuscan_Vesp  as churn
        on  sbv.account_number = churn.account_number -- 105696

-- How many of these accounts are in faulty Account's list...
select  count(distinct churn.account_number) as hits
from    Rhirani.At_Risk_Cuscan_Vesp                     as churn
        inner join vespa_analysts.accounts_to_exclude   as faulties
        on  churn.account_number = faulties.account_number -- 2722

-- what are the segments we were given... (can we symplify this?)...
select  distinct new_vespa_segment
from    Rhirani.At_Risk_Cuscan_Vesp

/*
new_vespa_segment
'Unstable_Rule6'
'EOC_Rule6'
'Long_Ten_Rule5'
'Long_Ten_Rule3'
'Long_Ten_Rule4'
'EOC_Rule1'
'EOC_Rule5'
'Unstable_Rule1'
'EOC_Rule4'
'Unstable_Rule2'
'Unstable_Rule4'
'EOC_Rule9'
'Unstable_Rule10'
'EOC_Rule7'
'EOC_Rule8'
'Unstable_Rule7'
*/

-- I can't be simplified, lets enhance the view...

select  *
        ,case   when lower(new_vespa_segment) like 'unstable%' then 'unstable'
                when lower(new_vespa_segment) like 'eoc%' then 'eoc'
                when lower(new_vespa_segment) like 'long_ten%' then 'long tenure'
        end as Churn_segment
into    atrisk_churnmodel
from    Rhirani.At_Risk_Cuscan_Vesp;

commit;

create  hg index atrisk_hd_01 on atrisk_churnmodel(account_number);
create  lf index atrisk_lf_01 on atrisk_churnmodel(Churn_segment);

commit;


-- Result...
select top 10 * from atrisk_churnmodel