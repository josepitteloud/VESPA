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

**Project Name:                                         Operational Dashboard Daily Summary - historical Logs
**Analysts:                                                     Angel Donnarumma        (angel.donnarumma_mirabel@skyiq.co.uk)
                                                                        Berwyn Cort                     (Berwyn.Cort@SkyIQ.co.uk)
**Lead(s):                                                      Jose Loureda
**Stakeholder:                                          VESPA / Strategic Insight Team.


**Business Brief:

        To create a table for Operational Dashboard to capture historical logs.

**Sections:

        A: CAPTURE LATEST WEEK'S LOG COUNTS AND BOXES

                A01:

        B: INSERT THIS INTO THE HISTORY TABLE



--------------------------------------------------------------------------------------------------------------------------------------------
USEFUL NOTE:    each building block can be treated as a stand alone unit, hence it is possible to copy/paste the logic and generate a table
                                out of any of them if needed/required...
--------------------------------------------------------------------------------------------------------------------------------------------

*/

create or replace procedure SIG_OpDash_Log_History -- execute vespa_toolbox_02_adsmartUniverse
as
begin


select  cast(LOG_RECEIVED_START_DATE_TIME_UTC as date) as Document_Date
        ,sum(Num_logs_sent_30d) as Logs
        ,count(distinct(dk_log_start_datehour_dim)) as LogsVT
        ,count(distinct ssbv.Account_number) as Distinct_Accounts
        ,count(distinct ssbv.Subscriber_id) as Distinct_Boxes
from    angeld.sig_single_box_view as ssbv
        inner join
        sk_prod.VESPA_DP_PROG_VIEWED_CURRENT as VDPVC
        on ssbv.subscriber_id = VDPVC.subscriber_id
        and dk_log_start_datehour_dim/100 > cast(dateformat(weekending,'yyyy')||dateformat(weekending,'mm')||dateformat(weekending,'dd') as int)-7
        and dk_log_start_datehour_dim/100 <= cast(dateformat(weekending,'yyyy')||dateformat(weekending,'mm')||dateformat(weekending,'dd') as int)
where   ssbv.status_vespa = 'Enabled'
        and ssbv.panel in('VESPA','VESPA11')
group   by Document_Date

end;


commit;

grant execute on SIG_OpDash_Log_History to vespa_group_low_security;
commit;

