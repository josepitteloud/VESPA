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
**Project Name:                         DB Maintenance Report (DBM)
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Stakeholder:                          SkyIQ - Gavin Meggs / Jose Loureda
**Due Date:                             14/02/2014
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:

        A report to monitor the DB space usage related to SkyIQ DB users...
        
        This is a report that is highly recommended to be ran out of office hours, where there is little interaction
        with the tables, hence getting a more accurate snapshot of the DB. Mind that such interaction could disrupt
        the execution of the report...

**Tables List:

	vespa_dbmaintenance_summary (output table)

--------------------------------------------------------------------------------------------------------------
*/
------------------------------
-- vespa_dbmaintenance_summary
------------------------------

create table vespa_dbmaintenance_summary	(
    uname		varchar(50)
    ,tname 		varchar(200)
    ,updated 	timestamp
    ,mbytes 	decimal(16,5)
);

commit;

grant select on vespa_dbmaintenance_summary to vespa_group_low_security;
commit;