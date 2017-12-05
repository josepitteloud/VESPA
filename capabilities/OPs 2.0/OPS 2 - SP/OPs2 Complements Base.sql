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
**Project Name:							OPS 2.0 Complements Base
**Analysts:                             Angel Donnarumma 	(angel.donnarumma_mirabel@skyiq.co.uk)
										Berwyn Cort			(Berwyn.Cort@skyiq.co.uk)
										James McKane		(James.McKane@skyiq.co.uk)
**Lead(s):                              Angel Donnarumma 	(angel.donnarumma_mirabel@skyiq.co.uk)
**Stakeholder:                          Operational Reports / SIG
**Due Date:                             28/03/2014
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:

	Here are all tables used by the complements design for the OPs 2.0 plataform
	to help the reports calculate the KPIs they need...

**Sections:

	S01: OPs 2.0 Complements Base
		S01.0 - List of Tables
		
--------------------------------------------------------------------------------------------------------------
*/

-------------------------
-- S01.0 - List of Tables
-------------------------

if object_id('vespa_Dialback_log_collection_dump') is not null
    drop table vespa_Dialback_log_collection_dump;
	
commit;

-- A staging table for pulling all the things out of the daily tables
create table vespa_Dialback_log_collection_dump (
        subscriber_id                   decimal(8)      not null
        ,stb_log_creation_date          datetime        not null
        ,doc_creation_date_from_9am     date            not null        -- futzing around with the "from 9AM day" thing doesn't affect profiling by hour :)
        ,first_event_mark               datetime        not null
        ,last_event_mark                datetime        not null
        ,log_event_count                int             not null
        ,hour_received                  tinyint         not null            -- could make doc_creation_date_from_9am a date, but don't know what other knock-on effects that'd have
        ,panel_id                       tinyint         not null
);
commit;

create index maybe_some_kind_of_fake_PK on vespa_Dialback_log_collection_dump (subscriber_id, doc_creation_date_from_9am);
commit;


if object_id('vespa_Dialback_log_daily_summary') is not null
    drop table vespa_Dialback_log_daily_summary;
	
commit;

-- Summarising into one record per box per day
create table vespa_Dialback_log_daily_summary (
        subscriber_id                   decimal(8)      not null
        ,log_date                       date            not null default 0
        ,logs_sent                      int             not null default 0
        ,coverage_starts                datetime        not null            -- might end up summarising over multiple logs for same
        ,coverage_ends                  datetime        not null            -- day. Some pathological cases missed, probably fine.
        ,log_event_count                int             not null
        ,hour_received                  tinyint         not null
        ,primary key (subscriber_id, log_date)
);
commit;