/*

**Project Name: 					Broadcast Reporing
**Analysts:							Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):							Jitesh Patel
**Stakeholder:						
**Due Date:							19/02/2013
**Project Code (Insight Collation):	
**Sharepoint Folder:				

**Business Brief:

This table is going to serve as a mapping channel to say which ACTIVE ACCOUNT in the VESPA PANELS is on which
VIRTUAL PANEL...

**Sections:
	
	S01 - Initialising the table
	S02 - Populating the table
	S03 - QA
------------------------------------------------------------------------------------------------------------------	
*/


----------------------------
/* Initialising the table */
----------------------------

if object_id('vespa_broadcast_reporting_vp_map') is not null
	drop table vespa_broadcast_reporting_vp_map;
	
commit;

create table vespa_broadcast_reporting_vp_map(
    account_number  varchar(20) not null unique
    ,vespa_panel    integer not null
    ,vp1            tinyint default 0
    ,vp2            tinyint default 0
    ,vp3            tinyint default 0
    ,vp4            tinyint default 0
    ,vp5            tinyint default 0
    ,vp6            tinyint default 0
    ,vp7            tinyint default 0
    ,vp8            tinyint default 0
    ,vp9            tinyint default 0
    ,vp10           tinyint default 0
)

grant select on vespa_broadcast_reporting_vp_map to vespa_group_low_security;

commit;


--------------------------
/* Populating the table */
--------------------------

-- We need to flag out all active ACCOUNT_NUMBERS that are in the VESPA PANELS...
-- for this we could benefit from SBV as it has these accounts build up on its section B02...

-- so, cake... all we need is to get the accounts out of SBV...
insert into	vespa_broadcast_reporting_vp_map
			(account_number
			,vespa_panel)
select	distinct
		account_number
		,panel_id_vespa
from	vespa_analysts.vespa_single_box_view
where	panel_id_vespa is not null;

commit;


--------------------------
/* 			QA 			*/
--------------------------

-- How many records have we got?
select	'total accounts in broadcast map table' as context
		,count(account_number)
from 	vespa_broadcast_reporting_vp_map

union

-- How many accounts do we have in vespa outside the undesired groups?
select	'total accounts outside undesired groups' as context
		,count(distinct account_number) 
from 	vespa_single_box_view
where 	panel not in ('SKYVIEW', 'CLASH!')