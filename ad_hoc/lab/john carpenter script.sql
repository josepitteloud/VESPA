/*
*********** Project Details

Project Name:	John Carpenter a+ and sky go interaction query
Analyst(s):		Angel Donnarumma
Lead:			Jose Loureda
Stakeholder:	John Carpenter
Date:			06/12/2012
Project Code:	NA
SharePoint:		http://rtci/Sky%20Projects/Forms/AllItems.aspx?RootFolder=%2fSky%20Projects%2fVespa%2fLab&FolderCTID=&View={95B15B22-959B-4B62-809A-AD43E02001BD}


*********** Brief 

We need to know the number and % of Anytime + active accounts (across all panels 6,7,12) which have also either streamed 
or downloaded Sky Go content in the last 30 days.

********** Modules

A: Initialisation
	A01 - Defining global variables
	A02 - Reseting transient tables
	
B: Base Generation
	B01 - Creating base with A+
	
C: Derivations
	C01 - Flaging overlaps between base and SkyGo table
	
D: Housekeeping
	D01 - Deleting variables

	
	
	
*/



--	A01 - Defining global variables
create variable @active_from	date;
create variable @active_to		date;
create variable @log_id			bigint;			-- required by the logger...
create variable @Refresh_id     varchar(40);	-- required by the logger...
create variable @run_Id         varchar(20);	-- required by the logger...
create variable @qacount		integer;		-- QA count holder...
create variable @proportion     decimal(18,2);

set	@active_to 		= '2012-11-28';
set @active_from 	= dateadd(day,-30,@active_to);
set @run_id			= 'JC Project';
set @refresh_id		= convert(varchar(10),today(),123);

EXECUTE logger_create_run @run_Id, @Refresh_id, @log_id output




--	A02 - Reseting transient tables

if object_id('acuniverse') is not null
	drop table acuniverse;
	
commit;

-- here we're creating the list of accounts that have A+ active
create table acuniverse (
	account_number varchar(20) primary key
);

commit;

EXECUTE logger_add_event @log_id, 3, 'A02: Reset Completed';


 	
--	B01 - Creating base with A+

-- we're filling the list from SBv as this already holds the Active accounts plus the distribution across the Vespa Panels...
insert into acuniverse
select 	distinct account_number
from 	vespa_analysts.vespa_single_box_view
where	lower(panel) <> 'skyview'
and		box_has_anytime_plus > 0;

commit;

set @qacount = -1;

select @qacount = count(1)
from acuniverse;

EXECUTE logger_add_event @log_id, 3, 'B01: Initial population of', @qacount;



--	C01 - Flaging overlaps between base and SkyGo table

-- Now lets get the accounts that have had any interaction with skyGo on the period of interest ( 30 days back from the 28/11)...
select distinct account_number
into #skygo
from sk_prod.SKY_PLAYER_USAGE_DETAIL
where activity_dt BETWEEN @active_from AND @active_to;

set @qacount = -1;

select 	@qacount = count(1)
from    acuniverse as a
        inner join #skygo as b
        on a.account_number = b.account_number; -- 273,700


EXECUTE logger_add_event @log_id, 3, 'C01: A+ users interacting with SkyGo:', @qacount;



-- 	D01 - Deleting variables

drop  variable @active_from;
drop  variable @active_to;
drop  variable @log_id;		-- required by the logger...
drop  variable @Refresh_id;	-- required by the logger...
drop  variable @run_Id;		-- required by the logger...
drop  variable @qacount;	-- QA count holder...
drop  variable @proportion;
commit;


-- output:
EXECUTE logger_get_latest_job_events 'JC Project', 3;