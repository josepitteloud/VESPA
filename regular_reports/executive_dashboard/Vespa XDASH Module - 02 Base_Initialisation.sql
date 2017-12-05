 /*
--------------------------------------------------------------------------------------------------------------
**Project Name: 					Vespa Executive Dashboard
**Analysts:							Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):							Jose Loureda
**Stakeholder:						Vespa Directors / Managers.
**Due Date:							22/02/2013
**Project Code (Insight Collation):	
**Sharepoint Folder:				http://rtci/Sky%20Projects/Forms/AllItems.aspx?RootFolder=%2fSky%20Projects%2fVespa%2fRegular%20reports
									%2fMeta%2fExecutive%20Dashboard&FolderCTID=&View={95B15B22-959B-4B62-809A-AD43E02001BD}
									
**Business Brief:

Module responsible for initialising any global variable required by the project...

**Code Modules:

M02: XDash Base Initialisation
		M02.1 - Initialize Variables
		M02.2 - Setting up the logger
		
--------------------------------------------------------------------------------------------------------------
*/

----------------------------------
/* M02.1 - Initialize Variables */
----------------------------------

if object_id('vespa_xdash_m02_baseinitialisation') is not null
	drop procedure vespa_xdash_m02_baseinitialisation;
	
commit;
go

create procedure vespa_xdash_m02_baseinitialisation
	@log_id	bigint output
as begin
	
	
	declare @logbatch_id	varchar(20)
	declare @logrefres_id	varchar(40)
	declare @Module_id		varchar(3)
	
	set	@Module_id = 'M03'

-----------------------------------
/* M02.2 - Setting up the logger */
-----------------------------------

	-- Now automatically detecting if it's a test build and logging appropriately...
	if lower(user) = 'vespa_analysts'
		set @logbatch_id = 'VespaXDash'
	else
		set @logbatch_id = 'XDash test ' || upper(right(user,1)) || upper(left(user,2))

	set @logrefres_id = convert(varchar(10),today(),123) || ' XD refresh'
	
	execute citeam.logger_create_run @logbatch_id, @logrefres_id, @log_ID output

	execute citeam.logger_add_event @log_ID, 3, @Module_id || ' : Log initialised'

end;

commit;
----------------------------------------------------------------- THE END...