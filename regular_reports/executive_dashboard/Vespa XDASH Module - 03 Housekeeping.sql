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

Module responsible for maintining transition tables and reseting any needed entity for a clear run...

**Code Modules:

M03: XDash Housekeeping
		M03.0 - Initialising environment
		M03.1 - Clear processing tables
		M03.2 - Fresh Start
		
--------------------------------------------------------------------------------------------------------------
*/

--------------------------------------
/* M03.0 - Initialising environment */
--------------------------------------

if object_id('vespa_xdash_m03_housekeeping') is not null
	drop procedure vespa_xdash_m03_housekeeping;
	
commit;
go

create procedure vespa_xdash_m03_housekeeping
	@fresh_start bit = null
as begin
	
	declare @done 		tinyint
	declare @alltasks 	tinyint
	
	select	@alltasks = count(1)
	from	vespa_xdash_tasks
	
	select	@done = count(1)
	from	vespa_xdash_tasks
	where	status = 1
	
	-------------------------------------
	/* M03.1 - Clear processing tables */
	-------------------------------------
	
	delete from vespa_xdash_o2_histviewman
	delete from vespa_xdash_o3_trafficlights
	delete from	vespa_xdash_o4_dpdreturnextract
	delete from vespa_xdash_o5_vespaPanelBalance
	delete from vespa_xdash_o6_vespaPanelBalanceHist
	
	update	vespa_xdash_o1_adhocmetrics
	set		lastweek 	= 0
			,lastmonth 	= 0
	
	commit
	
	if @done = @alltasks 
		begin
		
			Update 	vespa_xdash_tasks
			set		status = 0
			
			commit
			
		end
	
	-------------------------
	/* M03.2 - Fresh Start */
	-------------------------

	if @fresh_start = 1
		begin
		
			Update 	vespa_xdash_tasks
			set		status = 0
			
			commit
			
		end
end;

commit; 
----------------------------------------------------------------- THE END...