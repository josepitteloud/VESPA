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

Module responsible for extracting a history of the latest 5 weeks from Scaling results to display trends on
the Vespa Panel Balance for Executive Distribution, aggregating figures to weekly basis (Average of metrics)...

**Module's Sections:

M10: Vespa Panel Balance Extractor
		M10.0 - Initialising environment
		M10.1 - Deriving Metric(s)
		M10.2 - QAing results
		M10.3 - Returning results
--------------------------------------------------------------------------------------------------------------
*/

--------------------------------------
/* M10.0 - Initialising environment */
--------------------------------------
create or replace procedure vespa_sp_xdash_m10_vespaPanelBalanceHist
as begin

	declare @maxdate date

	select  @maxdate = max(weekending)
	from    vespa_analysts.Vespa_PanMan_hist_trafficlight

--------------------------------
/* M10.1 - Deriving Metric(s) */
--------------------------------

	select  weekending
			,avg(daily_panel)
	from    vespa_analysts.Vespa_PanMan_hist_trafficlight
	where   weekending >= (@maxdate - 30)
	group   by  weekending
	order   by  weekending desc

---------------------------
/* M10.2 - QAing results */
---------------------------
-- NIP...

-------------------------------
/* M10.3 - Returning results */
-------------------------------
-- NIP...


end;

commit;
------------------------------------------------------ THE END...
