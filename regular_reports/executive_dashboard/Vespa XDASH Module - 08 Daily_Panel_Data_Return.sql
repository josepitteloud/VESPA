 /*
--------------------------------------------------------------------------------------------------------------
**Project Name:                                         Vespa Executive Dashboard
**Analysts:                                                     Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                                                      Jose Loureda
**Stakeholder:                                          Vespa Directors / Managers.
**Due Date:                                                     22/02/2013
**Project Code (Insight Collation):     
**Sharepoint Folder:                            http://rtci/Sky%20Projects/Forms/AllItems.aspx?RootFolder=%2fSky%20Projects%2fVespa%2fRegular%20reports
                                                                        %2fMeta%2fExecutive%20Dashboard&FolderCTID=&View={95B15B22-959B-4B62-809A-AD43E02001BD}
                                                                        
**Business Brief:

Module responsible for extracting a history of the latest 5 weeks of count for accounts returning data to
to the Vespa Panel...

**Module's Sections:

M08: Daily Panel Data Return Extractor
                M08.0 - Initialising environment
                M08.1 - Deriving Metric(s)
                M08.2 - QAing results
                M08.3 - Returning results
--------------------------------------------------------------------------------------------------------------
*/

--------------------------------------
/* M08.0 - Initialising environment */
--------------------------------------

create or replace procedure vespa_sp_xdash_m08_dpdreturnextract
as begin

--------------------------------
/* M08.1 - Deriving Metric(s) */
--------------------------------

        select  Top 5
				xdashbase.weekending
				,left(calendar.subs_week_and_year,4) || '-' || right(calendar.subs_week_and_year,2)       as Sky_week
				,panmanbase.reliably_returning
				,xdashbase.returning_data
				,panmanbase.enabled
        from    (
					select  xdashhist.weekending
									,count(1) as returning_data
					from    vespa_analysts.vespa_xdash_hist_acinteractionkpi as xdashhist
					where   xdashhist.panel_id in (12,11)
					group   by  xdashhist.weekending
				) as xdashbase
				left join  (
								select  panmanhist.weekending
										,min(case when panmanhist.concept = 'Panel ac report ok'    then coalesce(panmanhist.daily_panel,0) end) as Reliably_returning
										,min(case when panmanhist.concept = 'ac population'         then coalesce(panmanhist.daily_panel,0) end) as Enabled
								from    vespa_analysts.vespa_panman_hist_summary as panmanhist
								group   by  panmanhist.weekending
							) as panmanbase
				on  xdashbase.weekending = panmanbase.weekending
				inner join  sk_prod.sky_calendar as calendar
				on  xdashbase.weekending = calendar.calendar_date
        order   by  Sky_week    desc

---------------------------
/* M08.2 - QAing results */
---------------------------
-- NIP...

-------------------------------
/* M08.3 - Returning results */
-------------------------------
-- NIP...

end;

commit;
---------------------------------------------------- THE END...
