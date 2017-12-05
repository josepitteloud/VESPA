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
**Project Name:							OPS 2.0
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jose Loureda
**Stakeholder:                          Operational Reports / SIG
**Due Date:                             20/09/2013
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:

**Modules:

M02: MASVG Base Initialisation
		M02.0 - Initialize Variables
		M02.1 - Setting up the logger
		M02.2 - Verifying data completeness
		M02.3 - Returning Results
		M02.4 - Setting Privileges

**Stats:

	-- running time: 1 sec...
	
--------------------------------------------------------------------------------------------------------------
*/

-------------------------------
-- M02.0 - Initialize Variables
-------------------------------

create or replace procedure sig_masvg_m02_base_initialisation
	@log_id		bigint 	output
	,@gtg_flag 	bit 	output
as begin

	MESSAGE cast(now() as timestamp)||' | Begining M02.0 - Initialising environment' TO CLIENT
	
	-- Local Variables...
	declare @logbatch_id	varchar(20)
	declare @logrefres_id	varchar(40)
	declare @Module_id		varchar(3)
	declare @from_dt    	integer
	declare @to_dt      	integer
	
	set	@Module_id = 'M02'
	
	MESSAGE cast(now() as timestamp)||' | @ M02.0: Initialisation DONE' TO CLIENT
	
--------------------------------
-- M02.1 - Setting up the logger
--------------------------------

	MESSAGE cast(now() as timestamp)||' | Begining M02.1 - Setting up the logger' TO CLIENT
	
	-- Now automatically detecting if it's a test build and logging appropriately...
	
	if lower(user) = 'vespa_analysts'
		set @logbatch_id = 'OPS_2'
	else
		set @logbatch_id = 'OPS_2 test ' || upper(right(user,1)) || upper(left(user,2))

	set @logrefres_id = convert(varchar(10),today(),123) || ' OPS2 refresh'
	
	execute citeam.logger_create_run @logbatch_id, @logrefres_id, @log_ID output

	execute citeam.logger_add_event @log_ID, 3, @Module_id || ' : Log initialised'
	set @log_ID = 18
	MESSAGE cast(now() as timestamp)||' | @ M02.0: Logger instantiation DONE' TO CLIENT
	
--------------------------------------
-- M02.2 - Verifying data completeness
--------------------------------------

	MESSAGE cast(now() as timestamp)||' | Begining M02.2 - Verifying data completeness' TO CLIENT

	select  @from_dt =  cast    (
									(
										cast   (
													(   
														dateformat (
																		(
																			case   when datepart(weekday,now()) = 7
																					then now()
																					else (now() - datepart(weekday,now()))
																			end
																		)
																		,'YYYYMMDD'
																	)
													) 	as varchar(10)
												)
										||'00'
									) as integer
								)


	set @to_dt = @from_dt + 23

	select @gtg_flag = 	case	when	(
											select  count(1) as hits
											from    sk_prod.vespa_dp_prog_viewed_current
											where   dk_event_start_datehour_dim between @from_dt and @to_dt
										)
										> 1000000 -- this should be more... up to 10m 
										then 1 
										else 0
						end
	
	MESSAGE cast(now() as timestamp)||' | @ M02.2: Data Verification DONE' TO CLIENT
	
----------------------------
-- M02.3 - Returning Results
----------------------------
	
	commit
	MESSAGE cast(now() as timestamp)||' | M02 Finished' TO CLIENT
	
	
end;

-----------------------------
-- M02.4 - Setting Privileges
-----------------------------

commit;
grant execute on sig_masvg_m02_base_initialisation to vespa_group_low_security;
commit;
----------------------------------------------------------------- THE END...