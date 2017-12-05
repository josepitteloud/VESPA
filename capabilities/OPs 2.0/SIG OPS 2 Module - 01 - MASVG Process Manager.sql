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

M01: MASVG Process Manager
        M01.0 - Initialising environment
		M01.1 - Housekeeping
        M01.2 - Identifying Pending Tasks
		M01.3 - Tasks Execution
        M01.4 - Returning results
		M01.5 - Setting Privileges

**Stats:

	-- running time: 1 sec...
	
--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M01.0 - Initialising environment
-----------------------------------

create or replace procedure sig_masvg_m01_process_manager
	@fresh_start bit = 0
as begin

	MESSAGE cast(now() as timestamp)||' | Begining M01.0 - Initialising environment' TO CLIENT

	-- Local Variables
	declare @thetask    varchar(50)
	declare @sql_       varchar(2000)
	declare @exe_status	integer
	declare @log_id		bigint
	declare @gtg_flag	bit
	declare	@Module_id	varchar(3)
	
	set @Module_id = 'M01'
	set @exe_status = -1
	
	-- Initialising the project...
	execute @exe_status = sig_masvg_m02_base_initialisation @log_id output ,@gtg_flag output
	
	execute citeam.logger_add_event @log_ID, 1, @Module_id || ' : EXE M02', @exe_status
	
	MESSAGE cast(now() as timestamp)||' | @ M01.0: Logger instantiation DONE' TO CLIENT
	
	MESSAGE cast(now() as timestamp)||' | @ M01.0: Initialisation DONE' TO CLIENT
	
	-- if all went well and we have enough data, lets carry on then...
	if (@exe_status = 0 and @gtg_flag = 1)
	begin
	
-----------------------	
-- M01.1 - Housekeeping
-----------------------

		MESSAGE cast(now() as timestamp)||' | Begining M01.1 - Housekeeping' TO CLIENT
		
		execute sig_masvg_m03_housekeeping @fresh_start
		
		MESSAGE cast(now() as timestamp)||' | @ M01.0: Housekeeping DONE' TO CLIENT
		
------------------------------------
-- M01.2 - Identifying Pending Tasks
------------------------------------
		
		MESSAGE cast(now() as timestamp)||' | Begining M01.2 - Identifying Pending Tasks' TO CLIENT

		while exists    (
							select 	first status
							from	m01_t1_process_manager 
							where	status = 0			--> Any tasks Pending?...
						)
		begin
		
			MESSAGE cast(now() as timestamp)||' | @ M01.2: Pending Tasks Found' TO CLIENT

			-- What task to execute?...
			select  @thetask = task
			from    m01_t1_process_manager
			where   sequencer = (
									select  min(sequencer)
									from    m01_t1_process_manager
									where   status = 0
								)
			
			MESSAGE cast(now() as timestamp)||' | @ M01.2: Task '||@thetask||' Pending' TO CLIENT
			
--------------------------
-- M01.3 - Tasks Execution		
--------------------------

			MESSAGE cast(now() as timestamp)||' | Begining M01.3 - Tasks Execution for: '||@thetask TO CLIENT
			
			set @exe_status = -1
			
			set @sql_ = 'execute @exe_status = '||@thetask
			execute (@sql_)
			
			if @exe_status = 0
			begin
				update	m01_t1_process_manager
				set		status = 1
				where	task = @thetask
				and		status = 0
				
				MESSAGE cast(now() as timestamp)||' | @ M01.3: '||@thetask||' DONE' TO CLIENT
				
				commit
			end
			else
			begin
				MESSAGE cast(now() as timestamp)||' | @ M01.3: '||@thetask||' FAILED('||@exe_status||')' TO CLIENT
				execute citeam.logger_add_event @log_ID, 1, @Module_id || ' : EXE M04 (ERROR) T-' ||@thetaks , @exe_status
				break
			end
			execute citeam.logger_add_event @log_ID, 1, @Module_id || ' : EXE M01 T-' ||@thetask , @exe_status
			
		end
	end
	else
	begin
		MESSAGE cast(now() as timestamp)||' | M01 Finished (insufficient records on viewing table to proceed)' TO CLIENT
		execute citeam.logger_add_event @log_ID, 1, @Module_id || ' : insufficient records on viewing table to proceed' , @exe_status
	end

----------------------------
-- M01.4 - Returning results
----------------------------

	MESSAGE cast(now() as timestamp)||' | M01 Finished' TO CLIENT
	commit
	
end;

-----------------------------
-- M01.5 - Setting Privileges
-----------------------------

commit;
grant execute on sig_masvg_m01_process_manager to vespa_group_low_security;
commit;