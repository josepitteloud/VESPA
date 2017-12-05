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

M03: MASVG Panel Composition
        M03.0 - Initialising environment
        M03.1 - Cleaning Before Input (CBI)
		M03.2 - Cleaning After Output (CAO)
		M03.3 - Returning results
		M03.4 - Setting Privileges

--------------------------------------------------------------------------------------------------------------
*/
-----------------------------------
-- M03.0 - Initialising environment
-----------------------------------

create or replace procedure sig_masvg_m03_housekeeping
	@reset	bit	= 0
as begin

	MESSAGE cast(now() as timestamp)||' | Begining M03.0 - Initialising environment' TO CLIENT
	
	declare @done 		tinyint
	declare @alltasks 	tinyint
	
	select	@alltasks = count(1)
	from	m01_t1_process_manager
	
	select	@done = count(1)
	from	m01_t1_process_manager
	where	status = 1
	
	MESSAGE cast(now() as timestamp)||' | @ M03.0: Initialisation DONE' TO CLIENT
	
--------------------------------------
-- M03.1 - Cleaning Before Input (CBI)
--------------------------------------

	if	(@reset = 1 or @alltasks = @done)
		begin
			
			MESSAGE cast(now() as timestamp)||' | Begining M03.1 - Cleaning Before Imput (CBI)' TO CLIENT
			
			delete from m04_t1_panel_sample_stage0
			delete from	m05_t1_panel_performance_stage0
			delete from m06_t1_panel_balance_stage0
			delete from m07_t1_box_base_stage0
			delete from m08_t1_account_base_stage0
			delete from sig_current_non_scaling_segments
			
			MESSAGE cast(now() as timestamp)||' | @ M03.1: Trunkating output tables DONE' TO CLIENT
			commit
			
			Update 	m01_t1_process_manager
			set		status = 0
				
			MESSAGE cast(now() as timestamp)||' | @ M03.1: All tasks completed, Re-initialising DONE' TO CLIENT
			commit
			
		end


--------------------------------------
-- M03.2 - Cleaning After Output (CAO)
--------------------------------------
	--else
		--begin
		
			--MESSAGE cast(now() as timestamp)||' | Begining M03.0 - Cleaning After Output (CAO)' TO CLIENT
		
			--Update 	m01_t1_process_manager
			--set		status = 0
			
			--MESSAGE cast(now() as timestamp)||' | @ M03.0: Forcing Re-initialisation DONE' TO CLIENT
			--commit
			
		--end

	commit
		
----------------------------		
-- M03.3 - Returning results
----------------------------

	MESSAGE cast(now() as timestamp)||' | M03 Finished' TO CLIENT
	commit

end;

-----------------------------
-- M03.4 - Setting Privileges
-----------------------------

commit;
grant execute on sig_masvg_m03_housekeeping to vespa_group_low_security;
commit;