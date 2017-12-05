/* --------------------------------------------------------------------------------------------------------------
**Project Name:                                                 APOC Skyview H2I
**Analysts:                             Angel Donnarumma        (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson          (Jason.Thompson@skyiq.co.uk)
										,Hoi Yu Tang            (HoiYu.Tang@skyiq.co.uk)
										,Jose Pitteloud         (jose.pitteloud@skyiq.co.uk)
**Stakeholder:                          Sky IDS
                                        ,Jose Loureda           (Jose.Loureda@skyiq.co.uk)
**Project Code (Insight Collation):     v289
**Sharepoint Folder:    
        http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FSIGEvolved%2FShared%20Documents%2F01%20Analysis%20Requests%2FV289%20-%20Skyview%20Futures%2F01%20Plans%20Briefs%20and%20Project%20Admin                                                        
                                                                      
**Business Brief:

        This Module wraps the M11 Individual scaling procedure execution 

**Module:
        
        M11b: Scaling wrapper procedure:
		
			-	M11b.0 - Initialising Environment
			-	M11b.2 - Preparing Variables
        
--------------------------------------------------------------------------------------------------------------
***/


-----------------------------------
-- M11b.0 - Initialising Environment
-----------------------------------

CREATE OR REPLACE PROCEDURE ${SQLFILE_ARG001}.v289_m11b_Scaling_Wrapper
        @processing_date date = NULL
AS BEGIN

        MESSAGE cast(now() as timestamp)||' | Begining M11b.0 - Initialising Environment' TO CLIENT
        
				
		DECLARE @sql1 			VARCHAR(2000)
		DECLARE @exe_status 	INT
		DECLARE @thursday 		DATE 
		
		-------------------------------------
		-- M11b.2 - EXECUTING Procedures
		-------------------------------------

		SELECT  @thursday = DATEFORMAT((@processing_date - DATEPART(weekday, @processing_date))-2, 'YYYY-MM-DD')

		SET @exe_status = -1
		
		MESSAGE cast(now() as timestamp)||' | @ M11b.2: Executing ->  @exe_status = V289_M11_01_SC3_v1_1__do_weekly_segmentation '||@thursday||', 9999 , '||DATE(GETDATE()) TO CLIENT
		
		EXECUTE @exe_status = V289_M11_01_SC3_v1_1__do_weekly_segmentation 		@thursday, 	9999, 	GETDATE() 				-- thurs, logid, batch date 
		
		IF @exe_status = 0 
			MESSAGE cast(now() as timestamp)||' | @ M11b.2: Weekly_segmentation proc DONE' TO CLIENT
		ELSE 
			BEGIN
			MESSAGE cast(now() as timestamp)||' | @ M11b.2: Weekly_segmentation proc FAILED!!! CODE:'||@exe_status TO CLIENT
			GOTO FATALITY
			END
		
		SET @exe_status = -1
		
		MESSAGE cast(now() as timestamp)||' | @ M11b.2: Executing ->  @exe_status = V289_M11_02_SC3_v1_1__prepare_panel_members '||@thursday||', '||@processing_date||', '||DATE(GETDATE())||', 9999' TO CLIENT
		EXECUTE @exe_status = V289_M11_02_SC3_v1_1__prepare_panel_members 		@thursday, @processing_date, GETDATE(), 9999 	-- thurs, scaling, batch date, logid			

		IF @exe_status = 0 
			MESSAGE cast(now() as timestamp)||' | @ M11b.2: Prepare_panel_members proc DONE' TO CLIENT
		ELSE
			BEGIN
			MESSAGE cast(now() as timestamp)||' | @ M11b.2: Prepare_panel_members proc FAILED!!! CODE:'||@exe_status TO CLIENT
			GOTO FATALITY
			END

			
		SET @exe_status = -1
		MESSAGE cast(now() as timestamp)||' | @ M11b.2: Executing ->  @exe_status = V289_M11_03_SC3I_v1_1__add_individual_data '||@thursday||', '||DATE(GETDATE())||', 9999'  TO CLIENT
		EXECUTE @exe_status =  V289_M11_03_SC3I_v1_1__add_individual_data 		@thursday, GETDATE(), 9999						-- Thur, Batch logid

		IF @exe_status = 0 
			MESSAGE cast(now() as timestamp)||' | @ M11b.2: Add_individual_data proc DONE' TO CLIENT
		ELSE 
			BEGIN
			MESSAGE cast(now() as timestamp)||' | @ M11b.2: Add_individual_data proc FAILED!!! CODE:'||@exe_status TO CLIENT
			GOTO FATALITY
			END
			
		SET @exe_status = -1
		
		MESSAGE cast(now() as timestamp)||' | @ M11b.2: Executing ->  @exe_status = V289_M11_04_SC3I_v1_1__make_weights_BARB '||@thursday||', '||@processing_date||', '||DATE(GETDATE())||', 9999' TO CLIENT
		EXECUTE @exe_status =  V289_M11_04_SC3I_v1_1__make_weights_BARB 		@thursday, @processing_date, GETDATE(), 9999  	-- Thur, Scaling, Batch Date, logid
		
		IF @exe_status = 0 
			MESSAGE cast(now() as timestamp)||' | @ M11b.2: Add_individual_data proc DONE' TO CLIENT
		ELSE 
			
			MESSAGE cast(now() as timestamp)||' | @ M11b.2: Add_individual_data proc FAILED!!! CODE:'||@exe_status TO CLIENT
			
		FATALITY:			
		
		MESSAGE cast(now() as timestamp)||' | M11b Finished' TO CLIENT
		
				
END;
GO
COMMIT;
GRANT EXECUTE ON v289_m05_barb_Matrices_generation to vespa_group_low_security;
COMMIT;





















