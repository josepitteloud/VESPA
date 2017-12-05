REM This is a simple cmd batch script that concatenates all of the H2I module into a single for simple deployment of all all procedures.
REM Simply execute the combined H2I_all_modules.sql file to deploy-redeploy all procedures.

copy	/Y	/B  					"H2I - M000 - Prevalidation module.sql" 							H2I_all_modules.sql
copy	/Y	/B  H2I_all_modules.sql+"H2I - M00 - Initialisation.sql" 									H2I_all_modules.sql
copy	/Y	/B  H2I_all_modules.sql+"H2I - M01 - process_manager.sql" 									H2I_all_modules.sql
copy	/Y	/B  H2I_all_modules.sql+"H2I - M02 - Housekeeping.sql" 										H2I_all_modules.sql
copy	/Y	/B  H2I_all_modules.sql+"H2I - M03 - barb_data_extraction.sql" 								H2I_all_modules.sql
copy	/Y	/B  H2I_all_modules.sql+"H2I - M04 - barb_data_preparation.sql" 							H2I_all_modules.sql
copy	/Y	/B  H2I_all_modules.sql+"H2I - M05 - barb_matrices_generation.sql" 							H2I_all_modules.sql
copy	/Y	/B  H2I_all_modules.sql+"H2I - M06 - DP_data_extraction.sql" 								H2I_all_modules.sql
copy	/Y	/B  H2I_all_modules.sql+"H2I - M07 - DP_data_preparation.sql" 								H2I_all_modules.sql
copy	/Y	/B  H2I_all_modules.sql+"H2I - M08 - Experian Data Preparation.sql" 						H2I_all_modules.sql
copy	/Y	/B  H2I_all_modules.sql+"H2I - M09 Session size assignment - 3rd approach.sql" 				H2I_all_modules.sql
copy	/Y	/B  H2I_all_modules.sql+"H2I - M10 - individuals_selection.sql" 							H2I_all_modules.sql
copy	/Y	/B  H2I_all_modules.sql+"H2I - M10b - M10_output_validation.sql" 							H2I_all_modules.sql
REM copy	/Y	/B  H2I_all_modules.sql+"H2I - M11 - Individual Scaling.sql" 								H2I_all_modules.sql
copy	/Y	/B  H2I_all_modules.sql+"H2I - M11 - Individual Scaling - Exclude Adsmart No Consent.sql" 	H2I_all_modules.sql
copy	/Y	/B  H2I_all_modules.sql+"H2I - M12 - Validation.sql" 										H2I_all_modules.sql
copy	/Y	/B  H2I_all_modules.sql+"H2I - save_tables.sql" 											H2I_all_modules.sql