REM Simply execute the combined OPs2_deployment_file.sql file to deploy-redeploy all procedures.

copy	/Y	/B  							"SIG OPS 2 Meta - 00 - MASVG Create Transient Tables.sql" 									OPs2_deployment_file.sql
copy	/Y	/B  OPs2_deployment_file.sql+	"SIG OPS 2 Module - 01 - MASVG Process Manager.sql" 										OPs2_deployment_file.sql
copy	/Y	/B  OPs2_deployment_file.sql+	"SIG OPS 2 Module - 02 - MASVG Base Initialisation.sql" 									OPs2_deployment_file.sql
copy	/Y	/B  OPs2_deployment_file.sql+	"SIG OPS 2 Module - 03 - MASVG Housekeeping.sql" 											OPs2_deployment_file.sql
copy	/Y	/B  OPs2_deployment_file.sql+	"SIG OPS 2 Module - 04 - MASVG Panel Composition.sql" 										OPs2_deployment_file.sql
copy	/Y	/B  OPs2_deployment_file.sql+	"SIG OPS 2 Module - 05 - MASVG Panel Performance.sql" 										OPs2_deployment_file.sql
copy	/Y	/B  OPs2_deployment_file.sql+	"SIG OPS 2 Module - 06 - MASVG Panel Balance.sql" 											OPs2_deployment_file.sql
copy	/Y	/B  OPs2_deployment_file.sql+	"SIG OPS 2 Module - 07 - MASVG Box Base.sql" 												OPs2_deployment_file.sql
copy	/Y	/B  OPs2_deployment_file.sql+	"SIG OPS 2 Module - 08 - MASVG Account Base.sql" 											OPs2_deployment_file.sql
copy	/Y	/B  OPs2_deployment_file.sql+	"SIG OPS 2 Module - 09 - MASVG Weekly Box View Generation.sql" 								OPs2_deployment_file.sql
copy	/Y	/B  OPs2_deployment_file.sql+	"SIG OPS 2 Module - 10 - MASVG Weekly Account View Generation.sql" 							OPs2_deployment_file.sql
copy	/Y	/B  OPs2_deployment_file.sql+	"SIG OPS 2 Module - 11 - MASVG Panel Measurements.sql" 										OPs2_deployment_file.sql
