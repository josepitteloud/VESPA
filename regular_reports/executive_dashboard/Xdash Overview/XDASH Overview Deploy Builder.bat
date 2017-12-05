REM Simply execute the combined XDASH_OVERVIEW_DeploymentFile.sql file to deploy-redeploy all procedures.

copy	/Y	/B  									"Vespa XDASH Overview.sql" 		XDASH_OVERVIEW_DeploymentFile.sql
copy	/Y	/B  XDASH_OVERVIEW_DeploymentFile.sql+	"XDash Overview Variables.sql" 	XDASH_OVERVIEW_DeploymentFile.sql
copy	/Y	/B  XDASH_OVERVIEW_DeploymentFile.sql+	"panbal temp patch on VA.sql"	XDASH_OVERVIEW_DeploymentFile.sql