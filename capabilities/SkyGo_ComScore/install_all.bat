REM This is a simple cmd batch script that concatenates all of the ComScore SkyGo procedures into a single file for deployment.
REM Simply execute the combined Vespa_ComScore_SkyGo_all_modules.sql file to deploy-redeploy all procedures.


logger

copy	/Y	/B					     "V239_SkyGoComscore_0-1_UniverseBuild__LOCAL.sql"			Vespa_ComScore_SkyGo_all_modules.sql
copy	/Y	/B	Vespa_ComScore_SkyGo_all_modules.sql+"V239_SkyGoComscore_1_EnvSetup__LOCAL.sql"				Vespa_ComScore_SkyGo_all_modules.sql
copy	/Y	/B	Vespa_ComScore_SkyGo_all_modules.sql+"V239_SkyGoComscore_2_ChannelMap_refresh__LOCAL.sql"		Vespa_ComScore_SkyGo_all_modules.sql
copy	/Y	/B	Vespa_ComScore_SkyGo_all_modules.sql+"V239_SkyGoComscore_2_ChannelMap_manual_mappings__LOCAL.sql"	Vespa_ComScore_SkyGo_all_modules.sql
copy	/Y	/B	Vespa_ComScore_SkyGo_all_modules.sql+"V239_SkyGoComscore_2_ChannelMap_RunBuild__LOCAL.sql"		Vespa_ComScore_SkyGo_all_modules.sql
copy	/Y	/B	Vespa_ComScore_SkyGo_all_modules.sql+"V239_SkyGoComscore_3_LinearBuild__LOCAL.sql"			Vespa_ComScore_SkyGo_all_modules.sql
copy	/Y	/B	Vespa_ComScore_SkyGo_all_modules.sql+"V239_SkyGoComscore_4_AggrView__LOCAL.sql"				Vespa_ComScore_SkyGo_all_modules.sql
copy	/Y	/B  	Vespa_ComScore_SkyGo_all_modules.sql+"V239_SkyGoComscore_0-0-1_RunBuild__LOCAL.sql"			Vespa_ComScore_SkyGo_all_modules.sql

#start "" "C:\Program Files (x86)\Synametrics Technologies\WinSQL\Winsql.exe" -g CP2_Calibration_all_modules.sql