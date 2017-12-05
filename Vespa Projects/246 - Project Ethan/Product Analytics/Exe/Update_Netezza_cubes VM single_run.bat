@echo off
setlocal enableextensions enabledelayedexpansion

set /a "x = 0"

echo %x%
set /a "x = x + 1"

echo Check 1... Executing "4 - PA - Z_PA_EVENTS_FACT Build_prerun_check.SQL"
REM	Execute Netezza check query 1
"C:\\Program Files (x86)\\Aginity\\Aginity Workbench for PureData System for Analytics\\Aginity.NetezzaWorkbench.exe" --unattended --stdout "tmp_prerun_check_1.log" --description TEST --action exec --connstr "Provider=NZOLEDB;Data Source=10.137.15.3;User ID=bd_smi;Password=B1gData3DM;Initial Catalog=SYSTEM" --dbtype NetezzaOleDb --sqlfile "C:\\USers\\tanghoiy\\PA cubes\\4 - PA - Z_PA_EVENTS_FACT Build_prerun_check.SQL"

REM	Check output from test on whether we have enough data to proceed with "4 - PA - Z_PA_EVENTS_FACT.SQL"
for /F "tokens=1,2,3 delims=|" %%A in (tmp_prerun_check_1.log) do (
	echo %%A,%%B,%%C
	set str1=%%C
	)

if "!str1!" == "1"	(

	echo Check 1 satified. Executing "4 - PA - Z_PA_EVENTS_FACT Build.SQL"
	REM	Execute "4 - PA - Z_PA_EVENTS_FACT Build.SQL"
	"C:\\Program Files (x86)\\Aginity\\Aginity Workbench for PureData System for Analytics\\Aginity.NetezzaWorkbench.exe" --unattended --description TEST --action exec --connstr "Provider=NZOLEDB;Data Source=10.137.15.3;User ID=bd_smi;Password=B1gData3DM;Initial Catalog=SYSTEM" --dbtype NetezzaOleDb --sqlfile "C:\\Users\\tanghoiy\\PA cubes\\4 - PA - Z_PA_EVENTS_FACT Build.SQL"

	
	set /a "y = 0"
	:while2
		echo %x%,%y%
		set /a "y = y + 1"

		echo Check 2... Executing "2 - PA - HSLvL 2&3 Cube [NETEZZA] V3_prerun_check.SQL"
		REM	Execute Netezza check query 2
		"C:\\Program Files (x86)\\Aginity\\Aginity Workbench for PureData System for Analytics\\Aginity.NetezzaWorkbench.exe" --unattended --stdout "tmp_prerun_check_2.log" --description TEST --action exec --connstr "Provider=NZOLEDB;Data Source=10.137.15.3;User ID=bd_smi;Password=B1gData3DM;Initial Catalog=SYSTEM" --dbtype NetezzaOleDb --sqlfile "C:\\USers\\tanghoiy\\PA cubes\\2 - PA - HSLvL 2&3 Cube [NETEZZA] V3_prerun_check.SQL"

		REM	Check output from test on whether we have enough data to proceed with "4 - PA - Z_PA_EVENTS_FACT.SQL"
		for /F "tokens=1,2,3 delims=|" %%A in (tmp_prerun_check_2.log) do (
			echo %%A,%%B,%%C
			set str2=%%C
			)

		if "!str2!" == "1"	(
			echo Check 2 satified. Executing "2 - PA - HSLvL 2&3 Cube [NETEZZA] V3.SQL"
			REM	Execute "2 - PA - HSLvL 2&3 Cube [NETEZZA] V3.SQL"
			"C:\\Program Files (x86)\\Aginity\\Aginity Workbench for PureData System for Analytics\\Aginity.NetezzaWorkbench.exe" --unattended --description TEST --action exec --connstr "Provider=NZOLEDB;Data Source=10.137.15.3;User ID=bd_smi;Password=B1gData3DM;Initial Catalog=SYSTEM" --dbtype NetezzaOleDb --sqlfile "C:\\Users\\tanghoiy\\PA cubes\\2 - PA - HSLvL 2&3 Cube [NETEZZA] V3.SQL"

			REM	Exit loop and start on the next day's batch
			goto :while1
			)

		REM	Wait 5 minutes before trying again
		timeout /t 300

		goto :while2

	)


echo Exiting...


endlocal



