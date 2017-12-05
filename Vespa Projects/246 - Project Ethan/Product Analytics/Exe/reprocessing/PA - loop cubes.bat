@echo on
setlocal enableextensions enabledelayedexpansion

set /a "x = 0"

:while1

	if %x% leq 100 (

		set /a "x = x + 1"

		echo %x%

		"C:\\Program Files (x86)\\Aginity\\Aginity Workbench for PureData System for Analytics\\Aginity.NetezzaWorkbench.exe" --unattended --description TEST --action exec --connstr "Provider=NZOLEDB;Data Source=10.137.15.3;User ID=tanghoiy;Password=1411_Pelican13;Initial Catalog=SYSTEM" --dbtype NetezzaOleDb --sqlfile "C:\Users\\tanghoiy\\SkyIQ\\Git_repository\\Vespa\\Vespa Projects\\246 - Project Ethan\\Product Analytics\\Exe\\reprocessing\\4 - PA - Z_PA_EVENTS_FACT Build REPROC.sql"

		"C:\\Program Files (x86)\\Aginity\\Aginity Workbench for PureData System for Analytics\\Aginity.NetezzaWorkbench.exe" --unattended --description TEST --action exec --connstr "Provider=NZOLEDB;Data Source=10.137.15.3;User ID=tanghoiy;Password=1411_Pelican13;Initial Catalog=SYSTEM" --dbtype NetezzaOleDb --sqlfile "C:\Users\\tanghoiy\\SkyIQ\\Git_repository\\Vespa\\Vespa Projects\\246 - Project Ethan\\Product Analytics\\Exe\\reprocessing\\2 - PA - HSLvL 2&3 Cube [NETEZZA] V3 REPROC.SQL"

		timeout /t 300

		goto :while1
	)


endlocal



