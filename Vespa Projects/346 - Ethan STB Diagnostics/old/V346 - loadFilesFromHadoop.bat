@Echo OFF

rem script to load daily files from hadoop
rem usage: <script> yyyymmdd username password, where yyyymmdd is the day of interest (for example script.bat 20150831), username and password are the credentials to the remote Olive server

SET dateToProcess=%1

set remoteUsername=%2

set remotePassword=%3

rem save current dir (we'll return here in the end
SET mypath=%~dp0

rem set swap directory: the files will be copied from hadoop to this directory on the local pc, and then from this to Olive
set tmpSwapDir="C:\temp"
rem set the path to the unzip program (files are in gz compressed format usually)
set zipPrg="C:\Leo\Programs\7zip\7za.exe"
rem path to winscp program for secure ftp
set scpPrg="C:\Program Files (x86)\WinSCP\WinSCP.com"
rem path of directory in Olive to store the data to be loaded
set olivePath="\\dcslopsketl01\psk_olive\clarityq\export\Decision_Sciences\Leonardo\Technical\data"

rem first let's use the credentials for the remote directory where we will be copying the files
echo issuing credentials to prepare end remote directory connection (directory in Olive)...
net use %olivePath%  %remotePassword%  /user:domain\%remoteUsername%


echo ...opening scp connection...

rem step 1 materialize the file in the server from the cluster
%scpPrg% /command ^
 "open scp://ler09:Leo123@ingest02.prod.bigdata.bskyb.com -rawsettings SendBuf=0 Compression=1 -timeout=999" ^
 "call hadoop fs -get /datasets/stb_diagnostics/pace/p="%dateToProcess% ^
 "get p=%dateToProcess% %tmpSwapDir%\" ^
 "rmdir p=%dateToProcess%" ^
 "exit"

rem working directory (it's where the compressed hourly files are)
set workDir="%tmpSwapDir%\p=%dateToProcess%"
 
cd %workDir%
 
echo ...unzipping files in local directory %workDir%...
 
 for %%f in (*.gz) do (
 %zipPrg%  x -aoa -y "%%f" "-o%workDir%\"
)

echo ...deleting gz files from %workDir%...

del /F /Q *.gz

echo ...copying directory into Olive: %olivePath% ...

xcopy %workDir% "%olivePath%\p=%dateToProcess%" /C /I /Y /E

rmdir %workDir% /S /Q

echo ...returning to directory where we started the journey: %mypath%

cd %mypath%
