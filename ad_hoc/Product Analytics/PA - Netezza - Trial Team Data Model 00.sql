/*
	------------------------------------------
	IDS
	Ethan Project - Product Analytics
	2015-09-07
	Angel Donnarumma (angel.donnarumma@sky.uk)
	------------------------------------------
	
	BUSINESS BRIEF:
	
		This tables need to be part of the current data model seating in Netezza (hosting PA data for UI interaction analysis)
		This approach is needed since the trial team needs to identify specifically who is using/ not using the STBs + 
		understanding the account (viewing_card) composition in terms of devices.
		
	SECTIONS:
	
		A0 - PA_TRIALTEAM_STB_MASTER
				
				This table host all STBs associated to a given viewing_card, granularity at 1 row per stb for each viewing_card.
				
		A1 - PA_TRIALTEAM_CE_MASTER

				This table host all CE devices and viewing_card level details for all accounts, granularity at 1 row per viewing_card.
			
	REF:
	
	Business Folder @ G:\RTCI\Sky Projects\Vespa\Measurements and Algorithms\Product Analytics\
	
			
*/

-------------------------------
-- A0 - PA_TRIALTEAM_STB_MASTER
-------------------------------

create table pa_trialteam_stb_master(

	viewing_card		integer
	,stb_owner			varchar(50)
	,stb_serial_number	varchar(17)
	,stb_type			varchar(20)
	,Active				boolean		default false
	,Home_office		varchar(6)	

);

commit;

------------------------------
-- A1 - PA_TRIALTEAM_CE_MASTER
------------------------------

create table pa_trialteam_ce_master(

	viewing_card	integer
	,stb_owner		varchar(50)
	,Active			boolean		default false
	,home_office	varchar(6)
	,isp			varchar(10)
	,ios0			integer
	,ios1			varchar(20)
	,ios2			varchar(20)
	,ios3			varchar(20)
	,ios4			varchar(20)
	,ios5			varchar(20)
	,ios52			varchar(20)
	,android0		integer
	,android		varchar(20)
	,android2		varchar(20)
	,viper			boolean		default false
	,ethan_ps_flag	boolean		default false


);

commit;


="insert into pa_trialteam_ce_master values("&A2&",'"&B2&"',"&C2&",'"&D2&"','"&E2&"',"&F2&",'"&G2&"','"&H2&"','"&I2&"','"&J2&"','"&K2&"','"&L2&"','"