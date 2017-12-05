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

**Project Name: 					ADSMARTABLE UNIVERSE
**Analysts:							Angel Donnarumma 	(angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):							
**Stakeholder:						VESPA

									
**Business Brief:

	A tool to extract a single day from the Viewing Tables, without having to worry about the names of the tables...
	
	Only applies for Phase 3 of the DP Viewing Data Structures

**Sections:

	A: DATE PARTS IDENTIFYCATION
		
		A01: Checking Input
		A02: Composing Table Name
	
	B: DP DATA EXTRACTION
	
		B01: Assembling query
		B02: Extracting Data
		
	C: RETURNING RESULTS
	
		C01: Listing DP active Accounts that have reported at least 1 day amongst last 30 days
		
--------------------------------------------------------------------------------------------------------------------------------------------
USEFUL NOTE:	each building block can be treated as a stand alone unit, hence it is possible to copy/paste the logic and generate a table
				out of any of them if needed/required...
--------------------------------------------------------------------------------------------------------------------------------------------	

*/

create or replace procedure vespa_toolbox_03_dpdataextraction
	@Event_date date = null
as begin
    
-- Initialisation
	declare @dp_tname 	varchar(50)
	declare @query		varchar(3000)
	declare @from_dt	integer
	declare @to_dt		integer
	
	set @dp_tname = 'SK_PROD.VESPA_DP_PROG_VIEWED_'
	select  @from_dt 	= cast((dateformat(@Event_date,'YYYYMMDD')||'00') as integer)
	select  @to_dt 		= cast((dateformat(@Event_date,'YYYYMMDD')||'23') as integer)
	
----------------------
-- A01: Checking Input
----------------------

	if @Event_date is null
	begin
		MESSAGE cast(now() as timestamp)||' | Toolbox 03 - DP Data Extraction: You need to provide a Date for extraction !!!' TO CLIENT
	end
	else
	begin
	
----------------------------
-- A02: Composing Table Name
----------------------------

		set @dp_tname = @dp_tname||datepart(year,@Event_date)||right(('00'||cast(datepart(month,@event_date) as varchar(2))),2) 
        
------------------------
-- B01: Assembling query
------------------------

		set @query = 'Select * into #temp_ from '||@dp_tname||' where dk_event_start_datehour_dim between '||@from_dt||' and '||@to_dt
	    --select @query
-----------------------
-- B02: Extracting Data
-----------------------

		execute (@query)
        select * from #temp_
        
    end
	
end;

commit;
grant execute on vespa_toolbox_03_dpdataextraction to vespa_group_low_security;
commit;