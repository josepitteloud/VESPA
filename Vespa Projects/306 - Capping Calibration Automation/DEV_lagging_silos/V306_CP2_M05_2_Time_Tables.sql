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

-----------------------------------------------------------------------------------

**Project Name:                         Capping Calibration Automation
**Analysts:                             Leonardo Ripoli  (Leonardo.Ripoli@sky.uk)
                                        Jonathan Green   (Jonathan.Green2@sky.uk)

**Lead(s):                              Hoi Yu Tang (hoiyu.tang@sky.uk)
**Stakeholder:                          Jose Loureda
**Project Code (Insight Collation):     V306
**SharePoint Folder:                    http://sp-department.bskyb.com/sites/IQSKY/SIG/Insight%20Collation%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FIQSKY%2FSIG%2FInsight%20Collation%20Documents%2F01%20Analysis%20Requests%2FV306%20-%20Foundation%20-%20Platform%20Maintenance%2FPhase%202%2FCapping%20Calibration%20Automation

**Business Brief:

Capping Calibration Automation ultimately aims to deliver improved Vespa viewing consumption through monthly alignment to BARB reference data.
The Capping algorithm was developed in order to truncate the length of viewing events where there is no actual audience present (e.g. TV set off, but STB on and registering viewing).

Up to this point, the parameters and thresholds that feed into the Capping algorithm have been largely static since the time of the original algorithm development by IQ and ultimate implementation within EDM.
Although a recent capping calibration exercise addressed exactly the issue realigning Vespa viewing to BARB, this was a highly manual process that required much resource to construct and perform the analyses and assessment.
Capping Calibration Automation will adopt those calculations and fundamental approach, but delivered as a self-contained and automated process that calculates the best set of capping thresholds and parameters in IQ/Olive
for ingest within into the EDM production environment

This project will also aim to increase the precision over which the Capping thresholds and parameters operate. For example, the current parameters are defined at the day-part level,
each of which spans a number of hours in the day. The intention is to explore the possibility of redefining the parameters at the hourly level in order to give greater control over the alignment process against BARB.
In theory, there should be little to no adjustment required to the actual flow of the Capping algorithm since the thresholds and parameters are contained in an external lookup table rather than being hard-coded in the SQL.




**Module:                               V306_CP2_M05_2_Time_Tables

Creation of the time base used by BARB and VESPA modules.

*/

create or replace procedure V306_CP2_M05_2_Time_Tables
											@capping_date	date = NULL
as begin

	execute M00_2_output_to_logger '@ M05 : V306_CP2_M05_2_Time_Tables'
	COMMIT



	----------------------------------------------------------------
	-- Create minute-by-minute vector for a single day
	----------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : Create minute-by-minute vector for a single day'
	COMMIT

	SELECT	ROW_NUM
	INTO	#MINUTES_VECTOR
	FROM	SA_ROWGENERATOR(0,1439)
	commit

	CREATE UNIQUE LF INDEX U_LF_IDX_1 ON #MINUTES_VECTOR(ROW_NUM)
	commit



	----------------------------------------------------------------
	-- Create minute-by-minute time base for date of interest
	----------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : Create minute-by-minute time base for date of interest'
	COMMIT

	execute DROP_LOCAL_TABLE 'UTC'
	commit

	SELECT
			CAL.UTC_DAY_DATE
		,   DATEADD(MINUTE,MINS.ROW_NUM,CAST(CAL.UTC_DAY_DATE AS TIMESTAMP))   AS  UTC_DATEHOURMIN
		,   DATEADD	(
							HOUR
						,   DATEPART(HOUR,UTC_DATEHOURMIN)
						,   CAST(DATE(UTC_DATEHOURMIN) AS TIMESTAMP)
					)                                                       AS  UTC_DATEHOUR
	INTO    UTC
	FROM
					sk_prod.VIQ_DATE	AS  CAL
		CROSS JOIN	#MINUTES_VECTOR		AS  MINS
		INNER JOIN	V306_CAPPING_DATES	AS	DAT		ON	CAL.UTC_DAY_DATE	=	DAT.capping_date
	GROUP BY
			UTC_DAY_DATE
		,   UTC_DATEHOURMIN
		,   UTC_DATEHOURMIN
	ORDER BY
			UTC_DAY_DATE
		,   UTC_DATEHOURMIN
		,   UTC_DATEHOURMIN
	COMMIT

	CREATE DATE INDEX DATE_IDX_1 ON UTC(UTC_DAY_DATE)	COMMIT
	CREATE DTTM INDEX DTTM_IDX_1 ON UTC(UTC_DATEHOURMIN)	COMMIT
	CREATE DTTM INDEX DTTM_IDX_2 ON UTC(UTC_DATEHOUR)	COMMIT

end;
commit;

grant execute on V306_CP2_M05_2_Time_Tables to vespa_group_low_security;
commit;
