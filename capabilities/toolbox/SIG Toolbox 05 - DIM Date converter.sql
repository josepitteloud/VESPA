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

**Project Name: 					
**Analysts:							Jose Pitteloud		(jose.pitteloud@skyiq.co.uk)
**Lead(s):							Jose Loureda
**Stakeholder:						VESPA / Strategic Insight Team.

									
**Business Brief:

	To provide a date merger for DIM dates 

**Sections:

*/


CREATE FUNCTION dt_conv (
        dt BIGINT
    ,   tm INTEGER )
RETURNS DATETIME
BEGIN
    DECLARE dtm DATETIME;
    SET dtm = CAST(CAST(LEFT (CAST (dt AS VARCHAR), 8) AS DATE)||' '||CAST(SUBSTRING (CAST (tm AS VARCHAR),2,2)||':'||
                    SUBSTRING (CAST (tm AS VARCHAR),4,2)||':'||SUBSTRING (CAST (tm AS VARCHAR),6,2)  AS TIME) AS DATETIME);
    RETURN dtm;
END



commit;
grant execute on dt_conv to vespa_group_low_security;
commit;
