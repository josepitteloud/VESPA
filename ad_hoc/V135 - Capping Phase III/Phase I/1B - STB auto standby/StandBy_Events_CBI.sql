/*

Obtain all standby IN events ffrom Netezza production database (CBI)

*/

CREATE EXTERNAL TABLE 'C:\\StandByIN_Events_5_11_Nov.csv'
USING
(
                DELIMITER ','
                Y2BASE 2000
                ENCODING 'internal'
                REMOTESOURCE 'ODBC'
                ESCAPECHAR '\'
)
AS
select subscriberid,adjusted_event_start_time
from DIS_REFERENCE.DIS_ETL.NORMALIZED_VIEWING_EVENTS 
where panelid = 12 
and adjusted_event_start_time >= '2012-11-05 00:00:00'
and adjusted_event_start_time <= '2012-11-11 23:59:59'
and event_type = 'EVSTANDBYIN'
