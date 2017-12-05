select	SALES_HOUSE_IDENTIFIER
		,count(1)  as hits
from 	ODS_ATOMIC..ATOMIC_BARB_SPOTS_ODS 
where 	LOG_STATION_CODE_FOR_SPOT in (4787) 
and 	dateof_transmission > '2014-05-01' 
--and 	dateof_transmission between '2013-11-29' and '2014-01-06'
group 	by sales_house_identifier 
order 	by 1
