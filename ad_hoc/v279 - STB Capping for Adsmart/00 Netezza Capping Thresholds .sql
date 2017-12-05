

--- Extract of Threshold history for Olive
---- The dates need to be for the week BEFORE the viewing data being assessed -----

select
        CAPPED_THRESHOLD_DATE
        ,extract(dow from CAPPED_THRESHOLD_DATE) as day_of_week
        ,CAPPED_THRESHOLD_HOUR
        ,CAPPED_THRESHOLD_CHANNEL_PACK
        ,CAPPED_THRESHOLD_BOX
        ,CAPPED_THRESHOLD_GENRE
        ,CAPPED_THRESHOLD_EVENT_DURATION
from
        DIS_PREPARE..CAPPED_THRESHOLD_DIM_PREPARE
where
        CAPPED_THRESHOLD_TYPE = 1 -- Live events
        and (CAPPED_THRESHOLD_DATE >= date('2014-01-27') and CAPPED_THRESHOLD_DATE <= date('2014-04-13'))
        
        


