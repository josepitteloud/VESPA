select rank() over ( partition by subscriber_id
                                 ,EVENT_START_DATE_TIME_UTC
                                 ,EVENT_END_DATE_TIME_UTC
                                 ,type_of_viewing_event
                                 ,Duration
                         order by BROADCAST_START_DATE_TIME_UTC)
        as Program_Order
       ,pk_viewing_prog_instance_fact
       ,subscriber_id
       ,EVENT_START_DATE_TIME_UTC
       ,EVENT_END_DATE_TIME_UTC
       ,BROADCAST_START_DATE_TIME_UTC
       ,Duration
       ,channel_name
       ,account_number
       ,case
                        when type_of_viewing_event = 'Sky+ time-shifted viewing event'
            then case
                                        when dateformat(EVENT_START_DATE_TIME_UTC,'YYYY-MM-DD') = dateformat(BROADCAST_START_DATE_TIME_UTC,'YYYY-MM-DD')
                                        then case
                                                        when cast(datediff(second,BROADCAST_START_DATE_TIME_UTC,EVENT_END_DATE_TIME_UTC)/(60.0*60.0) as int) = 0
                                                        then 1
                                                        else 2
                                                        end
                                        else 3
                                        end
                        else 0
        end as Viewing_Type_Detailed
        ,case
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=1 then 'Sun'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=2 then 'Mon'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=3 then 'Tue'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=4 then 'Wed'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=5 then 'Thu'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=6 then 'Fri'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=7 then 'Sat'
        end as EVENT_START_DOW
         ,hour(EVENT_START_DATE_TIME_UTC) as EVENT_START_HOUR
         ,genre_description
         ,service_key
into VEA_07_13_Jan_Viewing_Events_Subscriber_Null
from sk_prod.vespa_events_all
where panel_id = 12
and EVENT_START_DATE_TIME_UTC   >= '2013-01-07 00:00:00'
and EVENT_START_DATE_TIME_UTC   <= '2013-01-13 23:59:59'
and type_of_viewing_event <> 'Non viewing event'
and subscriber_id is null
and Duration > 6
--42,061 Row(s) affected

-- Remove events that have second and later programmes
delete from VEA_07_13_Jan_Viewing_Events_Subscriber_Null where Program_Order > 1
--22,456 Row(s) affected


--Account Number Investigation

select rank() over ( partition by subscriber_id
                                 ,EVENT_START_DATE_TIME_UTC
                                 ,EVENT_END_DATE_TIME_UTC
                                 ,type_of_viewing_event
                                 ,Duration
                         order by BROADCAST_START_DATE_TIME_UTC)
        as Program_Order
       ,pk_viewing_prog_instance_fact
       ,subscriber_id
       ,EVENT_START_DATE_TIME_UTC
       ,EVENT_END_DATE_TIME_UTC
       ,BROADCAST_START_DATE_TIME_UTC
       ,Duration
       ,channel_name
       ,account_number
       ,case
                        when type_of_viewing_event = 'Sky+ time-shifted viewing event'
            then case
                                        when dateformat(EVENT_START_DATE_TIME_UTC,'YYYY-MM-DD') = dateformat(BROADCAST_START_DATE_TIME_UTC,'YYYY-MM-DD')
                                        then case
                                                        when cast(datediff(second,BROADCAST_START_DATE_TIME_UTC,EVENT_END_DATE_TIME_UTC)/(60.0*60.0) as int) = 0
                                                        then 1
                                                        else 2
                                                        end
                                        else 3
                                        end
                        else 0
        end as Viewing_Type_Detailed
        ,case
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=1 then 'Sun'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=2 then 'Mon'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=3 then 'Tue'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=4 then 'Wed'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=5 then 'Thu'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=6 then 'Fri'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=7 then 'Sat'
        end as EVENT_START_DOW
         ,hour(EVENT_START_DATE_TIME_UTC) as EVENT_START_HOUR
         ,genre_description
         ,service_key
into VEA_07_13_Jan_Viewing_Events_account_number_null
from sk_prod.vespa_events_all
where panel_id = 12
and EVENT_START_DATE_TIME_UTC   >= '2013-01-07 00:00:00'
and EVENT_START_DATE_TIME_UTC   <= '2013-01-13 23:59:59'
and type_of_viewing_event <> 'Non viewing event'
and account_number is null
and Duration > 6
--10,071,037 Row(s) affected

-- Remove events that have second and later programmes
delete from VEA_07_13_Jan_Viewing_Events_account_number_null where Program_Order > 1
--2,939,516 Row(s) affected

--Both

select rank() over ( partition by subscriber_id
                                 ,EVENT_START_DATE_TIME_UTC
                                 ,EVENT_END_DATE_TIME_UTC
                                 ,type_of_viewing_event
                                 ,Duration
                         order by BROADCAST_START_DATE_TIME_UTC)
        as Program_Order
       ,pk_viewing_prog_instance_fact
       ,subscriber_id
       ,EVENT_START_DATE_TIME_UTC
       ,EVENT_END_DATE_TIME_UTC
       ,BROADCAST_START_DATE_TIME_UTC
       ,Duration
       ,channel_name
       ,account_number
       ,case
                        when type_of_viewing_event = 'Sky+ time-shifted viewing event'
            then case
                                        when dateformat(EVENT_START_DATE_TIME_UTC,'YYYY-MM-DD') = dateformat(BROADCAST_START_DATE_TIME_UTC,'YYYY-MM-DD')
                                        then case
                                                        when cast(datediff(second,BROADCAST_START_DATE_TIME_UTC,EVENT_END_DATE_TIME_UTC)/(60.0*60.0) as int) = 0
                                                        then 1
                                                        else 2
                                                        end
                                        else 3
                                        end
                        else 0
        end as Viewing_Type_Detailed
        ,case
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=1 then 'Sun'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=2 then 'Mon'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=3 then 'Tue'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=4 then 'Wed'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=5 then 'Thu'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=6 then 'Fri'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=7 then 'Sat'
        end as EVENT_START_DOW
         ,hour(EVENT_START_DATE_TIME_UTC) as EVENT_START_HOUR
         ,genre_description
                 ,service_key
into VEA_07_13_Jan_Viewing_Events_Both_null
from sk_prod.vespa_events_all
where panel_id = 12
and EVENT_START_DATE_TIME_UTC   >= '2013-01-07 00:00:00'
and EVENT_START_DATE_TIME_UTC   <= '2013-01-13 23:59:59'
and type_of_viewing_event <> 'Non viewing event'
and subscriber_id is null
and account_number is null
and Duration > 6
--5,472 Row(s) affected

-- Remove events that have second and later programmes
delete from VEA_07_13_Jan_Viewing_Events_Both_null where Program_Order > 1
--2,815 Row(s) affected


