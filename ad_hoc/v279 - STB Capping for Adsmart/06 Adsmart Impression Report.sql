
exec STBCAP_adsmart_impressions_report



/* Report on adsmart Impressions */



IF object_id('STBCAP_adsmart_impressions_report') IS NOT NULL THEN DROP PROCEDURE STBCAP_adsmart_impressions_report END IF;

create procedure STBCAP_adsmart_impressions_report
as
BEGIN

----------------------------------------------------------------
-------- Adsmart Impressions Data Sourced From Netezza

--- Pre-capped slots
/*SELECT DTH_VIEWING_EVENT_ID, SCMS_SUBSCRIBER_ID, EVENT_START_DATETIME, EVENT_END_DATETIME, EVENT_ACTION
FROM DIS_REFERENCE..FINAL_DTH_VIEWING_EVENT_HISTORY
WHERE EVENT_ACTION IN ('AdSmart Substitution', 'AdSmart No Substitution')
AND PANEL_ID_REPORTED IN (11,12)
AND TX_DAY<4
AND date(EVENT_START_DATETIME) between '2014-04-14' and '2014-04-20'
AND DTH_VIEWING_EVENT_DATA_SOURCE='AMS Viewing Event'
AND LIVE_EVENT_FLAG = 1
*/

/* The above is imported into Olive.
In olive I have previuosly summarised the viewing data in the vespa_dp_prog_viewed_yyyymm tables
and applied the STB cap algorithm to each viewing event

I then match the impressions data from netezza to this viewing data to calculte whether the impression
happened before or after the STB cap and the Vespa cap.
*/


----- Summarise Adsmart impressions against the STB cap and Vespa Cap
/*
select
        EVENT_ACTION
        ,case when v.cap_end_time < v.event_end_time then 'Vespa Capped' else 'Vespa Not Capped' end as vespa_cap
        ,case when a.EVENT_START_DATETIME > dateadd(mi, stb_cap, v.event_start_time)
                                        then 'Ad After STB Cap' else 'Ad Before STB Cap' end as stb_ad_cap
        ,case when a.EVENT_START_DATETIME > v.cap_end_time
                                        then 'Ad After Vespa Cap' else 'Ad Before Vespa Cap' end as vespa_ad_cap
        ,case   when stb_ad_cap = 'Ad After STB Cap' and vespa_ad_cap = 'Ad After Vespa Cap' then 'Save'
                when stb_ad_cap = 'Ad After STB Cap' and vespa_ad_cap = 'Ad Before Cap' then 'Aggressive'
                when stb_ad_cap = 'Ad Before STB Cap' and vespa_ad_cap = 'Ad After Cap' then 'Served Error'
                when stb_ad_cap = 'Ad Before STB Cap' and vespa_ad_cap = 'Ad Before Cap' then 'OK Serve'
        end as impression_status
        ,count(1)
        ,sum(scaling_weight)
from
        STBCAP_Netezza_Adsmart_Slots a -- impression data imported from Netezza
        inner join STBCAP_viewing_data v -- my summarised viewing data
                on a.SCMS_SUBSCRIBER_ID = v.subscriber_id
where   a.EVENT_START_DATETIME >= v.event_start_time   -- which viewing event does the impression happen. can the pull off associated STB and Vespa cap
        and a.EVENT_START_DATETIME < v.event_end_time
        and a.EVENT_END_DATETIME > v.event_start_time
        and a.EVENT_END_DATETIME <= v.event_end_time
group by
        EVENT_ACTION
        ,vespa_cap
        ,stb_ad_cap
        ,vespa_ad_cap
        ,impression_status

*/
------ Summary by Day/Time [Adsmart Substitutions Only, Based upon Programme Event Start Time]
/*
select
         date(v.event_start_time) as the_date
        ,case
                when hour(v.event_start_time) between 4 and 5 then '01 Early Morning'
                when hour(v.event_start_time) between 6 and 9 then '02 Breakfast'
                when hour(v.event_start_time) between 10 and 14 then '03 Mid Day'
                when hour(v.event_start_time) between 15 and 19 then '04 Early Evening'
                when hour(v.event_start_time) between 20 and 22 then '05 Prime Time'
                else '06 Late Evening'
        end as the_time
        ,case when v.cap_end_time < v.event_end_time then 'Vespa Capped' else 'Vespa Not Capped' end as vespa_cap
        ,case when a.EVENT_START_DATETIME > dateadd(mi, stb_cap, v.event_start_time)
                                        then 'Ad After STB Cap' else 'Ad Before STB Cap' end as stb_ad_cap
        ,case when a.EVENT_START_DATETIME > v.cap_end_time
                                        then 'Ad After Vespa Cap' else 'Ad Before Vespa Cap' end as vespa_ad_cap
        ,case   when stb_ad_cap = 'Ad After STB Cap' and vespa_ad_cap = 'Ad After Vespa Cap' then 'Save'
                when stb_ad_cap = 'Ad After STB Cap' and vespa_ad_cap = 'Ad Before Vespa Cap' then 'Aggressive'
                when stb_ad_cap = 'Ad Before STB Cap' and vespa_ad_cap = 'Ad After Vespa Cap' then 'Served Error'
                when stb_ad_cap = 'Ad Before STB Cap' and vespa_ad_cap = 'Ad Before Vespa Cap' then 'OK Serve'
        end as impression_status
        ,count(1)
        ,sum(scaling_weight)
from
        STBCAP_Netezza_Adsmart_Slots a -- impression data imported from Netezza
        inner join STBCAP_viewing_data v -- my summarised viewing data
                on a.SCMS_SUBSCRIBER_ID = v.subscriber_id
where   a.EVENT_START_DATETIME >= v.event_start_time   -- which viewing event does the impression happen. can the pull off associated STB and Vespa cap
        and a.EVENT_START_DATETIME < v.event_end_time
        and a.EVENT_END_DATETIME > v.event_start_time
        and a.EVENT_END_DATETIME <= v.event_end_time
        and EVENT_ACTION = 'AdSmart Substitution'
group by
        the_date
        ,the_time
        ,vespa_cap
        ,stb_ad_cap
        ,vespa_ad_cap
        ,impression_status

*/

------ Summary by Day/Time [Adsmart Substitutions Only, Based upon Adsmart Event Start Time]
/*
select
         date(a.event_start_datetime) as the_date
        ,case
                when hour(a.event_start_datetime) between 4 and 5 then '01 Early Morning'
                when hour(a.event_start_datetime) between 6 and 9 then '02 Breakfast'
                when hour(a.event_start_datetime) between 10 and 14 then '03 Mid Day'
                when hour(a.event_start_datetime) between 15 and 19 then '04 Early Evening'
                when hour(a.event_start_datetime) between 20 and 22 then '05 Prime Time'
                else '06 Late Evening'
        end as the_time
        ,case when v.cap_end_time < v.event_end_time then 'Vespa Capped' else 'Vespa Not Capped' end as vespa_cap
        ,case when a.EVENT_START_DATETIME > dateadd(mi, stb_cap, v.event_start_time)
                                        then 'Ad After STB Cap' else 'Ad Before STB Cap' end as stb_ad_cap
        ,case when a.EVENT_START_DATETIME > v.cap_end_time
                                        then 'Ad After Vespa Cap' else 'Ad Before Vespa Cap' end as vespa_ad_cap
        ,case   when stb_ad_cap = 'Ad After STB Cap' and vespa_ad_cap = 'Ad After Vespa Cap' then 'Save'
                when stb_ad_cap = 'Ad After STB Cap' and vespa_ad_cap = 'Ad Before Vespa Cap' then 'Aggressive'
                when stb_ad_cap = 'Ad Before STB Cap' and vespa_ad_cap = 'Ad After Vespa Cap' then 'Served Error'
                when stb_ad_cap = 'Ad Before STB Cap' and vespa_ad_cap = 'Ad Before Vespa Cap' then 'OK Serve'
        end as impression_status
        ,count(1)
        ,sum(scaling_weight)
from
        STBCAP_Netezza_Adsmart_Slots a -- impression data imported from Netezza
        inner join STBCAP_viewing_data v -- my summarised viewing data
                on a.SCMS_SUBSCRIBER_ID = v.subscriber_id
where   a.EVENT_START_DATETIME >= v.event_start_time   -- which viewing event does the impression happen. can the pull off associated STB and Vespa cap
        and a.EVENT_START_DATETIME < v.event_end_time
        and a.EVENT_END_DATETIME > v.event_start_time
        and a.EVENT_END_DATETIME <= v.event_end_time
        and EVENT_ACTION = 'AdSmart Substitution'
group by
        the_date
        ,the_time
        ,vespa_cap
        ,stb_ad_cap
        ,vespa_ad_cap
        ,impression_status

*/

------ Summary by day/time/Genre [Adsmart Substitutions Only, Based upon Adsmart Event Start Time, Genre of first programme instance]

select
         date(a.event_start_datetime) as the_date
        ,case
                when hour(a.event_start_datetime) between 4 and 5 then '01 Early Morning'
                when hour(a.event_start_datetime) between 6 and 9 then '02 Breakfast'
                when hour(a.event_start_datetime) between 10 and 14 then '03 Mid Day'
                when hour(a.event_start_datetime) between 15 and 19 then '04 Early Evening'
                when hour(a.event_start_datetime) between 20 and 22 then '05 Prime Time'
                else '06 Late Evening'
        end as the_time
        ,v.genre
        ,case when v.cap_end_time < v.event_end_time then 'Vespa Capped' else 'Vespa Not Capped' end as vespa_cap
        ,case when a.EVENT_START_DATETIME > dateadd(mi, stb_cap, v.event_start_time)
                                        then 'Ad After STB Cap' else 'Ad Before STB Cap' end as stb_ad_cap
        ,case when a.EVENT_START_DATETIME > v.cap_end_time
                                        then 'Ad After Vespa Cap' else 'Ad Before Vespa Cap' end as vespa_ad_cap
        ,case   when stb_ad_cap = 'Ad After STB Cap' and vespa_ad_cap = 'Ad After Vespa Cap' then 'Save'
                when stb_ad_cap = 'Ad After STB Cap' and vespa_ad_cap = 'Ad Before Vespa Cap' then 'Aggressive'
                when stb_ad_cap = 'Ad Before STB Cap' and vespa_ad_cap = 'Ad After Vespa Cap' then 'Served Error'
                when stb_ad_cap = 'Ad Before STB Cap' and vespa_ad_cap = 'Ad Before Vespa Cap' then 'OK Serve'
        end as impression_status
        ,count(1)
        ,sum(scaling_weight)
from
        STBCAP_Netezza_Adsmart_Slots a -- impression data imported from Netezza
        inner join STBCAP_viewing_data v -- my summarised viewing data
                on a.SCMS_SUBSCRIBER_ID = v.subscriber_id
where   a.EVENT_START_DATETIME >= v.event_start_time   -- which viewing event does the impression happen. can the pull off associated STB and Vespa cap
        and a.EVENT_START_DATETIME < v.event_end_time
        and a.EVENT_END_DATETIME > v.event_start_time
        and a.EVENT_END_DATETIME <= v.event_end_time
        and EVENT_ACTION = 'AdSmart Substitution'
group by
        the_date
        ,the_time
        ,v.genre
        ,vespa_cap
        ,stb_ad_cap
        ,vespa_ad_cap
        ,impression_status



------ Summary by day/time/Service Key [Adsmart Substitutions Only, Based upon Adsmart Event Start Time]

select
         date(a.event_start_datetime) as the_date
        ,case
                when hour(a.event_start_datetime) between 4 and 5 then '01 Early Morning'
                when hour(a.event_start_datetime) between 6 and 9 then '02 Breakfast'
                when hour(a.event_start_datetime) between 10 and 14 then '03 Mid Day'
                when hour(a.event_start_datetime) between 15 and 19 then '04 Early Evening'
                when hour(a.event_start_datetime) between 20 and 22 then '05 Prime Time'
                else '06 Late Evening'
        end as the_time
        ,v.service_key || ':' || ska.vespa_name as the_channel
        ,case when v.cap_end_time < v.event_end_time then 'Vespa Capped' else 'Vespa Not Capped' end as vespa_cap
        ,case when a.EVENT_START_DATETIME > dateadd(mi, stb_cap, v.event_start_time)
                                        then 'Ad After STB Cap' else 'Ad Before STB Cap' end as stb_ad_cap
        ,case when a.EVENT_START_DATETIME > v.cap_end_time
                                        then 'Ad After Vespa Cap' else 'Ad Before Vespa Cap' end as vespa_ad_cap
        ,case   when stb_ad_cap = 'Ad After STB Cap' and vespa_ad_cap = 'Ad After Vespa Cap' then 'Save'
                when stb_ad_cap = 'Ad After STB Cap' and vespa_ad_cap = 'Ad Before Vespa Cap' then 'Aggressive'
                when stb_ad_cap = 'Ad Before STB Cap' and vespa_ad_cap = 'Ad After Vespa Cap' then 'Served Error'
                when stb_ad_cap = 'Ad Before STB Cap' and vespa_ad_cap = 'Ad Before Vespa Cap' then 'OK Serve'
        end as impression_status
        ,count(1)
        ,sum(scaling_weight)
from
        STBCAP_Netezza_Adsmart_Slots a -- impression data imported from Netezza
        inner join STBCAP_viewing_data v -- my summarised viewing data
                on a.SCMS_SUBSCRIBER_ID = v.subscriber_id
        inner join vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES ska
                on v.service_key = ska.service_key
where   a.EVENT_START_DATETIME >= v.event_start_time   -- which viewing event does the impression happen. can the pull off associated STB and Vespa cap
        and a.EVENT_START_DATETIME < v.event_end_time
        and a.EVENT_END_DATETIME > v.event_start_time
        and a.EVENT_END_DATETIME <= v.event_end_time
        and EVENT_ACTION = 'AdSmart Substitution'
        and a.EVENT_START_DATETIME between ska.EFFECTIVE_FROM and ska.EFFECTIVE_TO
group by
        the_date
        ,the_time
        ,the_channel
        ,vespa_cap
        ,stb_ad_cap
        ,vespa_ad_cap
        ,impression_status





END
