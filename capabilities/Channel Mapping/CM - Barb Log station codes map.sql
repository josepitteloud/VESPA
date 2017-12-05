
-- Channel Mapping Log Station code map against service keys
select  ska.*
        ,max(s1)    as log_option_1
		,max(s2)	as log_option_2
        ,max(s3)	as log_option_3
        ,max(s4)	as log_option_4
        ,max(s5)	as log_option_5
        ,max(s6)	as log_option_6
        ,max(s7)	as log_option_7
        ,max(s8)	as log_option_8
        ,max(s9)	as log_option_9
        ,max(s10)	as log_option_10
from    (
            select  distinct
                    base.service_key
                    ,case when base.seq = 1 then base.log_station_code else null end	as s1
            		-- ,case when base.seq = 2 then base.log_station_code else null end	as s2
            		,case when base.seq = 3 then base.log_station_code else null end	as s3
            		,case when base.seq = 4 then base.log_station_code else null end	as s4
            		,case when base.seq = 5 then base.log_station_code else null end	as s5
            		,case when base.seq = 6 then base.log_station_code else null end	as s6
            		,case when base.seq = 7 then base.log_station_code else null end	as s7
            		,case when base.seq = 8 then base.log_station_code else null end	as s8
            		,case when base.seq = 9 then base.log_station_code else null end	as s9
            		,case when base.seq = 10 then base.log_station_code else null end	as s10	
            from    (   
                        select  service_key
                                ,log_station_code
                                ,dense_rank() over  (
                                                        partition by    service_key
                                                        order by        log_station_code
                                                    )   as seq
                        from    vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_BARB
                        where   effective_to = '2999-12-31 00:00:00.000'
                        and     service_key is not null
                    )   as base
                    inner join  (
                                    select  service_key
                                            ,log_station_code
                                            ,dense_rank() over  (
                                                                    partition by    service_key
                                                                    order by        log_station_code
                                                                )   as seq
                                    from    vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_BARB
                                    where   effective_to = '2999-12-31 00:00:00.000'
                                    and     service_key is not null
                                )   as mirror_
                    on  base.service_key = mirror_.service_key
        )   as slicing
        inner join  (
                        select  service_key
                                ,vespa_name
                                ,full_name
                                ,channel_name
                        from    vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_attributes
                        where   activex = 'Y'
                    )   as ska
        on  slicing.service_key = ska.service_key
group   by  ska.service_key
            ,ska.vespa_name
            ,ska.full_name
            ,ska.channel_name
			
			
			
-- Channel Mapping Log Station code map against service keys (VERSION 2 part 1)
/*
	proceeding with the following query will retrieve the active snapshot
	of SKA-SKB Maps
*/
create or replace view cm_stuff as
select  ska.*
        ,skb.*
from    (
			select  service_key
					,vespa_name
					,epg_name
					,full_name
					,channel_name
					,BARB_REPORTED
					,ACTIVEx
					,CHANNEL_OWNER
					,FORMAT
					,PARENT_SERVICE_KEY
					,TIMESHIFT_STATUS
					,TIMESHIFT_MINUTES
					,TYPE_ID
					,PRIMARY_SALES_HOUSE
					,CHANNEL_GROUP
					,retail
			from    vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_attributes
			where   activex = 'Y'
			--and		lower(trim(channel_reach)) <> 'roi'
		)   as ska
        left join	(
						select  distinct
								log_station_code
								,case when map.log_station is null then 1 else 0 end    as new_flag
								,service_key as sk_ref
								,STI_CODE
								,PANEL_CODE
								,PROMO_PANEL_CODE
						from    vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_BARB    as skb
								left join log_codes as map
								on  map.string_search   like '%'||cast(skb.service_key as varchar)||'%'
								and map.log_station     = skb.log_station_code
						where   effective_to = '2999-12-31 00:00:00.000'
						and     service_key is not null
					)   as skb
        on  skb.sk_ref = ska.service_key
-- Channel Mapping Log Station Code map against service keys (Version 2 part 2)
/*
	To complement above with any Map for log_station_code that are no longer active
	we proceed with below query
*/
union
select  ska_base.*
        ,skb_base.*
from    (
            select  distinct
                    log_station_code
            		,case when map.log_station is null then 1 else 0 end    as new_flag
            		,service_key as sk_ref
            		,STI_CODE
            		,PANEL_CODE
            		,PROMO_PANEL_CODE
            from    vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_BARB    as skb
                    left join log_codes as map
            		on  map.string_search   like '%'||cast(skb.service_key as varchar)||'%'
            		and map.log_station     = skb.log_station_code
                    left join   (
                                    select  distinct
                                            log_station_code    as thelog
                                    from    vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_BARB
                                    where   effective_to = '2999-12-31 00:00:00.000'
                                    and     service_key is not null
                                )   as active_logs
                    on  skb.log_station_code    = active_logs.thelog
            where   effective_to >='2013-12-01 00:00:00.000'
			and		effective_to < '2999-12-31 00:00:00.000'
            and     service_key is not null
            and     active_logs.thelog is null --> to exclude all active logs mapped before
        )   as skb_base
        inner join  (
                        select  ska.service_key
								,ska.vespa_name
								,ska.epg_name
								,ska.full_name
								,ska.channel_name
								,ska.BARB_REPORTED
								,ska.ACTIVEx
								,ska.CHANNEL_OWNER
								,ska.FORMAT
								,ska.PARENT_SERVICE_KEY
								,ska.TIMESHIFT_STATUS
								,ska.TIMESHIFT_MINUTES
								,ska.TYPE_ID
								,ska.PRIMARY_SALES_HOUSE
								,ska.CHANNEL_GROUP
								,ska.retail
                        from    vespa_analysts.channel_map_prod_service_key_attributes as ska
                                inner join  (
                                                select  service_key
                                                        ,max(effective_to)  as active
                                                from    vespa_analysts.channel_map_prod_service_key_attributes
                                                group   by  service_key
                                            )   as triming
                                on  ska.service_key     = triming.service_key
                                and ska.effective_to    = triming.active
                        where   ska.service_key < 65535
						--and		lower(trim(channel_reach)) <> 'roi'
                    )   as ska_base
        on  skb_base.sk_ref    = ska_base.service_key
		
commit	
		
select  a.*
from    cm_stuff    as a
        left join   (
                        select  distinct service_key
                        from    (
                                    select  parent_service_key as psk
                                            ,service_key
                                            ,format
                                            ,retail
                                            ,dense_rank() over  (
                                                                    partition by    psk
                                                                                    ,retail
                                                                    order by        service_key
                                                                )   as keep_the_1
                                    from    cm_stuff    as a
                                            inner join  (
                                                            select  parent_service_key      as psk
                                                                    ,retail                 as x
                                                                    ,count(distinct retail) as hits
                                                            from    cm_stuff
                                                            where   service_key < 65535
                                                            and     retail <>''
                                                            group   by  psk
                                                                        ,retail
                                                            having  hits = 1
                                                        )   as b
                                            on  a.parent_service_key     = b.psk
                                    group   by  psk
                                                ,service_key
                                                ,format
                                                ,retail
                                )   as base
                        where   keep_the_1 <>1
                    )   as b
        on  a.service_key   = b.service_key
where   b.service_key is null
--and     a.channel_group <> 'Pub'