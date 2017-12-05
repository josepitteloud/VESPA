
-- Author: Angel Donnarumma

with base as    (
                    select  distinct
							the_base.account_number
                            ,the_base.subscriber_id
                            ,the_base.event_start_date_time_utc                                      as event_start
                            ,coalesce (the_base.capping_end_Date_time_utc,the_base.event_end_date_time_utc)   as event_end
                            ,datediff(ss, event_start, event_end)                           as thedur
                            ,the_base.channel_name
                    from    VESPA_DP_PROG_VIEWED_201607 as the_base
							inner join	z_sample as ref
							on	the_base.account_number = ref.account_number
                    where   the_base.service_key <> 65535
                )
select  account_number
        ,channel_name
        ,count(1)       as freq
        ,sum(thedur)    as n_secs_spent
from    base
group   by  account_number
            ,channel_name


