
exec STBCAP_apply_agorithm '2014-09-01', '2014-09-07', '2014-09-04'


------------------------------------------------------------
------------------------------------------------------------
------------------------------------------------------------
------------------------------------------------------------

-- Before this procedure is executed youneed to make sure that you have
-- imported from Netezza the capping thresholds from the previous week


--********* change reference to vespa_dp_prog_viewed_current as required ***************


IF object_id('STBCAP_apply_agorithm') IS NOT NULL THEN DROP PROCEDURE STBCAP_apply_agorithm END IF;

create procedure STBCAP_apply_agorithm
        @date_from date -- these are the dates of viewing to apply the STB cap to
        ,@date_to date
        ,@date_thursday date
as
begin

------------------------------------------------------------
------------------------------------------------------------
-- Summarise the Netezza Threshold data
-- over 1 week for each capping segment


/*
create table STBCAP_netezza_threshold_summary (
day_of_week integer
,CAPPED_THRESHOLD_HOUR integer
,CAPPED_THRESHOLD_CHANNEL_PACK varchar(50)
,CAPPED_THRESHOLD_BOX varchar(50)
,CAPPED_THRESHOLD_GENRE varchar(50)
,day_count integer
,avg_threshold_mins decimal(10,2)
,min_threshold_mins decimal(10,2)
,max_threshold_max decimal(10,2)
)

create lf index ind_day on STBCAP_netezza_threshold_summary(day_of_week)
create lf index ind_hour on STBCAP_netezza_threshold_summary(CAPPED_THRESHOLD_HOUR)
create lf index ind_pack on STBCAP_netezza_threshold_summary(CAPPED_THRESHOLD_CHANNEL_PACK)
create lf index ind_box on STBCAP_netezza_threshold_summary(CAPPED_THRESHOLD_BOX)
create lf index ind_genre on STBCAP_netezza_threshold_summary(CAPPED_THRESHOLD_GENRE)
*/


-- 1 week summary
delete from STBCAP_netezza_threshold_summary
insert into STBCAP_netezza_threshold_summary
select
        day_of_week
        ,CAPPED_THRESHOLD_HOUR
        ,CAPPED_THRESHOLD_CHANNEL_PACK
        ,CAPPED_THRESHOLD_BOX
        ,CAPPED_THRESHOLD_GENRE
        ,count(1) as day_count
        ,avg(CAPPED_THRESHOLD_EVENT_DURATION)
        ,min(CAPPED_THRESHOLD_EVENT_DURATION)
        ,max(CAPPED_THRESHOLD_EVENT_DURATION)
from
        STBCAP_Netezza_thresholds_live
where
        the_date between dateadd(dd, -7, @date_from) and dateadd(dd, -7, @date_to) --- Get thresholds for the week before viewing
group by
        day_of_week
        ,CAPPED_THRESHOLD_HOUR
        ,CAPPED_THRESHOLD_CHANNEL_PACK
        ,CAPPED_THRESHOLD_BOX
        ,CAPPED_THRESHOLD_GENRE

commit

------------------------------------------------------------
------------------------------------------------------------
-- Summarise the viewing data
--

/*create table STBCAP_viewing_data (
row_id bigint primary key identity
,event_start_date_time_utc timestamp
,subscriber_id integer
,service_key integer
,event_start_time timestamp
,event_end_time timestamp
,cap_end_time timestamp
,day_of_week integer
,event_start_hour integer
,channel_pack varchar(50)
,box varchar(50)
,genre varchar(50)
,sub_genre varchar(50)
,stb_cap integer
,scaling_weight numeric
,scaling_universe varchar(30)
,account_number varchar(20)
,event_start_date date
)

create hg index ind_subscriber_id on STBCAP_viewing_data(subscriber_id)
create hg index ind_event_start_date_time_utc on STBCAP_viewing_data(event_start_date_time_utc)
create hg index ind_service_key on STBCAP_viewing_data(service_key)
create hg index ind_acc on STBCAP_viewing_data(account_number)
create hg index ind_date on STBCAP_viewing_data(event_start_date)
*/

delete from STBCAP_viewing_data
insert into STBCAP_viewing_data (
event_start_date_time_utc
,subscriber_id
,service_key
,event_start_time
,event_end_time
,cap_end_time
,day_of_week
,event_start_hour
,channel_pack
,box
,genre
,sub_genre
,stb_cap
,account_number
,event_start_date
)

select
        event_start_date_time_utc
        ,subscriber_id
        ,service_key
        ,event_start_date_time_utc
        ,event_end_date_time_utc
        ,capping_end_date_time_utc
        ,datepart(dw, event_start_date_time_utc)
        ,datepart(hh, event_start_date_time_utc)
        ,'Other' -- set channel pack to be other, update in later query
        ,'Other' -- set box to be other, update in later query
        ,genre_description
        ,sub_genre_description
        ,120 -- default stb_cap is 120 mins if no matching capping segment
        ,account_number
        ,date(event_start_date_time_utc)
from
        -- sk_prod.vespa_dp_prog_viewed_current dp
         sk_prod.vespa_dp_prog_viewed_201409 dp
where
        live_recorded = 'LIVE'
        and INSTANCE_START_DATE_TIME_UTC < INSTANCE_END_DATE_TIME_UTC
        and (Panel_id = 12 or Panel_id = 11)
        and type_of_viewing_event <> 'Non viewing event'
        and type_of_viewing_event is not null
        and account_number is not null
        and subscriber_id is not null
        and BROADCAST_START_DATE_TIME_UTC is not null
        and (event_start_date_time_utc between @date_from and dateadd(dd, 1, @date_to))
commit

--- de-dupe viewing data
        select subscriber_id, event_start_date_time_utc, min(row_id) as min_row_id
        into STBCAP_temp
        from STBCAP_viewing_data
        group by subscriber_id, event_start_date_time_utc
        commit

        create hg index ind_row on STBCAP_temp(min_row_id)
        commit

        delete from STBCAP_viewing_data
        from STBCAP_viewing_data a left join STBCAP_temp b
        on a.row_id = b.min_row_id
        where b.min_row_id is null
        commit

        drop table STBCAP_temp
        commit


--- Update the Channel Pack
       update STBCAP_viewing_data stb
        set stb.channel_pack = trim(cm.channel_pack)
        from Vespa_Analysts.Channel_Map_Dev_Service_Key_Attributes cm
        where stb.service_key = cm.service_key

        commit


------- GET Primary/Seconday Box
--- This is from Capping Code
select distinct account_number
          ,subscriber_id
          ,service_instance_id
       into STBCAP_relevant_boxes
      from sk_prod.vespa_dp_prog_viewed_201403
     where (event_start_date_time_utc >= '2014-03-24 00:00:00' and event_start_date_time_utc < '2014-03-31 00:00:00') --- Date Change
       and (Panel_id = 12 or Panel_id = 11)
       and account_number is not null
       and subscriber_id is not null
commit

create hg index ind_acc on STBCAP_relevant_boxes (account_number)
create hg index ind_sub on STBCAP_relevant_boxes (subscriber_id)
create hg index ind_service on STBCAP_relevant_boxes (service_instance_id)
commit

delete from STBCAP_box_lookup
commit
/*CREATE TABLE STBCAP_box_lookup (
        subscriber_id      bigint
        ,account_number     varchar(20)
        ,service_instance_id varchar(50)
        ,PS_flag            varchar(1)
)
*/
   insert into STBCAP_box_lookup
    select
        subscriber_id
        ,min(account_number)
        ,min(service_instance_id)
        ,'U'
    from STBCAP_relevant_boxes
    where subscriber_id is not null -- dunno if there are any, but we need to check
        and account_number is not null
    group by subscriber_id
    commit

   select distinct account_number, 1 as Dummy
    into STBCAP_deduplicated_accounts
    from STBCAP_relevant_boxes
    commit

create unique index fake_pk on STBCAP_deduplicated_accounts (account_number)
commit

select distinct
        --da.account_number,        -- we're joining back in on service_instance_id, so we don't need account_number
        csh.service_instance_id,
        case
            when csh.subscription_sub_type = 'DTV Primary Viewing' then 'P'
            when csh.subscription_sub_type = 'DTV Extra Subscription' then 'S'
        end as PS_flag
    into STBCAP_all_PS_flags
    from STBCAP_deduplicated_accounts as da
    inner join cust_subs_hist as csh
    on da.account_number = csh.account_number
    where csh.SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing','DTV Extra Subscription')
    and csh.status_code in ('AC','AB','PC')
    and csh.effective_from_dt <= date('2014-03-27') -- Thursday
    and csh.effective_to_dt > date('2014-03-27') -- Thursday
commit

create index idx1 on STBCAP_all_PS_flags (service_instance_id)
commit

 update STBCAP_box_lookup
    set STBCAP_box_lookup.PS_flag = apsf.PS_flag
    from STBCAP_box_lookup
    inner join STBCAP_all_PS_flags as apsf
    on STBCAP_box_lookup.service_instance_id = apsf.service_instance_id
commit

drop table STBCAP_relevant_boxes
drop table STBCAP_deduplicated_accounts
drop table STBCAP_all_PS_flags
commit
------- Above from Capping code

-- Update Primary/Secondary Box
-- to match the values in the Threhold table
update STBCAP_viewing_data stb
        set stb.box = case when lk.ps_flag = 'P' then 'Primary DTV'
                           when lk.ps_flag = 'S' then 'Secondary DTV'
                           else 'U'
                      end
        from STBCAP_box_lookup lk
        where stb.subscriber_id = lk.subscriber_id
commit

delete from STBCAP_box_lookup
commit
------------------------------------------------------------
------------------------------------------------------------
--- Algorithm add STB Cap

---- Now added to the Create table
--alter table STBCAP_viewing_data
--add stb_cap integer

-- Set to default of 120 mins
update STBCAP_viewing_data
        set stb_cap = 120



-- Set matching capping segments to threshold
update STBCAP_viewing_data v
        set stb_cap = t.avg_threshold_mins
        from STBCAP_netezza_threshold_summary t
        where v.day_of_week = t.day_of_week
                and v.event_start_hour = t.CAPPED_THRESHOLD_HOUR
                and v.channel_pack = t.CAPPED_THRESHOLD_CHANNEL_PACK
                and v.sub_genre = t.CAPPED_THRESHOLD_GENRE
                and ((v.box = t.CAPPED_THRESHOLD_BOX and v.event_start_hour >= 4 and v.event_start_hour <= 19)
                      or (v.event_start_hour < 4 or v.event_start_hour > 19))
commit

------------------------------------------------------------
------------------------------------------------------------

--- Add scaling weight and scaling universe

update STBCAP_viewing_data v
        set scaling_weight = adsmart_scaling_weight
        from sk_prod.VIQ_VIEWING_DATA_SCALING s
        where v.account_number = s.account_number
        -- and event_start_date = s.adjusted_event_start_date_vespa
        and s.adjusted_event_start_date_vespa = @date_thursday --- Change Date
commit

update STBCAP_viewing_data v
        set scaling_universe = scaling_universe_key
        from sk_prod.VIQ_VIEWING_DATA_SCALING s
        where v.account_number = s.account_number
        -- and event_start_date = s.adjusted_event_start_date_vespa
         and s.adjusted_event_start_date_vespa = @date_thursday --- Change Date
commit


END







