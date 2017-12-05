-- Ops Dash slicers

-- Opt Out

   -- S1 - All Accounts
	select  rtm
			,count(distinct (case when viewing_consent_flag = 'Y' then account_number else null end)) as viewing_allowed
			,count(distinct (case when viewing_consent_flag = 'N' then account_number else null end)) as viewing_not_allowed
			,count(distinct (case when viewing_consent_flag ='?' then account_number else null end)) as viewing_capture_is_question
			,count(distinct (case when viewing_consent_flag is null then account_number else null end)) as viewing_capture_is_NULL
			,count(distinct account_number) as total_records
	from    vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
	where   Cust_active_DTV = 1
	and     cust_active_dt <= (select convert(date,today() - datepart(weekday,today())-2)) --last thursday
	group 	by	rtm

    /*UNION
    select 'Grand Total'
            ,sum(case when viewing_consent_flag = 'Y' then 1 else 0 end) as viewing_allowed
            ,sum(case when viewing_consent_flag = 'N' then 1 else 0 end) as viewing_not_allowed
            ,sum(case when viewing_consent_flag ='?' then 1 else 0 end) as viewing_capture_is_question
            ,sum(case when viewing_consent_flag is null then 1 else 0 end) as viewing_capture_is_NULL
            ,count(*) as total_records
            ,cast(viewing_allowed as float) / cast(total_records as float) as consent
            ,cast(viewing_allowed as float) /(cast(viewing_allowed as float) + cast(viewing_not_allowed as float) ) as consent_comm
    from    angeld.SIG_SINGLE_ACCOUNT_VIEW
    where   Cust_active_DTV = 1
    and     cust_active_dt <= (select convert(date,today() - datepart(weekday,today())-2)) --last thursday*/

    -- After May
    select  rtm
            ,count(distinct (case when viewing_consent_flag = 'Y' then account_number else null end)) as viewing_allowed
			,count(distinct (case when viewing_consent_flag = 'N' then account_number else null end)) as viewing_not_allowed
			,count(distinct (case when viewing_consent_flag ='?' then account_number else null end)) as viewing_capture_is_question
			,count(distinct (case when viewing_consent_flag is null then account_number else null end)) as viewing_capture_is_NULL
            ,count(*) as total_records
            ,cast(viewing_allowed as float) / cast(total_records as float) as consent
    from    vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
    where   Cust_active_DTV = 1
    and     cust_active_dt <= (select convert(date,today() - datepart(weekday,today())-2)) --last thursday
    and     cust_active_dt >= '2011-05-26' -- Chordant Fix in place from 26th of May
    group	by	rtm
    /* UNION
    select  'Grand Total'
            ,sum(case when viewing_consent_flag = 'Y' then 1 else 0 end) as viewing_allowed
            ,sum(case when viewing_consent_flag = 'N' then 1 else 0 end) as viewing_not_allowed
            ,sum(case when viewing_consent_flag ='?' then 1 else 0 end) as viewing_capture_is_question
            ,sum(case when viewing_consent_flag is null then 1 else 0 end) as viewing_capture_is_NULL
            ,count(*) as total_records
            ,cast(viewing_allowed as float) / cast(total_records as float) as consent
    from    angeld.SIG_SINGLE_ACCOUNT_VIEW
    where   Cust_active_DTV = 1
    and     cust_active_dt <= (select convert(date,today() - datepart(weekday,today())-2)) --last thursday
    and     cust_active_dt >= '2011-05-26' -- Chordant Fix in place from 26th of May */

    -- After April
    select  rtm
            ,count(distinct (case when viewing_consent_flag = 'Y' then account_number else null end)) as viewing_allowed
			,count(distinct (case when viewing_consent_flag = 'N' then account_number else null end)) as viewing_not_allowed
			,count(distinct (case when viewing_consent_flag ='?' then account_number else null end)) as viewing_capture_is_question
			,count(distinct (case when viewing_consent_flag is null then account_number else null end)) as viewing_capture_is_NULL
            ,count(*) as total_records
            ,cast(viewing_allowed as float) / cast(total_records as float) as consent
    from    vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
    where   Cust_active_DTV = 1
    and     cust_active_dt <= (select convert(date,today() - datepart(weekday,today())-2)) --last thursday
    and     cust_active_dt >= '2011-04-28' -- RTMs collecting opt-out data since 28th of April
    group 	by	rtm
    /* UNION
    select  'Grand Total'
            ,sum(case when viewing_consent_flag = 'Y' then 1 else 0 end) as viewing_allowed
            ,sum(case when viewing_consent_flag = 'N' then 1 else 0 end) as viewing_not_allowed
            ,sum(case when viewing_consent_flag ='?' then 1 else 0 end) as viewing_capture_is_question
            ,sum(case when viewing_consent_flag is null then 1 else 0 end) as viewing_capture_is_NULL
            ,count(*) as total_records
            ,cast(viewing_allowed as float) / cast(total_records as float) as consent
    from    angeld.SIG_SINGLE_ACCOUNT_VIEW
    where   Cust_active_DTV = 1
    and     cust_active_dt <= (select convert(date,today() - datepart(weekday,today())-2)) --last thursday
    and     cust_active_dt >= '2011-04-28' -- RTMs collecting opt-out data since 28th of April */

    -- Last week
    select  rtm
            ,count(distinct (case when viewing_consent_flag = 'Y' then account_number else null end)) as viewing_allowed
			,count(distinct (case when viewing_consent_flag = 'N' then account_number else null end)) as viewing_not_allowed
			,count(distinct (case when viewing_consent_flag ='?' then account_number else null end)) as viewing_capture_is_question
			,count(distinct (case when viewing_consent_flag is null then account_number else null end)) as viewing_capture_is_NULL
            ,count(*) as total_records
            ,cast(viewing_allowed as float) / cast(total_records as float) as consent
    from    vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
    where   Cust_active_DTV = 1
    and     dateadd(day, 7, cust_active_dt) > convert(date,today() - datepart(weekday,today())-2) --last thursday
    group 	by	rtm
    /* UNION
    select  'Grand Total'
            ,sum(case when viewing_consent_flag = 'Y' then 1 else 0 end) as viewing_allowed
            ,sum(case when viewing_consent_flag = 'N' then 1 else 0 end) as viewing_not_allowed
            ,sum(case when viewing_consent_flag ='?' then 1 else 0 end) as viewing_capture_is_question
            ,sum(case when viewing_consent_flag is null then 1 else 0 end) as viewing_capture_is_NULL
            ,count(*) as total_records
            ,cast(viewing_allowed as float) / cast(total_records as float) as consent
    from    angeld.SIG_SINGLE_ACCOUNT_VIEW
    where   Cust_active_DTV = 1
    and     dateadd(day, 7, cust_active_dt) > convert(date,today() - datepart(weekday,today())-2) --last thursday */



-- Weekly Enablement

replaced by

select  SBV.enablement_date
		,case 	when SBV.ps_flag = 'P' then 'Primary'
				when SBV.ps_flag = 'S' then 'Secondary'
				else null
		end		as ps_flag
		,case 	when SBV.Num_logs_sent_30d > 0 then 'Returned Data' else 'Cero Returned' end	as returned_data
		,case	when SAV.box_type_subs in ('F) SkyPlus', 'E) SkyPlus Combi') 	then 'Sky+ Subscription'
				when SAV.box_type_subs = 'H) FDB'								then 'FDB'
				when SAV.box_type_subs in ('G) Multiroom', 'Unknown')			then 'Multiroom/Unknown'
                when SAV.box_type_subs like '%HDx%'                             then 'HD box with No HD Sub'
				else 'HD box with HD Sub'
		end		subscriptions
		,count(distinct SBV.subscriber_id)	as nboxes
from 	vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
		inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
		on SBV.account_number = SAV.account_number
where 	SBV.status_vespa in ('Enabled','DisableRequested','DisablePending')
and 	SBV.panel in ('VESPA','VESPA11')
group 	by	SBV.enablement_date
			,ps_flag
			,returned_data
            ,subscriptions
			
select  SBV.enablement_date
		,case 	when SBV.ps_flag = 'P' then 'Primary'
				when SBV.ps_flag = 'S' then 'Secondary'
				else null
		end		as ps_flag
		,case 	when SBV.Num_logs_sent_30d > 0 then 'Returned Data' else 'Cero Returned' end	as returned_data
		,case	when SAV.box_type_subs in ('F) SkyPlus', 'E) SkyPlus Combi') 	then 'Sky+ Subscription'
				when SAV.box_type_subs = 'H) FDB'								then 'FDB'
				when SAV.box_type_subs in ('G) Multiroom', 'Unknown')			then 'Multiroom/Unknown'
                when SAV.box_type_subs like '%HDx%'                             then 'HD box with No HD Sub'
				else 'HD box with HD Sub'
		end		subscriptions
		,count(distinct SBV.subscriber_id)	as nboxes
        ,count(distinct SAV.account_number) as naccounts
from 	vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
		inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
		on SBV.account_number = SAV.account_number
where 	SBV.status_vespa in ('Enabled','DisableRequested','DisablePending')
and 	SBV.panel in ('VESPA','VESPA11')
group 	by	SBV.enablement_date
			,ps_flag
			,returned_data
            ,subscriptions

--By box
-- select base.enablement_date
       -- ,sum(Box) as 'Total Boxes'
       -- ,sum(Primary_Box_Enabled) as 'Primary Box Enabled'
       -- ,sum(Primary_Box_Enabled_Returning_Data) as 'Primary Box Enabled & Returning Data'
       -- ,sum(Non_Primary_Box_Enabled) as 'Non-Primary Box Enabled'
       -- ,sum(Non_Primary_Box_Enabled_Returning_Data) as 'Non-Primary Box Enabled & Returning Data'
       -- ,sum(Primary_Box_HD_with_HD_Subscription) as 'Primary Box - HD with HD Subscription'
       -- ,sum(Primary_Box_HD_with_no_HD_Subscription) as 'Primary Box - HD with no HD Subscription'
       -- ,sum(Primary_Box_Sky) as 'Primary Box - Sky+'
       -- ,sum(Primary_Box_FDB) as 'Primary Box - FDB'
       -- ,sum(Primary_Box_OtherUnknown) as 'Primary Box - Other/Unknown'
       -- ,sum(Secondary_Box_HD_with_HD_Subscription) as 'Secondary Box - HD with HD Subscription'
       -- ,sum(Secondary_Box_HD_with_no_HD_Subscription) as 'Secondary Box - HD with no HD Subscription'
       -- ,sum(Secondary_Box_Sky) as 'Secondary Box - Sky+'
       -- ,sum(Secondary_Box_FDB) as 'Secondary Box - FDB'
       -- ,sum(Secondary_Box_OtherUnknown) as 'Secondary Box - Other/Unknown'
-- from (
-- select  enablement_date
        -- ,count (Card_Subscriber_ID) as Box
        -- ,case when ps_flag = 'P' then count (Card_Subscriber_ID) else 0 end as Primary_Box_Enabled
        -- ,case when ps_flag = 'P' and Num_logs_sent_30d > 0 then count (Card_Subscriber_ID) else 0 end as Primary_Box_Enabled_Returning_Data
        -- ,case when ps_flag = 'S' then count (Card_Subscriber_ID) else 0 end as Non_Primary_Box_Enabled
        -- ,case when ps_flag = 'S' and Num_logs_sent_30d > 0 then count (Card_Subscriber_ID) else 0 end as Non_Primary_Box_Enabled_Returning_Data
        -- ,case when ps_flag = 'P' and SBV.HD_Box_Physical = 1 and SAV.HD_box_subs = 1 then count (Card_Subscriber_ID) else 0 end as Primary_Box_HD_with_HD_Subscription
        -- ,case when ps_flag = 'P' and SBV.HD_Box_Physical = 1 and SAV.HD_box_subs = 0 then count (Card_Subscriber_ID) else 0 end as Primary_Box_HD_with_no_HD_Subscription
        -- ,case when ps_flag = 'P' and SAV.box_type_subs in ('F) SkyPlus', 'E) SkyPlus Combi') then count (Card_Subscriber_ID) else 0 end as Primary_Box_Sky
        -- ,case when ps_flag = 'P' and SAV.box_type_subs = 'H) FDB' then count (Card_Subscriber_ID) else 0 end as Primary_Box_FDB
        -- ,case when ps_flag = 'P' and SAV.box_type_subs in ('G) Multiroom', 'Unknown') then count (Card_Subscriber_ID) else 0 end as Primary_Box_OtherUnknown
        -- ,case when ps_flag = 'S' and SBV.HD_Box_Physical = 1 and SAV.HD_box_subs = 1 then count (Card_Subscriber_ID) else 0 end as Secondary_Box_HD_with_HD_Subscription
        -- ,case when ps_flag = 'S' and SBV.HD_Box_Physical = 1 and SAV.HD_box_subs = 0 then count (Card_Subscriber_ID) else 0 end as Secondary_Box_HD_with_no_HD_Subscription
        -- ,case when ps_flag = 'S' and SAV.box_type_subs in ('F) SkyPlus', 'E) SkyPlus Combi') then count (Card_Subscriber_ID) else 0 end as Secondary_Box_Sky
        -- ,case when ps_flag = 'S' and SAV.box_type_subs = 'H) FDB' then count (Card_Subscriber_ID) else 0 end as Secondary_Box_FDB
        -- ,case when ps_flag = 'S' and SAV.box_type_subs in ('G) Multiroom', 'Unknown') then count (Card_Subscriber_ID) else 0 end as Secondary_Box_OtherUnknown
    -- from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
    -- inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
    -- on SBV.account_number = SAV.account_number
    -- where SBV.status_vespa = 'Enabled'
    -- and SBV.panel = 'VESPA'
    -- group by SBV.enablement_date, SBV.ps_flag, SBV.Num_logs_sent_30d, SBV.HD_Box_Physical, SAV.HD_box_subs, SAV.box_type_subs
    -- ) as base
-- group by base.enablement_date
-- order by base.enablement_date asc

------------
--By account
------------


/* NOTE FOR ME: I THINK I CAN USE THE SAME LOGIC AS BY BOX IN HERE USING SAV KPIS FOR DATA RETURN*/
-- [DONE]
/*
	see above active SQL for ref of what in place (at account level)
*/

/*
select base.enablement_date
       ,count(distinct account_number) as Total_Accounts
       ,sum(Primary_Box_Enabled) as 'Primary Box Enabled'
       ,sum(Primary_Box_Enabled_Returning_Data) as 'Primary Box Enabled & Returning Data'
       ,sum(Primary_Box_Only) as Primary_Box_Only
       ,sum(Primary_Box_Only_Box_Returned_Data) as Primary_Box_Only_Box_Returned_Data
       ,sum(Multiple_Box_Enabled) as Multiple_Box_Enabled
       ,sum(Multiple_Box_Enabled_Returning_Data) as Multiple_Box_Enabled_Returning_Data
       ,sum(Multiple_Box_Enabled_Not_Returning_Data) as Multiple_Box_Enabled_Not_Returning_Data
       ,sum(only_non_primary_boxes_enabled) as only_non_primary_boxes_enabled
       ,null as leaving_space_for_percentages_1
       ,null as leaving_space_for_percentages_2
       ,null as leaving_space_for_percentages_3
       ,sum(enabled_TT) as 'A) Top Tier'
       ,sum(enabled_DS) as 'B) Dual Sports'
       ,sum(enabled_DM) as 'C) Dual Movies'
       ,sum(enabled_DS_SM) as 'D) Dual Sports & Single Movies'
       ,sum(enabled_DM_SS) as 'E) Dual Movies & Single Sports'
       ,sum(enabled_SS_SM) as 'F) Single Sports & Single Movies'
       ,sum(enabled_SS) as 'G) Single Sports'
       ,sum(enabled_SM) as 'H) Single Movies'
       ,sum(enabled_no_premiums) as 'I) No Premiums'
       ,sum(Primary_Box_HD_w_HD_Subs_and_MR) as 'Primary Box HD with HD Subs & Multiroom'
       ,sum(Primary_Box_HD_w_HD_Subs) as 'Primary Box HD with HD Subs'
       ,sum(Primary_Box_HD_w_no_HD_Subs_and_MR) as 'Primary Box HD with no HD Subs & Multiroom'
       ,sum(Primary_Box_HD_w_no_HD_Subs) as 'Primary Box HD with no HD Subs'
       ,sum(SkyPlus_Primary_Box_w_MR) as 'Skyplus Primary Box with Multiroom'
       ,sum(SkyPlus_Primary_Box) as 'Skyplus Primary Box'
       ,sum(Basic_Primary_Box_w_MR) as 'Basic Primary Box with Multiroom'
       ,sum(Basic_Primary_Box) as 'Basic Primary Box'
       ,sum(Unknown_Primary_Box_w_MR) as 'Unknown Primary Box with Multiroom'
       ,sum(Unknown_Primary_Box) as 'Unknown Primary Box'
       ,sum(No_PrimaryHD_Box_w_HD_Subs_Secondary_Box) as 'No Primary Box HD Secondary Box with HD Subs '
       ,sum(No_PrimaryHD_Box_Secondary_Box_no_HD_Subs) as 'No Primary Box HD Secondary Box with no HD Subs'
       ,sum(No_Primary_Box_Skyplus_Secondary_Box) as 'No Primary Box Skyplus Secondary Box'
       ,sum(No_Primary_Box_Basic_Secondary_Box) as 'No Primary Box Basic Secondary Box'
       ,sum(No_Primary_Box_Unknown_Secondary_Box) as 'No Primary Box Unknown Secondary'

from (
select  distinct SAV.account_number
        ,enablement_date
        ,case when ps_flag = 'P' then count (Card_Subscriber_ID) else 0 end as Primary_Box_Enabled
        ,case when ps_flag = 'P' and Num_logs_sent_30d > 0 then count (Card_Subscriber_ID) else 0 end as Primary_Box_Enabled_Returning_Data
        ,case when ps_flag = 'P' and Num_Boxes = 1 then count (Card_Subscriber_ID) else 0 end as Primary_Box_Only
        ,case when ps_flag = 'P' and Num_Boxes = 1  and Num_logs_sent_30d > 0 then count (Card_Subscriber_ID) else 0 end as Primary_Box_Only_Box_Returned_Data
        ,case when ps_flag in('P','S') and Num_Boxes > 1 then count (Card_Subscriber_ID) else 0 end as Multiple_Box_Enabled
        ,case when ps_flag in('P','S') and Num_Boxes > 1 and Num_logs_sent_30d > 0 then count (Card_Subscriber_ID) else 0 end as Multiple_Box_Enabled_Returning_Data
        ,case when ps_flag in('P','S') and Num_Boxes > 1 and Num_logs_sent_30d is null then count (Card_Subscriber_ID) else 0 end as Multiple_Box_Enabled_Not_Returning_Data
        ,case when ps_flag = 'S' and Num_Boxes > 1  then count (Card_Subscriber_ID) else 0 end as only_non_primary_boxes_enabled
                -- Now for the package categories:
        ,case when prem_movies = 2 and prem_sports =2  then count (Card_Subscriber_ID) else 0 end as enabled_TT
        ,case when prem_movies = 0 and prem_sports =2  then count (Card_Subscriber_ID) else 0 end as enabled_DS
        ,case when prem_movies = 2 and prem_sports =0  then count (Card_Subscriber_ID) else 0 end as enabled_DM
        ,case when prem_movies = 1 and prem_sports =2  then count (Card_Subscriber_ID) else 0 end as enabled_DS_SM
        ,case when prem_movies = 2 and prem_sports =1  then count (Card_Subscriber_ID) else 0 end as enabled_DM_SS
        ,case when prem_movies = 1 and prem_sports =1  then count (Card_Subscriber_ID) else 0 end as enabled_SS_SM
        ,case when prem_movies = 0 and prem_sports =1  then count (Card_Subscriber_ID) else 0 end as enabled_SS
        ,case when prem_movies = 1 and prem_sports =0  then count (Card_Subscriber_ID) else 0 end as enabled_SM
        ,case when prem_movies = 0 and prem_sports =0  then count (Card_Subscriber_ID) else 0 end as enabled_no_premiums
                -- Now for the box types:
        ,case when ps_flag = 'P' and SBV.HD_Box_Physical = 1 and SAV.HD_box_subs = 1 and Num_Boxes > 1 then count (Card_Subscriber_ID) else 0 end as Primary_Box_HD_w_HD_Subs_and_MR
        ,case when ps_flag = 'P' and SBV.HD_Box_Physical = 1 and SAV.HD_box_subs = 1 then count (Card_Subscriber_ID) else 0 end as Primary_Box_HD_w_HD_Subs
        ,case when ps_flag = 'P' and SBV.HD_Box_Physical = 1 and SAV.HD_box_subs = 0 and Num_Boxes > 1 then count (Card_Subscriber_ID) else 0 end as Primary_Box_HD_w_no_HD_Subs_and_MR
        ,case when ps_flag = 'P' and SBV.HD_Box_Physical = 1 and SAV.HD_box_subs = 0 then count (Card_Subscriber_ID) else 0 end as Primary_Box_HD_w_no_HD_Subs
        ,case when ps_flag = 'P' and box_type_physical like 'Sky+%' and Num_Boxes > 1 then count (Card_Subscriber_ID) else 0 end as SkyPlus_Primary_Box_w_MR
        ,case when ps_flag = 'P' and box_type_physical like 'Sky+%' and Num_Boxes > 1 then count (Card_Subscriber_ID) else 0 end as SkyPlus_Primary_Box
        ,case when ps_flag = 'P' and box_type_physical = 'Basic' and Num_Boxes > 1 then count (Card_Subscriber_ID) else 0 end as Basic_Primary_Box_w_MR
        ,case when ps_flag = 'P' and box_type_physical = 'Basic' then count (Card_Subscriber_ID) else 0 end as Basic_Primary_Box
        ,case when ps_flag = 'P' and box_type_physical is null and Num_Boxes > 1 then count (Card_Subscriber_ID) else 0 end as Unknown_Primary_Box_w_MR
        ,case when ps_flag = 'P' and box_type_physical is null then count (Card_Subscriber_ID) else 0 end as Unknown_Primary_Box
        ,case when ps_flag <> 'P' and SAV.HD_box_subs = 1 and SBV.HD_Box_Physical = 1 then count (Card_Subscriber_ID) else 0 end as No_PrimaryHD_Box_w_HD_Subs_Secondary_Box
        ,case when ps_flag <> 'P' and SAV.HD_box_subs = 0 and SBV.HD_Box_Physical = 1 then count (Card_Subscriber_ID) else 0 end as No_PrimaryHD_Box_Secondary_Box_no_HD_Subs
        ,case when ps_flag <> 'P' and box_type_physical = 'Sky+' then count (Card_Subscriber_ID) else 0 end as No_Primary_Box_Skyplus_Secondary_Box
        ,case when ps_flag <> 'P' and box_type_physical = 'Basic' then count (Card_Subscriber_ID) else 0 end as No_Primary_Box_Basic_Secondary_Box
        ,case when ps_flag <> 'P' and box_type_physical is null then count (Card_Subscriber_ID) else 0 end as No_Primary_Box_Unknown_Secondary_Box
    from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
    inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
    on SBV.account_number = SAV.account_number
    where SBV.status_vespa = 'Enabled'
    and SBV.panel = 'VESPA'
    group by SBV.enablement_date, SAV.account_number, SBV.ps_flag, SBV.Num_logs_sent_30d, SBV.HD_Box_Physical, SAV.HD_box_subs, SAV.box_type_subs, SAV.Num_Boxes, SBV.status_vespa, SAV.prem_movies, SAV.prem_sports, SBV.box_type_physical
    ) as base
group by base.enablement_date
order by base.enablement_date asc
*/

--Cumulative by box

Replaced by

select  enablement_date
        ,runx+runxx                                                                           as t_box
        ,sum(min(x)) over (order by cast(dateformat(enablement_date,'YYYYMMDD') as integer))  as runx
        ,sum(min(y)) over (order by cast(dateformat(enablement_date,'YYYYMMDD') as integer))  as runy
        ,sum(min(xx)) over (order by cast(dateformat(enablement_date,'YYYYMMDD') as integer)) as runxx
        ,sum(min(yy)) over (order by cast(dateformat(enablement_date,'YYYYMMDD') as integer)) as runyy
        ,cast(runy as float)/cast(runx as float)    as p_y
        ,cast(runyy as float)/cast(runxx as float)  as p_yy
        ,cast((runy+runyy) as float) / cast(t_box as float) as p_ret
from    (
            select  enablement_date
                    ,count(distinct (case when ps_flag = 'P' then subscriber_id else null end)) as x
                    ,count(distinct (case when ps_flag = 'P' and Num_logs_sent_30d > 0 then subscriber_id else null end)) as y
                    ,count(distinct (case when ps_flag = 'S' then subscriber_id else null end)) as xx
                    ,count(distinct (case when ps_flag = 'S' and Num_logs_sent_30d > 0 then subscriber_id else null end)) as yy
            from    vespa_analysts.sig_single_box_view
            where 	status_vespa in ('Enabled','DisableRequested','DisablePending')
            and 	panel in ('VESPA','VESPA11')
            group   by  enablement_date
        )   as base
group   by  enablement_date


/*
select base1.enablement_date as enabled_date
        ,base1.Box as total_boxes_temp
        ,sum(base2.Box) as total_boxes
        ,sum(base2.Primary_Box_Enabled) as primary_box_enabled
        ,sum(base2.Primary_Box_Enabled_Returning_Data) as primary_box_enabled_and_returned_data
        ,sum(base2.Non_Primary_Box_Enabled) as non_primary_box_enabled
        ,sum(base2.Non_Primary_Box_Enabled_Returning_Data) as non_primary_box_enabled_and_returned_data
from (
select  ROW_NUMBER () OVER (order by enablement_date) as Row
       ,base.enablement_date
       ,sum(Box) as Box
       ,sum(Primary_Box_Enabled) as Primary_Box_Enabled
       ,sum(Primary_Box_Enabled_Returning_Data) as Primary_Box_Enabled_Returning_Data
       ,sum(Non_Primary_Box_Enabled) as Non_Primary_Box_Enabled
       ,sum(Non_Primary_Box_Enabled_Returning_Data) as Non_Primary_Box_Enabled_Returning_Data
from (
select  enablement_date
        ,count (Card_Subscriber_ID) as Box
        ,case when ps_flag = 'P' then count (Card_Subscriber_ID) else 0 end as Primary_Box_Enabled
        ,case when ps_flag = 'P' and Num_logs_sent_30d > 0 then count (Card_Subscriber_ID) else 0 end as Primary_Box_Enabled_Returning_Data
        ,case when ps_flag = 'S' then count (Card_Subscriber_ID) else 0 end as Non_Primary_Box_Enabled
        ,case when ps_flag = 'S' and Num_logs_sent_30d > 0 then count (Card_Subscriber_ID) else 0 end as Non_Primary_Box_Enabled_Returning_Data
    from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
    inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
    on SBV.account_number = SAV.account_number
    where SBV.status_vespa = 'Enabled'
    and SBV.panel = 'VESPA'
    group by SBV.enablement_date, SBV.ps_flag, SBV.Num_logs_sent_30d, SBV.HD_Box_Physical, SAV.HD_box_subs, SAV.box_type_subs
    ) as base
group by base.enablement_date
    ) as base1
    inner join (
    select  ROW_NUMBER () OVER (order by enablement_date) as Row
       ,base.enablement_date
       ,sum(Box) as Box
       ,sum(Primary_Box_Enabled) as Primary_Box_Enabled
       ,sum(Primary_Box_Enabled_Returning_Data) as Primary_Box_Enabled_Returning_Data
       ,sum(Non_Primary_Box_Enabled) as Non_Primary_Box_Enabled
       ,sum(Non_Primary_Box_Enabled_Returning_Data) as Non_Primary_Box_Enabled_Returning_Data
from (
select  enablement_date
        ,count (Card_Subscriber_ID) as Box
        ,case when ps_flag = 'P' then count (Card_Subscriber_ID) else 0 end as Primary_Box_Enabled
        ,case when ps_flag = 'P' and Num_logs_sent_30d > 0 then count (Card_Subscriber_ID) else 0 end as Primary_Box_Enabled_Returning_Data
        ,case when ps_flag = 'S' then count (Card_Subscriber_ID) else 0 end as Non_Primary_Box_Enabled
        ,case when ps_flag = 'S' and Num_logs_sent_30d > 0 then count (Card_Subscriber_ID) else 0 end as Non_Primary_Box_Enabled_Returning_Data
    from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
    inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
    on SBV.account_number = SAV.account_number
    where SBV.status_vespa = 'Enabled'
    and SBV.panel = 'VESPA'
    group by SBV.enablement_date, SBV.ps_flag, SBV.Num_logs_sent_30d, SBV.HD_Box_Physical, SAV.HD_box_subs, SAV.box_type_subs
    ) as base
group by base.enablement_date
    ) as base2
    on base1.Row >= base2.Row
group by base1.enablement_date
        ,base1.Box
order by base1.enablement_date
*/

--Box Type and Premiums

--Box Types

select  box_type_subs as [Box Type]
        ,count(distinct account_number) as [Sky Base]
        ,count(distinct case when panel in ('VESPA','VESPA11') and status_vespa in ('Enabled','DisableRequested','DisablePending') then account_number end) as [Dialy Panel]
from    vespa_analysts.sig_single_account_view
group   by box_type_subs

--Premiums

select  case when prem_sports = 2 and prem_movies = 2 then 'A) Top Tier'
             when prem_sports = 2 and prem_movies = 0 then 'B) Dual Sports'
             when prem_sports = 0 and prem_movies = 2 then 'C) Dual Movies'
             when prem_sports = 2 and prem_movies = 1 then 'D) Dual Sports & Single Movies'
             when prem_sports = 1 and prem_movies = 2 then 'E) Dual Movies & Single Sports'
             when prem_sports = 1 and prem_movies = 1 then 'F) Single Sports & Single Movies'
             when prem_sports = 1 and prem_movies = 0 then 'G) Single Sports'
             when prem_sports = 0 and prem_movies = 1 then 'H) Single Movies'
             when prem_sports = 0 and prem_movies = 0 then 'I) Basic'
             else                                          'J) Unknown'
             end as Premiums
        ,count(distinct account_number) as [Sky Base]
        ,count(distinct case when panel in ('VESPA','VESPA11') and status_vespa in ('Enabled','DisableRequested','DisablePending') then account_number end) as [Daily Panel]
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
group by Premiums
order by Premiums


--Adsmart

--Adsmartable / not Adsmartable at account level

Replaced by

select  case    when adsmart_flag = 1 then 'Adsmartable'
                else 'Not Adsmartable'    
        end     [Adsmartable Accounts + Personalisation Consent]
        ,count(distinct account_number) as [Sky Base]
        ,count(distinct case when panel in ('VESPA','VESPA11') and status_vespa in ('Enabled','DisableRequested','DisablePending') then account_number end) as [Daily Panel]
        ,count(distinct case when panel in ('VESPA','VESPA11') and status_vespa in ('Enabled','DisableRequested','DisablePending') and avg_reporting_quality > 0 then account_number end) as [Daily Panel Reporting]
from    vespa_analysts.sig_single_account_view
where   viewing_consent_flag = 'Y'
group   by  adsmart_flag

/*
select  case when ssbv.adsmart_flag = 0 then 'Not Adsmartable'
             when ssbv.adsmart_flag = 1 then 'Adsmartable'
        end as Adsmartable_or_Not
        ,count(distinct ssbv.account_number) as Sky_Base
        ,count(distinct case when ssbv.panel in('VESPA','VESPA11') then ssbv.account_number end) as Vespa_DP
        ,count(distinct case when ssbv.panel in('VESPA','VESPA11') and reporting_quality > 0 then ssbv.account_number end) as Vespa_DP_Reporting
from    angeld.sig_single_box_view as ssbv
        inner join
        angeld.sig_single_account_view as ssav
        on ssbv.account_number = ssav.account_number
        and ssav.viewing_consent_flag = 'Y'
group   by ssbv.adsmart_flag
order   by Adsmartable_or_Not
*/

--Adsmartable boxes at box level

select  ssbv.box_model || ' / ' || ssbv.description as Box_Type_Desc
        ,count(distinct ssbv.service_instance_id) as Sky_Base
        ,count(distinct case when ssbv.panel in('VESPA','VESPA11') then ssbv.service_instance_id end) as Vespa_DP
        ,count(distinct case when ssbv.panel in('VESPA','VESPA11') and reporting_quality > 0 then ssbv.service_instance_id end) as Vespa_DP_Reporting
from    angeld.sig_single_box_view as ssbv
        inner join
        angeld.sig_single_account_view as ssav
        on ssbv.account_number = ssav.account_number
        and ssav.viewing_consent_flag = 'Y'
where   ssbv.adsmart_flag = 1
group   by ssbv.box_model
        ,ssbv.description



/*
select * from (
select * from  vespa_analysts.sig_single_box_view
) as base
select * from vespa_analysts.sig_single_account_view
*/








