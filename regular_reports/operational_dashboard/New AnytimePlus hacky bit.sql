-- So this guy needs to be split out into P/S boxes, and then we need the numbers of
-- reporting boxes by day. But most of the query about building the anytime+ flags
-- is here already, so that's cool. Going forwards we want to append this table
-- thing into the daily summary, but that'll happen next week or something.


select account_number, card_subscriber_id
into vespa_panel_4
 from sk_prod.campaign_history_cust  a
inner join
sk_prod.campaign_history_lookup_cust   b
on a.cell_id = b.cell_id
where cell_name like 'Vespa Enablement%'
and writeback_datetime >=  '20111010'
and a.account_number not in (select account_number from sk_prod.campaign_history_cust  a
inner join
sk_prod.campaign_history_lookup_cust   b
on a.cell_id = b.cell_id
where cell_name like 'Vespa Disablement%'
and writeback_datetime >=  '20111010')

--add new columns
alter table vespa_panel_4
add x_anytime_plus_enabled as char(1)
,add anytime_plus_account as char(1)

--set anytime_plus_account
update vespa_panel_4
set anytime_plus_account = 'N'

update vespa_panel_4    a
set anytime_plus_account = 'Y'
from sk_prod.cust_subs_hist    b
where a.account_number = b.account_number
and subscription_sub_type = 'PDL subscriptions'
AND    status_code = 'AC'

--create temp table for anytime_plus eligible boxes to join back on card_sub_id
select card_subscriber_id,x_anytime_plus_enabled into vespa_card_anytime_plus
from sk_prod.cust_card_subscriber_link    c
inner join
sk_prod.cust_set_top_box d
on c.service_instance_id = d.service_instance_id
where active_box_flag = 'Y'
and d.x_anytime_plus_enabled = 'Y'
and c.account_number in (select account_number from vespa_panel_4)


-- set x_anytime_plus_enabled
update vespa_panel_4
set x_anytime_plus_enabled = 'N'

update vespa_panel_4 a
set a.x_anytime_plus_enabled = 'Y'
from vespa_card_anytime_plus    b
where a.card_subscriber_id = b.card_subscriber_id




--account_level
select anytime_plus_account,  count(distinct account_number) from vespa_panel_4
group by
anytime_plus_account

--count by card_sub_id
select anytime_plus_account, x_anytime_plus_enabled, count(distinct card_subscriber_id) from vespa_panel_4
group by anytime_plus_account, x_anytime_plus_enabled


select count(distinct card_subscriber_id) from vespa_panel_4
where anytime_plus_account = 'Y' and x_anytime_plus_enabled = 'Y'



