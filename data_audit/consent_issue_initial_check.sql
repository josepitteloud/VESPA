
if object_id('panel_accounts_examine') is not null drop table panel_accounts_examine;
commit;
if object_id('panel_accounts_examine_hist') is not null drop table panel_accounts_examine_hist;
commit;
if object_id('subscriber_accounts_examine_hist') is not null drop table subscriber_accounts_examine_hist;
commit;
if object_id('subscriber_accounts_examine') is not null drop table subscriber_accounts_examine;
commit;


go

begin
select vpsh.account_number, max(vpsh.selected_dt) selected_dt
into #panel_account_status_hist
from
sk_prod.vespa_panel_status_hist vpsh
,Sleary.agent_console_issue_20130405 con_issue
where vpsh.account_number = con_issue.account_number
group by vpsh.account_number

select vpsh.account_number
into #panel_account_status
from
sk_prod.vespa_panel_status vpsh
,Sleary.agent_console_issue_20130405 con_issue
where vpsh.account_number = con_issue.account_number
group by vpsh.account_number

select * into panel_accounts_examine_hist
from
(select vpsh.* from sk_prod.vespa_panel_status_hist vpsh,
#panel_account_status_hist acc_stat
where vpsh.account_number = acc_stat.account_number
and vpsh.selected_dt = acc_stat.selected_dt) t

select * into panel_accounts_examine
from
(select vpsh.* from sk_prod.vespa_panel_status vpsh,
#panel_account_status acc_stat
where vpsh.account_number = acc_stat.account_number) t

commit

----subscriber_status
select vssh.card_subscriber_id, max(vssh.created_dt) created_dt
into #subscriber_account_status_hist
from
sk_prod.vespa_subscriber_status_hist vssh
,Sleary.agent_console_issue_20130405 con_issue
where vssh.account_number = con_issue.account_number
group by vssh.card_subscriber_id

select distinct vss.card_subscriber_id
into #subscriber_account_status
from
sk_prod.vespa_subscriber_status vss
,Sleary.agent_console_issue_20130405 con_issue
where vss.account_number = con_issue.account_number

select * into subscriber_accounts_examine_hist
from
(select vssh.* from sk_prod.vespa_subscriber_status_hist vssh,
#subscriber_account_status_hist acc_stat
where vssh.card_subscriber_id = acc_stat.card_subscriber_id
and vssh.created_dt = acc_stat.created_dt) t

select * into subscriber_accounts_examine
from
(select vss.* from sk_prod.vespa_subscriber_status vss,
#subscriber_account_status acc_stat
where vss.card_subscriber_id = acc_stat.card_subscriber_id) t


commit
end
