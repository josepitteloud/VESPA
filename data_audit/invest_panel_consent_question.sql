select * into agent_console_cust_20130405
from
(Select * from Sleary.agent_console_issue_20130405) t



---daily panel data new tables

--investigate daily panel tables and viewers affected

create table account_invest_total
(account_number varchar(20),
dk_event_start_datehour_dim bigint,
panel_id int)

insert into account_invest_total
select b.account_number, max(dk_event_start_datehour_dim), panel_id from 
sk_prod.vespa_dp_prog_viewed_201304 a,
(select distinct account_number FROM agent_console_cust_20130405
where viewing_data_capture_allowed = 'Y'
and DTV_ORDER_DT >= '2011-03-30') b
where cast (a.account_number as varchar(20)) = cast (b.account_number as varchar(20))
group by b.account_number, panel_id
union
select b.account_number, max(dk_event_start_datehour_dim), panel_id from 
sk_prod.vespa_dp_prog_viewed_201303 a,
(select distinct account_number FROM agent_console_cust_20130405
where viewing_data_capture_allowed = 'Y'
and DTV_ORDER_DT >= '2011-03-30') b
where cast (a.account_number as varchar(20)) = cast (b.account_number as varchar(20))
group by b.account_number, panel_id
union
select b.account_number, max(dk_event_start_datehour_dim), panel_id from 
sk_prod.vespa_dp_prog_viewed_201302 a,
(select distinct account_number FROM agent_console_cust_20130405
where viewing_data_capture_allowed = 'Y'
and DTV_ORDER_DT >= '2011-03-30') b
where cast (a.account_number as varchar(20)) = cast (b.account_number as varchar(20))
group by b.account_number, panel_id
union
select b.account_number, max(dk_event_start_datehour_dim), panel_id from 
sk_prod.vespa_dp_prog_viewed_201301 a,
(select distinct account_number FROM agent_console_cust_20130405
where viewing_data_capture_allowed = 'Y'
and DTV_ORDER_DT >= '2011-03-30') b
where cast (a.account_number as varchar(20)) = cast (b.account_number as varchar(20))
group by b.account_number, panel_id
union
select b.account_number, max(dk_event_start_datehour_dim), panel_id from 
sk_prod.vespa_dp_prog_viewed_201212 a,
(select distinct account_number FROM agent_console_cust_20130405
where viewing_data_capture_allowed = 'Y'
and DTV_ORDER_DT >= '2011-03-30') b
where cast (a.account_number as varchar(20)) = cast (b.account_number as varchar(20))
group by b.account_number, panel_id

commit

--investigate alternate day panel tables and viewers affected

---alternate day new tables

create table account_invest_total_ap
(account_number varchar(20),
dk_event_start_datehour_dim bigint,
panel_id int)

insert into account_invest_total_ap
select b.account_number, max(dk_event_start_datehour_dim), panel_id from 
sk_prod.vespa_ap_prog_viewed_201304 a,
(select distinct account_number FROM agent_console_cust_20130405
where viewing_data_capture_allowed = 'Y'
and DTV_ORDER_DT >= '2011-03-30') b
where cast (a.account_number as varchar(20)) = cast (b.account_number as varchar(20))
group by b.account_number, panel_id
union
select b.account_number, max(dk_event_start_datehour_dim), panel_id from 
sk_prod.vespa_ap_prog_viewed_201303 a,
(select distinct account_number FROM agent_console_cust_20130405
where viewing_data_capture_allowed = 'Y'
and DTV_ORDER_DT >= '2011-03-30') b
where cast (a.account_number as varchar(20)) = cast (b.account_number as varchar(20))
group by b.account_number, panel_id
union
select b.account_number, max(dk_event_start_datehour_dim), panel_id from 
sk_prod.vespa_ap_prog_viewed_201302 a,
(select distinct account_number FROM agent_console_cust_20130405
where viewing_data_capture_allowed = 'Y'
and DTV_ORDER_DT >= '2011-03-30') b
where cast (a.account_number as varchar(20)) = cast (b.account_number as varchar(20))
group by b.account_number, panel_id
union
select b.account_number, max(dk_event_start_datehour_dim), panel_id from 
sk_prod.vespa_ap_prog_viewed_201301 a,
(select distinct account_number FROM agent_console_cust_20130405
where viewing_data_capture_allowed = 'Y'
and DTV_ORDER_DT >= '2011-03-30') b
where cast (a.account_number as varchar(20)) = cast (b.account_number as varchar(20))
group by b.account_number, panel_id

commit

-----------events all phase 2 data

create table account_invest_total_ea
(account_number varchar(20),
panel_id int,
dk_event_start_datehour_dim bigint)

insert into account_invest_total_ea
select a.account_number, a.panel_id,max(dk_event_start_datehour_dim) from 
sk_prod.vespa_events_all a,
(select distinct account_number FROM agent_console_cust_20130405
where viewing_data_capture_allowed = 'Y'
and DTV_ORDER_DT >= '2011-03-30') b
where cast (a.account_number as varchar(20)) = cast (b.account_number as varchar(20))
group by a.account_number, a.panel_id

commit


-------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------

create table account_invest_total_ph_one
(account_number varchar(20),
panel_id int,
date_of_event varchar(10))

--phase 1 data

CREATE VARIABLE @var_period_start       datetime;
CREATE VARIABLE @var_period_end         datetime;
Create VARIABLE @var_scan_start_dt      datetime;
Create VARIABLE @var_period_end_dt      datetime;


CREATE VARIABLE @var_barb_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;
CREATE VARIABLE @i                      integer;

--rollback
-- Scaling Variables
-- Set the date variables

SET @var_period_start           = '2011-05-03'; -- Monday
SET @var_period_end             = '2012-10-04'; -- Mon

SET @i=datediff(dd,@var_period_start,@var_period_end);

SET @var_sql = ' insert into account_invest_total_ph_one
select distinct(a.account_number) account_number,panel_id, ''##^^*^*##''
from sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*## a, agent_console_cust_20130405 b where cast (a.account_number as varchar(20)) = cast (b.account_number as varchar(20))';

-- loop though each days viewing logs to identify repeat data returners
SET @var_cntr = 0;

WHILE @var_cntr <= @i
BEGIN
       EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(dateadd(day, @var_cntr, @var_period_start), 'yyyymmdd')))

       COMMIT

       SET @var_cntr = @var_cntr + 1
END

---------------------------------------------------------------------------------------------------------------------------

drop table account_exclude_issue_new

create table account_exclude_issue_new
(account_number varchar(20),
panel_id int,
event_date varchar(30),
source varchar(30))

delete from account_exclude_issue_new

commit

insert into account_exclude_issue_new
select distinct a.account_number, a.panel_no, cast(a.modified_dt as varchar(30)),
'panel_hist_status' source
from sk_prod.vespa_panel_status_hist a,
(select distinct account_number FROM agent_console_cust_20130405
where viewing_data_capture_allowed = 'Y'
and DTV_ORDER_DT >= '2011-03-30') b
where cast(a.account_number as varchar(20)) = cast(b.account_number as varchar(20))
union all
select distinct a.account_number, a.panel_no, cast(a.selected_dt as varchar(30)),
'panel_status' source
from sk_prod.vespa_panel_status a,
(select distinct account_number FROM agent_console_cust_20130405
where viewing_data_capture_allowed = 'Y'
and DTV_ORDER_DT >= '2011-03-30') b
where cast(a.account_number as varchar(20)) = cast(b.account_number as varchar(20))
union all
select distinct account_number,panel_id, cast(dk_event_start_datehour_dim as varchar(30)),
'Alternate_Day_Ph2_New' from
account_invest_total_ap
union all
select distinct account_number,panel_id, cast(dk_event_start_datehour_dim as varchar(30)),
'Daily_Panel_Ph2_New'
from
account_invest_total
union all
select distinct account_number,panel_id, cast(dk_event_start_datehour_dim as varchar(30)),
'vespa_events_all' from
account_invest_total_ea
union all
select distinct account_number,panel_id, cast(date_of_event as varchar(30)),
'phase_1_data' from
account_invest_total_ph_one
union all
select distinct a.account_number, a.vespa_panel, '2013040300',
'broadcaster_reporting'
from vespa_analysts.vespa_broadcast_reporting_vp_map a,
(select distinct account_number FROM agent_console_cust_20130405
where viewing_data_capture_allowed = 'Y'
and DTV_ORDER_DT >= '2011-03-30') b
where cast(a.account_number as varchar(20)) = cast(b.account_number as varchar(20))
union all
select distinct a.account_number, 12 vespa_panel, substr(cast(b.viewing_start_date_key as varchar),1,8) viewing_event_date,
'VIQ_Programme_Fact' source into tst_tk
from sk_prod.viq_household a,sk_prod.viq_viewing_data b,
(select distinct account_number FROM agent_console_cust_20130405
where viewing_data_capture_allowed = 'Y'
and DTV_ORDER_DT >= '2011-03-30') c
where a.household_key = b.household_key
and a.account_number = c.account_number
UNION ALL
select distinct a.account_number, 12 vespa_panel, substr(cast(b.viewed_start_date_key as varchar),1,8) viewing_event_date,
'VIQ_SLOT_FACT' source
from sk_prod.viq_household a,sk_prod.SLOT_DATA b,
(select distinct account_number FROM agent_console_cust_20130405
where viewing_data_capture_allowed = 'Y'
and DTV_ORDER_DT >= '2011-03-30') c
where a.household_key = b.household_key
and a.account_number = c.account_number
UNION ALL
select distinct a.account_number, 12 vespa_panel, substr(cast(b.viewed_start_date_key as varchar),1,8) viewing_event_date,
'VIQ_SLOT_FACT_HIST' source
from sk_prod.viq_household a,sk_prod.SLOT_DATA_HISTORY b,
(select distinct account_number FROM agent_console_cust_20130405
where viewing_data_capture_allowed = 'Y'
and DTV_ORDER_DT >= '2011-03-30') c
where a.household_key = b.household_key
and a.account_number = c.account_number

commit

grant select on kinnairt.account_exclude_issue_new to sleary
grant select on kinnairt.account_exclude_issue_new to louredaj
grant select on kinnairt.account_exclude_issue_new to bednaszs
grant select on kinnairt.account_exclude_issue_new to vespa_analysts

--create table vespa_analysts.accounts_to_exclude
--(account_number varchar(20))

--grant select on vespa_analysts.accounts_to_exclude to vespa_group_low_security, sk_prodreg;

--grant select,delete,update,insert on vespa_analysts.accounts_to_exclude to kinnairt;



delete from vespa_analysts.accounts_to_exclude

commit

insert into vespa_analysts.accounts_to_exclude
select distinct account_number FROM account_exclude_issue_new

commit
 


----------connect to vespa analysts schema


--create vespa_analysts table so that we have an exclusion list of all of those affected

select * into vespa_analysts.accounts_to_exclude
from
(select distinct account_number from kinnairt.account_exclude_issue_new) t


grant select on vespa_analysts.accounts_to_exclude to vespa_group_low_security