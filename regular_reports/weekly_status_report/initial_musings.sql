-- Initial musings about how to integrate churn with Vespa. As it turns
-- out, there's only a bunch mre questios to ask and we should summarise
-- this in an email tomorrow morning to try and figure out what we're going
-- to consider as churn and whatnot. Oh well.

select  account_number
       ,effective_from_dt as churn_date
       ,case when status_code = 'PO'
             then 'CUSCAN'
             else 'SYSCAN'
         end as churn_type
       ,RANK() OVER (PARTITION BY  csh.account_number
                     ORDER BY  csh.effective_from_dt,csh.cb_row_id) AS 'RANK'  --Rank to get the first event
  into mychurnrecords
  from sk_prod.cust_subs_hist as csh
 where subscription_sub_type ='DTV Primary Viewing'     --DTV stack
   and status_code in ('PO','SC')                       --CUSCAN and SYSCAN status codes
   and effective_from_dt between '2012-01-12' and '2012-01-18'
   and effective_from_dt != effective_to_dt
;

--Get a count of the first churn events; must be done separately as SQL does not
--support window functions like rank() in a HAVING clause
select count(*)
  from mychurnrecords
 where rank = 1;

 -- doesn't take too long!
delete from mychurnrecords where rank > 1;
-- 28k people churned off Sky in a week? okay.

create unique index fake_pk on mychurnrecords (account_number);

select count(1), count(distinct mcr.account_number)
from mychurnrecords as mcr
inner join vespa_analysts.vespa_single_box_view as svb
on mcr.account_number = svb.account_number;
-- 1529    1218
-- wait... so these are guys that churned when? ad they're still on the box view....

select top 10 * from vespa_analysts.vespa_single_box_view


select panel_id_4_cells_confirm, status_vespa, count(1)
from mychurnrecords as mcr
inner join vespa_analysts.vespa_single_box_view as svb
on mcr.account_number = svb.account_number
group by panel_id_4_cells_confirm, status_vespa;
/*panel_id_4_cells_confirm        status_vespa    count(1)
0       Enabled 1367
1       Enabled 33
1       DisablePending  102
1       EnablePending   27
*/
--  ha wtf soooo messy? this kind of calls into question how we're recognising the
-- vespa panel at any point in time. Do we actually check that these peopel have
-- active DTV subscriptions? lolmagical...

select distinct account_number
into #account_churner
from mychurnrecords;

select distinct account_number
from sk_prod.VESPA_STB_PROG_EVENTS_20120124
where account_number in (select account_number from #account_churner)
order by account_number;
/*account_number
210094481012
220017348271
620003336299
621018036080
621019728065
621295940152
621331494966
621486914180
630088538438
*/

-- Okay... so that's guys that have churned off the base; what about people that SAV says aren't active?

select distinct account_number
into #my_account_lookup
from vespa_analysts.vespa_single_box_view
where panel_id_4_cells_confirm = 1 and status_vespa in ('Enabled','DisablePending');

commit;

create unique index fake_pk on #my_account_lookup (account_number);

select CUST_ACTIVE_DTV, count(1) as hits
from #my_account_lookup as mal
inner join sk_prod.cust_single_account_view as sav
on mal.account_number = sav.account_number
group by CUST_ACTIVE_DTV;
/*CUST_ACTIVE_DTV hits
0       221
1       280187
*/
-- lol, so we do have churners here who are still flagged as enabld. Are they reporting?

select mal.account_number
into #churnerguys
from #my_account_lookup as mal
inner join sk_prod.cust_single_account_view as sav
on mal.account_number = sav.account_number
where CUST_ACTIVE_DTV = 0;

select count(1), count(distinct account_number)
from sk_prod.VESPA_STB_PROG_EVENTS_20120124
where account_number in (select account_number from #churnerguys);
-- 351 records, 5 accounts - they're still returning data! wtf? How are we supposed
-- to track churners? how are we supposed to tell how big the panel is at any point
-- in time? this is all super messed up. Panel4 vs Panel5, open vs closed loop
-- enablement, and churn in there too making the whole population extra messy :-/

select distinct account_number
from sk_prod.VESPA_STB_PROG_EVENTS_20120124
where account_number in (select account_number from #churnerguys)
order by account_number;
/*account_number
210094481012 - matches a churned
220006142156 - nope!
621019728065 - 
621295940152
630088538438
*/




