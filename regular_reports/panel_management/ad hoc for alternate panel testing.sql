-- So we're kicking around the stafforr test build of Single Box View
-- to get the other panels in and generally try out the alternate panel
-- bits of the Panel Management report.

-- What do things look like at the moment?
select panel, count(1) as boxes, count(distinct account_number) as accounts
from vespa_single_box_view
group by panel
order by panel;
/* Clash boxes are always fun, aren't they? They'll be guys that were on both 4 and 5 and haven't reported as 4 yet, and are still in STB log lookup as 5.
        130337  100897
CLASH!  123     121
SKYVIEW 32826   24241
VESPA   615835  411185

*/

-- Pull out the account numbers to put them into new panels
select account_number
,convert(double, null) as account_sampler
,convert(varchar(10), null) as panel_decision
into #PanMan_Panel_faking
from vespa_single_box_view
where panel = 'VESPA'
group by account_number;

-- Can't randomise in one step because we want to randomise by accounts and not by boxes.
update #PanMan_Panel_faking
set account_sampler = rand(rowid(#PanMan_Panel_faking) * datediff(ss, '2012-12-21 00:00:00', getdate()));

select min(account_sampler), max(account_sampler) from #PanMan_Panel_faking;

-- Keep 20% for Vespa and split the rest between alternate panels 6 and 7
update #PanMan_Panel_faking
set panel_decision = case
    when account_sampler < .2 then 'VESPA'
    when account_sampler < .6 then 'ALT6'
    else 'ALT7'
end;

commit;
create unique index fake_pk on #PanMan_Panel_faking (account_number);
commit;

update vespa_single_box_view
set panel = pf.panel_decision
from vespa_single_box_view inner join #PanMan_Panel_faking as pf
on vespa_single_box_view.account_number = pf.account_number;

-- OK, so how did that split things up?
select panel, count(1) as boxes, count(distinct account_number) as accounts
from vespa_single_box_view
group by panel
order by panel;
/* Ok, so because of account clashes etc, that also sucked a few items out
** of... well... every other category (not just Vespa) but hey, it's still
** only dev... though it'll get messy as we're reassigning whole acocunts
** from one panel to another, and if some households boxes are split across
** multiple panels... Might have to first segregate households which are on
** Sky View and request their removal just because.
        130271  100842
ALT6    246859  164762
ALT7    246481  164630
CLASH!  28      27
SKYVIEW 32821   24238
VESPA   122661  81793
*/


