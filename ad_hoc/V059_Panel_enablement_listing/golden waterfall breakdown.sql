/****************** WATERFALL ANALYSIS OF GOLDEN BOXES ******************/

-- The table "PanMan_adhoc_waterfall_base" arrives frm another script known
-- as "external - box loss waterfall.sql" and e just bring that in here as a
-- dependency. We've got the sky base and marks for where each account falls
-- out in the waterfall, so now, we just poke in the golden box list and see
-- where the fit in the waterfall.

-- Again, the waterfall is by household so we need to group up  these boxes.
-- We'll need to sum up how many boxes are NULL prefix or otherwise, and start
-- by producing numbers for accounts having only prefix free boxes. We also
-- want to split off box reliability so that the number of chcks it makes
-- exceeds 4 (do we have the percentage column on it?). Oh, any Anytime? That
-- gives us 8 universes to think about; suggest collapsing some of them.

-- Remember that the golden box source table is created in a different file,
--      \Vespa\ad_hoc\golden box anlaysis\golden_box_analysis.sql

-- Also, the percentage column is missing from the current build of the table?
-- that's okay, we've got the expected and the missed numbers.

-- Okay, again grouping the golden boxes by account, but this time we need all
-- of them. Though we don't want to exclude Vespa because we'll pick that up
-- from the waterfall anyway.

drop table PanMan_Golden_account_waterfalling;
drop table PanMan_Golden_waterfalling_summary;

select bas.account_number
    ,min(convert(tinyint,left(on_time, 1)))                         as worst_calling_back
    ,max(convert(tinyint,anytimeplus))                              as has_anytime_plus
    ,max(case when prefix is null then 1 else 0 end)                as has_null_prefix_boxes
    ,max(case when prefix = '' then 1 else 0 end)                   as has_emptystring_prefix_boxes
    ,max(case when prefix <> '' then 1 else 0 end)                  as has_bad_boxes
    ,convert(int, 0)                                            as waterfall_exit   -- there are no zeros, should help track non-updates
    ,convert(tinyint, 0)                                            as universe         -- 1 to 18, 0 is rejected by filters
into PanMan_Golden_account_waterfalling
from greenj.golden_boxes as bas
left join rampup_panel as ram on bas.account_number =ram.account_number
where ram.account_number is null
group by bas.account_number;

commit;
create unique index fake_pk on PanMan_Golden_account_waterfalling (account_number);

-- OK, and now attatch to the waterfall thing we built for all accounts:
update PanMan_Golden_account_waterfalling
set waterfall_exit = case when awb.knockout_level is null then -1 else awb.knockout_level end
from PanMan_Golden_account_waterfalling
inner join PanMan_adhoc_waterfall_base as awb
on PanMan_Golden_account_waterfalling.account_number = awb.account_number;

commit;

-- Assign each box to its universe:
update PanMan_Golden_account_waterfalling
   set universe = 1 + has_anytime_plus
                    + 2 * case  when worst_calling_back > 6 then 2
                                else worst_calling_back - 4
                          end
                    + 6 * case  when has_null_prefix_boxes = 1 and has_emptystring_prefix_boxes = 0 then 0
                                when has_null_prefix_boxes = 0 and has_emptystring_prefix_boxes = 1 then 1
                                else 2
                          end
  from PanMan_Golden_account_waterfalling
 where worst_calling_back >= 4
   and has_bad_boxes = 0;
-- ok, so how do those universe numbers map to the conditions? Check they split like we want...
-- Okay, so we've got some that are not updating? that means they're not active customers and we'll
-- assign them 6? Not quite, because the null ones are ones that don't fall out at all
update PanMan_Golden_account_waterfalling
set waterfall_exit = 6
where universe <> 0 and waterfall_exit = 0;

-- So these guys survive the waterfall:
update PanMan_Golden_account_waterfalling
set waterfall_exit = 25 -- there are 6-24 exit codes, and after that everything left (25) are good boxes
where universe <> 0 and waterfall_exit =-1;

commit;

select universe, waterfall_exit, count(1) as accounts
into PanMan_Golden_waterfalling_summary
from PanMan_Golden_account_waterfalling
where universe <> 0
group by universe, waterfall_exit;
-- okay, but then we still need to add the zeros etc or the pivot won't look right...

create unique index fake_pk on PanMan_Golden_waterfalling_summary (universe, waterfall_exit);

-- Put a quick listing of the numbers we want for waterfall stages
create table #mynumbers (
        my_counter         int          primary key
);

create variable t int;
set t = 6;
while t <= 24
begin
        insert into #mynumbers values (t)
        set t = t + 1
end;

select distinct universe
into #all_universes
from PanMan_Golden_waterfalling_summary;

-- don't bother with indices, the lookups are small...
insert into PanMan_Golden_waterfalling_summary
select u.universe
      ,n.my_counter
      ,0
  from #mynumbers as n
       cross join #all_universes as u
       left  join PanMan_Golden_waterfalling_summary as pgws on pgws.universe       = u.universe
                                                            and pgws.waterfall_exit = n.my_counter
 where pgws.accounts is null;

-- Selecting out the results!
  select sum(case when universe = 0 then accounts else 0 end) as universe0
        ,sum(case when universe = 1 then accounts else 0 end) as universe1
        ,sum(case when universe = 2 then accounts else 0 end) as universe2
        ,sum(case when universe = 3 then accounts else 0 end) as universe3
        ,sum(case when universe = 4 then accounts else 0 end) as universe4
        ,sum(case when universe = 5 then accounts else 0 end) as universe5
        ,sum(case when universe = 6 then accounts else 0 end) as universe6
        ,waterfall_exit
    from PanMan_Golden_waterfalling_summary as a
group by waterfall_exit
order by waterfall_exit;
-- OK, that'ds the listing that we wanted, we're done here, just pivot that in
-- Excel and we're away.



