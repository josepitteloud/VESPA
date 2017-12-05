-- Investigation into why Dialback disagrees so much with Panman.
-- Turns out that going by event start gives you far less continuity
-- than by doc creation date; there are a lot of boxes that report
-- every day but have a few days a month where noone watches any TV.
-- That's ok, we just switch SBV over to the doc creation date.

select logs_every_day_30d, count(distinct sbv.subscriber_id) as hits
from vespa_single_box_view as sbv
inner join Vespa_PanMan_all_households as ah
on sbv.account_number = ah.account_number
group by logs_every_day_30d
/* -- Vespa:
logs_every_day_30d      hits
0       591591
1       24410
*/

-- All:
select logs_every_day_30d, count(distinct sbv.subscriber_id) as hits
from vespa_single_box_view as sbv
group by logs_every_day_30d
/*
logs_every_day_30d      hits
0       751660
1       27460
*/

-- So in comparison, 16k households reporting reliably doesn't seem bad at all.
-- But, WTF is the dialback doing then? Dialback says it's 180k boxes, which
-- we aren't seeing at all...

create variable @dialback_fragment varchar(2000);
create variable @panman_fragment varchar(2000);
create variable @scanning_day date;

-- Dialback version:
set @dialback_fragment = 'insert into dialback_dump (
        subscriber_id
        ,stb_log_creation_date
        ,doc_creation_date_from_9am
        ,doc_creation_date_from_6am
)
select
        subscriber_id
        ,stb_log_creation_date
        ,convert(date, dateadd(hh, -9, min(document_creation_date)))
        ,convert(date, dateadd(hh, -6, min(document_creation_date)))
from sk_prod.VESPA_STB_PROG_EVENTS_#*££*# -- will get replaced by the daily stamp of each table
where panel_id = 4 and document_creation_date is not null
group by subscriber_id, stb_log_creation_date
';

--SBV version:
set @panman_fragment = 'insert into panman_dump
    select
        subscriber_id
        ,convert(date, dateadd(hh, -6, min(adjusted_event_start_time)))
        ,convert(date, dateadd(hh, -6, min(document_creation_date)))
    from sk_prod.VESPA_STB_PROG_EVENTS_#*££*# -- will get replaced by the daily stamp of each table
    where panel_id = 4
    group by subscriber_id
';

-- The Dialback goes through a bit more process, but should still properly distinct out the
-- The big difference seems to be down to the 9AM thing? May have to actually line a few of
-- these guys up about the same reporting cycle... it may indeed be something like watching
-- between 6AM and 9AM making things count significantly differently? No, wait, the Dialback
-- goes on document creation date, the SBV goes on minimum event start time; could be mess
-- induced by playback stuff, which is great, because who knows how playback is to be considered?
-- But the Dialback does clip to the relevant period too. Reakon we rebuild the PanMan with
-- the filter we're using for Dialback (but with 6AM) and see if it aligns. Event start vs
-- Doc creation... we'd expect the Dialback version to have fewer numbers than the PanMan
-- one though? Weird.

drop table dialback_dump;
create table dialback_dump (
        subscriber_id                           bigint
        ,stb_log_creation_date                  datetime
        ,doc_creation_date_from_9am             date
        ,doc_creation_date_from_6am             date
);

drop table panman_dump;
create table panman_dump (
        subscriber_id                           bigint
        ,event_time_from_6am                    date
        ,doc_creation_date_from_6am             date
);

set @scanning_day = '2012-02-01';

while @scanning_day <= '2012-03-01'
begin
        execute(replace(@dialback_fragment, '#*££*#', dateformat(@scanning_day,'yyyymmdd')))
        execute(replace(@panman_fragment, '#*££*#', dateformat(@scanning_day,'yyyymmdd')))

        commit
        set @scanning_day = dateadd(day, 1, @scanning_day)
end;

-- OK, now let's look at what we've picked up...
select count(1), count(distinct subscriber_id) from dialback_dump
-- 16,477,787 and 410,530

select count(1), count(distinct subscriber_id) from panman_dump
-- 9,718,081 and 410,530

select distinct subscriber_id, doc_creation_date_from_9am
into dialback_summary1
from dialback_dump;
-- 9375584

select distinct subscriber_id, event_time_from_6am
into panman_summary1
from panman_dump;
-- 8472016
-- weird they're off by that much

select doc_creation_date_from_9am, count(1) as hits from dialback_summary1
group by doc_creation_date_from_9am order by doc_creation_date_from_9am;

select event_time_from_6am, count(1) as hits from panman_summary1
group by event_time_from_6am order by event_time_from_6am;

delete from dialback_summary1 where doc_creation_date_from_9am < '2012-02-01' or doc_creation_date_from_9am > '2012-02-29';
delete from panman_summary1 where event_time_from_6am < '2012-02-01' or event_time_from_6am > '2012-02-29';

-- these should be topping out at 29?
select hits, count(1) as subscribers
from (select subscriber_id, count(1) as hits from dialback_summary1 group by subscriber_id) as t
group by hits
order by hits desc;
/*hits    subscribers
29      188523
28      39579
27      15849
26      9739
25      6813
24      5345
23      4414
22      3770
21      3369
20      3306
19      3212
18      2794
17      2754
16      2705
15      2682
14      2862
13      2754
12      2790
11      4745
10      38328
9       6940
8       6521
7       5003
6       5122
5       5633
4       6133
3       7073
2       8519
1       11415
*/

select hits, count(1) as subscribers
from (select subscriber_id, count(1) as hits from panman_summary1 group by subscriber_id) as t
group by hits
order by hits desc;
/*
29      25410
28      35129
27      33933
26      32334
25      32609
24      33419
23      32319
22      26431
21      17608
20      10159
19      5775
18      4042
17      3541
16      3376
15      3308
14      3283
13      3195
12      3197
11      6125
10      16531
9       16027
8       13541
7       9337
6       6476
5       5691
4       5781
3       6244
2       6725
1       7954
*/