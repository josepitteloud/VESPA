/*  Import Panel 5 data return logs from Netezza data extracts

Daily Netezza extracts are created by performing the following query:
    select
           date(log_received_datetime) as dt
           , scms_subscriber_id
    from dis_reference..FINAL_DTH_VIEWING_event_history
    where
        dt = '2014-02-14' -- chnage date as appropriate
        and panel_id_reported = 5
    group by
           dt
           , scms_subscriber_id
    order by
           dt
           , scms_subscriber_id
    ;


~~~
2014/02/13  Author  :   Hoi Yu Tang, hoiyu.tang@skyiq.co.uk

*/

--------------------
-- Initialise tables
--------------------
create table #netezza_p5_char(
        dt                      varchar(30)     default null
        , scms_subscriber_id    varchar(8)      default null
        )
;

/* Creation of tables if running within own schema for the very first time
drop table netezza_p5;
create table netezza_p5(
        dt                      date            default null
        , scms_subscriber_id    varchar(8)      default null
        , card_subscriber_id    varchar(8)      default null
        )
;

create date index idx2 on netezza_p5(dt);
create hg index idx1 on netezza_p5(scms_subscriber_id);
create hg index idx3 on netezza_p5(card_subscriber_id);

grant select on netezza_p5 to greenj;
*/

truncate table netezza_p5;





---------------------------------
-- Import from daily csv extracts
---------------------------------
load table      #netezza_p5_char
(               dt',',
                scms_subscriber_id'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/tanghoi/Netezza_P5_logs/2013-12-24_2014-01-31_subIDs.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
SKIP 1
;


load table      #netezza_p5_char
(               dt',',
                scms_subscriber_id'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/tanghoi/Netezza_P5_logs/2014-02-01_2014-02-07_subIDs.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
SKIP 1
;


load table      #netezza_p5_char
(               dt',',
                scms_subscriber_id'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/tanghoi/Netezza_P5_logs/2014-02-08_2014-02-14_subIDs.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
SKIP 1
;


load table      #netezza_p5_char
(               dt',',
                scms_subscriber_id'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/tanghoi/Netezza_P5_logs/2014-02-15_2014-02-21_subIDs.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
SKIP 1
;


load table      #netezza_p5_char
(               dt',',
                scms_subscriber_id'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/tanghoi/Netezza_P5_logs/2014-02-22_2014-02-28_subIDs.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
SKIP 1
;


load table      #netezza_p5_char
(               dt',',
                scms_subscriber_id'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/tanghoi/Netezza_P5_logs/2014-03-01_2014-03-07_subIDs.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
SKIP 1
;


load table      #netezza_p5_char
(               dt',',
                scms_subscriber_id'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/tanghoi/Netezza_P5_logs/2014-03-08_2014-03-14_subIDs.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
SKIP 1
;


load table      #netezza_p5_char
(               dt',',
                scms_subscriber_id'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/tanghoi/Netezza_P5_logs/2014-03-15_2014-03-18_subIDs.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
SKIP 1
;


----------------------------------------------------
-- Finally, convert dates and add card_subscriber_id
----------------------------------------------------
insert into netezza_p5(
    dt
    , scms_subscriber_id
    , card_subscriber_id
    )
select
        cast(left(a.dt,10) as date) as dt
        , scms_subscriber_id
        , right('00000000' + scms_subscriber_id,8) as  card_subscriber_id
from #netezza_p5_char as a
;




/* Checks...

select count() from netezza_p5; --2485853

select max(dt) from netezza_p5;

select top 20 * from netezza_p5;

select
        dt
        , count()
from netezza_p5
group by dt
order by dt
;

*/



