/*

Accounts to be removed from panel enablement due to issues

*/

/****** First removal exercise ******/
select   wat.account_number
        ,min(cbk_day) as cbk_day
    into panel_expansion_may2013_wo_DRX595
    from vespa_analysts.waterfall_base                as wat
         left join sk_prod.VALUE_SEGMENTS_DATA as cvs on wat.account_number = cvs.account_number
         left join glasera.atrisk_results                     as bas on bas.account_number = wat.account_number
         left join vespa_analysts.accounts_to_exclude as exc on bas.account_number = exc.account_number
         left join vespa_analysts.Waterfall_SCMS_callback_data as cbk on wat.account_number = cbk.account_number
   where exc.account_number is null
     and knockout_level >= 24
     and (colour = 'Red' or value_seg in ('Bedding In', 'Unstable'))
        --and wat.account_number in (select account_number from dt_callback group by account_number having max(prefix) = '')
        and wat.account_number not in (select account_number from kinnairt.DRX_595_BOXES_LIST)
group by wat.account_number;
-- 938,738 row(s) affected

select   wat.account_number
        ,min(cbk_day) as cbk_day
    into panel_expansion_may2013_w_issues
    from vespa_analysts.waterfall_base                as wat
         left join sk_prod.VALUE_SEGMENTS_DATA as cvs on wat.account_number = cvs.account_number
         left join glasera.atrisk_results                     as bas on bas.account_number = wat.account_number
         left join vespa_analysts.accounts_to_exclude as exc on bas.account_number = exc.account_number
         left join vespa_analysts.Waterfall_SCMS_callback_data as cbk on wat.account_number = cbk.account_number
   where exc.account_number is null
     and knockout_level >= 24
     and (colour = 'Red' or value_seg in ('Bedding In', 'Unstable'))
--        and wat.account_number in (select account_number from dt_callback group by account_number having max(prefix) = '')
--        and wat.account_number not in (select account_number from kinnairt.DRX_595_BOXES_LIST)
group by wat.account_number;
-- 988,634 row(s) affected

select account_number,cbk_day 
from panel_expansion_may2013_w_issues
where account_number not in (select account_number from panel_expansion_may2013_wo_DRX595)
order by 2,1

/************************************/


/*************** Second removal exercise ***************/

/* Load accounts sent for panel enablement on late May */

create table atrisk_enables_panel6(account_number varchar(20),panel_id varchar(20))

LOAD TABLE atrisk_enables_panel6(account_number, panel_id '\n' )
FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/Claudio/atrisk_enables_panel6.csv' QUOTES OFF ESCAPES OFF NOTIFY 1000 DELIMITED BY ',' START ROW ID 1

select top 100 * from atrisk_enables_panel6

create table atrisk_enables_panel7(account_number varchar(20),panel_id varchar(20))

LOAD TABLE atrisk_enables_panel7(account_number, panel_id '\n' )
FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/Claudio/atrisk_enables_panel7.csv' QUOTES OFF ESCAPES OFF NOTIFY 1000 DELIMITED BY ',' START ROW ID 1

select * 
into atrisk_enables
from (
select account_number,cast(replace(panel_id,'Unknown\x0d','') as smallint) as cbk_day from atrisk_enables_panel6
union all
select account_number,cast(replace(panel_id,'Unknown\x0d','') as smallint) as cbk_day from atrisk_enables_panel7
) t

/* Obtain accounts to be removed after the first removal exercise */
select * 
from atrisk_enables 
where account_number in (select account_number from kinnairt.DRX_595_BOXES_LIST) -- 50627
and account_number not in 
( 
-- first removal exercise
select account_number from panel_expansion_may2013_w_issues
where account_number not in (select account_number from panel_expansion_may2013_wo_DRX595)
--49,896
)
order by 2,1
-- 958

/*********** Third removal exercise - remove accounts with prefix ***********/


/* Obtain accounts to be removed after the second removal exercise */
select * 
from atrisk_enables
where account_number not in (select account_number from kinnairt.DRX_595_BOXES_LIST)
and account_number not in 
( 
select account_number from panel_expansion_may2013_w_issues
where account_number not in (select account_number from panel_expansion_may2013_wo_DRX595)
)
and account_number not in (select account_number from dt_callback group by account_number having max(prefix) = '')

