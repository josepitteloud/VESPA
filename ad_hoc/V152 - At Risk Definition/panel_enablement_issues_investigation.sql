
--Additional code added 03/03/2013 to remove accounts where a prefix exists.

-- Ranking callbacks by most recent, only including those who are in the table #temp_accounts
--#dt_callback1 callback_seq ascending
--#dt_callback2 callback_seq descending
select  account_number
       ,subscriber_id
       ,dt
       ,callback_seq
       ,prefix
       ,rank() over (partition by account_number,subscriber_id order by dt desc, callback_seq, prefix desc, row_id desc) as dt_callback_seq
into    #dt_callback1
from    vespa_analysts.waterfall_callback_data
where   subscriber_id is not null
and     account_number is not null;

select  account_number
       ,subscriber_id
       ,dt
       ,callback_seq
       ,prefix
       ,rank() over (partition by account_number,subscriber_id order by dt desc, callback_seq desc, prefix desc, row_id desc) as dt_callback_seq
into    #dt_callback2
from    vespa_analysts.waterfall_callback_data
where   subscriber_id is not null
and     account_number is not null;

delete from #dt_callback1 where dt_callback_seq >1;
delete from #dt_callback2 where dt_callback_seq >1;
select top 20 * from #dt_callback1;
select top 20 * from #dt_callback2;


--Added penultimate line on 04/06/2013 to remove accounts with a DRX 595 box model
select count(*) as desc1 from (
    select   wat.account_number
            ,min(cbk_day) as cbk_day
        from vespa_analysts.waterfall_base                as wat
             left join sk_prod.VALUE_SEGMENTS_DATA as cvs on wat.account_number = cvs.account_number
             left join atrisk_results                     as bas on bas.account_number = wat.account_number
             left join vespa_analysts.accounts_to_exclude as exc on bas.account_number = exc.account_number
             left join vespa_analysts.Waterfall_SCMS_callback_data as cbk on wat.account_number = cbk.account_number
       where exc.account_number is null
         and knockout_level >= 24
         and (colour = 'Red' or value_seg in ('Bedding In', 'Unstable'))
    group by wat.account_number) as sub1;

select count(*) as desc2 from (
    select   wat.account_number
            ,min(cbk_day) as cbk_day
        from vespa_analysts.waterfall_base                as wat
             left join sk_prod.VALUE_SEGMENTS_DATA as cvs on wat.account_number = cvs.account_number
             left join atrisk_results                     as bas on bas.account_number = wat.account_number
             left join vespa_analysts.accounts_to_exclude as exc on bas.account_number = exc.account_number
             left join vespa_analysts.Waterfall_SCMS_callback_data as cbk on wat.account_number = cbk.account_number
       where exc.account_number is null
         and knockout_level >= 24
         and (colour = 'Red' or value_seg in ('Bedding In', 'Unstable'))
             and wat.account_number in (select account_number from #dt_callback1 group by account_number having max(prefix) = '')
    group by wat.account_number) as sub1;

select count(*) as desc3 from (
    select   wat.account_number
            ,min(cbk_day) as cbk_day
        from vespa_analysts.waterfall_base                as wat
             left join sk_prod.VALUE_SEGMENTS_DATA as cvs on wat.account_number = cvs.account_number
             left join atrisk_results                     as bas on bas.account_number = wat.account_number
             left join vespa_analysts.accounts_to_exclude as exc on bas.account_number = exc.account_number
             left join vespa_analysts.Waterfall_SCMS_callback_data as cbk on wat.account_number = cbk.account_number
       where exc.account_number is null
         and knockout_level >= 24
         and (colour = 'Red' or value_seg in ('Bedding In', 'Unstable'))
             and wat.account_number in (select account_number from #dt_callback1 group by account_number having max(prefix) = '')
             and wat.account_number not in (select account_number from kinnairt.DRX_595_BOXES_LIST)
    group by wat.account_number) as sub1;

select count(*) as desc2 from (
    select   wat.account_number
            ,min(cbk_day) as cbk_day
        from vespa_analysts.waterfall_base                as wat
             left join sk_prod.VALUE_SEGMENTS_DATA as cvs on wat.account_number = cvs.account_number
             left join atrisk_results                     as bas on bas.account_number = wat.account_number
             left join vespa_analysts.accounts_to_exclude as exc on bas.account_number = exc.account_number
             left join vespa_analysts.Waterfall_SCMS_callback_data as cbk on wat.account_number = cbk.account_number
       where exc.account_number is null
         and knockout_level >= 24
         and (colour = 'Red' or value_seg in ('Bedding In', 'Unstable'))
             and wat.account_number in (select account_number from #dt_callback2 group by account_number having max(prefix) = '')
    group by wat.account_number) as sub1;

select count(*) as desc3 from (
    select   wat.account_number
            ,min(cbk_day) as cbk_day
        from vespa_analysts.waterfall_base                as wat
             left join sk_prod.VALUE_SEGMENTS_DATA as cvs on wat.account_number = cvs.account_number
             left join atrisk_results                     as bas on bas.account_number = wat.account_number
             left join vespa_analysts.accounts_to_exclude as exc on bas.account_number = exc.account_number
             left join vespa_analysts.Waterfall_SCMS_callback_data as cbk on wat.account_number = cbk.account_number
       where exc.account_number is null
         and knockout_level >= 24
         and (colour = 'Red' or value_seg in ('Bedding In', 'Unstable'))
             and wat.account_number in (select account_number from #dt_callback2 group by account_number having max(prefix) = '')
             and wat.account_number not in (select account_number from kinnairt.DRX_595_BOXES_LIST)
    group by wat.account_number) as sub1;


