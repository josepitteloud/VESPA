--Account numbers of interest
select   wat.account_number
        ,min(cbk_day) as cbk_day
    INTO #temp_accounts
    from vespa_analysts.waterfall_base                as wat
         left join sk_prod.VALUE_SEGMENTS_DATA as cvs on wat.account_number = cvs.account_number
         left join atrisk_results                     as bas on bas.account_number = wat.account_number
         left join vespa_analysts.accounts_to_exclude as exc on bas.account_number = exc.account_number
         left join vespa_analysts.Waterfall_SCMS_callback_data as cbk on wat.account_number = cbk.account_number
   where exc.account_number is null
     and knockout_level >= 24
     and (colour = 'Red' or value_seg in ('Bedding In', 'Unstable'))
group by wat.account_number;

-- Ranking callbacks by most recent
select  account_number
       ,subscriber_id
       ,dt
       ,callback_seq
       ,prefix
       ,rank() over (partition by account_number,subscriber_id order by dt desc, callback_seq desc, prefix desc, row_id desc) as dt_callback_seq
into    dt_callback
from    vespa_analysts.waterfall_callback_data
where   subscriber_id is not null
and     account_number is not null
AND     account_number IN (SELECT account_number FROM #temp_accounts);

--Check
SELECT top 10 * FROM dt_callback;

-- remove older callbacks
delete from dt_callback where dt_callback_seq >1;

--Counts of prefixeded accounts
SELECT       prefix, count(*)
        FROM dt_callback
    GROUP BY prefix;

