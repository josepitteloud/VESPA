--The following code appears in the Waterfall procedure, where I believe the problem with prefixes starts.
--The reason being is that subscriber_id gets called repeatedly and has a record for every call, and there
--is no guarantee that the prefix is the same throughout a customer's history. We have about a million
--customers who have registered an empty_prefix value of one rather than a zero, which their most recent
--prefix would have indicated. E.g. Account_number 200005005042, subscriber_id 6366 has a prefix of 1470
--on 2013-01-18, however because the previous call (on 2012-12-07) has no prefix then it comes up as
--an empty prefix when we run the code


--      update Waterfall_Box_Rules as bas
--         set bas.known_prefix       = case when cb.subscriber_id is not null then 1 else 0 end,
--             bas.empty_prefix       = case when trim(prefix) = '' or prefix is null then 1 else 0 end
--        from vespa_analysts.Waterfall_callback_data  cb
--       where bas.subscriber_id = cb.subscriber_id
--         and cb.callback_seq = 1
--      commit



--The code used to find the most recent prefixes for each account_number / subscriber_id was as follows

--Create a table with all subscriber_ids who have at least one empty prefix in their history
select distinct account_number
        into #temp_prefix_dt1
        from vespa_analysts.Waterfall_callback_data
        where prefix = '' or prefix is null;
select count(distinct account_number) from #temp_prefix_dt1;
--8404251

--Create table that holds all accounts where the latest prefix is empty
select s.*
  into #temp_prefix_dt2
  from (select account_number,
           prefix,
           dt,
           row_number() over(partition by account_number
                                 order by dt desc) as rk
      from vespa_analysts.waterfall_callback_data) s
 where s.rk = 1 and prefix = '' or prefix is null;

select count(distinct account_number) from #temp_prefix_dt2
--7461368

--Create table containing only those accounts whose have had an empty prefix in the past
--but their most recent prefix was not empty and, hence, should not be in our tables
--Note using a left outer join and where condition as NOT IN does not appear to
select        d1.account_number
        into  #temp_prefix_dt3
        from  #temp_prefix_dt1 d1
        left outer join #temp_prefix_dt2 d2
        on    d1.account_number = d2.account_number
        where d2.account_number is null;
--942884 Row(s) affected


--Using the code in Churn_Identify_segments_April_2013 find the distinct account_numbers who are in the
--table vespa_analysts
select   distinct wat.account_number
        ,min(cbk_day) as cbk_day
    into #temp_account_with_prefix
    from vespa_analysts.waterfall_base                as wat
         left join sk_prod.VALUE_SEGMENTS_DATA as cvs on wat.account_number = cvs.account_number
         left join atrisk_results                     as bas on bas.account_number = wat.account_number
         left join vespa_analysts.accounts_to_exclude as exc on bas.account_number = exc.account_number
         left join vespa_analysts.Waterfall_SCMS_callback_data as cbk on wat.account_number = cbk.account_number
   where exc.account_number is null
     and knockout_level >= 24
     and (colour = 'Red' or value_seg in ('Bedding In', 'Unstable'))
--         and wat.account_number in (select account_number from dt_callback group by account_number having max(prefix) = '')
--         and wat.account_number not in (select account_number from kinnairt.DRX_595_BOXES_LIST);
group by wat.account_number;
--988634 Row(s) affected

--Same as above but want those without a prefix
select   distinct wat.account_number
        ,min(cbk_day) as cbk_day
    into #temp_account_no_prefix
    from vespa_analysts.waterfall_base                as wat
         left join sk_prod.VALUE_SEGMENTS_DATA as cvs on wat.account_number = cvs.account_number
         left join atrisk_results                     as bas on bas.account_number = wat.account_number
         left join vespa_analysts.accounts_to_exclude as exc on bas.account_number = exc.account_number
         left join vespa_analysts.Waterfall_SCMS_callback_data as cbk on wat.account_number = cbk.account_number
   where exc.account_number is null
     and knockout_level >= 24
     and (colour = 'Red' or value_seg in ('Bedding In', 'Unstable'))
         and wat.account_number in (select account_number from dt_callback group by account_number having max(prefix) = '')
--         and wat.account_number not in (select account_number from kinnairt.DRX_595_BOXES_LIST);
group by wat.account_number;
--958163 Row(s) affected

--Find those account_numbers that have a prefix
select *
        into #temp_account_prefixed
        from #temp_account_with_prefix
        where account_number not in (select account_number from #temp_account_no_prefix);
--30471 Row(s) affected

--Find those customers in vespa_analysts.waterfall_base who should not be there
select count(*) from #temp_account_prefixed where account_number in (select account_number from #temp_prefix_dt3);
--23753


