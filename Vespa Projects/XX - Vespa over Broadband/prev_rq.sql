create table #temp(subscriber_id int);
insert into #temp values(43980);

  select tmp.subscriber_id
         ,sum(coalesce(case when dt between '2013-12-17' and '2013-12-31' then data_received else 0 end,0))/15.0 as x
    from #temp as tmp
         left join vespa_analysts.panel_data as pan on pan.subscriber_id = tmp.subscriber_id
group by tmp.subscriber_id
order by x

