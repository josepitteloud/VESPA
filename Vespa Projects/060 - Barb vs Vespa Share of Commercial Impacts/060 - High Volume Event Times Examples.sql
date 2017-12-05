select Adjusted_Event_Start_Time
,Event_Type
,Channel_Name
,prog.Epg_Title
,count(*) as records
,count(distinct subscriber_id) as boxes
into #record_count
 from sk_prod.VESPA_STB_PROG_EVENTS_20120701 as vw
          left outer join   sk_prod.VESPA_EPG_DIM as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
        -- Filter for viewing events during extraction
    where panel_id in (4,5,12)
group by Adjusted_Event_Start_Time
,Event_Type
,Channel_Name
,prog.Epg_Title
order by records desc
;

select top 500 * from #record_count order by records desc;

select Adjusted_Event_Start_Time
,Event_Type
,Channel_Name
,prog.Epg_Title
,count(*) as records
,count(distinct subscriber_id) as boxes
into #record_count_20120702
 from sk_prod.VESPA_STB_PROG_EVENTS_20120702 as vw
          left outer join   sk_prod.VESPA_EPG_DIM as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
        -- Filter for viewing events during extraction
    where panel_id in (4,5,12)
group by Adjusted_Event_Start_Time
,Event_Type
,Channel_Name
,prog.Epg_Title
order by records desc
;

select top 500 * from #record_count_20120702 order by records desc;
commit;
select * from sk_prod.VESPA_STB_PROG_EVENTS_20120702 where Adjusted_Event_Start_Time='2012-07-02 12:05:10'
