      -- first branch
  select *
    into #PO_events1
    from #PO_events_in_output
   where effective_from_dt > [2_Years_Prior] 
			  and effective_from_dt =< Snapshot_Date
;

      -- aggregate      
  select account_number
								,max(effective_from_dt) as effective_from_dt_Max
				into #PO_events2
				from #PO_events1
group by account_number
;

      -- filter
  select account_number
        ,effective_from_dt_Max as Date_of_Last_PO_Call
    into #PO_events3
    from #PO_events2
;

		  		-- second branch
  select *
    into #PO_events4
    from #PO_events_in_output
   where effective_from_dt > [2_Years_Prior] 
			  and effective_from_dt =< Snapshot_Date
;

      -- aggregate
  select account_number
        ,count() as PO_events_Last_2_Years
    into #PO_events5
    from #PO_events4
;

      -- third branch
  select *
    into #PO_events6
    from #PO_events_in_output
   where effective_from_dt > [1_Year_Prior] 
			  and effective_from_dt =< Snapshot_Date
;

      -- aggregate
  select account_number
        ,count() as PO_events_Last_Year
    into #PO_events7
    from #PO_events6
;

      -- fourth branch
  select *
    into #PO_events8
    from #PO_events_in_output
   where effective_from_dt > [9_Months_Prior] and effective_from_dt <= Snapshot_Date
;

      -- aggregate
  select account_number
        ,count() as PO_events_Last_9_Months
    into #PO_events9
    from #PO_events8
;

      -- fifth branch
  select *
    into #PO_events10
    from #PO_events_in_output
   where effective_from_dt > [6_Months_Prior] and effective_from_dt <= Snapshot_Date
;

      -- aggregate
  select account_number
        ,count() as PO_events_Last_6_Months
    into #PO_events11
    from #PO_events10
;

      -- sixth branch
  select *
    into #PO_events12
    from #PO_events_in_output
   where effective_from_dt > [3_Months_Prior] and effective_from_dt <= Snapshot_Date
;

  select account_number
        ,count() as PO_events_Last_3_Months
    into #PO_events13
    from #PO_events12
;

  select *
    into #PO_events_output
    from #PO_events3
         full outer join #PO_events5
         full outer join #PO_events7
         full outer join #PO_events9
         full outer join #PO_events11
         full outer join #PO_events13
;

