      -- first branch
  select *
    into #SC_events1
    from #SC_events_in_output
   where effective_from_dt > [2_Years_Prior] 
			  and effective_from_dt =< Snapshot_Date
;

      -- aggregate      
  select account_number
								,max(effective_from_dt) as effective_from_dt_Max
				into #SC_events2
				from #SC_events1
group by account_number
;

      -- filter
  select account_number
        ,effective_from_dt_Max as Date_of_Last_SC_Call
    into #SC_events3
    from #SC_events2
;

		  		-- second branch
  select *
    into #SC_events4
    from #SC_events_in_output
   where effective_from_dt > [2_Years_Prior] 
			  and effective_from_dt =< Snapshot_Date
;

      -- aggregate
  select account_number
        ,count() as SC_events_Last_2_Years
    into #SC_events5
    from #SC_events4
;

      -- third branch
  select *
    into #SC_events6
    from #SC_events_in_output
   where effective_from_dt > [1_Year_Prior] 
			  and effective_from_dt =< Snapshot_Date
;

      -- aggregate
  select account_number
        ,count() as SC_events_Last_Year
    into #SC_events7
    from #SC_events6
;

      -- fourth branch
  select *
    into #SC_events8
    from #SC_events_in_output
   where effective_from_dt > [9_Months_Prior] and effective_from_dt <= Snapshot_Date
;

      -- aggregate
  select account_number
        ,count() as SC_events_Last_9_Months
    into #SC_events9
    from #SC_events8
;

      -- fifth branch
  select *
    into #SC_events10
    from #SC_events_in_output
   where effective_from_dt > [6_Months_Prior] and effective_from_dt <= Snapshot_Date
;

      -- aggregate
  select account_number
        ,count() as SC_events_Last_6_Months
    into #SC_events11
    from #SC_events10
;

      -- sixth branch
  select *
    into #SC_events12
    from #SC_events_in_output
   where effective_from_dt > [3_Months_Prior] and effective_from_dt <= Snapshot_Date
;

  select account_number
        ,count() as SC_events_Last_3_Months
    into #SC_events13
    from #SC_events12
;

  select *
    into #SC_events_output
    from #SC_events3
         full outer join #SC_events5
         full outer join #SC_events7
         full outer join #SC_events9
         full outer join #SC_events11
         full outer join #SC_events13
;

