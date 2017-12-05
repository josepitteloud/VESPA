      -- first branch
  select *
    into #PC_events1
    from #PC_events_in_output
   where effective_from_dt > [2_Years_Prior] 
			  and effective_from_dt =< Snapshot_Date
;

      -- aggregate      
  select account_number
								,max(effective_from_dt) as effective_from_dt_Max
				into #PC_events2
				from #PC_events1
group by account_number
;

      -- filter
  select account_number
        ,effective_from_dt_Max as Date_of_Last_PC_Call
    into #PC_events3
    from #PC_events2
;

		  		-- second branch
  select *
    into #PC_events4
    from #PC_events_in_output
   where effective_from_dt > [2_Years_Prior] 
			  and effective_from_dt =< Snapshot_Date
;

      -- aggregate
  select account_number
        ,count() as PC_events_Last_2_Years
    into #PC_events5
    from #PC_events4
;

      -- third branch
  select *
    into #PC_events6
    from #PC_events_in_output
   where effective_from_dt > [1_Year_Prior] 
			  and effective_from_dt =< Snapshot_Date
;

      -- aggregate
  select account_number
        ,count() as PC_events_Last_Year
    into #PC_events7
    from #PC_events6
;

      -- fourth branch
  select *
    into #PC_events8
    from #PC_events_in_output
   where effective_from_dt > [9_Months_Prior] and effective_from_dt <= Snapshot_Date
;

      -- aggregate
  select account_number
        ,count() as PC_events_Last_9_Months
    into #PC_events9
    from #PC_events8
;

      -- fifth branch
  select *
    into #PC_events10
    from #PC_events_in_output
   where effective_from_dt > [6_Months_Prior] and effective_from_dt <= Snapshot_Date
;

      -- aggregate
  select account_number
        ,count() as PC_events_Last_6_Months
    into #PC_events11
    from #PC_events10
;

      -- sixth branch
  select *
    into #PC_events12
    from #PC_events_in_output
   where effective_from_dt > [3_Months_Prior] and effective_from_dt <= Snapshot_Date
;

  select account_number
        ,count() as PC_events_Last_3_Months
    into #PC_events13
    from #PC_events12
;

  select *
    into #PC_events_output
    from #PC_events3
         full outer join #PC_events5
         full outer join #PC_events7
         full outer join #PC_events9
         full outer join #PC_events11
         full outer join #PC_events13
;

