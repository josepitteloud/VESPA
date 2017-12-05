      -- first branch
  select *
    into #AB_events1
    from #AB_events_in_output
   where effective_from_dt > [2_Years_Prior] 
			  and effective_from_dt =< Snapshot_Date
;

      -- aggregate      
  select account_number
								,max(effective_from_dt) as effective_from_dt_Max
				into #AB_events2
				from #AB_events1
group by account_number
;

      -- filter
  select account_number
        ,effective_from_dt_Max as Date_of_Last_AB_Call
    into #AB_events3
    from #AB_events2
;

		  		-- second branch
  select *
    into #AB_events4
    from #AB_events_in_output
   where effective_from_dt > [2_Years_Prior] 
			  and effective_from_dt =< Snapshot_Date
;

      -- aggregate
  select account_number
        ,count() as AB_events_Last_2_Years
    into #AB_events5
    from #AB_events4
;

      -- third branch
  select *
    into #AB_events6
    from #AB_events_in_output
   where effective_from_dt > [1_Year_Prior] 
			  and effective_from_dt =< Snapshot_Date
;

      -- aggregate
  select account_number
        ,count() as AB_events_Last_Year
    into #AB_events7
    from #AB_events6
;

      -- fourth branch
  select *
    into #AB_events8
    from #AB_events_in_output
   where effective_from_dt > [9_Months_Prior] and effective_from_dt <= Snapshot_Date
;

      -- aggregate
  select account_number
        ,count() as AB_events_Last_9_Months
    into #AB_events9
    from #AB_events8
;

      -- fifth branch
  select *
    into #AB_events10
    from #AB_events_in_output
   where effective_from_dt > [6_Months_Prior] and effective_from_dt <= Snapshot_Date
;

      -- aggregate
  select account_number
        ,count() as AB_events_Last_6_Months
    into #AB_events11
    from #AB_events10
;

      -- sixth branch
  select *
    into #AB_events12
    from #AB_events_in_output
   where effective_from_dt > [3_Months_Prior] and effective_from_dt <= Snapshot_Date
;

  select account_number
        ,count() as AB_events_Last_3_Months
    into #AB_events13
    from #AB_events12
;

  select *
    into #AB_events_output
    from #AB_events3
         full outer join #AB_events5
         full outer join #AB_events7
         full outer join #AB_events9
         full outer join #AB_events11
         full outer join #AB_events13
;

