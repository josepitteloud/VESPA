      -- merge
  select *
    into #HistoricalChurnBehaviour1
    from #SC_events_output                 as sco
         full outer join #AB_events_output as abo on sco.account_number = abo.account_number
         full outer join #PO_events_output as poo on sco.account_number = poo.account_number
         full outer join #PC_events_output as pco on sco.account_number = pco.account_number
;

      -- filter
  select SC_events_output.account_number
        ,case when SC_events_Last_Year     is null then 0 else SC_events_Last_Year     end
        ,case when SC_events_Last_3_Months is null then 0 else SC_events_Last_3_Months end
        ,case when SC_events_Last_6_Months is null then 0 else SC_events_Last_6_Months end
        ,case when SC_events_Last_9_Months is null then 0 else SC_events_Last_9_Months end
        ,case when SC_events_Last_2_Years  is null then 0 else SC_events_Last_2_Years  end
        ,Date_of_Last_SC_call
								,AB_events_output.account_number
        ,case when AB_events_Last_Year     is null then 0 else AB_events_Last_Year     end
        ,case when AB_events_Last_3_Months is null then 0 else AB_events_Last_3_Months end
        ,case when AB_events_Last_6_Months is null then 0 else AB_events_Last_6_Months end
        ,case when AB_events_Last_9_Months is null then 0 else AB_events_Last_9_Months end
        ,case when AB_events_Last_2_Years  is null then 0 else AB_events_Last_2_Years  end
        ,Date_of_Last_AB_call
								,PO_events_output.account_number
        ,case when PO_events_Last_Year     is null then 0 else PO_events_Last_Year     end
        ,case when PO_events_Last_3_Months is null then 0 else PO_events_Last_3_Months end
        ,case when PO_events_Last_6_Months is null then 0 else PO_events_Last_6_Months end
        ,case when PO_events_Last_9_Months is null then 0 else PO_events_Last_9_Months end
        ,case when PO_events_Last_2_Years  is null then 0 else PO_events_Last_2_Years  end
        ,Date_of_Last_PO_call
								,PC_events_output.account_number
        ,case when PC_events_Last_Year     is null then 0 else PC_events_Last_Year     end
        ,case when PC_events_Last_3_Months is null then 0 else PC_events_Last_3_Months end
        ,case when PC_events_Last_6_Months is null then 0 else PC_events_Last_6_Months end
        ,case when PC_events_Last_9_Months is null then 0 else PC_events_Last_9_Months end
        ,case when PC_events_Last_2_Years  is null then 0 else PC_events_Last_2_Years  end
        ,Date_of_Last_PC_call
				into #HistoricalChurnBehaviour_output
    from #HistoricalChurnBehaviour1

