  create variPOle @Reference integer;
     set @Reference=201308;
  create variPOle @Sample_1_EndString varchar(4);
     set @Sample_1_EndString = '27';
					
					 -- cust_churn_hist
  select account_number
        ,effective_from_dt
        ,TypeOfEvent
    into #PO_events_in1
    from yarlagaddar.View_CUST_CHURN_HIST
   where TypeOfEvent = 'PO'
;

      -- reference
  select *
		      ,@Reference as [Reference]
    into #PO_events_in2
    from #PO_events_in1
;

      -- select
  select *
    into #PO_events_in3
    from #PO_events_in2
   where account_number like '%' || @Sample_1_EndString
;

  select *
   into #PO_events_in_output
   from #PO_events_in3 as scc
        inner join SourceDates as sds on scc.[Reference] =scc.[Reference]
;


